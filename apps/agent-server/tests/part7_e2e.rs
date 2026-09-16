use agent_contracts::{
    EventKind, GatewayConnectionId, HandlerError, HandlerOutcome, Harness, HarnessContext,
    HarnessDescription, HarnessId, HarnessVersion, InitializationError, LlmRequest, Message,
    OperationId, OperationOutcome, OperationResult, OutcomeBuilder, ProjectId, SessionEvent,
    SessionId, SessionStatus, WaitId, WaitMode, WaitSpec,
};
use agent_gateways::{
    GatewayCallbackConfig, GatewayConnectionConfig, GatewayKind, GatewayRegistry,
};
use agent_runtime::{
    CompletionRuntime, CompletionSettings, DispatcherSettings, HarnessRegistry,
    OperationDispatcher, RetryPolicy, SchedulerSettings, SessionScheduler, WaitExpirationSettings,
    WaitExpirationWorker,
};
use agent_server::{ServerSettings, app_with_settings, serve_on_listener_with_shutdown};
use agent_store::{
    EventType, NewExternalEvent, NewSession, OperationStatus, PoolConfig, Store, StoreError,
};
use async_trait::async_trait;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, State},
    http::{HeaderMap, Method, Request, StatusCode, header},
    response::Response,
    routing::{get, post},
};
use hmac::{Hmac, Mac};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::Sha256;
use sqlx::PgPool;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

const CONNECTION: &str = "llm-e2e";
const GATEWAY_TOKEN: &str = "gateway-token";
const CALLBACK_SECRET: &str = "callback-secret";

struct Database {
    admin_url: String,
    name: String,
    store: Store,
}

impl Database {
    async fn new() -> Self {
        let admin_url = std::env::var("TEST_DATABASE_URL").expect(
            "TEST_DATABASE_URL must identify PostgreSQL with permission to create databases",
        );
        let admin = PgPool::connect(&admin_url).await.unwrap();
        let name = format!("agent_part7_e2e_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
        let mut url = Url::parse(&admin_url).unwrap();
        url.set_path(&format!("/{name}"));
        let store = Store::connect(url.as_str(), PoolConfig::default())
            .await
            .unwrap();
        store.migrate().await.unwrap();
        Self {
            admin_url,
            name,
            store,
        }
    }

    async fn close(self) {
        self.store.pool().close().await;
        let admin = PgPool::connect(&self.admin_url).await.unwrap();
        sqlx::query(&format!("DROP DATABASE {} WITH (FORCE)", self.name))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
    }
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
struct RecordingConfig {
    connection: String,
    account_id: Uuid,
    model_id: String,
}

#[derive(Deserialize, Serialize)]
struct RecordingState {
    pending: Option<OperationId>,
    response_id: Option<String>,
    preserved_provider_escape: bool,
}

struct RecordingHarness;

#[derive(Deserialize, Serialize)]
struct LifecycleState {
    pending: Option<OperationId>,
    wait_id: Option<WaitId>,
    turns: u32,
    ignored_old_completions: u32,
}

struct LifecycleHarness;

#[derive(Deserialize, Serialize)]
struct RecoveryState {
    turns: u32,
    wait_resumes: u32,
    operation_completions: u32,
}

struct RecoveryHarness {
    fail_second_message: Arc<AtomicBool>,
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
struct ExecutionCleanupConfig {
    connection: String,
    machine_id: Uuid,
    #[schemars(with = "serde_json::Value")]
    handle: agent_contracts::execution_core::ExecutionHandle,
}

#[derive(Deserialize, Serialize)]
struct ExecutionCleanupState;

struct ExecutionCleanupHarness;

#[async_trait]
impl Harness for ExecutionCleanupHarness {
    type Config = ExecutionCleanupConfig;
    type State = ExecutionCleanupState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("part8-execution-cleanup".into()),
            version: HarnessVersion("1".into()),
            name: "Part 8 execution cleanup fixture".into(),
            description: "Chooses process termination when a session is cancelled".into(),
        }
    }

    fn initialize(&self, _config: &Self::Config) -> Result<Self::State, InitializationError> {
        Ok(ExecutionCleanupState)
    }

    async fn handle(
        &self,
        config: &Self::Config,
        state: Self::State,
        event: SessionEvent,
        _context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<Self::State>, HandlerError> {
        let mut outcome = OutcomeBuilder::new(state);
        if let EventKind::CancellationRequested { .. } = event.kind {
            outcome
                .control_execution(
                    GatewayConnectionId(config.connection.clone()),
                    config.machine_id,
                    agent_contracts::execution::Operation::Terminate {
                        handle: config.handle,
                        grace_period_ms: Some(100),
                    },
                )
                .map_err(|error| HandlerError(error.to_string()))?;
            outcome.set_status(SessionStatus::Cancelling);
        }
        Ok(outcome.finish())
    }
}

#[async_trait]
impl Harness for RecoveryHarness {
    type Config = RecordingConfig;
    type State = RecoveryState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("part8-recovery".into()),
            version: HarnessVersion("1".into()),
            name: "Part 8 recovery fixture".into(),
            description: "Blocks its second message until the test clears an operational fault"
                .into(),
        }
    }

    fn initialize(&self, _config: &Self::Config) -> Result<Self::State, InitializationError> {
        Ok(RecoveryState {
            turns: 0,
            wait_resumes: 0,
            operation_completions: 0,
        })
    }

    async fn handle(
        &self,
        config: &Self::Config,
        state: Self::State,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<Self::State>, HandlerError> {
        let mut outcome = OutcomeBuilder::new(state);
        match event.kind {
            EventKind::UserMessage { message } => {
                if outcome.state_mut().turns == 1 && self.fail_second_message.load(Ordering::SeqCst)
                {
                    return Err(HandlerError("temporary fixture fault".into()));
                }
                outcome.state_mut().turns += 1;
                if outcome.state_mut().turns == 1 {
                    outcome.request_llm(
                        GatewayConnectionId(config.connection.clone()),
                        LlmRequest::Fresh {
                            account_id: config.account_id,
                            model_id: config.model_id.clone(),
                            instructions: None,
                            messages: vec![message],
                            tools: vec![],
                            provider_options: Map::new(),
                        },
                    );
                    outcome
                        .create_wait(WaitSpec {
                            mode: WaitMode::Expiration,
                            payload: json!({"kind":"short pause"}),
                            response_schema: None,
                            expires_at: Some(
                                chrono::Utc::now() + chrono::Duration::milliseconds(50),
                            ),
                        })
                        .map_err(|error| HandlerError(error.to_string()))?;
                    outcome.set_status(SessionStatus::Running);
                }
            }
            EventKind::WaitResumed { wait_id, .. } => {
                let wait = context
                    .wait(wait_id)
                    .await
                    .map_err(|error| HandlerError(error.to_string()))?
                    .ok_or_else(|| HandlerError("wait missing".into()))?;
                if wait.status != agent_contracts::WaitStatus::Expired {
                    return Err(HandlerError("wait was not expired".into()));
                }
                outcome.state_mut().wait_resumes += 1;
            }
            EventKind::OperationCompleted { operation_id, .. } => {
                let operation = context
                    .operation(operation_id)
                    .await
                    .map_err(|error| HandlerError(error.to_string()))?
                    .ok_or_else(|| HandlerError("operation missing".into()))?;
                if operation.outcome.is_none() {
                    return Err(HandlerError("operation has no outcome".into()));
                }
                outcome.state_mut().operation_completions += 1;
                outcome.set_status(SessionStatus::Idle);
            }
            EventKind::CancellationRequested { .. } => {}
        }
        Ok(outcome.finish())
    }
}

#[async_trait]
impl Harness for LifecycleHarness {
    type Config = RecordingConfig;
    type State = LifecycleState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("part8-lifecycle".into()),
            version: HarnessVersion("1".into()),
            name: "Part 8 lifecycle fixture".into(),
            description: "Chooses withdrawal or gateway cancellation by admission state".into(),
        }
    }

    fn initialize(&self, _config: &Self::Config) -> Result<Self::State, InitializationError> {
        Ok(LifecycleState {
            pending: None,
            wait_id: None,
            turns: 0,
            ignored_old_completions: 0,
        })
    }

    async fn handle(
        &self,
        config: &Self::Config,
        state: Self::State,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<Self::State>, HandlerError> {
        let mut outcome = OutcomeBuilder::new(state);
        match event.kind {
            EventKind::UserMessage { message } => {
                let operation_id = outcome.request_llm(
                    GatewayConnectionId(config.connection.clone()),
                    LlmRequest::Fresh {
                        account_id: config.account_id,
                        model_id: config.model_id.clone(),
                        instructions: None,
                        messages: vec![message],
                        tools: vec![],
                        provider_options: Map::new(),
                    },
                );
                outcome.state_mut().turns += 1;
                outcome.state_mut().pending = Some(operation_id);
                if outcome.state_mut().turns == 1 {
                    let wait_id = outcome
                        .create_wait(WaitSpec {
                            mode: WaitMode::External,
                            payload: json!({"prompt":"optional approval"}),
                            response_schema: None,
                            expires_at: None,
                        })
                        .map_err(|error| HandlerError(error.to_string()))?;
                    outcome.state_mut().wait_id = Some(wait_id);
                }
                outcome.set_status(SessionStatus::Running);
            }
            EventKind::CancellationRequested { .. } => {
                if let Some(target) = outcome.state_mut().pending.take() {
                    let operation = context
                        .operation(target)
                        .await
                        .map_err(|error| HandlerError(error.to_string()))?
                        .ok_or_else(|| HandlerError("target operation missing".into()))?;
                    if operation.gateway_job_id.is_some() {
                        outcome.request_llm_cancellation(target);
                    } else {
                        outcome.withdraw_operation(target);
                    }
                }
                if let Some(wait_id) = outcome.state_mut().wait_id.take() {
                    outcome.cancel_wait(wait_id);
                }
                outcome.set_status(SessionStatus::Cancelled);
            }
            EventKind::OperationCompleted { operation_id, .. } => {
                if outcome.state_mut().pending == Some(operation_id) {
                    outcome.state_mut().pending = None;
                    outcome.set_status(SessionStatus::Idle);
                } else {
                    outcome.state_mut().ignored_old_completions += 1;
                }
            }
            EventKind::WaitResumed { wait_id, .. } => {
                if outcome.state_mut().wait_id == Some(wait_id) {
                    outcome.state_mut().wait_id = None;
                }
            }
        }
        Ok(outcome.finish())
    }
}

#[async_trait]
impl Harness for RecordingHarness {
    type Config = RecordingConfig;
    type State = RecordingState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("part7-e2e".into()),
            version: HarnessVersion("1".into()),
            name: "Part 7 end-to-end fixture".into(),
            description: "Records the operation result read through the durable context".into(),
        }
    }

    fn initialize(&self, _config: &Self::Config) -> Result<Self::State, InitializationError> {
        Ok(RecordingState {
            pending: None,
            response_id: None,
            preserved_provider_escape: false,
        })
    }

    async fn handle(
        &self,
        config: &Self::Config,
        state: Self::State,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<Self::State>, HandlerError> {
        let mut outcome = OutcomeBuilder::new(state);
        match event.kind {
            EventKind::UserMessage { message } => {
                let operation_id = outcome.request_llm(
                    GatewayConnectionId(config.connection.clone()),
                    LlmRequest::Fresh {
                        account_id: config.account_id,
                        model_id: config.model_id.clone(),
                        instructions: None,
                        messages: vec![message],
                        tools: vec![],
                        provider_options: Map::new(),
                    },
                );
                outcome.state_mut().pending = Some(operation_id);
                outcome.set_status(SessionStatus::Running);
            }
            EventKind::OperationCompleted { operation_id, .. } => {
                let operation = context
                    .operation(operation_id)
                    .await
                    .map_err(|error| HandlerError(error.to_string()))?
                    .ok_or_else(|| HandlerError("completed operation is missing".into()))?;
                let Some(OperationOutcome::Succeeded(OperationResult::Llm(response))) =
                    operation.outcome
                else {
                    return Err(HandlerError("LLM operation did not succeed".into()));
                };
                outcome.state_mut().pending = None;
                outcome.state_mut().response_id = Some(response.id.clone());
                outcome.state_mut().preserved_provider_escape =
                    serde_json::to_string(&response.message)
                        .map_err(|error| HandlerError(error.to_string()))?
                        .contains("\\ud800");
                outcome.set_status(SessionStatus::Idle);
            }
            _ => {}
        }
        Ok(outcome.finish())
    }
}

#[derive(Clone)]
struct GatewayFixture {
    jobs: Arc<Mutex<VecDeque<Uuid>>>,
    submissions: Arc<Mutex<Vec<Value>>>,
    retrievals: Arc<Mutex<Vec<Uuid>>>,
    cancellations: Arc<Mutex<Vec<Uuid>>>,
}

async fn cancel_job(
    State(fixture): State<GatewayFixture>,
    Path(job_id): Path<Uuid>,
) -> Json<Value> {
    fixture.cancellations.lock().unwrap().push(job_id);
    Json(json!({
        "id": job_id,
        "status": "running",
        "cancelRequestedAt": "2026-09-15T00:00:00Z"
    }))
}

async fn submit_job(
    State(fixture): State<GatewayFixture>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    assert_eq!(
        headers.get(header::AUTHORIZATION).unwrap(),
        &format!("Bearer {GATEWAY_TOKEN}")
    );
    let request: Value = serde_json::from_slice(&body).unwrap();
    fixture.submissions.lock().unwrap().push(request);
    let job_id = fixture.jobs.lock().unwrap().pop_front().unwrap();
    (
        StatusCode::ACCEPTED,
        Json(json!({"id": job_id, "status": "queued"})),
    )
}

async fn get_job(
    State(fixture): State<GatewayFixture>,
    Path(job_id): Path<Uuid>,
    headers: HeaderMap,
) -> Response {
    assert_eq!(
        headers.get(header::AUTHORIZATION).unwrap(),
        &format!("Bearer {GATEWAY_TOKEN}")
    );
    fixture.retrievals.lock().unwrap().push(job_id);
    let body = format!(
        r#"{{"id":"{job_id}","status":"succeeded","response":{{"id":"response-{job_id}","modelId":"test-model","message":{{"role":"assistant","provider":"openai","content":[{{"native":"\ud800"}}]}},"stopReason":"stop","durationMs":2,"timestamp":1}},"error":null}}"#
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap()
}

async fn serve_gateway(
    jobs: Vec<Uuid>,
) -> (
    Url,
    GatewayFixture,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let fixture = GatewayFixture {
        jobs: Arc::new(Mutex::new(jobs.into())),
        submissions: Default::default(),
        retrievals: Default::default(),
        cancellations: Default::default(),
    };
    let router = Router::new()
        .route("/v1/jobs", post(submit_job))
        .route("/v1/jobs/{job_id}", get(get_job))
        .route("/v1/jobs/{job_id}/cancel", post(cancel_job))
        .with_state(fixture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(server_shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    (
        format!("http://{address}/").parse().unwrap(),
        fixture,
        shutdown,
        task,
    )
}

fn build_scheduler(store: &Store, registry: Arc<HarnessRegistry>) -> SessionScheduler {
    SessionScheduler::new(
        store.clone(),
        registry,
        SchedulerSettings {
            poll_interval: Duration::from_millis(1),
            lease_duration: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(100),
            ..SchedulerSettings::default()
        },
    )
    .unwrap()
}

fn build_dispatcher(store: &Store, gateways: Arc<GatewayRegistry>) -> OperationDispatcher {
    OperationDispatcher::new(
        store.clone(),
        gateways,
        DispatcherSettings {
            poll_interval: Duration::from_millis(1),
            lease_duration: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(100),
            retry_delays: vec![Duration::from_millis(1)],
            dependency_delay: Duration::from_millis(1),
            result_check_delay: Duration::from_millis(5),
            shutdown_grace: Duration::from_secs(1),
            ..DispatcherSettings::default()
        },
    )
    .unwrap()
}

fn build_completion(store: &Store, gateways: Arc<GatewayRegistry>) -> CompletionRuntime {
    CompletionRuntime::new(
        store.clone(),
        gateways,
        CompletionSettings {
            poll_interval: Duration::from_millis(1),
            lease_duration: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(100),
            retry_delay: Duration::from_millis(1),
            fallback_interval: Duration::from_millis(5),
            unmatched_retry_delay: Duration::from_millis(1),
            unmatched_retention: Duration::from_secs(60),
            request_retention: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(1),
            ..CompletionSettings::default()
        },
    )
    .unwrap()
}

async fn create_session(
    store: &Store,
    registry: &HarnessRegistry,
    idempotency_key: &str,
) -> SessionId {
    let session_id = SessionId::new();
    let configuration = serde_json::value::to_raw_value(&RecordingConfig {
        connection: CONNECTION.into(),
        account_id: Uuid::new_v4(),
        model_id: "test-model".into(),
    })
    .unwrap();
    let state = registry
        .get(&HarnessId("part7-e2e".into()), &HarnessVersion("1".into()))
        .unwrap()
        .initialize(configuration.as_ref())
        .unwrap();
    store
        .create_session(NewSession {
            id: session_id,
            project_id: ProjectId("part7".into()),
            harness_id: HarnessId("part7-e2e".into()),
            harness_version: HarnessVersion("1".into()),
            configuration,
            state,
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload: serde_json::value::to_raw_value(
                &json!({"message": Message::user_text("do the work")}),
            )
            .unwrap(),
            idempotency_key: Some(idempotency_key.into()),
        })
        .await
        .unwrap();
    session_id
}

async fn create_lifecycle_session(store: &Store, registry: &HarnessRegistry) -> SessionId {
    let session_id = SessionId::new();
    let configuration = serde_json::value::to_raw_value(&RecordingConfig {
        connection: CONNECTION.into(),
        account_id: Uuid::new_v4(),
        model_id: "test-model".into(),
    })
    .unwrap();
    let state = registry
        .get(
            &HarnessId("part8-lifecycle".into()),
            &HarnessVersion("1".into()),
        )
        .unwrap()
        .initialize(configuration.as_ref())
        .unwrap();
    store
        .create_session(NewSession {
            id: session_id,
            project_id: ProjectId("part8".into()),
            harness_id: HarnessId("part8-lifecycle".into()),
            harness_version: HarnessVersion("1".into()),
            configuration,
            state,
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    session_id
}

async fn lifecycle_input(store: &Store, session_id: SessionId, key: &str) {
    store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload: serde_json::value::to_raw_value(&json!({"message": Message::user_text(key)}))
                .unwrap(),
            idempotency_key: Some(key.into()),
        })
        .await
        .unwrap();
}

async fn lifecycle_cancel(store: &Store, session_id: SessionId, key: &str) {
    store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::CancellationRequested,
            payload: serde_json::value::to_raw_value(&json!({"reason":"stop"})).unwrap(),
            idempotency_key: Some(key.into()),
        })
        .await
        .unwrap();
}

fn callback_signature(timestamp: &str, event_id: Uuid, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(CALLBACK_SECRET.as_bytes()).unwrap();
    mac.update(format!("{timestamp}.{event_id}.{body}").as_bytes());
    format!("v1={}", hex::encode(mac.finalize().into_bytes()))
}

async fn send_callback(
    router: &Router,
    header_event_id: Uuid,
    payload_event_id: Uuid,
    job_id: Uuid,
) -> StatusCode {
    let body = json!({
        "eventId": payload_event_id,
        "type": "job.succeeded",
        "jobId": job_id,
        "completedAt": "2026-01-01T00:00:00Z"
    })
    .to_string();
    let timestamp = chrono::Utc::now().timestamp().to_string();
    let signature = callback_signature(&timestamp, header_event_id, &body);
    router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/v1/callbacks/llm/{CONNECTION}"))
                .header("x-llm-gateway-event-id", header_event_id.to_string())
                .header("x-llm-gateway-timestamp", timestamp)
                .header("x-llm-gateway-signature", signature)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn assert_session_observed_result(store: &Store, session_id: SessionId, job_id: Uuid) {
    let session = store.session(session_id).await.unwrap();
    assert_eq!(session.status, SessionStatus::Idle);
    assert_eq!(session.state_version.0, 2);
    let state: RecordingState = serde_json::from_str(session.state.get()).unwrap();
    assert_eq!(state.pending, None);
    assert_eq!(state.response_id, Some(format!("response-{job_id}")));
    assert!(state.preserved_provider_escape);
    let operation = store
        .operations(session_id, None, 10)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(operation.status, OperationStatus::Succeeded);
    let completion_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
    )
    .bind(operation.id.0)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(completion_events, 1);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn callback_before_mapping_and_polling_fallback_complete_the_real_async_path() {
    let database = Database::new().await;
    let callback_job = Uuid::new_v4();
    let fallback_job = Uuid::new_v4();
    let (gateway_url, fixture, gateway_shutdown, gateway_task) =
        serve_gateway(vec![callback_job, fallback_job]).await;
    let connection = GatewayConnectionConfig {
        id: GatewayConnectionId(CONNECTION.into()),
        kind: GatewayKind::Llm,
        base_url: gateway_url,
        bearer_token: GATEWAY_TOKEN.into(),
        timeout_ms: 1_000,
    };
    let gateways = Arc::new(GatewayRegistry::from_configs(vec![connection.clone()]).unwrap());
    let mut harnesses = HarnessRegistry::new();
    harnesses.register(RecordingHarness).unwrap();

    let callback_session = create_session(&database.store, &harnesses, "callback").await;
    let scheduler = build_scheduler(&database.store, Arc::new(harnesses));
    assert!(
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    let callback_operation = database
        .store
        .operations(callback_session, None, 10)
        .await
        .unwrap()
        .remove(0);

    let mut callback_harnesses = HarnessRegistry::new();
    callback_harnesses.register(RecordingHarness).unwrap();
    let (callback_router, _) = app_with_settings(
        database.store.clone(),
        callback_harnesses,
        vec!["backend-token".into()],
        ServerSettings {
            gateway_connections: vec![connection],
            callback_connections: vec![GatewayCallbackConfig {
                id: GatewayConnectionId(CONNECTION.into()),
                kind: GatewayKind::Llm,
                secrets: vec![CALLBACK_SECRET.into()],
            }],
            ..ServerSettings::default()
        },
    );
    let callback_event = Uuid::new_v4();
    assert_eq!(
        send_callback(
            &callback_router,
            callback_event,
            callback_event,
            callback_job,
        )
        .await,
        StatusCode::ACCEPTED
    );

    let completion = build_completion(&database.store, gateways.clone());
    assert!(completion.process_one_receipt().await.unwrap());
    let unmatched: (String, Option<Uuid>) = sqlx::query_as(
        "SELECT status,operation_id FROM gateway_callback_receipts WHERE gateway_event_id=$1",
    )
    .bind(callback_event)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(unmatched, ("pending".into(), None));
    assert_eq!(
        database
            .store
            .operation(callback_operation.id)
            .await
            .unwrap()
            .gateway_job_id,
        None
    );

    let dispatcher = build_dispatcher(&database.store, gateways.clone());
    assert!(dispatcher.process_one().await.unwrap());
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(completion.process_one_receipt().await.unwrap());
    let matched: (String, Option<Uuid>) = sqlx::query_as(
        "SELECT status,operation_id FROM gateway_callback_receipts WHERE gateway_event_id=$1",
    )
    .bind(callback_event)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(matched, ("processed".into(), Some(callback_operation.id.0)));
    assert!(completion.process_one_result().await.unwrap());
    assert!(
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    assert_session_observed_result(&database.store, callback_session, callback_job).await;

    let duplicate = send_callback(
        &callback_router,
        callback_event,
        callback_event,
        callback_job,
    )
    .await;
    assert_eq!(duplicate, StatusCode::ACCEPTED);
    assert_eq!(
        send_callback(
            &callback_router,
            callback_event,
            callback_event,
            Uuid::new_v4(),
        )
        .await,
        StatusCode::CONFLICT
    );
    assert_eq!(
        send_callback(
            &callback_router,
            callback_event,
            Uuid::new_v4(),
            callback_job,
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    let receipt_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM gateway_callback_receipts WHERE gateway_event_id=$1",
    )
    .bind(callback_event)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(receipt_count, 1);

    let mut fallback_harnesses = HarnessRegistry::new();
    fallback_harnesses.register(RecordingHarness).unwrap();
    let fallback_session = create_session(&database.store, &fallback_harnesses, "fallback").await;
    let fallback_scheduler = build_scheduler(&database.store, Arc::new(fallback_harnesses));
    assert!(
        fallback_scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    assert!(dispatcher.process_one().await.unwrap());
    tokio::time::sleep(Duration::from_millis(10)).await;

    // A fresh worker instance proves that accepted work is recovered without callback state.
    let restarted_completion = build_completion(&database.store, gateways);
    assert!(restarted_completion.process_one_result().await.unwrap());
    assert!(
        fallback_scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    assert_session_observed_result(&database.store, fallback_session, fallback_job).await;

    {
        let submissions = fixture.submissions.lock().unwrap();
        assert_eq!(submissions.len(), 2);
        assert_eq!(
            submissions[0]["idempotencyKey"],
            callback_operation.id.to_string()
        );
    }
    assert_eq!(
        fixture.retrievals.lock().unwrap().as_slice(),
        &[callback_job, fallback_job]
    );

    gateway_shutdown.cancel();
    gateway_task.await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn cancellation_harness_withdraws_unsubmitted_work_and_ignores_old_wait_and_result() {
    let database = Database::new().await;
    let new_job = Uuid::new_v4();
    let (gateway_url, fixture, gateway_stop, gateway_task) = serve_gateway(vec![new_job]).await;
    let gateways = Arc::new(
        GatewayRegistry::from_configs(vec![GatewayConnectionConfig {
            id: GatewayConnectionId(CONNECTION.into()),
            kind: GatewayKind::Llm,
            base_url: gateway_url,
            bearer_token: GATEWAY_TOKEN.into(),
            timeout_ms: 1_000,
        }])
        .unwrap(),
    );
    let mut registry = HarnessRegistry::new();
    registry.register(LifecycleHarness).unwrap();
    let session_id = create_lifecycle_session(&database.store, &registry).await;
    let scheduler = build_scheduler(&database.store, Arc::new(registry));
    let dispatcher = build_dispatcher(&database.store, gateways);
    lifecycle_input(&database.store, session_id, "first").await;
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let old_operation = database
        .store
        .operations(session_id, None, 10)
        .await
        .unwrap()[0]
        .id;
    let old_wait = database.store.waits(session_id, None, 10).await.unwrap()[0].id;
    lifecycle_cancel(&database.store, session_id, "cancel").await;
    lifecycle_input(&database.store, session_id, "second").await;
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        database.store.wait(old_wait).await.unwrap().status,
        agent_store::WaitStatus::Cancelled
    );
    assert_eq!(
        database.store.session(session_id).await.unwrap().status,
        SessionStatus::Running
    );
    let state: LifecycleState = serde_json::from_str(
        database
            .store
            .session(session_id)
            .await
            .unwrap()
            .state
            .get(),
    )
    .unwrap();
    let new_operation = state.pending.unwrap();
    assert_ne!(old_operation, new_operation);

    assert!(dispatcher.process_one().await.unwrap());
    assert_eq!(
        database
            .store
            .operation(old_operation)
            .await
            .unwrap()
            .status,
        OperationStatus::Cancelled
    );
    assert!(dispatcher.process_one().await.unwrap());
    assert_eq!(fixture.submissions.lock().unwrap().len(), 1);
    assert_eq!(fixture.cancellations.lock().unwrap().len(), 0);
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let session = database.store.session(session_id).await.unwrap();
    let state: LifecycleState = serde_json::from_str(session.state.get()).unwrap();
    assert_eq!(state.pending, Some(new_operation));
    assert_eq!(session.status, SessionStatus::Running);
    assert!(state.ignored_old_completions >= 2);
    gateway_stop.cancel();
    gateway_task.await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn cancellation_harness_requests_gateway_cancel_and_late_accepted_result_is_correlated() {
    let database = Database::new().await;
    let old_job = Uuid::new_v4();
    let new_job = Uuid::new_v4();
    let (gateway_url, fixture, gateway_stop, gateway_task) =
        serve_gateway(vec![old_job, new_job]).await;
    let gateways = Arc::new(
        GatewayRegistry::from_configs(vec![GatewayConnectionConfig {
            id: GatewayConnectionId(CONNECTION.into()),
            kind: GatewayKind::Llm,
            base_url: gateway_url,
            bearer_token: GATEWAY_TOKEN.into(),
            timeout_ms: 1_000,
        }])
        .unwrap(),
    );
    let mut registry = HarnessRegistry::new();
    registry.register(LifecycleHarness).unwrap();
    let session_id = create_lifecycle_session(&database.store, &registry).await;
    let scheduler = build_scheduler(&database.store, Arc::new(registry));
    let dispatcher = build_dispatcher(&database.store, gateways.clone());
    let completion = build_completion(&database.store, gateways);
    lifecycle_input(&database.store, session_id, "first").await;
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let old_operation = database
        .store
        .operations(session_id, None, 10)
        .await
        .unwrap()[0]
        .id;
    assert!(dispatcher.process_one().await.unwrap());
    assert_eq!(
        database
            .store
            .operation(old_operation)
            .await
            .unwrap()
            .status,
        OperationStatus::Accepted
    );
    lifecycle_cancel(&database.store, session_id, "cancel").await;
    lifecycle_input(&database.store, session_id, "second").await;
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let new_operation = serde_json::from_str::<LifecycleState>(
        database
            .store
            .session(session_id)
            .await
            .unwrap()
            .state
            .get(),
    )
    .unwrap()
    .pending
    .unwrap();
    assert_ne!(new_operation, old_operation);
    assert_eq!(
        database.store.session(session_id).await.unwrap().status,
        SessionStatus::Running
    );

    assert!(dispatcher.process_one().await.unwrap());
    assert_eq!(fixture.cancellations.lock().unwrap().as_slice(), &[old_job]);
    assert!(dispatcher.process_one().await.unwrap());
    assert_eq!(fixture.submissions.lock().unwrap().len(), 2);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(completion.process_one_result().await.unwrap());
    assert_eq!(
        database
            .store
            .operation(old_operation)
            .await
            .unwrap()
            .status,
        OperationStatus::Succeeded
    );
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let session = database.store.session(session_id).await.unwrap();
    let state: LifecycleState = serde_json::from_str(session.state.get()).unwrap();
    assert_eq!(state.pending, Some(new_operation));
    assert_eq!(session.status, SessionStatus::Running);
    assert!(state.ignored_old_completions >= 2);
    gateway_stop.cancel();
    gateway_task.await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn blocked_head_accumulates_results_then_concurrent_retry_replays_only_that_event() {
    let database = Database::new().await;
    let job_id = Uuid::new_v4();
    let (gateway_url, fixture, gateway_stop, gateway_task) = serve_gateway(vec![job_id]).await;
    let gateways = Arc::new(
        GatewayRegistry::from_configs(vec![GatewayConnectionConfig {
            id: GatewayConnectionId(CONNECTION.into()),
            kind: GatewayKind::Llm,
            base_url: gateway_url,
            bearer_token: GATEWAY_TOKEN.into(),
            timeout_ms: 1_000,
        }])
        .unwrap(),
    );
    let fail = Arc::new(AtomicBool::new(true));
    let mut registry = HarnessRegistry::new();
    registry
        .register(RecoveryHarness {
            fail_second_message: fail.clone(),
        })
        .unwrap();
    let session_id = SessionId::new();
    let configuration = serde_json::value::to_raw_value(&RecordingConfig {
        connection: CONNECTION.into(),
        account_id: Uuid::new_v4(),
        model_id: "test-model".into(),
    })
    .unwrap();
    let state = registry
        .get(
            &HarnessId("part8-recovery".into()),
            &HarnessVersion("1".into()),
        )
        .unwrap()
        .initialize(configuration.as_ref())
        .unwrap();
    database
        .store
        .create_session(NewSession {
            id: session_id,
            project_id: ProjectId("part8".into()),
            harness_id: HarnessId("part8-recovery".into()),
            harness_version: HarnessVersion("1".into()),
            configuration,
            state,
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings {
            retry_policy: RetryPolicy {
                retry_delays: vec![Duration::from_millis(1), Duration::from_millis(1)],
            },
            ..SchedulerSettings::default()
        },
    )
    .unwrap();
    let dispatcher = build_dispatcher(&database.store, gateways.clone());
    let completion = build_completion(&database.store, gateways);
    lifecycle_input(&database.store, session_id, "first").await;
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let operation_id = database
        .store
        .operations(session_id, None, 10)
        .await
        .unwrap()[0]
        .id;
    let wait_id = database.store.waits(session_id, None, 10).await.unwrap()[0].id;
    lifecycle_input(&database.store, session_id, "second").await;
    let blocked_event_id: Uuid =
        sqlx::query_scalar("SELECT id FROM session_events WHERE session_id=$1 AND sequence=2")
            .bind(session_id.0)
            .fetch_one(database.store.pool())
            .await
            .unwrap();
    assert!(dispatcher.process_one().await.unwrap());
    for _ in 0..3 {
        assert!(
            scheduler
                .process_one(CancellationToken::new())
                .await
                .unwrap()
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let blocked = database.store.session(session_id).await.unwrap();
    assert!(!blocked.processing_enabled);
    assert_eq!(blocked.blocked_event_id.unwrap().0, blocked_event_id);
    tokio::time::sleep(Duration::from_millis(60)).await;
    let wait_worker =
        WaitExpirationWorker::new(database.store.clone(), WaitExpirationSettings::default())
            .unwrap();
    assert_eq!(wait_worker.process_batch().await.unwrap(), 1);
    assert!(completion.process_one_result().await.unwrap());
    assert_eq!(
        database.store.wait(wait_id).await.unwrap().status,
        agent_store::WaitStatus::Expired
    );
    assert_eq!(
        database.store.operation(operation_id).await.unwrap().status,
        OperationStatus::Succeeded
    );
    assert_eq!(
        database
            .store
            .session(session_id)
            .await
            .unwrap()
            .state_version
            .0,
        1
    );
    let queued: Vec<(i64, String)> = sqlx::query_as(
        "SELECT sequence,status FROM session_events WHERE session_id=$1 AND sequence>=2 ORDER BY sequence",
    ).bind(session_id.0).fetch_all(database.store.pool()).await.unwrap();
    assert_eq!(
        queued,
        vec![
            (2, "blocked".into()),
            (3, "pending".into()),
            (4, "pending".into())
        ]
    );

    fail.store(false, Ordering::SeqCst);
    let request = serde_json::value::to_raw_value(&json!({
        "expected_event_id": blocked_event_id, "expected_processing_revision": blocked.processing_revision,
    })).unwrap();
    let event_id = agent_contracts::EventId(blocked_event_id);
    let (left, right) = tokio::join!(
        database.store.retry_blocked_event(
            session_id,
            event_id,
            blocked.processing_revision,
            "same-retry",
            &request,
            None
        ),
        database.store.retry_blocked_event(
            session_id,
            event_id,
            blocked.processing_revision,
            "same-retry",
            &request,
            None
        ),
    );
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.processing_revision, right.processing_revision);
    assert_eq!(left.retried_at, right.retried_at);
    let audit_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM session_processing_retries WHERE session_id=$1")
            .bind(session_id.0)
            .fetch_one(database.store.pool())
            .await
            .unwrap();
    assert_eq!(audit_count, 1);
    for _ in 0..3 {
        assert!(
            scheduler
                .process_one(CancellationToken::new())
                .await
                .unwrap()
        );
    }
    let session = database.store.session(session_id).await.unwrap();
    let state: RecoveryState = serde_json::from_str(session.state.get()).unwrap();
    assert_eq!(
        (state.turns, state.wait_resumes, state.operation_completions),
        (2, 1, 1)
    );
    assert_eq!(session.status, SessionStatus::Idle);
    assert_eq!(fixture.submissions.lock().unwrap().len(), 1);
    assert_eq!(
        database
            .store
            .operations(session_id, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    let duplicate = database
        .store
        .retry_blocked_event(
            session_id,
            event_id,
            blocked.processing_revision,
            "same-retry",
            &request,
            None,
        )
        .await
        .unwrap();
    assert_eq!(duplicate.retried_at, left.retried_at);
    assert!(matches!(
        database
            .store
            .retry_blocked_event(
                session_id,
                event_id,
                blocked.processing_revision,
                "new-key",
                &request,
                None,
            )
            .await,
        Err(StoreError::StaleProcessingRetry)
    ));
    gateway_stop.cancel();
    gateway_task.await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn cancellation_harness_can_dispatch_explicit_execution_termination_without_observation() {
    let database = Database::new().await;
    let job_id = Uuid::new_v4();
    let (gateway_url, fixture, gateway_stop, gateway_task) = serve_gateway(vec![job_id]).await;
    let connection = GatewayConnectionId("execution-e2e".into());
    let gateways = Arc::new(
        GatewayRegistry::from_configs(vec![GatewayConnectionConfig {
            id: connection.clone(),
            kind: GatewayKind::Execution,
            base_url: gateway_url,
            bearer_token: GATEWAY_TOKEN.into(),
            timeout_ms: 1_000,
        }])
        .unwrap(),
    );
    let config = ExecutionCleanupConfig {
        connection: connection.0.clone(),
        machine_id: Uuid::new_v4(),
        handle: agent_contracts::execution_core::ExecutionHandle {
            id: Uuid::new_v4(),
            generation_id: Uuid::new_v4(),
        },
    };
    let mut registry = HarnessRegistry::new();
    registry.register(ExecutionCleanupHarness).unwrap();
    let configuration = serde_json::value::to_raw_value(&config).unwrap();
    let state = registry
        .get(
            &HarnessId("part8-execution-cleanup".into()),
            &HarnessVersion("1".into()),
        )
        .unwrap()
        .initialize(configuration.as_ref())
        .unwrap();
    let session_id = SessionId::new();
    database
        .store
        .create_session(NewSession {
            id: session_id,
            project_id: ProjectId("part8".into()),
            harness_id: HarnessId("part8-execution-cleanup".into()),
            harness_version: HarnessVersion("1".into()),
            configuration,
            state,
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let scheduler = build_scheduler(&database.store, Arc::new(registry));
    let dispatcher = build_dispatcher(&database.store, gateways);
    lifecycle_cancel(&database.store, session_id, "cancel").await;
    assert!(
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    assert_eq!(
        database.store.session(session_id).await.unwrap().status,
        SessionStatus::Cancelling
    );
    assert!(dispatcher.process_one().await.unwrap());
    {
        let submissions = fixture.submissions.lock().unwrap();
        assert_eq!(submissions.len(), 1);
        assert_eq!(
            submissions[0]["request"]["operation"],
            "execution.terminate"
        );
        assert_eq!(
            submissions[0]["request"]["params"]["handle"]["id"],
            config.handle.id.to_string()
        );
    }
    assert!(!dispatcher.process_one().await.unwrap());
    gateway_stop.cancel();
    gateway_task.await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn combined_server_restart_delivers_a_result_and_cleans_its_request() {
    let database = Database::new().await;
    let job_id = Uuid::new_v4();
    let (gateway_url, fixture, gateway_stop, gateway_task) = serve_gateway(vec![job_id]).await;
    let connection = GatewayConnectionConfig {
        id: GatewayConnectionId(CONNECTION.into()),
        kind: GatewayKind::Llm,
        base_url: gateway_url,
        bearer_token: GATEWAY_TOKEN.into(),
        timeout_ms: 1_000,
    };
    let mut registry = HarnessRegistry::new();
    registry.register(RecordingHarness).unwrap();
    let session_id = create_session(&database.store, &registry, "one-message").await;
    // A duplicate input with the same identity must not create a second handler turn.
    let duplicate = database
        .store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload: serde_json::value::to_raw_value(
                &json!({"message": Message::user_text("do the work")}),
            )
            .unwrap(),
            idempotency_key: Some("one-message".into()),
        })
        .await
        .unwrap();
    assert!(matches!(duplicate, agent_store::EnqueueResult::Existing(_)));

    let mut settings = ServerSettings {
        gateway_connections: vec![connection],
        ..ServerSettings::default()
    };
    settings.scheduler.poll_interval = Duration::from_millis(5);
    settings.dispatcher.poll_interval = Duration::from_millis(5);
    settings.dispatcher.request_retention = Duration::from_millis(50);
    settings.dispatcher.result_check_delay = Duration::from_millis(5);
    settings.completion.enabled = false;
    settings.completion.poll_interval = Duration::from_millis(5);
    settings.completion.fallback_interval = Duration::from_millis(5);
    settings.completion.request_retention = Duration::from_millis(50);
    settings.request_cleanup.poll_interval = Duration::from_millis(5);
    settings.shutdown_grace = Duration::from_secs(1);

    let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_stop = CancellationToken::new();
    let first_signal = first_stop.clone();
    let first_store = database.store.clone();
    let first_settings = settings.clone();
    let first = tokio::spawn(async move {
        serve_on_listener_with_shutdown(
            first_listener,
            first_store,
            registry,
            vec!["backend-token".into()],
            first_settings,
            first_signal.cancelled_owned(),
        )
        .await
    });
    let operation_id = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(operation) = database
                .store
                .operations(session_id, None, 10)
                .await
                .unwrap()
                .first()
                && operation.status == OperationStatus::Accepted
            {
                break operation.id;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    first_stop.cancel();
    first.await.unwrap().unwrap();
    assert_eq!(fixture.submissions.lock().unwrap().len(), 1);
    assert!(
        database
            .store
            .operation_request(operation_id)
            .await
            .unwrap()
            .is_some()
    );

    let mut second_registry = HarnessRegistry::new();
    second_registry.register(RecordingHarness).unwrap();
    settings.completion.enabled = true;
    let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_stop = CancellationToken::new();
    let second_signal = second_stop.clone();
    let second_store = database.store.clone();
    let second = tokio::spawn(async move {
        serve_on_listener_with_shutdown(
            second_listener,
            second_store,
            second_registry,
            vec!["backend-token".into()],
            settings,
            second_signal.cancelled_owned(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let session = database.store.session(session_id).await.unwrap();
            let request = database
                .store
                .operation_request(operation_id)
                .await
                .unwrap();
            if session.state_version.0 == 2 && request.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    second_stop.cancel();
    second.await.unwrap().unwrap();
    assert_session_observed_result(&database.store, session_id, job_id).await;
    assert_eq!(fixture.submissions.lock().unwrap().len(), 1);
    let completion_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE session_id=$1 AND type='operation_completed'",
    )
    .bind(session_id.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(completion_events, 1);
    gateway_stop.cancel();
    gateway_task.await.unwrap();
    database.close().await;
}
