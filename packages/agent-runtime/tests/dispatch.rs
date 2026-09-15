use agent_contracts::{
    GatewayConnectionId, HarnessId, HarnessVersion, LlmRequest, Message, OperationId,
    OperationKind, OperationRequest, ProjectId, SessionId, ToolDefinition,
};
use agent_gateways::{
    CancellationAcknowledgement, ExecutionJobStatus, GatewayClient, GatewayConnectionConfig,
    GatewayError, GatewayFailure, GatewayJobStatus, GatewayKind, GatewayRegistry,
    JobAcknowledgement, LlmJobStatus,
};
use agent_runtime::{DispatcherSettings, OperationDispatcher};
use agent_store::{
    EventType, HandlerClaim, NewExternalEvent, NewOperation, NewSession, OperationPhase,
    OperationStatus, OutcomeCommit, PoolConfig, Store,
};
use async_trait::async_trait;
use axum::{Router, body::Bytes, http::StatusCode, response::Response, routing::post};
use serde_json::{Map, json, value::RawValue};
use sqlx::PgPool;
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

struct Database {
    admin_url: String,
    name: String,
    store: Store,
}

impl Database {
    async fn new() -> Self {
        let admin_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL is required");
        let admin = PgPool::connect(&admin_url).await.unwrap();
        let name = format!("agent_dispatch_test_{}", Uuid::new_v4().simple());
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

fn raw(value: impl Into<String>) -> Box<RawValue> {
    RawValue::from_string(value.into()).unwrap()
}

async fn session(store: &Store) -> SessionId {
    let id = SessionId::new();
    store
        .create_session(NewSession {
            id,
            project_id: ProjectId("dispatch".into()),
            harness_id: HarnessId("fixture".into()),
            harness_version: HarnessVersion("1".into()),
            configuration: raw("{}"),
            state: raw("{}"),
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    id
}

async fn stage(store: &Store, session_id: SessionId, key: &str, operations: Vec<NewOperation>) {
    store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload: raw(
                r#"{"message":{"role":"user","content":[{"type":"text","text":"work"}]}}"#,
            ),
            idempotency_key: Some(key.into()),
        })
        .await
        .unwrap();
    let event = store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    store
        .commit_handler_outcome(
            &HandlerClaim::from(&event),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw("{}"),
                status: None,
                history: vec![],
                operations,
                waits: vec![],
            },
        )
        .await
        .unwrap();
}

fn operation(id: OperationId, request: OperationRequest) -> NewOperation {
    let (kind, connection, target, previous) = match &request {
        OperationRequest::Llm {
            connection,
            request,
        } => (
            OperationKind::Llm,
            Some(connection.clone()),
            None,
            match request {
                LlmRequest::Continuation {
                    previous_operation_id,
                    ..
                } => Some(*previous_operation_id),
                _ => None,
            },
        ),
        OperationRequest::Execution { connection, .. } => (
            OperationKind::Execution,
            Some(connection.clone()),
            None,
            None,
        ),
        OperationRequest::LlmCancellation { target } => {
            (OperationKind::LlmCancellation, None, Some(*target), None)
        }
        OperationRequest::Withdraw { target } => {
            (OperationKind::Withdraw, None, Some(*target), None)
        }
    };
    NewOperation {
        id,
        kind,
        gateway_connection_id: connection,
        target_operation_id: target,
        previous_operation_id: previous,
        request: serde_json::value::to_raw_value(&request).unwrap(),
    }
}

fn llm_request(connection: &str) -> OperationRequest {
    OperationRequest::Llm {
        connection: GatewayConnectionId(connection.into()),
        request: LlmRequest::Fresh {
            account_id: Uuid::new_v4(),
            model_id: "test-model".into(),
            instructions: None,
            messages: vec![Message::User {
                base: agent_contracts::MessageBase::default(),
                content: vec![agent_contracts::ContentPart::Text {
                    text: "hello".into(),
                    metadata: None,
                }],
            }],
            tools: Vec::<ToolDefinition>::new(),
            provider_options: Map::new(),
        },
    }
}

fn llm_ack(id: Uuid, status: LlmJobStatus) -> JobAcknowledgement {
    JobAcknowledgement {
        id,
        status: GatewayJobStatus::Llm(status),
    }
}

fn execution_ack(id: Uuid, status: ExecutionJobStatus) -> JobAcknowledgement {
    JobAcknowledgement {
        id,
        status: GatewayJobStatus::Execution(status),
    }
}

#[derive(Clone)]
struct MockClient {
    kind: GatewayKind,
    submissions: Arc<Mutex<Vec<String>>>,
    results: Arc<Mutex<VecDeque<Result<JobAcknowledgement, GatewayError>>>>,
    lookups: SharedQueue<Option<JobAcknowledgement>>,
    cancellations: Arc<Mutex<Vec<Uuid>>>,
}

type SharedQueue<T> = Arc<Mutex<VecDeque<Result<T, GatewayError>>>>;

struct BlockingClient {
    started: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl GatewayClient for BlockingClient {
    fn kind(&self) -> GatewayKind {
        GatewayKind::Llm
    }

    async fn submit(&self, _request: &RawValue) -> Result<JobAcknowledgement, GatewayError> {
        self.started.notify_one();
        std::future::pending().await
    }
}

impl MockClient {
    fn new(kind: GatewayKind, results: Vec<Result<JobAcknowledgement, GatewayError>>) -> Self {
        Self {
            kind,
            submissions: Default::default(),
            results: Arc::new(Mutex::new(results.into())),
            lookups: Default::default(),
            cancellations: Default::default(),
        }
    }
}

#[async_trait]
impl GatewayClient for MockClient {
    fn kind(&self) -> GatewayKind {
        self.kind
    }
    async fn submit(&self, request: &RawValue) -> Result<JobAcknowledgement, GatewayError> {
        self.submissions.lock().unwrap().push(request.get().into());
        self.results
            .lock()
            .unwrap()
            .pop_front()
            .expect("mock submission result")
    }
    async fn find_by_idempotency_key(
        &self,
        _idempotency_key: &str,
    ) -> Result<Option<JobAcknowledgement>, GatewayError> {
        self.lookups.lock().unwrap().pop_front().unwrap_or(Ok(None))
    }
    async fn cancel(&self, job_id: Uuid) -> Result<CancellationAcknowledgement, GatewayError> {
        self.cancellations.lock().unwrap().push(job_id);
        Ok(CancellationAcknowledgement { accepted: true })
    }
}

fn dispatcher(store: &Store, id: &str, client: MockClient) -> OperationDispatcher {
    let mut gateways = GatewayRegistry::new();
    gateways
        .register(GatewayConnectionId(id.into()), Arc::new(client))
        .unwrap();
    OperationDispatcher::new(
        store.clone(),
        Arc::new(gateways),
        DispatcherSettings {
            retry_delays: vec![Duration::from_millis(1)],
            dependency_delay: Duration::from_millis(1),
            result_check_delay: Duration::from_millis(1),
            ..DispatcherSettings::default()
        },
    )
    .unwrap()
}

fn http_dispatcher(
    store: &Store,
    id: &str,
    kind: GatewayKind,
    address: std::net::SocketAddr,
    timeout_ms: u64,
) -> OperationDispatcher {
    let gateways = GatewayRegistry::from_configs(vec![GatewayConnectionConfig {
        id: GatewayConnectionId(id.into()),
        kind,
        base_url: format!("http://{address}/").parse().unwrap(),
        bearer_token: "dispatch-test-token".into(),
        timeout_ms,
    }])
    .unwrap();
    OperationDispatcher::new(
        store.clone(),
        Arc::new(gateways),
        DispatcherSettings {
            retry_delays: vec![Duration::from_millis(1)],
            dependency_delay: Duration::from_millis(1),
            result_check_delay: Duration::from_millis(1),
            lease_duration: Duration::from_millis(200),
            lease_renewal_interval: Duration::from_millis(50),
            ..DispatcherSettings::default()
        },
    )
    .unwrap()
}

async fn serve(app: Router) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (address, task)
}

fn drop_http_response() -> Response {
    panic!("fixture drops the accepted HTTP response")
}

async fn accept_staged_operation(store: &Store, operation_id: OperationId, job_id: Uuid) {
    let claim = store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.operation.id, operation_id);
    store
        .record_operation_accepted(&claim, job_id, chrono::Utc::now(), Some(202))
        .await
        .unwrap();
}

async fn complete_accepted_operation(store: &Store, operation_id: OperationId) {
    let claim = store
        .claim_operation(OperationPhase::ResultRetrieval, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.operation.id, operation_id);
    let result = raw(r#"{"kind":"llm","value":{}}"#);
    store
        .complete_operation(
            &claim,
            OperationStatus::Succeeded,
            Some(&result),
            None,
            None,
        )
        .await
        .unwrap();
}

async fn commit_operations_on_head(store: &Store, operations: Vec<NewOperation>) {
    let event = store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    store
        .commit_handler_outcome(
            &HandlerClaim::from(&event),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw("{}"),
                status: None,
                history: vec![],
                operations,
                waits: vec![],
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn fresh_llm_submission_uses_one_stable_request_and_records_acceptance() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let operation_id = OperationId::new();
    let mut request = llm_request("llm");
    let OperationRequest::Llm {
        request: LlmRequest::Fresh { messages, .. },
        ..
    } = &mut request
    else {
        unreachable!()
    };
    messages.push(Message::Assistant {
        base: agent_contracts::MessageBase::default(),
        provider: agent_contracts::Provider::Openai,
        content: vec![raw(r#"{"native":"\ud800"}"#)],
    });
    stage(
        &db.store,
        session_id,
        "one",
        vec![operation(operation_id, request)],
    )
    .await;
    let job_id = Uuid::new_v4();
    let client = MockClient::new(
        GatewayKind::Llm,
        vec![Ok(llm_ack(job_id, LlmJobStatus::Queued))],
    );
    let dispatcher = dispatcher(&db.store, "llm", client.clone());
    assert!(dispatcher.process_one().await.unwrap());
    let stored = db.store.operation(operation_id).await.unwrap();
    assert_eq!(stored.status, OperationStatus::Accepted);
    assert_eq!(stored.gateway_job_id, Some(job_id));
    let submitted = client.submissions.lock().unwrap()[0].clone();
    assert!(submitted.contains(&format!(r#""idempotencyKey":"{operation_id}""#)));
    assert!(!submitted.contains("instructions"));
    assert!(submitted.contains(r#""role":"user""#));
    assert!(submitted.contains(r#"\ud800"#));
    assert!(
        db.store
            .operation_request(operation_id)
            .await
            .unwrap()
            .is_some()
    );
    let request_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM session_operation_requests WHERE operation_id=$1")
            .bind(operation_id.0)
            .fetch_one(db.store.pool())
            .await
            .unwrap();
    assert_eq!(request_rows, 1);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn execution_submission_preserves_generation_and_protocol_payload() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let operation_id = OperationId::new();
    let generation = Uuid::new_v4();
    stage(
        &db.store,
        session_id,
        "one",
        vec![operation(
            operation_id,
            OperationRequest::Execution {
                connection: GatewayConnectionId("execution".into()),
                machine_id: Uuid::new_v4(),
                expected_generation_id: Some(generation),
                request: agent_contracts::execution::Payload::Single(
                    agent_contracts::execution::Operation::Info,
                ),
            },
        )],
    )
    .await;
    let client = MockClient::new(
        GatewayKind::Execution,
        vec![Ok(execution_ack(
            Uuid::new_v4(),
            ExecutionJobStatus::Queued,
        ))],
    );
    dispatcher(&db.store, "execution", client.clone())
        .process_one()
        .await
        .unwrap();
    let submitted: serde_json::Value =
        serde_json::from_str(&client.submissions.lock().unwrap()[0]).unwrap();
    assert_eq!(submitted["idempotencyKey"], operation_id.to_string());
    assert_eq!(
        submitted["request"]["expected_generation_id"],
        generation.to_string()
    );
    assert_eq!(submitted["request"]["operation"], "runtime.info");
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn definite_rejection_is_delivered_but_uncertainty_is_preserved() {
    let db = Database::new().await;
    let rejected_session = session(&db.store).await;
    let rejected_id = OperationId::new();
    stage(
        &db.store,
        rejected_session,
        "reject",
        vec![operation(rejected_id, llm_request("reject"))],
    )
    .await;
    let rejected = MockClient::new(
        GatewayKind::Llm,
        vec![Err(GatewayError::Rejected {
            status: 409,
            code: "account_disabled".into(),
            message: "account is disabled".into(),
        })],
    );
    dispatcher(&db.store, "reject", rejected)
        .process_one()
        .await
        .unwrap();
    let rejected_operation = db.store.operation(rejected_id).await.unwrap();
    assert_eq!(rejected_operation.status, OperationStatus::Failed);
    assert_eq!(rejected_operation.error.unwrap()["source"], "admission");
    let completion_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
    )
    .bind(rejected_id.0)
    .fetch_one(db.store.pool())
    .await
    .unwrap();
    assert_eq!(completion_events, 1);

    let uncertain_session = session(&db.store).await;
    let uncertain_id = OperationId::new();
    stage(
        &db.store,
        uncertain_session,
        "uncertain",
        vec![operation(uncertain_id, llm_request("uncertain"))],
    )
    .await;
    let client = MockClient::new(
        GatewayKind::Llm,
        vec![
            Err(GatewayError::Uncertain(GatewayFailure {
                status: None,
                code: "response_lost".into(),
                message: "response lost".into(),
            })),
            Err(GatewayError::Rejected {
                status: 401,
                code: "unauthorized".into(),
                message: "key revoked".into(),
            }),
        ],
    );
    let dispatcher = dispatcher(&db.store, "uncertain", client.clone());
    dispatcher.process_one().await.unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    dispatcher.process_one().await.unwrap();
    let operation = db.store.operation(uncertain_id).await.unwrap();
    assert_eq!(operation.status, OperationStatus::Submitting);
    {
        let bodies = client.submissions.lock().unwrap();
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0], bodies[1]);
    }
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn uncertain_submission_recovers_the_existing_job_by_stable_key() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let operation_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "recover",
        vec![operation(operation_id, llm_request("llm"))],
    )
    .await;
    let client = MockClient::new(
        GatewayKind::Llm,
        vec![Err(GatewayError::Uncertain(GatewayFailure {
            status: None,
            code: "response_lost".into(),
            message: "response lost".into(),
        }))],
    );
    let job_id = Uuid::new_v4();
    client
        .lookups
        .lock()
        .unwrap()
        .push_back(Ok(Some(llm_ack(job_id, LlmJobStatus::Running))));
    let dispatcher = dispatcher(&db.store, "llm", client.clone());
    dispatcher.process_one().await.unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    dispatcher.process_one().await.unwrap();
    let stored = db.store.operation(operation_id).await.unwrap();
    assert_eq!(stored.status, OperationStatus::Accepted);
    assert_eq!(stored.gateway_job_id, Some(job_id));
    assert_eq!(client.submissions.lock().unwrap().len(), 1);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn llm_cancellation_targets_the_mapped_job_and_completes_independently() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let target_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "target",
        vec![operation(target_id, llm_request("llm"))],
    )
    .await;
    let job_id = Uuid::new_v4();
    let cancellations = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/v1/jobs",
            post(move || async move { axum::Json(json!({"id":job_id,"status":"queued"})) }),
        )
        .route(
            "/v1/jobs/{id}/cancel",
            post({
                let cancellations = cancellations.clone();
                move || {
                    cancellations.fetch_add(1, Ordering::SeqCst);
                    async move {
                        axum::Json(json!({
                            "id":job_id,
                            "status":"running",
                            "cancelRequestedAt":"2026-09-15T00:00:00Z"
                        }))
                    }
                }
            }),
        );
    let (address, server) = serve(app).await;
    let dispatcher = http_dispatcher(&db.store, "llm", GatewayKind::Llm, address, 1000);
    dispatcher.process_one().await.unwrap();

    let cancellation_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "cancel",
        vec![operation(
            cancellation_id,
            OperationRequest::LlmCancellation { target: target_id },
        )],
    )
    .await;
    dispatcher.process_one().await.unwrap();
    let cancellation = db.store.operation(cancellation_id).await.unwrap();
    assert_eq!(cancellation.status, OperationStatus::Succeeded);
    assert_eq!(cancellations.load(Ordering::SeqCst), 1);
    let result: serde_json::Value =
        serde_json::from_str(cancellation.result.unwrap().get()).unwrap();
    assert_eq!(result["value"]["accepted"], true);
    assert_eq!(
        db.store.operation(target_id).await.unwrap().status,
        OperationStatus::Accepted
    );
    server.abort();
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn continuation_resolves_the_successful_parent_gateway_job() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let parent_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "parent",
        vec![operation(parent_id, llm_request("llm"))],
    )
    .await;
    let parent_job_id = Uuid::new_v4();
    let client = MockClient::new(
        GatewayKind::Llm,
        vec![
            Ok(llm_ack(parent_job_id, LlmJobStatus::Queued)),
            Ok(llm_ack(Uuid::new_v4(), LlmJobStatus::Queued)),
        ],
    );
    let dispatcher = dispatcher(&db.store, "llm", client.clone());
    dispatcher.process_one().await.unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    let result_claim = db
        .store
        .claim_operation(OperationPhase::ResultRetrieval, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let result = raw(r#"{"kind":"llm","value":{}}"#);
    db.store
        .complete_operation(
            &result_claim,
            OperationStatus::Succeeded,
            Some(&result),
            None,
            None,
        )
        .await
        .unwrap();

    let continuation_id = OperationId::new();
    commit_operations_on_head(
        &db.store,
        vec![operation(
            continuation_id,
            OperationRequest::Llm {
                connection: GatewayConnectionId("llm".into()),
                request: LlmRequest::Continuation {
                    previous_operation_id: parent_id,
                    messages: vec![],
                },
            },
        )],
    )
    .await;
    dispatcher.process_one().await.unwrap();
    {
        let bodies = client.submissions.lock().unwrap();
        let continuation: serde_json::Value = serde_json::from_str(&bodies[1]).unwrap();
        assert_eq!(continuation["previousJobId"], parent_job_id.to_string());
        assert_eq!(continuation["idempotencyKey"], continuation_id.to_string());
    }
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn continuation_waits_for_an_outstanding_parent_without_gateway_io() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let parent_id = OperationId(Uuid::from_u128(1));
    let continuation_id = OperationId(Uuid::from_u128(2));
    stage(
        &db.store,
        session_id,
        "continuation-wait",
        vec![
            operation(parent_id, llm_request("llm")),
            operation(
                continuation_id,
                OperationRequest::Llm {
                    connection: GatewayConnectionId("llm".into()),
                    request: LlmRequest::Continuation {
                        previous_operation_id: parent_id,
                        messages: vec![],
                    },
                },
            ),
        ],
    )
    .await;
    sqlx::query(
        "UPDATE session_operations SET next_attempt_at=clock_timestamp()+interval '1 hour' WHERE id=$1",
    )
    .bind(parent_id.0)
    .execute(db.store.pool())
    .await
    .unwrap();
    let client = MockClient::new(GatewayKind::Llm, vec![]);
    dispatcher(&db.store, "llm", client.clone())
        .process_one()
        .await
        .unwrap();
    assert_eq!(
        db.store.operation(continuation_id).await.unwrap().status,
        OperationStatus::Pending
    );
    assert!(client.submissions.lock().unwrap().is_empty());
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn invalid_continuations_fail_before_submission_and_expired_parents_keep_gateway_rejection() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let failed_parent = OperationId::new();
    stage(
        &db.store,
        session_id,
        "failed-parent",
        vec![operation(failed_parent, llm_request("llm"))],
    )
    .await;
    dispatcher(
        &db.store,
        "llm",
        MockClient::new(
            GatewayKind::Llm,
            vec![Err(GatewayError::Rejected {
                status: 409,
                code: "account_disabled".into(),
                message: "account disabled".into(),
            })],
        ),
    )
    .process_one()
    .await
    .unwrap();
    let failed_continuation = OperationId::new();
    stage(
        &db.store,
        session_id,
        "failed-continuation",
        vec![operation(
            failed_continuation,
            OperationRequest::Llm {
                connection: GatewayConnectionId("llm".into()),
                request: LlmRequest::Continuation {
                    previous_operation_id: failed_parent,
                    messages: vec![],
                },
            },
        )],
    )
    .await;
    let no_io = MockClient::new(GatewayKind::Llm, vec![]);
    dispatcher(&db.store, "llm", no_io.clone())
        .process_one()
        .await
        .unwrap();
    let failed = db.store.operation(failed_continuation).await.unwrap();
    assert_eq!(failed.status, OperationStatus::Failed);
    assert_eq!(failed.error.unwrap()["code"], "continuation_parent_failed");
    assert!(no_io.submissions.lock().unwrap().is_empty());

    let successful_parent = OperationId::new();
    stage(
        &db.store,
        session_id,
        "successful-parent",
        vec![operation(successful_parent, llm_request("first"))],
    )
    .await;
    accept_staged_operation(&db.store, successful_parent, Uuid::new_v4()).await;
    complete_accepted_operation(&db.store, successful_parent).await;
    let mismatched_continuation = OperationId::new();
    stage(
        &db.store,
        session_id,
        "mismatched-continuation",
        vec![operation(
            mismatched_continuation,
            OperationRequest::Llm {
                connection: GatewayConnectionId("second".into()),
                request: LlmRequest::Continuation {
                    previous_operation_id: successful_parent,
                    messages: vec![],
                },
            },
        )],
    )
    .await;
    let no_io = MockClient::new(GatewayKind::Llm, vec![]);
    dispatcher(&db.store, "second", no_io.clone())
        .process_one()
        .await
        .unwrap();
    let mismatched = db.store.operation(mismatched_continuation).await.unwrap();
    assert_eq!(mismatched.status, OperationStatus::Failed);
    assert_eq!(mismatched.error.unwrap()["code"], "invalid_continuation");
    assert!(no_io.submissions.lock().unwrap().is_empty());

    let expired_parent = OperationId::new();
    stage(
        &db.store,
        session_id,
        "expired-parent",
        vec![operation(expired_parent, llm_request("http"))],
    )
    .await;
    accept_staged_operation(&db.store, expired_parent, Uuid::new_v4()).await;
    complete_accepted_operation(&db.store, expired_parent).await;
    let expired_continuation = OperationId::new();
    stage(
        &db.store,
        session_id,
        "expired-continuation",
        vec![operation(
            expired_continuation,
            OperationRequest::Llm {
                connection: GatewayConnectionId("http".into()),
                request: LlmRequest::Continuation {
                    previous_operation_id: expired_parent,
                    messages: vec![],
                },
            },
        )],
    )
    .await;
    let (address, server) = serve(Router::new().route(
        "/v1/jobs",
        post(|| async {
            (
                StatusCode::GONE,
                axum::Json(json!({"error":{
                    "code":"previous_request_expired",
                    "message":"previous request expired"
                }})),
            )
        }),
    ))
    .await;
    http_dispatcher(&db.store, "http", GatewayKind::Llm, address, 1000)
        .process_one()
        .await
        .unwrap();
    let expired = db.store.operation(expired_continuation).await.unwrap();
    assert_eq!(expired.status, OperationStatus::Failed);
    assert_eq!(expired.error.unwrap()["code"], "previous_request_expired");
    server.abort();
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn withdrawal_wins_before_submission_without_contacting_the_gateway() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let target_id = OperationId(Uuid::from_u128(1));
    let withdrawal_id = OperationId(Uuid::from_u128(2));
    stage(
        &db.store,
        session_id,
        "withdraw",
        vec![
            operation(target_id, llm_request("llm")),
            operation(
                withdrawal_id,
                OperationRequest::Withdraw { target: target_id },
            ),
        ],
    )
    .await;
    let client = MockClient::new(GatewayKind::Llm, vec![]);
    dispatcher(&db.store, "llm", client.clone())
        .process_one()
        .await
        .unwrap();
    assert_eq!(
        db.store.operation(target_id).await.unwrap().status,
        OperationStatus::Cancelled
    );
    assert_eq!(
        db.store.operation(withdrawal_id).await.unwrap().status,
        OperationStatus::Succeeded
    );
    assert!(client.submissions.lock().unwrap().is_empty());
    let expiring_requests: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_operation_requests \
         WHERE operation_id IN ($1,$2) AND expires_at IS NOT NULL",
    )
    .bind(target_id.0)
    .bind(withdrawal_id.0)
    .fetch_one(db.store.pool())
    .await
    .unwrap();
    assert_eq!(expiring_requests, 2);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn dropped_acceptance_response_is_recovered_by_identity_with_raw_body_unchanged() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let operation_id = OperationId::new();
    let mut request = llm_request("llm");
    let OperationRequest::Llm {
        request: LlmRequest::Fresh { messages, .. },
        ..
    } = &mut request
    else {
        unreachable!()
    };
    messages.push(Message::Assistant {
        base: agent_contracts::MessageBase::default(),
        provider: agent_contracts::Provider::Openai,
        content: vec![raw(r#"{"native":"\ud800"}"#)],
    });
    stage(
        &db.store,
        session_id,
        "dropped-response",
        vec![operation(operation_id, request)],
    )
    .await;

    let job_id = Uuid::new_v4();
    let submissions = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured = submissions.clone();
    let app = Router::new().route(
        "/v1/jobs",
        post(move |body: Bytes| {
            let captured = captured.clone();
            async move {
                captured
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(body.to_vec()).unwrap());
                drop_http_response()
            }
        })
        .get(move || async move {
            axum::Json(json!({"data":[{
                "id":job_id,
                "idempotencyKey":operation_id.to_string(),
                "status":"running"
            }],"nextCursor":null}))
        }),
    );
    let (address, server) = serve(app).await;
    let dispatcher = http_dispatcher(&db.store, "llm", GatewayKind::Llm, address, 1000);
    dispatcher.process_one().await.unwrap();
    assert_eq!(
        db.store.operation(operation_id).await.unwrap().status,
        OperationStatus::Submitting
    );
    tokio::time::sleep(Duration::from_millis(3)).await;
    dispatcher.process_one().await.unwrap();
    let stored = db.store.operation(operation_id).await.unwrap();
    assert_eq!(stored.status, OperationStatus::Accepted);
    assert_eq!(stored.gateway_job_id, Some(job_id));
    {
        let submissions = submissions.lock().unwrap();
        assert_eq!(submissions.len(), 1);
        assert!(submissions[0].contains(r#"\ud800"#));
    }
    server.abort();
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn lost_lease_fences_acceptance_and_restart_recovers_the_same_gateway_job() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let operation_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "lost-lease",
        vec![operation(operation_id, llm_request("llm"))],
    )
    .await;
    let job_id = Uuid::new_v4();
    let posts = Arc::new(AtomicUsize::new(0));
    let post_count = posts.clone();
    let store = db.store.clone();
    let app = Router::new().route(
        "/v1/jobs",
        post(move || {
            let store = store.clone();
            let post_count = post_count.clone();
            async move {
                post_count.fetch_add(1, Ordering::SeqCst);
                sqlx::query("UPDATE session_operations SET lease_token=$2 WHERE id=$1")
                    .bind(operation_id.0)
                    .bind(Uuid::new_v4())
                    .execute(store.pool())
                    .await
                    .unwrap();
                axum::Json(json!({"id":job_id,"status":"queued"}))
            }
        })
        .get(move || async move {
            axum::Json(json!({"data":[{
                "id":job_id,
                "idempotencyKey":operation_id.to_string(),
                "status":"queued"
            }],"nextCursor":null}))
        }),
    );
    let (address, server) = serve(app).await;
    http_dispatcher(&db.store, "llm", GatewayKind::Llm, address, 1000)
        .process_one()
        .await
        .unwrap();
    let fenced = db.store.operation(operation_id).await.unwrap();
    assert_eq!(fenced.status, OperationStatus::Submitting);
    assert_eq!(fenced.gateway_job_id, None);

    sqlx::query(
        "UPDATE session_operations SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(operation_id.0)
    .execute(db.store.pool())
    .await
    .unwrap();
    http_dispatcher(&db.store, "llm", GatewayKind::Llm, address, 1000)
        .process_one()
        .await
        .unwrap();
    let recovered = db.store.operation(operation_id).await.unwrap();
    assert_eq!(recovered.status, OperationStatus::Accepted);
    assert_eq!(recovered.gateway_job_id, Some(job_id));
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    server.abort();
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn dispatch_wins_a_withdrawal_race_once_http_submission_has_started() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let target_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "dispatch-first",
        vec![operation(target_id, llm_request("llm"))],
    )
    .await;
    let job_id = Uuid::new_v4();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/v1/jobs",
        post({
            let started = started.clone();
            let release = release.clone();
            move || {
                let started = started.clone();
                let release = release.clone();
                async move {
                    started.notify_one();
                    release.notified().await;
                    axum::Json(json!({"id":job_id,"status":"queued"}))
                }
            }
        }),
    );
    let (address, server) = serve(app).await;
    let dispatcher = http_dispatcher(&db.store, "llm", GatewayKind::Llm, address, 1000);
    let dispatch_task = tokio::spawn({
        let dispatcher = dispatcher.clone();
        async move { dispatcher.process_one().await.unwrap() }
    });
    started.notified().await;
    let withdrawal_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "withdraw-second",
        vec![operation(
            withdrawal_id,
            OperationRequest::Withdraw { target: target_id },
        )],
    )
    .await;
    dispatcher.process_one().await.unwrap();
    release.notify_one();
    dispatch_task.await.unwrap();
    assert_eq!(
        db.store.operation(target_id).await.unwrap().status,
        OperationStatus::Accepted
    );
    let withdrawal = db.store.operation(withdrawal_id).await.unwrap();
    assert_eq!(withdrawal.status, OperationStatus::Succeeded);
    let result: serde_json::Value = serde_json::from_str(withdrawal.result.unwrap().get()).unwrap();
    assert_eq!(result["value"]["withdrawn"], false);
    server.abort();
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn cancellation_timeout_retries_after_the_target_becomes_terminal() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let target_id = OperationId::new();
    let job_id = Uuid::new_v4();
    stage(
        &db.store,
        session_id,
        "cancel-target",
        vec![operation(target_id, llm_request("llm"))],
    )
    .await;
    accept_staged_operation(&db.store, target_id, job_id).await;
    let cancellation_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "cancel-timeout",
        vec![operation(
            cancellation_id,
            OperationRequest::LlmCancellation { target: target_id },
        )],
    )
    .await;
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/v1/jobs/{id}/cancel",
        post({
            let calls = calls.clone();
            move || {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call == 0 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    axum::Json(json!({
                        "id":job_id,
                        "status":"succeeded",
                        "cancelRequestedAt":null
                    }))
                }
            }
        }),
    );
    let (address, server) = serve(app).await;
    let dispatcher = http_dispatcher(&db.store, "llm", GatewayKind::Llm, address, 10);
    dispatcher.process_one().await.unwrap();
    assert_eq!(
        db.store.operation(cancellation_id).await.unwrap().status,
        OperationStatus::Submitting
    );
    complete_accepted_operation(&db.store, target_id).await;
    tokio::time::sleep(Duration::from_millis(3)).await;
    dispatcher.process_one().await.unwrap();
    let cancellation = db.store.operation(cancellation_id).await.unwrap();
    assert_eq!(cancellation.status, OperationStatus::Succeeded);
    let result: serde_json::Value =
        serde_json::from_str(cancellation.result.unwrap().get()).unwrap();
    assert_eq!(result["value"]["accepted"], false);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.abort();
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn malformed_cancellation_acknowledgement_stays_unresolved_in_the_dispatcher() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let target_id = OperationId::new();
    let job_id = Uuid::new_v4();
    stage(
        &db.store,
        session_id,
        "malformed-cancel-target",
        vec![operation(target_id, llm_request("llm"))],
    )
    .await;
    accept_staged_operation(&db.store, target_id, job_id).await;
    let cancellation_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "malformed-cancel",
        vec![operation(
            cancellation_id,
            OperationRequest::LlmCancellation { target: target_id },
        )],
    )
    .await;
    let (address, server) = serve(Router::new().route(
        "/v1/jobs/{id}/cancel",
        post(|| async { axum::Json(json!({})) }),
    ))
    .await;
    http_dispatcher(&db.store, "llm", GatewayKind::Llm, address, 1000)
        .process_one()
        .await
        .unwrap();
    assert_eq!(
        db.store.operation(cancellation_id).await.unwrap().status,
        OperationStatus::Submitting
    );
    let completion_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
    )
    .bind(cancellation_id.0)
    .fetch_one(db.store.pool())
    .await
    .unwrap();
    assert_eq!(completion_events, 0);
    let attempt: (Option<i32>, serde_json::Value) = sqlx::query_as(
        "SELECT http_status,error FROM session_operation_attempts WHERE operation_id=$1",
    )
    .bind(cancellation_id.0)
    .fetch_one(db.store.pool())
    .await
    .unwrap();
    assert_eq!(attempt.0, Some(200));
    assert_eq!(attempt.1["code"], "invalid_cancellation_response");
    server.abort();
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn retry_http_statuses_and_idempotency_conflicts_keep_their_meaning() {
    for (status, expected_operation_status, code) in [
        (
            StatusCode::TOO_MANY_REQUESTS,
            OperationStatus::Pending,
            "busy",
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            OperationStatus::Submitting,
            "unavailable",
        ),
        (
            StatusCode::CONFLICT,
            OperationStatus::Failed,
            "idempotency_conflict",
        ),
    ] {
        let db = Database::new().await;
        let session_id = session(&db.store).await;
        let operation_id = OperationId::new();
        stage(
            &db.store,
            session_id,
            code,
            vec![operation(operation_id, llm_request("llm"))],
        )
        .await;
        let app = Router::new().route(
            "/v1/jobs",
            post(move || async move {
                (
                    status,
                    axum::Json(json!({"error":{"code":code,"message":"gateway reply"}})),
                )
            }),
        );
        let (address, server) = serve(app).await;
        http_dispatcher(&db.store, "llm", GatewayKind::Llm, address, 1000)
            .process_one()
            .await
            .unwrap();
        let operation = db.store.operation(operation_id).await.unwrap();
        assert_eq!(operation.status, expected_operation_status);
        let attempt: (Option<i32>, Option<serde_json::Value>) = sqlx::query_as(
            "SELECT http_status,error FROM session_operation_attempts WHERE operation_id=$1",
        )
        .bind(operation_id.0)
        .fetch_one(db.store.pool())
        .await
        .unwrap();
        assert_eq!(attempt.0, Some(i32::from(status.as_u16())));
        if status == StatusCode::CONFLICT {
            assert_eq!(operation.error.unwrap()["code"], "idempotency_conflict");
        } else {
            assert_eq!(attempt.1.unwrap()["code"], code);
        }
        server.abort();
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn idempotency_conflict_never_rekeys_or_adopts_after_uncertain_submission() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let operation_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "sticky-conflict",
        vec![operation(operation_id, llm_request("llm"))],
    )
    .await;
    let bodies = Arc::new(Mutex::new(Vec::<String>::new()));
    let post_calls = Arc::new(AtomicUsize::new(0));
    let lookups = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/v1/jobs",
        post({
            let bodies = bodies.clone();
            let post_calls = post_calls.clone();
            move |body: Bytes| {
                bodies
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(body.to_vec()).unwrap());
                let call = post_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if call == 0 {
                        drop_http_response()
                    } else {
                        Response::builder()
                            .status(StatusCode::CONFLICT)
                            .header("content-type", "application/json")
                            .body(axum::body::Body::from(
                                json!({"error":{
                                    "code":"idempotency_conflict",
                                    "message":"different input"
                                }})
                                .to_string(),
                            ))
                            .unwrap()
                    }
                }
            }
        })
        .get({
            let lookups = lookups.clone();
            move || {
                lookups.fetch_add(1, Ordering::SeqCst);
                async { axum::Json(json!({"data":[],"nextCursor":null})) }
            }
        }),
    );
    let (address, server) = serve(app).await;
    let dispatcher = http_dispatcher(&db.store, "llm", GatewayKind::Llm, address, 1000);
    dispatcher.process_one().await.unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    dispatcher.process_one().await.unwrap();
    let operation = db.store.operation(operation_id).await.unwrap();
    assert_eq!(operation.status, OperationStatus::Submitting);
    assert_eq!(operation.gateway_job_id, None);
    assert_eq!(lookups.load(Ordering::SeqCst), 1);
    {
        let bodies = bodies.lock().unwrap();
        assert_eq!(bodies.len(), 2);
        assert_eq!(bodies[0], bodies[1]);
        assert!(bodies[0].contains(&operation_id.to_string()));
    }
    let latest_attempt: (Option<i32>, Option<serde_json::Value>) = sqlx::query_as(
        "SELECT http_status,error FROM session_operation_attempts \
         WHERE operation_id=$1 ORDER BY attempt_number DESC LIMIT 1",
    )
    .bind(operation_id.0)
    .fetch_one(db.store.pool())
    .await
    .unwrap();
    assert_eq!(latest_attempt.0, Some(409));
    assert_eq!(latest_attempt.1.unwrap()["code"], "idempotency_conflict");
    server.abort();
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn execution_start_is_submitted_once_without_automatic_observation() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let operation_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "execution-http",
        vec![operation(
            operation_id,
            OperationRequest::Execution {
                connection: GatewayConnectionId("execution".into()),
                machine_id: Uuid::new_v4(),
                expected_generation_id: None,
                request: agent_contracts::execution::Payload::Single(
                    agent_contracts::execution::Operation::Info,
                ),
            },
        )],
    )
    .await;
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/v1/jobs",
        post({
            let calls = calls.clone();
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { axum::Json(json!({"id":Uuid::new_v4(),"status":"queued"})) }
            }
        }),
    );
    let (address, server) = serve(app).await;
    let dispatcher = http_dispatcher(
        &db.store,
        "execution",
        GatewayKind::Execution,
        address,
        1000,
    );
    assert!(dispatcher.process_one().await.unwrap());
    assert!(!dispatcher.process_one().await.unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        db.store.operation(operation_id).await.unwrap().status,
        OperationStatus::Accepted
    );
    server.abort();
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn forced_dispatch_shutdown_leaves_the_submission_recoverable() {
    let db = Database::new().await;
    let session_id = session(&db.store).await;
    let operation_id = OperationId::new();
    stage(
        &db.store,
        session_id,
        "shutdown",
        vec![operation(operation_id, llm_request("llm"))],
    )
    .await;
    let started = Arc::new(tokio::sync::Notify::new());
    let mut gateways = GatewayRegistry::new();
    gateways
        .register(
            GatewayConnectionId("llm".into()),
            Arc::new(BlockingClient {
                started: started.clone(),
            }),
        )
        .unwrap();
    let dispatcher = OperationDispatcher::new(
        db.store.clone(),
        Arc::new(gateways),
        DispatcherSettings {
            poll_interval: Duration::from_millis(1),
            lease_duration: Duration::from_millis(100),
            lease_renewal_interval: Duration::from_millis(20),
            shutdown_grace: Duration::from_millis(30),
            ..DispatcherSettings::default()
        },
    )
    .unwrap();
    let stop = CancellationToken::new();
    let worker = tokio::spawn(dispatcher.run(stop.clone()));
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    stop.cancel();
    worker.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    let recovered = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.operation.id, operation_id);
    assert!(recovered.recovering_uncertain);
    db.close().await;
}
