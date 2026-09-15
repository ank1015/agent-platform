use agent_contracts::{
    EventKind, GatewayConnectionId, HandlerError, HandlerOutcome, Harness, HarnessContext,
    HarnessDescription, HarnessId, HarnessVersion, InitializationError, OperationId, OperationKind,
    OperationOutcome, ProjectId, SessionEvent, SessionId,
};
use agent_gateways::{
    ExecutionJobMetadata, ExecutionJobStatus, GatewayClient, GatewayConnectionConfig, GatewayError,
    GatewayJob, GatewayJobOutcome, GatewayJobStatus, GatewayKind, GatewayRegistry, LlmJobStatus,
};
use agent_runtime::{
    CompletionRuntime, CompletionSettings, HarnessRegistry, SchedulerSettings, SessionScheduler,
};
use agent_store::{
    EventType, HandlerClaim, NewExternalEvent, NewOperation, NewSession, OperationPhase,
    OperationStatus, OutcomeCommit, PoolConfig, Store,
};
use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use chrono::Utc;
use serde_json::{json, value::RawValue};
use sqlx::PgPool;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use url::Url;
use uuid::Uuid;

struct CaptureHarness;

#[async_trait]
impl Harness for CaptureHarness {
    type Config = serde_json::Value;
    type State = serde_json::Value;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("fixture".into()),
            version: HarnessVersion("1".into()),
            name: "Completion fixture".into(),
            description: "Reads completed execution operations".into(),
        }
    }

    fn initialize(
        &self,
        _config: &serde_json::Value,
    ) -> Result<serde_json::Value, InitializationError> {
        Ok(json!({ "saw_partial_batch_failure": false }))
    }

    async fn handle(
        &self,
        _config: &serde_json::Value,
        mut state: serde_json::Value,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<serde_json::Value>, HandlerError> {
        if let EventKind::OperationCompleted { operation_id, .. } = event.kind {
            let operation = context
                .operation(operation_id)
                .await
                .map_err(|error| HandlerError(error.to_string()))?
                .ok_or_else(|| HandlerError("completed operation is missing".into()))?;
            let Some(OperationOutcome::Failed(failure)) = operation.outcome else {
                return Err(HandlerError("expected a failed operation".into()));
            };
            let response = failure
                .execution_response
                .ok_or_else(|| HandlerError("execution response is missing".into()))?;
            let value =
                serde_json::to_value(response).map_err(|error| HandlerError(error.to_string()))?;
            state["saw_partial_batch_failure"] = json!(
                value["result"]["succeeded"] == json!(false)
                    && value["result"]["results"].as_array().is_some_and(|items| {
                        items.len() == 2
                            && items[0]["status"] == json!("ok")
                            && items[1]["status"] == json!("error")
                    })
            );
        }
        Ok(agent_contracts::OutcomeBuilder::new(state).finish())
    }
}

struct Database {
    admin_url: String,
    name: String,
    store: Store,
}

impl Database {
    async fn new() -> Self {
        let admin_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL is required");
        let admin = PgPool::connect(&admin_url).await.unwrap();
        let name = format!("agent_completion_test_{}", Uuid::new_v4().simple());
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

struct ResultClient {
    kind: GatewayKind,
    outcomes: Mutex<VecDeque<GatewayJobOutcome>>,
}

struct SlowResultClient {
    delay: Duration,
}

struct MismatchedExecutionResultClient;

struct PausedResultClient {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl GatewayClient for ResultClient {
    fn kind(&self) -> GatewayKind {
        self.kind
    }

    async fn submit(
        &self,
        _request: &RawValue,
    ) -> Result<agent_gateways::JobAcknowledgement, GatewayError> {
        unreachable!("completion tests do not dispatch")
    }

    async fn get_job(&self, job_id: Uuid) -> Result<GatewayJob, GatewayError> {
        let outcome = self.outcomes.lock().unwrap().pop_front().unwrap();
        let status = match (&self.kind, &outcome) {
            (GatewayKind::Llm, GatewayJobOutcome::Pending) => {
                GatewayJobStatus::Llm(LlmJobStatus::Running)
            }
            (GatewayKind::Llm, GatewayJobOutcome::Succeeded(_)) => {
                GatewayJobStatus::Llm(LlmJobStatus::Succeeded)
            }
            (GatewayKind::Llm, GatewayJobOutcome::Failed { .. }) => {
                GatewayJobStatus::Llm(LlmJobStatus::Failed)
            }
            (GatewayKind::Llm, GatewayJobOutcome::Cancelled) => {
                GatewayJobStatus::Llm(LlmJobStatus::Cancelled)
            }
            (GatewayKind::Execution, GatewayJobOutcome::Pending) => {
                GatewayJobStatus::Execution(ExecutionJobStatus::WaitingResponse)
            }
            (GatewayKind::Execution, GatewayJobOutcome::Succeeded(_)) => {
                GatewayJobStatus::Execution(ExecutionJobStatus::Succeeded)
            }
            (GatewayKind::Execution, GatewayJobOutcome::Failed { .. }) => {
                GatewayJobStatus::Execution(ExecutionJobStatus::Failed)
            }
            (GatewayKind::Execution, GatewayJobOutcome::Unknown(_)) => {
                GatewayJobStatus::Execution(ExecutionJobStatus::Unknown)
            }
            _ => unreachable!("gateway kind and outcome do not match"),
        };
        Ok(GatewayJob {
            id: job_id,
            status,
            outcome,
            execution: (self.kind == GatewayKind::Execution).then_some(ExecutionJobMetadata {
                machine_id: Uuid::nil(),
                runtime_generation_id: None,
            }),
        })
    }
}

#[async_trait]
impl GatewayClient for SlowResultClient {
    fn kind(&self) -> GatewayKind {
        GatewayKind::Llm
    }

    async fn submit(
        &self,
        _request: &RawValue,
    ) -> Result<agent_gateways::JobAcknowledgement, GatewayError> {
        unreachable!("completion tests do not dispatch")
    }

    async fn get_job(&self, job_id: Uuid) -> Result<GatewayJob, GatewayError> {
        tokio::time::sleep(self.delay).await;
        Ok(GatewayJob {
            id: job_id,
            status: GatewayJobStatus::Llm(LlmJobStatus::Cancelled),
            outcome: GatewayJobOutcome::Cancelled,
            execution: None,
        })
    }
}

#[async_trait]
impl GatewayClient for MismatchedExecutionResultClient {
    fn kind(&self) -> GatewayKind {
        GatewayKind::Execution
    }

    async fn submit(
        &self,
        _request: &RawValue,
    ) -> Result<agent_gateways::JobAcknowledgement, GatewayError> {
        unreachable!("completion tests do not dispatch")
    }

    async fn get_job(&self, job_id: Uuid) -> Result<GatewayJob, GatewayError> {
        Ok(GatewayJob {
            id: job_id,
            status: GatewayJobStatus::Execution(ExecutionJobStatus::Unknown),
            outcome: GatewayJobOutcome::Unknown(json!({"code":"unknown","message":"lost"})),
            execution: Some(ExecutionJobMetadata {
                machine_id: Uuid::new_v4(),
                runtime_generation_id: None,
            }),
        })
    }
}

#[async_trait]
impl GatewayClient for PausedResultClient {
    fn kind(&self) -> GatewayKind {
        GatewayKind::Llm
    }

    async fn submit(
        &self,
        _request: &RawValue,
    ) -> Result<agent_gateways::JobAcknowledgement, GatewayError> {
        unreachable!("completion tests do not dispatch")
    }

    async fn get_job(&self, job_id: Uuid) -> Result<GatewayJob, GatewayError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(GatewayJob {
            id: job_id,
            status: GatewayJobStatus::Llm(LlmJobStatus::Cancelled),
            outcome: GatewayJobOutcome::Cancelled,
            execution: None,
        })
    }
}

fn raw(value: impl Into<String>) -> Box<RawValue> {
    RawValue::from_string(value.into()).unwrap()
}

async fn stage_operation(
    store: &Store,
    operation_id: OperationId,
    kind: OperationKind,
    connection: &str,
) -> SessionId {
    let session_id = SessionId::new();
    let request = if kind == OperationKind::Execution {
        serde_json::value::to_raw_value(&agent_contracts::OperationRequest::Execution {
            connection: GatewayConnectionId(connection.into()),
            machine_id: Uuid::nil(),
            expected_generation_id: None,
            request: agent_contracts::execution::Payload::Single(
                agent_contracts::execution::Operation::Info,
            ),
        })
        .unwrap()
    } else {
        raw("{}")
    };
    store
        .create_session(NewSession {
            id: session_id,
            project_id: ProjectId("completion".into()),
            harness_id: HarnessId("fixture".into()),
            harness_version: HarnessVersion("1".into()),
            configuration: raw("{}"),
            state: raw("{}"),
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload: raw(
                r#"{"message":{"role":"user","content":[{"type":"text","text":"work"}]}}"#,
            ),
            idempotency_key: Some("input".into()),
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
                operations: vec![NewOperation {
                    id: operation_id,
                    kind,
                    gateway_connection_id: Some(GatewayConnectionId(connection.into())),
                    target_operation_id: None,
                    previous_operation_id: None,
                    request,
                }],
                waits: vec![],
            },
        )
        .await
        .unwrap();
    session_id
}

fn runtime(store: &Store, registry: GatewayRegistry) -> CompletionRuntime {
    CompletionRuntime::new(
        store.clone(),
        Arc::new(registry),
        CompletionSettings {
            poll_interval: Duration::from_millis(1),
            lease_duration: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(100),
            retry_delay: Duration::from_millis(1),
            fallback_interval: Duration::from_millis(1),
            unmatched_retry_delay: Duration::from_millis(1),
            unmatched_retention: Duration::from_secs(60),
            request_retention: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(1),
            ..CompletionSettings::default()
        },
    )
    .unwrap()
}

async fn accept_operation(
    store: &Store,
    operation_id: OperationId,
    kind: OperationKind,
    connection: &str,
) -> Uuid {
    stage_operation(store, operation_id, kind, connection).await;
    let job_id = Uuid::new_v4();
    let claim = store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    store
        .record_operation_accepted(&claim, job_id, Utc::now(), Some(202))
        .await
        .unwrap();
    job_id
}

fn gateway_registry_with_outcomes(
    connection: &str,
    kind: GatewayKind,
    outcomes: Vec<GatewayJobOutcome>,
) -> GatewayRegistry {
    let mut registry = GatewayRegistry::new();
    registry
        .register(
            GatewayConnectionId(connection.into()),
            Arc::new(ResultClient {
                kind,
                outcomes: Mutex::new(outcomes.into()),
            }),
        )
        .unwrap();
    registry
}

async fn retrieval_error_registry() -> (GatewayRegistry, tokio::task::JoinHandle<()>) {
    async fn job(Path(_job_id): Path<Uuid>, headers: HeaderMap) -> impl IntoResponse {
        match headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        {
            Some("Bearer unauthorized") => (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":{"code":"unauthorized","message":"denied"}})),
            )
                .into_response(),
            Some("Bearer missing") => (
                StatusCode::NOT_FOUND,
                Json(json!({"error":{"code":"not_found","message":"missing"}})),
            )
                .into_response(),
            Some("Bearer timeout") => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                (StatusCode::OK, Json(json!({}))).into_response()
            }
            _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url: Url = format!("http://{}/", listener.local_addr().unwrap())
        .parse()
        .unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/v1/jobs/{job_id}", get(job)))
            .await
            .unwrap();
    });
    let registry = GatewayRegistry::from_configs(vec![
        GatewayConnectionConfig {
            id: GatewayConnectionId("unauthorized".into()),
            kind: GatewayKind::Llm,
            base_url: base_url.clone(),
            bearer_token: "unauthorized".into(),
            timeout_ms: 100,
        },
        GatewayConnectionConfig {
            id: GatewayConnectionId("missing".into()),
            kind: GatewayKind::Llm,
            base_url: base_url.clone(),
            bearer_token: "missing".into(),
            timeout_ms: 100,
        },
        GatewayConnectionConfig {
            id: GatewayConnectionId("timeout".into()),
            kind: GatewayKind::Llm,
            base_url,
            bearer_token: "timeout".into(),
            timeout_ms: 20,
        },
    ])
    .unwrap();
    (registry, server)
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn early_callbacks_and_fallback_checks_deliver_typed_terminal_results_once() {
    let database = Database::new().await;
    let llm_operation = OperationId::new();
    let llm_job = Uuid::new_v4();
    stage_operation(&database.store, llm_operation, OperationKind::Llm, "llm").await;

    database
        .store
        .record_callback_receipt(
            GatewayConnectionId("llm".into()),
            Uuid::new_v4(),
            llm_job,
            raw(r#"{"type":"job.succeeded"}"#).as_ref(),
        )
        .await
        .unwrap();

    let mut registry = GatewayRegistry::new();
    registry
        .register(
            GatewayConnectionId("llm".into()),
            Arc::new(ResultClient {
                kind: GatewayKind::Llm,
                outcomes: Mutex::new(
                    vec![GatewayJobOutcome::Succeeded(raw(
                        r#"{"id":"response-1","modelId":"model","message":{"role":"assistant","provider":"openai","content":[]},"stopReason":"stop","durationMs":2,"timestamp":1}"#,
                    ))]
                    .into(),
                ),
            }),
        )
        .unwrap();
    let execution_response = raw(format!(
        r#"{{"protocol_version":1,"request_id":"command","generation_id":"{}","status":"error","error":{{"code":"invalid_argument","message":"bad command"}}}}"#,
        Uuid::new_v4()
    ));
    registry
        .register(
            GatewayConnectionId("execution".into()),
            Arc::new(ResultClient {
                kind: GatewayKind::Execution,
                outcomes: Mutex::new(
                    vec![
                        GatewayJobOutcome::Pending,
                        GatewayJobOutcome::Failed {
                            response: Some(execution_response),
                            error: None,
                        },
                    ]
                    .into(),
                ),
            }),
        )
        .unwrap();
    let runtime = runtime(&database.store, registry);

    assert!(runtime.process_one_receipt().await.unwrap());
    let llm_submission = database
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    database
        .store
        .record_operation_accepted(
            &llm_submission,
            llm_job,
            Utc::now() + chrono::Duration::hours(1),
            Some(202),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(runtime.process_one_receipt().await.unwrap());
    assert!(runtime.process_one_result().await.unwrap());
    let completed_llm = database.store.operation(llm_operation).await.unwrap();
    assert_eq!(completed_llm.status, OperationStatus::Succeeded);
    assert!(completed_llm.result.unwrap().get().contains("response-1"));

    let execution_operation = OperationId::new();
    stage_operation(
        &database.store,
        execution_operation,
        OperationKind::Execution,
        "execution",
    )
    .await;
    let execution_submission = database
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    database
        .store
        .record_operation_accepted(&execution_submission, Uuid::new_v4(), Utc::now(), Some(202))
        .await
        .unwrap();
    assert!(runtime.process_one_result().await.unwrap());
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(runtime.process_one_result().await.unwrap());
    let completed_execution = database.store.operation(execution_operation).await.unwrap();
    assert_eq!(completed_execution.status, OperationStatus::Failed);
    assert!(completed_execution.result.is_some());
    assert_eq!(
        completed_execution.error.unwrap()["source"],
        json!("protocol")
    );

    for operation in [llm_operation, execution_operation] {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
        )
        .bind(operation.0)
        .fetch_one(database.store.pool())
        .await
        .unwrap();
        assert_eq!(count, 1);
    }
    let receipt_status: String =
        sqlx::query_scalar("SELECT status FROM gateway_callback_receipts LIMIT 1")
            .fetch_one(database.store.pool())
            .await
            .unwrap();
    assert_eq!(receipt_status, "processed");
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn result_renewal_does_not_block_completion_persistence() {
    let database = Database::new().await;
    let operation_id = OperationId::new();
    accept_operation(&database.store, operation_id, OperationKind::Llm, "slow").await;
    sqlx::raw_sql(
        "CREATE FUNCTION slow_completion() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN IF NEW.status='cancelled' THEN PERFORM pg_sleep(0.15); END IF; RETURN NEW; END $$; \
         CREATE TRIGGER slow_completion BEFORE UPDATE ON session_operations \
         FOR EACH ROW EXECUTE FUNCTION slow_completion();",
    )
    .execute(database.store.pool())
    .await
    .unwrap();
    let registry = gateway_registry_with_outcomes(
        "slow",
        GatewayKind::Llm,
        vec![GatewayJobOutcome::Cancelled],
    );
    let runtime = CompletionRuntime::new(
        database.store.clone(),
        Arc::new(registry),
        CompletionSettings {
            lease_duration: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(20),
            ..CompletionSettings::default()
        },
    )
    .unwrap();

    assert!(runtime.process_one_result().await.unwrap());
    assert_eq!(
        database.store.operation(operation_id).await.unwrap().status,
        OperationStatus::Cancelled
    );
    let completion_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
    )
    .bind(operation_id.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(completion_events, 1);
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn result_work_stops_after_lease_ownership_is_lost() {
    let database = Database::new().await;
    let operation_id = OperationId::new();
    accept_operation(
        &database.store,
        operation_id,
        OperationKind::Llm,
        "ownership",
    )
    .await;
    let mut registry = GatewayRegistry::new();
    registry
        .register(
            GatewayConnectionId("ownership".into()),
            Arc::new(SlowResultClient {
                delay: Duration::from_millis(250),
            }),
        )
        .unwrap();
    let runtime = CompletionRuntime::new(
        database.store.clone(),
        Arc::new(registry),
        CompletionSettings {
            lease_duration: Duration::from_secs(1),
            lease_renewal_interval: Duration::from_millis(20),
            ..CompletionSettings::default()
        },
    )
    .unwrap();
    let worker = tokio::spawn(async move { runtime.process_one_result().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    sqlx::query("UPDATE session_operations SET lease_token=$2 WHERE id=$1")
        .bind(operation_id.0)
        .bind(Uuid::new_v4())
        .execute(database.store.pool())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(150), worker)
        .await
        .expect("lease loss should stop result work")
        .unwrap()
        .unwrap();
    assert_eq!(
        database.store.operation(operation_id).await.unwrap().status,
        OperationStatus::Accepted
    );
    let completion_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
    )
    .bind(operation_id.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(completion_events, 0);
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn execution_job_identity_mismatch_remains_unresolved() {
    let database = Database::new().await;
    let operation_id = OperationId::new();
    accept_operation(
        &database.store,
        operation_id,
        OperationKind::Execution,
        "identity",
    )
    .await;
    let mut registry = GatewayRegistry::new();
    registry
        .register(
            GatewayConnectionId("identity".into()),
            Arc::new(MismatchedExecutionResultClient),
        )
        .unwrap();
    let runtime = runtime(&database.store, registry);

    assert!(runtime.process_one_result().await.unwrap());
    let operation = database.store.operation(operation_id).await.unwrap();
    assert_eq!(operation.status, OperationStatus::Accepted);
    assert_eq!(
        operation.error.unwrap()["code"],
        json!("gateway_job_identity_mismatch")
    );
    let completion_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
    )
    .bind(operation_id.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(completion_events, 0);
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn transient_scan_failure_recovers_and_processes_results() {
    let database = Database::new().await;
    let operation_id = OperationId::new();
    accept_operation(
        &database.store,
        operation_id,
        OperationKind::Llm,
        "recovery",
    )
    .await;
    let mut database_url = Url::parse(&database.admin_url).unwrap();
    database_url.set_path(&format!("/{}", database.name));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='20ms'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(database_url.as_str())
        .await
        .unwrap();
    let mut lock = database.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE gateway_callback_receipts IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let runtime = CompletionRuntime::new(
        Store::from_pool(pool.clone()),
        Arc::new(gateway_registry_with_outcomes(
            "recovery",
            GatewayKind::Llm,
            vec![GatewayJobOutcome::Cancelled],
        )),
        CompletionSettings {
            poll_interval: Duration::from_millis(1),
            infrastructure_retry_delay: Duration::from_millis(10),
            shutdown_grace: Duration::from_secs(1),
            ..CompletionSettings::default()
        },
    )
    .unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker = tokio::spawn(runtime.run(shutdown.clone()));

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!worker.is_finished());
    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if database.store.operation(operation_id).await.unwrap().status
                == OperationStatus::Cancelled
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker.await.unwrap().unwrap();
    pool.close().await;
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn shutdown_interrupts_a_blocked_scan() {
    let database = Database::new().await;
    let mut lock = database.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE gateway_callback_receipts IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let runtime = CompletionRuntime::new(
        database.store.clone(),
        Arc::new(GatewayRegistry::new()),
        CompletionSettings {
            infrastructure_retry_delay: Duration::from_secs(10),
            ..CompletionSettings::default()
        },
    )
    .unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker = tokio::spawn(runtime.run(shutdown.clone()));

    tokio::time::sleep(Duration::from_millis(20)).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_millis(250), worker)
        .await
        .expect("shutdown should interrupt the blocked scan")
        .unwrap()
        .unwrap();
    lock.rollback().await.unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn shutdown_interrupts_scan_failure_backoff() {
    let database = Database::new().await;
    let mut database_url = Url::parse(&database.admin_url).unwrap();
    database_url.set_path(&format!("/{}", database.name));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='20ms'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(database_url.as_str())
        .await
        .unwrap();
    let mut lock = database.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE gateway_callback_receipts IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let runtime = CompletionRuntime::new(
        Store::from_pool(pool.clone()),
        Arc::new(GatewayRegistry::new()),
        CompletionSettings {
            infrastructure_retry_delay: Duration::from_secs(10),
            ..CompletionSettings::default()
        },
    )
    .unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker = tokio::spawn(runtime.run(shutdown.clone()));

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(!worker.is_finished());
    shutdown.cancel();
    tokio::time::timeout(Duration::from_millis(250), worker)
        .await
        .expect("shutdown should interrupt infrastructure backoff")
        .unwrap()
        .unwrap();
    lock.rollback().await.unwrap();
    pool.close().await;
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn unmatched_receipts_do_not_starve_result_retrieval_at_capacity_one() {
    let database = Database::new().await;
    let operation_id = OperationId::new();
    accept_operation(&database.store, operation_id, OperationKind::Llm, "fair").await;
    database
        .store
        .record_callback_receipt(
            GatewayConnectionId("fair".into()),
            Uuid::new_v4(),
            Uuid::new_v4(),
            raw("{}").as_ref(),
        )
        .await
        .unwrap();
    let runtime = CompletionRuntime::new(
        database.store.clone(),
        Arc::new(gateway_registry_with_outcomes(
            "fair",
            GatewayKind::Llm,
            vec![GatewayJobOutcome::Cancelled],
        )),
        CompletionSettings {
            max_concurrent_results: 1,
            poll_interval: Duration::from_millis(1),
            unmatched_retry_delay: Duration::from_micros(1),
            shutdown_grace: Duration::from_secs(1),
            ..CompletionSettings::default()
        },
    )
    .unwrap();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let worker = tokio::spawn(runtime.run(shutdown.clone()));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if database.store.operation(operation_id).await.unwrap().status
                == OperationStatus::Cancelled
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    worker.await.unwrap().unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn unmatched_retention_blocks_only_receipts_without_an_operation_mapping() {
    let database = Database::new().await;
    let matching_operation = OperationId::new();
    let matching_job = accept_operation(
        &database.store,
        matching_operation,
        OperationKind::Llm,
        "retention",
    )
    .await;
    let unmatched = database
        .store
        .record_callback_receipt(
            GatewayConnectionId("retention".into()),
            Uuid::new_v4(),
            Uuid::new_v4(),
            raw("{}").as_ref(),
        )
        .await
        .unwrap();
    let matched = database
        .store
        .record_callback_receipt(
            GatewayConnectionId("retention".into()),
            Uuid::new_v4(),
            matching_job,
            raw("{}").as_ref(),
        )
        .await
        .unwrap();
    sqlx::query(
        "UPDATE gateway_callback_receipts SET received_at=now()-interval '2 hours' WHERE id IN ($1,$2)",
    )
    .bind(unmatched.id)
    .bind(matched.id)
    .execute(database.store.pool())
    .await
    .unwrap();
    let completion = CompletionRuntime::new(
        database.store.clone(),
        Arc::new(GatewayRegistry::new()),
        CompletionSettings {
            unmatched_retention: Duration::from_secs(60),
            unmatched_retry_delay: Duration::from_millis(1),
            ..CompletionSettings::default()
        },
    )
    .unwrap();

    assert!(completion.process_one_receipt().await.unwrap());
    assert!(completion.process_one_receipt().await.unwrap());
    let unmatched_status: String =
        sqlx::query_scalar("SELECT status FROM gateway_callback_receipts WHERE id=$1")
            .bind(unmatched.id)
            .fetch_one(database.store.pool())
            .await
            .unwrap();
    let matched_status: String =
        sqlx::query_scalar("SELECT status FROM gateway_callback_receipts WHERE id=$1")
            .bind(matched.id)
            .fetch_one(database.store.pool())
            .await
            .unwrap();
    assert_eq!(unmatched_status, "blocked");
    assert_eq!(matched_status, "processed");
    assert_eq!(
        database
            .store
            .operation(matching_operation)
            .await
            .unwrap()
            .status,
        OperationStatus::Accepted
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn retrieval_http_failures_leave_operations_unresolved() {
    let database = Database::new().await;
    let mut operations = Vec::new();
    for connection in ["unauthorized", "missing", "timeout"] {
        let operation = OperationId::new();
        accept_operation(&database.store, operation, OperationKind::Llm, connection).await;
        operations.push(operation);
    }
    let (registry, server) = retrieval_error_registry().await;
    let completion = runtime(&database.store, registry);

    for _ in 0..operations.len() {
        assert!(completion.process_one_result().await.unwrap());
    }
    for operation_id in operations {
        let operation = database.store.operation(operation_id).await.unwrap();
        assert_eq!(operation.status, OperationStatus::Accepted);
        assert!(operation.error.is_some());
        let completion_events: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
        )
        .bind(operation_id.0)
        .fetch_one(database.store.pool())
        .await
        .unwrap();
        assert_eq!(completion_events, 0);
    }
    server.abort();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn callbacks_and_results_after_terminal_unknown_cannot_change_the_outcome() {
    let database = Database::new().await;
    let operation_id = OperationId::new();
    let job_id = accept_operation(
        &database.store,
        operation_id,
        OperationKind::Execution,
        "late",
    )
    .await;
    let registry = gateway_registry_with_outcomes(
        "late",
        GatewayKind::Execution,
        vec![GatewayJobOutcome::Unknown(
            json!({"code":"lost","message":"result was lost"}),
        )],
    );
    let completion = runtime(&database.store, registry);
    assert!(completion.process_one_result().await.unwrap());
    assert_eq!(
        database.store.operation(operation_id).await.unwrap().status,
        OperationStatus::Unknown
    );

    database
        .store
        .record_callback_receipt(
            GatewayConnectionId("late".into()),
            Uuid::new_v4(),
            job_id,
            raw(r#"{"type":"job.succeeded"}"#).as_ref(),
        )
        .await
        .unwrap();
    assert!(completion.process_one_receipt().await.unwrap());
    assert!(!completion.process_one_result().await.unwrap());
    let operation = database.store.operation(operation_id).await.unwrap();
    assert_eq!(operation.status, OperationStatus::Unknown);
    let completion_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
    )
    .bind(operation_id.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(completion_events, 1);
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn callback_wake_during_fallback_retrieval_still_completes_once() {
    let database = Database::new().await;
    let operation_id = OperationId::new();
    let job_id = accept_operation(&database.store, operation_id, OperationKind::Llm, "race").await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut registry = GatewayRegistry::new();
    registry
        .register(
            GatewayConnectionId("race".into()),
            Arc::new(PausedResultClient {
                entered: entered.clone(),
                release: release.clone(),
            }),
        )
        .unwrap();
    let completion = runtime(&database.store, registry);
    let result_worker = {
        let completion = completion.clone();
        tokio::spawn(async move { completion.process_one_result().await })
    };
    entered.notified().await;
    database
        .store
        .record_callback_receipt(
            GatewayConnectionId("race".into()),
            Uuid::new_v4(),
            job_id,
            raw("{}").as_ref(),
        )
        .await
        .unwrap();
    assert!(completion.process_one_receipt().await.unwrap());
    release.notify_one();
    assert!(result_worker.await.unwrap().unwrap());

    assert_eq!(
        database.store.operation(operation_id).await.unwrap().status,
        OperationStatus::Cancelled
    );
    let completion_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
    )
    .bind(operation_id.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(completion_events, 1);
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn partial_execution_batch_failure_is_available_to_the_harness_context() {
    let database = Database::new().await;
    let operation_id = OperationId::new();
    let job_id = accept_operation(
        &database.store,
        operation_id,
        OperationKind::Execution,
        "partial",
    )
    .await;
    let response = raw(format!(
        r#"{{"protocol_version":1,"request_id":"{job_id}","generation_id":"{}","status":"ok","result":{{"succeeded":false,"results":[{{"request_id":"one","status":"ok","result":{{}}}},{{"request_id":"two","status":"error","error":{{"code":"invalid_argument","message":"bad input"}}}}]}}}}"#,
        Uuid::new_v4()
    ));
    let registry = gateway_registry_with_outcomes(
        "partial",
        GatewayKind::Execution,
        vec![GatewayJobOutcome::Failed {
            response: Some(response),
            error: None,
        }],
    );
    let completion = runtime(&database.store, registry);
    assert!(completion.process_one_result().await.unwrap());

    let mut harnesses = HarnessRegistry::new();
    harnesses.register(CaptureHarness).unwrap();
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(harnesses),
        SchedulerSettings::default(),
    )
    .unwrap();
    assert!(
        scheduler
            .process_one(tokio_util::sync::CancellationToken::new())
            .await
            .unwrap()
    );
    let session_id = database
        .store
        .operation(operation_id)
        .await
        .unwrap()
        .session_id;
    let state: serde_json::Value = serde_json::from_str(
        database
            .store
            .session(session_id)
            .await
            .unwrap()
            .state
            .get(),
    )
    .unwrap();
    assert_eq!(state["saw_partial_batch_failure"], json!(true));
    assert_eq!(
        database
            .store
            .operations(session_id, None, 10)
            .await
            .unwrap()
            .len(),
        1,
        "the platform must not stage an execution.observe operation automatically"
    );
    database.close().await;
}
