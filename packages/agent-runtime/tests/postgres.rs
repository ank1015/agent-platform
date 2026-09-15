use agent_contracts::{
    EventKind, FailureSource, HandlerError, HandlerOutcome, Harness, HarnessContext,
    HarnessDescription, HarnessId, HarnessVersion, HistoryQuery, HistorySequence,
    InitializationError, OperationFailure, OutcomeBuilder, ProjectId, SessionEvent, SessionId,
    SessionStatus, StagedWait, ValidationError, WaitId, WaitMode, WaitResumeReason, WaitSpec,
};
use agent_runtime::{
    HarnessRegistry, SchedulerSettings, SessionScheduler, WaitExpirationSettings,
    WaitExpirationWorker,
};
use agent_store::{
    EventStatus, EventType, HandlerClaim, NewExternalEvent, NewSession, NewWait, OperationPhase,
    OperationStatus, OutcomeCommit, PoolConfig, Store, WaitStatus,
};
use async_trait::async_trait;
use chrono::Utc;
use harness_fixture::FixtureHarness;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue};
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

#[derive(Deserialize, JsonSchema, Serialize)]
struct SlowConfig {
    delay_ms: u64,
    #[serde(default)]
    cooperative: bool,
}

#[derive(Deserialize, Serialize)]
struct SlowState {
    handled: u64,
}

struct SlowHarness;

#[async_trait]
impl Harness for SlowHarness {
    type Config = SlowConfig;
    type State = SlowState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("slow".into()),
            version: HarnessVersion("1".into()),
            name: "Slow fixture".into(),
            description: "Lease renewal fixture".into(),
        }
    }

    fn initialize(&self, _config: &SlowConfig) -> Result<SlowState, InitializationError> {
        Ok(SlowState { handled: 0 })
    }

    async fn handle(
        &self,
        config: &SlowConfig,
        state: SlowState,
        _event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<SlowState>, HandlerError> {
        if config.cooperative {
            while !context.stop_requested() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        } else {
            tokio::time::sleep(Duration::from_millis(config.delay_ms)).await;
        }
        let mut outcome = OutcomeBuilder::new(state);
        outcome.state_mut().handled += 1;
        Ok(outcome.finish())
    }
}

#[derive(Deserialize, Serialize)]
struct WaitState {
    wait_id: Option<WaitId>,
    resumed: bool,
}

struct WaitHarness;

#[async_trait]
impl Harness for WaitHarness {
    type Config = SlowConfig;
    type State = WaitState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("wait".into()),
            version: HarnessVersion("1".into()),
            name: "Wait fixture".into(),
            description: "Durable wait fixture".into(),
        }
    }

    fn initialize(&self, _config: &SlowConfig) -> Result<WaitState, InitializationError> {
        Ok(WaitState {
            wait_id: None,
            resumed: false,
        })
    }

    async fn handle(
        &self,
        _config: &SlowConfig,
        state: WaitState,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<WaitState>, HandlerError> {
        let mut outcome = OutcomeBuilder::new(state);
        match event.kind {
            EventKind::UserMessage { message } => {
                outcome.append_message(message);
                let wait_id = outcome
                    .create_wait(WaitSpec {
                        mode: WaitMode::External,
                        payload: json!({"prompt": "reply"}),
                        response_schema: Some(json!({"type": "string"})),
                        expires_at: None,
                    })
                    .map_err(|error| HandlerError(error.to_string()))?;
                outcome.state_mut().wait_id = Some(wait_id);
                outcome.set_status(SessionStatus::Waiting);
            }
            EventKind::WaitResumed { wait_id, reason } => {
                let history = context
                    .history(HistoryQuery {
                        after_sequence: HistorySequence(0),
                        limit: 10,
                    })
                    .await
                    .map_err(|error| HandlerError(error.to_string()))?;
                if history.len() != 1 || context.history_through_sequence() != HistorySequence(1) {
                    return Err(HandlerError("history boundary is incorrect".into()));
                }
                let wait = context
                    .wait(wait_id)
                    .await
                    .map_err(|error| HandlerError(error.to_string()))?
                    .ok_or_else(|| HandlerError("resolved wait is missing".into()))?;
                if reason == WaitResumeReason::Cancelled {
                    if wait.status != agent_contracts::WaitStatus::Cancelled
                        || wait.resolution.is_some()
                    {
                        return Err(HandlerError("cancelled wait is incomplete".into()));
                    }
                    outcome.set_status(SessionStatus::Cancelled);
                } else {
                    if wait.status != agent_contracts::WaitStatus::Resolved
                        || wait.resolution != Some(json!("continue"))
                    {
                        return Err(HandlerError("resolved wait is incomplete".into()));
                    }
                    outcome.set_status(SessionStatus::Idle);
                }
                outcome.state_mut().resumed = true;
            }
            EventKind::CancellationRequested { .. } => {
                if let Some(wait_id) = outcome.state_mut().wait_id {
                    outcome.cancel_wait(wait_id);
                }
                outcome.set_status(SessionStatus::Cancelled);
            }
            _ => {}
        }
        Ok(outcome.finish())
    }
}

struct InvalidOutcomeHarness;

#[async_trait]
impl Harness for InvalidOutcomeHarness {
    type Config = SlowConfig;
    type State = SlowState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("invalid-outcome".into()),
            version: HarnessVersion("1".into()),
            name: "Invalid outcome fixture".into(),
            description: "Outcome validation fixture".into(),
        }
    }

    fn initialize(&self, _config: &SlowConfig) -> Result<SlowState, InitializationError> {
        Ok(SlowState { handled: 0 })
    }

    async fn handle(
        &self,
        _config: &SlowConfig,
        state: SlowState,
        _event: SessionEvent,
        _context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<SlowState>, HandlerError> {
        let id = WaitId::new();
        let spec = WaitSpec {
            mode: WaitMode::External,
            payload: json!({}),
            response_schema: None,
            expires_at: None,
        };
        Ok(HandlerOutcome {
            cancelled_waits: vec![],
            progress: vec![],
            state,
            history: vec![],
            operations: vec![],
            waits: vec![
                StagedWait {
                    id,
                    spec: spec.clone(),
                },
                StagedWait { id, spec },
            ],
            status: None,
        })
    }
}

struct ReadingHarness {
    panic_validation: bool,
}

#[async_trait]
impl Harness for ReadingHarness {
    type Config = SlowConfig;
    type State = SlowState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("reading".into()),
            version: HarnessVersion("1".into()),
            name: "Reading fixture".into(),
            description: "Context failure fixture".into(),
        }
    }

    fn validate_config(&self, _config: &SlowConfig) -> Result<(), ValidationError> {
        if self.panic_validation {
            panic!("validation panic");
        }
        Ok(())
    }

    fn initialize(&self, _config: &SlowConfig) -> Result<SlowState, InitializationError> {
        Ok(SlowState { handled: 0 })
    }

    async fn handle(
        &self,
        _config: &SlowConfig,
        state: SlowState,
        _event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<SlowState>, HandlerError> {
        context
            .history(HistoryQuery {
                after_sequence: HistorySequence(0),
                limit: 10,
            })
            .await
            .map_err(|error| HandlerError(error.to_string()))?;
        Ok(OutcomeBuilder::new(state).finish())
    }
}

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
        let name = format!("agent_runtime_test_{}", Uuid::new_v4().simple());
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

async fn fixture_session(store: &Store, registry: &HarnessRegistry) -> SessionId {
    let id = SessionId::new();
    let configuration = raw(format!(
        r#"{{"connection":"llm-primary","account_id":"{}","model_id":"test-model"}}"#,
        Uuid::new_v4()
    ));
    let state = registry
        .get(&HarnessId("fixture".into()), &HarnessVersion("1".into()))
        .unwrap()
        .initialize(&configuration)
        .unwrap();
    store
        .create_session(NewSession {
            id,
            project_id: ProjectId("project".into()),
            harness_id: HarnessId("fixture".into()),
            harness_version: HarnessVersion("1".into()),
            configuration,
            state,
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    id
}

async fn basic_session(store: &Store, harness: &str, delay_ms: u64) -> SessionId {
    let id = SessionId::new();
    store
        .create_session(NewSession {
            id,
            project_id: ProjectId("project".into()),
            harness_id: HarnessId(harness.into()),
            harness_version: HarnessVersion("1".into()),
            configuration: raw(format!(r#"{{"delay_ms":{delay_ms}}}"#)),
            state: raw(r#"{"handled":0}"#),
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    message(store, id, "message-1", "hello").await;
    id
}

async fn message(store: &Store, session_id: SessionId, key: &str, text: &str) {
    store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload: serde_json::value::to_raw_value(&json!({
                "message": {
                    "role": "user",
                    "content": [{"type": "text", "text": text}]
                }
            }))
            .unwrap(),
            idempotency_key: Some(key.into()),
        })
        .await
        .unwrap();
}

async fn due_wait(store: &Store, seconds_overdue: i64) -> (SessionId, WaitId) {
    let session_id = basic_session(store, "slow", 0).await;
    let claim = store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.session.id, session_id);
    let wait_id = WaitId::new();
    store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
            OutcomeCommit {
                state: raw("{}"),
                status: Some(SessionStatus::Waiting),
                history: vec![],
                operations: vec![],
                cancelled_waits: vec![],
                progress: vec![],
                waits: vec![NewWait {
                    id: wait_id,
                    mode: WaitMode::Expiration,
                    payload: raw("{}"),
                    response_schema: None,
                    expires_at: Some(Utc::now() - chrono::Duration::seconds(seconds_overdue)),
                }],
            },
        )
        .await
        .unwrap();
    (session_id, wait_id)
}

async fn wait_until_claimed(store: &Store, session_id: SessionId) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if store
                .session(session_id)
                .await
                .unwrap()
                .current_event_id
                .is_some()
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn fixture_harness_processes_message_and_operation_completion() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(FixtureHarness).unwrap();
    let session_id = fixture_session(&database.store, &registry).await;
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings::default(),
    )
    .unwrap();

    message(&database.store, session_id, "message-1", "hello").await;
    assert!(
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    let session = database.store.session(session_id).await.unwrap();
    assert_eq!(session.state_version.0, 1);
    assert_eq!(session.status, agent_contracts::SessionStatus::Running);
    assert!(session.state.get().contains(r#"\ud800"#));
    let history = database
        .store
        .history(
            session_id,
            agent_contracts::HistorySequence(0),
            agent_contracts::HistorySequence(i64::MAX),
            10,
        )
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    let operations = database
        .store
        .operations(session_id, None, 10)
        .await
        .unwrap();
    assert_eq!(operations.len(), 1);

    let claimed_operation = database
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let failure = serde_json::to_value(OperationFailure {
        source: FailureSource::Admission,
        code: "rejected".into(),
        message: "fixture rejection".into(),
        execution_response: None,
    })
    .unwrap();
    database
        .store
        .complete_operation(
            &claimed_operation,
            OperationStatus::Failed,
            None,
            Some(failure),
            None,
        )
        .await
        .unwrap();
    assert!(
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    let session = database.store.session(session_id).await.unwrap();
    assert_eq!(session.state_version.0, 2);
    assert_eq!(session.status, agent_contracts::SessionStatus::Idle);
    assert!(session.state.get().contains(r#""pending":null"#));
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn cancellation_is_harness_directed_and_late_completion_does_not_reactivate() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(FixtureHarness).unwrap();
    let session_id = fixture_session(&database.store, &registry).await;
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings::default(),
    )
    .unwrap();
    message(&database.store, session_id, "first", "hello").await;
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
    database
        .store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::CancellationRequested,
            payload: raw("{}"),
            idempotency_key: Some("cancel".into()),
        })
        .await
        .unwrap();
    assert_eq!(
        database.store.session(session_id).await.unwrap().status,
        SessionStatus::Running
    );
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        database.store.session(session_id).await.unwrap().status,
        SessionStatus::Cancelled
    );
    let claim = database
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.operation.id, operation_id);
    database
        .store
        .complete_operation(
            &claim,
            OperationStatus::Failed,
            None,
            Some(json!({"code":"late_rejection"})),
            None,
        )
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        database.store.session(session_id).await.unwrap().status,
        SessionStatus::Cancelled
    );
    message(&database.store, session_id, "second", "new turn").await;
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        database.store.session(session_id).await.unwrap().status,
        SessionStatus::Running
    );
    assert_eq!(
        database
            .store
            .operations(session_id, None, 10)
            .await
            .unwrap()
            .len(),
        2
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn wait_expiration_worker_recovers_due_waits_without_duplicate_events() {
    let database = Database::new().await;
    let session_id = basic_session(&database.store, "slow", 0).await;
    let claim = database
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let expiration = WaitId::new();
    let either = WaitId::new();
    let external = WaitId::new();
    let waits = [
        (
            expiration,
            WaitMode::Expiration,
            Some(Utc::now() - chrono::Duration::seconds(1)),
        ),
        (
            either,
            WaitMode::Either,
            Some(Utc::now() - chrono::Duration::seconds(1)),
        ),
        (external, WaitMode::External, None),
    ]
    .into_iter()
    .map(|(id, mode, expires_at)| NewWait {
        id,
        mode,
        payload: raw("{}"),
        response_schema: None,
        expires_at,
    })
    .collect();
    database
        .store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
            OutcomeCommit {
                state: raw("{}"),
                status: Some(SessionStatus::Waiting),
                history: vec![],
                operations: vec![],
                waits,
                cancelled_waits: vec![],
                progress: vec![],
            },
        )
        .await
        .unwrap();
    let settings = WaitExpirationSettings {
        batch_size: 10,
        ..WaitExpirationSettings::default()
    };
    let first = WaitExpirationWorker::new(database.store.clone(), settings.clone()).unwrap();
    let restarted = WaitExpirationWorker::new(database.store.clone(), settings).unwrap();
    let (left, right) = tokio::join!(first.process_batch(), restarted.process_batch());
    assert!(left.unwrap() + right.unwrap() >= 2);
    assert_eq!(
        database.store.wait(expiration).await.unwrap().status,
        WaitStatus::Expired
    );
    assert_eq!(
        database.store.wait(either).await.unwrap().status,
        WaitStatus::Expired
    );
    assert_eq!(
        database.store.wait(external).await.unwrap().status,
        WaitStatus::Pending
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE session_id=$1 AND type='wait_resumed'",
    )
    .bind(session_id.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(count, 2);
    assert_eq!(restarted.process_batch().await.unwrap(), 0);

    let next_claim = database
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next_claim.session.id, session_id);
    let next_wait = WaitId::new();
    database
        .store
        .commit_handler_outcome(
            &HandlerClaim::from(&next_claim),
            OutcomeCommit {
                state: raw("{}"),
                status: Some(SessionStatus::Waiting),
                history: vec![],
                operations: vec![],
                cancelled_waits: vec![],
                progress: vec![],
                waits: vec![NewWait {
                    id: next_wait,
                    mode: WaitMode::Expiration,
                    payload: raw("{}"),
                    response_schema: None,
                    expires_at: Some(Utc::now() - chrono::Duration::seconds(1)),
                }],
            },
        )
        .await
        .unwrap();
    let stop = CancellationToken::new();
    let worker_stop = stop.clone();
    let worker = tokio::spawn(async move { restarted.run(worker_stop).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if database.store.wait(next_wait).await.unwrap().status == WaitStatus::Expired {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn locked_oldest_wait_does_not_starve_other_sessions_at_batch_size_one() {
    let database = Database::new().await;
    let (old_session, old_wait) = due_wait(&database.store, 20).await;
    let (_, later_wait) = due_wait(&database.store, 10).await;
    let mut lock = database.store.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM sessions WHERE id=$1 FOR UPDATE")
        .bind(old_session.0)
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let worker = WaitExpirationWorker::new(
        database.store.clone(),
        WaitExpirationSettings {
            batch_size: 1,
            ..WaitExpirationSettings::default()
        },
    )
    .unwrap();
    assert!(worker.process_batch().await.is_err());
    assert_eq!(worker.process_batch().await.unwrap(), 1);
    assert_eq!(
        database.store.wait(later_wait).await.unwrap().status,
        WaitStatus::Expired
    );
    lock.rollback().await.unwrap();
    assert_eq!(worker.process_batch().await.unwrap(), 0);
    assert_eq!(worker.process_batch().await.unwrap(), 1);
    assert_eq!(
        database.store.wait(old_wait).await.unwrap().status,
        WaitStatus::Expired
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn wait_worker_shutdown_interrupts_scan_and_active_transition() {
    let database = Database::new().await;
    let mut table_lock = database.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE session_waits IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *table_lock)
        .await
        .unwrap();
    let worker =
        WaitExpirationWorker::new(database.store.clone(), WaitExpirationSettings::default())
            .unwrap();
    let stop = CancellationToken::new();
    let mut task = tokio::spawn(worker.run(stop.clone()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    stop.cancel();
    tokio::time::timeout(Duration::from_millis(250), &mut task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    table_lock.rollback().await.unwrap();

    let (session_id, wait_id) = due_wait(&database.store, 1).await;
    let mut session_lock = database.store.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM sessions WHERE id=$1 FOR UPDATE")
        .bind(session_id.0)
        .fetch_one(&mut *session_lock)
        .await
        .unwrap();
    let worker =
        WaitExpirationWorker::new(database.store.clone(), WaitExpirationSettings::default())
            .unwrap();
    let stop = CancellationToken::new();
    let mut task = tokio::spawn(worker.run(stop.clone()));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() \
                 AND wait_event_type='Lock' AND query LIKE 'SELECT next_event_sequence FROM sessions%')",
            ).fetch_one(database.store.pool()).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_millis(250), &mut task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    session_lock.rollback().await.unwrap();
    assert_eq!(
        database.store.wait(wait_id).await.unwrap().status,
        WaitStatus::Pending
    );
    let pre_cancelled = CancellationToken::new();
    pre_cancelled.cancel();
    WaitExpirationWorker::new(database.store.clone(), WaitExpirationSettings::default())
        .unwrap()
        .run(pre_cancelled)
        .await
        .unwrap();
    assert_eq!(
        database.store.wait(wait_id).await.unwrap().status,
        WaitStatus::Pending
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn wait_worker_retries_after_transient_scan_failure() {
    let database = Database::new().await;
    let (_, wait_id) = due_wait(&database.store, 1).await;
    let mut table_lock = database.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE session_waits IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *table_lock)
        .await
        .unwrap();
    let worker = WaitExpirationWorker::new(
        database.store.clone(),
        WaitExpirationSettings {
            poll_interval: Duration::from_millis(10),
            ..WaitExpirationSettings::default()
        },
    )
    .unwrap();
    let stop = CancellationToken::new();
    let task = tokio::spawn(worker.run(stop.clone()));
    tokio::time::sleep(Duration::from_millis(3_200)).await;
    table_lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if database.store.wait(wait_id).await.unwrap().status == WaitStatus::Expired {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    task.await.unwrap().unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn repeated_handler_failure_blocks_processing_after_three_attempts() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(FixtureHarness).unwrap();
    let session_id = fixture_session(&database.store, &registry).await;
    let settings = SchedulerSettings {
        retry_policy: agent_runtime::RetryPolicy {
            retry_delays: vec![Duration::from_millis(1), Duration::from_millis(1)],
        },
        ..SchedulerSettings::default()
    };
    let scheduler =
        SessionScheduler::new(database.store.clone(), Arc::new(registry), settings).unwrap();

    sqlx::query("UPDATE sessions SET configuration=$2::json WHERE id=$1")
        .bind(session_id.0)
        .bind(r#"{"connection":"llm","account_id":"invalid","model_id":"model"}"#)
        .execute(database.store.pool())
        .await
        .unwrap();
    message(&database.store, session_id, "message-1", "hello").await;
    let event_id = database
        .store
        .claim_next_event(Duration::from_millis(1))
        .await
        .unwrap()
        .unwrap()
        .event
        .id;
    tokio::time::sleep(Duration::from_millis(2)).await;

    for _ in 0..3 {
        assert!(
            scheduler
                .process_one(CancellationToken::new())
                .await
                .unwrap()
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(
        database
            .store
            .handler_failure_count(event_id)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        database.store.event(event_id).await.unwrap().status,
        EventStatus::Blocked
    );
    let session = database.store.session(session_id).await.unwrap();
    assert!(!session.processing_enabled);
    assert_eq!(session.processing_error.unwrap()["code"], "handler_failed");
    assert!(
        !scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    let request = raw(format!(
        r#"{{"expected_event_id":"{event_id}","expected_processing_revision":{}}}"#,
        session.processing_revision,
    ));
    database
        .store
        .retry_blocked_event(
            session_id,
            event_id,
            session.processing_revision,
            "operator-retry",
            &request,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        database
            .store
            .handler_failure_count(event_id)
            .await
            .unwrap(),
        0
    );
    assert!(
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    assert_eq!(
        database
            .store
            .handler_failure_count(event_id)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        database.store.event(event_id).await.unwrap().status,
        EventStatus::Pending
    );
    assert!(
        database
            .store
            .session(session_id)
            .await
            .unwrap()
            .processing_enabled
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn lease_renewal_keeps_one_handler_authoritative_while_new_input_queues() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(SlowHarness).unwrap();
    let configuration = raw(r#"{"delay_ms":80}"#);
    let state = registry
        .get(&HarnessId("slow".into()), &HarnessVersion("1".into()))
        .unwrap()
        .initialize(&configuration)
        .unwrap();
    let session_id = SessionId::new();
    database
        .store
        .create_session(NewSession {
            id: session_id,
            project_id: ProjectId("project".into()),
            harness_id: HarnessId("slow".into()),
            harness_version: HarnessVersion("1".into()),
            configuration,
            state,
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let settings = SchedulerSettings {
        lease_duration: Duration::from_millis(30),
        lease_renewal_interval: Duration::from_millis(5),
        ..SchedulerSettings::default()
    };
    let scheduler =
        SessionScheduler::new(database.store.clone(), Arc::new(registry), settings).unwrap();

    message(&database.store, session_id, "message-1", "first").await;
    let first = tokio::spawn({
        let scheduler = scheduler.clone();
        async move {
            scheduler
                .process_one(CancellationToken::new())
                .await
                .unwrap()
        }
    });
    wait_until_claimed(&database.store, session_id).await;
    tokio::time::sleep(Duration::from_millis(45)).await;
    message(&database.store, session_id, "message-2", "second").await;
    assert!(
        !scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    assert!(first.await.unwrap());
    assert!(
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap()
    );
    assert_eq!(
        serde_json::from_str::<SlowState>(
            database
                .store
                .session(session_id)
                .await
                .unwrap()
                .state
                .get()
        )
        .unwrap()
        .handled,
        2
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn lease_renewal_continues_during_outcome_preparation() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(FixtureHarness).unwrap();
    let session_id = fixture_session(&database.store, &registry).await;
    message(&database.store, session_id, "message-1", "hello").await;
    let mut lock = database.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE session_operations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings {
            lease_duration: Duration::from_millis(100),
            lease_renewal_interval: Duration::from_millis(20),
            ..SchedulerSettings::default()
        },
    )
    .unwrap();
    let worker = tokio::spawn(async move {
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap();
    });
    wait_until_claimed(&database.store, session_id).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    lock.rollback().await.unwrap();
    worker.await.unwrap();
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
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn lost_ownership_stops_local_handler_within_the_infrastructure_grace() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(SlowHarness).unwrap();
    let session_id = basic_session(&database.store, "slow", 60_000).await;
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings {
            lease_duration: Duration::from_millis(100),
            lease_renewal_interval: Duration::from_millis(20),
            ownership_loss_grace: Duration::from_millis(50),
            ..SchedulerSettings::default()
        },
    )
    .unwrap();
    let mut worker = tokio::spawn(async move {
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap();
    });
    wait_until_claimed(&database.store, session_id).await;
    sqlx::query(
        "UPDATE sessions SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(session_id.0)
    .execute(database.store.pool())
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_millis(300), &mut worker)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        database
            .store
            .session(session_id)
            .await
            .unwrap()
            .state_version
            .0,
        0
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn context_database_outage_does_not_consume_handler_retries() {
    let database = Database::new().await;
    let session_id = basic_session(&database.store, "reading", 0).await;
    let event_id = agent_contracts::EventId(
        sqlx::query_scalar("SELECT id FROM session_events WHERE session_id=$1")
            .bind(session_id.0)
            .fetch_one(database.store.pool())
            .await
            .unwrap(),
    );
    let mut registry = HarnessRegistry::new();
    registry
        .register(ReadingHarness {
            panic_validation: false,
        })
        .unwrap();
    let mut url = Url::parse(&database.admin_url).unwrap();
    url.set_path(&format!("/{}", database.name));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='50ms'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(url.as_str())
        .await
        .unwrap();
    let scheduler = SessionScheduler::new(
        Store::from_pool(pool.clone()),
        Arc::new(registry),
        SchedulerSettings {
            retry_policy: agent_runtime::RetryPolicy {
                retry_delays: vec![],
            },
            ..SchedulerSettings::default()
        },
    )
    .unwrap();
    let mut lock = database.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE session_history IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    lock.rollback().await.unwrap();
    let session = database.store.session(session_id).await.unwrap();
    assert!(session.processing_enabled);
    assert_eq!(
        database
            .store
            .handler_failure_count(event_id)
            .await
            .unwrap(),
        0
    );
    pool.close().await;
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn validation_panic_blocks_only_its_session() {
    let database = Database::new().await;
    let session_id = basic_session(&database.store, "reading", 0).await;
    let healthy = basic_session(&database.store, "slow", 0).await;
    let mut registry = HarnessRegistry::new();
    registry
        .register(ReadingHarness {
            panic_validation: true,
        })
        .unwrap();
    registry.register(SlowHarness).unwrap();
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings::default(),
    )
    .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let session = database.store.session(session_id).await.unwrap();
    assert!(!session.processing_enabled);
    assert_eq!(
        session.processing_error.unwrap()["code"],
        "harness_validation_panicked"
    );
    assert_eq!(
        database
            .store
            .session(healthy)
            .await
            .unwrap()
            .state_version
            .0,
        1
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn confirmed_commit_rollback_retries_the_same_prepared_outcome() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(FixtureHarness).unwrap();
    let session_id = fixture_session(&database.store, &registry).await;
    message(&database.store, session_id, "message-1", "hello").await;
    sqlx::raw_sql(
        "CREATE SEQUENCE runtime_commit_attempt;
         CREATE FUNCTION runtime_fail_first_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF nextval('runtime_commit_attempt') = 1 THEN
                 RAISE EXCEPTION 'transient rollback' USING ERRCODE='40001';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER runtime_fail_first_commit BEFORE INSERT ON session_history
         FOR EACH ROW EXECUTE FUNCTION runtime_fail_first_commit();",
    )
    .execute(database.store.pool())
    .await
    .unwrap();
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings::default(),
    )
    .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let session = database.store.session(session_id).await.unwrap();
    assert_eq!(session.state_version.0, 1);
    assert_eq!(
        database
            .store
            .history(
                session_id,
                HistorySequence(0),
                HistorySequence(i64::MAX),
                10,
            )
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        database
            .store
            .operations(session_id, None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn ownership_loss_during_commit_retry_discards_the_prepared_outcome() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(FixtureHarness).unwrap();
    let session_id = fixture_session(&database.store, &registry).await;
    message(&database.store, session_id, "message-1", "hello").await;
    sqlx::raw_sql(
        "CREATE SEQUENCE runtime_lost_commit_attempt;
         CREATE FUNCTION runtime_fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM nextval('runtime_lost_commit_attempt');
             RAISE EXCEPTION 'transient rollback' USING ERRCODE='40001';
         END $$;
         CREATE TRIGGER runtime_fail_commit BEFORE INSERT ON session_history
         FOR EACH ROW EXECUTE FUNCTION runtime_fail_commit();",
    )
    .execute(database.store.pool())
    .await
    .unwrap();
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings {
            lease_duration: Duration::from_millis(500),
            lease_renewal_interval: Duration::from_millis(20),
            commit_retry_delays: vec![Duration::from_millis(200)],
            ..SchedulerSettings::default()
        },
    )
    .unwrap();
    let worker = tokio::spawn(async move {
        scheduler
            .process_one(CancellationToken::new())
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let called: bool =
                sqlx::query_scalar("SELECT is_called FROM runtime_lost_commit_attempt")
                    .fetch_one(database.store.pool())
                    .await
                    .unwrap();
            if called {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    sqlx::query(
        "UPDATE sessions SET lease_token=$2,lease_expires_at=clock_timestamp()+interval '1 second' \
         WHERE id=$1",
    )
    .bind(session_id.0)
    .bind(Uuid::new_v4())
    .execute(database.store.pool())
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_millis(300), worker)
        .await
        .unwrap()
        .unwrap();
    let session = database.store.session(session_id).await.unwrap();
    assert_eq!(session.state_version.0, 0);
    assert!(
        database
            .store
            .history(
                session_id,
                HistorySequence(0),
                HistorySequence(i64::MAX),
                10,
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .store
            .operations(session_id, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn scheduler_run_bounds_capacity_and_advances_multiple_sessions() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(SlowHarness).unwrap();
    let sessions = vec![
        basic_session(&database.store, "slow", 200).await,
        basic_session(&database.store, "slow", 200).await,
        basic_session(&database.store, "slow", 200).await,
    ];
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings {
            max_concurrent_handlers: 2,
            poll_interval: Duration::from_millis(5),
            lease_duration: Duration::from_millis(500),
            lease_renewal_interval: Duration::from_millis(50),
            ..SchedulerSettings::default()
        },
    )
    .unwrap();
    let stop = CancellationToken::new();
    let runtime = tokio::spawn(scheduler.run(stop.clone()));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let active: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM sessions WHERE current_event_id IS NOT NULL",
            )
            .fetch_one(database.store.pool())
            .await
            .unwrap();
            if active == 2 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let mut completed = true;
            for session_id in &sessions {
                completed &= database
                    .store
                    .session(*session_id)
                    .await
                    .unwrap()
                    .state_version
                    .0
                    == 1;
            }
            if completed {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    runtime.await.unwrap().unwrap();
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn scheduler_shutdown_is_cooperative_then_forced_and_restart_recovers() {
    let database = Database::new().await;
    let mut cooperative_registry = HarnessRegistry::new();
    cooperative_registry.register(SlowHarness).unwrap();
    let cooperative = basic_session(&database.store, "slow", 0).await;
    sqlx::query(
        "UPDATE sessions SET configuration='{\"delay_ms\":0,\"cooperative\":true}'::json WHERE id=$1",
    )
    .bind(cooperative.0)
    .execute(database.store.pool())
    .await
    .unwrap();
    let cooperative_scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(cooperative_registry),
        SchedulerSettings {
            poll_interval: Duration::from_millis(5),
            shutdown_grace: Duration::from_millis(200),
            ..SchedulerSettings::default()
        },
    )
    .unwrap();
    let stop = CancellationToken::new();
    let runtime = tokio::spawn(cooperative_scheduler.run(stop.clone()));
    wait_until_claimed(&database.store, cooperative).await;
    stop.cancel();
    runtime.await.unwrap().unwrap();
    assert_eq!(
        database
            .store
            .session(cooperative)
            .await
            .unwrap()
            .state_version
            .0,
        1
    );

    let mut forced_registry = HarnessRegistry::new();
    forced_registry.register(SlowHarness).unwrap();
    let forced = basic_session(&database.store, "slow", 60_000).await;
    let settings = SchedulerSettings {
        max_concurrent_handlers: 1,
        poll_interval: Duration::from_millis(5),
        lease_duration: Duration::from_millis(100),
        lease_renewal_interval: Duration::from_millis(20),
        shutdown_grace: Duration::from_millis(50),
        ..SchedulerSettings::default()
    };
    let forced_scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(forced_registry),
        settings.clone(),
    )
    .unwrap();
    let stop = CancellationToken::new();
    let runtime = tokio::spawn(forced_scheduler.run(stop.clone()));
    wait_until_claimed(&database.store, forced).await;
    stop.cancel();
    tokio::time::timeout(Duration::from_millis(300), runtime)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        database
            .store
            .session(forced)
            .await
            .unwrap()
            .state_version
            .0,
        0
    );
    sqlx::query("UPDATE sessions SET configuration='{\"delay_ms\":0}'::json WHERE id=$1")
        .bind(forced.0)
        .execute(database.store.pool())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut restart_registry = HarnessRegistry::new();
    restart_registry.register(SlowHarness).unwrap();
    let restart =
        SessionScheduler::new(database.store.clone(), Arc::new(restart_registry), settings)
            .unwrap();
    assert!(restart.process_one(CancellationToken::new()).await.unwrap());
    assert_eq!(
        database
            .store
            .session(forced)
            .await
            .unwrap()
            .state_version
            .0,
        1
    );
    let abandoned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_event_attempts a JOIN session_events e ON e.id=a.event_id \
         WHERE e.session_id=$1 AND a.status='abandoned'",
    )
    .bind(forced.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(abandoned, 1);
    let event_id = agent_contracts::EventId(
        sqlx::query_scalar("SELECT id FROM session_events WHERE session_id=$1")
            .bind(forced.0)
            .fetch_one(database.store.pool())
            .await
            .unwrap(),
    );
    assert_eq!(
        database
            .store
            .handler_failure_count(event_id)
            .await
            .unwrap(),
        0
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn invalid_state_and_missing_harness_are_session_local_operational_blocks() {
    let database = Database::new().await;
    let missing = basic_session(&database.store, "missing", 0).await;
    let invalid_state = basic_session(&database.store, "slow", 0).await;
    sqlx::query("UPDATE sessions SET state='{\"wrong\":0}'::json WHERE id=$1")
        .bind(invalid_state.0)
        .execute(database.store.pool())
        .await
        .unwrap();
    let healthy = basic_session(&database.store, "slow", 0).await;
    let mut registry = HarnessRegistry::new();
    registry.register(SlowHarness).unwrap();
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings::default(),
    )
    .unwrap();
    for _ in 0..3 {
        assert!(
            scheduler
                .process_one(CancellationToken::new())
                .await
                .unwrap()
        );
    }
    let missing = database.store.session(missing).await.unwrap();
    assert!(!missing.processing_enabled);
    assert_eq!(
        missing.processing_error.unwrap()["code"],
        "harness_not_registered"
    );
    let invalid_state = database.store.session(invalid_state).await.unwrap();
    assert!(!invalid_state.processing_enabled);
    assert_eq!(
        invalid_state.processing_error.unwrap()["code"],
        "invalid_handler_data"
    );
    assert_eq!(
        database
            .store
            .session(healthy)
            .await
            .unwrap()
            .state_version
            .0,
        1
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn durable_wait_resolution_is_delivered_through_the_real_store_context() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(WaitHarness).unwrap();
    let configuration = raw(r#"{"delay_ms":0}"#);
    let state = registry
        .get(&HarnessId("wait".into()), &HarnessVersion("1".into()))
        .unwrap()
        .initialize(&configuration)
        .unwrap();
    let session_id = SessionId::new();
    database
        .store
        .create_session(NewSession {
            id: session_id,
            project_id: ProjectId("project".into()),
            harness_id: HarnessId("wait".into()),
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
        SchedulerSettings::default(),
    )
    .unwrap();
    message(&database.store, session_id, "message-1", "wait").await;
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let waits = database
        .store
        .waits(session_id, Some(agent_store::WaitStatus::Pending), 10)
        .await
        .unwrap();
    assert_eq!(waits.len(), 1);
    database
        .store
        .resolve_wait(waits[0].id, "resolution-1", raw(r#""continue""#).as_ref())
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let session = database.store.session(session_id).await.unwrap();
    assert_eq!(session.status, SessionStatus::Idle);
    let state: WaitState = serde_json::from_str(session.state.get()).unwrap();
    assert!(state.resumed);
    assert_eq!(
        database
            .store
            .history(
                session_id,
                HistorySequence(0),
                HistorySequence(i64::MAX),
                10,
            )
            .await
            .unwrap()
            .len(),
        1
    );
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn harness_staged_wait_cancellation_resumes_once_and_preserves_cancelled_status() {
    let database = Database::new().await;
    let mut registry = HarnessRegistry::new();
    registry.register(WaitHarness).unwrap();
    let configuration = raw(r#"{"delay_ms":0}"#);
    let state = registry
        .get(&HarnessId("wait".into()), &HarnessVersion("1".into()))
        .unwrap()
        .initialize(&configuration)
        .unwrap();
    let session_id = SessionId::new();
    database
        .store
        .create_session(NewSession {
            id: session_id,
            project_id: ProjectId("project".into()),
            harness_id: HarnessId("wait".into()),
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
        SchedulerSettings::default(),
    )
    .unwrap();
    message(&database.store, session_id, "message", "wait").await;
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let wait_id = database
        .store
        .waits(session_id, Some(WaitStatus::Pending), 10)
        .await
        .unwrap()[0]
        .id;
    database
        .store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::CancellationRequested,
            payload: raw("{}"),
            idempotency_key: Some("cancel".into()),
        })
        .await
        .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        database.store.wait(wait_id).await.unwrap().status,
        WaitStatus::Cancelled
    );
    assert_eq!(
        database.store.session(session_id).await.unwrap().status,
        SessionStatus::Cancelled
    );
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let session = database.store.session(session_id).await.unwrap();
    assert_eq!(session.status, SessionStatus::Cancelled);
    assert!(
        serde_json::from_str::<WaitState>(session.state.get())
            .unwrap()
            .resumed
    );
    assert!(matches!(
        database
            .store
            .resolve_wait(wait_id, "reply", raw(r#""continue""#).as_ref())
            .await,
        Err(agent_store::StoreError::WaitNotResolvable)
    ));
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn invalid_outcome_blocks_without_partial_state_or_wait_writes() {
    let database = Database::new().await;
    let session_id = basic_session(&database.store, "invalid-outcome", 0).await;
    let mut registry = HarnessRegistry::new();
    registry.register(InvalidOutcomeHarness).unwrap();
    let scheduler = SessionScheduler::new(
        database.store.clone(),
        Arc::new(registry),
        SchedulerSettings::default(),
    )
    .unwrap();
    scheduler
        .process_one(CancellationToken::new())
        .await
        .unwrap();
    let session = database.store.session(session_id).await.unwrap();
    assert_eq!(session.state_version.0, 0);
    assert!(!session.processing_enabled);
    assert_eq!(
        session.processing_error.unwrap()["code"],
        "invalid_handler_outcome"
    );
    assert!(
        database
            .store
            .waits(session_id, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    database.close().await;
}
