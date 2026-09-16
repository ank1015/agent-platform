use agent_contracts::{
    EventSequence, GatewayConnectionId, HarnessId, HarnessVersion, HistoryEntryId, HistorySequence,
    OperationId, OperationKind, ProjectId, SessionId, SessionStatus, StateVersion, WaitId,
    WaitMode,
};
use agent_store::{
    CALLBACK_READY_CHANNEL, EnqueueResult, EventStatus, EventType, HandlerAttemptStatus,
    HandlerClaim, MIGRATOR, NewExternalEvent, NewHistoryEntry, NewOperation, NewSession, NewWait,
    OPERATION_READY_CHANNEL, OperationPhase, OperationStatus, OutcomeCommit, PoolConfig,
    SessionFilter, Store, StoreError, WaitStatus,
};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{json, value::RawValue};
use sqlx::{PgPool, postgres::PgListener};
use std::time::Duration;
use url::Url;
use uuid::Uuid;

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
        let name = format!("agent_store_test_{}", Uuid::new_v4().simple());
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

fn raw(value: &str) -> Box<RawValue> {
    RawValue::from_string(value.to_owned()).unwrap()
}

async fn session(store: &Store, project: &str) -> SessionId {
    let id = SessionId::new();
    store
        .create_session(NewSession {
            id,
            project_id: ProjectId(project.into()),
            harness_id: HarnessId("fixture".into()),
            harness_version: HarnessVersion("1".into()),
            configuration: raw(r#"{"native":"\ud800"}"#),
            state: raw(r#"{"step":0}"#),
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    id
}

async fn input(store: &Store, session_id: SessionId, key: &str) {
    store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload: raw(
                r#"{"message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#,
            ),
            idempotency_key: Some(key.into()),
        })
        .await
        .unwrap();
}

fn empty_outcome(state: &str) -> OutcomeCommit {
    OutcomeCommit {
        cancelled_waits: vec![],
        progress: vec![],
        state: raw(state),
        status: None,
        history: vec![],
        operations: vec![],
        waits: vec![],
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn durable_work_commits_emit_transactional_notifications() {
    let db = Database::new().await;
    let mut listener = PgListener::connect_with(db.store.pool()).await.unwrap();
    listener.listen(OPERATION_READY_CHANNEL).await.unwrap();
    listener.listen(CALLBACK_READY_CHANNEL).await.unwrap();

    let session_id = session(&db.store, "notifications").await;
    input(&db.store, session_id, "message").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    db.store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
            OutcomeCommit {
                operations: vec![NewOperation {
                    id: OperationId::new(),
                    kind: OperationKind::Llm,
                    gateway_connection_id: Some(GatewayConnectionId("llm".into())),
                    target_operation_id: None,
                    previous_operation_id: None,
                    request: raw(r#"{"messages":[]}"#),
                }],
                ..empty_outcome("{}")
            },
        )
        .await
        .unwrap();
    let operation = tokio::time::timeout(Duration::from_secs(1), listener.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.channel(), OPERATION_READY_CHANNEL);

    db.store
        .record_callback_receipt(
            GatewayConnectionId("llm".into()),
            Uuid::new_v4(),
            Uuid::new_v4(),
            raw(r#"{"type":"job.succeeded"}"#).as_ref(),
        )
        .await
        .unwrap();
    let callback = tokio::time::timeout(Duration::from_secs(1), listener.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(callback.channel(), CALLBACK_READY_CHANNEL);

    drop(listener);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn schema_readiness_requires_exact_successful_migrations() {
    let db = Database::new().await;
    assert!(db.store.schema_ready().await.unwrap());

    let migration: (i64, String, chrono::DateTime<Utc>, bool, Vec<u8>, i64) = sqlx::query_as(
        "SELECT version,description,installed_on,success,checksum,execution_time \
         FROM _sqlx_migrations",
    )
    .fetch_one(db.store.pool())
    .await
    .unwrap();
    sqlx::query("DELETE FROM _sqlx_migrations")
        .execute(db.store.pool())
        .await
        .unwrap();
    assert!(!db.store.schema_ready().await.unwrap());
    sqlx::query(
        "INSERT INTO _sqlx_migrations \
         (version,description,installed_on,success,checksum,execution_time) \
         VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(migration.0)
    .bind(&migration.1)
    .bind(migration.2)
    .bind(migration.3)
    .bind(&migration.4)
    .bind(migration.5)
    .execute(db.store.pool())
    .await
    .unwrap();

    sqlx::query("UPDATE _sqlx_migrations SET success=false")
        .execute(db.store.pool())
        .await
        .unwrap();
    assert!(!db.store.schema_ready().await.unwrap());
    sqlx::query("UPDATE _sqlx_migrations SET success=true")
        .execute(db.store.pool())
        .await
        .unwrap();

    sqlx::query("UPDATE _sqlx_migrations SET checksum=decode('00','hex')")
        .execute(db.store.pool())
        .await
        .unwrap();
    assert!(!db.store.schema_ready().await.unwrap());
    let checksum = MIGRATOR.iter().next().unwrap().checksum.clone();
    sqlx::query("UPDATE _sqlx_migrations SET checksum=$1")
        .bind(checksum.as_ref())
        .execute(db.store.pool())
        .await
        .unwrap();

    sqlx::query("ALTER TABLE session_waits RENAME TO unavailable_session_waits")
        .execute(db.store.pool())
        .await
        .unwrap();
    assert!(!db.store.schema_ready().await.unwrap());
    sqlx::query("ALTER TABLE unavailable_session_waits RENAME TO session_waits")
        .execute(db.store.pool())
        .await
        .unwrap();

    db.store.pool().close().await;
    assert!(db.store.schema_ready().await.is_err());
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn migrations_sequences_atomic_commit_and_raw_json() {
    let db = Database::new().await;
    db.store.ping().await.unwrap();
    let session_id = session(&db.store, "project-a").await;
    let saved = db.store.session(session_id).await.unwrap();
    assert_eq!(saved.configuration.get(), r#"{"native":"\ud800"}"#);

    input(&db.store, session_id, "one").await;
    let claimed = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.event.sequence, EventSequence(1));
    assert_eq!(claimed.history_through_sequence, HistorySequence(0));
    assert!(
        db.store
            .claim_next_event(Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );

    input(&db.store, session_id, "two").await;
    let operation_id = OperationId::new();
    let wait_id = WaitId::new();
    db.store
        .commit_handler_outcome(
            &HandlerClaim::from(&claimed),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw(r#"{"step":1,"native":"\ud800"}"#),
                status: Some(SessionStatus::Running),
                history: vec![NewHistoryEntry {
                    id: HistoryEntryId::new(),
                    message: raw(
                        r#"{"role":"assistant","provider":"openai","content":[{"text":"\ud800"}]}"#,
                    ),
                }],
                operations: vec![NewOperation {
                    id: operation_id,
                    kind: OperationKind::Llm,
                    gateway_connection_id: Some(GatewayConnectionId("llm".into())),
                    target_operation_id: None,
                    previous_operation_id: None,
                    request: raw(r#"{"messages":[{"text":"\ud800"}]}"#),
                }],
                waits: vec![NewWait {
                    id: wait_id,
                    mode: WaitMode::External,
                    payload: raw(r#"{"prompt":"reply"}"#),
                    response_schema: None,
                    expires_at: None,
                }],
            },
        )
        .await
        .unwrap();
    assert_eq!(
        db.store
            .handler_attempt_status(claimed.attempt_id)
            .await
            .unwrap(),
        Some(HandlerAttemptStatus::Committed)
    );
    assert!(matches!(
        db.store
            .commit_handler_outcome(
                &HandlerClaim::from(&claimed),
                empty_outcome(r#"{"duplicate":true}"#)
            )
            .await,
        Err(StoreError::StaleClaim)
    ));

    let saved = db.store.session(session_id).await.unwrap();
    assert_eq!(saved.state_version, StateVersion(1));
    assert_eq!(saved.next_event_sequence, EventSequence(3));
    assert_eq!(saved.next_history_sequence, HistorySequence(2));
    assert!(saved.state.get().contains(r#"\ud800"#));
    let history = db
        .store
        .history(session_id, HistorySequence(0), HistorySequence(1), 10)
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert!(history[0].message.get().contains(r#"\ud800"#));
    assert!(
        db.store
            .operation_request(operation_id)
            .await
            .unwrap()
            .unwrap()
            .get()
            .contains(r#"\ud800"#)
    );
    assert_eq!(
        db.store.wait(wait_id).await.unwrap().status,
        WaitStatus::Pending
    );

    let second = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.event.sequence, EventSequence(2));
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn input_idempotency_is_atomic_under_concurrency() {
    let db = Database::new().await;
    let session_id = session(&db.store, "project").await;
    let make = || NewExternalEvent {
        session_id,
        event_type: EventType::UserMessage,
        payload: raw(r#"{"message":"same"}"#),
        idempotency_key: Some("request-1".into()),
    };
    let (left, right) = tokio::join!(
        db.store.enqueue_external_event(make()),
        db.store.enqueue_external_event(make())
    );
    let inserted = [left.unwrap(), right.unwrap()]
        .into_iter()
        .filter(|result| matches!(result, EnqueueResult::Inserted(_)))
        .count();
    assert_eq!(inserted, 1);
    let conflict = db
        .store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload: raw(r#"{"message":"changed"}"#),
            idempotency_key: Some("request-1".into()),
        })
        .await;
    assert!(matches!(conflict, Err(StoreError::IdempotencyConflict)));
    assert_eq!(
        db.store
            .session(session_id)
            .await
            .unwrap()
            .next_event_sequence,
        EventSequence(2)
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn stale_claims_fail_and_invalid_outcomes_roll_back() {
    let db = Database::new().await;
    let session_id = session(&db.store, "project").await;
    input(&db.store, session_id, "one").await;
    let first = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE sessions SET lease_expires_at=now()-interval '1 second' WHERE id=$1")
        .bind(session_id.0)
        .execute(db.store.pool())
        .await
        .unwrap();
    let second = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        db.store
            .commit_handler_outcome(&HandlerClaim::from(&first), empty_outcome(r#"{"step":1}"#))
            .await,
        Err(StoreError::StaleClaim)
    ));

    let bad_history_id = HistoryEntryId::new();
    let result = db
        .store
        .commit_handler_outcome(
            &HandlerClaim::from(&second),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw(r#"{"step":2}"#),
                status: None,
                history: vec![NewHistoryEntry {
                    id: bad_history_id,
                    message: raw(r#"{"role":"assistant","provider":"openai","content":[]}"#),
                }],
                operations: vec![],
                waits: vec![NewWait {
                    id: WaitId::new(),
                    mode: WaitMode::External,
                    payload: raw("{}"),
                    response_schema: None,
                    expires_at: Some(Utc::now() + ChronoDuration::minutes(1)),
                }],
            },
        )
        .await;
    assert!(matches!(result, Err(StoreError::Database(_))));
    let saved = db.store.session(session_id).await.unwrap();
    assert_eq!(saved.state_version, StateVersion(0));
    assert_eq!(saved.current_event_id, Some(second.event.id));
    assert_eq!(
        db.store.event(second.event.id).await.unwrap().status,
        EventStatus::Processing
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM session_history WHERE id=$1")
        .bind(bad_history_id.0)
        .fetch_one(db.store.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn operations_callbacks_completion_and_request_cleanup_converge() {
    let db = Database::new().await;
    let session_id = session(&db.store, "project").await;
    input(&db.store, session_id, "one").await;
    let event_claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let operation_id = OperationId::new();
    db.store
        .commit_handler_outcome(
            &HandlerClaim::from(&event_claim),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw("{}"),
                status: None,
                history: vec![],
                operations: vec![NewOperation {
                    id: operation_id,
                    kind: OperationKind::Llm,
                    gateway_connection_id: Some(GatewayConnectionId("llm".into())),
                    target_operation_id: None,
                    previous_operation_id: None,
                    request: raw(r#"{"messages":[]}"#),
                }],
                waits: vec![],
            },
        )
        .await
        .unwrap();
    let operation_claim = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let job_id = Uuid::new_v4();
    let callback = db
        .store
        .record_callback_receipt(
            GatewayConnectionId("llm".into()),
            Uuid::new_v4(),
            job_id,
            raw(r#"{"status":"succeeded"}"#).as_ref(),
        )
        .await
        .unwrap();
    let early_callback_claim = db
        .store
        .claim_callback_receipt(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(early_callback_claim.receipt.id, callback.id);
    assert_eq!(
        db.store
            .match_callback_receipt(&early_callback_claim, Utc::now())
            .await
            .unwrap(),
        None
    );

    db.store
        .record_operation_accepted(&operation_claim, job_id, Utc::now(), Some(202))
        .await
        .unwrap();
    assert!(
        db.store
            .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    let callback_claim = db
        .store
        .claim_callback_receipt(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.store
            .match_callback_receipt(&callback_claim, Utc::now())
            .await
            .unwrap(),
        Some(operation_id)
    );

    let result_claim = db
        .store
        .claim_operation(OperationPhase::ResultRetrieval, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    db.store
        .record_operation_retry(
            &result_claim,
            false,
            Utc::now(),
            Some(503),
            json!({"message":"temporary result read failure"}),
        )
        .await
        .unwrap();
    assert_eq!(
        db.store.operation(operation_id).await.unwrap().status,
        OperationStatus::Accepted
    );
    assert!(
        db.store
            .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    let result_claim = db
        .store
        .claim_operation(OperationPhase::ResultRetrieval, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    db.store
        .record_operation_pending_result(&result_claim, Utc::now(), Some(200))
        .await
        .unwrap();
    let result_claim = db
        .store
        .claim_operation(OperationPhase::ResultRetrieval, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let completion = db
        .store
        .complete_operation(
            &result_claim,
            OperationStatus::Succeeded,
            Some(raw(r#"{"kind":"llm","value":{"id":"response"}}"#).as_ref()),
            None,
            Some(Utc::now() - ChronoDuration::seconds(1)),
        )
        .await
        .unwrap();
    db.store
        .complete_callback_receipt(&callback_claim)
        .await
        .unwrap();
    assert_eq!(completion.sequence, EventSequence(2));
    assert_eq!(completion.event_type, EventType::OperationCompleted);
    assert_eq!(
        db.store
            .delete_expired_operation_requests(10)
            .await
            .unwrap(),
        1
    );
    let operation = db.store.operation(operation_id).await.unwrap();
    assert_eq!(operation.status, OperationStatus::Succeeded);
    assert!(
        db.store
            .operation_request(operation_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(operation.result.is_some());
    let completion_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE operation_id=$1 AND type='operation_completed'",
    )
    .bind(operation_id.0)
    .fetch_one(db.store.pool())
    .await
    .unwrap();
    assert_eq!(completion_count, 1);
    assert!(matches!(
        db.store
            .complete_operation(
                &result_claim,
                OperationStatus::Failed,
                Some(raw(r#"{"different":true}"#).as_ref()),
                None,
                None
            )
            .await,
        Err(StoreError::StaleClaim)
    ));
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn wait_deadline_and_resolution_are_one_atomic_transition() {
    let db = Database::new().await;
    let session_id = session(&db.store, "project").await;
    input(&db.store, session_id, "one").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let wait_id = WaitId::new();
    let expired_wait_id = WaitId::new();
    db.store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw("{}"),
                status: Some(SessionStatus::Waiting),
                history: vec![],
                operations: vec![],
                waits: vec![
                    NewWait {
                        id: wait_id,
                        mode: WaitMode::Either,
                        payload: raw("{}"),
                        response_schema: None,
                        expires_at: Some(Utc::now() + ChronoDuration::minutes(1)),
                    },
                    NewWait {
                        id: expired_wait_id,
                        mode: WaitMode::Either,
                        payload: raw("{}"),
                        response_schema: None,
                        expires_at: Some(Utc::now() - ChronoDuration::seconds(1)),
                    },
                ],
            },
        )
        .await
        .unwrap();
    let first = db
        .store
        .resolve_wait(wait_id, "reply-1", raw(r#"{"answer":true}"#).as_ref())
        .await
        .unwrap();
    assert!(first.newly_resolved);
    let retry = db
        .store
        .resolve_wait(wait_id, "reply-1", raw(r#"{"answer":true}"#).as_ref())
        .await
        .unwrap();
    assert!(!retry.newly_resolved);
    assert_eq!(first.event.id, retry.event.id);
    assert!(matches!(
        db.store
            .resolve_wait(wait_id, "reply-1", raw(r#"{"answer":false}"#).as_ref())
            .await,
        Err(StoreError::IdempotencyConflict)
    ));
    assert!(db.store.expire_wait(wait_id).await.unwrap().is_none());
    assert!(matches!(
        db.store
            .resolve_wait(expired_wait_id, "late", raw(r#"{"answer":true}"#).as_ref())
            .await,
        Err(StoreError::WaitNotResolvable)
    ));
    assert!(
        db.store
            .expire_wait(expired_wait_id)
            .await
            .unwrap()
            .is_some()
    );
    let wait = db.store.wait(wait_id).await.unwrap();
    assert_eq!(wait.status, WaitStatus::Resolved);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM session_events WHERE wait_id=$1")
        .bind(wait_id.0)
        .fetch_one(db.store.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn cross_session_relationships_are_rejected_atomically() {
    let db = Database::new().await;
    let first_session = session(&db.store, "project").await;
    let second_session = session(&db.store, "project").await;

    input(&db.store, first_session, "first").await;
    let first_claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let existing_operation = OperationId::new();
    db.store
        .commit_handler_outcome(
            &HandlerClaim::from(&first_claim),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw("{}"),
                status: None,
                history: vec![],
                operations: vec![NewOperation {
                    id: existing_operation,
                    kind: OperationKind::Llm,
                    gateway_connection_id: Some(GatewayConnectionId("llm".into())),
                    target_operation_id: None,
                    previous_operation_id: None,
                    request: raw("{}"),
                }],
                waits: vec![],
            },
        )
        .await
        .unwrap();

    input(&db.store, second_session, "second").await;
    let second_claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second_claim.session.id, second_session);
    let result = db
        .store
        .commit_handler_outcome(
            &HandlerClaim::from(&second_claim),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw(r#"{"changed":true}"#),
                status: None,
                history: vec![],
                operations: vec![NewOperation {
                    id: OperationId::new(),
                    kind: OperationKind::LlmCancellation,
                    gateway_connection_id: None,
                    target_operation_id: Some(existing_operation),
                    previous_operation_id: None,
                    request: raw("{}"),
                }],
                waits: vec![],
            },
        )
        .await;
    assert!(matches!(result, Err(StoreError::Database(_))));
    assert_eq!(
        db.store
            .session(second_session)
            .await
            .unwrap()
            .state_version,
        StateVersion(0)
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn withdrawal_coordinates_with_dispatch() {
    let db = Database::new().await;

    let create_pair = |target: OperationId, withdrawal: OperationId| OutcomeCommit {
        cancelled_waits: vec![],
        progress: vec![],
        state: raw("{}"),
        status: None,
        history: vec![],
        operations: vec![
            NewOperation {
                id: target,
                kind: OperationKind::Llm,
                gateway_connection_id: Some(GatewayConnectionId("llm".into())),
                target_operation_id: None,
                previous_operation_id: None,
                request: raw("{}"),
            },
            NewOperation {
                id: withdrawal,
                kind: OperationKind::Withdraw,
                gateway_connection_id: None,
                target_operation_id: Some(target),
                previous_operation_id: None,
                request: raw("{}"),
            },
        ],
        waits: vec![],
    };

    let first_session = session(&db.store, "project").await;
    input(&db.store, first_session, "first").await;
    let event = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let target = OperationId::new();
    let withdrawal = OperationId::new();
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&event), create_pair(target, withdrawal))
        .await
        .unwrap();
    let withdrawal_claim = db
        .store
        .claim_operation(OperationPhase::Withdrawal, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let (withdrawn, events) = db
        .store
        .complete_withdrawal(&withdrawal_claim, None)
        .await
        .unwrap();
    assert!(withdrawn);
    assert_eq!(events.len(), 2);
    assert_eq!(
        db.store.operation(target).await.unwrap().status,
        OperationStatus::Cancelled
    );
    assert_eq!(
        db.store.operation(withdrawal).await.unwrap().status,
        OperationStatus::Succeeded
    );
    assert!(matches!(
        db.store.complete_withdrawal(&withdrawal_claim, None).await,
        Err(StoreError::StaleClaim)
    ));

    let second_session = session(&db.store, "project").await;
    input(&db.store, second_session, "second").await;
    let event = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let target = OperationId::new();
    let withdrawal = OperationId::new();
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&event), create_pair(target, withdrawal))
        .await
        .unwrap();
    let withdrawal_claim = db
        .store
        .claim_operation(OperationPhase::Withdrawal, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let (withdrawal_result, dispatch_result) = tokio::join!(
        db.store.complete_withdrawal(&withdrawal_claim, None),
        db.store
            .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
    );
    let (withdrawn, events) = withdrawal_result.unwrap();
    let dispatch_claim = dispatch_result.unwrap();
    let target_status = db.store.operation(target).await.unwrap().status;
    if withdrawn {
        assert_eq!(events.len(), 2);
        assert!(dispatch_claim.is_none());
        assert_eq!(target_status, OperationStatus::Cancelled);
    } else {
        assert_eq!(events.len(), 1);
        assert_eq!(dispatch_claim.unwrap().operation.id, target);
        assert_eq!(target_status, OperationStatus::Submitting);
    }
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn lightweight_queries_are_scoped_bounded_and_paginated() {
    let db = Database::new().await;
    let first = session(&db.store, "project-a").await;
    let _second = session(&db.store, "project-a").await;
    let _other = session(&db.store, "project-b").await;
    let page = db
        .store
        .list_sessions(SessionFilter {
            project_id: Some(ProjectId("project-a".into())),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.sessions.len(), 1);
    let next = db
        .store
        .list_sessions(SessionFilter {
            project_id: Some(ProjectId("project-a".into())),
            cursor: page.next_cursor,
            limit: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(next.sessions.len(), 1);
    assert_ne!(page.sessions[0].id, next.sessions[0].id);

    input(&db.store, first, "message").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let history_id = HistoryEntryId::new();
    let operation_id = OperationId::new();
    let wait_id = WaitId::new();
    db.store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw("{}"),
                status: None,
                history: vec![NewHistoryEntry {
                    id: history_id,
                    message: raw(r#"{"role":"user","content":[]}"#),
                }],
                operations: vec![NewOperation {
                    id: operation_id,
                    kind: OperationKind::Llm,
                    gateway_connection_id: Some(GatewayConnectionId("llm".into())),
                    target_operation_id: None,
                    previous_operation_id: None,
                    request: raw(r#"{"large":"payload"}"#),
                }],
                waits: vec![NewWait {
                    id: wait_id,
                    mode: WaitMode::Either,
                    payload: raw("{}"),
                    response_schema: None,
                    expires_at: Some(Utc::now() - ChronoDuration::seconds(1)),
                }],
            },
        )
        .await
        .unwrap();
    assert!(
        db.store
            .history_entry(first, history_id, HistorySequence(0))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.store
            .history_entry(first, history_id, HistorySequence(1))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(db.store.operations(first, None, 10).await.unwrap().len(), 1);
    assert!(
        db.store
            .operation_for_session(SessionId::new(), operation_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        db.store
            .waits(first, Some(WaitStatus::Pending), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        db.store
            .wait_for_session(SessionId::new(), wait_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(db.store.due_wait_ids(10).await.unwrap(), vec![wait_id]);
    assert!(
        db.store
            .operation_request(operation_id)
            .await
            .unwrap()
            .is_some()
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn blocked_head_preserves_fifo_and_other_sessions_progress() {
    let db = Database::new().await;
    let blocked_session = session(&db.store, "project").await;
    input(&db.store, blocked_session, "first").await;
    input(&db.store, blocked_session, "second").await;
    let other_session = session(&db.store, "project").await;
    input(&db.store, other_session, "other").await;

    let first = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.session.id, blocked_session);
    db.store
        .record_handler_failure(
            &HandlerClaim::from(&first),
            json!({"message":"blocked"}),
            Utc::now(),
            true,
        )
        .await
        .unwrap();
    let other = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(other.session.id, other_session);
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&other), empty_outcome("{}"))
        .await
        .unwrap();
    assert!(
        db.store
            .claim_next_event(Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn request_cleanup_only_removes_settled_recovery_safe_payloads() {
    let db = Database::new().await;
    let session_id = session(&db.store, "project").await;
    input(&db.store, session_id, "message").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let pending = OperationId::new();
    let submitting = OperationId::new();
    let accepted = OperationId::new();
    let unknown = OperationId::new();
    let operations = [pending, submitting, accepted, unknown]
        .into_iter()
        .map(|id| NewOperation {
            id,
            kind: OperationKind::Llm,
            gateway_connection_id: Some(GatewayConnectionId("llm".into())),
            target_operation_id: None,
            previous_operation_id: None,
            request: raw("{}"),
        })
        .collect();
    db.store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
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
    sqlx::query("UPDATE session_operations SET status='submitting' WHERE id=$1")
        .bind(submitting.0)
        .execute(db.store.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE session_operations SET status='accepted',gateway_job_id=$2,submitted_at=now() \
         WHERE id=$1",
    )
    .bind(accepted.0)
    .bind(Uuid::new_v4())
    .execute(db.store.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE session_operations SET status='unknown',completed_at=now() WHERE id=$1")
        .bind(unknown.0)
        .execute(db.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE session_operation_requests SET expires_at=now()-interval '1 second'")
        .execute(db.store.pool())
        .await
        .unwrap();
    assert_eq!(
        db.store
            .delete_expired_operation_requests(10)
            .await
            .unwrap(),
        1
    );
    for retained in [pending, submitting, accepted] {
        assert!(
            db.store
                .operation_request(retained)
                .await
                .unwrap()
                .is_some()
        );
    }
    assert!(db.store.operation_request(unknown).await.unwrap().is_none());
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn claims_are_exclusive_and_cleared_leases_fail_cleanly() {
    let db = Database::new().await;
    let first = session(&db.store, "project").await;
    let second = session(&db.store, "project").await;
    input(&db.store, first, "first").await;
    input(&db.store, second, "second").await;
    let (left, right) = tokio::join!(
        db.store.claim_next_event(Duration::from_secs(30)),
        db.store.claim_next_event(Duration::from_secs(30))
    );
    let left = left.unwrap().unwrap();
    let right = right.unwrap().unwrap();
    assert_ne!(left.session.id, right.session.id);
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&left), empty_outcome("{}"))
        .await
        .unwrap();
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&right), empty_outcome("{}"))
        .await
        .unwrap();

    let (_, operation_id) = review_setup_operation_for_test(&db).await;
    let operation_claim = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation_claim.operation.id, operation_id);
    db.store
        .record_operation_retry(
            &operation_claim,
            false,
            Utc::now() + ChronoDuration::minutes(1),
            None,
            json!({"message":"retry"}),
        )
        .await
        .unwrap();
    assert!(matches!(
        db.store
            .renew_operation_claim(&operation_claim, Duration::from_secs(30))
            .await,
        Err(StoreError::StaleClaim)
    ));

    let callback = db
        .store
        .record_callback_receipt(
            GatewayConnectionId("llm".into()),
            Uuid::new_v4(),
            Uuid::new_v4(),
            raw("{}").as_ref(),
        )
        .await
        .unwrap();
    let callback_claim = db
        .store
        .claim_callback_receipt(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(callback_claim.receipt.id, callback.id);
    db.store
        .retry_callback_receipt(
            &callback_claim,
            Utc::now() + ChronoDuration::minutes(1),
            json!({"message":"retry"}),
        )
        .await
        .unwrap();
    assert!(matches!(
        db.store
            .renew_callback_receipt_claim(&callback_claim, Duration::from_secs(30))
            .await,
        Err(StoreError::StaleClaim)
    ));
    db.close().await;
}

async fn review_setup_operation_for_test(db: &Database) -> (SessionId, OperationId) {
    let session_id = session(&db.store, "operation").await;
    input(&db.store, session_id, "operation").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let operation_id = OperationId::new();
    let mut outcome = empty_outcome("{}");
    outcome.operations.push(NewOperation {
        id: operation_id,
        kind: OperationKind::Llm,
        gateway_connection_id: Some(GatewayConnectionId("llm".into())),
        target_operation_id: None,
        previous_operation_id: None,
        request: raw("{}"),
    });
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&claim), outcome)
        .await
        .unwrap();
    (session_id, operation_id)
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn wall_clock_expiry_is_checked_after_lock_waits() {
    let db = Database::new().await;

    let claim_session = session(&db.store, "claim").await;
    input(&db.store, claim_session, "claim").await;
    let claimed = db
        .store
        .claim_next_event(Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("UPDATE sessions SET updated_at=updated_at WHERE id=$1")
        .bind(claim_session.0)
        .execute(&mut *lock)
        .await
        .unwrap();
    let store = db.store.clone();
    let commit = tokio::spawn(async move {
        store
            .commit_handler_outcome(
                &HandlerClaim::from(&claimed),
                empty_outcome(r#"{"changed":true}"#),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(600)).await;
    lock.commit().await.unwrap();
    assert!(matches!(commit.await.unwrap(), Err(StoreError::StaleClaim)));
    sqlx::query("UPDATE sessions SET processing_enabled=false WHERE id=$1")
        .bind(claim_session.0)
        .execute(db.store.pool())
        .await
        .unwrap();

    let wait_session = session(&db.store, "wait").await;
    input(&db.store, wait_session, "wait").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let wait_id = WaitId::new();
    let mut outcome = empty_outcome("{}");
    outcome.waits.push(NewWait {
        id: wait_id,
        mode: WaitMode::Either,
        payload: raw("{}"),
        response_schema: None,
        expires_at: Some(Utc::now() + ChronoDuration::milliseconds(300)),
    });
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&claim), outcome)
        .await
        .unwrap();
    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("UPDATE sessions SET updated_at=updated_at WHERE id=$1")
        .bind(wait_session.0)
        .execute(&mut *lock)
        .await
        .unwrap();
    let store = db.store.clone();
    let reply = tokio::spawn(async move {
        store
            .resolve_wait(wait_id, "reply", raw("{}").as_ref())
            .await
    });
    tokio::time::sleep(Duration::from_millis(600)).await;
    lock.commit().await.unwrap();
    let reply = reply.await.unwrap();
    assert!(
        matches!(reply, Err(StoreError::WaitNotResolvable)),
        "unexpected wait reply result: {reply:?}"
    );
    assert!(db.store.expire_wait(wait_id).await.unwrap().is_some());

    let (_, operation_id) = review_setup_operation_for_test(&db).await;
    let operation_claim = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation_claim.operation.id, operation_id);
    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("UPDATE session_operations SET next_attempt_at=next_attempt_at WHERE id=$1")
        .bind(operation_id.0)
        .execute(&mut *lock)
        .await
        .unwrap();
    let store = db.store.clone();
    let renewal = tokio::spawn(async move {
        store
            .renew_operation_claim(&operation_claim, Duration::from_secs(30))
            .await
    });
    tokio::time::sleep(Duration::from_millis(600)).await;
    lock.commit().await.unwrap();
    assert!(matches!(
        renewal.await.unwrap(),
        Err(StoreError::StaleClaim)
    ));

    let callback = db
        .store
        .record_callback_receipt(
            GatewayConnectionId("llm".into()),
            Uuid::new_v4(),
            Uuid::new_v4(),
            raw("{}").as_ref(),
        )
        .await
        .unwrap();
    let callback_claim = db
        .store
        .claim_callback_receipt(Duration::from_millis(300))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(callback_claim.receipt.id, callback.id);
    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("UPDATE gateway_callback_receipts SET next_attempt_at=next_attempt_at WHERE id=$1")
        .bind(callback.id)
        .execute(&mut *lock)
        .await
        .unwrap();
    let store = db.store.clone();
    let completion =
        tokio::spawn(async move { store.complete_callback_receipt(&callback_claim).await });
    tokio::time::sleep(Duration::from_millis(600)).await;
    lock.commit().await.unwrap();
    assert!(matches!(
        completion.await.unwrap(),
        Err(StoreError::StaleClaim)
    ));
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn concurrent_wait_reply_and_expiration_have_one_winner() {
    let db = Database::new().await;
    let session_id = session(&db.store, "wait-race").await;
    input(&db.store, session_id, "wait-race").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let wait_id = WaitId::new();
    let mut outcome = empty_outcome("{}");
    outcome.waits.push(NewWait {
        id: wait_id,
        mode: WaitMode::Either,
        payload: raw("{}"),
        response_schema: None,
        expires_at: Some(Utc::now() - ChronoDuration::milliseconds(1)),
    });
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&claim), outcome)
        .await
        .unwrap();

    let response = raw("{}");
    let (reply, expiration) = tokio::join!(
        db.store.resolve_wait(wait_id, "reply", response.as_ref()),
        db.store.expire_wait(wait_id)
    );
    assert!(matches!(reply, Err(StoreError::WaitNotResolvable)));
    assert!(expiration.unwrap().is_some());
    assert_eq!(
        db.store.wait(wait_id).await.unwrap().status,
        WaitStatus::Expired
    );
    let event_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM session_events WHERE wait_id=$1")
            .bind(wait_id.0)
            .fetch_one(db.store.pool())
            .await
            .unwrap();
    assert_eq!(event_count, 1);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn cancelled_wait_is_committed_with_its_resumption_and_deadline_wins() {
    let db = Database::new().await;
    let session_id = session(&db.store, "wait-cancel").await;
    input(&db.store, session_id, "first").await;
    let first = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let cancel_id = WaitId::new();
    let expired_id = WaitId::new();
    let mut create = empty_outcome("{}");
    create.waits = vec![
        NewWait {
            id: cancel_id,
            mode: WaitMode::External,
            payload: raw("{}"),
            response_schema: None,
            expires_at: None,
        },
        NewWait {
            id: expired_id,
            mode: WaitMode::Either,
            payload: raw("{}"),
            response_schema: None,
            expires_at: Some(Utc::now() - ChronoDuration::seconds(1)),
        },
    ];
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&first), create)
        .await
        .unwrap();
    input(&db.store, session_id, "second").await;
    let second = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let mut cancel = empty_outcome("{}");
    cancel.status = Some(SessionStatus::Cancelled);
    cancel.cancelled_waits = vec![cancel_id, expired_id];
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&second), cancel)
        .await
        .unwrap();

    assert_eq!(
        db.store.wait(cancel_id).await.unwrap().status,
        WaitStatus::Cancelled
    );
    assert_eq!(
        db.store.wait(expired_id).await.unwrap().status,
        WaitStatus::Expired
    );
    let rows = sqlx::query_as::<_, (i64, String)>(
        "SELECT sequence,payload::text FROM session_events WHERE wait_id IS NOT NULL \
         AND session_id=$1 ORDER BY sequence",
    )
    .bind(session_id.0)
    .fetch_all(db.store.pool())
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 3);
    assert_eq!(rows[1].0, 4);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rows[0].1).unwrap()["reason"],
        "cancelled"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rows[1].1).unwrap()["reason"],
        "expired"
    );
    assert!(matches!(
        db.store
            .resolve_wait(cancel_id, "reply", raw("{}").as_ref())
            .await,
        Err(StoreError::WaitNotResolvable)
    ));
    assert!(db.store.expire_wait(expired_id).await.unwrap().is_none());
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn newly_created_wait_can_be_cancelled_in_the_same_outcome() {
    let db = Database::new().await;
    let session_id = session(&db.store, "create-cancel").await;
    input(&db.store, session_id, "message").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let wait_id = WaitId::new();
    let mut outcome = empty_outcome("{}");
    outcome.waits.push(NewWait {
        id: wait_id,
        mode: WaitMode::External,
        payload: raw("{}"),
        response_schema: None,
        expires_at: None,
    });
    outcome.cancelled_waits.push(wait_id);
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&claim), outcome)
        .await
        .unwrap();
    assert_eq!(
        db.store.wait(wait_id).await.unwrap().status,
        WaitStatus::Cancelled
    );
    let event = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.event.sequence.0, 2);
    assert_eq!(event.event.event_type, EventType::WaitResumed);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(event.event.payload.get()).unwrap()["reason"],
        "cancelled"
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn cross_session_wait_cancellation_rolls_back_the_handler_outcome() {
    let db = Database::new().await;
    let owner = session(&db.store, "owner").await;
    input(&db.store, owner, "owner").await;
    let owner_claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let wait_id = WaitId::new();
    let mut create = empty_outcome("{}");
    create.waits.push(NewWait {
        id: wait_id,
        mode: WaitMode::External,
        payload: raw("{}"),
        response_schema: None,
        expires_at: None,
    });
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&owner_claim), create)
        .await
        .unwrap();
    let intruder = session(&db.store, "intruder").await;
    input(&db.store, intruder, "intruder").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let mut invalid = empty_outcome("{\"changed\":true}");
    invalid.cancelled_waits.push(wait_id);
    assert!(matches!(
        db.store
            .commit_handler_outcome(&HandlerClaim::from(&claim), invalid)
            .await,
        Err(StoreError::WaitNotFound(_))
    ));
    assert_eq!(db.store.session(intruder).await.unwrap().state_version.0, 0);
    assert_eq!(
        db.store.wait(wait_id).await.unwrap().status,
        WaitStatus::Pending
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn blocked_processing_retry_is_guarded_idempotent_and_resets_failure_budget() {
    let db = Database::new().await;
    let session_id = session(&db.store, "recovery").await;
    input(&db.store, session_id, "first").await;
    input(&db.store, session_id, "second").await;
    let first = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    db.store
        .record_handler_failure(
            &HandlerClaim::from(&first),
            json!({"code":"handler_failed"}),
            Utc::now(),
            true,
        )
        .await
        .unwrap();
    let blocked = db.store.session(session_id).await.unwrap();
    assert_eq!(blocked.processing_revision, 1);
    assert_eq!(blocked.blocked_event_id, Some(first.event.id));
    let request = raw(&format!(
        "{{\"expected_event_id\":\"{}\",\"expected_processing_revision\":1}}",
        first.event.id
    ));
    assert!(matches!(
        db.store
            .retry_blocked_event(session_id, first.event.id, 0, "stale", &request, None)
            .await,
        Err(StoreError::StaleProcessingRetry)
    ));
    let accepted = db
        .store
        .retry_blocked_event(
            session_id,
            first.event.id,
            1,
            "retry",
            &request,
            Some("operator retry"),
        )
        .await
        .unwrap();
    assert_eq!(accepted.processing_revision, 2);
    assert_eq!(
        db.store
            .handler_failure_count(first.event.id)
            .await
            .unwrap(),
        0
    );
    assert!(
        db.store
            .session(session_id)
            .await
            .unwrap()
            .processing_error
            .is_none()
    );
    let retried = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retried.event.id, first.event.id);
    assert_eq!(retried.attempt_number, 2);
    db.store
        .record_handler_failure(
            &HandlerClaim::from(&retried),
            json!({"code":"handler_failed"}),
            Utc::now(),
            true,
        )
        .await
        .unwrap();
    assert_eq!(
        db.store
            .handler_failure_count(first.event.id)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.store
            .session(session_id)
            .await
            .unwrap()
            .processing_revision,
        3
    );
    let duplicate = db
        .store
        .retry_blocked_event(
            session_id,
            first.event.id,
            1,
            "retry",
            &request,
            Some("operator retry"),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.processing_revision, accepted.processing_revision);
    assert_eq!(duplicate.retried_at, accepted.retried_at);
    assert!(matches!(
        db.store
            .retry_blocked_event(
                session_id,
                first.event.id,
                1,
                "retry",
                raw("{}").as_ref(),
                None
            )
            .await,
        Err(StoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        db.store
            .retry_blocked_event(session_id, first.event.id, 1, "stale-new", &request, None)
            .await,
        Err(StoreError::StaleProcessingRetry)
    ));
    let request2 = raw(&format!(
        "{{\"expected_event_id\":\"{}\",\"expected_processing_revision\":3}}",
        first.event.id
    ));
    db.store
        .retry_blocked_event(session_id, first.event.id, 3, "retry-two", &request2, None)
        .await
        .unwrap();
    let retried_again = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retried_again.event.id, first.event.id);
    db.store
        .commit_handler_outcome(
            &HandlerClaim::from(&retried_again),
            empty_outcome("{\"recovered\":true}"),
        )
        .await
        .unwrap();
    let next = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.event.sequence.0, 2);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops disposable databases"]
async fn external_reply_and_handler_wait_cancellation_have_one_winner() {
    let db = Database::new().await;
    let session_id = session(&db.store, "reply-cancel-race").await;
    input(&db.store, session_id, "first").await;
    let first = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let wait_id = WaitId::new();
    let mut create = empty_outcome("{}");
    create.waits.push(NewWait {
        id: wait_id,
        mode: WaitMode::External,
        payload: raw("{}"),
        response_schema: None,
        expires_at: None,
    });
    db.store
        .commit_handler_outcome(&HandlerClaim::from(&first), create)
        .await
        .unwrap();
    input(&db.store, session_id, "second").await;
    let second = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let mut cancel = empty_outcome("{}");
    cancel.cancelled_waits.push(wait_id);
    let response = raw("{}");
    let handler_claim = HandlerClaim::from(&second);
    let (committed, reply) = tokio::join!(
        db.store.commit_handler_outcome(&handler_claim, cancel),
        db.store.resolve_wait(wait_id, "reply", &response),
    );
    committed.unwrap();
    let status = db.store.wait(wait_id).await.unwrap().status;
    match status {
        WaitStatus::Cancelled => assert!(matches!(reply, Err(StoreError::WaitNotResolvable))),
        WaitStatus::Resolved => assert!(reply.unwrap().newly_resolved),
        other => panic!("unexpected wait status: {other:?}"),
    }
    let events: Vec<String> =
        sqlx::query_scalar("SELECT payload::text FROM session_events WHERE wait_id=$1")
            .bind(wait_id.0)
            .fetch_all(db.store.pool())
            .await
            .unwrap();
    assert_eq!(events.len(), 1);
    let expected = if status == WaitStatus::Cancelled {
        "cancelled"
    } else {
        "resolved"
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&events[0]).unwrap()["reason"],
        expected
    );
    db.close().await;
}
