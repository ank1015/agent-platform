use agent_contracts::{
    GatewayConnectionId, HarnessId, HarnessVersion, HistoryEntryId, OperationId, OperationKind,
    ProjectId, SessionId, SessionStatus, WaitId, WaitMode,
};
use agent_runtime::HarnessRegistry;
use agent_runtime::{RequestCleanupSettings, RequestCleanupWorker};
use agent_server::{ServerSettings, app, app_with_settings, serve_on_listener_with_shutdown};
use agent_store::{
    EventType, HandlerClaim, NewExternalEvent, NewHistoryEntry, NewOperation, NewSession, NewWait,
    OperationPhase, OperationStatus, OutcomeCommit, PoolConfig, Store,
};
use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
    response::Response,
};
use chrono::{Duration as ChronoDuration, Utc};
use http_body_util::BodyExt;
use serde_json::{Value, json, value::RawValue};
use sqlx::PgPool;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

const TOKEN: &str = "part9-test-token";

struct Database {
    admin_url: String,
    name: String,
    store: Store,
}
impl Database {
    async fn new() -> Self {
        let admin_url = std::env::var("TEST_DATABASE_URL").expect("requires TEST_DATABASE_URL");
        let admin = PgPool::connect(&admin_url).await.unwrap();
        let name = format!("agent_part9_test_{}", Uuid::new_v4().simple());
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

fn raw(x: &str) -> Box<RawValue> {
    RawValue::from_string(x.to_owned()).unwrap()
}

async fn session(store: &Store) -> SessionId {
    let id = SessionId::new();
    store
        .create_session(NewSession {
            id,
            project_id: ProjectId("part9".into()),
            harness_id: HarnessId("probe".into()),
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

async fn input(store: &Store, id: SessionId, key: &str) {
    store
        .enqueue_external_event(NewExternalEvent {
            session_id: id,
            event_type: EventType::UserMessage,
            payload: raw(
                r#"{"message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#,
            ),
            idempotency_key: Some(key.into()),
        })
        .await
        .unwrap();
}

async fn call(router: &Router, uri: &str) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn sse(router: &Router, uri: &str, last: Option<i64>) -> Response {
    let mut request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"));
    if let Some(last) = last {
        request = request.header("Last-Event-ID", last.to_string());
    }
    router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn history_inspection_updates_and_request_retention_are_application_ready() {
    let db = Database::new().await;
    let id = session(&db.store).await;
    let other = session(&db.store).await;
    let router = app(db.store.clone(), HarnessRegistry::new(), vec![TOKEN.into()]);
    input(&db.store, id, "first").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let event = claim.event.id;
    let operation = OperationId::new();
    let wait = WaitId::new();
    let first = HistoryEntryId::new();
    let second = HistoryEntryId::new();
    let before = db.store.update_cursor(id).await.unwrap().0;
    db.store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
            OutcomeCommit {
                state: raw(r#"{"done":true}"#),
                status: Some(SessionStatus::Waiting),
                history: vec![
                    NewHistoryEntry {
                        id: first,
                        message: raw(r#"{"role":"user","content":[{"type":"text","text":"one"}]}"#),
                    },
                    NewHistoryEntry {
                        id: second,
                        message: raw(
                            r#"{"role":"custom","tag":"harness.note","data":{"note":"two"}}"#,
                        ),
                    },
                ],
                operations: vec![NewOperation {
                    id: operation,
                    kind: OperationKind::Llm,
                    gateway_connection_id: Some(GatewayConnectionId("llm-test".into())),
                    target_operation_id: None,
                    previous_operation_id: None,
                    request: raw("{}"),
                }],
                waits: vec![NewWait {
                    id: wait,
                    mode: WaitMode::External,
                    payload: raw("{}"),
                    response_schema: None,
                    expires_at: None,
                }],
                cancelled_waits: vec![],
                progress: vec![json!({"phase":"thinking"})],
            },
        )
        .await
        .unwrap();

    let path = format!("/v1/sessions/{id}");
    let (status, detail) = call(&router, &path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["history_through_sequence"], 2);
    assert!(detail["update_through_sequence"].as_i64().unwrap() > before);

    let history = format!("{path}/history?after_sequence=0&limit=1");
    let (status, page) = call(&router, &history).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["data"].as_array().unwrap().len(), 1);
    assert_eq!(page["through_sequence"], 2);
    assert_eq!(page["has_more"], true);
    let (status, next) = call(
        &router,
        &format!("{path}/history?after_sequence=1&through_sequence=2"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(next["data"][0]["id"], second.to_string());
    let (status, entry) = call(&router, &format!("{path}/history/{first}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(entry["source_event_id"], event.to_string());
    let (status, by_event) =
        call(&router, &format!("{path}/history?source_event_id={event}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(by_event["data"].as_array().unwrap().len(), 2);
    assert_eq!(
        call(&router, &format!("/v1/sessions/{other}/history/{first}"))
            .await
            .0,
        StatusCode::NOT_FOUND
    );

    let (status, ops) = call(&router, &format!("{path}/operations")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ops["data"][0]["request_retention"]["available"], true);
    assert!(ops["data"][0].get("result").is_none());
    let (status, request) = call(&router, &format!("{path}/operations/{operation}/request")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(request["request"], json!({}));
    let (status, ops_detail) = call(&router, &format!("{path}/operations/{operation}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ops_detail["source_event_id"], event.to_string());
    assert_eq!(
        call(
            &router,
            &format!("/v1/sessions/{other}/operations/{operation}")
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    let (status, attempts) =
        call(&router, &format!("{path}/operations/{operation}/attempts")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(attempts["data"].as_array().unwrap().len(), 0);
    let operation_claim = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation_claim.operation.id, operation);
    let (_, attempts) = call(&router, &format!("{path}/operations/{operation}/attempts")).await;
    assert_eq!(attempts["data"][0]["phase"], "submission");
    assert_eq!(attempts["data"][0]["status"], "running");
    db.store
        .defer_operation(&operation_claim, Utc::now())
        .await
        .unwrap();
    let (_, attempts) = call(
        &router,
        &format!("{path}/operations/{operation}/attempts?after_attempt=0"),
    )
    .await;
    assert_eq!(attempts["data"][0]["status"], "succeeded");
    assert_eq!(
        call(
            &router,
            &format!("{path}/operations/{operation}/attempts?after_attempt=1")
        )
        .await
        .1["data"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    let (status, events) = call(&router, &format!("{path}/events?after_sequence=0")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(events["data"][0]["status"], "handled");
    assert!(events["data"][0].get("payload").is_none());
    let (status, event_detail) = call(&router, &format!("{path}/events/{event}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(event_detail["payload"]["message"]["role"], "user");
    let (status, attempts) = call(&router, &format!("{path}/events/{event}/attempts")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(attempts["data"][0]["status"], "committed");

    let (status, updates) = call(
        &router,
        &format!("{path}/updates?after_sequence={before}&limit=2"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updates["has_more"], true);
    let through = updates["through_sequence"].as_i64().unwrap();
    let (status, all) = call(
        &router,
        &format!("{path}/updates?after_sequence={before}&through_sequence={through}&limit=100"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let sequence: Vec<i64> = all["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["sequence"].as_i64().unwrap())
        .collect();
    assert!(sequence.windows(2).all(|x| x[0] + 1 == x[1]));
    assert!(
        all["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["kind"] == "harness.progress"
                && x["payload"]["phase"] == "thinking"
                && x["source_event_id"] == event.to_string()
                && x["schema_version"] == 1)
    );

    let submission = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let completed = db
        .store
        .complete_operation(
            &submission,
            OperationStatus::Failed,
            None,
            Some(json!({"code":"rejected"})),
            Some(Utc::now() - ChronoDuration::seconds(1)),
        )
        .await
        .unwrap();
    let (_, completed_detail) = call(&router, &format!("{path}/operations/{operation}")).await;
    assert_eq!(completed_detail["status"], "failed");
    assert_eq!(completed_detail["error"]["code"], "rejected");
    let (_, after_completion) =
        call(&router, &format!("{path}/updates?after_sequence={through}")).await;
    assert!(
        after_completion["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["kind"] == "operation.changed")
    );
    assert!(
        after_completion["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["kind"] == "event.changed"
                && x["payload"]["event_id"] == completed.id.to_string())
    );
    let before_cleanup = db.store.update_cursor(id).await.unwrap().0;
    assert_eq!(
        db.store
            .delete_expired_operation_requests(10)
            .await
            .unwrap(),
        1
    );
    let cleanup_updates = db
        .store
        .updates(id, before_cleanup, None, 10)
        .await
        .unwrap();
    assert_eq!(cleanup_updates.updates.len(), 1);
    assert_eq!(cleanup_updates.updates[0].kind, "operation.changed");
    let (status, request) = call(&router, &format!("{path}/operations/{operation}/request")).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(request["error"]["code"], "request_payload_expired");
    let (_, list) = call(&router, &format!("{path}/operations")).await;
    assert_eq!(list["data"][0]["request_retention"]["available"], false);
    assert!(list["data"][0]["request_retention"]["removed_at"].is_string());

    let final_cursor = db.store.update_cursor(id).await.unwrap().0;
    let removed = db
        .store
        .prune_updates(Utc::now() + ChronoDuration::seconds(1), 10)
        .await
        .unwrap();
    assert!(removed > 0);
    let (status, expired) = call(&router, &format!("{path}/updates?after_sequence=0")).await;
    assert_eq!(status, StatusCode::GONE);
    assert_eq!(expired["error"]["code"], "resnapshot_required");
    let floor = db.store.update_cursor(id).await.unwrap().1;
    assert_eq!(floor, final_cursor + 1);
    assert_eq!(
        call(
            &router,
            &format!("{path}/updates?after_sequence={}", floor - 1)
        )
        .await
        .0,
        StatusCode::OK
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn sse_replays_after_last_event_id_and_rejects_expired_cursors() {
    let db = Database::new().await;
    let id = session(&db.store).await;
    let router = app(db.store.clone(), HarnessRegistry::new(), vec![TOKEN.into()]);
    input(&db.store, id, "first").await;
    input(&db.store, id, "second").await;
    let path = format!("/v1/sessions/{id}/updates/stream");
    let first = sse(&router, &path, None).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()[header::CONTENT_TYPE], "text/event-stream");
    let mut body = first.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
    assert!(text.contains("id: 1"));
    drop(body);
    let second = sse(&router, &path, Some(1)).await;
    assert_eq!(second.status(), StatusCode::OK);
    let mut body = second.into_body();
    let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
    assert!(text.contains("id: 2"));
    input(&db.store, id, "third").await;
    let frame = tokio::time::timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let text = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
    assert!(text.contains("id: 3"));
    drop(body);
    db.store
        .prune_updates(Utc::now() + ChronoDuration::seconds(1), 10)
        .await
        .unwrap();
    assert_eq!(
        sse(&router, &path, Some(0)).await.status(),
        StatusCode::GONE
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn rejected_outcome_writes_no_application_update() {
    let db = Database::new().await;
    let id = session(&db.store).await;
    input(&db.store, id, "first").await;
    let claim = db
        .store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let before = db.store.update_cursor(id).await.unwrap().0;
    let result = db
        .store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
            OutcomeCommit {
                state: raw(r#"{"changed":true}"#),
                status: None,
                history: vec![NewHistoryEntry {
                    id: HistoryEntryId::new(),
                    message: raw("{}"),
                }],
                operations: vec![],
                waits: vec![],
                cancelled_waits: vec![],
                progress: vec![json!({"phase":"invalid"})],
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(db.store.update_cursor(id).await.unwrap().0, before);
    assert_eq!(db.store.session(id).await.unwrap().state_version.0, 0);
    db.close().await;
}

async fn pending_operation(store: &Store) -> (SessionId, OperationId) {
    let id = session(store).await;
    input(store, id, "first").await;
    let claim = store
        .claim_next_event(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let operation = OperationId::new();
    store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
            OutcomeCommit {
                state: raw("{}"),
                status: Some(SessionStatus::Running),
                history: vec![],
                operations: vec![NewOperation {
                    id: operation,
                    kind: OperationKind::Llm,
                    gateway_connection_id: Some(GatewayConnectionId("test".into())),
                    target_operation_id: None,
                    previous_operation_id: None,
                    request: raw("{}"),
                }],
                waits: vec![],
                cancelled_waits: vec![],
                progress: vec![],
            },
        )
        .await
        .unwrap();
    (id, operation)
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn submission_retry_and_deferral_publish_operation_changes() {
    let db = Database::new().await;
    let (id, operation) = pending_operation(&db.store).await;
    let before_claim = db.store.update_cursor(id).await.unwrap().0;
    let claim = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.operation.id, operation);
    assert_eq!(
        db.store.operation(operation).await.unwrap().status,
        OperationStatus::Submitting
    );
    assert_eq!(
        db.store
            .updates(id, before_claim, None, 10)
            .await
            .unwrap()
            .updates
            .len(),
        1
    );

    let before_retry = db.store.update_cursor(id).await.unwrap().0;
    db.store
        .record_operation_retry(
            &claim,
            false,
            Utc::now() - ChronoDuration::seconds(1),
            Some(503),
            json!({"code":"gateway_unavailable"}),
        )
        .await
        .unwrap();
    let retry = db.store.updates(id, before_retry, None, 10).await.unwrap();
    assert_eq!(retry.updates.len(), 1);
    assert_eq!(retry.updates[0].kind, "operation.changed");

    let before_second_claim = db.store.update_cursor(id).await.unwrap().0;
    let second = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.store
            .updates(id, before_second_claim, None, 10)
            .await
            .unwrap()
            .updates
            .len(),
        1
    );
    let before_defer = db.store.update_cursor(id).await.unwrap().0;
    db.store
        .defer_operation(&second, Utc::now() - ChronoDuration::seconds(1))
        .await
        .unwrap();
    assert_eq!(
        db.store
            .updates(id, before_defer, None, 10)
            .await
            .unwrap()
            .updates
            .len(),
        1
    );
    let stale_cursor = db.store.update_cursor(id).await.unwrap().0;
    assert!(db.store.defer_operation(&second, Utc::now()).await.is_err());
    assert_eq!(db.store.update_cursor(id).await.unwrap().0, stale_cursor);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn concurrent_operation_retry_and_input_allocate_contiguous_updates() {
    let db = Database::new().await;
    let (id, operation) = pending_operation(&db.store).await;
    let claim = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    let before = db.store.update_cursor(id).await.unwrap().0;
    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM sessions WHERE id=$1 FOR UPDATE")
        .bind(id.0)
        .execute(&mut *lock)
        .await
        .unwrap();
    let retry_store = db.store.clone();
    let retry = tokio::spawn(async move {
        retry_store
            .record_operation_retry(
                &claim,
                false,
                Utc::now() + ChronoDuration::seconds(60),
                Some(503),
                json!({"code":"temporary_failure"}),
            )
            .await
    });
    let input_store = db.store.clone();
    let message = tokio::spawn(async move { input(&input_store, id, "during-retry").await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), retry)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), message)
        .await
        .unwrap()
        .unwrap();
    let changes = db
        .store
        .updates(id, before, None, 10)
        .await
        .unwrap()
        .updates;
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].sequence + 1, changes[1].sequence);
    assert!(changes.iter().any(|x| x.kind == "operation.changed"));
    assert!(changes.iter().any(|x| x.kind == "event.changed"));
    assert_eq!(
        db.store.operation(operation).await.unwrap().status,
        OperationStatus::Pending
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn sse_draining_interrupts_blocked_reads_and_buffered_replay() {
    let db = Database::new().await;
    let id = session(&db.store).await;
    let (router, readiness) = agent_server::app_with_settings(
        db.store.clone(),
        HarnessRegistry::new(),
        vec![TOKEN.into()],
        agent_server::ServerSettings::default(),
    );
    let response = sse(&router, &format!("/v1/sessions/{id}/updates/stream"), None).await;
    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE session_updates IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let mut reader = tokio::spawn(async move {
        let mut body = response.into_body();
        body.frame().await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() \
                 AND wait_event_type='Lock' AND query LIKE 'SELECT session_id,sequence,schema_version%')",
            )
            .fetch_one(db.store.pool())
            .await
            .unwrap();
            if blocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    readiness.begin_draining();
    assert!(
        tokio::time::timeout(Duration::from_millis(400), &mut reader)
            .await
            .is_ok()
    );
    lock.rollback().await.unwrap();

    let other = session(&db.store).await;
    input(&db.store, other, "one").await;
    input(&db.store, other, "two").await;
    let (router, readiness) = agent_server::app_with_settings(
        db.store.clone(),
        HarnessRegistry::new(),
        vec![TOKEN.into()],
        agent_server::ServerSettings::default(),
    );
    let response = sse(
        &router,
        &format!("/v1/sessions/{other}/updates/stream"),
        None,
    )
    .await;
    let mut body = response.into_body();
    assert!(body.frame().await.is_some());
    readiness.begin_draining();
    assert!(body.frame().await.is_none());
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn create_and_patch_bootstrap_cursors_replay_every_later_change() {
    let db = Database::new().await;
    let id = SessionId::new();
    let mut registry = HarnessRegistry::new();
    registry.register(harness_fixture::FixtureHarness).unwrap();
    let router = app(db.store.clone(), registry, vec![TOKEN.into()]);
    let creation = json!({
        "id":id,
        "project_id":"part9",
        "harness_id":"fixture",
        "harness_version":"1",
        "configuration":{"connection":"test","account_id":"test","model_id":"test"}
    });
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/sessions")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(creation.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let created: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(created["update_through_sequence"], 0);
    input(&db.store, id, "after-create").await;
    let (_, replay) = call(
        &router,
        &format!("/v1/sessions/{id}/updates?after_sequence=0"),
    )
    .await;
    assert!(
        replay["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["kind"] == "event.changed")
    );

    let before_patch = db.store.update_cursor(id).await.unwrap().0;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("/v1/sessions/{id}"))
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"name":"renamed"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let patched: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(patched["name"], "renamed");
    assert_eq!(patched["update_through_sequence"], before_patch);
    input(&db.store, id, "after-patch").await;
    let (_, replay) = call(
        &router,
        &format!("/v1/sessions/{id}/updates?after_sequence={before_patch}"),
    )
    .await;
    assert!(
        replay["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["kind"] == "session.changed")
    );
    assert!(
        replay["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["kind"] == "event.changed")
    );
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn concurrent_patch_snapshot_cannot_skip_a_message_update() {
    let db = Database::new().await;
    let id = session(&db.store).await;
    let router = app(db.store.clone(), HarnessRegistry::new(), vec![TOKEN.into()]);
    let before = db.store.update_cursor(id).await.unwrap().0;
    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM sessions WHERE id=$1 FOR UPDATE")
        .bind(id.0)
        .execute(&mut *lock)
        .await
        .unwrap();
    let patch_router = router.clone();
    let patch = tokio::spawn(async move {
        patch_router
            .oneshot(
                Request::builder()
                    .method(Method::PATCH)
                    .uri(format!("/v1/sessions/{id}"))
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"name":"concurrent"}"#))
                    .unwrap(),
            )
            .await
            .unwrap()
    });
    blocked_sql(db.store.pool(), "UPDATE sessions SET").await;
    let input_store = db.store.clone();
    let message = tokio::spawn(async move { input(&input_store, id, "concurrent-message").await });
    lock.rollback().await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(2), patch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let snapshot: Value = serde_json::from_slice(&body).unwrap();
    message.await.unwrap();
    let cursor = snapshot["update_through_sequence"].as_i64().unwrap();
    assert_eq!(cursor, before);
    let (_, replay) = call(
        &router,
        &format!("/v1/sessions/{id}/updates?after_sequence={cursor}"),
    )
    .await;
    assert!(
        replay["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["kind"] == "event.changed")
    );
    db.close().await;
}

async fn blocked_sql(pool: &PgPool, prefix: &str) -> String {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let query: Option<String> = sqlx::query_scalar(
                "SELECT query FROM pg_stat_activity WHERE datname=current_database() \
                 AND wait_event_type='Lock' AND query LIKE $1 LIMIT 1",
            )
            .bind(format!("{prefix}%"))
            .fetch_optional(pool)
            .await
            .unwrap();
            if let Some(query) = query {
                return query;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn inspection_ownership_checks_and_history_cursor_read_only_identity_columns() {
    let db = Database::new().await;
    let (id, operation) = pending_operation(&db.store).await;
    let event = db.store.events(id, 0, 10, None).await.unwrap()[0].id;

    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE session_events IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let store = db.store.clone();
    let task = tokio::spawn(async move { store.handler_attempts(id, event, 0, 10).await });
    let query = blocked_sql(
        db.store.pool(),
        "SELECT EXISTS(SELECT 1 FROM session_events",
    )
    .await;
    assert!(!query.contains("payload"));
    lock.rollback().await.unwrap();
    task.await.unwrap().unwrap();

    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE session_operations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let store = db.store.clone();
    let task =
        tokio::spawn(async move { store.operation_request_for_session(id, operation).await });
    let query = blocked_sql(
        db.store.pool(),
        "SELECT EXISTS(SELECT 1 FROM session_operations",
    )
    .await;
    assert!(!query.contains("result"));
    lock.rollback().await.unwrap();
    assert!(task.await.unwrap().unwrap().is_some());

    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE session_operations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let store = db.store.clone();
    let task = tokio::spawn(async move { store.operation_attempts(id, operation, 0, 10).await });
    let query = blocked_sql(
        db.store.pool(),
        "SELECT EXISTS(SELECT 1 FROM session_operations",
    )
    .await;
    assert!(!query.contains("result"));
    lock.rollback().await.unwrap();
    task.await.unwrap().unwrap();

    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let store = db.store.clone();
    let task = tokio::spawn(async move { store.history_cursor(id).await });
    let query = blocked_sql(
        db.store.pool(),
        "SELECT next_history_sequence-1 FROM sessions",
    )
    .await;
    assert!(!query.contains("configuration"));
    assert!(!query.contains("state"));
    lock.rollback().await.unwrap();
    task.await.unwrap().unwrap();
    db.close().await;
}

async fn settle_due(store: &Store, operation: OperationId) {
    let claim = store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.operation.id, operation);
    store
        .complete_operation(
            &claim,
            OperationStatus::Failed,
            None,
            Some(json!({"code":"test_rejection"})),
            Some(Utc::now() - ChronoDuration::seconds(1)),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn request_cleanup_skips_locked_and_uncertain_work_then_revisits_it() {
    let db = Database::new().await;
    let (old_session, old_operation) = pending_operation(&db.store).await;
    let (_, later_operation) = pending_operation(&db.store).await;
    let (_, uncertain_operation) = pending_operation(&db.store).await;
    settle_due(&db.store, old_operation).await;
    settle_due(&db.store, later_operation).await;
    let uncertain = db
        .store
        .claim_operation(OperationPhase::Submission, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(uncertain.operation.id, uncertain_operation);
    sqlx::query("UPDATE session_operation_requests SET expires_at=clock_timestamp()-interval '1 second' WHERE operation_id=$1")
        .bind(uncertain_operation.0)
        .execute(db.store.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE session_operation_requests SET expires_at=clock_timestamp()-interval '20 seconds' WHERE operation_id=$1")
        .bind(old_operation.0)
        .execute(db.store.pool())
        .await
        .unwrap();
    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM sessions WHERE id=$1 FOR UPDATE")
        .bind(old_session.0)
        .execute(&mut *lock)
        .await
        .unwrap();
    let cleanup = RequestCleanupWorker::new(
        db.store.clone(),
        RequestCleanupSettings {
            batch_size: 1,
            ..RequestCleanupSettings::default()
        },
    )
    .unwrap();
    let candidates = db
        .store
        .expired_operation_request_candidates(None, 10)
        .await
        .unwrap();
    assert_eq!(
        candidates.iter().map(|(_, op)| *op).collect::<Vec<_>>(),
        vec![old_operation, later_operation]
    );
    assert_ne!(
        db.store.operation(old_operation).await.unwrap().session_id,
        db.store
            .operation(later_operation)
            .await
            .unwrap()
            .session_id
    );
    assert_eq!(cleanup.process_batch().await.unwrap(), 1);
    assert_eq!(
        db.store
            .expired_operation_request_candidates(Some(candidates[0]), 1)
            .await
            .unwrap()[0]
            .1,
        later_operation
    );
    assert_eq!(cleanup.process_batch().await.unwrap(), 1);
    assert!(
        db.store
            .operation_request(later_operation)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db.store
            .operation_request(old_operation)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        db.store
            .operation_request(uncertain_operation)
            .await
            .unwrap()
            .is_some()
    );
    lock.rollback().await.unwrap();
    assert_eq!(cleanup.process_batch().await.unwrap(), 0);
    assert_eq!(cleanup.process_batch().await.unwrap(), 1);
    assert!(
        db.store
            .operation_request(old_operation)
            .await
            .unwrap()
            .is_none()
    );
    let restarted =
        RequestCleanupWorker::new(db.store.clone(), RequestCleanupSettings::default()).unwrap();
    assert_eq!(restarted.process_batch().await.unwrap(), 0);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn request_cleanup_shutdown_interrupts_a_blocked_scan() {
    let db = Database::new().await;
    let (_, operation) = pending_operation(&db.store).await;
    settle_due(&db.store, operation).await;
    let mut lock = db.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE session_operation_requests IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();

    let worker = RequestCleanupWorker::new(
        db.store.clone(),
        RequestCleanupSettings {
            poll_interval: Duration::from_millis(10),
            ..RequestCleanupSettings::default()
        },
    )
    .unwrap();
    let stop = CancellationToken::new();
    let worker_stop = stop.clone();
    let task = tokio::spawn(async move { worker.run(worker_stop).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    stop.cancel();
    tokio::time::timeout(Duration::from_millis(250), task)
        .await
        .expect("cleanup should stop without waiting for the blocked scan")
        .unwrap()
        .unwrap();
    lock.rollback().await.unwrap();
    assert!(
        db.store
            .operation_request(operation)
            .await
            .unwrap()
            .is_some()
    );

    let restarted =
        RequestCleanupWorker::new(db.store.clone(), RequestCleanupSettings::default()).unwrap();
    assert_eq!(restarted.process_batch().await.unwrap(), 1);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn server_runs_request_cleanup_and_exposes_bounded_authenticated_metrics() {
    let db = Database::new().await;
    let (id, operation) = pending_operation(&db.store).await;
    settle_due(&db.store, operation).await;
    let mut settings = ServerSettings::default();
    settings.scheduler.enabled = false;
    settings.dispatcher.enabled = false;
    settings.completion.enabled = false;
    settings.wait_expiration.enabled = false;
    settings.request_cleanup.poll_interval = Duration::from_millis(10);
    settings.max_concurrent_sse = 1;

    let (router, _) = app_with_settings(
        db.store.clone(),
        HarnessRegistry::new(),
        vec![TOKEN.into()],
        settings.clone(),
    );
    let unauthenticated = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let path = format!("/v1/sessions/{id}/updates/stream");
    let first = sse(&router, &path, None).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        sse(&router, &path, None).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    let metrics = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/metrics")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metrics.status(), StatusCode::OK);
    let body = metrics.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("agent_sse_subscriptions 1"));
    assert!(body.contains("agent_operation_requests_cleanup_ready 1"));
    drop(first);
    assert_eq!(sse(&router, &path, None).await.status(), StatusCode::OK);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stop = CancellationToken::new();
    let worker_stop = stop.clone();
    let store = db.store.clone();
    let task = tokio::spawn(async move {
        serve_on_listener_with_shutdown(
            listener,
            store,
            HarnessRegistry::new(),
            vec![TOKEN.into()],
            settings,
            worker_stop.cancelled_owned(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if db
                .store
                .operation_request(operation)
                .await
                .unwrap()
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    task.await.unwrap().unwrap();
    assert!(
        db.store
            .operation_request_status(id, operation)
            .await
            .unwrap()
            .2
            .is_some()
    );
    db.close().await;
}
