use agent_contracts::{
    HandlerError, HandlerOutcome, Harness, HarnessContext, HarnessDescription, HarnessId,
    HarnessVersion, InitializationError, ProjectId, SessionEvent, SessionId, ValidationError,
    WaitId, WaitMode,
};
use agent_gateways::{GatewayCallbackConfig, GatewayKind};
use agent_runtime::HarnessRegistry;
use agent_server::{
    ServerError, ServerSettings, app, app_with_settings, build_registry,
    serve_on_listener_with_shutdown,
};
use agent_store::{
    EventType, HandlerClaim, NewExternalEvent, NewSession, NewWait, OutcomeCommit, PoolConfig,
    Store, WaitStatus,
};
use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
    response::Response,
};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use sha2::Sha256;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

const TOKEN: &str = "test-service-token";

#[derive(Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct TestConfig {
    #[schemars(with = "serde_json::Value")]
    native: Box<RawValue>,
}

#[derive(Deserialize, Serialize)]
struct TestState {
    native: Box<RawValue>,
}

struct TestHarness {
    version: &'static str,
}

#[derive(Deserialize, JsonSchema, Serialize)]
struct ShutdownConfig {
    cooperative: bool,
}

#[derive(Deserialize, Serialize)]
struct ShutdownState {
    stopped: bool,
}

struct ShutdownHarness;

#[async_trait]
impl Harness for ShutdownHarness {
    type Config = ShutdownConfig;
    type State = ShutdownState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("shutdown".into()),
            version: HarnessVersion("1".into()),
            name: "Shutdown fixture".into(),
            description: "Active server shutdown fixture".into(),
        }
    }

    fn initialize(&self, _config: &ShutdownConfig) -> Result<ShutdownState, InitializationError> {
        Ok(ShutdownState { stopped: false })
    }

    async fn handle(
        &self,
        config: &ShutdownConfig,
        mut state: ShutdownState,
        _event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<ShutdownState>, HandlerError> {
        if config.cooperative {
            while !context.stop_requested() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            state.stopped = true;
            Ok(agent_contracts::OutcomeBuilder::new(state).finish())
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            Ok(agent_contracts::OutcomeBuilder::new(state).finish())
        }
    }
}

#[async_trait]
impl Harness for TestHarness {
    type Config = TestConfig;
    type State = TestState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("fixture-api".into()),
            version: HarnessVersion(self.version.into()),
            name: "Fixture API".into(),
            description: "Server contract fixture".into(),
        }
    }

    fn validate_config(&self, config: &TestConfig) -> Result<(), ValidationError> {
        if config.native.get() == "null" {
            return Err(ValidationError("native cannot be null".into()));
        }
        Ok(())
    }

    fn initialize(&self, config: &TestConfig) -> Result<TestState, InitializationError> {
        if config.native.get() == r#""fail""# {
            return Err(InitializationError("requested failure".into()));
        }
        Ok(TestState {
            native: config.native.clone(),
        })
    }

    async fn handle(
        &self,
        _config: &TestConfig,
        state: TestState,
        _event: SessionEvent,
        _context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<TestState>, HandlerError> {
        Ok(agent_contracts::OutcomeBuilder::new(state).finish())
    }
}

fn registry() -> HarnessRegistry {
    let mut registry = HarnessRegistry::new();
    registry.register(TestHarness { version: "1" }).unwrap();
    registry.register(TestHarness { version: "2" }).unwrap();
    registry
}

fn lazy_store() -> Store {
    Store::from_pool(
        PgPoolOptions::new()
            .connect_lazy("postgresql://localhost/agent_server_unused")
            .unwrap(),
    )
}

async fn call(
    router: &Router,
    method: Method,
    uri: &str,
    body: Option<&str>,
    token: Option<&str>,
) -> (StatusCode, String) {
    let response = call_response(router, method, uri, body, token, true).await;
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

async fn call_response(
    router: &Router,
    method: Method,
    uri: &str,
    body: Option<&str>,
    token: Option<&str>,
    json_content_type: bool,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if body.is_some() && json_content_type {
        request = request.header(header::CONTENT_TYPE, "application/json");
    }
    router
        .clone()
        .oneshot(
            request
                .body(Body::from(body.unwrap_or_default().to_owned()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn call_idempotent(
    router: &Router,
    method: Method,
    uri: &str,
    body: &str,
    key: &str,
) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .header("idempotency-key", key)
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn call_llm_callback(
    router: &Router,
    connection: &str,
    event_id: Uuid,
    timestamp: &str,
    signature: &str,
    body: &str,
) -> StatusCode {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/v1/callbacks/llm/{connection}"))
                .header("x-llm-gateway-event-id", event_id.to_string())
                .header("x-llm-gateway-timestamp", timestamp)
                .header("x-llm-gateway-signature", signature)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

fn callback_signature(secret: &str, timestamp: &str, event_id: Uuid, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{timestamp}.{event_id}.{body}").as_bytes());
    format!("v1={}", hex::encode(mac.finalize().into_bytes()))
}

async fn assert_error(response: Response, status: StatusCode, code: &str) {
    assert_eq!(response.status(), status);
    let header_request_id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], code);
    assert_eq!(body["error"]["request_id"], header_request_id);
}

#[tokio::test]
async fn authentication_and_discovery_do_not_require_a_database() {
    let router = app(
        lazy_store(),
        registry(),
        vec![TOKEN.into(), "rotated-token".into()],
    );
    assert_eq!(
        call(&router, Method::GET, "/healthz", None, None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&router, Method::GET, "/v1/harnesses", None, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        call(&router, Method::GET, "/v1/not-a-route", None, None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let (status, body) = call(
        &router,
        Method::GET,
        "/v1/harnesses/fixture-api/versions",
        None,
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["data"][0]["version"], "1");
    assert_eq!(body["data"][1]["version"], "2");
    let (status, body) = call(
        &router,
        Method::GET,
        "/v1/harnesses/fixture-api/versions/1",
        None,
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(serde_json::from_str::<Value>(&body).unwrap()["configuration_schema"].is_object());

    assert_eq!(
        call(
            &router,
            Method::GET,
            "/v1/harnesses",
            None,
            Some("rotated-token")
        )
        .await
        .0,
        StatusCode::OK
    );
    assert!(build_registry().unwrap().descriptions().is_empty());
}

#[tokio::test]
async fn errors_body_limits_and_draining_follow_the_http_contract() {
    let (router, readiness) = app_with_settings(
        lazy_store(),
        registry(),
        vec![TOKEN.into()],
        ServerSettings {
            max_body_bytes: 64,
            ..ServerSettings::default()
        },
    );

    let unauthorized = call_response(&router, Method::GET, "/v1/harnesses", None, None, true).await;
    assert_eq!(unauthorized.headers()[header::WWW_AUTHENTICATE], "Bearer");
    assert_error(unauthorized, StatusCode::UNAUTHORIZED, "unauthorized").await;

    let method = call_response(
        &router,
        Method::DELETE,
        "/v1/sessions",
        None,
        Some(TOKEN),
        true,
    )
    .await;
    assert!(method.headers().contains_key(header::ALLOW));
    assert_error(method, StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed").await;

    assert_error(
        call_response(
            &router,
            Method::GET,
            "/v1/not-a-route",
            None,
            Some(TOKEN),
            true,
        )
        .await,
        StatusCode::NOT_FOUND,
        "route_not_found",
    )
    .await;
    assert_error(
        call_response(
            &router,
            Method::GET,
            "/v1/sessions/not-a-uuid",
            None,
            Some(TOKEN),
            true,
        )
        .await,
        StatusCode::BAD_REQUEST,
        "invalid_request",
    )
    .await;
    assert_error(
        call_response(
            &router,
            Method::GET,
            "/v1/sessions?status=unknown",
            None,
            Some(TOKEN),
            true,
        )
        .await,
        StatusCode::BAD_REQUEST,
        "invalid_request",
    )
    .await;
    assert_error(
        call_response(
            &router,
            Method::POST,
            "/v1/sessions",
            Some("{}"),
            Some(TOKEN),
            false,
        )
        .await,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported_media_type",
    )
    .await;
    assert_error(
        call_response(
            &router,
            Method::POST,
            "/v1/sessions",
            Some(&"x".repeat(65)),
            Some(TOKEN),
            true,
        )
        .await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    )
    .await;

    readiness.begin_draining();
    assert_error(
        call_response(&router, Method::GET, "/readyz", None, None, true).await,
        StatusCode::SERVICE_UNAVAILABLE,
        "not_ready",
    )
    .await;
    assert_error(
        call_response(
            &router,
            Method::GET,
            "/v1/harnesses",
            None,
            Some(TOKEN),
            true,
        )
        .await,
        StatusCode::SERVICE_UNAVAILABLE,
        "shutting_down",
    )
    .await;
}

struct Database {
    admin_url: String,
    url: String,
    name: String,
    store: Store,
}

impl Database {
    async fn new() -> Self {
        let admin_url = std::env::var("TEST_DATABASE_URL").expect(
            "TEST_DATABASE_URL must identify PostgreSQL with permission to create databases",
        );
        let admin = PgPool::connect(&admin_url).await.unwrap();
        let name = format!("agent_server_test_{}", Uuid::new_v4().simple());
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
            url: url.to_string(),
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

fn raw_json(value: &str) -> Box<RawValue> {
    RawValue::from_string(value.to_owned()).unwrap()
}

async fn stored_session(store: &Store, project_id: &str) -> SessionId {
    let id = SessionId::new();
    store
        .create_session(NewSession {
            id,
            project_id: ProjectId(project_id.into()),
            harness_id: HarnessId("fixture-api".into()),
            harness_version: HarnessVersion("1".into()),
            configuration: raw_json(r#"{"native":"fixed"}"#),
            state: raw_json(r#"{"step":0}"#),
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    id
}

async fn active_shutdown_session(store: &Store, cooperative: bool) -> SessionId {
    let id = SessionId::new();
    store
        .create_session(NewSession {
            id,
            project_id: ProjectId("shutdown".into()),
            harness_id: HarnessId("shutdown".into()),
            harness_version: HarnessVersion("1".into()),
            configuration: serde_json::value::to_raw_value(&json!({
                "cooperative": cooperative
            }))
            .unwrap(),
            state: raw_json(r#"{"stopped":false}"#),
            name: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    store
        .enqueue_external_event(NewExternalEvent {
            session_id: id,
            event_type: EventType::CancellationRequested,
            payload: raw_json(r#"{"reason":null}"#),
            idempotency_key: Some("shutdown".into()),
        })
        .await
        .unwrap();
    id
}

async fn stored_waits(store: &Store, session_id: SessionId, waits: Vec<NewWait>) {
    store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::CancellationRequested,
            payload: raw_json("{}"),
            idempotency_key: Some("wait-fixture".into()),
        })
        .await
        .unwrap();
    let claim = store
        .claim_next_event(std::time::Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.session.id, session_id);
    store
        .commit_handler_outcome(
            &HandlerClaim::from(&claim),
            OutcomeCommit {
                cancelled_waits: vec![],
                progress: vec![],
                state: raw_json(r#"{"step":1}"#),
                status: None,
                history: vec![],
                operations: vec![],
                waits,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn durable_input_validation_does_not_touch_the_database() {
    let router = app(lazy_store(), registry(), vec![TOKEN.into()]);
    let session_id = Uuid::new_v4();
    let message_uri = format!("/v1/sessions/{session_id}/messages");

    assert_eq!(
        call(
            &router,
            Method::POST,
            &message_uri,
            Some(r#"{"message":{"role":"user","content":[]}}"#),
            Some(TOKEN),
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &message_uri,
            r#"{"message":{"role":"system","content":[]}}"#,
            "system-message",
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &message_uri,
            r#"{"message":{"role":"user","content":[]}}"#,
            "contains whitespace",
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );

    for body in [
        r#"{"message":{"role":"user","content":[{"type":"image","url":"not-a-url"}]}}"#,
        r#"{"message":{"role":"user","content":[{"type":"text","text":"hello","extra":true}]}}"#,
        r#"{"message":{"role":"user","content":[],"extra":true}}"#,
    ] {
        assert_eq!(
            call_idempotent(
                &router,
                Method::POST,
                &message_uri,
                body,
                &Uuid::new_v4().to_string(),
            )
            .await
            .0,
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    for uri in [
        "/v1/sessions/%FF/messages",
        "/v1/sessions/%FF/cancel",
        "/v1/sessions/%FF/waits",
        "/v1/sessions/00000000-0000-0000-0000-000000000000/waits/%FF",
        "/v1/sessions/00000000-0000-0000-0000-000000000000/waits/%FF/resolve",
    ] {
        let method =
            if uri.ends_with("messages") || uri.ends_with("cancel") || uri.ends_with("resolve") {
                Method::POST
            } else {
                Method::GET
            };
        let response = call_response(&router, method, uri, Some("{}"), Some(TOKEN), true).await;
        assert_error(response, StatusCode::BAD_REQUEST, "invalid_request").await;
    }

    let response = call_response(
        &router,
        Method::DELETE,
        &format!("/v1/sessions/{session_id}/waits"),
        None,
        Some(TOKEN),
        true,
    )
    .await;
    assert_error(
        response,
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
    )
    .await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn signed_callbacks_are_admitted_without_bearer_auth_and_deduplicated() {
    let database = Database::new().await;
    let connection = "llm-primary";
    let secret = "callback-secret";
    let (router, _) = app_with_settings(
        database.store.clone(),
        registry(),
        vec![TOKEN.into()],
        ServerSettings {
            callback_connections: vec![GatewayCallbackConfig {
                id: agent_contracts::GatewayConnectionId(connection.into()),
                kind: GatewayKind::Llm,
                secrets: vec![secret.into()],
            }],
            ..ServerSettings::default()
        },
    );
    let event_id = Uuid::new_v4();
    let job_id = Uuid::new_v4();
    let body = json!({
        "eventId": event_id,
        "type": "job.succeeded",
        "jobId": job_id,
        "completedAt": chrono::Utc::now()
    })
    .to_string();
    let timestamp = chrono::Utc::now().timestamp().to_string();
    let signature = callback_signature(secret, &timestamp, event_id, &body);

    assert_eq!(
        call_llm_callback(&router, connection, event_id, &timestamp, &signature, &body,).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        call_llm_callback(&router, connection, event_id, &timestamp, &signature, &body,).await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        call_llm_callback(
            &router,
            connection,
            event_id,
            &timestamp,
            "v1=0000000000000000000000000000000000000000000000000000000000000000",
            &body,
        )
        .await,
        StatusCode::UNAUTHORIZED
    );
    let receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM gateway_callback_receipts \
         WHERE gateway_connection_id=$1 AND gateway_event_id=$2 AND gateway_job_id=$3",
    )
    .bind(connection)
    .bind(event_id)
    .bind(job_id)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(receipts, 1);
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn session_management_api_is_complete() {
    let database = Database::new().await;
    let router = app(database.store.clone(), registry(), vec![TOKEN.into()]);
    assert_eq!(
        call(&router, Method::GET, "/readyz", None, None).await.0,
        StatusCode::OK
    );

    let invalid_id = Uuid::new_v4();
    let invalid = json!({
        "id": invalid_id,
        "project_id": "project-a",
        "harness_id": "fixture-api",
        "harness_version": "1",
        "configuration": {"native": null}
    });
    let (status, body) = call(
        &router,
        Method::POST,
        "/v1/sessions",
        Some(&invalid.to_string()),
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(!body.contains("native cannot be null"));
    assert!(
        database
            .store
            .session(agent_contracts::SessionId(invalid_id))
            .await
            .is_err()
    );

    let failed_id = Uuid::new_v4();
    let failed = json!({
        "id": failed_id,
        "project_id": "project-a",
        "harness_id": "fixture-api",
        "harness_version": "1",
        "configuration": {"native": "fail"}
    });
    let (status, body) = call(
        &router,
        Method::POST,
        "/v1/sessions",
        Some(&failed.to_string()),
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!body.contains("requested failure"));
    assert!(
        database
            .store
            .session(agent_contracts::SessionId(failed_id))
            .await
            .is_err()
    );

    let session_id = Uuid::new_v4();
    let create = json!({
        "id": session_id,
        "project_id": "project-a",
        "harness_id": "fixture-api",
        "harness_version": "1",
        "configuration": {"native": "ordinary"},
        "name": "First",
        "metadata": {"source": "test"}
    });
    let response = call_response(
        &router,
        Method::POST,
        "/v1/sessions",
        Some(&create.to_string()),
        Some(TOKEN),
        true,
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.headers()[header::LOCATION],
        format!("/v1/sessions/{session_id}")
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "idle");
    assert_eq!(body["state_version"], 0);
    assert!(body.get("state").is_none());
    let child_rows: i64 = sqlx::query_scalar(
        "SELECT \
         (SELECT count(*) FROM session_events) + \
         (SELECT count(*) FROM session_event_attempts) + \
         (SELECT count(*) FROM session_history) + \
         (SELECT count(*) FROM session_operations) + \
         (SELECT count(*) FROM session_operation_requests) + \
         (SELECT count(*) FROM session_operation_attempts) + \
         (SELECT count(*) FROM session_waits) + \
         (SELECT count(*) FROM gateway_callback_receipts)",
    )
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(child_rows, 0);
    assert_eq!(
        call(
            &router,
            Method::POST,
            "/v1/sessions",
            Some(&create.to_string()),
            Some(TOKEN)
        )
        .await
        .0,
        StatusCode::CONFLICT
    );

    let session_uri = format!("/v1/sessions/{session_id}");
    let patch_name = call(
        &router,
        Method::PATCH,
        &session_uri,
        Some(r#"{"name":"Renamed"}"#),
        Some(TOKEN),
    );
    let patch_metadata = call(
        &router,
        Method::PATCH,
        &session_uri,
        Some(r#"{"metadata":{"updated":true}}"#),
        Some(TOKEN),
    );
    let (name_result, metadata_result) = tokio::join!(patch_name, patch_metadata);
    assert_eq!(name_result.0, StatusCode::OK);
    assert_eq!(metadata_result.0, StatusCode::OK);

    let (status, detail) = call(
        &router,
        Method::GET,
        &format!("/v1/sessions/{session_id}"),
        None,
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let detail: Value = serde_json::from_str(&detail).unwrap();
    assert_eq!(detail["name"], "Renamed");
    assert_eq!(detail["metadata"], json!({"updated": true}));
    assert!(detail.get("state").is_none());
    assert!(detail.get("lease_token").is_none());

    assert_eq!(
        call(
            &router,
            Method::PATCH,
            &session_uri,
            Some(r#"{"name":null}"#),
            Some(TOKEN),
        )
        .await
        .0,
        StatusCode::OK
    );
    let (_, detail) = call(&router, Method::GET, &session_uri, None, Some(TOKEN)).await;
    let detail: Value = serde_json::from_str(&detail).unwrap();
    assert!(detail["name"].is_null());
    assert_eq!(detail["metadata"], json!({"updated": true}));

    for immutable in [
        r#"{"id":"00000000-0000-0000-0000-000000000000"}"#,
        r#"{"project_id":"other"}"#,
        r#"{"harness_id":"other"}"#,
        r#"{"harness_version":"other"}"#,
        r#"{"configuration":{}}"#,
        r#"{"state":{}}"#,
        r#"{"status":"failed"}"#,
    ] {
        assert_eq!(
            call(
                &router,
                Method::PATCH,
                &session_uri,
                Some(immutable),
                Some(TOKEN),
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }

    let raw_id = Uuid::new_v4();
    let raw_create = format!(
        r#"{{"id":"{raw_id}","project_id":"project-b","harness_id":"fixture-api","harness_version":"2","configuration":{{"native":{{"text":"\ud800"}}}}}}"#
    );
    let (status, body) = call(
        &router,
        Method::POST,
        "/v1/sessions",
        Some(&raw_create),
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(body.contains(r#"\ud800"#));
    assert!(
        database
            .store
            .session(agent_contracts::SessionId(raw_id))
            .await
            .unwrap()
            .configuration
            .get()
            .contains(r#"\ud800"#)
    );

    let second_id = Uuid::new_v4();
    let second_create = json!({
        "id": second_id,
        "project_id": "project-a",
        "harness_id": "fixture-api",
        "harness_version": "1",
        "configuration": {"native": "second"}
    });
    assert_eq!(
        call(
            &router,
            Method::POST,
            "/v1/sessions",
            Some(&second_create.to_string()),
            Some(TOKEN),
        )
        .await
        .0,
        StatusCode::CREATED
    );

    let (status, list) = call(
        &router,
        Method::GET,
        "/v1/sessions?project_id=project-a&status=idle&harness_id=fixture-api&limit=1",
        None,
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let list: Value = serde_json::from_str(&list).unwrap();
    assert_eq!(list["data"].as_array().unwrap().len(), 1);
    assert!(list["data"][0].get("configuration").is_none());
    assert!(list["data"][0].get("state").is_none());
    let first_page_id = list["data"][0]["id"].as_str().unwrap();
    let cursor = list["next_cursor"].as_str().unwrap();
    let (_, next_page) = call(
        &router,
        Method::GET,
        &format!(
            "/v1/sessions?project_id=project-a&status=idle&harness_id=fixture-api&limit=1&cursor={cursor}"
        ),
        None,
        Some(TOKEN),
    )
    .await;
    let next_page: Value = serde_json::from_str(&next_page).unwrap();
    assert_eq!(next_page["data"].as_array().unwrap().len(), 1);
    assert_ne!(next_page["data"][0]["id"], first_page_id);
    assert!(next_page["next_cursor"].is_null());

    assert_eq!(
        call(
            &router,
            Method::GET,
            "/v1/sessions?cursor=not-a-cursor",
            None,
            Some(TOKEN),
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        call(
            &router,
            Method::GET,
            "/v1/harnesses/missing/versions",
            None,
            Some(TOKEN),
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call(
            &router,
            Method::GET,
            &format!("/v1/sessions/{}", Uuid::new_v4()),
            None,
            Some(TOKEN),
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    let registry_independent = app(
        database.store.clone(),
        HarnessRegistry::new(),
        vec![TOKEN.into()],
    );
    assert_eq!(
        call(
            &registry_independent,
            Method::GET,
            &session_uri,
            None,
            Some(TOKEN),
        )
        .await
        .0,
        StatusCode::OK
    );

    database.store.pool().close().await;
    assert_eq!(
        call(&router, Method::GET, "/v1/sessions", None, Some(TOKEN),)
            .await
            .0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    drop(registry_independent);
    drop(router);
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn durable_inputs_and_wait_resolution_api_are_complete() {
    let database = Database::new().await;
    let router = app(database.store.clone(), registry(), vec![TOKEN.into()]);
    let session_id = stored_session(&database.store, "part-four").await;
    sqlx::query("UPDATE sessions SET processing_enabled=false WHERE id=$1")
        .bind(session_id.0)
        .execute(database.store.pool())
        .await
        .unwrap();

    let message_uri = format!("/v1/sessions/{session_id}/messages");
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &message_uri,
            r#"{"message":{"role":"user","content":[{"type":"image","url":"not-a-url"}]}}"#,
            "invalid-image",
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let event_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM session_events WHERE session_id=$1")
            .bind(session_id.0)
            .fetch_one(database.store.pool())
            .await
            .unwrap();
    assert_eq!(event_count, 0);
    let message = r#"{"message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#;
    let (status, first_ack) =
        call_idempotent(&router, Method::POST, &message_uri, message, "shared-key").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(first_ack["session_id"], session_id.to_string());
    assert_eq!(first_ack["event_sequence"], 1);

    let (status, retry_ack) =
        call_idempotent(&router, Method::POST, &message_uri, message, "shared-key").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(retry_ack, first_ack);
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &message_uri,
            r#"{ "message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#,
            "shared-key",
        )
        .await
        .0,
        StatusCode::CONFLICT
    );

    let cancel_uri = format!("/v1/sessions/{session_id}/cancel");
    let (status, cancel_ack) = call_idempotent(
        &router,
        Method::POST,
        &cancel_uri,
        r#"{"reason":"stop when handled"}"#,
        "shared-key",
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(cancel_ack["event_sequence"], 2);

    let saved = database.store.session(session_id).await.unwrap();
    assert_eq!(saved.state.get(), r#"{"step":0}"#);
    assert_eq!(saved.state_version.0, 0);
    assert_eq!(saved.status, agent_contracts::SessionStatus::Idle);
    assert!(!saved.processing_enabled);
    let derived_count: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM session_history WHERE session_id=$1) + \
         (SELECT count(*) FROM session_operations WHERE session_id=$1) + \
         (SELECT count(*) FROM session_waits WHERE session_id=$1)",
    )
    .bind(session_id.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(derived_count, 0);

    let concurrent_session = stored_session(&database.store, "concurrent").await;
    let concurrent_uri = format!("/v1/sessions/{concurrent_session}/messages");
    let left = call_idempotent(
        &router,
        Method::POST,
        &concurrent_uri,
        message,
        "concurrent-key",
    );
    let right = call_idempotent(
        &router,
        Method::POST,
        &concurrent_uri,
        message,
        "concurrent-key",
    );
    let (left, right) = tokio::join!(left, right);
    assert_eq!(left.0, StatusCode::ACCEPTED);
    assert_eq!(left.1, right.1);
    sqlx::query("UPDATE sessions SET processing_enabled=false WHERE id=$1")
        .bind(concurrent_session.0)
        .execute(database.store.pool())
        .await
        .unwrap();

    for status in [
        "idle",
        "running",
        "waiting",
        "cancelling",
        "cancelled",
        "failed",
    ] {
        let id = stored_session(&database.store, status).await;
        sqlx::query("UPDATE sessions SET status=$2 WHERE id=$1")
            .bind(id.0)
            .bind(status)
            .execute(database.store.pool())
            .await
            .unwrap();
        assert_eq!(
            call_idempotent(
                &router,
                Method::POST,
                &format!("/v1/sessions/{id}/messages"),
                message,
                status,
            )
            .await
            .0,
            StatusCode::ACCEPTED
        );
        let cancel_uri = format!("/v1/sessions/{id}/cancel");
        let first = call_idempotent(&router, Method::POST, &cancel_uri, "{}", status).await;
        let retry = call_idempotent(&router, Method::POST, &cancel_uri, "{}", status).await;
        assert_eq!(first.0, StatusCode::ACCEPTED);
        assert_eq!(first.1, retry.1);
        sqlx::query("UPDATE sessions SET processing_enabled=false WHERE id=$1")
            .bind(id.0)
            .execute(database.store.pool())
            .await
            .unwrap();
    }

    let wait_session = stored_session(&database.store, "waits").await;
    let schema_wait = WaitId::new();
    let null_wait = WaitId::new();
    let expiration_wait = WaitId::new();
    let pending_wait = WaitId::new();
    let const_wait = WaitId::new();
    let scalar_wait = WaitId::new();
    let conflict_wait = WaitId::new();
    let race_wait = WaitId::new();
    let wait_clock = database
        .store
        .session(wait_session)
        .await
        .unwrap()
        .updated_at;
    let schema = json!({
        "$defs": {"decision": {"type": "boolean"}},
        "type": "object",
        "properties": {"approved": {"$ref": "#/$defs/decision"}},
        "required": ["approved"],
        "additionalProperties": false
    });
    stored_waits(
        &database.store,
        wait_session,
        vec![
            NewWait {
                id: schema_wait,
                mode: WaitMode::Either,
                payload: raw_json(r#"{"prompt":"schema"}"#),
                response_schema: Some(raw_json(&schema.to_string())),
                expires_at: Some(wait_clock + std::time::Duration::from_secs(3_600)),
            },
            NewWait {
                id: null_wait,
                mode: WaitMode::External,
                payload: raw_json(r#"{"prompt":"null"}"#),
                response_schema: None,
                expires_at: None,
            },
            NewWait {
                id: expiration_wait,
                mode: WaitMode::Expiration,
                payload: raw_json(r#"{"prompt":"expiration"}"#),
                response_schema: None,
                expires_at: Some(wait_clock + std::time::Duration::from_secs(3_600)),
            },
            NewWait {
                id: pending_wait,
                mode: WaitMode::External,
                payload: raw_json(r#"{"prompt":"pending"}"#),
                response_schema: None,
                expires_at: None,
            },
            NewWait {
                id: const_wait,
                mode: WaitMode::External,
                payload: raw_json(r#"{"prompt":"const"}"#),
                response_schema: Some(raw_json(r#"{"const":{"$ref":"literal-value"}}"#)),
                expires_at: None,
            },
            NewWait {
                id: scalar_wait,
                mode: WaitMode::External,
                payload: raw_json(r#"{"prompt":"scalar"}"#),
                response_schema: None,
                expires_at: None,
            },
            NewWait {
                id: conflict_wait,
                mode: WaitMode::External,
                payload: raw_json(r#"{"prompt":"conflict"}"#),
                response_schema: None,
                expires_at: None,
            },
            NewWait {
                id: race_wait,
                mode: WaitMode::Either,
                payload: raw_json(r#"{"prompt":"race"}"#),
                response_schema: None,
                expires_at: Some(wait_clock - std::time::Duration::from_secs(1)),
            },
        ],
    )
    .await;

    let waits_uri = format!("/v1/sessions/{wait_session}/waits");
    let (status, first_page) = call(
        &router,
        Method::GET,
        &format!("{waits_uri}?limit=4"),
        None,
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let first_page: Value = serde_json::from_str(&first_page).unwrap();
    assert_eq!(first_page["data"].as_array().unwrap().len(), 4);
    let cursor = first_page["next_cursor"].as_str().unwrap();
    let (status, second_page) = call(
        &router,
        Method::GET,
        &format!("{waits_uri}?limit=4&cursor={cursor}"),
        None,
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let second_page: Value = serde_json::from_str(&second_page).unwrap();
    assert_eq!(second_page["data"].as_array().unwrap().len(), 4);
    assert!(second_page["next_cursor"].is_null());

    let other_session = stored_session(&database.store, "other").await;
    assert_eq!(
        call(
            &router,
            Method::GET,
            &format!("/v1/sessions/{other_session}/waits/{schema_wait}"),
            None,
            Some(TOKEN),
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &format!("/v1/sessions/{other_session}/waits/{schema_wait}/resolve"),
            r#"{"response":{"approved":true}}"#,
            "wrong-session",
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );

    let schema_resolve = format!("{waits_uri}/{schema_wait}/resolve");
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &schema_resolve,
            r#"{"response":{"approved":"yes"}}"#,
            "approve",
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let (status, resolution_ack) = call_idempotent(
        &router,
        Method::POST,
        &schema_resolve,
        r#"{"response":{"approved":true}}"#,
        "approve",
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(resolution_ack["wait_id"], schema_wait.to_string());
    assert_eq!(resolution_ack["event_sequence"], 2);

    sqlx::query(
        "UPDATE session_waits SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(schema_wait.0)
    .execute(database.store.pool())
    .await
    .unwrap();
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &schema_resolve,
            r#"{"response":{"approved":true}}"#,
            "approve",
        )
        .await
        .1,
        resolution_ack
    );
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &schema_resolve,
            r#"{"response":{"approved":false}}"#,
            "approve",
        )
        .await
        .0,
        StatusCode::CONFLICT
    );

    let null_resolve = format!("{waits_uri}/{null_wait}/resolve");
    let (status, null_ack) = call_idempotent(
        &router,
        Method::POST,
        &null_resolve,
        r#"{"response":null}"#,
        "null-response",
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &format!("{waits_uri}/{expiration_wait}/resolve"),
            r#"{"response":null}"#,
            "not-external",
        )
        .await
        .0,
        StatusCode::CONFLICT
    );

    let const_uri = format!("{waits_uri}/{const_wait}/resolve");
    assert_eq!(
        call_idempotent(
            &router,
            Method::POST,
            &const_uri,
            r#"{"response":{"$ref":"literal-value"}}"#,
            "const",
        )
        .await
        .0,
        StatusCode::ACCEPTED
    );

    let scalar_uri = format!("{waits_uri}/{scalar_wait}/resolve");
    let left = call_idempotent(
        &router,
        Method::POST,
        &scalar_uri,
        r#"{"response":7}"#,
        "scalar",
    );
    let right = call_idempotent(
        &router,
        Method::POST,
        &scalar_uri,
        r#"{"response":7}"#,
        "scalar",
    );
    let (left, right) = tokio::join!(left, right);
    assert_eq!(left.0, StatusCode::ACCEPTED);
    assert_eq!(left.1, right.1);

    let conflict_uri = format!("{waits_uri}/{conflict_wait}/resolve");
    let left = call_idempotent(
        &router,
        Method::POST,
        &conflict_uri,
        r#"{"response":"left"}"#,
        "left",
    );
    let right = call_idempotent(
        &router,
        Method::POST,
        &conflict_uri,
        r#"{"response":"right"}"#,
        "right",
    );
    let (left, right) = tokio::join!(left, right);
    let statuses = [left.0, right.0];
    assert!(statuses.contains(&StatusCode::ACCEPTED));
    assert!(statuses.contains(&StatusCode::CONFLICT));

    let race_uri = format!("{waits_uri}/{race_wait}/resolve");
    let reply = call_idempotent(
        &router,
        Method::POST,
        &race_uri,
        r#"{"response":"reply"}"#,
        "race",
    );
    let expiration = database.store.expire_wait(race_wait);
    let (reply, expiration) = tokio::join!(reply, expiration);
    assert_eq!(reply.0, StatusCode::CONFLICT);
    assert!(expiration.unwrap().is_some());
    assert_eq!(
        database.store.wait(race_wait).await.unwrap().status,
        WaitStatus::Expired
    );

    let (status, resolved) = call(
        &router,
        Method::GET,
        &format!("{waits_uri}?status=resolved"),
        None,
        Some(TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let resolved: Value = serde_json::from_str(&resolved).unwrap();
    assert_eq!(resolved["data"].as_array().unwrap().len(), 5);
    let wait_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM session_events WHERE session_id=$1 AND type='wait_resumed'",
    )
    .bind(wait_session.0)
    .fetch_one(database.store.pool())
    .await
    .unwrap();
    assert_eq!(wait_events, 6);

    drop(router);
    database.store.pool().close().await;
    let reopened = Store::connect(&database.url, PoolConfig::default())
        .await
        .unwrap();
    let reopened_router = app(reopened.clone(), registry(), vec![TOKEN.into()]);
    assert_eq!(
        call_idempotent(
            &reopened_router,
            Method::POST,
            &message_uri,
            message,
            "shared-key",
        )
        .await
        .1,
        first_ack
    );
    assert_eq!(
        call_idempotent(
            &reopened_router,
            Method::POST,
            &cancel_uri,
            r#"{"reason":"stop when handled"}"#,
            "shared-key",
        )
        .await
        .1,
        cancel_ack
    );
    assert_eq!(
        call_idempotent(
            &reopened_router,
            Method::POST,
            &schema_resolve,
            r#"{"response":{"approved":true}}"#,
            "approve",
        )
        .await
        .1,
        resolution_ack
    );
    assert_eq!(
        call_idempotent(
            &reopened_router,
            Method::POST,
            &null_resolve,
            r#"{"response":null}"#,
            "null-response",
        )
        .await
        .1,
        null_ack
    );
    drop(reopened_router);
    reopened.pool().close().await;
    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn injected_shutdown_drains_requests_and_enforces_its_deadline() {
    let database = Database::new().await;

    let mut lock = database.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_on_listener_with_shutdown(
        listener,
        database.store.clone(),
        registry(),
        vec![TOKEN.into()],
        ServerSettings {
            shutdown_grace: std::time::Duration::from_secs(1),
            ..ServerSettings::default()
        },
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let mut client = send_list_request(address).await;
    wait_for_blocked_query(database.store.pool()).await;
    shutdown_tx.send(()).unwrap();
    lock.commit().await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        client.read_to_end(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(String::from_utf8(response).unwrap().contains("200 OK"));
    assert!(server.await.unwrap().is_ok());

    let mut lock = database.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_on_listener_with_shutdown(
        listener,
        database.store.clone(),
        registry(),
        vec![TOKEN.into()],
        ServerSettings {
            shutdown_grace: std::time::Duration::from_millis(25),
            ..ServerSettings::default()
        },
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let mut client = send_list_request(address).await;
    wait_for_blocked_query(database.store.pool()).await;
    shutdown_tx.send(()).unwrap();
    assert!(matches!(
        server.await.unwrap(),
        Err(ServerError::ShutdownTimedOut)
    ));
    lock.rollback().await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        client.read_to_end(&mut response),
    )
    .await
    .unwrap()
    .unwrap();

    database.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn server_shutdown_supervises_cooperative_and_forced_active_handlers() {
    let database = Database::new().await;
    let cooperative = active_shutdown_session(&database.store, true).await;
    let mut registry = HarnessRegistry::new();
    registry.register(ShutdownHarness).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_on_listener_with_shutdown(
        listener,
        database.store.clone(),
        registry,
        vec![TOKEN.into()],
        ServerSettings {
            shutdown_grace: std::time::Duration::from_millis(500),
            scheduler: agent_runtime::SchedulerSettings {
                poll_interval: std::time::Duration::from_millis(5),
                shutdown_grace: std::time::Duration::from_millis(400),
                ..agent_runtime::SchedulerSettings::default()
            },
            ..ServerSettings::default()
        },
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if database
                .store
                .session(cooperative)
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
    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
    let session = database.store.session(cooperative).await.unwrap();
    assert_eq!(session.state_version.0, 1);
    let state: ShutdownState = serde_json::from_str(session.state.get()).unwrap();
    assert!(state.stopped);

    let forced = active_shutdown_session(&database.store, false).await;
    let mut registry = HarnessRegistry::new();
    registry.register(ShutdownHarness).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_on_listener_with_shutdown(
        listener,
        database.store.clone(),
        registry,
        vec![TOKEN.into()],
        ServerSettings {
            shutdown_grace: std::time::Duration::from_millis(200),
            scheduler: agent_runtime::SchedulerSettings {
                poll_interval: std::time::Duration::from_millis(5),
                lease_duration: std::time::Duration::from_millis(100),
                lease_renewal_interval: std::time::Duration::from_millis(20),
                shutdown_grace: std::time::Duration::from_millis(50),
                ..agent_runtime::SchedulerSettings::default()
            },
            ..ServerSettings::default()
        },
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if database
                .store
                .session(forced)
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
    shutdown_tx.send(()).unwrap();
    server.await.unwrap().unwrap();
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
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(
        database
            .store
            .claim_next_event(std::time::Duration::from_secs(1))
            .await
            .unwrap()
            .is_some()
    );
    database.close().await;
}

async fn send_list_request(address: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!(
                "GET /v1/sessions HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    stream
}

async fn wait_for_blocked_query(pool: &PgPool) {
    for _ in 0..100 {
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
             WHERE datname=current_database() AND wait_event_type='Lock' \
             AND query LIKE 'SELECT id,project_id,harness_id%')",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if blocked {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("session list request did not reach the database");
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL; creates and drops a disposable database"]
async fn processing_retry_api_requires_auth_and_exact_blocked_revision() {
    let database = Database::new().await;
    let session_id = stored_session(&database.store, "recovery").await;
    database
        .store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload: raw_json(
                r#"{"message":{"role":"user","content":[{"type":"text","text":"hello"}]}}"#,
            ),
            idempotency_key: Some("message".into()),
        })
        .await
        .unwrap();
    let claim = database
        .store
        .claim_next_event(std::time::Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    database
        .store
        .record_handler_failure(
            &HandlerClaim::from(&claim),
            json!({"code":"blocked"}),
            chrono::Utc::now(),
            true,
        )
        .await
        .unwrap();
    let router = app(database.store.clone(), registry(), vec![TOKEN.into()]);
    let uri = format!("/v1/sessions/{session_id}/processing/retry");
    let detail_uri = format!("/v1/sessions/{session_id}");
    let (status, detail) = call(&router, Method::GET, &detail_uri, None, Some(TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    let detail: Value = serde_json::from_str(&detail).unwrap();
    assert_eq!(detail["processing_health"]["revision"], 1);
    assert_eq!(
        detail["processing_health"]["blocked_event_id"],
        claim.event.id.to_string()
    );
    let body = format!(
        r#"{{"expected_event_id":"{}","expected_processing_revision":1,"reason":"operator retry"}}"#,
        claim.event.id
    );
    let (status, _) = call(&router, Method::POST, &uri, Some(&body), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let stale = format!(
        r#"{{"expected_event_id":"{}","expected_processing_revision":0}}"#,
        claim.event.id
    );
    let (status, response) = call_idempotent(&router, Method::POST, &uri, &stale, "stale").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"]["code"], "stale_processing_retry");
    let (status, accepted) = call_idempotent(&router, Method::POST, &uri, &body, "retry").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(accepted["event_id"], claim.event.id.to_string());
    assert_eq!(accepted["processing_revision"], 2);
    let (status, duplicate) = call_idempotent(&router, Method::POST, &uri, &body, "retry").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(duplicate, accepted);
    let (status, response) = call_idempotent(&router, Method::POST, &uri, &stale, "retry").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"]["code"], "idempotency_conflict");
    let (status, response) = call_idempotent(&router, Method::POST, &uri, &body, "new-key").await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"]["code"], "stale_processing_retry");
    let reclaimed = database
        .store
        .claim_next_event(std::time::Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.event.id, claim.event.id);
    database
        .store
        .commit_handler_outcome(
            &HandlerClaim::from(&reclaimed),
            OutcomeCommit {
                state: raw_json("{}"),
                status: None,
                history: vec![],
                operations: vec![],
                waits: vec![],
                cancelled_waits: vec![],
                progress: vec![],
            },
        )
        .await
        .unwrap();
    let (status, duplicate_after_handling) =
        call_idempotent(&router, Method::POST, &uri, &body, "retry").await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(duplicate_after_handling, accepted);
    database.close().await;
}
