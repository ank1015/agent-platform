use crate::{ApiError, AppState};
use agent_store::{WaitCursor, WaitFilter, WaitRecord, WaitStatus};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, PathRejection, QueryRejection},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListWaits {
    status: Option<ApiWaitStatus>,
    cursor: Option<String>,
    limit: Option<i64>,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ApiWaitStatus {
    Pending,
    Resolved,
    Expired,
    Cancelled,
}

#[derive(Deserialize, Serialize)]
struct Cursor {
    created_at: DateTime<Utc>,
    id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveWait {
    response: Box<RawValue>,
}

#[derive(Serialize)]
struct WaitPage {
    data: Vec<WaitDetail>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct WaitDetail {
    id: Uuid,
    session_id: Uuid,
    source_event_id: Uuid,
    resolution_mode: &'static str,
    payload: Box<RawValue>,
    response_schema: Option<Box<RawValue>>,
    expires_at: Option<DateTime<Utc>>,
    status: ApiWaitStatus,
    resolution: Option<Box<RawValue>>,
    created_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct WaitResolutionAcknowledgement {
    #[serde(flatten)]
    event: super::inputs::EventAcknowledgement,
    wait_id: Uuid,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/sessions/{session_id}/waits", get(list))
        .route("/sessions/{session_id}/waits/{wait_id}", get(detail))
        .route(
            "/sessions/{session_id}/waits/{wait_id}/resolve",
            post(resolve),
        )
}

async fn list(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<ListWaits>, QueryRejection>,
) -> Result<Json<WaitPage>, ApiError> {
    let Path(session_id) = path.map_err(super::sessions::path_rejection)?;
    let session_id = super::sessions::parse_session_id(&session_id)?;
    let Query(query) = query.map_err(|error| ApiError::bad_request(error.body_text()))?;
    let cursor = query.cursor.as_deref().map(decode_cursor).transpose()?;
    let page = state
        .store
        .list_waits(
            session_id,
            WaitFilter {
                status: query.status.map(WaitStatus::from),
                cursor,
                limit: query.limit,
            },
        )
        .await?;
    Ok(Json(WaitPage {
        data: page.waits.into_iter().map(WaitDetail::from).collect(),
        next_cursor: page.next_cursor.map(encode_cursor).transpose()?,
    }))
}

async fn detail(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Json<WaitDetail>, ApiError> {
    let Path((session_id, wait_id)) = path.map_err(super::sessions::path_rejection)?;
    let session_id = super::sessions::parse_session_id(&session_id)?;
    let wait_id = parse_wait_id(&wait_id)?;
    let wait = state
        .store
        .wait_for_session(session_id, wait_id)
        .await?
        .ok_or(agent_store::StoreError::WaitNotFound(wait_id))?;
    Ok(Json(WaitDetail::from(wait)))
}

async fn resolve(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    headers: HeaderMap,
    request: Result<Json<Box<RawValue>>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Path((session_id, wait_id)) = path.map_err(super::sessions::path_rejection)?;
    let session_id = super::sessions::parse_session_id(&session_id)?;
    let wait_id = parse_wait_id(&wait_id)?;
    let key = super::inputs::idempotency_key(&headers)?;
    let Json(body) = request.map_err(super::sessions::json_rejection)?;
    let request: ResolveWait = serde_json::from_str(body.get())
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let wait = state
        .store
        .wait_for_session(session_id, wait_id)
        .await?
        .ok_or(agent_store::StoreError::WaitNotFound(wait_id))?;
    validate_response_schema(&wait, request.response.as_ref())?;
    let resolution = state
        .store
        .resolve_wait(wait_id, &key, request.response.as_ref())
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(WaitResolutionAcknowledgement {
            event: super::inputs::acknowledgement(resolution.event),
            wait_id: wait_id.0,
        }),
    )
        .into_response())
}

impl From<ApiWaitStatus> for WaitStatus {
    fn from(status: ApiWaitStatus) -> Self {
        match status {
            ApiWaitStatus::Pending => Self::Pending,
            ApiWaitStatus::Resolved => Self::Resolved,
            ApiWaitStatus::Expired => Self::Expired,
            ApiWaitStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<WaitStatus> for ApiWaitStatus {
    fn from(status: WaitStatus) -> Self {
        match status {
            WaitStatus::Pending => Self::Pending,
            WaitStatus::Resolved => Self::Resolved,
            WaitStatus::Expired => Self::Expired,
            WaitStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl From<WaitRecord> for WaitDetail {
    fn from(wait: WaitRecord) -> Self {
        Self {
            id: wait.id.0,
            session_id: wait.session_id.0,
            source_event_id: wait.source_event_id.0,
            resolution_mode: match wait.mode {
                agent_contracts::WaitMode::External => "external",
                agent_contracts::WaitMode::Expiration => "expiration",
                agent_contracts::WaitMode::Either => "either",
            },
            payload: wait.payload,
            response_schema: wait.response_schema,
            expires_at: wait.expires_at,
            status: wait.status.into(),
            resolution: wait.resolution_payload,
            created_at: wait.created_at,
            finished_at: wait.finished_at,
        }
    }
}

fn validate_response_schema(wait: &WaitRecord, response: &RawValue) -> Result<(), ApiError> {
    let Some(schema) = &wait.response_schema else {
        return Ok(());
    };
    let schema: Value = serde_json::from_str(schema.get()).map_err(|_| {
        tracing::error!(wait_id = %wait.id, "stored wait schema is invalid JSON");
        internal_schema_error()
    })?;
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .map_err(|_| {
            tracing::error!(wait_id = %wait.id, "stored wait schema could not be compiled");
            internal_schema_error()
        })?;
    let response: Value = serde_json::from_str(response.get()).map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_wait_response",
            "response cannot be validated against the wait schema",
        )
    })?;
    if !validator.is_valid(&response) {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_wait_response",
            "response does not match the wait response schema",
        ));
    }
    Ok(())
}

fn internal_schema_error() -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "invalid_wait_schema",
        "the wait response schema is invalid",
    )
}

fn parse_wait_id(value: &str) -> Result<agent_contracts::WaitId, ApiError> {
    value
        .parse::<Uuid>()
        .map(agent_contracts::WaitId)
        .map_err(|_| ApiError::bad_request("wait_id must be a UUID"))
}

fn decode_cursor(value: &str) -> Result<WaitCursor, ApiError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| ApiError::bad_request("cursor is invalid"))?;
    let cursor: Cursor =
        serde_json::from_slice(&bytes).map_err(|_| ApiError::bad_request("cursor is invalid"))?;
    Ok(WaitCursor {
        created_at: cursor.created_at,
        id: agent_contracts::WaitId(cursor.id),
    })
}

fn encode_cursor(cursor: WaitCursor) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(&Cursor {
        created_at: cursor.created_at,
        id: cursor.id.0,
    })
    .map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "an internal error occurred",
        )
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn schema_compiler_accepts_literal_ref_data_and_rejects_external_resources() {
        let local = json!({
            "$defs": {"answer": {"type": "boolean"}},
            "$ref": "#/$defs/answer"
        });
        let literal = json!({"const": {"$ref": "literal-value"}});
        let external = json!({"$ref": "https://example.com/schema"});
        assert!(
            jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .build(&local)
                .is_ok()
        );
        assert!(
            jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .build(&literal)
                .is_ok()
        );
        assert!(
            jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .build(&external)
                .is_err()
        );
    }
}
