use crate::{ApiError, AppState, runtime_initialization};
use agent_contracts::{HarnessId, HarnessVersion, ProjectId, SessionId, SessionStatus};
use agent_store::{NewSession, SessionCursor, SessionFilter, SessionRecord, SessionSummary};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, PathRejection, QueryRejection},
    },
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value, value::RawValue};
use uuid::Uuid;

const MAX_PROJECT_ID: usize = 300;
const MAX_HARNESS_ID: usize = 100;
const MAX_HARNESS_VERSION: usize = 100;
const MAX_NAME: usize = 500;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateSession {
    id: Uuid,
    project_id: String,
    harness_id: String,
    harness_version: String,
    configuration: Box<RawValue>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default = "empty_object")]
    metadata: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListSessions {
    project_id: Option<String>,
    status: Option<SessionStatus>,
    harness_id: Option<String>,
    cursor: Option<String>,
    limit: Option<i64>,
}

#[derive(Deserialize, Serialize)]
struct Cursor {
    created_at: DateTime<Utc>,
    id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PatchSession {
    #[serde(default)]
    name: PatchField<String>,
    #[serde(default)]
    metadata: PatchField<Value>,
}

#[derive(Default)]
enum PatchField<T> {
    #[default]
    Missing,
    Null,
    Value(T),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for PatchField<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<T>::deserialize(deserializer).map(|value| match value {
            Some(value) => Self::Value(value),
            None => Self::Null,
        })
    }
}

#[derive(Serialize)]
struct SessionPage {
    data: Vec<SessionListItem>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct SessionListItem {
    id: Uuid,
    project_id: String,
    harness_id: String,
    harness_version: String,
    name: Option<String>,
    metadata: Value,
    status: SessionStatus,
    state_version: i64,
    processing_health: ProcessingHealth,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct SessionDetail {
    #[serde(flatten)]
    summary: SessionListItem,
    configuration: Box<RawValue>,
    history_through_sequence: i64,
    update_through_sequence: i64,
}

#[derive(Serialize)]
struct ProcessingHealth {
    enabled: bool,
    error: Option<Value>,
    revision: i64,
    blocked_event_id: Option<Uuid>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/sessions", post(create).get(list))
        .route("/sessions/{session_id}", get(detail).patch(update))
}

async fn create(
    State(state): State<AppState>,
    request: Result<Json<CreateSession>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(request) = request.map_err(json_rejection)?;
    validate_text("project_id", &request.project_id, MAX_PROJECT_ID)?;
    validate_text("harness_id", &request.harness_id, MAX_HARNESS_ID)?;
    validate_text(
        "harness_version",
        &request.harness_version,
        MAX_HARNESS_VERSION,
    )?;
    if let Some(name) = &request.name {
        validate_optional_text("name", name, MAX_NAME)?;
    }
    validate_metadata(&request.metadata)?;

    let harness_id = HarnessId(request.harness_id);
    let harness_version = HarnessVersion(request.harness_version);
    let harness = state
        .registry
        .get(&harness_id, &harness_version)
        .ok_or_else(|| {
            ApiError::not_found(
                "harness_version_not_found",
                format!(
                    "harness {} version {} was not found",
                    harness_id.0, harness_version.0
                ),
            )
        })?;
    let initial_state = harness
        .initialize(&request.configuration)
        .map_err(|error| runtime_initialization(error, &harness_id, &harness_version))?;
    let session = state
        .store
        .create_session(NewSession {
            id: SessionId(request.id),
            project_id: ProjectId(request.project_id),
            harness_id,
            harness_version,
            configuration: request.configuration,
            state: initial_state,
            name: request.name,
            metadata: request.metadata,
        })
        .await?;
    let location = format!("/v1/sessions/{}", session.id);
    let detail = SessionDetail::from(session);
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(detail),
    )
        .into_response())
}

async fn list(
    State(state): State<AppState>,
    query: Result<Query<ListSessions>, QueryRejection>,
) -> Result<Json<SessionPage>, ApiError> {
    let Query(query) = query.map_err(|error| ApiError::bad_request(error.body_text()))?;
    if let Some(project_id) = &query.project_id {
        validate_text("project_id", project_id, MAX_PROJECT_ID)?;
    }
    if let Some(harness_id) = &query.harness_id {
        validate_text("harness_id", harness_id, MAX_HARNESS_ID)?;
    }
    let cursor = query.cursor.as_deref().map(decode_cursor).transpose()?;
    let page = state
        .store
        .list_sessions(SessionFilter {
            project_id: query.project_id.map(ProjectId),
            status: query.status,
            harness_id: query.harness_id.map(HarnessId),
            cursor,
            limit: query.limit,
        })
        .await?;
    Ok(Json(SessionPage {
        data: page
            .sessions
            .into_iter()
            .map(SessionListItem::from)
            .collect(),
        next_cursor: page.next_cursor.map(encode_cursor).transpose()?,
    }))
}

async fn detail(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<SessionDetail>, ApiError> {
    let Path(session_id) = path.map_err(path_rejection)?;
    let id = parse_session_id(&session_id)?;
    let (update_through_sequence, _) = state.store.update_cursor(id).await?;
    let mut detail = SessionDetail::from(state.store.session(id).await?);
    detail.update_through_sequence = update_through_sequence;
    Ok(Json(detail))
}

async fn update(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    request: Result<Json<PatchSession>, JsonRejection>,
) -> Result<Json<SessionDetail>, ApiError> {
    let Path(session_id) = path.map_err(path_rejection)?;
    let id = parse_session_id(&session_id)?;
    let Json(request) = request.map_err(json_rejection)?;
    let name = match request.name {
        PatchField::Missing => None,
        PatchField::Null => Some(None),
        PatchField::Value(value) => {
            validate_optional_text("name", &value, MAX_NAME)?;
            Some(Some(value))
        }
    };
    let metadata = match request.metadata {
        PatchField::Missing => None,
        PatchField::Null => {
            return Err(ApiError::bad_request("metadata must be an object"));
        }
        PatchField::Value(value) => {
            validate_metadata(&value)?;
            Some(value)
        }
    };
    if name.is_none() && metadata.is_none() {
        return Err(ApiError::bad_request(
            "at least one of name or metadata must be supplied",
        ));
    }
    let (update_through_sequence, _) = state.store.update_cursor(id).await?;
    let session = state
        .store
        .patch_session_metadata(id, name, metadata)
        .await?;
    let mut detail = SessionDetail::from(session);
    detail.update_through_sequence = update_through_sequence;
    Ok(Json(detail))
}

impl From<SessionRecord> for SessionDetail {
    fn from(session: SessionRecord) -> Self {
        Self {
            summary: SessionListItem {
                id: session.id.0,
                project_id: session.project_id.0,
                harness_id: session.harness_id.0,
                harness_version: session.harness_version.0,
                name: session.name,
                metadata: session.metadata,
                status: session.status,
                state_version: session.state_version.0,
                processing_health: ProcessingHealth {
                    enabled: session.processing_enabled,
                    error: session.processing_error,
                    revision: session.processing_revision,
                    blocked_event_id: session.blocked_event_id.map(|id| id.0),
                },
                created_at: session.created_at,
                updated_at: session.updated_at,
            },
            configuration: session.configuration,
            history_through_sequence: session.next_history_sequence.0 - 1,
            update_through_sequence: 0,
        }
    }
}

impl From<SessionSummary> for SessionListItem {
    fn from(session: SessionSummary) -> Self {
        Self {
            id: session.id.0,
            project_id: session.project_id.0,
            harness_id: session.harness_id.0,
            harness_version: session.harness_version.0,
            name: session.name,
            metadata: session.metadata,
            status: session.status,
            state_version: session.state_version.0,
            processing_health: ProcessingHealth {
                enabled: session.processing_enabled,
                error: session.processing_error,
                revision: session.processing_revision,
                blocked_event_id: session.blocked_event_id.map(|id| id.0),
            },
            created_at: session.created_at,
            updated_at: session.updated_at,
        }
    }
}

pub(crate) fn parse_session_id(value: &str) -> Result<SessionId, ApiError> {
    value
        .parse::<Uuid>()
        .map(SessionId)
        .map_err(|_| ApiError::bad_request("session_id must be a UUID"))
}

fn validate_text(field: &str, value: &str, max: usize) -> Result<(), ApiError> {
    if value.is_empty() || value.len() > max {
        return Err(ApiError::bad_request(format!(
            "{field} must contain between 1 and {max} bytes"
        )));
    }
    Ok(())
}

fn validate_optional_text(field: &str, value: &str, max: usize) -> Result<(), ApiError> {
    if value.len() > max {
        return Err(ApiError::bad_request(format!(
            "{field} must contain at most {max} bytes"
        )));
    }
    Ok(())
}

fn validate_metadata(value: &Value) -> Result<(), ApiError> {
    if !value.is_object() {
        return Err(ApiError::bad_request("metadata must be an object"));
    }
    Ok(())
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

fn decode_cursor(value: &str) -> Result<SessionCursor, ApiError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| ApiError::bad_request("cursor is invalid"))?;
    let cursor: Cursor =
        serde_json::from_slice(&bytes).map_err(|_| ApiError::bad_request("cursor is invalid"))?;
    Ok(SessionCursor {
        created_at: cursor.created_at,
        id: SessionId(cursor.id),
    })
}

fn encode_cursor(cursor: SessionCursor) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(&Cursor {
        created_at: cursor.created_at,
        id: cursor.id.0,
    })
    .map_err(|error| {
        tracing::error!(error = %error, "failed to encode session cursor");
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "an internal error occurred",
        )
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub(crate) fn json_rejection(error: JsonRejection) -> ApiError {
    let rejection_status = error.status();
    let (status, code) = match rejection_status {
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            (StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type")
        }
        StatusCode::PAYLOAD_TOO_LARGE => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
        _ => (StatusCode::BAD_REQUEST, "invalid_json"),
    };
    ApiError::new(status, code, error.body_text())
}

pub(crate) fn path_rejection(error: PathRejection) -> ApiError {
    ApiError::bad_request(error.body_text())
}
