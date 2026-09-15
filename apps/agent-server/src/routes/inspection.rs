use crate::{ApiError, AppState};
use agent_contracts::{EventId, OperationId, OperationKind};
use agent_store::{
    EventRecord, EventStatus, EventSummary, EventType, HandlerAttemptRecord,
    OperationAttemptRecord, OperationRecord, OperationStatus, OperationSummary,
};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::StatusCode,
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListOperations {
    cursor: Option<String>,
    limit: Option<i64>,
    kind: Option<OperationKind>,
    status: Option<String>,
    source_event_id: Option<Uuid>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListEvents {
    after_sequence: Option<i64>,
    limit: Option<i64>,
    status: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListAttempts {
    after_attempt: Option<i32>,
    limit: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct OperationCursor {
    created_at: DateTime<Utc>,
    id: Uuid,
}

#[derive(Serialize)]
struct Page<T> {
    data: Vec<T>,
    next_cursor: Option<String>,
    has_more: bool,
}

#[derive(Serialize)]
struct OperationItem {
    id: Uuid,
    session_id: Uuid,
    source_event_id: Uuid,
    kind: OperationKind,
    status: &'static str,
    gateway_connection_id: Option<String>,
    gateway_job_id: Option<Uuid>,
    target_operation_id: Option<Uuid>,
    previous_operation_id: Option<Uuid>,
    request_retention: RequestRetention,
    created_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct RequestRetention {
    available: bool,
    eligible_for_cleanup_at: Option<DateTime<Utc>>,
    removed_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct OperationDetail {
    #[serde(flatten)]
    summary: OperationItem,
    result: Option<Box<RawValue>>,
    error: Option<Value>,
    request_hash: String,
}

#[derive(Serialize)]
struct EventItem {
    id: Uuid,
    session_id: Uuid,
    sequence: i64,
    event_type: &'static str,
    status: &'static str,
    operation_id: Option<Uuid>,
    wait_id: Option<Uuid>,
    created_at: DateTime<Utc>,
    handled_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct EventDetail {
    #[serde(flatten)]
    summary: EventItem,
    payload: Box<RawValue>,
    current_failure_count: i64,
}

#[derive(Serialize)]
struct HandlerAttemptItem {
    id: Uuid,
    event_id: Uuid,
    attempt_number: i32,
    status: String,
    input_state_version: i64,
    history_through_sequence: i64,
    error: Option<Value>,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct OperationAttemptItem {
    id: Uuid,
    operation_id: Uuid,
    attempt_number: i32,
    phase: String,
    status: String,
    http_status: Option<i32>,
    error: Option<Value>,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct RequestPayload {
    request: Box<RawValue>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/sessions/{session_id}/operations", get(operations))
        .route(
            "/sessions/{session_id}/operations/{operation_id}",
            get(operation_detail),
        )
        .route(
            "/sessions/{session_id}/operations/{operation_id}/request",
            get(operation_request),
        )
        .route(
            "/sessions/{session_id}/operations/{operation_id}/attempts",
            get(operation_attempts),
        )
        .route("/sessions/{session_id}/events", get(events))
        .route(
            "/sessions/{session_id}/events/{event_id}",
            get(event_detail),
        )
        .route(
            "/sessions/{session_id}/events/{event_id}/attempts",
            get(handler_attempts),
        )
}

fn uuid(raw: &str, label: &str) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|_| ApiError::bad_request(format!("invalid {label} ID")))
}
fn session(raw: &str) -> Result<agent_contracts::SessionId, ApiError> {
    super::sessions::parse_session_id(raw)
}
fn limit(raw: Option<i64>) -> Result<i64, ApiError> {
    let x = raw.unwrap_or(50);
    if !(1..=100).contains(&x) {
        return Err(ApiError::bad_request("limit must be 1..100"));
    }
    Ok(x)
}
fn status(raw: Option<String>) -> Result<Option<OperationStatus>, ApiError> {
    raw.map(|x| match x.as_str() {
        "pending" => Ok(OperationStatus::Pending),
        "submitting" => Ok(OperationStatus::Submitting),
        "accepted" => Ok(OperationStatus::Accepted),
        "succeeded" => Ok(OperationStatus::Succeeded),
        "failed" => Ok(OperationStatus::Failed),
        "cancelled" => Ok(OperationStatus::Cancelled),
        "unknown" => Ok(OperationStatus::Unknown),
        _ => Err(ApiError::bad_request("invalid operation status")),
    })
    .transpose()
}
fn event_status(raw: Option<String>) -> Result<Option<EventStatus>, ApiError> {
    raw.map(|x| match x.as_str() {
        "pending" => Ok(EventStatus::Pending),
        "processing" => Ok(EventStatus::Processing),
        "handled" => Ok(EventStatus::Handled),
        "blocked" => Ok(EventStatus::Blocked),
        _ => Err(ApiError::bad_request("invalid event status")),
    })
    .transpose()
}
fn operation_status(x: OperationStatus) -> &'static str {
    match x {
        OperationStatus::Pending => "pending",
        OperationStatus::Submitting => "submitting",
        OperationStatus::Accepted => "accepted",
        OperationStatus::Succeeded => "succeeded",
        OperationStatus::Failed => "failed",
        OperationStatus::Cancelled => "cancelled",
        OperationStatus::Unknown => "unknown",
    }
}
fn event_status_text(x: EventStatus) -> &'static str {
    match x {
        EventStatus::Pending => "pending",
        EventStatus::Processing => "processing",
        EventStatus::Handled => "handled",
        EventStatus::Blocked => "blocked",
    }
}
fn event_type_text(x: EventType) -> &'static str {
    x.as_str()
}

fn encode_cursor(x: &OperationSummary) -> Result<String, ApiError> {
    let raw = serde_json::to_vec(&OperationCursor {
        created_at: x.created_at,
        id: x.id.0,
    })
    .map_err(|_| ApiError::bad_request("invalid operation cursor"))?;
    Ok(URL_SAFE_NO_PAD.encode(raw))
}
fn decode_cursor(raw: &str) -> Result<(DateTime<Utc>, Uuid), ApiError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| ApiError::bad_request("invalid operation cursor"))?;
    let x: OperationCursor = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::bad_request("invalid operation cursor"))?;
    Ok((x.created_at, x.id))
}

async fn operations(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<ListOperations>, QueryRejection>,
) -> Result<Json<Page<OperationItem>>, ApiError> {
    let Path(raw) = path.map_err(super::sessions::path_rejection)?;
    let id = session(&raw)?;
    let Query(q) = query.map_err(|e| ApiError::bad_request(e.body_text()))?;
    let limit = limit(q.limit)?;
    let cursor = q.cursor.as_deref().map(decode_cursor).transpose()?;
    let mut rows = state
        .store
        .operations_summary(
            id,
            cursor,
            limit + 1,
            q.kind,
            status(q.status)?,
            q.source_event_id.map(EventId),
        )
        .await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if has_more {
        rows.last().map(encode_cursor).transpose()?
    } else {
        None
    };
    Ok(Json(Page {
        data: rows.into_iter().map(OperationItem::from).collect(),
        next_cursor,
        has_more,
    }))
}

async fn operation_detail(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Json<OperationDetail>, ApiError> {
    let Path((raw, operation)) = path.map_err(super::sessions::path_rejection)?;
    let id = session(&raw)?;
    let operation_id = OperationId(uuid(&operation, "operation")?);
    let record = state
        .store
        .operation_for_session(id, operation_id)
        .await?
        .ok_or(agent_store::StoreError::OperationNotFound(operation_id))?;
    let (available, expires_at, removed_at) = state
        .store
        .operation_request_status(id, operation_id)
        .await?;
    Ok(Json(OperationDetail {
        summary: OperationItem::from_record(
            &record,
            RequestRetention {
                available,
                eligible_for_cleanup_at: expires_at,
                removed_at,
            },
        ),
        result: record.result,
        error: record.error,
        request_hash: record.request_hash,
    }))
}

async fn operation_request(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Json<RequestPayload>, ApiError> {
    let Path((raw, operation)) = path.map_err(super::sessions::path_rejection)?;
    let id = session(&raw)?;
    let operation_id = OperationId(uuid(&operation, "operation")?);
    let request = state
        .store
        .operation_request_for_session(id, operation_id)
        .await?
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::GONE,
                "request_payload_expired",
                "operation request payload was removed",
            )
        })?;
    Ok(Json(RequestPayload { request }))
}

async fn events(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<ListEvents>, QueryRejection>,
) -> Result<Json<Page<EventItem>>, ApiError> {
    let Path(raw) = path.map_err(super::sessions::path_rejection)?;
    let id = session(&raw)?;
    let Query(q) = query.map_err(|e| ApiError::bad_request(e.body_text()))?;
    let after = q.after_sequence.unwrap_or(0);
    let limit = limit(q.limit)?;
    let mut rows = state
        .store
        .events(id, after, limit + 1, event_status(q.status)?)
        .await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if has_more {
        rows.last().map(|x| x.sequence.0.to_string())
    } else {
        None
    };
    Ok(Json(Page {
        data: rows.into_iter().map(EventItem::from).collect(),
        next_cursor,
        has_more,
    }))
}

async fn event_detail(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Json<EventDetail>, ApiError> {
    let Path((raw, event)) = path.map_err(super::sessions::path_rejection)?;
    let id = session(&raw)?;
    let event_id = EventId(uuid(&event, "event")?);
    state.store.session_exists(id).await?;
    let record = state
        .store
        .event_for_session(id, event_id)
        .await?
        .ok_or(agent_store::StoreError::EventNotFound(event_id))?;
    let count = state.store.handler_failure_count(event_id).await?;
    let summary = EventItem::from_record(&record);
    Ok(Json(EventDetail {
        summary,
        payload: record.payload,
        current_failure_count: count,
    }))
}

async fn handler_attempts(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    query: Result<Query<ListAttempts>, QueryRejection>,
) -> Result<Json<Page<HandlerAttemptItem>>, ApiError> {
    let Path((raw, event)) = path.map_err(super::sessions::path_rejection)?;
    let id = session(&raw)?;
    let event_id = EventId(uuid(&event, "event")?);
    let Query(q) = query.map_err(|e| ApiError::bad_request(e.body_text()))?;
    let limit = limit(q.limit)?;
    let mut rows = state
        .store
        .handler_attempts(id, event_id, q.after_attempt.unwrap_or(0), limit + 1)
        .await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if has_more {
        rows.last().map(|x| x.attempt_number.to_string())
    } else {
        None
    };
    Ok(Json(Page {
        data: rows.into_iter().map(HandlerAttemptItem::from).collect(),
        next_cursor,
        has_more,
    }))
}

async fn operation_attempts(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    query: Result<Query<ListAttempts>, QueryRejection>,
) -> Result<Json<Page<OperationAttemptItem>>, ApiError> {
    let Path((raw, operation)) = path.map_err(super::sessions::path_rejection)?;
    let id = session(&raw)?;
    let operation_id = OperationId(uuid(&operation, "operation")?);
    let Query(q) = query.map_err(|e| ApiError::bad_request(e.body_text()))?;
    let limit = limit(q.limit)?;
    let mut rows = state
        .store
        .operation_attempts(id, operation_id, q.after_attempt.unwrap_or(0), limit + 1)
        .await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if has_more {
        rows.last().map(|x| x.attempt_number.to_string())
    } else {
        None
    };
    Ok(Json(Page {
        data: rows.into_iter().map(OperationAttemptItem::from).collect(),
        next_cursor,
        has_more,
    }))
}

impl From<OperationSummary> for OperationItem {
    fn from(x: OperationSummary) -> Self {
        Self {
            id: x.id.0,
            session_id: x.session_id.0,
            source_event_id: x.source_event_id.0,
            kind: x.kind,
            status: operation_status(x.status),
            gateway_connection_id: x.gateway_connection_id.map(|id| id.0),
            gateway_job_id: x.gateway_job_id,
            target_operation_id: x.target_operation_id.map(|id| id.0),
            previous_operation_id: x.previous_operation_id.map(|id| id.0),
            request_retention: RequestRetention {
                available: x.request_available,
                eligible_for_cleanup_at: x.request_expires_at,
                removed_at: x.request_removed_at,
            },
            created_at: x.created_at,
            completed_at: x.completed_at,
        }
    }
}
impl OperationItem {
    fn from_record(x: &OperationRecord, retention: RequestRetention) -> Self {
        Self {
            id: x.id.0,
            session_id: x.session_id.0,
            source_event_id: x.source_event_id.0,
            kind: x.kind,
            status: operation_status(x.status),
            gateway_connection_id: x.gateway_connection_id.as_ref().map(|id| id.0.clone()),
            gateway_job_id: x.gateway_job_id,
            target_operation_id: x.target_operation_id.map(|id| id.0),
            previous_operation_id: x.previous_operation_id.map(|id| id.0),
            request_retention: retention,
            created_at: x.created_at,
            completed_at: x.completed_at,
        }
    }
}
impl From<EventSummary> for EventItem {
    fn from(x: EventSummary) -> Self {
        Self {
            id: x.id.0,
            session_id: x.session_id.0,
            sequence: x.sequence.0,
            event_type: event_type_text(x.event_type),
            status: event_status_text(x.status),
            operation_id: x.operation_id.map(|id| id.0),
            wait_id: x.wait_id.map(|id| id.0),
            created_at: x.created_at,
            handled_at: x.handled_at,
        }
    }
}
impl EventItem {
    fn from_record(x: &EventRecord) -> Self {
        Self {
            id: x.id.0,
            session_id: x.session_id.0,
            sequence: x.sequence.0,
            event_type: event_type_text(x.event_type),
            status: event_status_text(x.status),
            operation_id: x.operation_id.map(|id| id.0),
            wait_id: x.wait_id.map(|id| id.0),
            created_at: x.created_at,
            handled_at: x.handled_at,
        }
    }
}
impl From<HandlerAttemptRecord> for HandlerAttemptItem {
    fn from(x: HandlerAttemptRecord) -> Self {
        Self {
            id: x.id,
            event_id: x.event_id.0,
            attempt_number: x.attempt_number,
            status: x.status,
            input_state_version: x.input_state_version,
            history_through_sequence: x.history_through_sequence,
            error: x.error,
            started_at: x.started_at,
            finished_at: x.finished_at,
        }
    }
}
impl From<OperationAttemptRecord> for OperationAttemptItem {
    fn from(x: OperationAttemptRecord) -> Self {
        Self {
            id: x.id,
            operation_id: x.operation_id.0,
            attempt_number: x.attempt_number,
            phase: x.phase,
            status: x.status,
            http_status: x.http_status,
            error: x.error,
            started_at: x.started_at,
            finished_at: x.finished_at,
        }
    }
}
