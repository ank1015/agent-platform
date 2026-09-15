use crate::{ApiError, AppState};
use agent_contracts::EventId;
use axum::{
    Json, Router,
    extract::{
        Path, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::{HeaderMap, StatusCode},
    routing::post,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetryRequest {
    expected_event_id: Uuid,
    expected_processing_revision: i64,
    reason: Option<String>,
}

#[derive(Serialize)]
struct RetryAcknowledgement {
    session_id: Uuid,
    event_id: Uuid,
    processing_revision: i64,
    retried_at: DateTime<Utc>,
}

pub fn router() -> Router<AppState> {
    Router::new().route("/sessions/{session_id}/processing/retry", post(retry))
}

async fn retry(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    request: Result<Json<Box<RawValue>>, JsonRejection>,
) -> Result<(StatusCode, Json<RetryAcknowledgement>), ApiError> {
    let Path(session_id) = path.map_err(super::sessions::path_rejection)?;
    let session_id = super::sessions::parse_session_id(&session_id)?;
    let key = super::inputs::idempotency_key(&headers)?;
    let Json(payload) = request.map_err(super::sessions::json_rejection)?;
    let request: RetryRequest = serde_json::from_str(payload.get())
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    if request.expected_processing_revision < 0 {
        return Err(ApiError::bad_request(
            "expected_processing_revision must be non-negative",
        ));
    }
    if request
        .reason
        .as_ref()
        .is_some_and(|reason| reason.len() > 1000)
    {
        return Err(ApiError::bad_request("reason must be at most 1000 bytes"));
    }
    let result = state
        .store
        .retry_blocked_event(
            session_id,
            EventId(request.expected_event_id),
            request.expected_processing_revision,
            &key,
            &payload,
            request.reason.as_deref(),
        )
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(RetryAcknowledgement {
            session_id: result.session_id.0,
            event_id: result.event_id.0,
            processing_revision: result.processing_revision,
            retried_at: result.retried_at,
        }),
    ))
}
