use crate::{ApiError, AppState};
use agent_contracts::{ContentPart, Message};
use agent_store::{EnqueueResult, EventRecord, EventType, NewExternalEvent};
use axum::{
    Json, Router,
    extract::{
        Path, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::{HeaderMap, StatusCode, header::HeaderName},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};
use url::Url;
use uuid::Uuid;

const MAX_IDEMPOTENCY_KEY_BYTES: usize = 200;
static IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageInput {
    message: Box<RawValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancellationInput {
    #[serde(default, rename = "reason")]
    _reason: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct EventAcknowledgement {
    session_id: Uuid,
    event_id: Uuid,
    event_sequence: i64,
    accepted_at: chrono::DateTime<chrono::Utc>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/sessions/{session_id}/messages", post(message))
        .route("/sessions/{session_id}/cancel", post(cancel))
}

async fn message(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    request: Result<Json<Box<RawValue>>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Path(session_id) = path.map_err(super::sessions::path_rejection)?;
    let session_id = super::sessions::parse_session_id(&session_id)?;
    let key = idempotency_key(&headers)?;
    let Json(payload) = request.map_err(super::sessions::json_rejection)?;
    let envelope: MessageInput = serde_json::from_str(payload.get())
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let message: Message = serde_json::from_str(envelope.message.get()).map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_message",
            "message does not match the user-message contract",
        )
    })?;
    validate_user_message(&message, envelope.message.as_ref())?;

    let result = state
        .store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::UserMessage,
            payload,
            idempotency_key: Some(key),
        })
        .await?;
    accepted(event(result))
}

async fn cancel(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    request: Result<Json<Box<RawValue>>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Path(session_id) = path.map_err(super::sessions::path_rejection)?;
    let session_id = super::sessions::parse_session_id(&session_id)?;
    let key = idempotency_key(&headers)?;
    let Json(payload) = request.map_err(super::sessions::json_rejection)?;
    let _: CancellationInput = serde_json::from_str(payload.get())
        .map_err(|error| ApiError::bad_request(error.to_string()))?;

    let result = state
        .store
        .enqueue_external_event(NewExternalEvent {
            session_id,
            event_type: EventType::CancellationRequested,
            payload,
            idempotency_key: Some(key),
        })
        .await?;
    accepted(event(result))
}

pub(crate) fn idempotency_key(headers: &HeaderMap) -> Result<String, ApiError> {
    let key = headers
        .get(&IDEMPOTENCY_KEY)
        .ok_or_else(|| ApiError::bad_request("Idempotency-Key header is required"))?
        .to_str()
        .map_err(|_| ApiError::bad_request("Idempotency-Key header is invalid"))?;
    if key.is_empty()
        || key.len() > MAX_IDEMPOTENCY_KEY_BYTES
        || key.chars().any(char::is_whitespace)
    {
        return Err(ApiError::bad_request(format!(
            "Idempotency-Key must contain 1 to {MAX_IDEMPOTENCY_KEY_BYTES} bytes without whitespace"
        )));
    }
    Ok(key.to_owned())
}

pub(crate) fn acknowledgement(event: EventRecord) -> EventAcknowledgement {
    EventAcknowledgement {
        session_id: event.session_id.0,
        event_id: event.id.0,
        event_sequence: event.sequence.0,
        accepted_at: event.created_at,
    }
}

fn event(result: EnqueueResult) -> EventRecord {
    match result {
        EnqueueResult::Inserted(event) | EnqueueResult::Existing(event) => event,
    }
}

fn accepted(event: EventRecord) -> Result<Response, ApiError> {
    Ok((StatusCode::ACCEPTED, Json(acknowledgement(event))).into_response())
}

fn validate_user_message(message: &Message, raw: &RawValue) -> Result<(), ApiError> {
    let Message::User { content, .. } = message else {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_message",
            "only user messages can be submitted to this endpoint",
        ));
    };
    let value: Value = serde_json::from_str(raw.get()).map_err(|_| invalid_message())?;
    let object = value.as_object().ok_or_else(invalid_message)?;
    ensure_known_fields(
        object.keys().map(String::as_str),
        &["role", "id", "timestamp", "metadata", "content"],
    )?;
    let raw_content = object
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(invalid_message)?;
    for (part, raw_part) in content.iter().zip(raw_content) {
        let object = raw_part.as_object().ok_or_else(invalid_message)?;
        match part {
            ContentPart::Text { .. } => ensure_known_fields(
                object.keys().map(String::as_str),
                &["type", "text", "metadata"],
            )?,
            ContentPart::Image { url, .. } => {
                ensure_known_fields(
                    object.keys().map(String::as_str),
                    &["type", "url", "detail", "metadata"],
                )?;
                let parsed = Url::parse(url).map_err(|_| invalid_message())?;
                if !matches!(parsed.scheme(), "http" | "https") || parsed.host().is_none() {
                    return Err(invalid_message());
                }
            }
        }
    }
    Ok(())
}

fn ensure_known_fields<'a>(
    fields: impl Iterator<Item = &'a str>,
    allowed: &[&str],
) -> Result<(), ApiError> {
    if fields.into_iter().any(|field| !allowed.contains(&field)) {
        return Err(invalid_message());
    }
    Ok(())
}

fn invalid_message() -> ApiError {
    ApiError::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid_message",
        "message does not match the user-message contract",
    )
}
