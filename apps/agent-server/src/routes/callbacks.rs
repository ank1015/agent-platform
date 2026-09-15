use crate::{ApiError, AppState};
use agent_contracts::GatewayConnectionId;
use agent_gateways::{CallbackHeaders, CallbackVerificationError, GatewayKind};
use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use serde_json::value::RawValue;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/callbacks/llm/{connection_id}", post(llm))
        .route("/callbacks/execution/{connection_id}", post(execution))
}

async fn llm(
    state: State<AppState>,
    path: Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    receive(state, path, headers, body, GatewayKind::Llm).await
}

async fn execution(
    state: State<AppState>,
    path: Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    receive(state, path, headers, body, GatewayKind::Execution).await
}

async fn receive(
    State(state): State<AppState>,
    Path(connection_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
    kind: GatewayKind,
) -> Result<StatusCode, ApiError> {
    let prefix = match kind {
        GatewayKind::Llm => "x-llm-gateway",
        GatewayKind::Execution => "x-execution-gateway",
    };
    let get = |suffix: &str| {
        let name = format!("{prefix}-{suffix}");
        headers.get(name).and_then(|value| value.to_str().ok())
    };
    let notification = state
        .callback_verifiers
        .verify(
            &GatewayConnectionId(connection_id.clone()),
            kind,
            CallbackHeaders {
                event_id: get("event-id").ok_or_else(invalid_signature)?,
                timestamp: get("timestamp").ok_or_else(invalid_signature)?,
                signature: get("signature").ok_or_else(invalid_signature)?,
            },
            &body,
            chrono::Utc::now().timestamp(),
            state.callback_tolerance,
        )
        .map_err(verification_error)?;
    let raw = RawValue::from_string(
        String::from_utf8(body.to_vec())
            .map_err(|_| ApiError::bad_request("callback body must be UTF-8 JSON"))?,
    )
    .map_err(|_| ApiError::bad_request("callback body must be valid JSON"))?;
    state
        .store
        .record_callback_receipt(
            GatewayConnectionId(connection_id),
            notification.event_id,
            notification.job_id,
            raw.as_ref(),
        )
        .await?;
    Ok(StatusCode::ACCEPTED)
}

fn invalid_signature() -> ApiError {
    ApiError::new(
        StatusCode::UNAUTHORIZED,
        "invalid_callback_signature",
        "callback authentication failed",
    )
}

fn verification_error(error: CallbackVerificationError) -> ApiError {
    match error {
        CallbackVerificationError::InvalidPayload => {
            ApiError::bad_request("callback payload is invalid")
        }
        CallbackVerificationError::NotConfigured => ApiError::not_found(
            "callback_connection_not_found",
            "callback connection is not configured",
        ),
        CallbackVerificationError::InvalidHeaders
        | CallbackVerificationError::Stale
        | CallbackVerificationError::InvalidSignature => invalid_signature(),
    }
}
