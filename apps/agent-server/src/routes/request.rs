use crate::{ApiError, AppState};
use axum::{
    extract::{MatchedPath, Request, State},
    http::{HeaderName, HeaderValue, StatusCode},
    middleware::Next,
    response::Response,
};
use std::time::Instant;
use tracing::Instrument;
use uuid::Uuid;

tokio::task_local! {
    static REQUEST_ID: Uuid;
}

static REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

pub fn request_id() -> Uuid {
    REQUEST_ID
        .try_with(|id| *id)
        .unwrap_or_else(|_| Uuid::new_v4())
}

pub async fn context(request: Request, next: Next) -> Response {
    let id = Uuid::new_v4();
    let method = request.method().clone();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or_else(|| request.uri().path())
        .to_owned();
    let started = Instant::now();
    let span = tracing::info_span!(
        "http_request",
        request_id = %id,
        method = %method,
        route = %path
    );
    let mut response = REQUEST_ID
        .scope(id, next.run(request).instrument(span))
        .await;
    response.headers_mut().insert(
        REQUEST_ID_HEADER.clone(),
        HeaderValue::from_str(&id.to_string()).expect("UUID is a valid header value"),
    );
    tracing::info!(
        request_id = %id,
        method = %method,
        route = %path,
        status = response.status().as_u16(),
        duration_ms = started.elapsed().as_millis(),
        "request completed"
    );
    response
}

pub async fn admit(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if !state.readiness.is_accepting() {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "shutting_down",
            "the service is shutting down",
        ));
    }
    Ok(next.run(request).await)
}
