pub mod auth;
pub mod callbacks;
pub mod harnesses;
pub mod health;
pub mod history;
pub mod inputs;
pub mod inspection;
pub mod metrics;
pub mod processing;
pub mod request;
pub mod sessions;
pub mod updates;
pub mod waits;

use crate::ApiError;

pub async fn not_found() -> ApiError {
    ApiError::not_found("route_not_found", "the requested route was not found")
}

pub async fn method_not_allowed() -> ApiError {
    ApiError::new(
        axum::http::StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "the requested method is not allowed for this route",
    )
}
