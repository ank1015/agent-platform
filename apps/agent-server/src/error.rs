use agent_contracts::{HarnessId, HarnessVersion};
use agent_runtime::RuntimeError;
use agent_store::StoreError;
use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use sqlx::error::DatabaseError;
use uuid::Uuid;

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[derive(Serialize)]
struct ErrorDetail {
    code: &'static str,
    message: String,
    request_id: Uuid,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    pub fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let unauthorized = self.status == StatusCode::UNAUTHORIZED;
        let mut response = (
            self.status,
            Json(ErrorBody {
                error: ErrorDetail {
                    code: self.code,
                    message: self.message,
                    request_id: crate::routes::request::request_id(),
                },
            }),
        )
            .into_response();
        if unauthorized {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                "Bearer".parse().expect("static header value"),
            );
        }
        response
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::SessionNotFound(id) => {
                Self::not_found("session_not_found", format!("session {id} was not found"))
            }
            StoreError::SessionAlreadyExists(_) => Self::new(
                StatusCode::CONFLICT,
                "session_already_exists",
                "a session with this ID already exists",
            ),
            StoreError::WaitNotFound(id) => Self::not_found(
                "wait_not_found",
                format!("wait {id} was not found in this session"),
            ),
            StoreError::OperationNotFound(id) => Self::not_found(
                "operation_not_found",
                format!("operation {id} was not found in this session"),
            ),
            StoreError::EventNotFound(id) => Self::not_found(
                "event_not_found",
                format!("event {id} was not found in this session"),
            ),
            StoreError::IdempotencyConflict => Self::new(
                StatusCode::CONFLICT,
                "idempotency_conflict",
                "the idempotency key was already used with a different request",
            ),
            StoreError::WaitNotResolvable => Self::new(
                StatusCode::CONFLICT,
                "wait_not_resolvable",
                "the wait cannot accept an external response",
            ),
            StoreError::StaleProcessingRetry => Self::new(
                StatusCode::CONFLICT,
                "stale_processing_retry",
                "the session is not blocked on the expected event and processing revision",
            ),
            StoreError::UpdateCursorExpired => Self::new(
                StatusCode::GONE,
                "resnapshot_required",
                "the update cursor expired; load a fresh session snapshot",
            ),
            StoreError::InvalidInput(message) => Self::bad_request(message),
            StoreError::Database(database) if database_is_unavailable(&database) => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_unavailable",
                "the database is unavailable",
            ),
            _ => {
                tracing::error!("unexpected store error");
                Self::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "an internal error occurred",
                )
            }
        }
    }
}

fn database_is_unavailable(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_) => true,
        sqlx::Error::Database(error) => transient_database_code(error.as_ref()),
        _ => false,
    }
}

fn transient_database_code(error: &dyn DatabaseError) -> bool {
    error.code().is_some_and(|code| {
        code.starts_with("08")
            || code.starts_with("53")
            || matches!(code.as_ref(), "57P01" | "57P02" | "57P03")
    })
}

pub fn runtime_initialization(
    error: RuntimeError,
    harness_id: &HarnessId,
    harness_version: &HarnessVersion,
) -> ApiError {
    match error {
        RuntimeError::InvalidConfiguration(_) => ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_configuration",
            "configuration is invalid for the selected harness version",
        ),
        RuntimeError::Initialization(_) => {
            tracing::error!(
                harness_id = %harness_id.0,
                harness_version = %harness_version.0,
                "harness initialization failed"
            );
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "initialization_failed",
                "the harness could not initialize the session",
            )
        }
        _ => {
            tracing::error!(
                harness_id = %harness_id.0,
                harness_version = %harness_version.0,
                "unexpected harness runtime error"
            );
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "an internal error occurred",
            )
        }
    }
}
