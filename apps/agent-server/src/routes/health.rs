use crate::{ApiError, AppState};
use axum::{Json, extract::State, http::StatusCode};
use serde::Serialize;

#[derive(Serialize)]
pub struct Health {
    status: &'static str,
}

pub async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

pub async fn ready(State(state): State<AppState>) -> Result<Json<Health>, ApiError> {
    if !state.readiness.is_accepting() {
        return Err(not_ready());
    }
    let ready = state.store.schema_ready().await.map_err(|_| {
        tracing::warn!("readiness database check failed");
        not_ready()
    })?;
    if !ready {
        return Err(not_ready());
    }
    Ok(Json(Health { status: "ready" }))
}

fn not_ready() -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "not_ready",
        "the service is not ready",
    )
}
