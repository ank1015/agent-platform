use crate::{ApiError, AppState};
use agent_contracts::{HarnessId, HarnessVersion};
use axum::{
    Json, Router,
    extract::{Path, State, rejection::PathRejection},
    routing::get,
};
use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
struct HarnessSummary {
    id: String,
    name: String,
    description: String,
}

#[derive(Serialize)]
struct HarnessList {
    data: Vec<HarnessSummary>,
}

#[derive(Serialize)]
struct VersionSummary {
    version: String,
    name: String,
    description: String,
}

#[derive(Serialize)]
struct VersionList {
    data: Vec<VersionSummary>,
}

#[derive(Serialize)]
struct HarnessVersionDetail {
    id: String,
    version: String,
    name: String,
    description: String,
    configuration_schema: Value,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/harnesses", get(list))
        .route("/harnesses/{harness_id}/versions", get(versions))
        .route("/harnesses/{harness_id}/versions/{version}", get(detail))
}

async fn list(State(state): State<AppState>) -> Json<HarnessList> {
    let mut data = Vec::new();
    for description in state.registry.descriptions() {
        if data
            .last()
            .is_some_and(|last: &HarnessSummary| last.id == description.id.0)
        {
            continue;
        }
        data.push(HarnessSummary {
            id: description.id.0.clone(),
            name: description.name.clone(),
            description: description.description.clone(),
        });
    }
    Json(HarnessList { data })
}

async fn versions(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<VersionList>, ApiError> {
    let Path(harness_id) = path.map_err(path_rejection)?;
    let mut data = state
        .registry
        .descriptions()
        .into_iter()
        .filter(|description| description.id.0 == harness_id)
        .map(|description| VersionSummary {
            version: description.version.0.clone(),
            name: description.name.clone(),
            description: description.description.clone(),
        })
        .collect::<Vec<_>>();
    if data.is_empty() {
        return Err(harness_not_found(&harness_id));
    }
    data.sort_by(|a, b| a.version.cmp(&b.version));
    Ok(Json(VersionList { data }))
}

async fn detail(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Json<HarnessVersionDetail>, ApiError> {
    let Path((harness_id, version)) = path.map_err(path_rejection)?;
    let harness = state
        .registry
        .get(
            &HarnessId(harness_id.clone()),
            &HarnessVersion(version.clone()),
        )
        .ok_or_else(|| {
            ApiError::not_found(
                "harness_version_not_found",
                format!("harness {harness_id} version {version} was not found"),
            )
        })?;
    let description = harness.description();
    Ok(Json(HarnessVersionDetail {
        id: description.id.0.clone(),
        version: description.version.0.clone(),
        name: description.name.clone(),
        description: description.description.clone(),
        configuration_schema: harness.configuration_schema().clone(),
    }))
}

fn harness_not_found(id: &str) -> ApiError {
    ApiError::not_found("harness_not_found", format!("harness {id} was not found"))
}

fn path_rejection(error: PathRejection) -> ApiError {
    ApiError::bad_request(error.body_text())
}
