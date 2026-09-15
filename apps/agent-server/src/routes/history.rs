use crate::{ApiError, AppState};
use agent_contracts::{EventId, HistoryEntryId, HistorySequence};
use agent_store::HistoryRecord;
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryQuery {
    after_sequence: Option<i64>,
    through_sequence: Option<i64>,
    limit: Option<i64>,
    source_event_id: Option<Uuid>,
}

#[derive(Serialize)]
struct Entry {
    id: Uuid,
    session_id: Uuid,
    sequence: i64,
    source_event_id: Option<Uuid>,
    role: String,
    message: Box<RawValue>,
    created_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct Page {
    data: Vec<Entry>,
    through_sequence: i64,
    next_after_sequence: Option<i64>,
    has_more: bool,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/sessions/{session_id}/history", get(list))
        .route("/sessions/{session_id}/history/{entry_id}", get(detail))
}

async fn list(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Json<Page>, ApiError> {
    let Path(raw_session) = path.map_err(super::sessions::path_rejection)?;
    let id = super::sessions::parse_session_id(&raw_session)?;
    let Query(query) = query.map_err(|e| ApiError::bad_request(e.body_text()))?;
    let after = query.after_sequence.unwrap_or(0);
    let limit = query.limit.unwrap_or(100);
    let current = state.store.history_cursor(id).await?;
    let through = query.through_sequence.unwrap_or(current);
    if after < 0 || !(1..=999).contains(&limit) || through < after || through > current {
        return Err(ApiError::bad_request("invalid history sequence or limit"));
    }
    let mut records = if let Some(event) = query.source_event_id {
        state
            .store
            .history_for_event(
                id,
                EventId(event),
                HistorySequence(after),
                HistorySequence(through),
                limit + 1,
            )
            .await?
    } else {
        state
            .store
            .history(
                id,
                HistorySequence(after),
                HistorySequence(through),
                limit + 1,
            )
            .await?
    };
    let has_more = records.len() > limit as usize;
    records.truncate(limit as usize);
    let next_after_sequence = records.last().map(|entry| entry.sequence.0);
    Ok(Json(Page {
        data: records.into_iter().map(Entry::from).collect(),
        through_sequence: through,
        next_after_sequence,
        has_more,
    }))
}

async fn detail(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Result<Json<Entry>, ApiError> {
    let Path((raw_session, raw_entry)) = path.map_err(super::sessions::path_rejection)?;
    let id = super::sessions::parse_session_id(&raw_session)?;
    let entry_id = Uuid::parse_str(&raw_entry)
        .map_err(|_| ApiError::bad_request("invalid history entry ID"))?;
    state.store.session_exists(id).await?;
    let entry = state
        .store
        .history_entry(id, HistoryEntryId(entry_id), HistorySequence(i64::MAX))
        .await?
        .ok_or_else(|| {
            ApiError::not_found(
                "history_entry_not_found",
                "history entry was not found in this session",
            )
        })?;
    Ok(Json(Entry::from(entry)))
}

impl From<HistoryRecord> for Entry {
    fn from(x: HistoryRecord) -> Self {
        Self {
            id: x.id.0,
            session_id: x.session_id.0,
            sequence: x.sequence.0,
            source_event_id: x.source_event_id.map(|id| id.0),
            role: x.role,
            message: x.message,
            created_at: x.created_at,
        }
    }
}
