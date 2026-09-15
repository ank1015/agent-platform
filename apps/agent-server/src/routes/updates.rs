use crate::{ApiError, AppState};
use agent_store::UpdateRecord;
use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::HeaderMap,
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use chrono::{DateTime, Utc};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{convert::Infallible, time::Duration};
use tokio::sync::OwnedSemaphorePermit;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateQuery {
    after_sequence: Option<i64>,
    through_sequence: Option<i64>,
    limit: Option<i64>,
}

#[derive(Serialize)]
struct UpdateItem {
    session_id: Uuid,
    sequence: i64,
    schema_version: i16,
    kind: String,
    payload: Box<RawValue>,
    source_event_id: Option<Uuid>,
    created_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct Page {
    data: Vec<UpdateItem>,
    through_sequence: i64,
    retained_sequence: i64,
    next_after_sequence: Option<i64>,
    has_more: bool,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/sessions/{session_id}/updates", get(list))
        .route("/sessions/{session_id}/updates/stream", get(stream))
}

async fn list(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<UpdateQuery>, QueryRejection>,
) -> Result<Json<Page>, ApiError> {
    let Path(raw) = path.map_err(super::sessions::path_rejection)?;
    let id = super::sessions::parse_session_id(&raw)?;
    let Query(q) = query.map_err(|e| ApiError::bad_request(e.body_text()))?;
    let limit = q.limit.unwrap_or(100);
    if !(1..=999).contains(&limit) {
        return Err(ApiError::bad_request("limit must be 1..999"));
    }
    let page = state
        .store
        .updates(id, q.after_sequence.unwrap_or(0), q.through_sequence, limit)
        .await?;
    let next_after_sequence = page.updates.last().map(|x| x.sequence);
    Ok(Json(Page {
        data: page.updates.into_iter().map(UpdateItem::from).collect(),
        through_sequence: page.through_sequence,
        retained_sequence: page.retained_sequence,
        next_after_sequence,
        has_more: page.has_more,
    }))
}

async fn stream(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<UpdateQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let Path(raw) = path.map_err(super::sessions::path_rejection)?;
    let id = super::sessions::parse_session_id(&raw)?;
    let Query(q) = query.map_err(|e| ApiError::bad_request(e.body_text()))?;
    if q.through_sequence.is_some() || q.limit.is_some() {
        return Err(ApiError::bad_request("SSE accepts only after_sequence"));
    }
    let last = headers.get("last-event-id");
    if last.is_some() && q.after_sequence.is_some() {
        return Err(ApiError::bad_request(
            "use Last-Event-ID or after_sequence, not both",
        ));
    }
    let after = if let Some(last) = last {
        last.to_str()
            .map_err(|_| ApiError::bad_request("invalid Last-Event-ID"))?
            .parse::<i64>()
            .map_err(|_| ApiError::bad_request("invalid Last-Event-ID"))?
    } else {
        q.after_sequence.unwrap_or(0)
    };
    let permit = state
        .metrics
        .sse_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError::new(
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "too_many_streams",
                "SSE subscription limit reached",
            )
        })?;
    state.store.updates(id, after, None, 1).await?;
    let feed = update_stream(state, id, after, permit);
    Ok(Sse::new(feed).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

fn update_stream(
    state: AppState,
    id: agent_contracts::SessionId,
    after: i64,
    permit: OwnedSemaphorePermit,
) -> impl Stream<Item = Result<Event, Infallible>> + Send + 'static {
    let draining = state.readiness.draining();
    async_stream::stream! {
        let _permit = permit;
        let mut cursor = after;
        'feed: loop {
            if draining.is_cancelled() {
                break;
            }
            let page = tokio::select! {
                biased;
                _ = draining.cancelled() => break 'feed,
                result = state.store.updates(id, cursor, None, 100) => result,
            };
            match page {
                Ok(page) => {
                    let has_more = page.has_more;
                    for update in page.updates {
                        if draining.is_cancelled() {
                            break 'feed;
                        }
                        cursor = update.sequence;
                        let item = UpdateItem::from(update);
                        match serde_json::to_string(&item) {
                            Ok(data) => yield Ok(Event::default()
                                .id(cursor.to_string())
                                .event(item.kind)
                                .data(data)
                                .retry(Duration::from_secs(1))),
                            Err(error) => {
                                tracing::error!(%error, "could not serialize update");
                                break;
                            }
                        }
                    }
                    if has_more {
                        continue;
                    }
                }
                Err(agent_store::StoreError::UpdateCursorExpired) => {
                    yield Ok(Event::default().event("resnapshot_required")
                        .data("update cursor expired; load a fresh session snapshot"));
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, "update stream lost its database read");
                    break;
                }
            }
            tokio::select! {
                biased;
                _ = draining.cancelled() => break 'feed,
                _ = tokio::time::sleep(Duration::from_millis(250)) => {},
            }
        }
    }
}

impl From<UpdateRecord> for UpdateItem {
    fn from(x: UpdateRecord) -> Self {
        Self {
            session_id: x.session_id.0,
            sequence: x.sequence,
            schema_version: x.schema_version,
            kind: x.kind,
            payload: x.payload,
            source_event_id: x.source_event_id.map(|id| id.0),
            created_at: x.created_at,
        }
    }
}
