use crate::{
    EventStatus, EventSummary, EventType, HandlerAttemptRecord, OperationAttemptRecord,
    OperationStatus, OperationSummary, Result, Store, StoreError, UpdatePage, UpdateRecord,
};
use agent_contracts::{
    EventId, EventSequence, HistorySequence, OperationId, OperationKind, SessionId,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, value::RawValue};
use sqlx::{Postgres, QueryBuilder, Row, Transaction, types::Json};
use uuid::Uuid;

pub(crate) async fn emit_update(
    tx: &mut Transaction<'_, Postgres>,
    session_id: SessionId,
    kind: &str,
    payload: &Value,
) -> Result<()> {
    emit_update_with_event(tx, session_id, kind, payload, None).await
}

pub(crate) async fn emit_update_with_event(
    tx: &mut Transaction<'_, Postgres>,
    session_id: SessionId,
    kind: &str,
    payload: &Value,
    source_event_id: Option<EventId>,
) -> Result<()> {
    let sequence: i64 = sqlx::query_scalar(
        "UPDATE sessions SET next_update_sequence=next_update_sequence+1 \
         WHERE id=$1 RETURNING next_update_sequence-1",
    )
    .bind(session_id.0)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO session_updates (session_id,sequence,kind,payload,source_event_id) \
         VALUES ($1,$2,$3,$4::json,$5)",
    )
    .bind(session_id.0)
    .bind(sequence)
    .bind(kind)
    .bind(serde_json::to_string(payload).map_err(|e| StoreError::InvalidInput(e.to_string()))?)
    .bind(source_event_id.map(|id| id.0))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl Store {
    pub async fn history_cursor(&self, session_id: SessionId) -> Result<i64> {
        sqlx::query_scalar("SELECT next_history_sequence-1 FROM sessions WHERE id=$1")
            .bind(session_id.0)
            .fetch_optional(self.pool())
            .await?
            .ok_or(StoreError::SessionNotFound(session_id))
    }

    pub async fn session_exists(&self, session_id: SessionId) -> Result<()> {
        self.history_cursor(session_id).await.map(|_| ())
    }

    async fn event_exists(&self, session_id: SessionId, event_id: EventId) -> Result<bool> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE session_id=$1 AND id=$2)",
        )
        .bind(session_id.0)
        .bind(event_id.0)
        .fetch_one(self.pool())
        .await
        .map_err(StoreError::from)
    }

    async fn operation_exists(&self, session_id: SessionId, id: OperationId) -> Result<bool> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM session_operations WHERE session_id=$1 AND id=$2)",
        )
        .bind(session_id.0)
        .bind(id.0)
        .fetch_one(self.pool())
        .await
        .map_err(StoreError::from)
    }

    pub async fn history_for_event(
        &self,
        session_id: SessionId,
        event_id: EventId,
        after: HistorySequence,
        through: HistorySequence,
        limit: i64,
    ) -> Result<Vec<crate::HistoryRecord>> {
        if !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput("limit must be 1..1000".into()));
        }
        self.update_cursor(session_id).await?;
        let rows = sqlx::query(
            "SELECT id,session_id,sequence,source_event_id,role,message::text AS message,created_at \
             FROM session_history WHERE session_id=$1 AND source_event_id=$2 \
             AND sequence>$3 AND sequence<=$4 ORDER BY sequence LIMIT $5",
        )
        .bind(session_id.0)
        .bind(event_id.0)
        .bind(after.0)
        .bind(through.0)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(super::store::history_from_row).collect()
    }
    pub async fn update_cursor(&self, session_id: SessionId) -> Result<(i64, i64)> {
        let row = sqlx::query(
            "SELECT next_update_sequence-1 AS through_sequence,retained_update_sequence \
             FROM sessions WHERE id=$1",
        )
        .bind(session_id.0)
        .fetch_optional(self.pool())
        .await?
        .ok_or(StoreError::SessionNotFound(session_id))?;
        Ok((
            row.try_get("through_sequence")?,
            row.try_get("retained_update_sequence")?,
        ))
    }

    pub async fn updates(
        &self,
        session_id: SessionId,
        after_sequence: i64,
        through_sequence: Option<i64>,
        limit: i64,
    ) -> Result<UpdatePage> {
        if after_sequence < 0 || !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "invalid update sequence or limit".into(),
            ));
        }
        let mut tx = self.pool().begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        let cursor_row = sqlx::query(
            "SELECT next_update_sequence-1 AS through_sequence,retained_update_sequence \
             FROM sessions WHERE id=$1",
        )
        .bind(session_id.0)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::SessionNotFound(session_id))?;
        let current: i64 = cursor_row.try_get("through_sequence")?;
        let retained: i64 = cursor_row.try_get("retained_update_sequence")?;
        if after_sequence < retained - 1 {
            return Err(StoreError::UpdateCursorExpired);
        }
        let through = through_sequence.unwrap_or(current);
        if through < after_sequence || through > current {
            return Err(StoreError::InvalidInput("invalid through_sequence".into()));
        }
        let rows = sqlx::query(
            "SELECT session_id,sequence,schema_version,kind,payload::text AS payload,source_event_id,created_at \
             FROM session_updates WHERE session_id=$1 AND sequence>$2 AND sequence<=$3 \
             ORDER BY sequence LIMIT $4",
        )
        .bind(session_id.0)
        .bind(after_sequence)
        .bind(through)
        .bind(limit + 1)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        let has_more = rows.len() > limit as usize;
        let updates = rows
            .into_iter()
            .take(limit as usize)
            .map(|row| {
                Ok(UpdateRecord {
                    session_id: SessionId(row.try_get("session_id")?),
                    sequence: row.try_get("sequence")?,
                    schema_version: row.try_get("schema_version")?,
                    kind: row.try_get("kind")?,
                    source_event_id: row
                        .try_get::<Option<Uuid>, _>("source_event_id")?
                        .map(EventId),
                    payload: RawValue::from_string(row.try_get("payload")?).map_err(|e| {
                        StoreError::InvalidStoredJson {
                            kind: "update payload",
                            message: e.to_string(),
                        }
                    })?,
                    created_at: row.try_get("created_at")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(UpdatePage {
            updates,
            through_sequence: through,
            retained_sequence: retained,
            has_more,
        })
    }

    pub async fn prune_updates(&self, before: DateTime<Utc>, session_limit: i64) -> Result<u64> {
        if !(1..=1000).contains(&session_limit) {
            return Err(StoreError::InvalidInput(
                "session limit must be 1..1000".into(),
            ));
        }
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT DISTINCT session_id FROM session_updates WHERE created_at<$1 LIMIT $2",
        )
        .bind(before)
        .bind(session_limit)
        .fetch_all(self.pool())
        .await?;
        let mut removed = 0;
        for id in ids {
            let mut tx = self.pool().begin().await?;
            let locked: Option<i64> = sqlx::query_scalar(
                "SELECT retained_update_sequence FROM sessions WHERE id=$1 FOR UPDATE SKIP LOCKED",
            )
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
            if locked.is_none() {
                continue;
            }
            let sequences: Vec<i64> = sqlx::query_scalar(
                "SELECT sequence FROM session_updates WHERE session_id=$1 AND created_at<$2 \
                 ORDER BY sequence LIMIT 1000",
            )
            .bind(id)
            .bind(before)
            .fetch_all(&mut *tx)
            .await?;
            if let Some(maximum) = sequences.last().copied() {
                let result =
                    sqlx::query("DELETE FROM session_updates WHERE session_id=$1 AND sequence<=$2")
                        .bind(id)
                        .bind(maximum)
                        .execute(&mut *tx)
                        .await?;
                sqlx::query(
                    "UPDATE sessions SET retained_update_sequence=GREATEST(retained_update_sequence,$2) WHERE id=$1",
                ).bind(id).bind(maximum + 1).execute(&mut *tx).await?;
                removed += result.rows_affected();
            }
            tx.commit().await?;
        }
        Ok(removed)
    }

    pub async fn event_for_session(
        &self,
        session_id: SessionId,
        event_id: EventId,
    ) -> Result<Option<crate::EventRecord>> {
        let row = sqlx::query(
            "SELECT id,session_id,sequence,type,payload::text AS payload,operation_id,wait_id, \
             status,created_at,handled_at FROM session_events WHERE session_id=$1 AND id=$2",
        )
        .bind(session_id.0)
        .bind(event_id.0)
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(super::store::event_from_row).transpose()
    }

    pub async fn events(
        &self,
        session_id: SessionId,
        after: i64,
        limit: i64,
        status: Option<EventStatus>,
    ) -> Result<Vec<EventSummary>> {
        if after < 0 || !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "invalid event sequence or limit".into(),
            ));
        }
        self.update_cursor(session_id).await?;
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT id,session_id,sequence,type,status,operation_id,wait_id,created_at,handled_at \
             FROM session_events WHERE session_id=",
        );
        query
            .push_bind(session_id.0)
            .push(" AND sequence>")
            .push_bind(after);
        if let Some(status) = status {
            query.push(" AND status=").push_bind(match status {
                EventStatus::Pending => "pending",
                EventStatus::Processing => "processing",
                EventStatus::Handled => "handled",
                EventStatus::Blocked => "blocked",
            });
        }
        query.push(" ORDER BY sequence LIMIT ").push_bind(limit);
        let rows = query.build().fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|row| {
                let kind: String = row.try_get("type")?;
                let status: String = row.try_get("status")?;
                Ok(EventSummary {
                    id: EventId(row.try_get("id")?),
                    session_id: SessionId(row.try_get("session_id")?),
                    sequence: EventSequence(row.try_get("sequence")?),
                    event_type: EventType::parse(&kind)
                        .ok_or_else(|| StoreError::InvalidInput(kind))?,
                    status: match status.as_str() {
                        "pending" => EventStatus::Pending,
                        "processing" => EventStatus::Processing,
                        "handled" => EventStatus::Handled,
                        "blocked" => EventStatus::Blocked,
                        _ => return Err(StoreError::InvalidInput(status)),
                    },
                    operation_id: row
                        .try_get::<Option<Uuid>, _>("operation_id")?
                        .map(OperationId),
                    wait_id: row
                        .try_get::<Option<Uuid>, _>("wait_id")?
                        .map(agent_contracts::WaitId),
                    created_at: row.try_get("created_at")?,
                    handled_at: row.try_get("handled_at")?,
                })
            })
            .collect()
    }

    pub async fn handler_attempts(
        &self,
        session_id: SessionId,
        event_id: EventId,
        after: i32,
        limit: i64,
    ) -> Result<Vec<HandlerAttemptRecord>> {
        if after < 0 || !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "invalid attempt cursor or limit".into(),
            ));
        }
        if !self.event_exists(session_id, event_id).await? {
            return Err(StoreError::EventNotFound(event_id));
        }
        let rows = sqlx::query(
            "SELECT id,event_id,attempt_number,status,input_state_version, \
             history_through_sequence,error,started_at,finished_at FROM session_event_attempts \
             WHERE event_id=$1 AND attempt_number>$2 ORDER BY attempt_number LIMIT $3",
        )
        .bind(event_id.0)
        .bind(after)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(HandlerAttemptRecord {
                    id: row.try_get("id")?,
                    event_id: EventId(row.try_get("event_id")?),
                    attempt_number: row.try_get("attempt_number")?,
                    status: row.try_get("status")?,
                    input_state_version: row.try_get("input_state_version")?,
                    history_through_sequence: row.try_get("history_through_sequence")?,
                    error: row.try_get::<Option<Json<Value>>, _>("error")?.map(|x| x.0),
                    started_at: row.try_get("started_at")?,
                    finished_at: row.try_get("finished_at")?,
                })
            })
            .collect()
    }

    pub async fn operations_summary(
        &self,
        session_id: SessionId,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
        kind: Option<OperationKind>,
        status: Option<OperationStatus>,
        source_event: Option<EventId>,
    ) -> Result<Vec<OperationSummary>> {
        if !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput("limit must be 1..1000".into()));
        }
        self.update_cursor(session_id).await?;
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT o.id,o.session_id,o.source_event_id,o.kind,o.status, \
             o.gateway_connection_id,o.gateway_job_id,o.target_operation_id,o.previous_operation_id, \
             r.operation_id IS NOT NULL AS request_available,r.expires_at AS request_expires_at, \
             o.request_removed_at,o.created_at,o.completed_at FROM session_operations o \
             LEFT JOIN session_operation_requests r ON r.operation_id=o.id WHERE o.session_id=",
        );
        query.push_bind(session_id.0);
        if let Some(kind) = kind {
            query.push(" AND o.kind=").push_bind(match kind {
                OperationKind::Llm => "llm",
                OperationKind::Execution => "execution",
                OperationKind::LlmCancellation => "llm_cancellation",
                OperationKind::Withdraw => "withdraw",
            });
        }
        if let Some(status) = status {
            query.push(" AND o.status=").push_bind(match status {
                OperationStatus::Pending => "pending",
                OperationStatus::Submitting => "submitting",
                OperationStatus::Accepted => "accepted",
                OperationStatus::Succeeded => "succeeded",
                OperationStatus::Failed => "failed",
                OperationStatus::Cancelled => "cancelled",
                OperationStatus::Unknown => "unknown",
            });
        }
        if let Some(event) = source_event {
            query.push(" AND o.source_event_id=").push_bind(event.0);
        }
        if let Some((created, id)) = cursor {
            query
                .push(" AND (o.created_at,o.id)<(")
                .push_bind(created)
                .push(",")
                .push_bind(id)
                .push(")");
        }
        query
            .push(" ORDER BY o.created_at DESC,o.id DESC LIMIT ")
            .push_bind(limit);
        let rows = query.build().fetch_all(self.pool()).await?;
        rows.into_iter()
            .map(|row| {
                let kind: String = row.try_get("kind")?;
                let status: String = row.try_get("status")?;
                Ok(OperationSummary {
                    id: OperationId(row.try_get("id")?),
                    session_id: SessionId(row.try_get("session_id")?),
                    source_event_id: EventId(row.try_get("source_event_id")?),
                    kind: match kind.as_str() {
                        "llm" => OperationKind::Llm,
                        "execution" => OperationKind::Execution,
                        "llm_cancellation" => OperationKind::LlmCancellation,
                        "withdraw" => OperationKind::Withdraw,
                        _ => return Err(StoreError::InvalidInput(kind)),
                    },
                    status: OperationStatus::parse(&status)
                        .ok_or_else(|| StoreError::InvalidInput(status))?,
                    gateway_connection_id: row
                        .try_get::<Option<String>, _>("gateway_connection_id")?
                        .map(agent_contracts::GatewayConnectionId),
                    gateway_job_id: row.try_get("gateway_job_id")?,
                    target_operation_id: row
                        .try_get::<Option<Uuid>, _>("target_operation_id")?
                        .map(OperationId),
                    previous_operation_id: row
                        .try_get::<Option<Uuid>, _>("previous_operation_id")?
                        .map(OperationId),
                    request_available: row.try_get("request_available")?,
                    request_expires_at: row.try_get("request_expires_at")?,
                    request_removed_at: row.try_get("request_removed_at")?,
                    created_at: row.try_get("created_at")?,
                    completed_at: row.try_get("completed_at")?,
                })
            })
            .collect()
    }

    pub async fn operation_attempts(
        &self,
        session_id: SessionId,
        operation_id: OperationId,
        after: i32,
        limit: i64,
    ) -> Result<Vec<OperationAttemptRecord>> {
        if after < 0 || !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "invalid attempt cursor or limit".into(),
            ));
        }
        if !self.operation_exists(session_id, operation_id).await? {
            return Err(StoreError::OperationNotFound(operation_id));
        }
        let rows = sqlx::query(
            "SELECT id,operation_id,attempt_number,phase,status,http_status,error,started_at,finished_at \
             FROM session_operation_attempts WHERE operation_id=$1 AND attempt_number>$2 \
             ORDER BY attempt_number LIMIT $3",
        )
        .bind(operation_id.0)
        .bind(after)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(OperationAttemptRecord {
                    id: row.try_get("id")?,
                    operation_id: OperationId(row.try_get("operation_id")?),
                    attempt_number: row.try_get("attempt_number")?,
                    phase: row.try_get("phase")?,
                    status: row.try_get("status")?,
                    http_status: row.try_get("http_status")?,
                    error: row.try_get::<Option<Json<Value>>, _>("error")?.map(|x| x.0),
                    started_at: row.try_get("started_at")?,
                    finished_at: row.try_get("finished_at")?,
                })
            })
            .collect()
    }

    pub async fn operation_request_for_session(
        &self,
        session_id: SessionId,
        id: OperationId,
    ) -> Result<Option<Box<RawValue>>> {
        if !self.operation_exists(session_id, id).await? {
            return Err(StoreError::OperationNotFound(id));
        }
        self.operation_request(id).await
    }

    pub async fn operation_request_status(
        &self,
        session_id: SessionId,
        id: OperationId,
    ) -> Result<(bool, Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
        let row = sqlx::query(
            "SELECT r.operation_id IS NOT NULL AS available,r.expires_at,o.request_removed_at \
             FROM session_operations o LEFT JOIN session_operation_requests r ON r.operation_id=o.id \
             WHERE o.session_id=$1 AND o.id=$2",
        )
        .bind(session_id.0)
        .bind(id.0)
        .fetch_optional(self.pool())
        .await?
        .ok_or(StoreError::OperationNotFound(id))?;
        Ok((
            row.try_get("available")?,
            row.try_get("expires_at")?,
            row.try_get("request_removed_at")?,
        ))
    }
}
