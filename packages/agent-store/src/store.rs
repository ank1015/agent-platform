use crate::inspection::{emit_update, emit_update_with_event};
use crate::{
    CallbackReceipt, ClaimedCallbackReceipt, ClaimedEvent, ClaimedOperation, DueWait,
    EnqueueResult, EventRecord, EventStatus, EventType, HandlerAttemptStatus, HandlerClaim,
    HistoryRecord, NewExternalEvent, NewSession, OperationPhase, OperationRecord, OperationStatus,
    OperationalMetrics, OutcomeCommit, ProcessingRetry, RawJson, ReceiptStatus, Result,
    SessionCursor, SessionFilter, SessionPage, SessionRecord, SessionSummary, StoreError,
    WaitCursor, WaitFilter, WaitFinish, WaitPage, WaitRecord, WaitResolution, WaitStatus,
};
use agent_contracts::{
    EventId, EventSequence, GatewayConnectionId, HarnessId, HarnessVersion, HistoryEntryId,
    HistorySequence, OperationId, OperationKind, ProjectId, SessionId, SessionStatus, StateVersion,
    WaitId, WaitMode,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json, value::RawValue};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgPoolOptions, types::Json};
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub max_connections: u32,
    pub acquire_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            acquire_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    // Pool and session records.
    pub async fn connect(url: &str, config: PoolConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .after_connect(|connection, _| {
                Box::pin(async move {
                    sqlx::query(
                        "SELECT set_config('statement_timeout','5s',false), \
                         set_config('lock_timeout','3s',false), \
                         set_config('idle_in_transaction_session_timeout','30s',false), \
                         set_config('timezone','UTC',false)",
                    )
                    .execute(connection)
                    .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        crate::MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    pub async fn operational_metrics(&self) -> Result<OperationalMetrics> {
        let row = sqlx::query(
            "SELECT \
             (SELECT count(*) FROM sessions WHERE lease_token IS NOT NULL AND lease_expires_at>clock_timestamp()) AS active_handlers, \
             (SELECT count(*) FROM session_operations WHERE lease_token IS NOT NULL AND lease_expires_at>clock_timestamp()) AS active_operation_claims, \
             (SELECT count(*) FROM session_events WHERE status='pending') AS pending_events, \
             (SELECT count(*) FROM session_events WHERE status='blocked') AS blocked_events, \
             (SELECT count(*) FROM session_operations WHERE status='pending') AS pending_operations, \
             (SELECT count(*) FROM session_operations WHERE status='submitting') AS submitting_operations, \
             (SELECT count(*) FROM session_operations WHERE status='accepted') AS accepted_operations, \
             (SELECT count(*) FROM session_waits WHERE status='pending' AND resolution_mode IN ('expiration','either') AND expires_at<=clock_timestamp()) AS overdue_waits, \
             (SELECT count(*) FROM session_operation_requests r JOIN session_operations o ON o.id=r.operation_id \
               WHERE r.expires_at<=clock_timestamp() AND o.status IN ('succeeded','failed','cancelled','unknown')) AS cleanup_ready_requests, \
             (SELECT count(*) FROM session_operations WHERE request_removed_at IS NOT NULL) AS removed_requests, \
             COALESCE((SELECT EXTRACT(EPOCH FROM clock_timestamp()-MIN(created_at))::double precision \
               FROM session_events WHERE status='pending'),0) AS oldest_pending_event_age_seconds",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(OperationalMetrics {
            active_handlers: row.try_get("active_handlers")?,
            active_operation_claims: row.try_get("active_operation_claims")?,
            pending_events: row.try_get("pending_events")?,
            blocked_events: row.try_get("blocked_events")?,
            pending_operations: row.try_get("pending_operations")?,
            submitting_operations: row.try_get("submitting_operations")?,
            accepted_operations: row.try_get("accepted_operations")?,
            overdue_waits: row.try_get("overdue_waits")?,
            cleanup_ready_requests: row.try_get("cleanup_ready_requests")?,
            removed_requests: row.try_get("removed_requests")?,
            oldest_pending_event_age_seconds: row.try_get("oldest_pending_event_age_seconds")?,
        })
    }

    pub async fn schema_ready(&self) -> Result<bool> {
        let tables_ready: bool = sqlx::query_scalar(
            "SELECT to_regclass('public.sessions') IS NOT NULL \
             AND to_regclass('public.session_events') IS NOT NULL \
             AND to_regclass('public.session_event_attempts') IS NOT NULL \
             AND to_regclass('public.session_history') IS NOT NULL \
             AND to_regclass('public.session_operations') IS NOT NULL \
             AND to_regclass('public.session_operation_requests') IS NOT NULL \
             AND to_regclass('public.session_operation_attempts') IS NOT NULL \
             AND to_regclass('public.session_waits') IS NOT NULL \
             AND to_regclass('public.gateway_callback_receipts') IS NOT NULL \
             AND to_regclass('public.session_processing_retries') IS NOT NULL \
             AND to_regclass('public.session_updates') IS NOT NULL \
             AND to_regclass('public._sqlx_migrations') IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await?;
        if !tables_ready {
            return Ok(false);
        }
        let rows =
            sqlx::query("SELECT version, checksum, success FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await?;
        let required = crate::MIGRATOR.iter().collect::<Vec<_>>();
        if rows.len() != required.len() {
            return Ok(false);
        }
        for (row, migration) in rows.iter().zip(required) {
            let version: i64 = row.try_get("version")?;
            let checksum: Vec<u8> = row.try_get("checksum")?;
            let success: bool = row.try_get("success")?;
            if version != migration.version
                || checksum.as_slice() != migration.checksum.as_ref()
                || !success
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub async fn create_session(&self, session: NewSession) -> Result<SessionRecord> {
        let id = session.id;
        let result = sqlx::query(
            "INSERT INTO sessions \
             (id, project_id, harness_id, harness_version, configuration, state, name, metadata) \
             VALUES ($1,$2,$3,$4,$5::json,$6::json,$7,$8)",
        )
        .bind(session.id.0)
        .bind(&session.project_id.0)
        .bind(&session.harness_id.0)
        .bind(&session.harness_version.0)
        .bind(session.configuration.get())
        .bind(session.state.get())
        .bind(session.name)
        .bind(Json(session.metadata))
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => self.session(id).await,
            Err(sqlx::Error::Database(error))
                if error.code().as_deref() == Some("23505")
                    && error.constraint() == Some("sessions_pkey") =>
            {
                Err(StoreError::SessionAlreadyExists(id))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn session(&self, id: SessionId) -> Result<SessionRecord> {
        let row = sqlx::query(SESSION_SELECT)
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::SessionNotFound(id))?;
        session_from_row(&row)
    }

    pub async fn list_sessions(&self, filter: SessionFilter) -> Result<SessionPage> {
        let limit = filter.limit.unwrap_or(50);
        if !(1..=100).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "limit must be between 1 and 100".into(),
            ));
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT id,project_id,harness_id,harness_version,name,metadata,status,state_version, \
             processing_enabled,processing_error,processing_revision, \
             (SELECT e.id FROM session_events e WHERE e.session_id=sessions.id \
               AND e.status='blocked' ORDER BY e.sequence LIMIT 1) AS blocked_event_id, \
             created_at,updated_at FROM sessions WHERE true",
        );
        if let Some(project_id) = filter.project_id {
            query.push(" AND project_id=").push_bind(project_id.0);
        }
        if let Some(status) = filter.status {
            query
                .push(" AND status=")
                .push_bind(session_status_str(status));
        }
        if let Some(harness_id) = filter.harness_id {
            query.push(" AND harness_id=").push_bind(harness_id.0);
        }
        if let Some(cursor) = filter.cursor {
            query
                .push(" AND (created_at,id)<(")
                .push_bind(cursor.created_at)
                .push(",")
                .push_bind(cursor.id.0)
                .push(")");
        }
        query
            .push(" ORDER BY created_at DESC,id DESC LIMIT ")
            .push_bind(limit + 1);
        let rows = query.build().fetch_all(&self.pool).await?;
        let mut sessions = rows
            .iter()
            .map(session_summary_from_row)
            .collect::<Result<Vec<_>>>()?;
        let has_more = sessions.len() > limit as usize;
        sessions.truncate(limit as usize);
        let next_cursor =
            has_more
                .then(|| sessions.last())
                .flatten()
                .map(|session| SessionCursor {
                    created_at: session.created_at,
                    id: session.id,
                });
        Ok(SessionPage {
            sessions,
            next_cursor,
        })
    }

    pub async fn patch_session_metadata(
        &self,
        id: SessionId,
        name: Option<Option<String>>,
        metadata: Option<Value>,
    ) -> Result<SessionRecord> {
        let replace_name = name.is_some();
        let name = name.flatten();
        let replace_metadata = metadata.is_some();
        let metadata = metadata.unwrap_or(Value::Null);
        let mut tx = self.pool.begin().await?;
        let changed = sqlx::query(
            "UPDATE sessions SET \
             name=CASE WHEN $2 THEN $3 ELSE name END, \
             metadata=CASE WHEN $4 THEN $5 ELSE metadata END, \
             updated_at=now() WHERE id=$1",
        )
        .bind(id.0)
        .bind(replace_name)
        .bind(name)
        .bind(replace_metadata)
        .bind(Json(metadata))
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() == 0 {
            return Err(StoreError::SessionNotFound(id));
        }
        emit_update(&mut tx, id, "session.changed", &json!({"session_id":id})).await?;
        tx.commit().await?;
        self.session(id).await
    }

    pub async fn enqueue_external_event(&self, input: NewExternalEvent) -> Result<EnqueueResult> {
        if !matches!(
            input.event_type,
            EventType::UserMessage | EventType::CancellationRequested
        ) {
            return Err(StoreError::InvalidInput(
                "only user messages and cancellation requests are external inputs".into(),
            ));
        }
        let hash = input
            .idempotency_key
            .as_ref()
            .map(|_| fingerprint(input.payload.as_ref()));
        let mut tx = self.pool.begin().await?;
        let next: Option<i64> =
            sqlx::query_scalar("SELECT next_event_sequence FROM sessions WHERE id=$1 FOR UPDATE")
                .bind(input.session_id.0)
                .fetch_optional(&mut *tx)
                .await?;
        let sequence = next.ok_or(StoreError::SessionNotFound(input.session_id))?;

        if let Some(key) = &input.idempotency_key
            && let Some(row) = sqlx::query(
                "SELECT id, session_id, sequence, type, payload::text AS payload, operation_id, \
                 wait_id, status, created_at, handled_at, request_hash \
                 FROM session_events WHERE session_id=$1 AND type=$2 AND idempotency_key=$3",
            )
            .bind(input.session_id.0)
            .bind(input.event_type.as_str())
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?
        {
            let stored_hash: Option<String> = row.try_get("request_hash")?;
            if stored_hash.as_deref() != hash.as_deref() {
                return Err(StoreError::IdempotencyConflict);
            }
            let event = event_from_row(&row)?;
            tx.commit().await?;
            return Ok(EnqueueResult::Existing(event));
        }

        let id = EventId::new();
        let row = sqlx::query(
            "INSERT INTO session_events \
             (id,session_id,sequence,type,payload,idempotency_key,request_hash) \
             VALUES ($1,$2,$3,$4,$5::json,$6,$7) \
             RETURNING id,session_id,sequence,type,payload::text AS payload,operation_id,wait_id, \
             status,created_at,handled_at",
        )
        .bind(id.0)
        .bind(input.session_id.0)
        .bind(sequence)
        .bind(input.event_type.as_str())
        .bind(input.payload.get())
        .bind(input.idempotency_key)
        .bind(hash)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE sessions SET next_event_sequence=next_event_sequence+1, updated_at=now() \
             WHERE id=$1",
        )
        .bind(input.session_id.0)
        .execute(&mut *tx)
        .await?;
        let event = event_from_row(&row)?;
        emit_update(
            &mut tx,
            input.session_id,
            "event.changed",
            &json!({"event_id":event.id,"status":"pending"}),
        )
        .await?;
        tx.commit().await?;
        Ok(EnqueueResult::Inserted(event))
    }

    // Ordered event inbox and handler ownership.

    pub async fn event(&self, id: EventId) -> Result<EventRecord> {
        let row = sqlx::query(EVENT_SELECT)
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::EventNotFound(id))?;
        event_from_row(&row)
    }

    pub async fn handler_attempt_status(
        &self,
        attempt_id: Uuid,
    ) -> Result<Option<HandlerAttemptStatus>> {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM session_event_attempts WHERE id=$1")
                .bind(attempt_id)
                .fetch_optional(&self.pool)
                .await?;
        status
            .map(|status| match status.as_str() {
                "running" => Ok(HandlerAttemptStatus::Running),
                "committed" => Ok(HandlerAttemptStatus::Committed),
                "errored" => Ok(HandlerAttemptStatus::Errored),
                "abandoned" => Ok(HandlerAttemptStatus::Abandoned),
                _ => Err(invalid("handler attempt status", status)),
            })
            .transpose()
    }

    pub async fn handler_failure_count(&self, event_id: EventId) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM session_event_attempts WHERE event_id=$1 AND status='errored') \
                    - retry_failure_baseline FROM session_events WHERE id=$1",
        )
        .bind(event_id.0)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn retry_blocked_event(
        &self,
        session_id: SessionId,
        event_id: EventId,
        expected_revision: i64,
        idempotency_key: &str,
        request: &RawValue,
        reason: Option<&str>,
    ) -> Result<ProcessingRetry> {
        let mut tx = self.pool.begin().await?;
        let session = sqlx::query(
            "SELECT processing_enabled,processing_revision,current_event_id,lease_token \
             FROM sessions WHERE id=$1 FOR UPDATE",
        )
        .bind(session_id.0)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::SessionNotFound(session_id))?;
        let request_hash = fingerprint(request);
        if let Some(existing) = sqlx::query(
            "SELECT request_hash,event_id,processing_revision,created_at \
             FROM session_processing_retries WHERE session_id=$1 AND idempotency_key=$2",
        )
        .bind(session_id.0)
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        {
            let stored_hash: String = existing.try_get("request_hash")?;
            if stored_hash != request_hash {
                return Err(StoreError::IdempotencyConflict);
            }
            return Ok(ProcessingRetry {
                session_id,
                event_id: EventId(existing.try_get("event_id")?),
                processing_revision: existing.try_get("processing_revision")?,
                retried_at: existing.try_get("created_at")?,
            });
        }
        let enabled: bool = session.try_get("processing_enabled")?;
        let revision: i64 = session.try_get("processing_revision")?;
        let current_event_id: Option<Uuid> = session.try_get("current_event_id")?;
        let lease_token: Option<Uuid> = session.try_get("lease_token")?;
        let head = sqlx::query(
            "SELECT id,status FROM session_events WHERE session_id=$1 \
             AND status IN ('pending','processing','blocked') ORDER BY sequence LIMIT 1",
        )
        .bind(session_id.0)
        .fetch_optional(&mut *tx)
        .await?;
        let matches_head = if let Some(head) = head {
            head.try_get::<Uuid, _>("id")? == event_id.0
                && head.try_get::<String, _>("status")? == "blocked"
        } else {
            false
        };
        if enabled
            || revision != expected_revision
            || current_event_id.is_some()
            || lease_token.is_some()
            || !matches_head
        {
            return Err(StoreError::StaleProcessingRetry);
        }
        let failure_baseline: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM session_event_attempts WHERE event_id=$1 AND status='errored'",
        )
        .bind(event_id.0)
        .fetch_one(&mut *tx)
        .await?;
        let next_revision = revision + 1;
        sqlx::query(
            "UPDATE session_events SET status='pending',next_attempt_at=clock_timestamp(), \
             last_error=NULL,retry_failure_baseline=$2 WHERE id=$1",
        )
        .bind(event_id.0)
        .bind(failure_baseline)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE sessions SET processing_enabled=true,processing_error=NULL, \
             processing_revision=$2,updated_at=now() WHERE id=$1",
        )
        .bind(session_id.0)
        .bind(next_revision)
        .execute(&mut *tx)
        .await?;
        let row = sqlx::query(
            "INSERT INTO session_processing_retries \
             (id,session_id,idempotency_key,request_hash,event_id,expected_processing_revision, \
              processing_revision,reason) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) \
             RETURNING created_at",
        )
        .bind(Uuid::new_v4())
        .bind(session_id.0)
        .bind(idempotency_key)
        .bind(request_hash)
        .bind(event_id.0)
        .bind(expected_revision)
        .bind(next_revision)
        .bind(reason)
        .fetch_one(&mut *tx)
        .await?;
        let retried_at = row.try_get("created_at")?;
        emit_update(
            &mut tx,
            session_id,
            "processing.changed",
            &json!({"event_id":event_id,"processing_revision":next_revision}),
        )
        .await?;
        emit_update(
            &mut tx,
            session_id,
            "event.changed",
            &json!({"event_id":event_id,"status":"pending"}),
        )
        .await?;
        tx.commit().await?;
        Ok(ProcessingRetry {
            session_id,
            event_id,
            processing_revision: next_revision,
            retried_at,
        })
    }

    pub async fn claim_next_event(&self, lease_for: Duration) -> Result<Option<ClaimedEvent>> {
        let mut tx = self.pool.begin().await?;
        let candidate = sqlx::query(
            "SELECT s.id AS session_id, e.id AS event_id \
             FROM sessions s \
             CROSS JOIN LATERAL ( \
               SELECT id,status,next_attempt_at FROM session_events \
               WHERE session_id=s.id AND status IN ('pending','processing','blocked') \
               ORDER BY sequence LIMIT 1 \
             ) e \
             WHERE s.processing_enabled \
               AND (s.lease_token IS NULL OR s.lease_expires_at <= clock_timestamp()) \
               AND e.status IN ('pending','processing') AND e.next_attempt_at <= clock_timestamp() \
             ORDER BY e.next_attempt_at, s.updated_at, s.id \
             FOR UPDATE OF s SKIP LOCKED LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(candidate) = candidate else {
            tx.rollback().await?;
            return Ok(None);
        };
        let session_id = SessionId(candidate.try_get("session_id")?);
        let event_id = EventId(candidate.try_get("event_id")?);

        sqlx::query(
            "UPDATE session_event_attempts SET status='abandoned', finished_at=now() \
             WHERE event_id=$1 AND status='running'",
        )
        .bind(event_id.0)
        .execute(&mut *tx)
        .await?;

        let attempt_number: i32 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(attempt_number),0)+1 FROM session_event_attempts WHERE event_id=$1",
        )
        .bind(event_id.0)
        .fetch_one(&mut *tx)
        .await?;
        let lease_token = Uuid::new_v4();
        let attempt_id = Uuid::new_v4();
        let lease_ms = duration_millis(lease_for)?;
        let state_version: i64 = sqlx::query_scalar(
            "UPDATE sessions SET current_event_id=$2, lease_token=$3, \
             lease_expires_at=clock_timestamp()+($4 * interval '1 millisecond'), updated_at=now() \
             WHERE id=$1 RETURNING state_version",
        )
        .bind(session_id.0)
        .bind(event_id.0)
        .bind(lease_token)
        .bind(lease_ms)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query("UPDATE session_events SET status='processing' WHERE id=$1")
            .bind(event_id.0)
            .execute(&mut *tx)
            .await?;
        let history_through: i64 =
            sqlx::query_scalar("SELECT next_history_sequence-1 FROM sessions WHERE id=$1")
                .bind(session_id.0)
                .fetch_one(&mut *tx)
                .await?;
        sqlx::query(
            "INSERT INTO session_event_attempts \
             (id,event_id,attempt_number,lease_token,input_state_version,history_through_sequence) \
             VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(attempt_id)
        .bind(event_id.0)
        .bind(attempt_number)
        .bind(lease_token)
        .bind(state_version)
        .bind(history_through)
        .execute(&mut *tx)
        .await?;

        let session_row = sqlx::query(SESSION_SELECT)
            .bind(session_id.0)
            .fetch_one(&mut *tx)
            .await?;
        let event_row = sqlx::query(EVENT_SELECT)
            .bind(event_id.0)
            .fetch_one(&mut *tx)
            .await?;
        let session = session_from_row(&session_row)?;
        let event = event_from_row(&event_row)?;
        emit_update(
            &mut tx,
            session_id,
            "event.changed",
            &json!({"event_id":event_id,"status":"processing"}),
        )
        .await?;
        tx.commit().await?;
        Ok(Some(ClaimedEvent {
            session,
            event,
            attempt_id,
            attempt_number,
            lease_token,
            history_through_sequence: HistorySequence(history_through),
        }))
    }

    pub async fn renew_event_claim(&self, claim: &HandlerClaim, lease_for: Duration) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_valid_session_claim(&mut tx, claim).await?;
        sqlx::query(
            "UPDATE sessions SET lease_expires_at=clock_timestamp()+($2 * interval '1 millisecond') \
             WHERE id=$1",
        )
        .bind(claim.session_id.0)
        .bind(duration_millis(lease_for)?)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_handler_failure(
        &self,
        claim: &HandlerClaim,
        error: Value,
        retry_at: DateTime<Utc>,
        blocked: bool,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_valid_session_claim(&mut tx, claim).await?;
        sqlx::query(
            "UPDATE session_event_attempts SET status='errored', error=$2, finished_at=now() \
             WHERE id=$1 AND lease_token=$3 AND status='running'",
        )
        .bind(claim.attempt_id)
        .bind(Json(error.clone()))
        .bind(claim.lease_token)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE session_events SET status=$2, next_attempt_at=$3, last_error=$4 WHERE id=$1",
        )
        .bind(claim.event_id.0)
        .bind(if blocked { "blocked" } else { "pending" })
        .bind(retry_at)
        .bind(Json(error.clone()))
        .execute(&mut *tx)
        .await?;
        if blocked {
            sqlx::query(
                "UPDATE sessions SET processing_enabled=false,processing_error=$2, \
                 processing_revision=processing_revision+1, \
                 current_event_id=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=now() \
                 WHERE id=$1",
            )
            .bind(claim.session_id.0)
            .bind(Json(error))
            .execute(&mut *tx)
            .await?;
        } else {
            release_session_claim(&mut tx, claim.session_id).await?;
        }
        emit_update(
            &mut tx,
            claim.session_id,
            "event.changed",
            &json!({"event_id":claim.event_id,"status":if blocked {"blocked"} else {"pending"}}),
        )
        .await?;
        if blocked {
            emit_update(
                &mut tx,
                claim.session_id,
                "processing.changed",
                &json!({"event_id":claim.event_id}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn abandon_handler_attempt(
        &self,
        claim: &HandlerClaim,
        error: Value,
        retry_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_valid_session_claim(&mut tx, claim).await?;
        sqlx::query(
            "UPDATE session_event_attempts SET status='abandoned', error=$2, finished_at=now() \
             WHERE id=$1 AND lease_token=$3 AND status='running'",
        )
        .bind(claim.attempt_id)
        .bind(Json(error.clone()))
        .bind(claim.lease_token)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE session_events SET status='pending',next_attempt_at=$2,last_error=$3 WHERE id=$1",
        )
        .bind(claim.event_id.0)
        .bind(retry_at)
        .bind(Json(error))
        .execute(&mut *tx)
        .await?;
        release_session_claim(&mut tx, claim.session_id).await?;
        emit_update(
            &mut tx,
            claim.session_id,
            "event.changed",
            &json!({"event_id":claim.event_id,"status":"pending"}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn commit_handler_outcome(
        &self,
        claim: &HandlerClaim,
        outcome: OutcomeCommit,
    ) -> Result<()> {
        self.commit_handler_outcome_classified(claim, outcome)
            .await
            .map_err(crate::HandlerCommitError::into_store_error)
    }

    pub async fn commit_handler_outcome_classified(
        &self,
        claim: &HandlerClaim,
        outcome: OutcomeCommit,
    ) -> std::result::Result<(), crate::HandlerCommitError> {
        let mut tx = self.pool.begin().await?;
        let (mut history_sequence, current_state_version) =
            lock_valid_session_claim(&mut tx, claim).await?;
        if current_state_version != claim.input_state_version.0 {
            return Err(crate::HandlerCommitError::StaleClaim);
        }

        for entry in &outcome.history {
            let message: agent_contracts::Message = serde_json::from_str(entry.message.get())
                .map_err(|error| StoreError::InvalidInput(error.to_string()))?;
            let role = message_role(&message);
            sqlx::query(
                "INSERT INTO session_history \
                 (id,session_id,sequence,source_event_id,role,message) \
                 VALUES ($1,$2,$3,$4,$5,$6::json)",
            )
            .bind(entry.id.0)
            .bind(claim.session_id.0)
            .bind(history_sequence)
            .bind(claim.event_id.0)
            .bind(role)
            .bind(entry.message.get())
            .execute(&mut *tx)
            .await?;
            history_sequence += 1;
        }
        if !outcome.history.is_empty() {
            emit_update(&mut tx,claim.session_id,"history.appended",&json!({"from_sequence":history_sequence-outcome.history.len() as i64,"through_sequence":history_sequence-1,"source_event_id":claim.event_id})).await?;
        }

        for operation in &outcome.operations {
            let request_hash = fingerprint(operation.request.as_ref());
            sqlx::query(
                "INSERT INTO session_operations \
                 (id,session_id,source_event_id,kind,gateway_connection_id,target_operation_id, \
                  previous_operation_id,gateway_idempotency_key,request_hash) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            )
            .bind(operation.id.0)
            .bind(claim.session_id.0)
            .bind(claim.event_id.0)
            .bind(operation_kind_str(operation.kind))
            .bind(
                operation
                    .gateway_connection_id
                    .as_ref()
                    .map(|connection| connection.0.as_str()),
            )
            .bind(operation.target_operation_id.map(|id| id.0))
            .bind(operation.previous_operation_id.map(|id| id.0))
            .bind(operation.id.to_string())
            .bind(request_hash)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO session_operation_requests (operation_id,request) VALUES ($1,$2::json)",
            )
            .bind(operation.id.0)
            .bind(operation.request.get())
            .execute(&mut *tx)
            .await?;
            emit_update(
                &mut tx,
                claim.session_id,
                "operation.changed",
                &json!({"operation_id":operation.id}),
            )
            .await?;
        }

        for wait in &outcome.waits {
            sqlx::query(
                "INSERT INTO session_waits \
                 (id,session_id,source_event_id,resolution_mode,payload,response_schema,expires_at) \
                 VALUES ($1,$2,$3,$4,$5::json,$6::json,$7)",
            )
            .bind(wait.id.0)
            .bind(claim.session_id.0)
            .bind(claim.event_id.0)
            .bind(wait_mode_str(wait.mode))
            .bind(wait.payload.get())
            .bind(wait.response_schema.as_deref().map(RawValue::get))
            .bind(wait.expires_at)
            .execute(&mut *tx)
            .await?;
            emit_update(
                &mut tx,
                claim.session_id,
                "wait.changed",
                &json!({"wait_id":wait.id}),
            )
            .await?;
        }

        let mut event_sequence: i64 =
            sqlx::query_scalar("SELECT next_event_sequence FROM sessions WHERE id=$1")
                .bind(claim.session_id.0)
                .fetch_one(&mut *tx)
                .await?;
        for wait_id in &outcome.cancelled_waits {
            let wait = sqlx::query(
                "SELECT status,resolution_mode,expires_at FROM session_waits \
                 WHERE id=$1 AND session_id=$2 FOR UPDATE",
            )
            .bind(wait_id.0)
            .bind(claim.session_id.0)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(StoreError::WaitNotFound(*wait_id))?;
            let status: String = wait.try_get("status")?;
            if status != "pending" {
                continue;
            }
            let mode: String = wait.try_get("resolution_mode")?;
            let expires_at: Option<DateTime<Utc>> = wait.try_get("expires_at")?;
            let now = database_clock(&mut tx).await?;
            let expired = matches!(mode.as_str(), "expiration" | "either")
                && expires_at.is_some_and(|deadline| deadline <= now);
            let finish = if expired {
                WaitFinish::Expired
            } else {
                WaitFinish::Cancelled
            };
            sqlx::query(
                "UPDATE session_waits SET status=$2,finished_at=clock_timestamp() WHERE id=$1",
            )
            .bind(wait_id.0)
            .bind(if expired { "expired" } else { "cancelled" })
            .execute(&mut *tx)
            .await?;
            let wait_event =
                insert_wait_event(&mut tx, claim.session_id, *wait_id, event_sequence, finish)
                    .await?;
            emit_update(
                &mut tx,
                claim.session_id,
                "wait.changed",
                &json!({"wait_id":wait_id}),
            )
            .await?;
            emit_update(
                &mut tx,
                claim.session_id,
                "event.changed",
                &json!({"event_id":wait_event.id}),
            )
            .await?;
            event_sequence += 1;
        }

        if outcome.progress.len() > 20
            || outcome
                .progress
                .iter()
                .any(|value| serde_json::to_string(value).is_ok_and(|s| s.len() > 2048))
        {
            return Err(StoreError::InvalidInput(
                "progress exceeds 20 entries or 2048 bytes per entry".into(),
            )
            .into());
        }
        for payload in &outcome.progress {
            emit_update_with_event(
                &mut tx,
                claim.session_id,
                "harness.progress",
                payload,
                Some(claim.event_id),
            )
            .await?;
        }

        let status = outcome.status.map(session_status_str);
        sqlx::query(
            "UPDATE sessions SET state=$2::json, status=COALESCE($3,status), \
             state_version=state_version+1, next_history_sequence=$4, current_event_id=NULL, \
             lease_token=NULL, lease_expires_at=NULL, next_event_sequence=$5, updated_at=now() WHERE id=$1",
        )
        .bind(claim.session_id.0)
        .bind(outcome.state.get())
        .bind(status)
        .bind(history_sequence)
        .bind(event_sequence)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE session_events SET status='handled', handled_at=now(), last_error=NULL WHERE id=$1",
        )
        .bind(claim.event_id.0)
        .execute(&mut *tx)
        .await?;
        let attempt = sqlx::query(
            "UPDATE session_event_attempts SET status='committed', finished_at=now() \
             WHERE id=$1 AND lease_token=$2 AND status='running'",
        )
        .bind(claim.attempt_id)
        .bind(claim.lease_token)
        .execute(&mut *tx)
        .await?;
        if attempt.rows_affected() != 1 {
            return Err(crate::HandlerCommitError::StaleClaim);
        }
        emit_update(
            &mut tx,
            claim.session_id,
            "session.changed",
            &json!({"session_id":claim.session_id}),
        )
        .await?;
        emit_update(
            &mut tx,
            claim.session_id,
            "event.changed",
            &json!({"event_id":claim.event_id,"status":"handled"}),
        )
        .await?;
        tx.commit()
            .await
            .map_err(crate::HandlerCommitError::from_commit)
    }

    // Bounded harness-context reads.

    pub async fn history(
        &self,
        session_id: SessionId,
        after: HistorySequence,
        through: HistorySequence,
        limit: i64,
    ) -> Result<Vec<HistoryRecord>> {
        if !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "limit must be between 1 and 1000".into(),
            ));
        }
        let rows = sqlx::query(
            "SELECT id,session_id,sequence,source_event_id,role,message::text AS message,created_at \
             FROM session_history WHERE session_id=$1 AND sequence>$2 AND sequence<=$3 \
             ORDER BY sequence LIMIT $4",
        )
        .bind(session_id.0)
        .bind(after.0)
        .bind(through.0)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(history_from_row).collect()
    }

    pub async fn history_entry(
        &self,
        session_id: SessionId,
        id: HistoryEntryId,
        through: HistorySequence,
    ) -> Result<Option<HistoryRecord>> {
        let row = sqlx::query(
            "SELECT id,session_id,sequence,source_event_id,role,message::text AS message,created_at \
             FROM session_history WHERE session_id=$1 AND id=$2 AND sequence<=$3",
        )
        .bind(session_id.0)
        .bind(id.0)
        .bind(through.0)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(history_from_row).transpose()
    }

    pub async fn operation(&self, id: OperationId) -> Result<OperationRecord> {
        let row = sqlx::query(OPERATION_SELECT)
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::OperationNotFound(id))?;
        operation_from_row(&row)
    }

    pub async fn operation_for_session(
        &self,
        session_id: SessionId,
        id: OperationId,
    ) -> Result<Option<OperationRecord>> {
        let row = sqlx::query(&format!(
            "{OPERATION_SELECT_BASE} WHERE o.session_id=$1 AND o.id=$2"
        ))
        .bind(session_id.0)
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(operation_from_row).transpose()
    }

    pub async fn operations(
        &self,
        session_id: SessionId,
        kind: Option<OperationKind>,
        limit: i64,
    ) -> Result<Vec<OperationRecord>> {
        if !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "limit must be between 1 and 1000".into(),
            ));
        }
        let mut query = QueryBuilder::<Postgres>::new(OPERATION_SELECT_BASE);
        query.push(" WHERE o.session_id=").push_bind(session_id.0);
        if let Some(kind) = kind {
            query
                .push(" AND o.kind=")
                .push_bind(operation_kind_str(kind));
        }
        query
            .push(" ORDER BY o.created_at DESC,o.id DESC LIMIT ")
            .push_bind(limit);
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.iter().map(operation_from_row).collect()
    }

    pub async fn operation_request(&self, id: OperationId) -> Result<Option<RawJson>> {
        let request: Option<String> = sqlx::query_scalar(
            "SELECT request::text FROM session_operation_requests WHERE operation_id=$1",
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?;
        optional_raw(request, "operation request")
    }

    pub async fn wait(&self, id: WaitId) -> Result<WaitRecord> {
        let row = sqlx::query(WAIT_SELECT)
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(StoreError::WaitNotFound(id))?;
        wait_from_row(&row)
    }

    pub async fn wait_for_session(
        &self,
        session_id: SessionId,
        id: WaitId,
    ) -> Result<Option<WaitRecord>> {
        let row = sqlx::query(&format!("{WAIT_SELECT_BASE} WHERE session_id=$1 AND id=$2"))
            .bind(session_id.0)
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(wait_from_row).transpose()
    }

    pub async fn waits(
        &self,
        session_id: SessionId,
        status: Option<WaitStatus>,
        limit: i64,
    ) -> Result<Vec<WaitRecord>> {
        if !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "limit must be between 1 and 1000".into(),
            ));
        }
        let mut query = QueryBuilder::<Postgres>::new(WAIT_SELECT_BASE);
        query.push(" WHERE session_id=").push_bind(session_id.0);
        if let Some(status) = status {
            query
                .push(" AND status=")
                .push_bind(wait_status_str(status));
        }
        query
            .push(" ORDER BY created_at DESC,id DESC LIMIT ")
            .push_bind(limit);
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.iter().map(wait_from_row).collect()
    }

    pub async fn list_waits(&self, session_id: SessionId, filter: WaitFilter) -> Result<WaitPage> {
        let limit = filter.limit.unwrap_or(50);
        if !(1..=100).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "limit must be between 1 and 100".into(),
            ));
        }
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=$1)")
            .bind(session_id.0)
            .fetch_one(&self.pool)
            .await?;
        if !exists {
            return Err(StoreError::SessionNotFound(session_id));
        }

        let mut query = QueryBuilder::<Postgres>::new(WAIT_SELECT_BASE);
        query.push(" WHERE session_id=").push_bind(session_id.0);
        if let Some(status) = filter.status {
            query
                .push(" AND status=")
                .push_bind(wait_status_str(status));
        }
        if let Some(cursor) = filter.cursor {
            query
                .push(" AND (created_at,id)<(")
                .push_bind(cursor.created_at)
                .push(",")
                .push_bind(cursor.id.0)
                .push(")");
        }
        query
            .push(" ORDER BY created_at DESC,id DESC LIMIT ")
            .push_bind(limit + 1);
        let rows = query.build().fetch_all(&self.pool).await?;
        let mut waits = rows.iter().map(wait_from_row).collect::<Result<Vec<_>>>()?;
        let has_more = waits.len() > limit as usize;
        waits.truncate(limit as usize);
        let next_cursor = has_more
            .then(|| waits.last())
            .flatten()
            .map(|wait| WaitCursor {
                created_at: wait.created_at,
                id: wait.id,
            });
        Ok(WaitPage { waits, next_cursor })
    }

    pub async fn due_wait_ids(&self, limit: i64) -> Result<Vec<WaitId>> {
        Ok(self
            .due_waits_after(None, limit)
            .await?
            .into_iter()
            .map(|wait| wait.id)
            .collect())
    }

    pub async fn due_waits_after(
        &self,
        after: Option<DueWait>,
        limit: i64,
    ) -> Result<Vec<DueWait>> {
        if !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "limit must be between 1 and 1000".into(),
            ));
        }
        let rows = sqlx::query(
            "SELECT id,expires_at FROM session_waits WHERE status='pending' \
             AND resolution_mode IN ('expiration','either') AND expires_at<=clock_timestamp() \
             AND ($2::timestamptz IS NULL OR (expires_at,id)>($2::timestamptz,$3::uuid)) \
             ORDER BY expires_at,id LIMIT $1",
        )
        .bind(limit)
        .bind(after.map(|wait| wait.expires_at))
        .bind(after.map(|wait| wait.id.0))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(DueWait {
                    id: WaitId(row.try_get("id")?),
                    expires_at: row.try_get("expires_at")?,
                })
            })
            .collect()
    }

    // Gateway operation ownership and settlement.

    pub async fn claim_operation(
        &self,
        phase: OperationPhase,
        lease_for: Duration,
    ) -> Result<Option<ClaimedOperation>> {
        let mut tx = self.pool.begin().await?;
        let eligibility = match phase {
            OperationPhase::Submission => {
                "o.status IN ('pending','submitting') AND o.kind IN ('llm','execution')"
            }
            OperationPhase::ResultRetrieval => "o.status='accepted'",
            OperationPhase::Cancellation => {
                "o.status IN ('pending','submitting') AND o.kind='llm_cancellation'"
            }
            OperationPhase::Withdrawal => {
                "o.status IN ('pending','submitting') AND o.kind='withdraw'"
            }
        };
        let row = sqlx::query(&format!(
            "SELECT o.id,o.session_id FROM session_operations o \
             JOIN sessions s ON s.id=o.session_id \
             WHERE {eligibility} AND o.next_attempt_at<=clock_timestamp() \
               AND (o.lease_token IS NULL OR o.lease_expires_at<=clock_timestamp()) \
             ORDER BY o.next_attempt_at,o.created_at,o.id \
             FOR UPDATE OF s SKIP LOCKED LIMIT 1"
        ))
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(None);
        };
        let operation_id = OperationId(row.try_get("id")?);
        let session_id = SessionId(row.try_get("session_id")?);
        let locked = sqlx::query(&format!(
            "SELECT o.status FROM session_operations o WHERE o.id=$1 AND {eligibility} \
             AND o.next_attempt_at<=clock_timestamp() \
             AND (o.lease_token IS NULL OR o.lease_expires_at<=clock_timestamp()) \
             FOR UPDATE SKIP LOCKED"
        ))
        .bind(operation_id.0)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(locked) = locked else {
            tx.rollback().await?;
            return Ok(None);
        };
        let previous_status: String = locked.try_get("status")?;
        let recovering_uncertain = previous_status == "submitting";
        sqlx::query(
            "UPDATE session_operation_attempts SET status='unknown', finished_at=now() \
             WHERE operation_id=$1 AND status='running'",
        )
        .bind(operation_id.0)
        .execute(&mut *tx)
        .await?;
        let attempt_number: i32 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(attempt_number),0)+1 FROM session_operation_attempts \
             WHERE operation_id=$1",
        )
        .bind(operation_id.0)
        .fetch_one(&mut *tx)
        .await?;
        let lease_token = Uuid::new_v4();
        let attempt_id = Uuid::new_v4();
        sqlx::query(
            "UPDATE session_operations SET status=CASE WHEN status='pending' THEN 'submitting' ELSE status END, \
             lease_token=$2, lease_expires_at=clock_timestamp()+($3 * interval '1 millisecond') WHERE id=$1",
        )
        .bind(operation_id.0)
        .bind(lease_token)
        .bind(duration_millis(lease_for)?)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO session_operation_attempts \
             (id,operation_id,attempt_number,phase,lease_token) VALUES ($1,$2,$3,$4,$5)",
        )
        .bind(attempt_id)
        .bind(operation_id.0)
        .bind(attempt_number)
        .bind(phase.as_str())
        .bind(lease_token)
        .execute(&mut *tx)
        .await?;
        let operation_row = sqlx::query(OPERATION_SELECT)
            .bind(operation_id.0)
            .fetch_one(&mut *tx)
            .await?;
        let operation = operation_from_row(&operation_row)?;
        let request = if matches!(
            phase,
            OperationPhase::Submission | OperationPhase::Cancellation
        ) {
            let text: Option<String> = sqlx::query_scalar(
                "SELECT request::text FROM session_operation_requests WHERE operation_id=$1",
            )
            .bind(operation_id.0)
            .fetch_optional(&mut *tx)
            .await?;
            optional_raw(text, "operation request")?
        } else {
            None
        };
        if previous_status == "pending" {
            emit_update(
                &mut tx,
                session_id,
                "operation.changed",
                &json!({"operation_id":operation_id}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(Some(ClaimedOperation {
            operation,
            request,
            phase,
            attempt_id,
            attempt_number,
            lease_token,
            recovering_uncertain,
        }))
    }

    pub async fn defer_operation(
        &self,
        claim: &ClaimedOperation,
        retry_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_session_sequence(&mut tx, claim.operation.session_id).await?;
        let current = lock_valid_operation_claim(&mut tx, claim).await?;
        if !matches!(
            claim.phase,
            OperationPhase::Submission | OperationPhase::Cancellation
        ) || current.status != OperationStatus::Submitting
        {
            return Err(StoreError::InvalidInput(
                "only a submission or cancellation claim can be deferred".into(),
            ));
        }
        sqlx::query(
            "UPDATE session_operations SET status='pending',next_attempt_at=$2,error=NULL, \
             lease_token=NULL,lease_expires_at=NULL WHERE id=$1",
        )
        .bind(claim.operation.id.0)
        .bind(retry_at)
        .execute(&mut *tx)
        .await?;
        finish_operation_attempt(&mut tx, claim, "succeeded", None, None).await?;
        emit_update(
            &mut tx,
            claim.operation.session_id,
            "operation.changed",
            &json!({"operation_id":claim.operation.id}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_operation_accepted(
        &self,
        claim: &ClaimedOperation,
        gateway_job_id: Uuid,
        result_check_at: DateTime<Utc>,
        http_status: Option<i32>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_session_sequence(&mut tx, claim.operation.session_id).await?;
        let current = lock_valid_operation_claim(&mut tx, claim).await?;
        if claim.phase != OperationPhase::Submission
            || current.status != OperationStatus::Submitting
            || current.gateway_job_id.is_some()
        {
            return Err(StoreError::InvalidInput(
                "only a new submission claim can accept a gateway job".into(),
            ));
        }
        sqlx::query(
            "UPDATE session_operations SET status='accepted',gateway_job_id=$2,submitted_at=now(), \
             error=NULL,lease_token=NULL,lease_expires_at=NULL,next_attempt_at=$3 WHERE id=$1",
        )
        .bind(claim.operation.id.0)
        .bind(gateway_job_id)
        .bind(result_check_at)
        .execute(&mut *tx)
        .await?;
        finish_operation_attempt(&mut tx, claim, "succeeded", http_status, None).await?;
        emit_update(
            &mut tx,
            claim.operation.session_id,
            "operation.changed",
            &json!({"operation_id":claim.operation.id}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn renew_operation_claim(
        &self,
        claim: &ClaimedOperation,
        lease_for: Duration,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_valid_operation_claim(&mut tx, claim).await?;
        sqlx::query(
            "UPDATE session_operations \
             SET lease_expires_at=clock_timestamp()+($2 * interval '1 millisecond') WHERE id=$1",
        )
        .bind(claim.operation.id.0)
        .bind(duration_millis(lease_for)?)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_operation_retry(
        &self,
        claim: &ClaimedOperation,
        uncertain: bool,
        retry_at: DateTime<Utc>,
        http_status: Option<i32>,
        error: Value,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_session_sequence(&mut tx, claim.operation.session_id).await?;
        let current = lock_valid_operation_claim(&mut tx, claim).await?;
        let next_status = match claim.phase {
            OperationPhase::Submission if current.status == OperationStatus::Submitting => {
                if uncertain {
                    "submitting"
                } else {
                    "pending"
                }
            }
            OperationPhase::ResultRetrieval if current.status == OperationStatus::Accepted => {
                "accepted"
            }
            OperationPhase::Cancellation if current.status == OperationStatus::Submitting => {
                if uncertain {
                    "submitting"
                } else {
                    "pending"
                }
            }
            _ => {
                return Err(StoreError::InvalidInput(
                    "operation retry does not match its claim phase and status".into(),
                ));
            }
        };
        sqlx::query(
            "UPDATE session_operations SET status=$2,next_attempt_at=$3,error=$4, \
             lease_token=NULL,lease_expires_at=NULL WHERE id=$1",
        )
        .bind(claim.operation.id.0)
        .bind(next_status)
        .bind(retry_at)
        .bind(Json(error.clone()))
        .execute(&mut *tx)
        .await?;
        finish_operation_attempt(
            &mut tx,
            claim,
            if uncertain { "unknown" } else { "failed" },
            http_status,
            Some(error),
        )
        .await?;
        emit_update(
            &mut tx,
            claim.operation.session_id,
            "operation.changed",
            &json!({"operation_id":claim.operation.id}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn record_operation_pending_result(
        &self,
        claim: &ClaimedOperation,
        check_again_at: DateTime<Utc>,
        http_status: Option<i32>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_session_sequence(&mut tx, claim.operation.session_id).await?;
        let current = lock_valid_operation_claim(&mut tx, claim).await?;
        if claim.phase != OperationPhase::ResultRetrieval
            || current.status != OperationStatus::Accepted
        {
            return Err(StoreError::InvalidInput(
                "a nonterminal result requires an accepted result-retrieval claim".into(),
            ));
        }
        sqlx::query(
            "UPDATE session_operations SET next_attempt_at=$2,error=NULL, \
             lease_token=NULL,lease_expires_at=NULL WHERE id=$1",
        )
        .bind(claim.operation.id.0)
        .bind(check_again_at)
        .execute(&mut *tx)
        .await?;
        finish_operation_attempt(&mut tx, claim, "succeeded", http_status, None).await?;
        if current.has_error {
            emit_update(
                &mut tx,
                claim.operation.session_id,
                "operation.changed",
                &json!({"operation_id":claim.operation.id}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn complete_withdrawal(
        &self,
        claim: &ClaimedOperation,
        request_expires_at: Option<DateTime<Utc>>,
    ) -> Result<(bool, Vec<EventRecord>)> {
        if claim.phase != OperationPhase::Withdrawal
            || claim.operation.kind != OperationKind::Withdraw
        {
            return Err(StoreError::InvalidInput(
                "withdrawal completion requires a withdrawal claim".into(),
            ));
        }
        let target_id = claim
            .operation
            .target_operation_id
            .ok_or_else(|| StoreError::InvalidInput("withdrawal operation has no target".into()))?;
        let mut tx = self.pool.begin().await?;
        let mut sequence = lock_session_sequence(&mut tx, claim.operation.session_id).await?;
        let current = lock_valid_operation_claim(&mut tx, claim).await?;
        if current.status != OperationStatus::Submitting {
            return Err(StoreError::InvalidInput(
                "withdrawal is not in a claimable state".into(),
            ));
        }
        let target = sqlx::query(
            "SELECT kind,status,lease_token FROM session_operations \
             WHERE session_id=$1 AND id=$2 FOR UPDATE",
        )
        .bind(claim.operation.session_id.0)
        .bind(target_id.0)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::OperationNotFound(target_id))?;
        let target_status: String = target.try_get("status")?;
        let target_status = OperationStatus::parse(&target_status)
            .ok_or_else(|| invalid("operation status", target_status))?;
        let target_lease: Option<Uuid> = target.try_get("lease_token")?;
        let target_kind = parse_operation_kind(target.try_get("kind")?)?;
        let withdrawn = target_status == OperationStatus::Pending && target_lease.is_none();
        let mut events = Vec::with_capacity(if withdrawn { 2 } else { 1 });

        if withdrawn {
            sqlx::query(
                "UPDATE session_operations SET status='cancelled',completed_at=now(), \
                 lease_token=NULL,lease_expires_at=NULL WHERE id=$1",
            )
            .bind(target_id.0)
            .execute(&mut *tx)
            .await?;
            events.push(
                insert_operation_event(
                    &mut tx,
                    claim.operation.session_id,
                    target_id,
                    target_kind,
                    OperationStatus::Cancelled,
                    sequence,
                )
                .await?,
            );
            sequence += 1;
            if let Some(expires_at) = request_expires_at {
                sqlx::query(
                    "UPDATE session_operation_requests SET expires_at=$2 WHERE operation_id=$1",
                )
                .bind(target_id.0)
                .bind(expires_at)
                .execute(&mut *tx)
                .await?;
            }
        }

        let result = format!("{{\"kind\":\"withdraw\",\"value\":{{\"withdrawn\":{withdrawn}}}}}");
        sqlx::query(
            "UPDATE session_operations SET status='succeeded',result=$2::json,completed_at=now(), \
             lease_token=NULL,lease_expires_at=NULL WHERE id=$1",
        )
        .bind(claim.operation.id.0)
        .bind(result)
        .execute(&mut *tx)
        .await?;
        if let Some(expires_at) = request_expires_at {
            sqlx::query(
                "UPDATE session_operation_requests SET expires_at=$2 WHERE operation_id=$1",
            )
            .bind(claim.operation.id.0)
            .bind(expires_at)
            .execute(&mut *tx)
            .await?;
        }
        events.push(
            insert_operation_event(
                &mut tx,
                claim.operation.session_id,
                claim.operation.id,
                OperationKind::Withdraw,
                OperationStatus::Succeeded,
                sequence,
            )
            .await?,
        );
        sequence += 1;
        sqlx::query("UPDATE sessions SET next_event_sequence=$2,updated_at=now() WHERE id=$1")
            .bind(claim.operation.session_id.0)
            .bind(sequence)
            .execute(&mut *tx)
            .await?;
        finish_operation_attempt(&mut tx, claim, "succeeded", None, None).await?;
        if withdrawn {
            emit_update(
                &mut tx,
                claim.operation.session_id,
                "operation.changed",
                &json!({"operation_id":target_id}),
            )
            .await?;
        }
        emit_update(
            &mut tx,
            claim.operation.session_id,
            "operation.changed",
            &json!({"operation_id":claim.operation.id}),
        )
        .await?;
        for event in &events {
            emit_update(
                &mut tx,
                claim.operation.session_id,
                "event.changed",
                &json!({"event_id":event.id}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok((withdrawn, events))
    }

    pub async fn complete_operation(
        &self,
        claim: &ClaimedOperation,
        status: OperationStatus,
        result: Option<&RawValue>,
        error: Option<Value>,
        request_expires_at: Option<DateTime<Utc>>,
    ) -> Result<EventRecord> {
        self.complete_operation_with_http_status(
            claim,
            status,
            result,
            error,
            request_expires_at,
            None,
        )
        .await
    }

    pub async fn complete_operation_with_http_status(
        &self,
        claim: &ClaimedOperation,
        status: OperationStatus,
        result: Option<&RawValue>,
        error: Option<Value>,
        request_expires_at: Option<DateTime<Utc>>,
        http_status: Option<i32>,
    ) -> Result<EventRecord> {
        if !matches!(
            status,
            OperationStatus::Succeeded
                | OperationStatus::Failed
                | OperationStatus::Cancelled
                | OperationStatus::Unknown
        ) {
            return Err(StoreError::InvalidInput(
                "operation completion requires a terminal status".into(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        let next_sequence = lock_session_sequence(&mut tx, claim.operation.session_id).await?;
        let current = lock_valid_operation_claim(&mut tx, claim).await?;
        let valid_phase = match claim.phase {
            OperationPhase::Submission => {
                current.status == OperationStatus::Submitting
                    && status == OperationStatus::Failed
                    && current.gateway_job_id.is_none()
            }
            OperationPhase::ResultRetrieval => current.status == OperationStatus::Accepted,
            OperationPhase::Cancellation => current.status == OperationStatus::Submitting,
            OperationPhase::Withdrawal => false,
        };
        if !valid_phase {
            return Err(StoreError::InvalidInput(
                "operation completion does not match its claim phase and status".into(),
            ));
        }
        sqlx::query(
            "UPDATE session_operations SET status=$2,result=$3::json,error=$4,completed_at=now(), \
             lease_token=NULL,lease_expires_at=NULL WHERE id=$1",
        )
        .bind(claim.operation.id.0)
        .bind(status.as_str())
        .bind(result.map(RawValue::get))
        .bind(error.as_ref().map(|value| Json(value.clone())))
        .execute(&mut *tx)
        .await?;
        if let Some(expires_at) = request_expires_at {
            sqlx::query(
                "UPDATE session_operation_requests SET expires_at=$2 WHERE operation_id=$1",
            )
            .bind(claim.operation.id.0)
            .bind(expires_at)
            .execute(&mut *tx)
            .await?;
        }
        let payload = format!(
            "{{\"operation_id\":\"{}\",\"operation_kind\":\"{}\",\"outcome_status\":\"{}\"}}",
            claim.operation.id,
            operation_kind_str(claim.operation.kind),
            status.as_str()
        );
        let event_id = EventId::new();
        let row = sqlx::query(
            "INSERT INTO session_events \
             (id,session_id,sequence,type,payload,operation_id) \
             VALUES ($1,$2,$3,'operation_completed',$4::json,$5) \
             RETURNING id,session_id,sequence,type,payload::text AS payload,operation_id,wait_id, \
             status,created_at,handled_at",
        )
        .bind(event_id.0)
        .bind(claim.operation.session_id.0)
        .bind(next_sequence)
        .bind(payload)
        .bind(claim.operation.id.0)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE sessions SET next_event_sequence=next_event_sequence+1,updated_at=now() WHERE id=$1",
        )
        .bind(claim.operation.session_id.0)
        .execute(&mut *tx)
        .await?;
        finish_operation_attempt(&mut tx, claim, "succeeded", http_status, None).await?;
        let event = event_from_row(&row)?;
        emit_update(
            &mut tx,
            claim.operation.session_id,
            "operation.changed",
            &json!({"operation_id":claim.operation.id}),
        )
        .await?;
        emit_update(
            &mut tx,
            claim.operation.session_id,
            "event.changed",
            &json!({"event_id":event.id}),
        )
        .await?;
        tx.commit().await?;
        Ok(event)
    }

    // Wait resolution. Session-row locking serializes event-sequence allocation.

    pub async fn resolve_wait(
        &self,
        wait_id: WaitId,
        idempotency_key: &str,
        response: &RawValue,
    ) -> Result<WaitResolution> {
        let hash = fingerprint(response);
        let mut tx = self.pool.begin().await?;
        let session_id: Option<Uuid> =
            sqlx::query_scalar("SELECT session_id FROM session_waits WHERE id=$1")
                .bind(wait_id.0)
                .fetch_optional(&mut *tx)
                .await?;
        let session_id = SessionId(session_id.ok_or(StoreError::WaitNotFound(wait_id))?);
        let next_sequence = lock_session_sequence(&mut tx, session_id).await?;
        let row = sqlx::query(
            "SELECT status,resolution_mode,expires_at,resolution_idempotency_key,resolution_hash \
             FROM session_waits WHERE id=$1 FOR UPDATE",
        )
        .bind(wait_id.0)
        .fetch_one(&mut *tx)
        .await?;
        let status: String = row.try_get("status")?;
        if status == "resolved" {
            let stored_key: Option<String> = row.try_get("resolution_idempotency_key")?;
            let stored_hash: Option<String> = row.try_get("resolution_hash")?;
            if stored_key.as_deref() != Some(idempotency_key)
                || stored_hash.as_deref() != Some(hash.as_str())
            {
                return Err(StoreError::IdempotencyConflict);
            }
            let event = wait_event_in_tx(&mut tx, wait_id).await?;
            tx.commit().await?;
            return Ok(WaitResolution {
                event,
                newly_resolved: false,
            });
        }
        let mode: String = row.try_get("resolution_mode")?;
        let expires_at: Option<DateTime<Utc>> = row.try_get("expires_at")?;
        let database_now = database_clock(&mut tx).await?;
        if status != "pending"
            || !matches!(mode.as_str(), "external" | "either")
            || expires_at.is_some_and(|deadline| database_now >= deadline)
        {
            return Err(StoreError::WaitNotResolvable);
        }
        sqlx::query(
            "UPDATE session_waits SET status='resolved',resolution_payload=$2::json, \
             resolution_idempotency_key=$3,resolution_hash=$4,finished_at=now() WHERE id=$1",
        )
        .bind(wait_id.0)
        .bind(response.get())
        .bind(idempotency_key)
        .bind(hash)
        .execute(&mut *tx)
        .await?;
        let event = insert_wait_event(
            &mut tx,
            session_id,
            wait_id,
            next_sequence,
            WaitFinish::Resolved,
        )
        .await?;
        advance_event_sequence(&mut tx, session_id).await?;
        emit_update(
            &mut tx,
            session_id,
            "wait.changed",
            &json!({"wait_id":wait_id}),
        )
        .await?;
        emit_update(
            &mut tx,
            session_id,
            "event.changed",
            &json!({"event_id":event.id}),
        )
        .await?;
        tx.commit().await?;
        Ok(WaitResolution {
            event,
            newly_resolved: true,
        })
    }

    pub async fn expire_wait(&self, wait_id: WaitId) -> Result<Option<EventRecord>> {
        let mut tx = self.pool.begin().await?;
        let session_id: Option<Uuid> =
            sqlx::query_scalar("SELECT session_id FROM session_waits WHERE id=$1")
                .bind(wait_id.0)
                .fetch_optional(&mut *tx)
                .await?;
        let session_id = SessionId(session_id.ok_or(StoreError::WaitNotFound(wait_id))?);
        let next_sequence = lock_session_sequence(&mut tx, session_id).await?;
        let wait = sqlx::query(
            "SELECT status,resolution_mode,expires_at FROM session_waits WHERE id=$1 FOR UPDATE",
        )
        .bind(wait_id.0)
        .fetch_one(&mut *tx)
        .await?;
        let status: String = wait.try_get("status")?;
        let mode: String = wait.try_get("resolution_mode")?;
        let expires_at: Option<DateTime<Utc>> = wait.try_get("expires_at")?;
        let now = database_clock(&mut tx).await?;
        if status != "pending"
            || !matches!(mode.as_str(), "expiration" | "either")
            || expires_at.is_none_or(|deadline| deadline > now)
        {
            tx.rollback().await?;
            return Ok(None);
        }
        sqlx::query("UPDATE session_waits SET status='expired',finished_at=now() WHERE id=$1")
            .bind(wait_id.0)
            .execute(&mut *tx)
            .await?;
        let event = insert_wait_event(
            &mut tx,
            session_id,
            wait_id,
            next_sequence,
            WaitFinish::Expired,
        )
        .await?;
        advance_event_sequence(&mut tx, session_id).await?;
        emit_update(
            &mut tx,
            session_id,
            "wait.changed",
            &json!({"wait_id":wait_id}),
        )
        .await?;
        emit_update(
            &mut tx,
            session_id,
            "event.changed",
            &json!({"event_id":event.id}),
        )
        .await?;
        tx.commit().await?;
        Ok(Some(event))
    }

    // Durable callback reconciliation, including callbacks that precede job mapping.

    pub async fn record_callback_receipt(
        &self,
        connection: GatewayConnectionId,
        gateway_event_id: Uuid,
        gateway_job_id: Uuid,
        payload: &RawValue,
    ) -> Result<CallbackReceipt> {
        let mut tx = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO gateway_callback_receipts \
             (id,gateway_connection_id,gateway_event_id,gateway_job_id,payload) \
             VALUES ($1,$2,$3,$4,$5::json) \
             ON CONFLICT (gateway_connection_id,gateway_event_id) DO NOTHING",
        )
        .bind(Uuid::new_v4())
        .bind(&connection.0)
        .bind(gateway_event_id)
        .bind(gateway_job_id)
        .bind(payload.get())
        .execute(&mut *tx)
        .await?;
        let row = sqlx::query(
            "SELECT id,gateway_connection_id,gateway_event_id,gateway_job_id, \
             payload::text AS payload,operation_id,status,received_at \
             FROM gateway_callback_receipts \
             WHERE gateway_connection_id=$1 AND gateway_event_id=$2 FOR UPDATE",
        )
        .bind(&connection.0)
        .bind(gateway_event_id)
        .fetch_one(&mut *tx)
        .await?;
        let receipt = callback_from_row(&row)?;
        if inserted.rows_affected() == 0
            && (receipt.gateway_job_id != gateway_job_id || receipt.payload.get() != payload.get())
        {
            return Err(StoreError::IdempotencyConflict);
        }
        tx.commit().await?;
        Ok(receipt)
    }

    pub async fn claim_callback_receipt(
        &self,
        lease_for: Duration,
    ) -> Result<Option<ClaimedCallbackReceipt>> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT id FROM gateway_callback_receipts \
             WHERE status IN ('pending','processing') AND next_attempt_at<=clock_timestamp() \
               AND (lease_token IS NULL OR lease_expires_at<=clock_timestamp()) \
             ORDER BY next_attempt_at,received_at,id FOR UPDATE SKIP LOCKED LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(None);
        };
        let id: Uuid = row.try_get("id")?;
        let lease_token = Uuid::new_v4();
        let row = sqlx::query(
            "UPDATE gateway_callback_receipts SET status='processing',lease_token=$2, \
             lease_expires_at=clock_timestamp()+($3 * interval '1 millisecond') WHERE id=$1 \
             RETURNING id,gateway_connection_id,gateway_event_id,gateway_job_id, \
             payload::text AS payload,operation_id,status,received_at",
        )
        .bind(id)
        .bind(lease_token)
        .bind(duration_millis(lease_for)?)
        .fetch_one(&mut *tx)
        .await?;
        let receipt = callback_from_row(&row)?;
        tx.commit().await?;
        Ok(Some(ClaimedCallbackReceipt {
            receipt,
            lease_token,
        }))
    }

    pub async fn match_callback_receipt(
        &self,
        claim: &ClaimedCallbackReceipt,
        retry_at: DateTime<Utc>,
    ) -> Result<Option<OperationId>> {
        let mut tx = self.pool.begin().await?;
        lock_valid_callback_claim(&mut tx, claim).await?;
        let row = sqlx::query(
            "SELECT gateway_connection_id,gateway_job_id \
             FROM gateway_callback_receipts WHERE id=$1",
        )
        .bind(claim.receipt.id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StoreError::InvalidInput("callback receipt was not found".into()))?;
        let connection: String = row.try_get("gateway_connection_id")?;
        let job_id: Uuid = row.try_get("gateway_job_id")?;
        let operation: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM session_operations \
             WHERE gateway_connection_id=$1 AND gateway_job_id=$2",
        )
        .bind(&connection)
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE gateway_callback_receipts SET operation_id=$2,next_attempt_at=$3, \
             status=CASE WHEN $2 IS NULL THEN 'pending' ELSE status END, \
             lease_token=CASE WHEN $2 IS NULL THEN NULL ELSE lease_token END, \
             lease_expires_at=CASE WHEN $2 IS NULL THEN NULL ELSE lease_expires_at END \
             WHERE id=$1",
        )
        .bind(claim.receipt.id)
        .bind(operation)
        .bind(retry_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(operation.map(OperationId))
    }

    pub async fn complete_callback_receipt(&self, claim: &ClaimedCallbackReceipt) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_valid_callback_claim(&mut tx, claim).await?;
        sqlx::query(
            "UPDATE session_operations SET next_attempt_at=LEAST(next_attempt_at,clock_timestamp()) \
             WHERE id=(SELECT operation_id FROM gateway_callback_receipts WHERE id=$1) \
               AND status='accepted'",
        )
        .bind(claim.receipt.id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE gateway_callback_receipts SET status='processed',processed_at=now(), \
             lease_token=NULL,lease_expires_at=NULL WHERE id=$1",
        )
        .bind(claim.receipt.id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn block_callback_receipt(
        &self,
        claim: &ClaimedCallbackReceipt,
        error: Value,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_valid_callback_claim(&mut tx, claim).await?;
        sqlx::query(
            "UPDATE gateway_callback_receipts SET status='blocked',last_error=$2, \
             lease_token=NULL,lease_expires_at=NULL WHERE id=$1",
        )
        .bind(claim.receipt.id)
        .bind(Json(error))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn block_unmatched_callback_receipt(
        &self,
        receipt_id: Uuid,
        error: Value,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE gateway_callback_receipts SET status='blocked',last_error=$2 \
             WHERE id=$1 AND status='pending' AND operation_id IS NULL \
               AND NOT EXISTS (SELECT 1 FROM session_operations o \
                 WHERE o.gateway_connection_id=gateway_callback_receipts.gateway_connection_id \
                   AND o.gateway_job_id=gateway_callback_receipts.gateway_job_id)",
        )
        .bind(receipt_id)
        .bind(Json(error))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn renew_callback_receipt_claim(
        &self,
        claim: &ClaimedCallbackReceipt,
        lease_for: Duration,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_valid_callback_claim(&mut tx, claim).await?;
        sqlx::query(
            "UPDATE gateway_callback_receipts \
             SET lease_expires_at=clock_timestamp()+($2 * interval '1 millisecond') WHERE id=$1",
        )
        .bind(claim.receipt.id)
        .bind(duration_millis(lease_for)?)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn retry_callback_receipt(
        &self,
        claim: &ClaimedCallbackReceipt,
        retry_at: DateTime<Utc>,
        error: Value,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        lock_valid_callback_claim(&mut tx, claim).await?;
        sqlx::query(
            "UPDATE gateway_callback_receipts SET status='pending',next_attempt_at=$2, \
             last_error=$3,lease_token=NULL,lease_expires_at=NULL \
             WHERE id=$1",
        )
        .bind(claim.receipt.id)
        .bind(retry_at)
        .bind(Json(error))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_expired_operation_requests(&self, limit: i64) -> Result<u64> {
        if !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput(
                "limit must be between 1 and 1000".into(),
            ));
        }
        let candidates = sqlx::query(
            "SELECT r.operation_id,o.session_id FROM session_operation_requests r \
             JOIN session_operations o ON o.id=r.operation_id \
             WHERE r.expires_at<=clock_timestamp() \
               AND o.status IN ('succeeded','failed','cancelled','unknown') \
             ORDER BY r.expires_at LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut removed = 0;
        for candidate in candidates {
            let id = OperationId(candidate.try_get("operation_id")?);
            let session_id = SessionId(candidate.try_get("session_id")?);
            let mut tx = self.pool.begin().await?;
            lock_session_sequence(&mut tx, session_id).await?;
            let eligible: Option<Uuid> = sqlx::query_scalar(
                "SELECT r.operation_id FROM session_operation_requests r \
                 JOIN session_operations o ON o.id=r.operation_id \
                 WHERE r.operation_id=$1 AND r.expires_at<=clock_timestamp() \
                   AND o.status IN ('succeeded','failed','cancelled','unknown') \
                 FOR UPDATE OF r SKIP LOCKED",
            )
            .bind(id.0)
            .fetch_optional(&mut *tx)
            .await?;
            if eligible.is_none() {
                tx.rollback().await?;
                continue;
            }
            sqlx::query("DELETE FROM session_operation_requests WHERE operation_id=$1")
                .bind(id.0)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "UPDATE session_operations SET request_removed_at=clock_timestamp() WHERE id=$1",
            )
            .bind(id.0)
            .execute(&mut *tx)
            .await?;
            emit_update(
                &mut tx,
                session_id,
                "operation.changed",
                &json!({"operation_id":id}),
            )
            .await?;
            tx.commit().await?;
            removed += 1;
        }
        Ok(removed)
    }

    pub async fn expired_operation_request_candidates(
        &self,
        after: Option<(DateTime<Utc>, OperationId)>,
        limit: i64,
    ) -> Result<Vec<(DateTime<Utc>, OperationId)>> {
        if !(1..=1000).contains(&limit) {
            return Err(StoreError::InvalidInput("limit must be 1..1000".into()));
        }
        let rows = sqlx::query(
            "SELECT r.expires_at,r.operation_id FROM session_operation_requests r \
             JOIN session_operations o ON o.id=r.operation_id \
             WHERE r.expires_at<=clock_timestamp() \
               AND o.status IN ('succeeded','failed','cancelled','unknown') \
               AND ($1::timestamptz IS NULL OR (r.expires_at,r.operation_id)>($1::timestamptz,$2::uuid)) \
             ORDER BY r.expires_at,r.operation_id LIMIT $3",
        )
        .bind(after.map(|(expires, _)| expires))
        .bind(after.map(|(_, id)| id.0))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get("expires_at")?,
                    OperationId(row.try_get("operation_id")?),
                ))
            })
            .collect()
    }

    pub async fn delete_expired_operation_request(&self, id: OperationId) -> Result<bool> {
        let session_id: Option<Uuid> =
            sqlx::query_scalar("SELECT session_id FROM session_operations WHERE id=$1")
                .bind(id.0)
                .fetch_optional(&self.pool)
                .await?;
        let Some(session_id) = session_id else {
            return Ok(false);
        };
        let session_id = SessionId(session_id);
        let mut tx = self.pool.begin().await?;
        let locked: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM sessions WHERE id=$1 FOR UPDATE SKIP LOCKED")
                .bind(session_id.0)
                .fetch_optional(&mut *tx)
                .await?;
        if locked.is_none() {
            return Ok(false);
        }
        let eligible: Option<Uuid> = sqlx::query_scalar(
            "SELECT r.operation_id FROM session_operation_requests r \
             JOIN session_operations o ON o.id=r.operation_id \
             WHERE r.operation_id=$1 AND r.expires_at<=clock_timestamp() \
               AND o.status IN ('succeeded','failed','cancelled','unknown') \
             FOR UPDATE OF r SKIP LOCKED",
        )
        .bind(id.0)
        .fetch_optional(&mut *tx)
        .await?;
        if eligible.is_none() {
            return Ok(false);
        }
        sqlx::query("DELETE FROM session_operation_requests WHERE operation_id=$1")
            .bind(id.0)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE session_operations SET request_removed_at=clock_timestamp() WHERE id=$1",
        )
        .bind(id.0)
        .execute(&mut *tx)
        .await?;
        emit_update(
            &mut tx,
            session_id,
            "operation.changed",
            &json!({"operation_id":id}),
        )
        .await?;
        tx.commit().await?;
        Ok(true)
    }
}

const SESSION_SELECT: &str = "SELECT id,project_id,harness_id,harness_version,configuration::text AS configuration, \
     state::text AS state,name,metadata,status,state_version,next_event_sequence, \
     next_history_sequence,processing_enabled,processing_error,processing_revision,current_event_id,lease_token, \
     (SELECT e.id FROM session_events e WHERE e.session_id=sessions.id \
       AND e.status='blocked' ORDER BY e.sequence LIMIT 1) AS blocked_event_id, \
     lease_expires_at,created_at,updated_at FROM sessions WHERE id=$1";
const EVENT_SELECT: &str = "SELECT id,session_id,sequence,type,payload::text AS payload,operation_id,wait_id,status, \
     created_at,handled_at FROM session_events WHERE id=$1";
const OPERATION_SELECT_BASE: &str = "SELECT o.id,o.session_id,o.source_event_id,o.kind,o.gateway_connection_id, \
     o.target_operation_id,o.previous_operation_id,o.gateway_idempotency_key,o.request_hash, \
     o.gateway_job_id,o.status,o.result::text AS result,o.error,o.created_at,o.completed_at \
     FROM session_operations o";
const OPERATION_SELECT: &str = "SELECT o.id,o.session_id,o.source_event_id,o.kind,o.gateway_connection_id, \
     o.target_operation_id,o.previous_operation_id,o.gateway_idempotency_key,o.request_hash, \
     o.gateway_job_id,o.status,o.result::text AS result,o.error,o.created_at,o.completed_at \
     FROM session_operations o WHERE o.id=$1";
const WAIT_SELECT_BASE: &str = "SELECT id,session_id,source_event_id,resolution_mode,payload::text AS payload, \
     response_schema::text AS response_schema,expires_at,status, \
     resolution_payload::text AS resolution_payload,finished_at,created_at FROM session_waits";
const WAIT_SELECT: &str = "SELECT id,session_id,source_event_id,resolution_mode,payload::text AS payload, \
     response_schema::text AS response_schema,expires_at,status, \
     resolution_payload::text AS resolution_payload,finished_at,created_at \
     FROM session_waits WHERE id=$1";

fn session_from_row(row: &sqlx::postgres::PgRow) -> Result<SessionRecord> {
    Ok(SessionRecord {
        id: SessionId(row.try_get("id")?),
        project_id: ProjectId(row.try_get("project_id")?),
        harness_id: HarnessId(row.try_get("harness_id")?),
        harness_version: HarnessVersion(row.try_get("harness_version")?),
        configuration: raw_json(row.try_get("configuration")?, "session configuration")?,
        state: raw_json(row.try_get("state")?, "session state")?,
        name: row.try_get("name")?,
        metadata: row.try_get::<Json<Value>, _>("metadata")?.0,
        status: parse_session_status(row.try_get("status")?)?,
        state_version: StateVersion(row.try_get("state_version")?),
        next_event_sequence: EventSequence(row.try_get("next_event_sequence")?),
        next_history_sequence: HistorySequence(row.try_get("next_history_sequence")?),
        processing_enabled: row.try_get("processing_enabled")?,
        processing_error: row
            .try_get::<Option<Json<Value>>, _>("processing_error")?
            .map(|value| value.0),
        processing_revision: row.try_get("processing_revision")?,
        blocked_event_id: row
            .try_get::<Option<Uuid>, _>("blocked_event_id")?
            .map(EventId),
        current_event_id: row
            .try_get::<Option<Uuid>, _>("current_event_id")?
            .map(EventId),
        lease_token: row.try_get("lease_token")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn session_summary_from_row(row: &sqlx::postgres::PgRow) -> Result<SessionSummary> {
    Ok(SessionSummary {
        id: SessionId(row.try_get("id")?),
        project_id: ProjectId(row.try_get("project_id")?),
        harness_id: HarnessId(row.try_get("harness_id")?),
        harness_version: HarnessVersion(row.try_get("harness_version")?),
        name: row.try_get("name")?,
        metadata: row.try_get::<Json<Value>, _>("metadata")?.0,
        status: parse_session_status(row.try_get("status")?)?,
        state_version: StateVersion(row.try_get("state_version")?),
        processing_enabled: row.try_get("processing_enabled")?,
        processing_error: row
            .try_get::<Option<Json<Value>>, _>("processing_error")?
            .map(|value| value.0),
        processing_revision: row.try_get("processing_revision")?,
        blocked_event_id: row
            .try_get::<Option<Uuid>, _>("blocked_event_id")?
            .map(EventId),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(crate) fn event_from_row(row: &sqlx::postgres::PgRow) -> Result<EventRecord> {
    let event_type: String = row.try_get("type")?;
    let status: String = row.try_get("status")?;
    Ok(EventRecord {
        id: EventId(row.try_get("id")?),
        session_id: SessionId(row.try_get("session_id")?),
        sequence: EventSequence(row.try_get("sequence")?),
        event_type: EventType::parse(&event_type)
            .ok_or_else(|| invalid("event type", event_type))?,
        payload: raw_json(row.try_get("payload")?, "event payload")?,
        operation_id: row
            .try_get::<Option<Uuid>, _>("operation_id")?
            .map(OperationId),
        wait_id: row.try_get::<Option<Uuid>, _>("wait_id")?.map(WaitId),
        status: match status.as_str() {
            "pending" => EventStatus::Pending,
            "processing" => EventStatus::Processing,
            "handled" => EventStatus::Handled,
            "blocked" => EventStatus::Blocked,
            _ => return Err(invalid("event status", status)),
        },
        created_at: row.try_get("created_at")?,
        handled_at: row.try_get("handled_at")?,
    })
}

pub(crate) fn history_from_row(row: &sqlx::postgres::PgRow) -> Result<HistoryRecord> {
    Ok(HistoryRecord {
        id: HistoryEntryId(row.try_get("id")?),
        session_id: SessionId(row.try_get("session_id")?),
        sequence: HistorySequence(row.try_get("sequence")?),
        source_event_id: row
            .try_get::<Option<Uuid>, _>("source_event_id")?
            .map(EventId),
        role: row.try_get("role")?,
        message: raw_json(row.try_get("message")?, "history message")?,
        created_at: row.try_get("created_at")?,
    })
}

fn operation_from_row(row: &sqlx::postgres::PgRow) -> Result<OperationRecord> {
    let status: String = row.try_get("status")?;
    Ok(OperationRecord {
        id: OperationId(row.try_get("id")?),
        session_id: SessionId(row.try_get("session_id")?),
        source_event_id: EventId(row.try_get("source_event_id")?),
        kind: parse_operation_kind(row.try_get("kind")?)?,
        gateway_connection_id: row
            .try_get::<Option<String>, _>("gateway_connection_id")?
            .map(GatewayConnectionId),
        target_operation_id: row
            .try_get::<Option<Uuid>, _>("target_operation_id")?
            .map(OperationId),
        previous_operation_id: row
            .try_get::<Option<Uuid>, _>("previous_operation_id")?
            .map(OperationId),
        gateway_idempotency_key: row.try_get("gateway_idempotency_key")?,
        request_hash: row.try_get("request_hash")?,
        gateway_job_id: row.try_get("gateway_job_id")?,
        status: OperationStatus::parse(&status)
            .ok_or_else(|| invalid("operation status", status))?,
        result: optional_raw(row.try_get("result")?, "operation result")?,
        error: row
            .try_get::<Option<Json<Value>>, _>("error")?
            .map(|value| value.0),
        created_at: row.try_get("created_at")?,
        completed_at: row.try_get("completed_at")?,
    })
}

fn wait_from_row(row: &sqlx::postgres::PgRow) -> Result<WaitRecord> {
    let status: String = row.try_get("status")?;
    Ok(WaitRecord {
        id: WaitId(row.try_get("id")?),
        session_id: SessionId(row.try_get("session_id")?),
        source_event_id: EventId(row.try_get("source_event_id")?),
        mode: parse_wait_mode(row.try_get("resolution_mode")?)?,
        payload: raw_json(row.try_get("payload")?, "wait payload")?,
        response_schema: optional_raw(row.try_get("response_schema")?, "wait response schema")?,
        expires_at: row.try_get("expires_at")?,
        status: match status.as_str() {
            "pending" => WaitStatus::Pending,
            "resolved" => WaitStatus::Resolved,
            "expired" => WaitStatus::Expired,
            "cancelled" => WaitStatus::Cancelled,
            _ => return Err(invalid("wait status", status)),
        },
        resolution_payload: optional_raw(
            row.try_get("resolution_payload")?,
            "wait resolution payload",
        )?,
        created_at: row.try_get("created_at")?,
        finished_at: row.try_get("finished_at")?,
    })
}

fn callback_from_row(row: &sqlx::postgres::PgRow) -> Result<CallbackReceipt> {
    let status: String = row.try_get("status")?;
    Ok(CallbackReceipt {
        id: row.try_get("id")?,
        gateway_connection_id: GatewayConnectionId(row.try_get("gateway_connection_id")?),
        gateway_event_id: row.try_get("gateway_event_id")?,
        gateway_job_id: row.try_get("gateway_job_id")?,
        payload: raw_json(row.try_get("payload")?, "callback payload")?,
        operation_id: row
            .try_get::<Option<Uuid>, _>("operation_id")?
            .map(OperationId),
        status: match status.as_str() {
            "pending" => ReceiptStatus::Pending,
            "processing" => ReceiptStatus::Processing,
            "processed" => ReceiptStatus::Processed,
            "blocked" => ReceiptStatus::Blocked,
            _ => return Err(invalid("callback status", status)),
        },
        received_at: row.try_get("received_at")?,
    })
}

async fn lock_valid_session_claim(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    claim: &HandlerClaim,
) -> Result<(i64, i64)> {
    let row = sqlx::query(
        "SELECT next_history_sequence,state_version,current_event_id,lease_token,lease_expires_at \
         FROM sessions WHERE id=$1 FOR UPDATE",
    )
    .bind(claim.session_id.0)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(StoreError::SessionNotFound(claim.session_id))?;
    let now = database_clock(tx).await?;
    let current_event: Option<Uuid> = row.try_get("current_event_id")?;
    let lease_token: Option<Uuid> = row.try_get("lease_token")?;
    let lease_expires_at: Option<DateTime<Utc>> = row.try_get("lease_expires_at")?;
    if current_event != Some(claim.event_id.0)
        || lease_token != Some(claim.lease_token)
        || lease_expires_at.is_none_or(|expires| expires <= now)
    {
        return Err(StoreError::StaleClaim);
    }
    Ok((
        row.try_get("next_history_sequence")?,
        row.try_get("state_version")?,
    ))
}

async fn release_session_claim(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session_id: SessionId,
) -> Result<()> {
    sqlx::query(
        "UPDATE sessions SET current_event_id=NULL,lease_token=NULL,lease_expires_at=NULL, \
         updated_at=now() WHERE id=$1",
    )
    .bind(session_id.0)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn lock_valid_operation_claim(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    claim: &ClaimedOperation,
) -> Result<OperationLock> {
    let row = sqlx::query(
        "SELECT status,gateway_job_id,error IS NOT NULL AS has_error,lease_token,lease_expires_at \
         FROM session_operations WHERE id=$1 FOR UPDATE",
    )
    .bind(claim.operation.id.0)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(StoreError::OperationNotFound(claim.operation.id))?;
    let now = database_clock(tx).await?;
    let lease_token: Option<Uuid> = row.try_get("lease_token")?;
    let lease_expires_at: Option<DateTime<Utc>> = row.try_get("lease_expires_at")?;
    if lease_token != Some(claim.lease_token)
        || lease_expires_at.is_none_or(|expires| expires <= now)
    {
        return Err(StoreError::StaleClaim);
    }
    let status: String = row.try_get("status")?;
    Ok(OperationLock {
        status: OperationStatus::parse(&status)
            .ok_or_else(|| invalid("operation status", status))?,
        gateway_job_id: row.try_get("gateway_job_id")?,
        has_error: row.try_get("has_error")?,
    })
}

struct OperationLock {
    status: OperationStatus,
    gateway_job_id: Option<Uuid>,
    has_error: bool,
}

async fn lock_valid_callback_claim(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    claim: &ClaimedCallbackReceipt,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT lease_token,lease_expires_at FROM gateway_callback_receipts \
         WHERE id=$1 FOR UPDATE",
    )
    .bind(claim.receipt.id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| StoreError::InvalidInput("callback receipt was not found".into()))?;
    let now = database_clock(tx).await?;
    let lease_token: Option<Uuid> = row.try_get("lease_token")?;
    let lease_expires_at: Option<DateTime<Utc>> = row.try_get("lease_expires_at")?;
    if lease_token != Some(claim.lease_token)
        || lease_expires_at.is_none_or(|expires| expires <= now)
    {
        return Err(StoreError::StaleClaim);
    }
    Ok(())
}

async fn database_clock(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<DateTime<Utc>> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **tx)
        .await?)
}

async fn finish_operation_attempt(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    claim: &ClaimedOperation,
    status: &str,
    http_status: Option<i32>,
    error: Option<Value>,
) -> Result<()> {
    let changed = sqlx::query(
        "UPDATE session_operation_attempts SET status=$2,http_status=$3,error=$4,finished_at=now() \
         WHERE id=$1 AND lease_token=$5 AND status='running'",
    )
    .bind(claim.attempt_id)
    .bind(status)
    .bind(http_status)
    .bind(error.map(Json))
    .bind(claim.lease_token)
    .execute(&mut **tx)
    .await?;
    if changed.rows_affected() != 1 {
        return Err(StoreError::StaleClaim);
    }
    Ok(())
}

async fn lock_session_sequence(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session_id: SessionId,
) -> Result<i64> {
    sqlx::query_scalar("SELECT next_event_sequence FROM sessions WHERE id=$1 FOR UPDATE")
        .bind(session_id.0)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(StoreError::SessionNotFound(session_id))
}

async fn advance_event_sequence(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session_id: SessionId,
) -> Result<()> {
    sqlx::query(
        "UPDATE sessions SET next_event_sequence=next_event_sequence+1,updated_at=now() WHERE id=$1",
    )
    .bind(session_id.0)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_wait_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session_id: SessionId,
    wait_id: WaitId,
    sequence: i64,
    finish: WaitFinish,
) -> Result<EventRecord> {
    let reason = match finish {
        WaitFinish::Resolved => "resolved",
        WaitFinish::Expired => "expired",
        WaitFinish::Cancelled => "cancelled",
    };
    let payload = format!("{{\"wait_id\":\"{}\",\"reason\":\"{}\"}}", wait_id, reason);
    let row = sqlx::query(
        "INSERT INTO session_events (id,session_id,sequence,type,payload,wait_id) \
         VALUES ($1,$2,$3,'wait_resumed',$4::json,$5) \
         RETURNING id,session_id,sequence,type,payload::text AS payload,operation_id,wait_id, \
         status,created_at,handled_at",
    )
    .bind(EventId::new().0)
    .bind(session_id.0)
    .bind(sequence)
    .bind(payload)
    .bind(wait_id.0)
    .fetch_one(&mut **tx)
    .await?;
    event_from_row(&row)
}

async fn insert_operation_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session_id: SessionId,
    operation_id: OperationId,
    kind: OperationKind,
    status: OperationStatus,
    sequence: i64,
) -> Result<EventRecord> {
    let payload = format!(
        "{{\"operation_id\":\"{}\",\"operation_kind\":\"{}\",\"outcome_status\":\"{}\"}}",
        operation_id,
        operation_kind_str(kind),
        status.as_str()
    );
    let row = sqlx::query(
        "INSERT INTO session_events (id,session_id,sequence,type,payload,operation_id) \
         VALUES ($1,$2,$3,'operation_completed',$4::json,$5) \
         RETURNING id,session_id,sequence,type,payload::text AS payload,operation_id,wait_id, \
         status,created_at,handled_at",
    )
    .bind(EventId::new().0)
    .bind(session_id.0)
    .bind(sequence)
    .bind(payload)
    .bind(operation_id.0)
    .fetch_one(&mut **tx)
    .await?;
    event_from_row(&row)
}

async fn wait_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    wait_id: WaitId,
) -> Result<EventRecord> {
    let row = sqlx::query(
        "SELECT id,session_id,sequence,type,payload::text AS payload,operation_id,wait_id,status, \
         created_at,handled_at FROM session_events WHERE wait_id=$1 AND type='wait_resumed'",
    )
    .bind(wait_id.0)
    .fetch_one(&mut **tx)
    .await?;
    event_from_row(&row)
}

fn raw_json(text: String, kind: &'static str) -> Result<RawJson> {
    RawValue::from_string(text).map_err(|error| StoreError::InvalidStoredJson {
        kind,
        message: error.to_string(),
    })
}

fn optional_raw(text: Option<String>, kind: &'static str) -> Result<Option<RawJson>> {
    text.map(|text| raw_json(text, kind)).transpose()
}

fn invalid(kind: &'static str, message: String) -> StoreError {
    StoreError::InvalidStoredJson { kind, message }
}

fn fingerprint(value: &RawValue) -> String {
    format!("{:x}", Sha256::digest(value.get().as_bytes()))
}

fn duration_millis(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis())
        .map_err(|_| StoreError::InvalidInput("lease duration is too large".into()))
}

fn session_status_str(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Idle => "idle",
        SessionStatus::Running => "running",
        SessionStatus::Waiting => "waiting",
        SessionStatus::Cancelling => "cancelling",
        SessionStatus::Cancelled => "cancelled",
        SessionStatus::Failed => "failed",
    }
}

fn parse_session_status(status: String) -> Result<SessionStatus> {
    match status.as_str() {
        "idle" => Ok(SessionStatus::Idle),
        "running" => Ok(SessionStatus::Running),
        "waiting" => Ok(SessionStatus::Waiting),
        "cancelling" => Ok(SessionStatus::Cancelling),
        "cancelled" => Ok(SessionStatus::Cancelled),
        "failed" => Ok(SessionStatus::Failed),
        _ => Err(invalid("session status", status)),
    }
}

fn message_role(message: &agent_contracts::Message) -> &'static str {
    match message {
        agent_contracts::Message::User { .. } => "user",
        agent_contracts::Message::Assistant { .. } => "assistant",
        agent_contracts::Message::ToolResult { .. } => "tool_result",
        agent_contracts::Message::System { .. } => "system",
        agent_contracts::Message::Custom { .. } => "custom",
    }
}

fn operation_kind_str(kind: OperationKind) -> &'static str {
    match kind {
        OperationKind::Llm => "llm",
        OperationKind::Execution => "execution",
        OperationKind::LlmCancellation => "llm_cancellation",
        OperationKind::Withdraw => "withdraw",
    }
}

fn parse_operation_kind(kind: String) -> Result<OperationKind> {
    match kind.as_str() {
        "llm" => Ok(OperationKind::Llm),
        "execution" => Ok(OperationKind::Execution),
        "llm_cancellation" => Ok(OperationKind::LlmCancellation),
        "withdraw" => Ok(OperationKind::Withdraw),
        _ => Err(invalid("operation kind", kind)),
    }
}

fn wait_mode_str(mode: WaitMode) -> &'static str {
    match mode {
        WaitMode::External => "external",
        WaitMode::Expiration => "expiration",
        WaitMode::Either => "either",
    }
}

fn parse_wait_mode(mode: String) -> Result<WaitMode> {
    match mode.as_str() {
        "external" => Ok(WaitMode::External),
        "expiration" => Ok(WaitMode::Expiration),
        "either" => Ok(WaitMode::Either),
        _ => Err(invalid("wait mode", mode)),
    }
}

fn wait_status_str(status: WaitStatus) -> &'static str {
    match status {
        WaitStatus::Pending => "pending",
        WaitStatus::Resolved => "resolved",
        WaitStatus::Expired => "expired",
        WaitStatus::Cancelled => "cancelled",
    }
}
