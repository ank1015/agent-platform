use agent_contracts::{
    EventId, EventSequence, GatewayConnectionId, HarnessId, HarnessVersion, HistoryEntryId,
    HistorySequence, OperationId, OperationKind, ProjectId, SessionId, SessionStatus, StateVersion,
    WaitId, WaitMode,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, value::RawValue};
use uuid::Uuid;

pub type RawJson = Box<RawValue>;

#[derive(Debug)]
pub struct NewSession {
    pub id: SessionId,
    pub project_id: ProjectId,
    pub harness_id: HarnessId,
    pub harness_version: HarnessVersion,
    pub configuration: RawJson,
    pub state: RawJson,
    pub name: Option<String>,
    pub metadata: Value,
}

#[derive(Debug)]
pub struct SessionRecord {
    pub id: SessionId,
    pub project_id: ProjectId,
    pub harness_id: HarnessId,
    pub harness_version: HarnessVersion,
    pub configuration: RawJson,
    pub state: RawJson,
    pub name: Option<String>,
    pub metadata: Value,
    pub status: SessionStatus,
    pub state_version: StateVersion,
    pub next_event_sequence: EventSequence,
    pub next_history_sequence: HistorySequence,
    pub processing_enabled: bool,
    pub processing_error: Option<Value>,
    pub processing_revision: i64,
    pub blocked_event_id: Option<EventId>,
    pub current_event_id: Option<EventId>,
    pub lease_token: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct SessionSummary {
    pub id: SessionId,
    pub project_id: ProjectId,
    pub harness_id: HarnessId,
    pub harness_version: HarnessVersion,
    pub name: Option<String>,
    pub metadata: Value,
    pub status: SessionStatus,
    pub state_version: StateVersion,
    pub processing_enabled: bool,
    pub processing_error: Option<Value>,
    pub processing_revision: i64,
    pub blocked_event_id: Option<EventId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug)]
pub struct SessionCursor {
    pub created_at: DateTime<Utc>,
    pub id: SessionId,
}

#[derive(Debug, Default)]
pub struct SessionFilter {
    pub project_id: Option<ProjectId>,
    pub status: Option<SessionStatus>,
    pub harness_id: Option<HarnessId>,
    pub cursor: Option<SessionCursor>,
    pub limit: Option<i64>,
}

#[derive(Debug)]
pub struct SessionPage {
    pub sessions: Vec<SessionSummary>,
    pub next_cursor: Option<SessionCursor>,
}

#[derive(Debug)]
pub struct OperationalMetrics {
    pub active_handlers: i64,
    pub active_operation_claims: i64,
    pub pending_events: i64,
    pub blocked_events: i64,
    pub pending_operations: i64,
    pub submitting_operations: i64,
    pub accepted_operations: i64,
    pub overdue_waits: i64,
    pub cleanup_ready_requests: i64,
    pub removed_requests: i64,
    pub oldest_pending_event_age_seconds: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventType {
    UserMessage,
    OperationCompleted,
    CancellationRequested,
    WaitResumed,
}

impl EventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserMessage => "user_message",
            Self::OperationCompleted => "operation_completed",
            Self::CancellationRequested => "cancellation_requested",
            Self::WaitResumed => "wait_resumed",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "user_message" => Some(Self::UserMessage),
            "operation_completed" => Some(Self::OperationCompleted),
            "cancellation_requested" => Some(Self::CancellationRequested),
            "wait_resumed" => Some(Self::WaitResumed),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventStatus {
    Pending,
    Processing,
    Handled,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandlerAttemptStatus {
    Running,
    Committed,
    Errored,
    Abandoned,
}

#[derive(Debug)]
pub struct NewExternalEvent {
    pub session_id: SessionId,
    pub event_type: EventType,
    pub payload: RawJson,
    pub idempotency_key: Option<String>,
}

#[derive(Debug)]
pub struct EventRecord {
    pub id: EventId,
    pub session_id: SessionId,
    pub sequence: EventSequence,
    pub event_type: EventType,
    pub payload: RawJson,
    pub operation_id: Option<OperationId>,
    pub wait_id: Option<WaitId>,
    pub status: EventStatus,
    pub created_at: DateTime<Utc>,
    pub handled_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct ClaimedEvent {
    pub session: SessionRecord,
    pub event: EventRecord,
    pub attempt_id: Uuid,
    pub attempt_number: i32,
    pub lease_token: Uuid,
    pub history_through_sequence: HistorySequence,
}

#[derive(Clone, Debug)]
pub struct NewHistoryEntry {
    pub id: HistoryEntryId,
    pub message: RawJson,
}

#[derive(Debug)]
pub struct HistoryRecord {
    pub id: HistoryEntryId,
    pub session_id: SessionId,
    pub sequence: HistorySequence,
    pub source_event_id: Option<EventId>,
    pub role: String,
    pub message: RawJson,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationStatus {
    Pending,
    Submitting,
    Accepted,
    Succeeded,
    Failed,
    Cancelled,
    Unknown,
}

impl OperationStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Submitting => "submitting",
            Self::Accepted => "accepted",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "submitting" => Some(Self::Submitting),
            "accepted" => Some(Self::Accepted),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NewOperation {
    pub id: OperationId,
    pub kind: OperationKind,
    pub gateway_connection_id: Option<GatewayConnectionId>,
    pub target_operation_id: Option<OperationId>,
    pub previous_operation_id: Option<OperationId>,
    pub request: RawJson,
}

#[derive(Debug)]
pub struct OperationRecord {
    pub id: OperationId,
    pub session_id: SessionId,
    pub source_event_id: EventId,
    pub kind: OperationKind,
    pub gateway_connection_id: Option<GatewayConnectionId>,
    pub target_operation_id: Option<OperationId>,
    pub previous_operation_id: Option<OperationId>,
    pub gateway_idempotency_key: String,
    pub request_hash: String,
    pub gateway_job_id: Option<Uuid>,
    pub status: OperationStatus,
    pub result: Option<RawJson>,
    pub error: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct ClaimedOperation {
    pub operation: OperationRecord,
    pub request: Option<RawJson>,
    pub phase: OperationPhase,
    pub attempt_id: Uuid,
    pub attempt_number: i32,
    pub lease_token: Uuid,
    /// True when an earlier submission or cancellation attempt had uncertain acceptance.
    pub recovering_uncertain: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationPhase {
    Submission,
    ResultRetrieval,
    Cancellation,
    Withdrawal,
}

impl OperationPhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Submission => "submission",
            Self::ResultRetrieval => "result_retrieval",
            Self::Cancellation => "cancellation",
            Self::Withdrawal => "withdrawal",
        }
    }
}

#[derive(Clone, Debug)]
pub struct NewWait {
    pub id: WaitId,
    pub mode: WaitMode,
    pub payload: RawJson,
    pub response_schema: Option<RawJson>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitStatus {
    Pending,
    Resolved,
    Expired,
    Cancelled,
}

#[derive(Debug)]
pub struct WaitRecord {
    pub id: WaitId,
    pub session_id: SessionId,
    pub source_event_id: EventId,
    pub mode: WaitMode,
    pub payload: RawJson,
    pub response_schema: Option<RawJson>,
    pub expires_at: Option<DateTime<Utc>>,
    pub status: WaitStatus,
    pub resolution_payload: Option<RawJson>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug)]
pub struct WaitCursor {
    pub created_at: DateTime<Utc>,
    pub id: WaitId,
}

#[derive(Clone, Copy, Debug)]
pub struct DueWait {
    pub id: WaitId,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct WaitFilter {
    pub status: Option<WaitStatus>,
    pub cursor: Option<WaitCursor>,
    pub limit: Option<i64>,
}

#[derive(Debug)]
pub struct WaitPage {
    pub waits: Vec<WaitRecord>,
    pub next_cursor: Option<WaitCursor>,
}

#[derive(Clone, Debug)]
pub struct OutcomeCommit {
    pub state: RawJson,
    pub status: Option<SessionStatus>,
    pub history: Vec<NewHistoryEntry>,
    pub operations: Vec<NewOperation>,
    pub waits: Vec<NewWait>,
    pub cancelled_waits: Vec<WaitId>,
    pub progress: Vec<Value>,
}

#[derive(Debug)]
pub struct UpdateRecord {
    pub session_id: SessionId,
    pub sequence: i64,
    pub schema_version: i16,
    pub kind: String,
    pub payload: RawJson,
    pub source_event_id: Option<EventId>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct UpdatePage {
    pub updates: Vec<UpdateRecord>,
    pub through_sequence: i64,
    pub retained_sequence: i64,
    pub has_more: bool,
}

#[derive(Debug)]
pub struct EventSummary {
    pub id: EventId,
    pub session_id: SessionId,
    pub sequence: EventSequence,
    pub event_type: EventType,
    pub status: EventStatus,
    pub operation_id: Option<OperationId>,
    pub wait_id: Option<WaitId>,
    pub created_at: DateTime<Utc>,
    pub handled_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct OperationSummary {
    pub id: OperationId,
    pub session_id: SessionId,
    pub source_event_id: EventId,
    pub kind: OperationKind,
    pub status: OperationStatus,
    pub gateway_connection_id: Option<GatewayConnectionId>,
    pub gateway_job_id: Option<Uuid>,
    pub target_operation_id: Option<OperationId>,
    pub previous_operation_id: Option<OperationId>,
    pub request_available: bool,
    pub request_expires_at: Option<DateTime<Utc>>,
    pub request_removed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct HandlerAttemptRecord {
    pub id: Uuid,
    pub event_id: EventId,
    pub attempt_number: i32,
    pub status: String,
    pub input_state_version: i64,
    pub history_through_sequence: i64,
    pub error: Option<Value>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct OperationAttemptRecord {
    pub id: Uuid,
    pub operation_id: OperationId,
    pub attempt_number: i32,
    pub phase: String,
    pub status: String,
    pub http_status: Option<i32>,
    pub error: Option<Value>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct HandlerClaim {
    pub session_id: SessionId,
    pub event_id: EventId,
    pub attempt_id: Uuid,
    pub lease_token: Uuid,
    pub input_state_version: StateVersion,
}

impl From<&ClaimedEvent> for HandlerClaim {
    fn from(claimed: &ClaimedEvent) -> Self {
        Self {
            session_id: claimed.session.id,
            event_id: claimed.event.id,
            attempt_id: claimed.attempt_id,
            lease_token: claimed.lease_token,
            input_state_version: claimed.session.state_version,
        }
    }
}

#[derive(Debug)]
pub enum EnqueueResult {
    Inserted(EventRecord),
    Existing(EventRecord),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitFinish {
    Resolved,
    Expired,
    Cancelled,
}

#[derive(Debug)]
pub struct ProcessingRetry {
    pub session_id: SessionId,
    pub event_id: EventId,
    pub processing_revision: i64,
    pub retried_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct WaitResolution {
    pub event: EventRecord,
    pub newly_resolved: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiptStatus {
    Pending,
    Processing,
    Processed,
    Blocked,
}

#[derive(Debug)]
pub struct CallbackReceipt {
    pub id: Uuid,
    pub gateway_connection_id: GatewayConnectionId,
    pub gateway_event_id: Uuid,
    pub gateway_job_id: Uuid,
    pub payload: RawJson,
    pub operation_id: Option<OperationId>,
    pub status: ReceiptStatus,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct ClaimedCallbackReceipt {
    pub receipt: CallbackReceipt,
    pub lease_token: Uuid,
}
