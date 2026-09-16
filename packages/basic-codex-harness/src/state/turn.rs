use super::tool::ToolPlanState;
use agent_contracts::{
    EventId, EventSequence, FailureSource, Message, OperationId, execution_core,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActiveTurn {
    pub(crate) id: Uuid,
    pub(crate) started_at_sequence: EventSequence,
    #[serde(default)]
    pub(crate) current_date: String,
    pub(crate) phase: TurnPhase,
    pub(crate) pending_steering: Vec<SteeringMessage>,
    pub(crate) retry: RetryState,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SteeringMessage {
    pub(crate) event_id: EventId,
    pub(crate) event_sequence: EventSequence,
    pub(crate) message: Message,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum TurnPhase {
    AwaitingLlm(AwaitingLlmState),
    ExecutingTools(ToolPlanState),
    Compacting(CompactionState),
    Cancelling,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AwaitingLlmState {
    pub(crate) operation_id: OperationId,
    pub(crate) purpose: LlmPurpose,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LlmPurpose {
    Turn,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompactionState {
    pub(crate) operation_id: OperationId,
    pub(crate) source_through_sequence: agent_contracts::HistorySequence,
    pub(crate) source_projection_sha256: String,
    pub(crate) attempt: u32,
    pub(crate) trigger: CompactionTrigger,
    pub(crate) initial_input: Option<Message>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompactionTrigger {
    PreTurn,
    MidTurn,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetryState {
    pub(crate) llm_attempts: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancellationState {
    pub(crate) reason: Option<String>,
    pub(crate) requested_at_sequence: EventSequence,
    pub(crate) turn_id: Option<Uuid>,
    pub(crate) targets: Vec<CancellationTarget>,
    pub(crate) processes: Vec<ProcessCleanup>,
    pub(crate) discovery: Option<ProcessDiscovery>,
    #[serde(default)]
    pub(crate) cleanup_failures: Vec<CleanupFailure>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CancellationTargetKind {
    Llm,
    Execution,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancellationTarget {
    pub(crate) operation_id: OperationId,
    pub(crate) kind: CancellationTargetKind,
    pub(crate) original_completed: bool,
    pub(crate) control: CancellationControl,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum CancellationControl {
    WithdrawalRequested {
        operation_id: OperationId,
    },
    LlmCancellationRequested {
        operation_id: OperationId,
        attempt: u32,
    },
    AwaitingCompletion,
    AwaitingDiscovery,
    Settled,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessCleanup {
    pub(crate) session_id: Option<i32>,
    pub(crate) handle: execution_core::ExecutionHandle,
    pub(crate) control: ProcessCleanupControl,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum ProcessCleanupControl {
    InterruptRequested {
        operation_id: OperationId,
    },
    TerminateRequested {
        operation_id: OperationId,
        attempt: u32,
    },
    Settled,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessDiscovery {
    pub(crate) operation_id: OperationId,
    pub(crate) attempt: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CleanupFailure {
    pub(crate) operation_id: OperationId,
    pub(crate) code: String,
    pub(crate) message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FailurePhase {
    Llm,
    Compaction,
    ToolPlanning,
    StopReason,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FailureState {
    pub(crate) phase: FailurePhase,
    pub(crate) operation_id: Option<OperationId>,
    pub(crate) source: Option<FailureSource>,
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) attempts: u32,
    pub(crate) occurred_at_sequence: EventSequence,
}
