mod context_window;
mod tool;
mod turn;

pub(crate) use context_window::{CompactionCheckpoint, ContextWindowState};
pub(crate) use tool::{
    ApplyPatchPhase, ApplyPatchState, FileToolState, ImageDetailChoice, OutputAccumulator,
    ParsedPatch, PatchChunk, PatchHunk, PatchMutation, PatchSummary, ShellCallPhase,
    ShellCallState, ToolCallKind, ToolCallProgress, ToolCallState, ToolCallStatus, ToolInput,
    ToolPlanState, ToolResultSnapshot, ToolScheduling, ToolSegment, TrackedExecution,
    ViewImageState,
};
pub(crate) use turn::{
    ActiveTurn, AwaitingLlmState, CancellationControl, CancellationState, CancellationTarget,
    CancellationTargetKind, CleanupFailure, CompactionState, CompactionTrigger, FailurePhase,
    FailureState, LlmPurpose, ProcessCleanup, ProcessCleanupControl, ProcessDiscovery, RetryState,
    SteeringMessage, TurnPhase,
};

use crate::STATE_SCHEMA_VERSION;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

/// JSON-serialized state persisted by the agent platform.
///
/// The type is public because it is the associated state of a public harness.
/// Its fields remain crate-private so state transitions stay owned by the
/// harness implementation.
#[derive(Debug, Serialize)]
pub struct BasicCodexState {
    pub(crate) schema_version: u32,
    pub(crate) active_turn: Option<ActiveTurn>,
    pub(crate) context_window: ContextWindowState,
    pub(crate) executions: Vec<TrackedExecution>,
    pub(crate) next_execution_session_id: i32,
    pub(crate) cancellation: Option<CancellationState>,
    pub(crate) terminal_failure: Option<FailureState>,
}

impl BasicCodexState {
    pub(crate) fn initial() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            active_turn: None,
            context_window: ContextWindowState::initial(),
            executions: Vec::new(),
            next_execution_session_id: 1000,
            cancellation: None,
            terminal_failure: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateWire {
    schema_version: u32,
    #[serde(default)]
    active_turn: Option<ActiveTurn>,
    context_window: ContextWindowState,
    #[serde(default)]
    executions: Vec<TrackedExecution>,
    #[serde(default = "initial_execution_session_id")]
    next_execution_session_id: i32,
    #[serde(default)]
    cancellation: Option<CancellationState>,
    #[serde(default)]
    terminal_failure: Option<FailureState>,
}

impl<'de> Deserialize<'de> for BasicCodexState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let state = StateWire::deserialize(deserializer)?;
        if state.schema_version != STATE_SCHEMA_VERSION {
            return Err(D::Error::custom(format!(
                "unsupported basic Codex state schema version {}; expected {}",
                state.schema_version, STATE_SCHEMA_VERSION
            )));
        }
        Ok(Self {
            schema_version: state.schema_version,
            active_turn: state.active_turn,
            context_window: state.context_window,
            executions: state.executions,
            next_execution_session_id: state.next_execution_session_id,
            cancellation: state.cancellation,
            terminal_failure: state.terminal_failure,
        })
    }
}

const fn initial_execution_session_id() -> i32 {
    1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_round_trips_without_losing_fields() {
        let state = BasicCodexState::initial();
        let encoded = serde_json::to_value(&state).unwrap();
        let decoded: BasicCodexState = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
        assert!(state.active_turn.is_none());
        assert!(state.context_window.model_history.is_empty());
        assert!(state.executions.is_empty());
        assert!(state.cancellation.is_none());
        assert!(state.terminal_failure.is_none());
    }

    #[test]
    fn active_state_from_before_prompt_context_defaults_the_turn_date() {
        let mut state = BasicCodexState::initial();
        state.active_turn = Some(ActiveTurn {
            id: uuid::Uuid::new_v4(),
            started_at_sequence: agent_contracts::EventSequence(1),
            current_date: "2026-09-15".into(),
            phase: TurnPhase::AwaitingLlm(AwaitingLlmState {
                operation_id: agent_contracts::OperationId::new(),
                purpose: LlmPurpose::Turn,
            }),
            pending_steering: Vec::new(),
            retry: RetryState::default(),
        });
        let mut encoded = serde_json::to_value(state).unwrap();
        encoded["active_turn"]
            .as_object_mut()
            .unwrap()
            .remove("current_date");
        let decoded: BasicCodexState = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.active_turn.unwrap().current_date, "");
    }
}
