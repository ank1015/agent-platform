use crate::state::{BasicCodexState, FailurePhase, FailureState};
use agent_contracts::{
    EventSequence, FailureSource, HandlerOutcome, OperationFailure, OperationId, OutcomeBuilder,
    SessionStatus,
};

pub(crate) struct FailureDetails {
    pub(crate) phase: FailurePhase,
    pub(crate) operation_id: Option<OperationId>,
    pub(crate) source: Option<FailureSource>,
    pub(crate) code: String,
    pub(crate) message: String,
    pub(crate) attempts: u32,
    pub(crate) event_sequence: EventSequence,
}

impl FailureDetails {
    pub(crate) fn operation(
        phase: FailurePhase,
        operation_id: OperationId,
        failure: OperationFailure,
        attempts: u32,
        event_sequence: EventSequence,
    ) -> Self {
        Self {
            phase,
            operation_id: Some(operation_id),
            source: Some(failure.source),
            code: failure.code,
            message: failure.message,
            attempts,
            event_sequence,
        }
    }

    pub(crate) fn malformed(
        phase: FailurePhase,
        operation_id: OperationId,
        message: impl Into<String>,
        attempts: u32,
        event_sequence: EventSequence,
    ) -> Self {
        Self {
            phase,
            operation_id: Some(operation_id),
            source: Some(FailureSource::Protocol),
            code: "malformed_provider_response".into(),
            message: message.into(),
            attempts,
            event_sequence,
        }
    }

    pub(crate) fn local(
        phase: FailurePhase,
        operation_id: Option<OperationId>,
        code: impl Into<String>,
        message: impl Into<String>,
        attempts: u32,
        event_sequence: EventSequence,
    ) -> Self {
        Self {
            phase,
            operation_id,
            source: None,
            code: code.into(),
            message: message.into(),
            attempts,
            event_sequence,
        }
    }
}

pub(crate) fn terminal(
    state: BasicCodexState,
    details: FailureDetails,
) -> HandlerOutcome<BasicCodexState> {
    let mut outcome = OutcomeBuilder::new(state);
    apply_terminal(&mut outcome, details);
    outcome.finish()
}

pub(crate) fn apply_terminal(
    outcome: &mut OutcomeBuilder<BasicCodexState>,
    details: FailureDetails,
) {
    outcome.state_mut().active_turn = None;
    outcome.state_mut().terminal_failure = Some(FailureState {
        phase: details.phase,
        operation_id: details.operation_id,
        source: details.source,
        code: details.code.clone(),
        message: details.message.clone(),
        attempts: details.attempts,
        occurred_at_sequence: details.event_sequence,
    });
    outcome.emit_progress(serde_json::json!({
        "type": "harness_failed",
        "phase": details.phase,
        "operation_id": details.operation_id,
        "code": details.code,
        "message": details.message,
        "attempts": details.attempts,
    }));
    outcome.set_status(SessionStatus::Failed);
}
