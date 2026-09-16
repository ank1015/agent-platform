use crate::{
    BasicCodexConfig, BasicCodexHarness,
    state::{
        BasicCodexState, CancellationControl, CancellationState, CancellationTarget,
        CancellationTargetKind, CleanupFailure, ProcessCleanup, ProcessCleanupControl,
        ProcessDiscovery, ToolCallStatus, TurnPhase,
    },
};
use agent_contracts::{
    EventSequence, HandlerError, HandlerOutcome, HarnessContext, OperationId, OperationKind,
    OperationOutcome, OperationOutcomeStatus, OperationResult, OutcomeBuilder, SessionId,
    SessionStatus, execution, execution_core,
};
use std::collections::BTreeMap;

const TERMINATION_GRACE_MS: u64 = 2_000;
const MAX_CLEANUP_ATTEMPTS: u32 = 2;

pub(crate) fn begin(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    mut state: BasicCodexState,
    reason: Option<String>,
    sequence: EventSequence,
) -> Result<HandlerOutcome<BasicCodexState>, HandlerError> {
    if state.cancellation.is_some() {
        let mut outcome = OutcomeBuilder::new(state);
        set_lifecycle_status(&mut outcome, false);
        return Ok(outcome.finish());
    }

    let turn_id = state.active_turn.as_ref().map(|turn| turn.id);
    let mut targets = Vec::new();
    if let Some(active) = state.active_turn.as_ref() {
        match &active.phase {
            TurnPhase::AwaitingLlm(awaiting) => {
                targets.push((awaiting.operation_id, CancellationTargetKind::Llm))
            }
            TurnPhase::Compacting(compacting) => {
                targets.push((compacting.operation_id, CancellationTargetKind::Llm))
            }
            TurnPhase::ExecutingTools(plan) => {
                targets.extend(plan.calls.iter().filter_map(|call| match call.status {
                    ToolCallStatus::Submitted { operation_id, .. } => {
                        Some((operation_id, CancellationTargetKind::Execution))
                    }
                    _ => None,
                }));
            }
            TurnPhase::Cancelling => {
                return Err(error(
                    "active turn is cancelling without durable cancellation state",
                ));
            }
        }
    }

    if let Some(active) = state.active_turn.as_mut() {
        active.pending_steering.clear();
        active.phase = TurnPhase::Cancelling;
    }
    state.cancellation = Some(CancellationState {
        reason,
        requested_at_sequence: sequence,
        turn_id,
        targets: targets
            .into_iter()
            .map(|(operation_id, kind)| CancellationTarget {
                operation_id,
                kind,
                original_completed: false,
                control: CancellationControl::AwaitingCompletion,
            })
            .collect(),
        processes: Vec::new(),
        discovery: None,
        cleanup_failures: Vec::new(),
    });

    let tracked: Vec<_> = state
        .executions
        .iter()
        .map(|item| (Some(item.session_id), item.handle))
        .collect();
    let mut outcome = OutcomeBuilder::new(state);

    let target_count = outcome
        .state_mut()
        .cancellation
        .as_ref()
        .map_or(0, |cancellation| cancellation.targets.len());
    for index in 0..target_count {
        let target_id = outcome
            .state_mut()
            .cancellation
            .as_ref()
            .and_then(|state| state.targets.get(index))
            .map(|target| target.operation_id)
            .ok_or_else(|| error("cancellation target disappeared while staging withdrawal"))?;
        let control_id = outcome.withdraw_operation(target_id);
        outcome.state_mut().cancellation.as_mut().unwrap().targets[index].control =
            CancellationControl::WithdrawalRequested {
                operation_id: control_id,
            };
    }
    for (session_id, handle) in tracked {
        stage_interrupt(&mut outcome, harness, config, session_id, handle, sequence)?;
    }

    let process_count = outcome
        .state_mut()
        .cancellation
        .as_ref()
        .unwrap()
        .processes
        .len();
    outcome.emit_progress(serde_json::json!({
        "type": "cancellation_started",
        "targets": target_count,
        "processes": process_count,
    }));
    set_lifecycle_status(&mut outcome, true);
    Ok(outcome.finish())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_completion(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    state: BasicCodexState,
    operation_id: OperationId,
    operation_kind: OperationKind,
    outcome_status: OperationOutcomeStatus,
    context: &dyn HarnessContext,
    platform_session_id: SessionId,
    sequence: EventSequence,
) -> Result<HandlerOutcome<BasicCodexState>, HandlerError> {
    let record = context
        .operation(operation_id)
        .await
        .map_err(|failure| error(format!("could not read cancellation operation: {failure}")))?
        .ok_or_else(|| error("completed cancellation operation was not found"))?;
    if record.kind != operation_kind {
        return Err(error(
            "cancellation completion kind does not match its operation record",
        ));
    }
    let operation_outcome = record
        .outcome
        .ok_or_else(|| error("completed cancellation operation has no terminal outcome"))?;
    if operation_outcome.status() != outcome_status {
        return Err(error(
            "cancellation completion status does not match its operation record",
        ));
    }

    let mut outcome = OutcomeBuilder::new(state);
    match operation_kind {
        OperationKind::Withdraw => {
            handle_withdrawal(&mut outcome, operation_id, operation_outcome)?
        }
        OperationKind::LlmCancellation => {
            handle_llm_cancellation(&mut outcome, operation_id, operation_outcome)?
        }
        OperationKind::Llm => handle_original_completion(
            &mut outcome,
            harness,
            config,
            operation_id,
            CancellationTargetKind::Llm,
            operation_outcome,
            platform_session_id,
            sequence,
        )?,
        OperationKind::Execution => {
            if is_process_control(outcome.state_mut(), operation_id) {
                handle_process_control(
                    &mut outcome,
                    harness,
                    config,
                    operation_id,
                    operation_outcome,
                    sequence,
                )?;
            } else if is_discovery(outcome.state_mut(), operation_id) {
                handle_discovery(
                    &mut outcome,
                    harness,
                    config,
                    operation_id,
                    operation_outcome,
                    platform_session_id,
                    sequence,
                )?;
            } else {
                handle_original_completion(
                    &mut outcome,
                    harness,
                    config,
                    operation_id,
                    CancellationTargetKind::Execution,
                    operation_outcome,
                    platform_session_id,
                    sequence,
                )?;
            }
        }
    }
    set_lifecycle_status(&mut outcome, true);
    Ok(outcome.finish())
}

fn handle_withdrawal(
    outcome: &mut OutcomeBuilder<BasicCodexState>,
    operation_id: OperationId,
    operation_outcome: OperationOutcome,
) -> Result<(), HandlerError> {
    let index = target_for_control(outcome.state_mut(), operation_id)?;
    if matches!(operation_outcome, OperationOutcome::Succeeded(ref result) if !matches!(result, OperationResult::Withdraw { .. }))
    {
        return Err(error(
            "successful withdrawal contains the wrong result kind",
        ));
    }
    let withdrawn = matches!(
        operation_outcome,
        OperationOutcome::Succeeded(OperationResult::Withdraw { withdrawn: true })
    );
    if withdrawn {
        let target = cancellation_mut(outcome.state_mut())
            .targets
            .get_mut(index)
            .ok_or_else(|| error("withdrawal target disappeared"))?;
        target.original_completed = true;
        target.control = CancellationControl::Settled;
        return Ok(());
    }

    let (target_id, target_kind) = {
        let target = cancellation_mut(outcome.state_mut())
            .targets
            .get(index)
            .ok_or_else(|| error("withdrawal target disappeared"))?;
        (target.operation_id, target.kind)
    };
    match target_kind {
        CancellationTargetKind::Llm => {
            let control_id = outcome.request_llm_cancellation(target_id);
            cancellation_mut(outcome.state_mut()).targets[index].control =
                CancellationControl::LlmCancellationRequested {
                    operation_id: control_id,
                    attempt: 1,
                };
        }
        CancellationTargetKind::Execution => {
            let target = &mut cancellation_mut(outcome.state_mut()).targets[index];
            target.control = if target.original_completed {
                CancellationControl::Settled
            } else {
                CancellationControl::AwaitingCompletion
            };
        }
    }
    Ok(())
}

fn handle_llm_cancellation(
    outcome: &mut OutcomeBuilder<BasicCodexState>,
    operation_id: OperationId,
    operation_outcome: OperationOutcome,
) -> Result<(), HandlerError> {
    let index = target_for_control(outcome.state_mut(), operation_id)?;
    let (target_id, attempt) = {
        let target = &cancellation_mut(outcome.state_mut()).targets[index];
        let CancellationControl::LlmCancellationRequested { attempt, .. } = target.control else {
            return Err(error(
                "LLM cancellation target has an invalid control state",
            ));
        };
        (target.operation_id, attempt)
    };
    match operation_outcome {
        OperationOutcome::Succeeded(OperationResult::LlmCancellation { .. })
        | OperationOutcome::Cancelled => {
            cancellation_mut(outcome.state_mut()).targets[index].control =
                CancellationControl::Settled;
        }
        OperationOutcome::Failed(failure) | OperationOutcome::Unknown(failure)
            if attempt < MAX_CLEANUP_ATTEMPTS =>
        {
            let next = outcome.request_llm_cancellation(target_id);
            cancellation_mut(outcome.state_mut()).targets[index].control =
                CancellationControl::LlmCancellationRequested {
                    operation_id: next,
                    attempt: attempt + 1,
                };
            record_cleanup_failure(
                outcome.state_mut(),
                operation_id,
                failure.code,
                failure.message,
            );
        }
        OperationOutcome::Failed(failure) | OperationOutcome::Unknown(failure) => {
            record_cleanup_failure(
                outcome.state_mut(),
                operation_id,
                failure.code,
                failure.message,
            );
            cancellation_mut(outcome.state_mut()).targets[index].control =
                CancellationControl::Settled;
        }
        OperationOutcome::Succeeded(_) => {
            return Err(error(
                "successful LLM cancellation contains the wrong result kind",
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_original_completion(
    outcome: &mut OutcomeBuilder<BasicCodexState>,
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    operation_id: OperationId,
    expected_kind: CancellationTargetKind,
    operation_outcome: OperationOutcome,
    platform_session_id: SessionId,
    sequence: EventSequence,
) -> Result<(), HandlerError> {
    match (&expected_kind, &operation_outcome) {
        (CancellationTargetKind::Llm, OperationOutcome::Succeeded(OperationResult::Llm(_)))
        | (
            CancellationTargetKind::Execution,
            OperationOutcome::Succeeded(OperationResult::Execution(_)),
        )
        | (
            _,
            OperationOutcome::Failed(_)
            | OperationOutcome::Unknown(_)
            | OperationOutcome::Cancelled,
        ) => {}
        _ => {
            return Err(error(
                "successful late operation contains the wrong result kind",
            ));
        }
    }
    let index = cancellation_mut(outcome.state_mut())
        .targets
        .iter()
        .position(|target| target.operation_id == operation_id)
        .ok_or_else(|| error("late operation completion is not a cancellation target"))?;
    if cancellation_mut(outcome.state_mut()).targets[index].kind != expected_kind {
        return Err(error("late operation completion has the wrong target kind"));
    }

    let active_processes = executions_from_outcome(&operation_outcome);
    let uncertain_execution = expected_kind == CancellationTargetKind::Execution
        && matches!(operation_outcome, OperationOutcome::Unknown(_))
        && active_processes.is_empty();
    for execution in active_processes {
        if execution.state != execution_core::ExecutionState::Finished {
            stage_interrupt(outcome, harness, config, None, execution.handle, sequence)?;
        }
    }

    let target = &mut cancellation_mut(outcome.state_mut()).targets[index];
    target.original_completed = true;
    if uncertain_execution {
        target.control = CancellationControl::AwaitingDiscovery;
        if cancellation_mut(outcome.state_mut()).discovery.is_none() {
            stage_discovery(outcome, harness, config, platform_session_id, sequence, 1)?;
        }
    } else if matches!(target.control, CancellationControl::AwaitingCompletion) {
        target.control = CancellationControl::Settled;
    }
    Ok(())
}

fn handle_process_control(
    outcome: &mut OutcomeBuilder<BasicCodexState>,
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    operation_id: OperationId,
    operation_outcome: OperationOutcome,
    sequence: EventSequence,
) -> Result<(), HandlerError> {
    if matches!(operation_outcome, OperationOutcome::Succeeded(ref result) if !matches!(result, OperationResult::Execution(_)))
    {
        return Err(error(
            "successful process cleanup contains the wrong result kind",
        ));
    }
    let index = process_for_control(outcome.state_mut(), operation_id)?;
    let (session_id, handle, is_interrupt, attempt) = {
        let process = &cancellation_mut(outcome.state_mut()).processes[index];
        match process.control {
            ProcessCleanupControl::InterruptRequested { .. } => {
                (process.session_id, process.handle, true, 1)
            }
            ProcessCleanupControl::TerminateRequested { attempt, .. } => {
                (process.session_id, process.handle, false, attempt)
            }
            ProcessCleanupControl::Settled => {
                return Ok(());
            }
        }
    };

    if is_interrupt {
        let finished = executions_from_outcome(&operation_outcome)
            .iter()
            .any(|execution| execution.state == execution_core::ExecutionState::Finished);
        if finished {
            settle_process(outcome.state_mut(), index, session_id);
        } else {
            stage_terminate(outcome, harness, config, index, handle, 1)?;
        }
        return Ok(());
    }

    match operation_outcome {
        OperationOutcome::Succeeded(_) | OperationOutcome::Cancelled => {
            settle_process(outcome.state_mut(), index, session_id);
        }
        OperationOutcome::Failed(failure) | OperationOutcome::Unknown(failure)
            if attempt < MAX_CLEANUP_ATTEMPTS =>
        {
            record_cleanup_failure(
                outcome.state_mut(),
                operation_id,
                failure.code,
                failure.message,
            );
            stage_terminate(outcome, harness, config, index, handle, attempt + 1)?;
        }
        OperationOutcome::Failed(failure) | OperationOutcome::Unknown(failure) => {
            record_cleanup_failure(
                outcome.state_mut(),
                operation_id,
                failure.code,
                failure.message,
            );
            settle_process(outcome.state_mut(), index, session_id);
            outcome.emit_progress(serde_json::json!({
                "type": "cancellation_cleanup_failed",
                "handle": handle,
                "attempts": attempt,
                "requested_at_sequence": sequence,
            }));
        }
    }
    Ok(())
}

fn handle_discovery(
    outcome: &mut OutcomeBuilder<BasicCodexState>,
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    operation_id: OperationId,
    operation_outcome: OperationOutcome,
    platform_session_id: SessionId,
    sequence: EventSequence,
) -> Result<(), HandlerError> {
    if matches!(operation_outcome, OperationOutcome::Succeeded(ref result) if !matches!(result, OperationResult::Execution(_)))
    {
        return Err(error(
            "successful process discovery contains the wrong result kind",
        ));
    }
    let attempt = cancellation_mut(outcome.state_mut())
        .discovery
        .as_ref()
        .ok_or_else(|| error("process discovery state disappeared"))?
        .attempt;
    let processes = executions_from_outcome(&operation_outcome);
    let succeeded = matches!(operation_outcome, OperationOutcome::Succeeded(_));
    if !succeeded && attempt < MAX_CLEANUP_ATTEMPTS {
        if let OperationOutcome::Failed(failure) | OperationOutcome::Unknown(failure) =
            operation_outcome
        {
            record_cleanup_failure(
                outcome.state_mut(),
                operation_id,
                failure.code,
                failure.message,
            );
        }
        stage_discovery(
            outcome,
            harness,
            config,
            platform_session_id,
            sequence,
            attempt + 1,
        )?;
        return Ok(());
    }

    cancellation_mut(outcome.state_mut()).discovery = None;
    for target in &mut cancellation_mut(outcome.state_mut()).targets {
        if matches!(target.control, CancellationControl::AwaitingDiscovery) {
            target.control = CancellationControl::Settled;
        }
    }
    for execution in processes {
        if execution.state != execution_core::ExecutionState::Finished {
            stage_interrupt(outcome, harness, config, None, execution.handle, sequence)?;
        }
    }
    Ok(())
}

fn stage_interrupt(
    outcome: &mut OutcomeBuilder<BasicCodexState>,
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    session_id: Option<i32>,
    handle: execution_core::ExecutionHandle,
    sequence: EventSequence,
) -> Result<(), HandlerError> {
    if cancellation_mut(outcome.state_mut())
        .processes
        .iter()
        .any(|process| process.handle == handle)
    {
        return Ok(());
    }
    let operation_id = outcome
        .request_execution_for_generation(
            harness.execution_connection_id().clone(),
            config.machine_id,
            handle.generation_id,
            execution::Payload::Single(execution::Operation::Interrupt {
                handle,
                operation_id: format!("cancel-{}-{}", sequence.0, handle.id),
            }),
        )
        .map_err(|failure| error(format!("could not stage process interrupt: {failure}")))?;
    cancellation_mut(outcome.state_mut())
        .processes
        .push(ProcessCleanup {
            session_id,
            handle,
            control: ProcessCleanupControl::InterruptRequested { operation_id },
        });
    Ok(())
}

fn stage_terminate(
    outcome: &mut OutcomeBuilder<BasicCodexState>,
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    index: usize,
    handle: execution_core::ExecutionHandle,
    attempt: u32,
) -> Result<(), HandlerError> {
    let operation_id = outcome
        .request_execution_for_generation(
            harness.execution_connection_id().clone(),
            config.machine_id,
            handle.generation_id,
            execution::Payload::Single(execution::Operation::Terminate {
                handle,
                grace_period_ms: Some(TERMINATION_GRACE_MS),
            }),
        )
        .map_err(|failure| error(format!("could not stage process termination: {failure}")))?;
    cancellation_mut(outcome.state_mut()).processes[index].control =
        ProcessCleanupControl::TerminateRequested {
            operation_id,
            attempt,
        };
    Ok(())
}

fn stage_discovery(
    outcome: &mut OutcomeBuilder<BasicCodexState>,
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    platform_session_id: SessionId,
    _sequence: EventSequence,
    attempt: u32,
) -> Result<(), HandlerError> {
    let mut labels = BTreeMap::new();
    labels.insert("agent_session_id".into(), platform_session_id.to_string());
    if let Some(turn_id) = cancellation_mut(outcome.state_mut()).turn_id {
        labels.insert("agent_turn_id".into(), turn_id.to_string());
    }
    let operation_id = outcome
        .request_execution(
            harness.execution_connection_id().clone(),
            config.machine_id,
            execution::Payload::Single(execution::Operation::List(execution::ListParams {
                state: execution::StateFilter::Active,
                labels,
                limit: 50,
                page_cursor: None,
            })),
        )
        .map_err(|failure| error(format!("could not stage process discovery: {failure}")))?;
    cancellation_mut(outcome.state_mut()).discovery = Some(ProcessDiscovery {
        operation_id,
        attempt,
    });
    Ok(())
}

fn executions_from_outcome(operation_outcome: &OperationOutcome) -> Vec<execution_core::Execution> {
    let response = match operation_outcome {
        OperationOutcome::Succeeded(OperationResult::Execution(response)) => Some(response),
        OperationOutcome::Failed(failure) | OperationOutcome::Unknown(failure) => {
            failure.execution_response.as_ref()
        }
        _ => None,
    };
    let Some(response) = response else {
        return Vec::new();
    };
    let execution::Outcome::Ok { result } = &response.outcome else {
        return Vec::new();
    };
    if let Some(execution) = result.get("execution")
        && let Ok(execution) = serde_json::from_value(execution.clone())
    {
        return vec![execution];
    }
    if let Some(executions) = result.get("executions")
        && let Ok(executions) = serde_json::from_value(executions.clone())
    {
        return executions;
    }
    serde_json::from_value(result.clone())
        .map(|execution| vec![execution])
        .unwrap_or_default()
}

fn target_for_control(
    state: &mut BasicCodexState,
    operation_id: OperationId,
) -> Result<usize, HandlerError> {
    cancellation_mut(state)
        .targets
        .iter()
        .position(|target| match target.control {
            CancellationControl::WithdrawalRequested {
                operation_id: control,
            }
            | CancellationControl::LlmCancellationRequested {
                operation_id: control,
                ..
            } => control == operation_id,
            _ => false,
        })
        .ok_or_else(|| error("completion does not match a cancellation control operation"))
}

fn process_for_control(
    state: &mut BasicCodexState,
    operation_id: OperationId,
) -> Result<usize, HandlerError> {
    cancellation_mut(state)
        .processes
        .iter()
        .position(|process| match process.control {
            ProcessCleanupControl::InterruptRequested {
                operation_id: control,
            }
            | ProcessCleanupControl::TerminateRequested {
                operation_id: control,
                ..
            } => control == operation_id,
            ProcessCleanupControl::Settled => false,
        })
        .ok_or_else(|| error("completion does not match a process cleanup operation"))
}

fn is_process_control(state: &mut BasicCodexState, operation_id: OperationId) -> bool {
    state.cancellation.as_ref().is_some_and(|cancellation| {
        cancellation
            .processes
            .iter()
            .any(|process| match process.control {
                ProcessCleanupControl::InterruptRequested {
                    operation_id: control,
                }
                | ProcessCleanupControl::TerminateRequested {
                    operation_id: control,
                    ..
                } => control == operation_id,
                ProcessCleanupControl::Settled => false,
            })
    })
}

fn is_discovery(state: &mut BasicCodexState, operation_id: OperationId) -> bool {
    state
        .cancellation
        .as_ref()
        .and_then(|cancellation| cancellation.discovery.as_ref())
        .is_some_and(|discovery| discovery.operation_id == operation_id)
}

fn settle_process(state: &mut BasicCodexState, index: usize, session_id: Option<i32>) {
    if let Some(process) = cancellation_mut(state).processes.get_mut(index) {
        process.control = ProcessCleanupControl::Settled;
    }
    if let Some(session_id) = session_id {
        state
            .executions
            .retain(|execution| execution.session_id != session_id);
    }
}

fn record_cleanup_failure(
    state: &mut BasicCodexState,
    operation_id: OperationId,
    code: String,
    message: String,
) {
    cancellation_mut(state)
        .cleanup_failures
        .push(CleanupFailure {
            operation_id,
            code,
            message,
        });
}

fn set_lifecycle_status(outcome: &mut OutcomeBuilder<BasicCodexState>, emit_completion: bool) {
    let complete = cancellation_complete(outcome.state_mut());
    if complete {
        let was_active = outcome.state_mut().active_turn.take().is_some();
        outcome.state_mut().executions.clear();
        if emit_completion && was_active {
            outcome.emit_progress(serde_json::json!({"type":"cancellation_completed"}));
        }
        outcome.set_status(SessionStatus::Cancelled);
    } else {
        outcome.set_status(SessionStatus::Cancelling);
    }
}

fn cancellation_complete(state: &BasicCodexState) -> bool {
    state.cancellation.as_ref().is_some_and(|cancellation| {
        cancellation.discovery.is_none()
            && cancellation.targets.iter().all(|target| {
                matches!(target.control, CancellationControl::Settled)
                    || (target.original_completed
                        && matches!(target.control, CancellationControl::AwaitingCompletion))
            })
            && cancellation
                .processes
                .iter()
                .all(|process| matches!(process.control, ProcessCleanupControl::Settled))
    })
}

fn cancellation_mut(state: &mut BasicCodexState) -> &mut CancellationState {
    state
        .cancellation
        .as_mut()
        .expect("cancellation handler requires cancellation state")
}

fn error(message: impl Into<String>) -> HandlerError {
    HandlerError(message.into())
}
