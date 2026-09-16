use crate::{
    BasicCodexConfig, BasicCodexHarness, cancellation, compaction, failure, llm,
    state::{
        ActiveTurn, AwaitingLlmState, BasicCodexState, CompactionCheckpoint, CompactionState,
        CompactionTrigger, FailurePhase, LlmPurpose, RetryState, SteeringMessage, TurnPhase,
    },
    tool_execution, tool_plan,
};
use agent_contracts::{
    EventId, EventKind, HandlerError, HandlerOutcome, HarnessContext, HistorySequence, Message,
    OperationKind, OperationOutcome, OperationResult, OutcomeBuilder, SessionEvent, SessionStatus,
    StopReason,
};
use uuid::Uuid;

pub(crate) async fn handle(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    mut state: BasicCodexState,
    event: SessionEvent,
    context: &dyn HarnessContext,
) -> Result<HandlerOutcome<BasicCodexState>, HandlerError> {
    let event_id = event.id;
    let sequence = event.sequence;
    let platform_session_id = event.session_id;
    let event_timestamp_ms = event.created_at.timestamp_millis();
    let event_date = event.created_at.date_naive().to_string();
    if let Some(active) = state.active_turn.as_mut()
        && active.current_date.is_empty()
    {
        active.current_date.clone_from(&event_date);
    }
    if state.terminal_failure.is_some() {
        let mut outcome = OutcomeBuilder::new(state);
        if let EventKind::UserMessage { message } = event.kind {
            if !matches!(message, Message::User { .. }) {
                return Err(error("user_message event did not contain a user message"));
            }
            outcome.append_message(message);
        }
        outcome.set_status(SessionStatus::Failed);
        return Ok(outcome.finish());
    }
    if state.cancellation.is_some() {
        return match event.kind {
            EventKind::OperationCompleted {
                operation_id,
                operation_kind,
                outcome_status,
            } => {
                cancellation::handle_completion(
                    harness,
                    config,
                    state,
                    operation_id,
                    operation_kind,
                    outcome_status,
                    context,
                    platform_session_id,
                    sequence,
                )
                .await
            }
            EventKind::UserMessage { message } => {
                if !matches!(message, Message::User { .. }) {
                    return Err(error("user_message event did not contain a user message"));
                }
                let status = if state.active_turn.is_some() {
                    SessionStatus::Cancelling
                } else {
                    SessionStatus::Cancelled
                };
                let mut outcome = OutcomeBuilder::new(state);
                outcome.append_message(message);
                outcome.set_status(status);
                Ok(outcome.finish())
            }
            EventKind::CancellationRequested { .. } | EventKind::WaitResumed { .. } => {
                let status = if state.active_turn.is_some() {
                    SessionStatus::Cancelling
                } else {
                    SessionStatus::Cancelled
                };
                let mut outcome = OutcomeBuilder::new(state);
                outcome.set_status(status);
                Ok(outcome.finish())
            }
        };
    }
    match event.kind {
        EventKind::UserMessage { message } => handle_user_message(
            harness,
            config,
            state,
            message,
            UserMessageContext {
                event_id,
                sequence,
                history_through_sequence: context.history_through_sequence(),
                platform_session_id,
                current_date: event_date,
            },
        ),
        EventKind::OperationCompleted {
            operation_id,
            operation_kind,
            outcome_status,
        } => {
            if operation_kind == OperationKind::Execution {
                return handle_execution_completion(
                    harness,
                    config,
                    state,
                    operation_id,
                    outcome_status,
                    context,
                    (platform_session_id, event_timestamp_ms),
                )
                .await;
            }
            if operation_kind != OperationKind::Llm {
                return Err(error("received an unsupported operation completion"));
            }
            let active = state
                .active_turn
                .as_ref()
                .ok_or_else(|| error("received an LLM completion without an active turn"))?;
            let is_compaction = matches!(&active.phase, TurnPhase::Compacting(_));
            let expected_operation = match &active.phase {
                TurnPhase::AwaitingLlm(awaiting) => {
                    if awaiting.purpose != LlmPurpose::Turn {
                        return Err(error("ordinary LLM phase has an invalid purpose"));
                    }
                    awaiting.operation_id
                }
                TurnPhase::Compacting(compacting) => compacting.operation_id,
                _ => {
                    return Err(error(
                        "received an LLM completion while the turn was not awaiting LLM work",
                    ));
                }
            };
            if expected_operation != operation_id {
                return Err(error(
                    "LLM completion does not match the operation awaited by the active turn",
                ));
            }
            let record = context
                .operation(operation_id)
                .await
                .map_err(|failure| {
                    error(format!("could not read completed LLM operation: {failure}"))
                })?
                .ok_or_else(|| error("completed LLM operation was not found"))?;
            if record.kind != OperationKind::Llm {
                return Err(error("completed operation record is not an LLM operation"));
            }
            let outcome = record
                .outcome
                .ok_or_else(|| error("completed LLM operation has no terminal outcome"))?;
            if outcome.status() != outcome_status {
                return Err(error(
                    "completion event status does not match the stored operation outcome",
                ));
            }
            if is_compaction {
                handle_compaction_outcome(
                    harness,
                    config,
                    state,
                    operation_id,
                    outcome,
                    platform_session_id,
                    sequence,
                )
            } else {
                handle_llm_outcome(
                    harness,
                    config,
                    state,
                    operation_id,
                    outcome,
                    (
                        platform_session_id,
                        event_timestamp_ms,
                        context.history_through_sequence(),
                        sequence,
                    ),
                )
            }
        }
        EventKind::CancellationRequested { reason } => {
            cancellation::begin(harness, config, state, reason, sequence)
        }
        EventKind::WaitResumed { .. } => Err(error(
            "basic Codex does not create waits in the current implementation",
        )),
    }
}

struct UserMessageContext {
    event_id: EventId,
    sequence: agent_contracts::EventSequence,
    history_through_sequence: HistorySequence,
    platform_session_id: agent_contracts::SessionId,
    current_date: String,
}

fn handle_user_message(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    mut state: BasicCodexState,
    message: Message,
    event_context: UserMessageContext,
) -> Result<HandlerOutcome<BasicCodexState>, HandlerError> {
    let UserMessageContext {
        event_id,
        sequence,
        history_through_sequence,
        platform_session_id,
        current_date,
    } = event_context;
    if !matches!(message, Message::User { .. }) {
        return Err(error("user_message event did not contain a user message"));
    }

    if let Some(active) = state.active_turn.as_mut() {
        active.pending_steering.push(SteeringMessage {
            event_id,
            event_sequence: sequence,
            message: message.clone(),
        });
        let mut outcome = OutcomeBuilder::new(state);
        outcome.append_message(message);
        return Ok(outcome.finish());
    }

    let mut outcome = OutcomeBuilder::new(state);
    outcome.append_message(message.clone());
    let active_turn_id = Uuid::new_v4();
    if needs_compaction(config, outcome.state_mut()) {
        let source_projection_sha256 =
            compaction::projection_sha256(&outcome.state_mut().context_window.model_history)
                .map_err(error)?;
        let request = llm::compaction_request(
            config,
            crate::prompt::PromptContext {
                session_id: platform_session_id,
                current_date: &current_date,
            },
            outcome.state_mut().context_window.model_history.clone(),
        )
        .map_err(error)?;
        let operation_id = outcome.request_llm(harness.llm_connection_id().clone(), request);
        outcome.state_mut().active_turn = Some(ActiveTurn {
            id: active_turn_id,
            started_at_sequence: sequence,
            current_date,
            phase: TurnPhase::Compacting(CompactionState {
                operation_id,
                source_through_sequence: history_through_sequence,
                source_projection_sha256,
                attempt: 1,
                trigger: CompactionTrigger::PreTurn,
                initial_input: Some(message),
            }),
            pending_steering: Vec::new(),
            retry: RetryState::default(),
        });
        outcome.emit_progress(serde_json::json!({
            "type": "context_compaction_started",
            "trigger": "pre_turn",
        }));
        outcome.set_status(SessionStatus::Running);
        return Ok(outcome.finish());
    }

    outcome
        .state_mut()
        .context_window
        .model_history
        .push(message);
    add_latest_projection_estimate(config, outcome.state_mut());
    let request = llm::fresh_request(
        config,
        crate::prompt::PromptContext {
            session_id: platform_session_id,
            current_date: &current_date,
        },
        outcome.state_mut().context_window.model_history.clone(),
    );
    let operation_id = outcome.request_llm(harness.llm_connection_id().clone(), request);
    outcome.state_mut().active_turn = Some(ActiveTurn {
        id: active_turn_id,
        started_at_sequence: sequence,
        current_date,
        phase: TurnPhase::AwaitingLlm(AwaitingLlmState {
            operation_id,
            purpose: LlmPurpose::Turn,
        }),
        pending_steering: Vec::new(),
        retry: RetryState::default(),
    });
    outcome.set_status(SessionStatus::Running);
    Ok(outcome.finish())
}

fn handle_llm_outcome(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    mut state: BasicCodexState,
    operation_id: agent_contracts::OperationId,
    outcome: OperationOutcome,
    event_context: (
        agent_contracts::SessionId,
        i64,
        HistorySequence,
        agent_contracts::EventSequence,
    ),
) -> Result<HandlerOutcome<BasicCodexState>, HandlerError> {
    let (platform_session_id, event_timestamp_ms, history_through_sequence, event_sequence) =
        event_context;
    match outcome {
        OperationOutcome::Succeeded(OperationResult::Llm(response)) => {
            let response = *response;
            if let Err(message) = llm::response_provider(config, &response) {
                let attempts = state
                    .active_turn
                    .as_ref()
                    .map_or(1, |turn| turn.retry.llm_attempts.saturating_add(1));
                return Ok(failure::terminal(
                    state,
                    failure::FailureDetails::malformed(
                        FailurePhase::Llm,
                        operation_id,
                        message,
                        attempts,
                        event_sequence,
                    ),
                ));
            }
            let stop_reason = response.stop_reason;
            let plan = tool_plan::parse(operation_id, &response.message, stop_reason);
            if let Some(tokens) = llm::estimated_context_tokens(response.usage.as_ref()) {
                state.context_window.estimated_tokens = Some(tokens);
            }
            state
                .context_window
                .model_history
                .push(response.message.clone());

            let mut result = OutcomeBuilder::new(state);
            result.append_message(response.message);

            let plan = match plan {
                Ok(plan) => plan,
                Err(failure) => {
                    let message = failure.to_string();
                    failure::apply_terminal(
                        &mut result,
                        failure::FailureDetails::local(
                            FailurePhase::ToolPlanning,
                            Some(operation_id),
                            "tool_plan_rejected",
                            message,
                            1,
                            event_sequence,
                        ),
                    );
                    return Ok(result.finish());
                }
            };
            if let Some(plan) = plan {
                if plan.is_complete() {
                    let messages = plan
                        .ordered_result_messages()
                        .map_err(|failure| error(failure.to_string()))?;
                    let source_through_sequence = advance_history_sequence(
                        history_through_sequence,
                        1_usize.saturating_add(messages.len()),
                    )?;
                    extend_projection(result.state_mut(), messages.iter().cloned());
                    for message in messages {
                        result.append_message(message);
                    }
                    stage_after_boundary(
                        harness,
                        config,
                        &mut result,
                        source_through_sequence,
                        platform_session_id,
                    )?;
                } else {
                    let active =
                        result.state_mut().active_turn.as_mut().ok_or_else(|| {
                            error("active turn disappeared while saving a tool plan")
                        })?;
                    active.phase = TurnPhase::ExecutingTools(plan);
                    tool_execution::schedule_ready(
                        harness,
                        config,
                        &mut result,
                        platform_session_id,
                        event_timestamp_ms,
                    )?;
                    if finish_completed_tool_plan(
                        harness,
                        config,
                        &mut result,
                        history_through_sequence,
                        platform_session_id,
                    )? {
                        return Ok(result.finish());
                    }
                    result.set_status(SessionStatus::Running);
                }
                return Ok(result.finish());
            }

            match stop_reason {
                StopReason::Stop | StopReason::Refusal => {
                    let has_steering = result
                        .state_mut()
                        .active_turn
                        .as_ref()
                        .is_some_and(|turn| !turn.pending_steering.is_empty());
                    if has_steering {
                        stage_after_boundary(
                            harness,
                            config,
                            &mut result,
                            advance_history_sequence(history_through_sequence, 1)?,
                            platform_session_id,
                        )?;
                    } else {
                        result.state_mut().active_turn = None;
                        result.set_status(SessionStatus::Idle);
                    }
                }
                StopReason::PauseTurn => {
                    stage_after_boundary(
                        harness,
                        config,
                        &mut result,
                        advance_history_sequence(history_through_sequence, 1)?,
                        platform_session_id,
                    )?;
                }
                StopReason::Length | StopReason::ContentFilter => {
                    failure::apply_terminal(
                        &mut result,
                        failure::FailureDetails::local(
                            FailurePhase::StopReason,
                            Some(operation_id),
                            match stop_reason {
                                StopReason::Length => "output_length",
                                StopReason::ContentFilter => "content_filter",
                                _ => unreachable!(),
                            },
                            format!("LLM turn ended with terminal stop reason {stop_reason:?}"),
                            1,
                            event_sequence,
                        ),
                    );
                }
                StopReason::ToolUse => unreachable!("tool_use without a plan is rejected above"),
            }
            Ok(result.finish())
        }
        OperationOutcome::Succeeded(_) => {
            Err(error("successful LLM operation contains a non-LLM result"))
        }
        OperationOutcome::Unknown(operation_failure) => {
            let retry_count = state
                .active_turn
                .as_ref()
                .ok_or_else(|| error("unknown LLM outcome has no active turn"))?
                .retry
                .llm_attempts;
            if retry_count < 1 {
                let current_date = active_current_date(&state)?.to_owned();
                let request = llm::fresh_request(
                    config,
                    crate::prompt::PromptContext {
                        session_id: platform_session_id,
                        current_date: &current_date,
                    },
                    state.context_window.model_history.clone(),
                );
                let mut result = OutcomeBuilder::new(state);
                let next_operation =
                    result.request_llm(harness.llm_connection_id().clone(), request);
                let active = result
                    .state_mut()
                    .active_turn
                    .as_mut()
                    .ok_or_else(|| error("active turn disappeared while retrying LLM work"))?;
                active.retry.llm_attempts += 1;
                active.phase = TurnPhase::AwaitingLlm(AwaitingLlmState {
                    operation_id: next_operation,
                    purpose: LlmPurpose::Turn,
                });
                let attempt = active.retry.llm_attempts + 1;
                result.emit_progress(serde_json::json!({
                    "type": "llm_retried",
                    "attempt": attempt,
                    "previous_operation_id": operation_id,
                }));
                result.set_status(SessionStatus::Running);
                Ok(result.finish())
            } else {
                Ok(failure::terminal(
                    state,
                    failure::FailureDetails::operation(
                        FailurePhase::Llm,
                        operation_id,
                        operation_failure,
                        retry_count.saturating_add(1),
                        event_sequence,
                    ),
                ))
            }
        }
        OperationOutcome::Failed(operation_failure) => {
            let attempts = state
                .active_turn
                .as_ref()
                .map_or(1, |turn| turn.retry.llm_attempts.saturating_add(1));
            Ok(failure::terminal(
                state,
                failure::FailureDetails::operation(
                    FailurePhase::Llm,
                    operation_id,
                    operation_failure,
                    attempts,
                    event_sequence,
                ),
            ))
        }
        OperationOutcome::Cancelled => Ok(failure::terminal(
            state,
            failure::FailureDetails::local(
                FailurePhase::Llm,
                Some(operation_id),
                "unexpected_llm_cancellation",
                "LLM operation was cancelled without a session cancellation request",
                1,
                event_sequence,
            ),
        )),
    }
}

fn handle_compaction_outcome(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    mut state: BasicCodexState,
    operation_id: agent_contracts::OperationId,
    outcome: OperationOutcome,
    platform_session_id: agent_contracts::SessionId,
    event_sequence: agent_contracts::EventSequence,
) -> Result<HandlerOutcome<BasicCodexState>, HandlerError> {
    let (source_through_sequence, source_projection_sha256, attempt, trigger) = {
        let active = state
            .active_turn
            .as_ref()
            .ok_or_else(|| error("compaction completion has no active turn"))?;
        let TurnPhase::Compacting(compacting) = &active.phase else {
            return Err(error("compaction completion has no compaction state"));
        };
        (
            compacting.source_through_sequence,
            compacting.source_projection_sha256.clone(),
            compacting.attempt,
            compacting.trigger,
        )
    };

    match outcome {
        OperationOutcome::Succeeded(OperationResult::Llm(response)) => {
            let actual_projection_sha256 =
                compaction::projection_sha256(&state.context_window.model_history)
                    .map_err(error)?;
            if actual_projection_sha256 != source_projection_sha256 {
                return Err(error(
                    "model projection changed while provider compaction was in flight",
                ));
            }
            let validated = match compaction::validate_response(config, *response) {
                Ok(validated) => validated,
                Err(message) => {
                    return Ok(failure::terminal(
                        state,
                        failure::FailureDetails::malformed(
                            FailurePhase::Compaction,
                            operation_id,
                            message,
                            attempt,
                            event_sequence,
                        ),
                    ));
                }
            };
            let retained = compaction::retained_user_messages(&state.context_window.model_history);
            let provider_item =
                compaction::provider_message(config.provider, validated.native_item.as_ref())
                    .map_err(error)?;
            let next_generation = state
                .context_window
                .generation
                .checked_add(1)
                .ok_or_else(|| error("context generation overflowed"))?;
            let checkpoint_message = compaction::checkpoint_message(
                validated.provider,
                &validated.response_id,
                validated.native_item.as_ref(),
                source_through_sequence,
                &source_projection_sha256,
                retained.len(),
                next_generation,
                trigger,
            )
            .map_err(error)?;

            state.context_window.generation = next_generation;
            state.context_window.model_history = retained.clone();
            state.context_window.model_history.push(provider_item);
            state.context_window.estimated_tokens = Some(compaction::estimate_request_tokens(
                config,
                &state.context_window.model_history,
            ));
            state.context_window.checkpoint = Some(CompactionCheckpoint {
                provider: validated.provider,
                response_id: validated.response_id,
                native_item: validated.native_item,
                source_through_sequence,
                source_projection_sha256,
                retained_user_messages: retained,
                generation: next_generation,
                usage: validated.usage,
            });

            let initial_input = {
                let active = state
                    .active_turn
                    .as_mut()
                    .ok_or_else(|| error("active turn disappeared after compaction"))?;
                let TurnPhase::Compacting(compacting) = &mut active.phase else {
                    return Err(error("active turn left compaction unexpectedly"));
                };
                compacting.initial_input.take()
            };
            let steering = {
                let active = state
                    .active_turn
                    .as_mut()
                    .ok_or_else(|| error("active turn disappeared after compaction"))?;
                std::mem::take(&mut active.pending_steering)
            };
            let mut post_compaction_input = Vec::new();
            if let Some(initial_input) = initial_input {
                post_compaction_input.push(initial_input);
            }
            post_compaction_input.extend(steering.into_iter().map(|item| item.message));
            extend_projection(&mut state, post_compaction_input);

            let current_date = active_current_date(&state)?.to_owned();
            let request = llm::fresh_request(
                config,
                crate::prompt::PromptContext {
                    session_id: platform_session_id,
                    current_date: &current_date,
                },
                state.context_window.model_history.clone(),
            );
            let mut result = OutcomeBuilder::new(state);
            result.append_message(checkpoint_message);
            let operation_id = result.request_llm(harness.llm_connection_id().clone(), request);
            let active =
                result.state_mut().active_turn.as_mut().ok_or_else(|| {
                    error("active turn disappeared while resuming after compaction")
                })?;
            active.phase = TurnPhase::AwaitingLlm(AwaitingLlmState {
                operation_id,
                purpose: LlmPurpose::Turn,
            });
            result.emit_progress(serde_json::json!({
                "type": "context_compaction_completed",
                "generation": next_generation,
                "trigger": trigger,
            }));
            result.set_status(SessionStatus::Running);
            Ok(result.finish())
        }
        OperationOutcome::Succeeded(_) => Err(error(
            "successful compaction operation contains a non-LLM result",
        )),
        OperationOutcome::Unknown(_) if attempt < 2 => {
            let current_date = active_current_date(&state)?.to_owned();
            let request = llm::compaction_request(
                config,
                crate::prompt::PromptContext {
                    session_id: platform_session_id,
                    current_date: &current_date,
                },
                state.context_window.model_history.clone(),
            )
            .map_err(error)?;
            let mut result = OutcomeBuilder::new(state);
            let operation_id = result.request_llm(harness.llm_connection_id().clone(), request);
            let active = result
                .state_mut()
                .active_turn
                .as_mut()
                .ok_or_else(|| error("active turn disappeared while retrying compaction"))?;
            let TurnPhase::Compacting(compacting) = &mut active.phase else {
                return Err(error("active turn left compaction before retry"));
            };
            compacting.operation_id = operation_id;
            compacting.attempt += 1;
            let retry_attempt = compacting.attempt;
            result.emit_progress(serde_json::json!({
                "type": "context_compaction_retried",
                "attempt": retry_attempt,
            }));
            result.set_status(SessionStatus::Running);
            Ok(result.finish())
        }
        OperationOutcome::Failed(operation_failure)
        | OperationOutcome::Unknown(operation_failure) => Ok(failure::terminal(
            state,
            failure::FailureDetails::operation(
                FailurePhase::Compaction,
                operation_id,
                operation_failure,
                attempt,
                event_sequence,
            ),
        )),
        OperationOutcome::Cancelled => Ok(failure::terminal(
            state,
            failure::FailureDetails::local(
                FailurePhase::Compaction,
                Some(operation_id),
                "unexpected_compaction_cancellation",
                "compaction was cancelled without a session cancellation request",
                attempt,
                event_sequence,
            ),
        )),
    }
}

async fn handle_execution_completion(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    mut state: BasicCodexState,
    operation_id: agent_contracts::OperationId,
    outcome_status: agent_contracts::OperationOutcomeStatus,
    context: &dyn HarnessContext,
    event_context: (agent_contracts::SessionId, i64),
) -> Result<HandlerOutcome<BasicCodexState>, HandlerError> {
    let (platform_session_id, event_timestamp_ms) = event_context;
    let record = context
        .operation(operation_id)
        .await
        .map_err(|failure| {
            error(format!(
                "could not read completed execution operation: {failure}"
            ))
        })?
        .ok_or_else(|| error("completed execution operation was not found"))?;
    if record.kind != OperationKind::Execution {
        return Err(error(
            "completed execution operation record has the wrong kind",
        ));
    }
    let outcome = record
        .outcome
        .ok_or_else(|| error("completed execution operation has no terminal outcome"))?;
    if outcome.status() != outcome_status {
        return Err(error(
            "completion event status does not match the stored execution outcome",
        ));
    }
    tool_execution::apply_completion(
        &mut state,
        operation_id,
        outcome,
        event_timestamp_ms,
        context,
    )
    .await?;
    let mut result = OutcomeBuilder::new(state);
    tool_execution::schedule_ready(
        harness,
        config,
        &mut result,
        platform_session_id,
        event_timestamp_ms,
    )?;
    if !finish_completed_tool_plan(
        harness,
        config,
        &mut result,
        context.history_through_sequence(),
        platform_session_id,
    )? {
        result.set_status(SessionStatus::Running);
    }
    Ok(result.finish())
}

fn finish_completed_tool_plan(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    result: &mut OutcomeBuilder<BasicCodexState>,
    history_through_sequence: HistorySequence,
    platform_session_id: agent_contracts::SessionId,
) -> Result<bool, HandlerError> {
    let messages = {
        let state = result.state_mut();
        let Some(active) = state.active_turn.as_ref() else {
            return Err(error("tool completion has no active turn"));
        };
        let TurnPhase::ExecutingTools(plan) = &active.phase else {
            return Err(error("tool completion has no active tool plan"));
        };
        if !plan.is_complete() {
            return Ok(false);
        }
        plan.ordered_result_messages()
            .map_err(|failure| error(failure.to_string()))?
    };
    let source_through_sequence =
        advance_history_sequence(history_through_sequence, messages.len())?;
    extend_projection(result.state_mut(), messages.iter().cloned());
    for message in messages {
        result.append_message(message);
    }
    stage_after_boundary(
        harness,
        config,
        result,
        source_through_sequence,
        platform_session_id,
    )?;
    Ok(true)
}

fn stage_after_boundary(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    result: &mut OutcomeBuilder<BasicCodexState>,
    history_through_sequence: HistorySequence,
    platform_session_id: agent_contracts::SessionId,
) -> Result<(), HandlerError> {
    if needs_compaction(config, result.state_mut()) {
        return stage_compaction(
            harness,
            config,
            result,
            history_through_sequence,
            CompactionTrigger::MidTurn,
            platform_session_id,
        );
    }
    let steering = {
        let active = result
            .state_mut()
            .active_turn
            .as_mut()
            .ok_or_else(|| error("active turn disappeared at an LLM boundary"))?;
        std::mem::take(&mut active.pending_steering)
    };
    extend_projection(
        result.state_mut(),
        steering.into_iter().map(|steering| steering.message),
    );
    let current_date = active_current_date(result.state_mut())?.to_owned();
    let request = llm::fresh_request(
        config,
        crate::prompt::PromptContext {
            session_id: platform_session_id,
            current_date: &current_date,
        },
        result.state_mut().context_window.model_history.clone(),
    );
    let next_operation = result.request_llm(harness.llm_connection_id().clone(), request);
    let active = result
        .state_mut()
        .active_turn
        .as_mut()
        .ok_or_else(|| error("active turn disappeared while staging the next LLM request"))?;
    active.phase = TurnPhase::AwaitingLlm(AwaitingLlmState {
        operation_id: next_operation,
        purpose: LlmPurpose::Turn,
    });
    result.set_status(SessionStatus::Running);
    Ok(())
}

fn stage_compaction(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    result: &mut OutcomeBuilder<BasicCodexState>,
    history_through_sequence: HistorySequence,
    trigger: CompactionTrigger,
    platform_session_id: agent_contracts::SessionId,
) -> Result<(), HandlerError> {
    let source_projection_sha256 =
        compaction::projection_sha256(&result.state_mut().context_window.model_history)
            .map_err(error)?;
    let current_date = active_current_date(result.state_mut())?.to_owned();
    let request = llm::compaction_request(
        config,
        crate::prompt::PromptContext {
            session_id: platform_session_id,
            current_date: &current_date,
        },
        result.state_mut().context_window.model_history.clone(),
    )
    .map_err(error)?;
    let operation_id = result.request_llm(harness.llm_connection_id().clone(), request);
    let active = result
        .state_mut()
        .active_turn
        .as_mut()
        .ok_or_else(|| error("active turn disappeared while staging compaction"))?;
    active.phase = TurnPhase::Compacting(CompactionState {
        operation_id,
        source_through_sequence: history_through_sequence,
        source_projection_sha256,
        attempt: 1,
        trigger,
        initial_input: None,
    });
    result.emit_progress(serde_json::json!({
        "type": "context_compaction_started",
        "trigger": trigger,
    }));
    result.set_status(SessionStatus::Running);
    Ok(())
}

fn needs_compaction(config: &BasicCodexConfig, state: &BasicCodexState) -> bool {
    state.context_window.estimated_tokens.unwrap_or_else(|| {
        compaction::estimate_request_tokens(config, &state.context_window.model_history)
    }) >= config.model.profile().auto_compact_limit
}

fn active_current_date(state: &BasicCodexState) -> Result<&str, HandlerError> {
    state
        .active_turn
        .as_ref()
        .map(|turn| turn.current_date.as_str())
        .ok_or_else(|| error("cannot build an LLM request without an active turn"))
}

fn extend_projection(state: &mut BasicCodexState, messages: impl IntoIterator<Item = Message>) {
    let messages: Vec<_> = messages.into_iter().collect();
    if let Some(estimate) = state.context_window.estimated_tokens.as_mut() {
        *estimate = estimate.saturating_add(
            messages
                .iter()
                .map(compaction::estimate_message_tokens)
                .sum::<u64>(),
        );
    }
    state.context_window.model_history.extend(messages);
}

fn add_latest_projection_estimate(config: &BasicCodexConfig, state: &mut BasicCodexState) {
    if state.context_window.estimated_tokens.is_none() {
        state.context_window.estimated_tokens = Some(compaction::estimate_request_tokens(
            config,
            &state.context_window.model_history,
        ));
    } else if let Some(message) = state.context_window.model_history.last() {
        state.context_window.estimated_tokens = state
            .context_window
            .estimated_tokens
            .map(|estimate| estimate.saturating_add(compaction::estimate_message_tokens(message)));
    }
}

fn advance_history_sequence(
    sequence: HistorySequence,
    added_entries: usize,
) -> Result<HistorySequence, HandlerError> {
    let added_entries = i64::try_from(added_entries)
        .map_err(|_| error("too many history entries to advance the compaction boundary"))?;
    sequence
        .0
        .checked_add(added_entries)
        .map(HistorySequence)
        .ok_or_else(|| error("compaction history boundary overflowed"))
}

fn error(message: impl Into<String>) -> HandlerError {
    HandlerError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BasicCodexModel, BasicCodexProvider, ReasoningEffort, WorkingDirectory};
    use agent_contracts::{
        AssistantResponse, EventId, EventSequence, FailureSource, GatewayConnectionId, Harness,
        HarnessId, HarnessVersion, HistorySequence, LlmRequest, MessageBase, OperationFailure,
        OperationOutcomeStatus, OperationRecord, OperationRequest, ProjectId, Provider, SessionId,
        SessionView, StateVersion, Usage,
    };
    use agent_test_support::MemoryContext;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use chrono::Utc;
    use serde_json::{json, value::to_raw_value};
    use std::{collections::BTreeMap, path::PathBuf, time::SystemTime};

    fn harness() -> BasicCodexHarness {
        BasicCodexHarness::new(
            GatewayConnectionId("llm-primary".into()),
            GatewayConnectionId("execution-primary".into()),
        )
        .unwrap()
    }

    fn config(provider: BasicCodexProvider) -> BasicCodexConfig {
        BasicCodexConfig {
            account_id: Uuid::new_v4(),
            provider,
            model: BasicCodexModel::Luna,
            reasoning_effort: ReasoningEffort::Medium,
            machine_id: Uuid::new_v4(),
            cwd: WorkingDirectory::new("/workspace").unwrap(),
            shell: Some("zsh".into()),
            platform: Some("linux".into()),
            additional_instructions: None,
        }
    }

    fn context(session_id: SessionId) -> MemoryContext {
        MemoryContext::new(
            SessionView {
                id: session_id,
                project_id: ProjectId("test-project".into()),
                harness_id: HarnessId(crate::HARNESS_ID.into()),
                harness_version: HarnessVersion(crate::HARNESS_VERSION.into()),
                status: SessionStatus::Idle,
                state_version: StateVersion(0),
                metadata: json!({}),
            },
            HistorySequence(0),
        )
    }

    fn event(session_id: SessionId, sequence: i64, kind: EventKind) -> SessionEvent {
        SessionEvent {
            id: EventId::new(),
            session_id,
            sequence: EventSequence(sequence),
            created_at: Utc::now(),
            kind,
        }
    }

    fn assistant(provider: Provider, stop_reason: StopReason) -> AssistantResponse {
        assistant_with_items(
            provider,
            stop_reason,
            vec![json!({
                "type": "output_text",
                "text": "hello from the model"
            })],
        )
    }

    fn assistant_with_items(
        provider: Provider,
        stop_reason: StopReason,
        items: Vec<serde_json::Value>,
    ) -> AssistantResponse {
        AssistantResponse {
            id: format!("response-{}", Uuid::new_v4()),
            model_id: "gpt-5.6-luna".into(),
            resolved_model_id: Some("gpt-5.6-luna-2026-09-01".into()),
            message: Message::Assistant {
                base: MessageBase::default(),
                provider,
                content: items
                    .into_iter()
                    .map(|item| to_raw_value(&item).unwrap())
                    .collect(),
            },
            stop_reason,
            usage: Some(Usage {
                input: Some(100),
                output: Some(20),
                cache_read: Some(10),
                cache_write: None,
                cost: None,
            }),
            duration_ms: 15,
            timestamp: Utc::now().timestamp_millis(),
        }
    }

    fn compaction_assistant(provider: Provider) -> AssistantResponse {
        assistant_with_items(
            provider,
            StopReason::Stop,
            vec![json!({
                "type": "compaction",
                "id": "cmp_1",
                "encrypted_content": "opaque-encrypted-summary"
            })],
        )
    }

    fn high_usage_assistant(provider: Provider, stop_reason: StopReason) -> AssistantResponse {
        let mut response = assistant(provider, stop_reason);
        response.usage = Some(Usage {
            input: Some(crate::model::AUTO_COMPACT_LIMIT - 100),
            output: Some(100),
            cache_read: None,
            cache_write: None,
            cost: None,
        });
        response
    }

    fn execution_response(
        handle: agent_contracts::execution_core::ExecutionHandle,
        state: agent_contracts::execution_core::ExecutionState,
        result: Option<agent_contracts::execution_core::ExecutionResult>,
        output: &str,
        cursor: &str,
        has_more: bool,
    ) -> agent_contracts::execution::Response {
        let execution = agent_contracts::execution_core::Execution {
            handle,
            command: agent_contracts::execution_core::Command::shell("fixture"),
            resolved_shell: None,
            cwd: PathBuf::from("/workspace"),
            io: agent_contracts::execution_core::IoMode::Pty { rows: 24, cols: 80 },
            labels: BTreeMap::new(),
            state,
            result,
            created_at: SystemTime::now(),
            started_at: Some(SystemTime::now()),
            finished_at: (state == agent_contracts::execution_core::ExecutionState::Finished)
                .then(SystemTime::now),
            output_incomplete: false,
            input_error: None,
        };
        agent_contracts::execution::Response {
            protocol_version: agent_contracts::execution::VERSION,
            request_id: Some(Uuid::new_v4().to_string()),
            generation_id: handle.generation_id,
            outcome: agent_contracts::execution::Outcome::Ok {
                result: json!({
                    "execution": execution,
                    "output": [{
                        "stream": "terminal",
                        "data_base64": STANDARD.encode(output.as_bytes()),
                    }],
                    "next_cursor": cursor,
                    "has_more": has_more,
                    "output_gap": false,
                    "return_reason": if state == agent_contracts::execution_core::ExecutionState::Finished {
                        "finished"
                    } else {
                        "wait_elapsed"
                    },
                }),
            },
        }
    }

    async fn start_turn(
        provider: BasicCodexProvider,
    ) -> (
        BasicCodexHarness,
        BasicCodexConfig,
        MemoryContext,
        SessionId,
        HandlerOutcome<BasicCodexState>,
    ) {
        let harness = harness();
        let config = config(provider);
        let session_id = SessionId::new();
        let context = context(session_id);
        let outcome = harness
            .handle(
                &config,
                BasicCodexState::initial(),
                event(
                    session_id,
                    1,
                    EventKind::UserMessage {
                        message: Message::user_text("hello"),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        (harness, config, context, session_id, outcome)
    }

    #[tokio::test]
    async fn idle_user_message_stages_a_fresh_request_and_enters_running() {
        let (_, config, _, _, outcome) = start_turn(BasicCodexProvider::Openai).await;
        assert_eq!(outcome.status, Some(SessionStatus::Running));
        assert_eq!(outcome.history.len(), 1);
        assert_eq!(outcome.operations.len(), 1);
        let agent_contracts::OperationRequest::Llm {
            connection,
            request,
        } = &outcome.operations[0].request
        else {
            panic!("expected LLM operation");
        };
        assert_eq!(connection.0, "llm-primary");
        let LlmRequest::Fresh {
            account_id,
            model_id,
            messages,
            ..
        } = request
        else {
            panic!("expected fresh request");
        };
        assert_eq!(*account_id, config.account_id);
        assert_eq!(model_id, "gpt-5.6-luna");
        assert_eq!(messages.len(), 2);
        assert!(
            serde_json::to_string(&messages[0])
                .unwrap()
                .contains("<environment_context>")
        );
        assert!(matches!(messages[1], Message::User { .. }));
        assert_eq!(
            outcome
                .state
                .active_turn
                .as_ref()
                .unwrap()
                .started_at_sequence,
            EventSequence(1)
        );
    }

    #[tokio::test]
    async fn terminal_assistant_is_saved_and_the_next_turn_sends_full_history() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let operation_id = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: operation_id,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant(Provider::Openai, StopReason::Stop),
                )))),
            })
            .unwrap();
        let completed = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(completed.status, Some(SessionStatus::Idle));
        assert_eq!(completed.history.len(), 1);
        assert!(matches!(completed.history[0], Message::Assistant { .. }));
        assert!(completed.state.active_turn.is_none());
        assert_eq!(completed.state.context_window.estimated_tokens, Some(130));
        assert_eq!(completed.state.context_window.model_history.len(), 2);

        let next = harness
            .handle(
                &config,
                completed.state,
                event(
                    session_id,
                    3,
                    EventKind::UserMessage {
                        message: Message::user_text("second turn"),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        let agent_contracts::OperationRequest::Llm { request, .. } = &next.operations[0].request
        else {
            panic!("expected LLM operation");
        };
        let LlmRequest::Fresh {
            messages,
            provider_options,
            ..
        } = request
        else {
            panic!("expected fresh request");
        };
        assert_eq!(messages.len(), 4);
        assert!(matches!(messages[0], Message::User { .. }));
        assert!(matches!(messages[1], Message::User { .. }));
        assert!(matches!(messages[2], Message::Assistant { .. }));
        assert!(matches!(messages[3], Message::User { .. }));
        assert_eq!(provider_options["prompt_cache_key"], session_id.to_string());
    }

    #[tokio::test]
    async fn pause_turn_stages_a_fresh_full_history_request() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Chatgpt).await;
        let operation_id = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: operation_id,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant(Provider::Chatgpt, StopReason::PauseTurn),
                )))),
            })
            .unwrap();
        let paused = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(paused.status, Some(SessionStatus::Running));
        assert_eq!(paused.operations.len(), 1);
        let agent_contracts::OperationRequest::Llm { request, .. } = &paused.operations[0].request
        else {
            panic!("expected LLM operation");
        };
        let LlmRequest::Fresh { messages, .. } = request else {
            panic!("expected fresh request");
        };
        assert_eq!(messages.len(), 3);
        assert!(matches!(messages[0], Message::User { .. }));
        assert!(matches!(messages[1], Message::User { .. }));
        assert!(matches!(messages[2], Message::Assistant { .. }));
    }

    #[tokio::test]
    async fn active_user_messages_are_steering_and_drain_in_event_order() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let operation_id = started.operations[0].id;

        let first_steering = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::UserMessage {
                        message: Message::user_text("first steering"),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(first_steering.status, None);
        assert!(first_steering.operations.is_empty());
        assert_eq!(first_steering.history.len(), 1);
        assert_eq!(
            first_steering
                .state
                .active_turn
                .as_ref()
                .unwrap()
                .pending_steering[0]
                .event_sequence,
            EventSequence(2)
        );
        assert_eq!(first_steering.state.context_window.model_history.len(), 1);

        let second_steering = harness
            .handle(
                &config,
                first_steering.state,
                event(
                    session_id,
                    3,
                    EventKind::UserMessage {
                        message: Message::user_text("second steering"),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        let pending = &second_steering
            .state
            .active_turn
            .as_ref()
            .unwrap()
            .pending_steering;
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].event_sequence, EventSequence(2));
        assert_eq!(pending[1].event_sequence, EventSequence(3));

        let recovered: BasicCodexState =
            serde_json::from_value(serde_json::to_value(second_steering.state).unwrap()).unwrap();
        let pending = &recovered.active_turn.as_ref().unwrap().pending_steering;
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].event_sequence, EventSequence(2));
        assert_eq!(pending[1].event_sequence, EventSequence(3));

        context
            .add_operation(&OperationRecord {
                id: operation_id,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant(Provider::Openai, StopReason::Stop),
                )))),
            })
            .unwrap();
        let continued = harness
            .handle(
                &config,
                recovered,
                event(
                    session_id,
                    4,
                    EventKind::OperationCompleted {
                        operation_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(continued.status, Some(SessionStatus::Running));
        assert_eq!(continued.history.len(), 1);
        assert!(
            continued
                .state
                .active_turn
                .as_ref()
                .unwrap()
                .pending_steering
                .is_empty()
        );
        let agent_contracts::OperationRequest::Llm { request, .. } =
            &continued.operations[0].request
        else {
            panic!("expected LLM operation");
        };
        let LlmRequest::Fresh { messages, .. } = request else {
            panic!("expected fresh request");
        };
        assert_eq!(messages.len(), 5);
        assert!(matches!(messages[0], Message::User { .. }));
        assert!(matches!(messages[1], Message::User { .. }));
        assert!(matches!(messages[2], Message::Assistant { .. }));
        assert_eq!(
            serde_json::to_value(&messages[3]).unwrap()["content"][0]["text"],
            "first steering"
        );
        assert_eq!(
            serde_json::to_value(&messages[4]).unwrap()["content"][0]["text"],
            "second steering"
        );
    }

    #[tokio::test]
    async fn pre_turn_compaction_holds_new_input_and_resumes_with_a_native_checkpoint() {
        let harness = harness();
        let config = config(BasicCodexProvider::Openai);
        let session_id = SessionId::new();
        let mut context = context(session_id);
        let mut state = BasicCodexState::initial();
        state.context_window.model_history = vec![
            Message::user_text("old user message"),
            assistant(Provider::Openai, StopReason::Stop).message,
        ];
        state.context_window.estimated_tokens = Some(crate::model::AUTO_COMPACT_LIMIT);

        let started = harness
            .handle(
                &config,
                state,
                event(
                    session_id,
                    10,
                    EventKind::UserMessage {
                        message: Message::user_text("new turn"),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(started.history.len(), 1);
        let TurnPhase::Compacting(compacting) = &started.state.active_turn.as_ref().unwrap().phase
        else {
            panic!("expected pre-turn compaction");
        };
        assert_eq!(compacting.trigger, CompactionTrigger::PreTurn);
        assert!(compacting.initial_input.is_some());
        let compaction_operation = compacting.operation_id;
        let OperationRequest::Llm { request, .. } = &started.operations[0].request else {
            panic!("expected LLM request");
        };
        let LlmRequest::Fresh { messages, .. } = request else {
            panic!("expected fresh compaction request");
        };
        assert_eq!(messages.len(), 4);
        assert_eq!(
            serde_json::to_value(messages.last().unwrap()).unwrap()["data"]["content"][0]["type"],
            "compaction_trigger"
        );
        assert!(
            !serde_json::to_string(messages)
                .unwrap()
                .contains("new turn")
        );

        let steered = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    11,
                    EventKind::UserMessage {
                        message: Message::user_text("steer while compacting"),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        let recovered: BasicCodexState =
            serde_json::from_value(serde_json::to_value(steered.state).unwrap()).unwrap();
        context
            .add_operation(&OperationRecord {
                id: compaction_operation,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    compaction_assistant(Provider::Openai),
                )))),
            })
            .unwrap();
        let resumed = harness
            .handle(
                &config,
                recovered,
                event(
                    session_id,
                    12,
                    EventKind::OperationCompleted {
                        operation_id: compaction_operation,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(resumed.status, Some(SessionStatus::Running));
        assert_eq!(resumed.state.context_window.generation, 2);
        assert!(resumed.state.context_window.checkpoint.is_some());
        assert_eq!(resumed.history.len(), 1);
        assert_eq!(
            serde_json::to_value(&resumed.history[0]).unwrap()["tag"],
            compaction::CHECKPOINT_TAG
        );
        let OperationRequest::Llm { request, .. } = &resumed.operations[0].request else {
            panic!("expected resumed LLM request");
        };
        let LlmRequest::Fresh { messages, .. } = request else {
            panic!("expected fresh resumed request");
        };
        assert_eq!(messages.len(), 5);
        assert_eq!(
            serde_json::to_value(&messages[2]).unwrap()["data"]["content"][0]["type"],
            "compaction"
        );
        assert_eq!(
            serde_json::to_value(&messages[3]).unwrap()["content"][0]["text"],
            "new turn"
        );
        assert_eq!(
            serde_json::to_value(&messages[4]).unwrap()["content"][0]["text"],
            "steer while compacting"
        );
    }

    #[tokio::test]
    async fn mid_turn_compaction_occurs_after_the_assistant_boundary() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Chatgpt).await;
        let operation_id = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: operation_id,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    high_usage_assistant(Provider::Chatgpt, StopReason::PauseTurn),
                )))),
            })
            .unwrap();
        let compacting = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        let TurnPhase::Compacting(compaction) =
            &compacting.state.active_turn.as_ref().unwrap().phase
        else {
            panic!("expected mid-turn compaction");
        };
        assert_eq!(compaction.trigger, CompactionTrigger::MidTurn);
        assert!(compaction.initial_input.is_none());
        let OperationRequest::Llm { request, .. } = &compacting.operations[0].request else {
            panic!("expected compaction LLM request");
        };
        let LlmRequest::Fresh { messages, .. } = request else {
            panic!("expected fresh compaction request");
        };
        assert_eq!(messages.len(), 4);
        assert!(matches!(messages[0], Message::User { .. }));
        assert!(matches!(messages[1], Message::User { .. }));
        assert!(matches!(messages[2], Message::Assistant { .. }));
        assert!(matches!(messages[3], Message::Custom { .. }));
    }

    #[tokio::test]
    async fn unknown_compaction_is_reissued_once_with_the_same_projection() {
        let harness = harness();
        let config = config(BasicCodexProvider::Openai);
        let session_id = SessionId::new();
        let mut context = context(session_id);
        let mut state = BasicCodexState::initial();
        state
            .context_window
            .model_history
            .push(Message::user_text("old"));
        state.context_window.estimated_tokens = Some(crate::model::AUTO_COMPACT_LIMIT);
        let started = harness
            .handle(
                &config,
                state,
                event(
                    session_id,
                    2,
                    EventKind::UserMessage {
                        message: Message::user_text("new"),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        let first_operation = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: first_operation,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Unknown(OperationFailure {
                    source: FailureSource::Gateway,
                    code: "unknown_submission".into(),
                    message: "result could not be reconciled".into(),
                    execution_response: None,
                })),
            })
            .unwrap();
        let retried = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    3,
                    EventKind::OperationCompleted {
                        operation_id: first_operation,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Unknown,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(retried.operations.len(), 1);
        let TurnPhase::Compacting(compacting) = &retried.state.active_turn.as_ref().unwrap().phase
        else {
            panic!("expected compaction retry");
        };
        assert_eq!(compacting.attempt, 2);
        assert_ne!(compacting.operation_id, first_operation);
    }

    #[tokio::test]
    async fn provider_mismatch_is_rejected_before_state_can_advance() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let operation_id = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: operation_id,
                kind: OperationKind::Llm,
                gateway_job_id: None,
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant(Provider::Chatgpt, StopReason::Stop),
                )))),
            })
            .unwrap();
        let result = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(result.status, Some(SessionStatus::Failed));
        let terminal = result.state.terminal_failure.unwrap();
        assert_eq!(terminal.code, "malformed_provider_response");
        assert!(
            terminal
                .message
                .contains("does not match configured provider")
        );
    }

    #[tokio::test]
    async fn tool_use_saves_the_assistant_and_a_restart_safe_plan() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let operation_id = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: operation_id,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant_with_items(
                        Provider::Openai,
                        StopReason::ToolUse,
                        vec![
                            json!({
                                "type":"function_call", "call_id":"call-exec",
                                "name":"exec_command", "arguments":"{\"cmd\":\"pwd\"}"
                            }),
                            json!({
                                "type":"custom_tool_call", "call_id":"call-patch",
                                "name":"apply_patch", "input":"*** Begin Patch\n*** Delete File: old\n*** End Patch"
                            }),
                        ],
                    ),
                )))),
            })
            .unwrap();
        let outcome = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(outcome.status, Some(SessionStatus::Running));
        assert_eq!(outcome.history.len(), 1);
        assert_eq!(outcome.operations.len(), 1);
        let TurnPhase::ExecutingTools(plan) = &outcome.state.active_turn.as_ref().unwrap().phase
        else {
            panic!("expected a durable tool plan");
        };
        assert_eq!(plan.assistant_operation_id, operation_id);
        assert_eq!(plan.calls.len(), 2);
        assert_eq!(plan.calls[0].call_id, "call-exec");
        assert_eq!(plan.calls[1].call_id, "call-patch");
        assert_eq!(plan.segments.len(), 2);

        let encoded = serde_json::to_value(outcome.state).unwrap();
        let recovered: BasicCodexState = serde_json::from_value(encoded).unwrap();
        assert!(matches!(
            recovered.active_turn.unwrap().phase,
            TurnPhase::ExecutingTools(_)
        ));
    }

    #[tokio::test]
    async fn running_command_can_be_polled_to_completion_through_fresh_llm_turns() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let first_llm_id = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: first_llm_id,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant_with_items(
                        Provider::Openai,
                        StopReason::ToolUse,
                        vec![json!({
                            "type": "function_call",
                            "call_id": "start-shell",
                            "name": "exec_command",
                            "arguments": "{\"cmd\":\"sleep 1; echo done\",\"tty\":true,\"yield_time_ms\":250}"
                        })],
                    ),
                )))),
            })
            .unwrap();
        let command = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id: first_llm_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(command.operations.len(), 1);
        let OperationRequest::Execution { request, .. } = &command.operations[0].request else {
            panic!("expected execution operation");
        };
        let agent_contracts::execution::Payload::Single(
            agent_contracts::execution::Operation::Start(start),
        ) = request
        else {
            panic!("expected execution start");
        };
        assert_eq!(
            start
                .shell_snapshot
                .as_ref()
                .expect("Codex starts request shell snapshots")
                .scope_id,
            session_id.to_string()
        );
        let command_operation = command.operations[0].id;
        let command_state: BasicCodexState =
            serde_json::from_value(serde_json::to_value(command.state).unwrap()).unwrap();
        let handle = agent_contracts::execution_core::ExecutionHandle {
            id: Uuid::new_v4(),
            generation_id: Uuid::new_v4(),
        };
        context
            .add_operation(&OperationRecord {
                id: command_operation,
                kind: OperationKind::Execution,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Execution(
                    execution_response(
                        handle,
                        agent_contracts::execution_core::ExecutionState::Running,
                        None,
                        "started\n",
                        "cursor-one",
                        false,
                    ),
                ))),
            })
            .unwrap();
        let command_result = harness
            .handle(
                &config,
                command_state,
                event(
                    session_id,
                    3,
                    EventKind::OperationCompleted {
                        operation_id: command_operation,
                        operation_kind: OperationKind::Execution,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(command_result.state.executions.len(), 1);
        assert_eq!(command_result.state.executions[0].session_id, 1000);
        assert_eq!(command_result.history.len(), 1);
        let tool_text = serde_json::to_value(&command_result.history[0]).unwrap();
        assert!(
            tool_text["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Process running with session ID 1000")
        );

        let second_llm_id = command_result.operations[0].id;
        let recovered_running_state: BasicCodexState =
            serde_json::from_value(serde_json::to_value(command_result.state).unwrap()).unwrap();
        context
            .add_operation(&OperationRecord {
                id: second_llm_id,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant_with_items(
                        Provider::Openai,
                        StopReason::ToolUse,
                        vec![json!({
                            "type": "function_call",
                            "call_id": "poll-shell",
                            "name": "write_stdin",
                            "arguments": "{\"session_id\":1000,\"chars\":\"\",\"yield_time_ms\":5000}"
                        })],
                    ),
                )))),
            })
            .unwrap();
        let poll = harness
            .handle(
                &config,
                recovered_running_state,
                event(
                    session_id,
                    4,
                    EventKind::OperationCompleted {
                        operation_id: second_llm_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        let OperationRequest::Execution {
            expected_generation_id,
            request,
            ..
        } = &poll.operations[0].request
        else {
            panic!("expected an execution observation");
        };
        assert_eq!(*expected_generation_id, Some(handle.generation_id));
        assert!(matches!(
            request,
            agent_contracts::execution::Payload::Single(
                agent_contracts::execution::Operation::Observe(_)
            )
        ));

        let poll_operation = poll.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: poll_operation,
                kind: OperationKind::Execution,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Execution(
                    execution_response(
                        handle,
                        agent_contracts::execution_core::ExecutionState::Finished,
                        Some(agent_contracts::execution_core::ExecutionResult::Exited {
                            exit_code: Some(0),
                            signal: None,
                        }),
                        "done\n",
                        "cursor-two",
                        false,
                    ),
                ))),
            })
            .unwrap();
        let finished = harness
            .handle(
                &config,
                poll.state,
                event(
                    session_id,
                    5,
                    EventKind::OperationCompleted {
                        operation_id: poll_operation,
                        operation_kind: OperationKind::Execution,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert!(finished.state.executions.is_empty());
        let finished_text = serde_json::to_value(&finished.history[0]).unwrap();
        let finished_text = finished_text["content"][0]["text"].as_str().unwrap();
        assert!(finished_text.contains("Process exited with code 0"));
        assert!(finished_text.contains("done"));
        assert_eq!(finished.operations.len(), 1);
        assert!(matches!(
            finished.operations[0].request,
            OperationRequest::Llm { .. }
        ));
    }

    #[tokio::test]
    async fn invalid_known_arguments_are_returned_to_the_model_in_a_fresh_request() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let operation_id = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: operation_id,
                kind: OperationKind::Llm,
                gateway_job_id: None,
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant_with_items(
                        Provider::Openai,
                        StopReason::ToolUse,
                        vec![json!({
                            "type":"function_call", "call_id":"bad-args",
                            "name":"exec_command", "arguments":"{}"
                        })],
                    ),
                )))),
            })
            .unwrap();
        let outcome = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(outcome.status, Some(SessionStatus::Running));
        assert_eq!(outcome.history.len(), 2);
        assert!(matches!(outcome.history[0], Message::Assistant { .. }));
        assert!(matches!(outcome.history[1], Message::ToolResult { .. }));
        assert_eq!(outcome.operations.len(), 1);
        let agent_contracts::OperationRequest::Llm { request, .. } = &outcome.operations[0].request
        else {
            panic!("expected LLM request");
        };
        let LlmRequest::Fresh {
            messages, tools, ..
        } = request
        else {
            panic!("expected fresh request");
        };
        assert_eq!(messages.len(), 4);
        assert!(matches!(messages[3], Message::ToolResult { .. }));
        assert_eq!(tools.len(), 4);
    }

    #[tokio::test]
    async fn fatal_tool_protocol_errors_preserve_the_native_assistant_for_audit() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let operation_id = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: operation_id,
                kind: OperationKind::Llm,
                gateway_job_id: None,
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant_with_items(
                        Provider::Openai,
                        StopReason::ToolUse,
                        vec![json!({
                            "type":"function_call", "call_id":"unknown",
                            "name":"not_a_tool", "arguments":"{}"
                        })],
                    ),
                )))),
            })
            .unwrap();
        let outcome = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(outcome.status, Some(SessionStatus::Failed));
        assert_eq!(outcome.history.len(), 1);
        assert!(matches!(outcome.history[0], Message::Assistant { .. }));
        assert_eq!(outcome.state.context_window.model_history.len(), 2);
        assert_eq!(outcome.progress.len(), 1);
    }

    #[tokio::test]
    async fn incomplete_and_tool_use_without_calls_fail_the_session() {
        for stop_reason in [
            StopReason::Length,
            StopReason::ContentFilter,
            StopReason::ToolUse,
        ] {
            let (harness, config, mut context, session_id, started) =
                start_turn(BasicCodexProvider::Openai).await;
            let operation_id = started.operations[0].id;
            context
                .add_operation(&OperationRecord {
                    id: operation_id,
                    kind: OperationKind::Llm,
                    gateway_job_id: None,
                    outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                        assistant(Provider::Openai, stop_reason),
                    )))),
                })
                .unwrap();
            let outcome = harness
                .handle(
                    &config,
                    started.state,
                    event(
                        session_id,
                        2,
                        EventKind::OperationCompleted {
                            operation_id,
                            operation_kind: OperationKind::Llm,
                            outcome_status: OperationOutcomeStatus::Succeeded,
                        },
                    ),
                    &context,
                )
                .await
                .unwrap();
            assert_eq!(outcome.status, Some(SessionStatus::Failed));
            assert_eq!(outcome.history.len(), 1);
        }
    }

    #[tokio::test]
    async fn cancellation_withdraws_then_cancels_llm_and_ignores_its_late_response() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let llm_operation = started.operations[0].id;
        let cancelling = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::CancellationRequested {
                        reason: Some("user stopped the session".into()),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(cancelling.status, Some(SessionStatus::Cancelling));
        assert_eq!(cancelling.operations.len(), 1);
        assert!(matches!(
            cancelling.operations[0].request,
            OperationRequest::Withdraw { target } if target == llm_operation
        ));
        assert!(matches!(
            cancelling.state.active_turn.as_ref().unwrap().phase,
            TurnPhase::Cancelling
        ));
        let recovered: BasicCodexState =
            serde_json::from_value(serde_json::to_value(cancelling.state).unwrap()).unwrap();

        let withdrawal = recovered
            .cancellation
            .as_ref()
            .and_then(|cancellation| cancellation.targets.first())
            .and_then(|target| match target.control {
                crate::state::CancellationControl::WithdrawalRequested { operation_id } => {
                    Some(operation_id)
                }
                _ => None,
            })
            .unwrap();
        context
            .add_operation(&OperationRecord {
                id: withdrawal,
                kind: OperationKind::Withdraw,
                gateway_job_id: None,
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Withdraw {
                    withdrawn: false,
                })),
            })
            .unwrap();
        let cancellation_requested = harness
            .handle(
                &config,
                recovered,
                event(
                    session_id,
                    3,
                    EventKind::OperationCompleted {
                        operation_id: withdrawal,
                        operation_kind: OperationKind::Withdraw,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(
            cancellation_requested.status,
            Some(SessionStatus::Cancelling)
        );
        assert_eq!(cancellation_requested.operations.len(), 1);
        assert!(matches!(
            cancellation_requested.operations[0].request,
            OperationRequest::LlmCancellation { target } if target == llm_operation
        ));

        let cancellation_operation = cancellation_requested.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: cancellation_operation,
                kind: OperationKind::LlmCancellation,
                gateway_job_id: None,
                outcome: Some(OperationOutcome::Succeeded(
                    OperationResult::LlmCancellation { accepted: true },
                )),
            })
            .unwrap();
        let cancelled = harness
            .handle(
                &config,
                cancellation_requested.state,
                event(
                    session_id,
                    4,
                    EventKind::OperationCompleted {
                        operation_id: cancellation_operation,
                        operation_kind: OperationKind::LlmCancellation,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(cancelled.status, Some(SessionStatus::Cancelled));
        assert!(cancelled.state.active_turn.is_none());

        context
            .add_operation(&OperationRecord {
                id: llm_operation,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant(Provider::Openai, StopReason::Stop),
                )))),
            })
            .unwrap();
        let late = harness
            .handle(
                &config,
                cancelled.state,
                event(
                    session_id,
                    5,
                    EventKind::OperationCompleted {
                        operation_id: llm_operation,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(late.status, Some(SessionStatus::Cancelled));
        assert!(late.history.is_empty());
        assert!(late.operations.is_empty());
    }

    #[tokio::test]
    async fn ordinary_llm_unknown_is_reissued_once_then_fails_durably() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let first = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: first,
                kind: OperationKind::Llm,
                gateway_job_id: None,
                outcome: Some(OperationOutcome::Unknown(OperationFailure {
                    source: FailureSource::Gateway,
                    code: "result_unknown".into(),
                    message: "the provider result could not be recovered".into(),
                    execution_response: None,
                })),
            })
            .unwrap();
        let retried = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id: first,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Unknown,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(retried.status, Some(SessionStatus::Running));
        assert_eq!(retried.operations.len(), 1);
        assert_eq!(
            retried
                .state
                .active_turn
                .as_ref()
                .unwrap()
                .retry
                .llm_attempts,
            1
        );
        let second = retried.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: second,
                kind: OperationKind::Llm,
                gateway_job_id: None,
                outcome: Some(OperationOutcome::Unknown(OperationFailure {
                    source: FailureSource::Gateway,
                    code: "result_unknown".into(),
                    message: "still unknown".into(),
                    execution_response: None,
                })),
            })
            .unwrap();
        let failed = harness
            .handle(
                &config,
                retried.state,
                event(
                    session_id,
                    3,
                    EventKind::OperationCompleted {
                        operation_id: second,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Unknown,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(failed.status, Some(SessionStatus::Failed));
        let failure = failed.state.terminal_failure.unwrap();
        assert_eq!(failure.code, "result_unknown");
        assert_eq!(failure.attempts, 2);
    }

    #[tokio::test]
    async fn cancellation_interrupts_then_terminates_tracked_processes() {
        let harness = harness();
        let config = config(BasicCodexProvider::Openai);
        let session_id = SessionId::new();
        let mut context = context(session_id);
        let handle = agent_contracts::execution_core::ExecutionHandle {
            id: Uuid::new_v4(),
            generation_id: Uuid::new_v4(),
        };
        let mut state = BasicCodexState::initial();
        state.executions.push(crate::state::TrackedExecution {
            session_id: 1000,
            handle,
            cursor: "cursor".into(),
            owner_turn_id: Uuid::new_v4(),
            tty: true,
        });
        let interrupting = harness
            .handle(
                &config,
                state,
                event(
                    session_id,
                    1,
                    EventKind::CancellationRequested { reason: None },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(interrupting.status, Some(SessionStatus::Cancelling));
        assert_eq!(interrupting.operations.len(), 1);
        let interrupt = interrupting.operations[0].id;
        assert!(matches!(
            &interrupting.operations[0].request,
            OperationRequest::Execution {
                expected_generation_id: Some(generation),
                request: agent_contracts::execution::Payload::Single(
                    agent_contracts::execution::Operation::Interrupt { handle: target, .. }
                ),
                ..
            } if *generation == handle.generation_id && *target == handle
        ));
        context
            .add_operation(&OperationRecord {
                id: interrupt,
                kind: OperationKind::Execution,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Execution(
                    execution_response(
                        handle,
                        agent_contracts::execution_core::ExecutionState::Running,
                        None,
                        "",
                        "cursor",
                        false,
                    ),
                ))),
            })
            .unwrap();
        let terminating = harness
            .handle(
                &config,
                interrupting.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id: interrupt,
                        operation_kind: OperationKind::Execution,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(terminating.status, Some(SessionStatus::Cancelling));
        assert_eq!(terminating.operations.len(), 1);
        let terminate = terminating.operations[0].id;
        assert!(matches!(
            &terminating.operations[0].request,
            OperationRequest::Execution {
                request: agent_contracts::execution::Payload::Single(
                    agent_contracts::execution::Operation::Terminate { handle: target, .. }
                ),
                ..
            } if *target == handle
        ));
        context
            .add_operation(&OperationRecord {
                id: terminate,
                kind: OperationKind::Execution,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Execution(
                    execution_response(
                        handle,
                        agent_contracts::execution_core::ExecutionState::Running,
                        None,
                        "",
                        "cursor",
                        false,
                    ),
                ))),
            })
            .unwrap();
        let cancelled = harness
            .handle(
                &config,
                terminating.state,
                event(
                    session_id,
                    3,
                    EventKind::OperationCompleted {
                        operation_id: terminate,
                        operation_kind: OperationKind::Execution,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(cancelled.status, Some(SessionStatus::Cancelled));
        assert!(cancelled.state.executions.is_empty());
    }

    #[tokio::test]
    async fn idle_cancellation_is_immediately_terminal() {
        let harness = harness();
        let config = config(BasicCodexProvider::Openai);
        let session_id = SessionId::new();
        let context = context(session_id);
        let cancelled = harness
            .handle(
                &config,
                BasicCodexState::initial(),
                event(
                    session_id,
                    1,
                    EventKind::CancellationRequested {
                        reason: Some("no longer needed".into()),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(cancelled.status, Some(SessionStatus::Cancelled));
        assert!(cancelled.operations.is_empty());
        assert_eq!(
            cancelled.state.cancellation.unwrap().reason.as_deref(),
            Some("no longer needed")
        );
    }

    #[tokio::test]
    async fn cancellation_during_compaction_withdraws_the_native_request() {
        let harness = harness();
        let config = config(BasicCodexProvider::Openai);
        let session_id = SessionId::new();
        let context = context(session_id);
        let mut state = BasicCodexState::initial();
        state
            .context_window
            .model_history
            .push(Message::user_text("old context"));
        state.context_window.estimated_tokens = Some(crate::model::AUTO_COMPACT_LIMIT);
        let compacting = harness
            .handle(
                &config,
                state,
                event(
                    session_id,
                    1,
                    EventKind::UserMessage {
                        message: Message::user_text("new turn"),
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        let compaction_operation = compacting.operations[0].id;
        assert!(matches!(
            compacting.state.active_turn.as_ref().unwrap().phase,
            TurnPhase::Compacting(_)
        ));
        let cancelling = harness
            .handle(
                &config,
                compacting.state,
                event(
                    session_id,
                    2,
                    EventKind::CancellationRequested { reason: None },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(cancelling.status, Some(SessionStatus::Cancelling));
        assert_eq!(cancelling.operations.len(), 1);
        assert!(matches!(
            cancelling.operations[0].request,
            OperationRequest::Withdraw { target } if target == compaction_operation
        ));
    }

    #[tokio::test]
    async fn cancellation_drops_undispatched_tool_segments() {
        let (harness, config, mut context, session_id, started) =
            start_turn(BasicCodexProvider::Openai).await;
        let llm_operation = started.operations[0].id;
        context
            .add_operation(&OperationRecord {
                id: llm_operation,
                kind: OperationKind::Llm,
                gateway_job_id: Some(Uuid::new_v4()),
                outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
                    assistant_with_items(
                        Provider::Openai,
                        StopReason::ToolUse,
                        vec![
                            json!({
                                "type":"function_call",
                                "call_id":"shell_1",
                                "name":"exec_command",
                                "arguments":"{\"cmd\":\"sleep 120\",\"yield_time_ms\":1000}"
                            }),
                            json!({
                                "type":"custom_tool_call",
                                "call_id":"patch_1",
                                "name":"apply_patch",
                                "input":"*** Begin Patch\n*** Add File: later.txt\n+later\n*** End Patch"
                            }),
                        ],
                    ),
                )))),
            })
            .unwrap();
        let executing = harness
            .handle(
                &config,
                started.state,
                event(
                    session_id,
                    2,
                    EventKind::OperationCompleted {
                        operation_id: llm_operation,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Succeeded,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(executing.operations.len(), 1);
        let submitted_shell = executing.operations[0].id;
        let cancelling = harness
            .handle(
                &config,
                executing.state,
                event(
                    session_id,
                    3,
                    EventKind::CancellationRequested { reason: None },
                ),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(cancelling.operations.len(), 1);
        assert!(matches!(
            cancelling.operations[0].request,
            OperationRequest::Withdraw { target } if target == submitted_shell
        ));
        assert!(matches!(
            cancelling.state.active_turn.as_ref().unwrap().phase,
            TurnPhase::Cancelling
        ));
    }
}
