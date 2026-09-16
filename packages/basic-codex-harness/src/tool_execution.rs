use crate::{
    BasicCodexConfig, BasicCodexHarness,
    filesystem_tools::{self, AppliedFileResult},
    shell_tools::{self, AppliedShellResult, PreparedShellOperation},
    state::{
        BasicCodexState, ToolCallProgress, ToolCallState, ToolCallStatus, ToolPlanState, TurnPhase,
    },
};
use agent_contracts::{
    HandlerError, HarnessContext, OperationId, OperationOutcome, OperationResult, OutcomeBuilder,
    SessionId,
};
use std::collections::HashSet;

struct PreparedCall {
    ordinal: u32,
    request: agent_contracts::execution::Payload,
    expected_generation_id: Option<uuid::Uuid>,
    progress: ToolCallProgress,
}

pub(crate) fn schedule_ready(
    harness: &BasicCodexHarness,
    config: &BasicCodexConfig,
    result: &mut OutcomeBuilder<BasicCodexState>,
    platform_session_id: SessionId,
    started_at_ms: i64,
) -> Result<(), HandlerError> {
    loop {
        let (turn_id, ordinals) = {
            let state = result.state_mut();
            let active = state
                .active_turn
                .as_ref()
                .ok_or_else(|| error("tool scheduling requires an active turn"))?;
            let TurnPhase::ExecutingTools(plan) = &active.phase else {
                return Err(error("tool scheduling requires an executing-tools phase"));
            };
            let Some(segment) = plan.current_segment() else {
                return Ok(());
            };
            (active.id, segment.call_ordinals.clone())
        };

        let mut busy_resources = submitted_resources(current_plan(result.state_mut())?);
        let mut prepared = Vec::new();
        let mut completed_immediately = false;

        for ordinal in ordinals {
            let call = current_plan(result.state_mut())?
                .calls
                .get(ordinal as usize)
                .cloned()
                .ok_or_else(|| error("tool plan contains an invalid call ordinal"))?;
            let resource = resource_session_id(&call);
            if resource.is_some_and(|resource| busy_resources.contains(&resource)) {
                continue;
            }
            let prepared_call = match call.status.clone() {
                ToolCallStatus::Pending => {
                    let prepared = match shell_tools::prepare_new(
                        &call,
                        config,
                        result.state_mut(),
                        platform_session_id,
                        turn_id,
                        started_at_ms,
                    ) {
                        Ok(Some(operation)) => Ok(from_shell(ordinal, operation)),
                        Ok(None) => {
                            filesystem_tools::prepare_new(&call, config).and_then(|value| {
                                value
                                    .map(|operation| from_file(ordinal, operation))
                                    .ok_or_else(|| {
                                        shell_tools::tool_error(
                                            "tool_not_implemented",
                                            format!("{} is not implemented yet", call.name),
                                        )
                                    })
                            })
                        }
                        Err(snapshot) => Err(snapshot),
                    };
                    match prepared {
                        Ok(operation) => operation,
                        Err(snapshot) => {
                            complete_call(
                                current_plan_mut(result.state_mut())?,
                                ordinal,
                                snapshot,
                            )?;
                            completed_immediately = true;
                            continue;
                        }
                    }
                }
                ToolCallStatus::Ready(ToolCallProgress::Shell(progress)) => {
                    match shell_tools::prepare_ready(*progress) {
                        Ok(operation) => from_shell(ordinal, operation),
                        Err(snapshot) => {
                            complete_call(
                                current_plan_mut(result.state_mut())?,
                                ordinal,
                                snapshot,
                            )?;
                            completed_immediately = true;
                            continue;
                        }
                    }
                }
                ToolCallStatus::Ready(ToolCallProgress::Filesystem(progress)) => {
                    match filesystem_tools::prepare_ready(progress, config) {
                        Ok(operation) => from_file(ordinal, operation),
                        Err(snapshot) => {
                            complete_call(
                                current_plan_mut(result.state_mut())?,
                                ordinal,
                                snapshot,
                            )?;
                            completed_immediately = true;
                            continue;
                        }
                    }
                }
                ToolCallStatus::Submitted { .. } | ToolCallStatus::Completed(_) => continue,
            };
            let operation_resource = match &prepared_call.progress {
                ToolCallProgress::Shell(progress) => progress.session_id,
                ToolCallProgress::Filesystem(_) => None,
            };
            if let Some(resource) = resource.or(operation_resource) {
                busy_resources.insert(resource);
            }
            prepared.push(prepared_call);
        }

        if prepared.is_empty() {
            if completed_immediately {
                current_plan_mut(result.state_mut())?.advance_completed_segments();
                continue;
            }
            return Ok(());
        }

        for prepared in prepared {
            let operation_id = match prepared.expected_generation_id {
                Some(generation) => result.request_execution_for_generation(
                    harness.execution_connection_id().clone(),
                    config.machine_id,
                    generation,
                    prepared.request,
                ),
                None => result.request_execution(
                    harness.execution_connection_id().clone(),
                    config.machine_id,
                    prepared.request,
                ),
            }
            .map_err(|failure| error(format!("could not stage shell operation: {failure}")))?;
            let call = current_plan_mut(result.state_mut())?
                .calls
                .get_mut(prepared.ordinal as usize)
                .ok_or_else(|| error("tool call disappeared while staging execution"))?;
            call.status = ToolCallStatus::Submitted {
                operation_id,
                progress: prepared.progress,
            };
        }
        return Ok(());
    }
}

pub(crate) async fn apply_completion(
    state: &mut BasicCodexState,
    operation_id: OperationId,
    outcome: OperationOutcome,
    completed_at_ms: i64,
    context: &dyn HarnessContext,
) -> Result<(), HandlerError> {
    let (turn_id, ordinal, call_id, progress) = {
        let active = state
            .active_turn
            .as_ref()
            .ok_or_else(|| error("execution completion has no active turn"))?;
        let TurnPhase::ExecutingTools(plan) = &active.phase else {
            return Err(error(
                "execution completion arrived outside the executing-tools phase",
            ));
        };
        let (ordinal, call_id, progress) = plan
            .calls
            .iter()
            .find_map(|call| match &call.status {
                ToolCallStatus::Submitted {
                    operation_id: submitted,
                    progress,
                } if *submitted == operation_id => {
                    Some((call.ordinal, call.call_id.clone(), progress.clone()))
                }
                _ => None,
            })
            .ok_or_else(|| error("execution completion does not match a submitted tool call"))?;
        (active.id, ordinal, call_id, progress)
    };

    let applied = match outcome {
        OperationOutcome::Succeeded(OperationResult::Execution(response)) => {
            apply_response(
                progress,
                response,
                state,
                turn_id,
                &call_id,
                completed_at_ms,
                context,
            )
            .await
        }
        OperationOutcome::Succeeded(_) => {
            return Err(error(
                "successful execution operation contains a non-execution result",
            ));
        }
        OperationOutcome::Failed(failure) => match failure.execution_response {
            Some(response) => {
                apply_response(
                    progress,
                    response,
                    state,
                    turn_id,
                    &call_id,
                    completed_at_ms,
                    context,
                )
                .await
            }
            None => Err(shell_tools::tool_error_with_details(
                &failure.code,
                format!(
                    "execution gateway rejected the operation: {}",
                    failure.message
                ),
                serde_json::json!({"operation_outcome":"failed", "failure":failure}),
            )),
        },
        OperationOutcome::Unknown(failure) => match progress {
            ToolCallProgress::Filesystem(progress) => {
                filesystem_tools::apply_unknown(progress, failure.message.clone()).map(|result| {
                    match result {
                        AppliedFileResult::Ready(progress) => {
                            AppliedToolResult::Ready(ToolCallProgress::Filesystem(progress))
                        }
                        AppliedFileResult::Completed(snapshot) => {
                            AppliedToolResult::Completed(snapshot)
                        }
                    }
                })
            }
            ToolCallProgress::Shell(_) => Err(shell_tools::tool_error_with_details(
                "execution_unknown",
                format!(
                    "execution outcome is unknown; do not automatically repeat the command or input: {}",
                    failure.message
                ),
                serde_json::json!({"operation_outcome":"unknown", "failure":failure}),
            )),
        },
        OperationOutcome::Cancelled => Err(shell_tools::tool_error(
            "execution_cancelled",
            "execution operation was cancelled",
        )),
    };

    let status = match applied {
        Ok(AppliedToolResult::Ready(progress)) => ToolCallStatus::Ready(progress),
        Ok(AppliedToolResult::Completed(snapshot)) | Err(snapshot) => {
            ToolCallStatus::Completed(snapshot)
        }
    };
    let plan = current_plan_mut(state)?;
    let call = plan
        .calls
        .get_mut(ordinal as usize)
        .ok_or_else(|| error("tool call disappeared while applying its result"))?;
    call.status = status;
    plan.advance_completed_segments();
    Ok(())
}

enum AppliedToolResult {
    Ready(ToolCallProgress),
    Completed(crate::state::ToolResultSnapshot),
}

async fn apply_response(
    progress: ToolCallProgress,
    response: agent_contracts::execution::Response,
    state: &mut BasicCodexState,
    turn_id: uuid::Uuid,
    call_id: &str,
    completed_at_ms: i64,
    context: &dyn HarnessContext,
) -> Result<AppliedToolResult, crate::state::ToolResultSnapshot> {
    match progress {
        ToolCallProgress::Shell(progress) => {
            shell_tools::apply_response(*progress, response, state, turn_id, completed_at_ms).map(
                |result| match result {
                    AppliedShellResult::Ready(progress) => {
                        AppliedToolResult::Ready(ToolCallProgress::Shell(progress))
                    }
                    AppliedShellResult::Completed(snapshot) => {
                        AppliedToolResult::Completed(snapshot)
                    }
                },
            )
        }
        ToolCallProgress::Filesystem(progress) => {
            filesystem_tools::apply_response(progress, response, turn_id, call_id, context)
                .await
                .map(|result| match result {
                    AppliedFileResult::Ready(progress) => {
                        AppliedToolResult::Ready(ToolCallProgress::Filesystem(progress))
                    }
                    AppliedFileResult::Completed(snapshot) => {
                        AppliedToolResult::Completed(snapshot)
                    }
                })
        }
    }
}

fn from_shell(ordinal: u32, operation: PreparedShellOperation) -> PreparedCall {
    PreparedCall {
        ordinal,
        request: operation.request,
        expected_generation_id: operation.expected_generation_id,
        progress: ToolCallProgress::Shell(Box::new(operation.progress)),
    }
}

fn from_file(ordinal: u32, operation: filesystem_tools::PreparedFileOperation) -> PreparedCall {
    PreparedCall {
        ordinal,
        request: operation.request,
        expected_generation_id: operation.expected_generation_id,
        progress: ToolCallProgress::Filesystem(operation.progress),
    }
}

fn submitted_resources(plan: &ToolPlanState) -> HashSet<i32> {
    plan.calls
        .iter()
        .filter_map(|call| match &call.status {
            ToolCallStatus::Submitted {
                progress: ToolCallProgress::Shell(progress),
                ..
            } => progress.session_id,
            _ => None,
        })
        .collect()
}

fn resource_session_id(call: &ToolCallState) -> Option<i32> {
    match &call.status {
        ToolCallStatus::Ready(ToolCallProgress::Shell(progress))
        | ToolCallStatus::Submitted {
            progress: ToolCallProgress::Shell(progress),
            ..
        } => progress.session_id,
        ToolCallStatus::Pending if call.name == "write_stdin" => match &call.input {
            crate::state::ToolInput::Json(value) => value
                .get("session_id")
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| i32::try_from(value).ok()),
            crate::state::ToolInput::Text(_) => None,
        },
        _ => None,
    }
}

fn complete_call(
    plan: &mut ToolPlanState,
    ordinal: u32,
    snapshot: crate::state::ToolResultSnapshot,
) -> Result<(), HandlerError> {
    let call = plan
        .calls
        .get_mut(ordinal as usize)
        .ok_or_else(|| error("tool call ordinal does not exist"))?;
    call.status = ToolCallStatus::Completed(snapshot);
    Ok(())
}

fn current_plan(state: &BasicCodexState) -> Result<&ToolPlanState, HandlerError> {
    let active = state
        .active_turn
        .as_ref()
        .ok_or_else(|| error("tool plan has no active turn"))?;
    let TurnPhase::ExecutingTools(plan) = &active.phase else {
        return Err(error("active turn does not contain a tool plan"));
    };
    Ok(plan)
}

fn current_plan_mut(state: &mut BasicCodexState) -> Result<&mut ToolPlanState, HandlerError> {
    let active = state
        .active_turn
        .as_mut()
        .ok_or_else(|| error("tool plan has no active turn"))?;
    let TurnPhase::ExecutingTools(plan) = &mut active.phase else {
        return Err(error("active turn does not contain a tool plan"));
    };
    Ok(plan)
}

fn error(message: impl Into<String>) -> HandlerError {
    HandlerError(message.into())
}
