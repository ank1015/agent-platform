use crate::{
    state::{
        ToolCallKind, ToolCallState, ToolCallStatus, ToolInput, ToolPlanState, ToolResultSnapshot,
        ToolScheduling, ToolSegment,
    },
    tool_catalog,
};
use agent_contracts::{
    ContentPart, Message, MessageBase, OperationId, StopReason, ToolResultError, ToolResultOutcome,
};
use serde_json::Value;
use std::{collections::HashSet, fmt};

#[derive(Debug)]
pub(crate) struct ToolPlanError(String);

impl fmt::Display for ToolPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

pub(crate) fn parse(
    assistant_operation_id: OperationId,
    message: &Message,
    stop_reason: StopReason,
) -> Result<Option<ToolPlanState>, ToolPlanError> {
    let Message::Assistant { content, .. } = message else {
        return Err(error("tool-call parsing requires an assistant message"));
    };
    let mut calls = Vec::new();
    let mut call_ids = HashSet::new();
    for item in content {
        let value: Value = serde_json::from_str(item.get())
            .map_err(|failure| error(format!("assistant item is invalid JSON: {failure}")))?;
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            continue;
        };
        let call_kind = match kind {
            "function_call" => ToolCallKind::Function,
            "custom_tool_call" => ToolCallKind::Custom,
            _ => continue,
        };
        let call_id = required_nonempty_string(&value, "call_id")?;
        if !call_ids.insert(call_id.clone()) {
            return Err(error(format!("duplicate tool call ID {call_id}")));
        }
        let name = required_nonempty_string(&value, "name")?;
        let descriptor = tool_catalog::descriptor(&name)
            .ok_or_else(|| error(format!("model requested unknown tool {name}")))?;
        if descriptor.call_kind != call_kind {
            return Err(error(format!(
                "tool {name} was called as {call_kind:?}, but its definition is {:?}",
                descriptor.call_kind
            )));
        }
        let (input, status) = match call_kind {
            ToolCallKind::Function => parse_function_input(&value, &descriptor),
            ToolCallKind::Custom => parse_custom_input(&value),
        };
        calls.push(ToolCallState {
            call_id,
            name,
            ordinal: u32::try_from(calls.len())
                .map_err(|_| error("assistant returned too many tool calls"))?,
            kind: call_kind,
            input,
            scheduling: descriptor.scheduling,
            status,
        });
    }

    if calls.is_empty() {
        return if stop_reason == StopReason::ToolUse {
            Err(error("tool_use response contains no callable items"))
        } else {
            Ok(None)
        };
    }
    if stop_reason != StopReason::ToolUse {
        return Err(error(format!(
            "assistant response contains tool calls with stop reason {stop_reason:?}"
        )));
    }

    let segments = build_segments(&calls);
    Ok(Some(ToolPlanState {
        assistant_operation_id,
        calls,
        segments,
        active_segment: 0,
    }))
}

fn parse_function_input(
    value: &Value,
    descriptor: &tool_catalog::ToolDescriptor,
) -> (ToolInput, ToolCallStatus) {
    let Some(arguments) = value.get("arguments").and_then(Value::as_str) else {
        return invalid_function_input(Value::Null, "function arguments must be a JSON string");
    };
    let parsed = match serde_json::from_str(arguments) {
        Ok(parsed) => parsed,
        Err(failure) => {
            return invalid_function_input(
                Value::String(arguments.into()),
                format!("function arguments are invalid JSON: {failure}"),
            );
        }
    };
    let schema = Value::Object(
        tool_catalog::parameters(descriptor)
            .expect("function descriptor has parameters")
            .clone(),
    );
    let validator = jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .build(&schema)
        .expect("built-in tool schema compiles");
    let validation_failure = validator
        .iter_errors(&parsed)
        .next()
        .map(|failure| failure.to_string());
    if let Some(failure) = validation_failure {
        return invalid_function_input(
            parsed,
            format!("function arguments do not match the tool schema: {failure}"),
        );
    }
    (ToolInput::Json(parsed), ToolCallStatus::Pending)
}

fn invalid_function_input(input: Value, message: impl Into<String>) -> (ToolInput, ToolCallStatus) {
    (
        ToolInput::Json(input),
        ToolCallStatus::Completed(error_snapshot(message)),
    )
}

fn parse_custom_input(value: &Value) -> (ToolInput, ToolCallStatus) {
    match value.get("input").and_then(Value::as_str) {
        Some(input) if !input.is_empty() => {
            (ToolInput::Text(input.into()), ToolCallStatus::Pending)
        }
        Some(_) => (
            ToolInput::Text(String::new()),
            ToolCallStatus::Completed(error_snapshot("custom tool input must not be empty")),
        ),
        None => (
            ToolInput::Text(String::new()),
            ToolCallStatus::Completed(error_snapshot("custom tool input must be a string")),
        ),
    }
}

fn error_snapshot(message: impl Into<String>) -> ToolResultSnapshot {
    let message = message.into();
    ToolResultSnapshot {
        content: vec![ContentPart::Text {
            text: message.clone(),
            metadata: None,
        }],
        outcome: ToolResultOutcome::Error {
            error: ToolResultError {
                message,
                name: Some("invalid_tool_arguments".into()),
            },
        },
        details: None,
    }
}

fn required_nonempty_string(value: &Value, field: &str) -> Result<String, ToolPlanError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| error(format!("tool call requires a nonempty {field}")))
}

fn build_segments(calls: &[ToolCallState]) -> Vec<ToolSegment> {
    let mut segments: Vec<ToolSegment> = Vec::new();
    for call in calls
        .iter()
        .filter(|call| matches!(call.status, ToolCallStatus::Pending))
    {
        match call.scheduling {
            ToolScheduling::ParallelSafe => {
                if let Some(segment) = segments.last_mut()
                    && segment.scheduling == ToolScheduling::ParallelSafe
                {
                    segment.call_ordinals.push(call.ordinal);
                    continue;
                }
                segments.push(ToolSegment {
                    scheduling: ToolScheduling::ParallelSafe,
                    call_ordinals: vec![call.ordinal],
                });
            }
            ToolScheduling::Exclusive => segments.push(ToolSegment {
                scheduling: ToolScheduling::Exclusive,
                call_ordinals: vec![call.ordinal],
            }),
        }
    }
    segments
}

impl ToolPlanState {
    pub(crate) fn current_segment(&self) -> Option<&ToolSegment> {
        self.segments.get(self.active_segment)
    }

    pub(crate) fn advance_completed_segments(&mut self) {
        while let Some(segment) = self.segments.get(self.active_segment) {
            let complete = segment.call_ordinals.iter().all(|ordinal| {
                self.calls
                    .get(*ordinal as usize)
                    .is_some_and(|call| matches!(call.status, ToolCallStatus::Completed(_)))
            });
            if !complete {
                break;
            }
            self.active_segment += 1;
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.calls
            .iter()
            .all(|call| matches!(call.status, ToolCallStatus::Completed(_)))
    }

    pub(crate) fn ordered_result_messages(&self) -> Result<Vec<Message>, ToolPlanError> {
        if !self.is_complete() {
            return Err(error("tool results requested before the plan completed"));
        }
        self.calls
            .iter()
            .map(|call| {
                let ToolCallStatus::Completed(snapshot) = &call.status else {
                    unreachable!("completion was checked above")
                };
                let details = snapshot
                    .details
                    .as_ref()
                    .map(serde_json::value::to_raw_value)
                    .transpose()
                    .map_err(|failure| {
                        error(format!(
                            "could not serialize tool result details: {failure}"
                        ))
                    })?;
                Ok(Message::ToolResult {
                    base: MessageBase::default(),
                    tool_name: call.name.clone(),
                    tool_call_id: call.call_id.clone(),
                    content: snapshot.content.clone(),
                    outcome: snapshot.outcome.clone(),
                    details,
                })
            })
            .collect()
    }
}

fn error(message: impl Into<String>) -> ToolPlanError {
    ToolPlanError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_contracts::Provider;
    use serde_json::{json, value::to_raw_value};

    fn assistant(items: Vec<Value>) -> Message {
        Message::Assistant {
            base: MessageBase::default(),
            provider: Provider::Openai,
            content: items
                .into_iter()
                .map(|item| to_raw_value(&item).unwrap())
                .collect(),
        }
    }

    fn success(text: &str) -> ToolResultSnapshot {
        ToolResultSnapshot {
            content: vec![ContentPart::Text {
                text: text.into(),
                metadata: None,
            }],
            outcome: ToolResultOutcome::Success,
            details: None,
        }
    }

    #[test]
    fn parses_mixed_calls_and_builds_ordered_segments() {
        let message = assistant(vec![
            json!({"type":"reasoning","encrypted_content":"opaque"}),
            json!({"type":"function_call","call_id":"c1","name":"exec_command","arguments":"{\"cmd\":\"pwd\"}"}),
            json!({"type":"function_call","call_id":"c2","name":"view_image","arguments":"{\"path\":\"/tmp/a.png\"}"}),
            json!({"type":"custom_tool_call","call_id":"c3","name":"apply_patch","input":"*** Begin Patch\n*** Delete File: old\n*** End Patch"}),
            json!({"type":"function_call","call_id":"c4","name":"write_stdin","arguments":"{\"session_id\":7}"}),
        ]);
        let plan = parse(OperationId::new(), &message, StopReason::ToolUse)
            .unwrap()
            .unwrap();
        assert_eq!(plan.calls.len(), 4);
        assert_eq!(plan.calls[0].ordinal, 0);
        assert_eq!(plan.calls[2].kind, ToolCallKind::Custom);
        assert_eq!(plan.segments.len(), 3);
        assert_eq!(plan.segments[0].scheduling, ToolScheduling::ParallelSafe);
        assert_eq!(plan.segments[0].call_ordinals, [0, 1]);
        assert_eq!(plan.segments[1].scheduling, ToolScheduling::Exclusive);
        assert_eq!(plan.segments[1].call_ordinals, [2]);
        assert_eq!(plan.segments[2].call_ordinals, [3]);
    }

    #[test]
    fn rejects_fatal_correlation_and_stop_reason_errors() {
        let duplicate = assistant(vec![
            json!({"type":"function_call","call_id":"same","name":"exec_command","arguments":"{\"cmd\":\"one\"}"}),
            json!({"type":"function_call","call_id":"same","name":"view_image","arguments":"{\"path\":\"/tmp/a.png\"}"}),
        ]);
        assert!(
            parse(OperationId::new(), &duplicate, StopReason::ToolUse)
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
        let call = assistant(vec![json!({
            "type":"function_call", "call_id":"c1", "name":"exec_command",
            "arguments":"{\"cmd\":\"pwd\"}"
        })]);
        assert!(
            parse(OperationId::new(), &call, StopReason::Stop)
                .unwrap_err()
                .to_string()
                .contains("stop reason")
        );
        let no_call = assistant(vec![
            json!({"type":"message","role":"assistant","content":[]}),
        ]);
        assert!(
            parse(OperationId::new(), &no_call, StopReason::ToolUse)
                .unwrap_err()
                .to_string()
                .contains("no callable")
        );
    }

    #[test]
    fn invalid_known_arguments_become_ordered_tool_errors() {
        let message = assistant(vec![
            json!({"type":"function_call","call_id":"bad","name":"exec_command","arguments":"{}"}),
            json!({"type":"function_call","call_id":"good","name":"view_image","arguments":"{\"path\":\"/tmp/a.png\"}"}),
        ]);
        let plan = parse(OperationId::new(), &message, StopReason::ToolUse)
            .unwrap()
            .unwrap();
        assert!(matches!(plan.calls[0].status, ToolCallStatus::Completed(_)));
        assert!(matches!(plan.calls[1].status, ToolCallStatus::Pending));
        assert_eq!(plan.segments.len(), 1);
        assert_eq!(plan.segments[0].call_ordinals, [1]);
    }

    #[test]
    fn buffers_parallel_results_and_emits_original_call_order() {
        let message = assistant(vec![
            json!({"type":"function_call","call_id":"first","name":"exec_command","arguments":"{\"cmd\":\"one\"}"}),
            json!({"type":"function_call","call_id":"second","name":"view_image","arguments":"{\"path\":\"/tmp/a.png\"}"}),
            json!({"type":"custom_tool_call","call_id":"third","name":"apply_patch","input":"patch"}),
        ]);
        let mut plan = parse(OperationId::new(), &message, StopReason::ToolUse)
            .unwrap()
            .unwrap();
        plan.calls[1].status = ToolCallStatus::Completed(success("result two"));
        plan.advance_completed_segments();
        assert_eq!(plan.active_segment, 0);
        plan.calls[0].status = ToolCallStatus::Completed(success("result one"));
        plan.advance_completed_segments();
        assert_eq!(plan.active_segment, 1);
        plan.calls[2].status = ToolCallStatus::Completed(success("result three"));
        plan.advance_completed_segments();
        assert!(plan.is_complete());
        let messages = plan.ordered_result_messages().unwrap();
        let ids = messages
            .iter()
            .map(|message| match message {
                Message::ToolResult { tool_call_id, .. } => tool_call_id.as_str(),
                _ => panic!("expected tool result"),
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, ["first", "second", "third"]);
    }
}
