use crate::{
    BasicCodexConfig,
    state::{
        BasicCodexState, OutputAccumulator, ShellCallPhase, ShellCallState, ToolCallState,
        ToolInput, ToolResultSnapshot, TrackedExecution,
    },
};
use agent_contracts::{
    ContentPart, SessionId, ToolResultError, ToolResultOutcome,
    execution::{self, BatchMode, Operation as ExecutionOperation, Payload},
    execution_core::{self, Command, ExecutionResult, ExecutionState, IoMode, Shell, ShellKind},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf};
use uuid::Uuid;

const DEFAULT_EXEC_YIELD_MS: u64 = 10_000;
const DEFAULT_WRITE_YIELD_MS: u64 = 250;
const MIN_YIELD_MS: u64 = 250;
const MAX_FOREGROUND_YIELD_MS: u64 = 30_000;
const MIN_EMPTY_POLL_MS: u64 = 5_000;
const MAX_EMPTY_POLL_MS: u64 = 300_000;
const DEFAULT_MAX_OUTPUT_TOKENS: usize = 10_000;
const MAX_MODEL_OUTPUT_TOKENS: usize = 10_000;
const OUTPUT_PAGE_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_PAGES_PER_CALL: u16 = 16;
const ACCUMULATOR_HALF_BYTES: usize = 24 * 1024;
const CTRL_C: &str = "\u{3}";

#[derive(Debug)]
pub(crate) struct PreparedShellOperation {
    pub(crate) request: Payload,
    pub(crate) expected_generation_id: Option<Uuid>,
    pub(crate) progress: ShellCallState,
}

#[derive(Debug)]
pub(crate) enum AppliedShellResult {
    Ready(Box<ShellCallState>),
    Completed(ToolResultSnapshot),
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct ShellToolError(String);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecCommandArgs {
    cmd: String,
    #[serde(default)]
    workdir: Option<String>,
    #[serde(default)]
    tty: bool,
    #[serde(default = "default_exec_yield_ms")]
    yield_time_ms: u64,
    #[serde(default)]
    max_output_tokens: Option<usize>,
    #[serde(default)]
    shell: Option<String>,
    #[serde(default = "default_login")]
    login: bool,
    #[serde(default)]
    sandbox_permissions: Option<String>,
    #[serde(default)]
    justification: Option<String>,
    #[serde(default)]
    prefix_rule: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteStdinArgs {
    session_id: i32,
    #[serde(default)]
    chars: String,
    #[serde(default = "default_write_yield_ms")]
    yield_time_ms: u64,
    #[serde(default)]
    max_output_tokens: Option<usize>,
}

#[derive(Deserialize)]
struct ObservationWire {
    execution: execution_core::Execution,
    output: Vec<OutputWire>,
    next_cursor: String,
    has_more: bool,
    output_gap: bool,
    return_reason: execution_core::ReturnReason,
}

#[derive(Deserialize)]
struct OutputWire {
    data_base64: String,
}

#[derive(Deserialize)]
struct BatchResultWire {
    results: Vec<execution::OperationResponse>,
}

pub(crate) fn prepare_new(
    call: &ToolCallState,
    config: &BasicCodexConfig,
    state: &BasicCodexState,
    platform_session_id: SessionId,
    turn_id: Uuid,
    started_at_ms: i64,
) -> Result<Option<PreparedShellOperation>, ToolResultSnapshot> {
    match call.name.as_str() {
        "exec_command" => prepare_exec(call, config, platform_session_id, turn_id, started_at_ms)
            .map(Some)
            .map_err(|error| tool_error("invalid_tool_arguments", error.to_string())),
        "write_stdin" => prepare_write(call, state, turn_id, started_at_ms)
            .map(Some)
            .map_err(|error| tool_error("invalid_tool_arguments", error.to_string())),
        _ => Ok(None),
    }
}

pub(crate) fn prepare_ready(
    progress: ShellCallState,
) -> Result<PreparedShellOperation, ToolResultSnapshot> {
    let Some(handle) = progress.handle else {
        return Err(tool_error(
            "invalid_execution_state",
            "shell output draining has no execution handle",
        ));
    };
    let request = Payload::Single(ExecutionOperation::Observe(execution::ObserveParams {
        handle,
        after_cursor: progress.cursor.clone(),
        wait_ms: 0,
        return_when: execution::WaitMode::FinishedOrTimeout,
        max_output_bytes: Some(OUTPUT_PAGE_BYTES),
    }));
    Ok(PreparedShellOperation {
        request,
        expected_generation_id: Some(handle.generation_id),
        progress,
    })
}

fn prepare_exec(
    call: &ToolCallState,
    config: &BasicCodexConfig,
    platform_session_id: SessionId,
    turn_id: Uuid,
    started_at_ms: i64,
) -> Result<PreparedShellOperation, ShellToolError> {
    let args: ExecCommandArgs = serde_json::from_value(json_input(call)?)
        .map_err(|error| invalid(format!("invalid exec_command arguments: {error}")))?;
    if args.cmd.is_empty() {
        return Err(invalid("cmd must not be empty"));
    }
    let cwd = resolve_workdir(config.cwd.as_str(), args.workdir.as_deref())?;
    let shell = args.shell.as_deref().map(parse_shell).transpose()?;
    // These fields are accepted for wire compatibility. The basic harness is
    // intentionally unrestricted, so no permission transition is performed.
    let _permission_compatibility = (
        args.sandbox_permissions,
        args.justification,
        args.prefix_rule,
    );
    let mut labels = BTreeMap::new();
    labels.insert("agent_session_id".into(), platform_session_id.to_string());
    labels.insert("agent_turn_id".into(), turn_id.to_string());
    let max_output_tokens = output_token_budget(args.max_output_tokens);
    let request = Payload::Single(ExecutionOperation::Start(execution::StartParams {
        start_id: stable_action_id(turn_id, &call.call_id, "start"),
        command: Command::Shell {
            script: args.cmd,
            shell,
            login: args.login,
        },
        cwd: Some(PathBuf::from(cwd)),
        env: BTreeMap::new(),
        shell_snapshot: Some(execution_core::ShellSnapshotRequest {
            scope_id: platform_session_id.to_string(),
        }),
        io: if args.tty {
            IoMode::Pty { rows: 24, cols: 80 }
        } else {
            IoMode::Pipes { stdin: false }
        },
        wait_ms: args
            .yield_time_ms
            .clamp(MIN_YIELD_MS, MAX_FOREGROUND_YIELD_MS),
        max_output_bytes: Some(OUTPUT_PAGE_BYTES),
        labels,
    }));
    Ok(PreparedShellOperation {
        request,
        expected_generation_id: None,
        progress: ShellCallState {
            phase: ShellCallPhase::Start,
            started_at_ms,
            chunk_id: chunk_id(),
            output: OutputAccumulator::default(),
            handle: None,
            cursor: None,
            session_id: None,
            tty: args.tty,
            max_output_tokens,
            last_execution: None,
            output_gap: false,
            pages_read: 0,
            more_output: false,
        },
    })
}

fn prepare_write(
    call: &ToolCallState,
    state: &BasicCodexState,
    turn_id: Uuid,
    started_at_ms: i64,
) -> Result<PreparedShellOperation, ShellToolError> {
    let args: WriteStdinArgs = serde_json::from_value(json_input(call)?)
        .map_err(|error| invalid(format!("invalid write_stdin arguments: {error}")))?;
    let tracked = state
        .executions
        .iter()
        .find(|execution| execution.session_id == args.session_id)
        .ok_or_else(|| invalid(format!("unknown session ID {}", args.session_id)))?;
    let observe = ExecutionOperation::Observe(execution::ObserveParams {
        handle: tracked.handle,
        after_cursor: Some(tracked.cursor.clone()),
        wait_ms: if args.chars.is_empty() {
            args.yield_time_ms
                .clamp(MIN_EMPTY_POLL_MS, MAX_EMPTY_POLL_MS)
        } else {
            args.yield_time_ms
                .clamp(MIN_YIELD_MS, MAX_FOREGROUND_YIELD_MS)
        },
        return_when: execution::WaitMode::FinishedOrTimeout,
        max_output_bytes: Some(OUTPUT_PAGE_BYTES),
    });
    let request = if args.chars.is_empty() {
        Payload::Single(observe)
    } else {
        let interaction = if args.chars == CTRL_C && !tracked.tty {
            ExecutionOperation::Interrupt {
                handle: tracked.handle,
                operation_id: stable_action_id(turn_id, &call.call_id, "interrupt"),
            }
        } else {
            ExecutionOperation::WriteInput {
                handle: tracked.handle,
                input_id: stable_action_id(turn_id, &call.call_id, "input"),
                data_base64: STANDARD.encode(args.chars.as_bytes()),
            }
        };
        Payload::Batch(execution::Batch {
            mode: BatchMode::Sequential,
            accepted_error_codes: Vec::new(),
            operations: vec![
                execution::BatchOperation {
                    request_id: "interaction".into(),
                    operation: interaction,
                },
                execution::BatchOperation {
                    request_id: "observe".into(),
                    operation: observe,
                },
            ],
        })
    };
    Ok(PreparedShellOperation {
        request,
        expected_generation_id: Some(tracked.handle.generation_id),
        progress: ShellCallState {
            phase: ShellCallPhase::Interact,
            started_at_ms,
            chunk_id: chunk_id(),
            output: OutputAccumulator::default(),
            handle: Some(tracked.handle),
            cursor: Some(tracked.cursor.clone()),
            session_id: Some(tracked.session_id),
            tty: tracked.tty,
            max_output_tokens: output_token_budget(args.max_output_tokens),
            last_execution: None,
            output_gap: false,
            pages_read: 0,
            more_output: false,
        },
    })
}

pub(crate) fn apply_response(
    mut progress: ShellCallState,
    response: execution::Response,
    state: &mut BasicCodexState,
    turn_id: Uuid,
    completed_at_ms: i64,
) -> Result<AppliedShellResult, ToolResultSnapshot> {
    let response_generation = response.generation_id;
    let observation = decode_observation(response, progress.phase)?;
    for chunk in &observation.output {
        let bytes = STANDARD.decode(&chunk.data_base64).map_err(|error| {
            tool_error(
                "invalid_execution_response",
                format!("execution output is not valid base64: {error}"),
            )
        })?;
        progress.output.append(&bytes);
    }
    progress.output_gap |= observation.output_gap;
    progress.handle = Some(observation.execution.handle);
    progress.cursor = Some(observation.next_cursor.clone());
    progress.last_execution = Some(observation.execution.clone());
    progress.pages_read = progress.pages_read.saturating_add(1);
    progress.more_output = observation.has_more;

    if observation.execution.handle.generation_id != response_generation {
        return Err(tool_error(
            "invalid_execution_response",
            "execution handle contains an inconsistent runtime generation",
        ));
    }

    let active = observation.execution.state != ExecutionState::Finished;
    if active {
        let session_id = match progress.session_id {
            Some(session_id) => session_id,
            None => allocate_session_id(state)
                .map_err(|error| tool_error("execution_session_limit", error.to_string()))?,
        };
        progress.session_id = Some(session_id);
        upsert_execution(
            state,
            TrackedExecution {
                session_id,
                handle: observation.execution.handle,
                cursor: observation.next_cursor.clone(),
                owner_turn_id: turn_id,
                tty: progress.tty,
            },
        );
    } else if let Some(session_id) = progress.session_id {
        if observation.has_more {
            if let Some(tracked) = state
                .executions
                .iter_mut()
                .find(|execution| execution.session_id == session_id)
            {
                tracked.cursor = observation.next_cursor.clone();
            }
        } else {
            state
                .executions
                .retain(|execution| execution.session_id != session_id);
        }
    }

    if observation.has_more && progress.pages_read < MAX_OUTPUT_PAGES_PER_CALL {
        progress.phase = ShellCallPhase::Drain;
        return Ok(AppliedShellResult::Ready(Box::new(progress)));
    }

    Ok(AppliedShellResult::Completed(format_result(
        &progress,
        &observation,
        completed_at_ms,
    )))
}

fn decode_observation(
    response: execution::Response,
    phase: ShellCallPhase,
) -> Result<ObservationWire, ToolResultSnapshot> {
    let result = match response.outcome {
        execution::Outcome::Ok { result } => result,
        execution::Outcome::Error { error } => {
            return Err(tool_error(
                &format!("execution_{:?}", error.code).to_lowercase(),
                error.message,
            ));
        }
    };
    let value = if phase == ShellCallPhase::Interact && result.get("results").is_some() {
        let batch: BatchResultWire = serde_json::from_value(result).map_err(|error| {
            tool_error(
                "invalid_execution_response",
                format!("invalid execution batch response: {error}"),
            )
        })?;
        let interaction_error = batch.results.iter().find_map(|item| match &item.outcome {
            execution::OperationOutcome::Error { error } => Some(error.message.clone()),
            execution::OperationOutcome::Skipped { reason } if item.request_id != "observe" => {
                Some(reason.clone())
            }
            _ => None,
        });
        let observe = batch
            .results
            .into_iter()
            .find(|item| item.request_id == "observe")
            .ok_or_else(|| {
                tool_error(
                    "invalid_execution_response",
                    "execution batch has no observe result",
                )
            })?;
        match observe.outcome {
            execution::OperationOutcome::Ok { result } => result,
            execution::OperationOutcome::Error { error } => {
                return Err(tool_error(
                    &format!("execution_{:?}", error.code).to_lowercase(),
                    error.message,
                ));
            }
            execution::OperationOutcome::Skipped { reason } => {
                let interaction = interaction_error.unwrap_or(reason);
                return Err(tool_error("execution_interaction_failed", interaction));
            }
        }
    } else {
        result
    };
    serde_json::from_value(value).map_err(|error| {
        tool_error(
            "invalid_execution_response",
            format!("invalid execution observation: {error}"),
        )
    })
}

fn format_result(
    progress: &ShellCallState,
    observation: &ObservationWire,
    completed_at_ms: i64,
) -> ToolResultSnapshot {
    let execution = &observation.execution;
    if let Some(ExecutionResult::StartFailed { message }) = &execution.result {
        return tool_error("execution_start_failed", message.clone());
    }
    if let Some(ExecutionResult::Lost { message }) = &execution.result {
        return tool_error("execution_lost", message.clone());
    }

    let elapsed_ms = completed_at_ms
        .saturating_sub(progress.started_at_ms)
        .max(0);
    let mut header = vec![
        format!("Chunk ID: {}", progress.chunk_id),
        format!("Wall time: {:.4} seconds", elapsed_ms as f64 / 1000.0),
    ];
    if execution.state == ExecutionState::Finished {
        match execution.result.as_ref() {
            Some(ExecutionResult::Exited { exit_code, signal })
            | Some(ExecutionResult::Terminated { exit_code, signal }) => {
                if let Some(exit_code) = exit_code {
                    header.push(format!("Process exited with code {exit_code}"));
                } else if let Some(signal) = signal {
                    header.push(format!("Process exited from signal {signal}"));
                } else {
                    header.push("Process exited without an exit code".into());
                }
            }
            _ => {}
        }
    } else if let Some(session_id) = progress.session_id {
        header.push(format!("Process running with session ID {session_id}"));
    }
    header.push(format!(
        "Original token count: {}",
        progress.output.total_bytes.div_ceil(4)
    ));
    header.push("Output:".into());

    let mut body = progress.output.model_text(progress.max_output_tokens);
    if progress.output_gap {
        body = format!(
            "Warning: earlier command output was no longer retained by the execution runtime.\n{body}"
        );
    }
    if execution.output_incomplete {
        body =
            format!("Warning: the execution runtime could not capture all command output.\n{body}");
    }
    if progress.more_output {
        body = format!(
            "Warning: more command output remains; call write_stdin again to continue reading.\n{body}"
        );
    }
    let text = format!("{}\n{}", header.join("\n"), body);
    ToolResultSnapshot {
        content: vec![ContentPart::Text {
            text,
            metadata: None,
        }],
        outcome: ToolResultOutcome::Success,
        details: Some(json!({
            "execution": execution,
            "next_cursor": observation.next_cursor,
            "return_reason": observation.return_reason,
            "output_gap": progress.output_gap,
            "has_more": observation.has_more,
            "pages_read": progress.pages_read,
        })),
    }
}

pub(crate) fn tool_error(name: &str, message: impl Into<String>) -> ToolResultSnapshot {
    let message = message.into();
    ToolResultSnapshot {
        content: vec![ContentPart::Text {
            text: message.clone(),
            metadata: None,
        }],
        outcome: ToolResultOutcome::Error {
            error: ToolResultError {
                message,
                name: Some(name.into()),
            },
        },
        details: None,
    }
}

pub(crate) fn tool_error_with_details(
    name: &str,
    message: impl Into<String>,
    details: Value,
) -> ToolResultSnapshot {
    let mut snapshot = tool_error(name, message);
    snapshot.details = Some(details);
    snapshot
}

fn json_input(call: &ToolCallState) -> Result<Value, ShellToolError> {
    match &call.input {
        ToolInput::Json(value) => Ok(value.clone()),
        ToolInput::Text(_) => Err(invalid("shell tool requires JSON arguments")),
    }
}

fn resolve_workdir(base: &str, requested: Option<&str>) -> Result<String, ShellToolError> {
    let Some(requested) = requested.filter(|value| !value.is_empty()) else {
        return Ok(base.into());
    };
    if requested.contains('\0') {
        return Err(invalid("workdir contains a NUL byte"));
    }
    if is_absolute(requested) {
        return Ok(requested.into());
    }
    let separator = if base.contains('\\') && !base.contains('/') {
        '\\'
    } else {
        '/'
    };
    Ok(format!(
        "{}{}{}",
        base.trim_end_matches(['/', '\\']),
        separator,
        requested
    ))
}

fn is_absolute(path: &str) -> bool {
    if path.starts_with('/') || path.starts_with(r"\\") {
        return true;
    }
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

fn parse_shell(value: &str) -> Result<Shell, ShellToolError> {
    let filename = value
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(value)
        .to_ascii_lowercase();
    let kind = match filename.as_str() {
        "sh" => ShellKind::Sh,
        "bash" => ShellKind::Bash,
        "zsh" => ShellKind::Zsh,
        "pwsh" | "pwsh.exe" | "powershell" | "powershell.exe" => ShellKind::PowerShell,
        "cmd" | "cmd.exe" => ShellKind::Cmd,
        _ => return Err(invalid(format!("unsupported shell {value}"))),
    };
    Ok(Shell {
        executable: PathBuf::from(value),
        kind,
    })
}

fn stable_action_id(turn_id: Uuid, call_id: &str, action: &str) -> String {
    let call_hash = format!("{:x}", Sha256::digest(call_id.as_bytes()));
    format!("basic-codex:{turn_id}:{call_hash}:{action}")
}

fn chunk_id() -> String {
    Uuid::new_v4().simple().to_string()[..6].to_owned()
}

fn output_token_budget(requested: Option<usize>) -> usize {
    requested
        .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS)
        .min(MAX_MODEL_OUTPUT_TOKENS)
}

const fn default_exec_yield_ms() -> u64 {
    DEFAULT_EXEC_YIELD_MS
}

const fn default_write_yield_ms() -> u64 {
    DEFAULT_WRITE_YIELD_MS
}

const fn default_login() -> bool {
    true
}

fn allocate_session_id(state: &mut BasicCodexState) -> Result<i32, ShellToolError> {
    let id = state.next_execution_session_id;
    state.next_execution_session_id = id
        .checked_add(1)
        .ok_or_else(|| invalid("execution session ID space exhausted"))?;
    Ok(id)
}

fn upsert_execution(state: &mut BasicCodexState, tracked: TrackedExecution) {
    if let Some(existing) = state
        .executions
        .iter_mut()
        .find(|execution| execution.session_id == tracked.session_id)
    {
        *existing = tracked;
    } else {
        state.executions.push(tracked);
    }
}

impl OutputAccumulator {
    pub(crate) fn append(&mut self, bytes: &[u8]) {
        self.total_bytes = self.total_bytes.saturating_add(bytes.len() as u64);
        let text = String::from_utf8_lossy(bytes);
        let mut remainder = text.as_ref();
        if self.head.len() < ACCUMULATOR_HALF_BYTES {
            let available = ACCUMULATOR_HALF_BYTES - self.head.len();
            let split = floor_char_boundary(remainder, available.min(remainder.len()));
            self.head.push_str(&remainder[..split]);
            remainder = &remainder[split..];
        }
        if !remainder.is_empty() {
            self.tail.push_str(remainder);
            if self.tail.len() > ACCUMULATOR_HALF_BYTES {
                let discard = self.tail.len() - ACCUMULATOR_HALF_BYTES;
                let boundary = ceil_char_boundary(&self.tail, discard);
                self.omitted_bytes = self.omitted_bytes.saturating_add(boundary as u64);
                self.tail.drain(..boundary);
            }
        }
    }

    pub(crate) fn model_text(&self, max_tokens: usize) -> String {
        let mut text = if self.omitted_bytes == 0 {
            format!("{}{}", self.head, self.tail)
        } else {
            format!(
                "{}\n... {} bytes omitted ...\n{}",
                self.head, self.omitted_bytes, self.tail
            )
        };
        let budget = max_tokens.saturating_mul(4);
        if text.len() <= budget {
            return text;
        }
        if budget == 0 {
            return String::new();
        }
        let marker = format!(
            "\n... model output truncated (original token count: {}) ...\n",
            self.total_bytes.div_ceil(4)
        );
        if marker.len() >= budget {
            text.truncate(floor_char_boundary(&text, budget));
            return text;
        }
        let remaining = budget - marker.len();
        let head_end = floor_char_boundary(&text, remaining / 2);
        let tail_start = ceil_char_boundary(&text, text.len() - (remaining - head_end));
        format!("{}{}{}", &text[..head_end], marker, &text[tail_start..])
    }
}

fn floor_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index > 0 && !value.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_char_boundary(value: &str, mut index: usize) -> usize {
    index = index.min(value.len());
    while index < value.len() && !value.is_char_boundary(index) {
        index += 1;
    }
    index
}

fn invalid(message: impl Into<String>) -> ShellToolError {
    ShellToolError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_accumulator_keeps_a_bounded_head_and_tail() {
        let mut output = OutputAccumulator::default();
        output.append(&vec![b'a'; ACCUMULATOR_HALF_BYTES]);
        output.append(&vec![b'b'; ACCUMULATOR_HALF_BYTES * 2]);
        assert_eq!(output.head.len(), ACCUMULATOR_HALF_BYTES);
        assert_eq!(output.tail.len(), ACCUMULATOR_HALF_BYTES);
        assert_eq!(output.omitted_bytes, ACCUMULATOR_HALF_BYTES as u64);
        assert!(output.model_text(10_000).contains("truncated"));
    }

    #[test]
    fn remote_workdirs_and_shells_do_not_use_the_server_platform() {
        assert_eq!(
            resolve_workdir(r"C:\work", Some("project")).unwrap(),
            r"C:\work\project"
        );
        assert_eq!(
            resolve_workdir("/work", Some("project")).unwrap(),
            "/work/project"
        );
        assert_eq!(
            parse_shell(r"C:\\Windows\\System32\\cmd.exe").unwrap().kind,
            ShellKind::Cmd
        );
    }

    #[test]
    fn action_identity_is_stable_per_model_tool_call() {
        let turn_id = Uuid::new_v4();
        let first = stable_action_id(turn_id, "call-first", "start");
        assert_eq!(first, stable_action_id(turn_id, "call-first", "start"));
        assert_ne!(first, stable_action_id(turn_id, "call-second", "start"));
        assert_ne!(first, stable_action_id(turn_id, "call-first", "input"));
    }
}
