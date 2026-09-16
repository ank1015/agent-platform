use agent_contracts::{ContentPart, OperationId, ToolResultOutcome, execution_core};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolPlanState {
    pub(crate) assistant_operation_id: OperationId,
    pub(crate) calls: Vec<ToolCallState>,
    pub(crate) segments: Vec<ToolSegment>,
    pub(crate) active_segment: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolCallState {
    pub(crate) call_id: String,
    pub(crate) name: String,
    pub(crate) ordinal: u32,
    pub(crate) kind: ToolCallKind,
    pub(crate) input: ToolInput,
    pub(crate) scheduling: ToolScheduling,
    pub(crate) status: ToolCallStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolCallKind {
    Function,
    Custom,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum ToolInput {
    Json(Value),
    Text(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolScheduling {
    ParallelSafe,
    Exclusive,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub(crate) enum ToolCallStatus {
    Pending,
    Ready(ToolCallProgress),
    Submitted {
        operation_id: OperationId,
        progress: ToolCallProgress,
    },
    Completed(ToolResultSnapshot),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub(crate) enum ToolCallProgress {
    Shell(Box<ShellCallState>),
    Filesystem(FileToolState),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "tool", content = "value", rename_all = "snake_case")]
pub(crate) enum FileToolState {
    ViewImage(ViewImageState),
    ApplyPatch(ApplyPatchState),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ImageDetailChoice {
    High,
    Original,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ViewImageState {
    pub(crate) path: String,
    pub(crate) detail: ImageDetailChoice,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ApplyPatchPhase {
    Reading,
    Mutating,
    Reconciling,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplyPatchState {
    pub(crate) phase: ApplyPatchPhase,
    pub(crate) patch: ParsedPatch,
    #[serde(default)]
    pub(crate) generation_id: Option<Uuid>,
    #[serde(default)]
    pub(crate) mutations: Vec<PatchMutation>,
    #[serde(default)]
    pub(crate) summary: Vec<PatchSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ParsedPatch {
    pub(crate) hunks: Vec<PatchHunk>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PatchHunk {
    Add {
        path: String,
        content: String,
    },
    Delete {
        path: String,
    },
    Update {
        path: String,
        move_path: Option<String>,
        chunks: Vec<PatchChunk>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PatchChunk {
    pub(crate) context: Option<String>,
    pub(crate) old_lines: Vec<String>,
    pub(crate) new_lines: Vec<String>,
    pub(crate) end_of_file: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum PatchMutation {
    Write {
        mutation_id: String,
        path: String,
        data_base64: String,
        desired_sha256: String,
        expected_sha256: Option<String>,
    },
    Remove {
        mutation_id: String,
        path: String,
        expected_sha256: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PatchSummary {
    pub(crate) action: String,
    pub(crate) path: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShellCallPhase {
    Start,
    Interact,
    Drain,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShellCallState {
    pub(crate) phase: ShellCallPhase,
    pub(crate) started_at_ms: i64,
    pub(crate) chunk_id: String,
    pub(crate) output: OutputAccumulator,
    pub(crate) handle: Option<execution_core::ExecutionHandle>,
    pub(crate) cursor: Option<String>,
    pub(crate) session_id: Option<i32>,
    pub(crate) tty: bool,
    pub(crate) max_output_tokens: usize,
    pub(crate) last_execution: Option<execution_core::Execution>,
    pub(crate) output_gap: bool,
    #[serde(default)]
    pub(crate) pages_read: u16,
    #[serde(default)]
    pub(crate) more_output: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputAccumulator {
    pub(crate) head: String,
    pub(crate) tail: String,
    pub(crate) total_bytes: u64,
    pub(crate) omitted_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolResultSnapshot {
    pub(crate) content: Vec<ContentPart>,
    pub(crate) outcome: ToolResultOutcome,
    pub(crate) details: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolSegment {
    pub(crate) scheduling: ToolScheduling,
    pub(crate) call_ordinals: Vec<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrackedExecution {
    pub(crate) session_id: i32,
    pub(crate) handle: execution_core::ExecutionHandle,
    pub(crate) cursor: String,
    pub(crate) owner_turn_id: Uuid,
    pub(crate) tty: bool,
}
