use crate::{
    BasicCodexConfig,
    shell_tools::{tool_error, tool_error_with_details},
    state::{
        ApplyPatchPhase, ApplyPatchState, FileToolState, ImageDetailChoice, ParsedPatch,
        PatchChunk, PatchHunk, PatchMutation, PatchSummary, ToolCallState, ToolInput,
        ToolResultSnapshot, ViewImageState,
    },
};
use agent_contracts::{
    AssetUpload, ContentPart, HarnessContext, ImageDetail, ToolResultOutcome,
    execution::{self, Batch, BatchMode, BatchOperation, Operation as ExecutionOperation, Payload},
    execution_core::{self, ErrorCode, FilePrecondition},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use image::{GenericImageView, ImageFormat, imageops::FilterType};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, io::Cursor, path::PathBuf};
use uuid::Uuid;

const MAX_FILE_BYTES: usize = 5 * 1024 * 1024;
const MAX_PATCH_BYTES: usize = 1024 * 1024;
const MAX_PATCH_PATHS: usize = 32;
const PATCH_SIZE: u64 = 32;

#[derive(Debug)]
pub(crate) struct PreparedFileOperation {
    pub(crate) request: Payload,
    pub(crate) expected_generation_id: Option<Uuid>,
    pub(crate) progress: FileToolState,
}

pub(crate) enum AppliedFileResult {
    Ready(FileToolState),
    Completed(ToolResultSnapshot),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewImageArgs {
    path: String,
    #[serde(default)]
    detail: Option<String>,
}

#[derive(Deserialize)]
struct ReadFileWire {
    path: PathBuf,
    metadata: execution_core::FileMetadata,
    data_base64: String,
    sha256: String,
}

#[derive(Deserialize)]
struct BatchWire {
    results: Vec<execution::OperationResponse>,
}

#[derive(Clone)]
struct FileVersion {
    data: Vec<u8>,
    sha256: String,
}

pub(crate) fn prepare_new(
    call: &ToolCallState,
    config: &BasicCodexConfig,
) -> Result<Option<PreparedFileOperation>, ToolResultSnapshot> {
    match call.name.as_str() {
        "view_image" => prepare_view_image(call, config).map(Some),
        "apply_patch" => prepare_apply_patch(call, config).map(Some),
        _ => Ok(None),
    }
}

pub(crate) fn prepare_ready(
    progress: FileToolState,
    config: &BasicCodexConfig,
) -> Result<PreparedFileOperation, ToolResultSnapshot> {
    let FileToolState::ApplyPatch(state) = &progress else {
        return Err(tool_error(
            "invalid_filesystem_state",
            "view_image cannot require another filesystem operation",
        ));
    };
    let operations = match state.phase {
        ApplyPatchPhase::Mutating => mutation_operations(state, config),
        ApplyPatchPhase::Reconciling => reconciliation_operations(state, config),
        ApplyPatchPhase::Reading => {
            return Err(tool_error(
                "invalid_filesystem_state",
                "initial patch reads were already submitted",
            ));
        }
    }?;
    Ok(PreparedFileOperation {
        request: Payload::Batch(Batch {
            mode: if state.phase == ApplyPatchPhase::Mutating {
                BatchMode::Sequential
            } else {
                BatchMode::Parallel
            },
            operations,
        }),
        expected_generation_id: if state.phase == ApplyPatchPhase::Mutating {
            state.generation_id
        } else {
            None
        },
        progress,
    })
}

pub(crate) async fn apply_response(
    progress: FileToolState,
    response: execution::Response,
    turn_id: Uuid,
    call_id: &str,
    context: &dyn HarnessContext,
) -> Result<AppliedFileResult, ToolResultSnapshot> {
    match progress {
        FileToolState::ViewImage(state) => apply_view_image(state, response, context).await,
        FileToolState::ApplyPatch(state) => apply_patch_response(state, response, turn_id, call_id),
    }
}

pub(crate) fn apply_unknown(
    mut progress: FileToolState,
    message: String,
) -> Result<AppliedFileResult, ToolResultSnapshot> {
    match &mut progress {
        FileToolState::ApplyPatch(state) if state.phase == ApplyPatchPhase::Mutating => {
            state.phase = ApplyPatchPhase::Reconciling;
            Ok(AppliedFileResult::Ready(progress))
        }
        _ => Err(tool_error_with_details(
            "filesystem_unknown",
            format!("filesystem operation outcome is unknown: {message}"),
            json!({"operation_outcome":"unknown"}),
        )),
    }
}

fn prepare_view_image(
    call: &ToolCallState,
    config: &BasicCodexConfig,
) -> Result<PreparedFileOperation, ToolResultSnapshot> {
    let ToolInput::Json(value) = &call.input else {
        return Err(tool_error(
            "invalid_tool_arguments",
            "view_image requires JSON arguments",
        ));
    };
    let args: ViewImageArgs = serde_json::from_value(value.clone()).map_err(|error| {
        tool_error(
            "invalid_tool_arguments",
            format!("invalid view_image arguments: {error}"),
        )
    })?;
    validate_path(&args.path)?;
    let detail = match args.detail.as_deref() {
        None | Some("high") => ImageDetailChoice::High,
        Some("original") => ImageDetailChoice::Original,
        Some(value) => {
            return Err(tool_error(
                "invalid_tool_arguments",
                format!("view_image detail must be `high` or `original`, got `{value}`"),
            ));
        }
    };
    Ok(PreparedFileOperation {
        request: Payload::Single(ExecutionOperation::ReadFile(execution::ReadFileParams {
            cwd: Some(PathBuf::from(config.cwd.as_str())),
            path: PathBuf::from(&args.path),
            max_bytes: Some(MAX_FILE_BYTES),
        })),
        expected_generation_id: None,
        progress: FileToolState::ViewImage(ViewImageState {
            path: args.path,
            detail,
        }),
    })
}

fn prepare_apply_patch(
    call: &ToolCallState,
    config: &BasicCodexConfig,
) -> Result<PreparedFileOperation, ToolResultSnapshot> {
    let ToolInput::Text(input) = &call.input else {
        return Err(tool_error(
            "invalid_tool_arguments",
            "apply_patch requires freeform patch text",
        ));
    };
    if input.len() > MAX_PATCH_BYTES {
        return Err(tool_error(
            "patch_too_large",
            "apply_patch input exceeds the 1 MiB limit",
        ));
    }
    let patch = parse_patch(input)
        .map_err(|message| tool_error("invalid_patch", format!("Invalid patch: {message}")))?;
    let paths = patch_paths(&patch)?;
    let operations = paths
        .iter()
        .enumerate()
        .map(|(index, path)| BatchOperation {
            request_id: format!("read-{index}"),
            operation: ExecutionOperation::ReadFile(execution::ReadFileParams {
                cwd: Some(PathBuf::from(config.cwd.as_str())),
                path: PathBuf::from(path),
                max_bytes: Some(MAX_FILE_BYTES),
            }),
        })
        .collect();
    Ok(PreparedFileOperation {
        request: Payload::Batch(Batch {
            mode: BatchMode::Parallel,
            operations,
        }),
        expected_generation_id: None,
        progress: FileToolState::ApplyPatch(ApplyPatchState {
            phase: ApplyPatchPhase::Reading,
            patch,
            generation_id: None,
            mutations: Vec::new(),
            summary: Vec::new(),
        }),
    })
}

async fn apply_view_image(
    state: ViewImageState,
    response: execution::Response,
    context: &dyn HarnessContext,
) -> Result<AppliedFileResult, ToolResultSnapshot> {
    let value = single_result(response)?;
    let file: ReadFileWire = serde_json::from_value(value).map_err(|error| {
        tool_error(
            "invalid_execution_response",
            format!("invalid filesystem read response: {error}"),
        )
    })?;
    if !file.metadata.is_file {
        return Err(tool_error("invalid_image", "image path is not a file"));
    }
    let bytes = STANDARD.decode(&file.data_base64).map_err(|error| {
        tool_error(
            "invalid_execution_response",
            format!("image bytes are not valid base64: {error}"),
        )
    })?;
    let prepared = prepare_image(&bytes, state.detail).map_err(|message| {
        tool_error(
            "invalid_image",
            format!("unable to process image: {message}"),
        )
    })?;
    let detail = match state.detail {
        ImageDetailChoice::High => ImageDetail::High,
        ImageDetailChoice::Original => ImageDetail::Original,
    };
    let published = context
        .publish_asset(AssetUpload {
            content_type: prepared.mime.into(),
            bytes: prepared.bytes.clone().into(),
        })
        .await
        .map_err(|message| {
            tool_error(
                "image_upload_failed",
                format!("unable to publish image for model input: {message}"),
            )
        })?;
    if !published.url.starts_with("http://") && !published.url.starts_with("https://") {
        return Err(tool_error(
            "image_upload_failed",
            "image bucket returned a non-HTTP URL",
        ));
    }
    Ok(AppliedFileResult::Completed(ToolResultSnapshot {
        content: vec![ContentPart::Image {
            url: published.url,
            detail: Some(detail),
            metadata: None,
        }],
        outcome: ToolResultOutcome::Success,
        details: Some(json!({
            "path": state.path,
            "resolved_path": file.path,
            "sha256": file.sha256,
            "published_sha256": published.sha256,
            "published_size_bytes": published.size_bytes,
            "source_width": prepared.source_width,
            "source_height": prepared.source_height,
            "width": prepared.width,
            "height": prepared.height,
            "detail": match state.detail { ImageDetailChoice::High => "high", ImageDetailChoice::Original => "original" },
        })),
    }))
}

fn apply_patch_response(
    mut state: ApplyPatchState,
    response: execution::Response,
    turn_id: Uuid,
    call_id: &str,
) -> Result<AppliedFileResult, ToolResultSnapshot> {
    match state.phase {
        ApplyPatchPhase::Reading => {
            state.generation_id = Some(response.generation_id);
            let paths = patch_paths(&state.patch)?;
            let reads = decode_read_batch(response, &paths)?;
            let (mutations, summary) = plan_mutations(&state.patch, reads, turn_id, call_id)?;
            if mutations.is_empty() {
                return Err(tool_error("empty_patch", "No files were modified."));
            }
            state.phase = ApplyPatchPhase::Mutating;
            state.mutations = mutations;
            state.summary = summary;
            Ok(AppliedFileResult::Ready(FileToolState::ApplyPatch(state)))
        }
        ApplyPatchPhase::Mutating => {
            let values = batch_results(response)?;
            let mut applied = Vec::new();
            for (index, item) in values.into_iter().enumerate() {
                match item.outcome {
                    execution::OperationOutcome::Ok { result } => applied.push(result),
                    execution::OperationOutcome::Error { error } => {
                        return Err(tool_error_with_details(
                            &format!("filesystem_{:?}", error.code).to_lowercase(),
                            format!("apply_patch failed: {}", error.message),
                            json!({"applied":applied,"failed_index":index,"summary":state.summary}),
                        ));
                    }
                    execution::OperationOutcome::Skipped { reason } => {
                        return Err(tool_error_with_details(
                            "patch_partially_applied",
                            format!(
                                "apply_patch stopped after an earlier mutation failed: {reason}"
                            ),
                            json!({"applied":applied,"skipped_index":index,"summary":state.summary}),
                        ));
                    }
                }
            }
            Ok(AppliedFileResult::Completed(patch_success(&state, false)))
        }
        ApplyPatchPhase::Reconciling => {
            let paths = state
                .mutations
                .iter()
                .map(mutation_path)
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let reads = decode_read_batch(response, &paths)?;
            for (mutation, current) in state.mutations.iter().zip(reads) {
                let desired = match mutation {
                    PatchMutation::Write { desired_sha256, .. } => current
                        .as_ref()
                        .is_some_and(|current| current.sha256 == *desired_sha256),
                    PatchMutation::Remove { .. } => current.is_none(),
                };
                if !desired {
                    return Err(tool_error_with_details(
                        "filesystem_unknown",
                        "apply_patch outcome is unknown and the intended final state could not be confirmed",
                        json!({"summary":state.summary}),
                    ));
                }
            }
            Ok(AppliedFileResult::Completed(patch_success(&state, true)))
        }
    }
}

fn patch_success(state: &ApplyPatchState, reconciled: bool) -> ToolResultSnapshot {
    let mut text = "Success. Updated the following files:".to_string();
    for item in &state.summary {
        text.push_str(&format!("\n{} {}", item.action, item.path));
    }
    ToolResultSnapshot {
        content: vec![ContentPart::Text {
            text,
            metadata: None,
        }],
        outcome: ToolResultOutcome::Success,
        details: Some(json!({"changes":state.summary,"reconciled_after_unknown":reconciled})),
    }
}

fn mutation_operations(
    state: &ApplyPatchState,
    config: &BasicCodexConfig,
) -> Result<Vec<BatchOperation>, ToolResultSnapshot> {
    Ok(state
        .mutations
        .iter()
        .enumerate()
        .map(|(index, mutation)| BatchOperation {
            request_id: format!("mutation-{index}"),
            operation: match mutation {
                PatchMutation::Write {
                    mutation_id,
                    path,
                    data_base64,
                    expected_sha256,
                    ..
                } => ExecutionOperation::WriteFile(execution::WriteFileParams {
                    mutation_id: mutation_id.clone(),
                    cwd: Some(PathBuf::from(config.cwd.as_str())),
                    path: PathBuf::from(path),
                    data_base64: data_base64.clone(),
                    create_parent_directories: true,
                    precondition: precondition(expected_sha256.as_deref()),
                }),
                PatchMutation::Remove {
                    mutation_id,
                    path,
                    expected_sha256,
                } => ExecutionOperation::RemoveFile(execution::RemoveFileParams {
                    mutation_id: mutation_id.clone(),
                    cwd: Some(PathBuf::from(config.cwd.as_str())),
                    path: PathBuf::from(path),
                    precondition: FilePrecondition::Sha256 {
                        sha256: expected_sha256.clone(),
                    },
                }),
            },
        })
        .collect())
}

fn reconciliation_operations(
    state: &ApplyPatchState,
    config: &BasicCodexConfig,
) -> Result<Vec<BatchOperation>, ToolResultSnapshot> {
    Ok(state
        .mutations
        .iter()
        .enumerate()
        .map(|(index, mutation)| BatchOperation {
            request_id: format!("read-{index}"),
            operation: ExecutionOperation::ReadFile(execution::ReadFileParams {
                cwd: Some(PathBuf::from(config.cwd.as_str())),
                path: PathBuf::from(mutation_path(mutation)),
                max_bytes: Some(MAX_FILE_BYTES),
            }),
        })
        .collect())
}

fn precondition(expected: Option<&str>) -> FilePrecondition {
    match expected {
        Some(sha256) => FilePrecondition::Sha256 {
            sha256: sha256.into(),
        },
        None => FilePrecondition::Missing,
    }
}

fn mutation_path(mutation: &PatchMutation) -> &str {
    match mutation {
        PatchMutation::Write { path, .. } | PatchMutation::Remove { path, .. } => path,
    }
}

fn decode_read_batch(
    response: execution::Response,
    paths: &[String],
) -> Result<Vec<Option<FileVersion>>, ToolResultSnapshot> {
    let results = batch_results(response)?;
    if results.len() != paths.len() {
        return Err(tool_error(
            "invalid_execution_response",
            "filesystem read batch returned an unexpected number of results",
        ));
    }
    results
        .into_iter()
        .map(|item| match item.outcome {
            execution::OperationOutcome::Ok { result } => {
                let file: ReadFileWire = serde_json::from_value(result).map_err(|error| {
                    tool_error(
                        "invalid_execution_response",
                        format!("invalid filesystem read response: {error}"),
                    )
                })?;
                let data = STANDARD.decode(file.data_base64).map_err(|error| {
                    tool_error(
                        "invalid_execution_response",
                        format!("filesystem file bytes are invalid base64: {error}"),
                    )
                })?;
                Ok(Some(FileVersion {
                    data,
                    sha256: file.sha256,
                }))
            }
            execution::OperationOutcome::Error { error } if error.code == ErrorCode::NotFound => {
                Ok(None)
            }
            execution::OperationOutcome::Error { error } => Err(tool_error(
                &format!("filesystem_{:?}", error.code).to_lowercase(),
                error.message,
            )),
            execution::OperationOutcome::Skipped { reason } => {
                Err(tool_error("filesystem_read_skipped", reason))
            }
        })
        .collect()
}

fn single_result(response: execution::Response) -> Result<Value, ToolResultSnapshot> {
    match response.outcome {
        execution::Outcome::Ok { result } => Ok(result),
        execution::Outcome::Error { error } => Err(tool_error(
            &format!("filesystem_{:?}", error.code).to_lowercase(),
            error.message,
        )),
    }
}

fn batch_results(
    response: execution::Response,
) -> Result<Vec<execution::OperationResponse>, ToolResultSnapshot> {
    let value = single_result(response)?;
    serde_json::from_value::<BatchWire>(value)
        .map(|batch| batch.results)
        .map_err(|error| {
            tool_error(
                "invalid_execution_response",
                format!("invalid filesystem batch response: {error}"),
            )
        })
}

fn patch_paths(patch: &ParsedPatch) -> Result<Vec<String>, ToolResultSnapshot> {
    let mut paths = Vec::new();
    for hunk in &patch.hunks {
        let (path, moved) = match hunk {
            PatchHunk::Add { path, .. } | PatchHunk::Delete { path } => (path, None),
            PatchHunk::Update {
                path, move_path, ..
            } => (path, move_path.as_ref()),
        };
        for path in std::iter::once(path).chain(moved) {
            validate_path(path)?;
            if !paths.contains(path) {
                paths.push(path.clone());
            }
        }
    }
    if paths.len() > MAX_PATCH_PATHS {
        return Err(tool_error(
            "patch_too_large",
            format!("apply_patch affects more than {MAX_PATCH_PATHS} paths"),
        ));
    }
    Ok(paths)
}

fn plan_mutations(
    patch: &ParsedPatch,
    reads: Vec<Option<FileVersion>>,
    turn_id: Uuid,
    call_id: &str,
) -> Result<(Vec<PatchMutation>, Vec<PatchSummary>), ToolResultSnapshot> {
    let paths = patch_paths(patch)?;
    let initial = paths.iter().cloned().zip(reads).collect::<HashMap<_, _>>();
    let mut virtual_files = initial.clone();
    let mut touched = Vec::new();
    let mut summary = Vec::new();

    for hunk in &patch.hunks {
        match hunk {
            PatchHunk::Add { path, content } => {
                virtual_files.insert(path.clone(), Some(version(content.as_bytes().to_vec())));
                touch(&mut touched, path);
                summary.push(PatchSummary {
                    action: "A".into(),
                    path: path.clone(),
                });
            }
            PatchHunk::Delete { path } => {
                require_file(&virtual_files, path)?;
                virtual_files.insert(path.clone(), None);
                touch(&mut touched, path);
                summary.push(PatchSummary {
                    action: "D".into(),
                    path: path.clone(),
                });
            }
            PatchHunk::Update {
                path,
                move_path,
                chunks,
            } => {
                let source = require_file(&virtual_files, path)?;
                let source = String::from_utf8(source.data.clone()).map_err(|_| {
                    tool_error(
                        "invalid_patch_target",
                        format!("Failed to read file to update {path}: file is not valid UTF-8"),
                    )
                })?;
                let updated = apply_chunks(path, &source, chunks)?;
                if let Some(destination) = move_path {
                    virtual_files.insert(path.clone(), None);
                    virtual_files.insert(destination.clone(), Some(version(updated.into_bytes())));
                    touch(&mut touched, destination);
                    touch(&mut touched, path);
                    summary.push(PatchSummary {
                        action: "M".into(),
                        path: format!("{path} -> {destination}"),
                    });
                } else {
                    virtual_files.insert(path.clone(), Some(version(updated.into_bytes())));
                    touch(&mut touched, path);
                    summary.push(PatchSummary {
                        action: "M".into(),
                        path: path.clone(),
                    });
                }
            }
        }
    }

    let mut mutations = Vec::new();
    for path in touched {
        let before = initial.get(&path).cloned().flatten();
        let after = virtual_files.get(&path).cloned().flatten();
        if before.as_ref().map(|file| &file.sha256) == after.as_ref().map(|file| &file.sha256) {
            continue;
        }
        let call_hash = format!("{:x}", Sha256::digest(call_id.as_bytes()));
        let mutation_id = format!("basic-codex:{turn_id}:{call_hash}:file:{}", mutations.len());
        match (before, after) {
            (before, Some(after)) => mutations.push(PatchMutation::Write {
                mutation_id,
                path,
                data_base64: STANDARD.encode(&after.data),
                desired_sha256: after.sha256,
                expected_sha256: before.map(|file| file.sha256),
            }),
            (Some(before), None) => mutations.push(PatchMutation::Remove {
                mutation_id,
                path,
                expected_sha256: before.sha256,
            }),
            (None, None) => {}
        }
    }
    Ok((mutations, summary))
}

fn require_file<'a>(
    files: &'a HashMap<String, Option<FileVersion>>,
    path: &str,
) -> Result<&'a FileVersion, ToolResultSnapshot> {
    files.get(path).and_then(Option::as_ref).ok_or_else(|| {
        tool_error(
            "patch_target_not_found",
            format!("Failed to read file to update {path}: file does not exist"),
        )
    })
}

fn touch(paths: &mut Vec<String>, path: &str) {
    if !paths.iter().any(|existing| existing == path) {
        paths.push(path.into());
    }
}

fn version(data: Vec<u8>) -> FileVersion {
    FileVersion {
        sha256: sha256(&data),
        data,
    }
}

fn sha256(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

fn apply_chunks(
    path: &str,
    source: &str,
    chunks: &[PatchChunk],
) -> Result<String, ToolResultSnapshot> {
    let mut lines = source.split('\n').map(str::to_owned).collect::<Vec<_>>();
    if lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    let mut cursor = 0;
    let mut replacements = Vec::new();
    for chunk in chunks {
        if let Some(context) = &chunk.context {
            cursor = seek(&lines, std::slice::from_ref(context), cursor, false)
                .map(|index| index + 1)
                .ok_or_else(|| {
                    tool_error(
                        "patch_context_not_found",
                        format!("Failed to find context '{context}' in {path}"),
                    )
                })?;
        }
        if chunk.old_lines.is_empty() {
            replacements.push((lines.len(), 0, chunk.new_lines.clone()));
            continue;
        }
        let mut old_lines = chunk.old_lines.as_slice();
        let mut new_lines = chunk.new_lines.as_slice();
        let mut start = seek(&lines, old_lines, cursor, chunk.end_of_file);
        if start.is_none() && old_lines.last().is_some_and(String::is_empty) {
            old_lines = &old_lines[..old_lines.len() - 1];
            if new_lines.last().is_some_and(String::is_empty) {
                new_lines = &new_lines[..new_lines.len() - 1];
            }
            start = seek(&lines, old_lines, cursor, chunk.end_of_file);
        }
        let start = start.ok_or_else(|| {
            tool_error(
                "patch_context_not_found",
                format!(
                    "Failed to find expected lines in {path}:\n{}",
                    chunk.old_lines.join("\n")
                ),
            )
        })?;
        replacements.push((start, old_lines.len(), new_lines.to_vec()));
        cursor = start + old_lines.len();
    }
    replacements.sort_by_key(|(start, _, _)| *start);
    for (start, old_len, new_lines) in replacements.into_iter().rev() {
        lines.splice(start..start + old_len, new_lines);
    }
    lines.push(String::new());
    Ok(lines.join("\n"))
}

fn seek(lines: &[String], pattern: &[String], start: usize, eof: bool) -> Option<usize> {
    if pattern.is_empty() {
        return Some(start.min(lines.len()));
    }
    if pattern.len() > lines.len() {
        return None;
    }
    let final_start = lines.len() - pattern.len();
    let search_start = if eof {
        final_start
    } else {
        start.min(final_start)
    };
    for comparison in [
        |left: &str, right: &str| left == right,
        |left: &str, right: &str| left.trim_end() == right.trim_end(),
        |left: &str, right: &str| left.trim() == right.trim(),
    ] {
        for index in search_start..=final_start {
            if lines[index..index + pattern.len()]
                .iter()
                .zip(pattern)
                .all(|(left, right)| comparison(left, right))
            {
                return Some(index);
            }
        }
    }
    None
}

fn parse_patch(input: &str) -> Result<ParsedPatch, String> {
    let lines = input
        .lines()
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect::<Vec<_>>();
    if lines.first() != Some(&"*** Begin Patch") {
        return Err("patch must begin with `*** Begin Patch`".into());
    }
    if lines.last() != Some(&"*** End Patch") {
        return Err("patch must end with `*** End Patch`".into());
    }
    let mut hunks = Vec::new();
    let mut index = 1;
    while index + 1 < lines.len() {
        let line = lines[index];
        if let Some(path) = line.strip_prefix("*** Add File: ") {
            validate_patch_path(path)?;
            index += 1;
            let mut added = Vec::new();
            while index + 1 < lines.len() && !lines[index].starts_with("*** ") {
                let Some(content) = lines[index].strip_prefix('+') else {
                    return Err(format!(
                        "add-file line must begin with `+`: {}",
                        lines[index]
                    ));
                };
                added.push(content);
                index += 1;
            }
            if added.is_empty() {
                return Err(format!("add-file hunk for {path} has no content"));
            }
            hunks.push(PatchHunk::Add {
                path: path.into(),
                content: format!("{}\n", added.join("\n")),
            });
        } else if let Some(path) = line.strip_prefix("*** Delete File: ") {
            validate_patch_path(path)?;
            hunks.push(PatchHunk::Delete { path: path.into() });
            index += 1;
        } else if let Some(path) = line.strip_prefix("*** Update File: ") {
            validate_patch_path(path)?;
            index += 1;
            let move_path = if index + 1 < lines.len() {
                lines[index]
                    .strip_prefix("*** Move to: ")
                    .map(str::to_owned)
            } else {
                None
            };
            if let Some(destination) = &move_path {
                validate_patch_path(destination)?;
                index += 1;
            }
            let mut chunks = Vec::new();
            let mut current = PatchChunk {
                context: None,
                old_lines: Vec::new(),
                new_lines: Vec::new(),
                end_of_file: false,
            };
            while index + 1 < lines.len()
                && !(lines[index].starts_with("*** ") && lines[index] != "*** End of File")
            {
                let change = lines[index];
                if change == "@@" || change.starts_with("@@ ") {
                    if !current.old_lines.is_empty() || !current.new_lines.is_empty() {
                        chunks.push(current);
                    }
                    current = PatchChunk {
                        context: change.strip_prefix("@@ ").map(str::to_owned),
                        old_lines: Vec::new(),
                        new_lines: Vec::new(),
                        end_of_file: false,
                    };
                } else if change == "*** End of File" {
                    current.end_of_file = true;
                } else if let Some(value) = change.strip_prefix('+') {
                    current.new_lines.push(value.into());
                } else if let Some(value) = change.strip_prefix('-') {
                    current.old_lines.push(value.into());
                } else if let Some(value) = change.strip_prefix(' ') {
                    current.old_lines.push(value.into());
                    current.new_lines.push(value.into());
                } else {
                    return Err(format!("invalid update line `{change}`"));
                }
                index += 1;
            }
            if !current.old_lines.is_empty() || !current.new_lines.is_empty() {
                chunks.push(current);
            }
            if chunks.is_empty() && move_path.is_none() {
                return Err(format!("update hunk for {path} has no changes"));
            }
            hunks.push(PatchHunk::Update {
                path: path.into(),
                move_path,
                chunks,
            });
        } else {
            return Err(format!("unexpected patch line `{line}`"));
        }
    }
    if hunks.is_empty() {
        return Err("patch contains no file hunks".into());
    }
    Ok(ParsedPatch { hunks })
}

fn validate_patch_path(path: &str) -> Result<(), String> {
    if path.is_empty() || path.contains('\0') {
        Err("patch paths must be nonempty and contain no NUL bytes".into())
    } else {
        Ok(())
    }
}

fn validate_path(path: &str) -> Result<(), ToolResultSnapshot> {
    validate_patch_path(path).map_err(|message| tool_error("invalid_path", message))
}

struct PreparedImage {
    bytes: Vec<u8>,
    mime: &'static str,
    source_width: u32,
    source_height: u32,
    width: u32,
    height: u32,
}

fn prepare_image(bytes: &[u8], detail: ImageDetailChoice) -> Result<PreparedImage, String> {
    let format = image::guess_format(bytes).map_err(|error| error.to_string())?;
    let image = image::load_from_memory(bytes).map_err(|error| error.to_string())?;
    let (source_width, source_height) = image.dimensions();
    let (max_dimension, max_patches) = match detail {
        ImageDetailChoice::High => (2048, 2_500),
        ImageDetailChoice::Original => (6000, 10_000),
    };
    let (width, height) = image_dimensions(source_width, source_height, max_dimension, max_patches);
    let resized = (width, height) != (source_width, source_height);
    let preserve = matches!(
        format,
        ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::Gif | ImageFormat::WebP
    );
    let (prepared_bytes, mime) = if !resized && preserve {
        (bytes.to_vec(), mime(format))
    } else {
        let image = if resized {
            image.resize_exact(width, height, FilterType::Triangle)
        } else {
            image
        };
        let mut output = Cursor::new(Vec::new());
        image
            .write_to(&mut output, ImageFormat::Png)
            .map_err(|error| error.to_string())?;
        (output.into_inner(), "image/png")
    };
    if prepared_bytes.len() > MAX_FILE_BYTES {
        return Err("prepared image exceeds the 5 MiB limit".into());
    }
    Ok(PreparedImage {
        bytes: prepared_bytes,
        mime,
        source_width,
        source_height,
        width,
        height,
    })
}

fn image_dimensions(width: u32, height: u32, max_dimension: u32, max_patches: u64) -> (u32, u32) {
    let dimension_scale = (max_dimension as f64 / width.max(height) as f64).min(1.0);
    let patches = u64::from(width.div_ceil(PATCH_SIZE as u32))
        * u64::from(height.div_ceil(PATCH_SIZE as u32));
    let patch_scale = if patches > max_patches {
        ((max_patches as f64 * (PATCH_SIZE * PATCH_SIZE) as f64) / (width as f64 * height as f64))
            .sqrt()
    } else {
        1.0
    };
    let scale = dimension_scale.min(patch_scale).min(1.0);
    (
        ((width as f64 * scale).floor() as u32).max(1),
        ((height as f64 * scale).floor() as u32).max(1),
    )
}

fn mime(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Png => "image/png",
        ImageFormat::Jpeg => "image/jpeg",
        ImageFormat::Gif => "image/gif",
        ImageFormat::WebP => "image/webp",
        _ => "image/png",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_contracts::{
        AssetPublisher, HarnessId, HarnessVersion, HistorySequence, ProjectId, SessionId,
        SessionStatus, SessionView, StateVersion,
    };
    use agent_test_support::MemoryContext;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct TestPublisher(Mutex<Option<AssetUpload>>);

    #[async_trait]
    impl AssetPublisher for TestPublisher {
        async fn publish(
            &self,
            image: AssetUpload,
        ) -> Result<agent_contracts::PublishedAsset, String> {
            let digest = sha256(&image.bytes);
            let url = format!("http://images.test/{digest}.png");
            let size_bytes = image.bytes.len();
            *self.0.lock().unwrap() = Some(image);
            Ok(agent_contracts::PublishedAsset {
                url,
                sha256: digest,
                size_bytes,
            })
        }
    }

    #[test]
    fn parses_and_applies_add_update_delete_and_move() {
        let patch = parse_patch(
            "*** Begin Patch\n*** Add File: new.txt\n+new\n*** Update File: old.txt\n*** Move to: moved.txt\n@@\n-old\n+updated\n*** Delete File: gone.txt\n*** End Patch\n",
        )
        .unwrap();
        assert_eq!(patch.hunks.len(), 3);
        let updated = apply_chunks(
            "old.txt",
            "old\n",
            match &patch.hunks[1] {
                PatchHunk::Update { chunks, .. } => chunks,
                _ => unreachable!(),
            },
        )
        .unwrap();
        assert_eq!(updated, "updated\n");
    }

    #[test]
    fn update_with_only_added_lines_appends_at_end_like_codex() {
        let patch =
            parse_patch("*** Begin Patch\n*** Update File: file.txt\n@@\n+third\n*** End Patch\n")
                .unwrap();
        let chunks = match &patch.hunks[0] {
            PatchHunk::Update { chunks, .. } => chunks,
            _ => unreachable!(),
        };
        assert_eq!(
            apply_chunks("file.txt", "first\nsecond\n", chunks).unwrap(),
            "first\nsecond\nthird\n"
        );
    }

    #[test]
    fn image_dimensions_obey_codex_limits() {
        let (width, height) = image_dimensions(8000, 4000, 2048, 2_500);
        assert!(width <= 2048 && height <= 2048);
        assert!(u64::from(width.div_ceil(32)) * u64::from(height.div_ceil(32)) <= 2_500);
    }

    #[tokio::test]
    async fn view_image_uploads_bytes_and_returns_an_http_image_part() {
        let mut encoded = Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(2, 3)
            .write_to(&mut encoded, ImageFormat::Png)
            .unwrap();
        let bytes = encoded.into_inner();
        let publisher = Arc::new(TestPublisher::default());
        let context = MemoryContext::new(
            SessionView {
                id: SessionId::new(),
                project_id: ProjectId("test-project".into()),
                harness_id: HarnessId(crate::HARNESS_ID.into()),
                harness_version: HarnessVersion(crate::HARNESS_VERSION.into()),
                status: SessionStatus::Running,
                state_version: StateVersion(0),
                metadata: json!({}),
            },
            HistorySequence(0),
        )
        .with_asset_publisher(publisher.clone());
        let result = apply_view_image(
            ViewImageState {
                path: "image.png".into(),
                detail: ImageDetailChoice::High,
            },
            execution::Response {
                protocol_version: 2,
                request_id: None,
                generation_id: Uuid::new_v4(),
                outcome: execution::Outcome::Ok {
                    result: json!({
                        "path": "/tmp/image.png",
                        "metadata": {
                            "is_file": true,
                            "is_directory": false,
                            "is_symlink": false,
                            "size": bytes.len(),
                            "modified_at_ms": null
                        },
                        "data_base64": STANDARD.encode(&bytes),
                        "sha256": sha256(&bytes)
                    }),
                },
            },
            &context,
        )
        .await
        .unwrap();

        let AppliedFileResult::Completed(snapshot) = result else {
            panic!("view_image must complete after its read");
        };
        assert!(matches!(
            snapshot.content.as_slice(),
            [ContentPart::Image { url, .. }] if url.starts_with("http://images.test/")
        ));
        let upload = publisher.0.lock().unwrap();
        let upload = upload.as_ref().unwrap();
        assert_eq!(upload.content_type, "image/png");
        assert_eq!(upload.bytes.as_ref(), bytes);
    }

    #[test]
    fn apply_patch_builds_a_generation_fenced_durable_write() {
        let patch =
            parse_patch("*** Begin Patch\n*** Add File: hello.txt\n+hello\n*** End Patch\n")
                .unwrap();
        let turn_id = Uuid::new_v4();
        let (mutations, summary) =
            plan_mutations(&patch, vec![None], turn_id, "call-apply-2").unwrap();
        let generation_id = Uuid::new_v4();
        let state = ApplyPatchState {
            phase: ApplyPatchPhase::Mutating,
            patch,
            generation_id: Some(generation_id),
            mutations,
            summary,
        };
        let config = BasicCodexConfig {
            account_id: Uuid::new_v4(),
            provider: crate::BasicCodexProvider::Openai,
            model: crate::BasicCodexModel::Luna,
            reasoning_effort: crate::ReasoningEffort::Medium,
            machine_id: Uuid::new_v4(),
            cwd: crate::WorkingDirectory::new("/workspace").unwrap(),
            shell: None,
            platform: None,
            additional_instructions: None,
        };
        let prepared = prepare_ready(FileToolState::ApplyPatch(state), &config).unwrap();
        assert_eq!(prepared.expected_generation_id, Some(generation_id));
        let Payload::Batch(batch) = prepared.request else {
            panic!("apply_patch mutations must use a batch");
        };
        assert_eq!(batch.operations.len(), 1);
        let ExecutionOperation::WriteFile(write) = &batch.operations[0].operation else {
            panic!("add-file patch must produce a write");
        };
        assert_eq!(write.data_base64, STANDARD.encode(b"hello\n"));
        assert!(matches!(write.precondition, FilePrecondition::Missing));

        let encoded = serde_json::to_value(&prepared.progress).unwrap();
        assert!(encoded.to_string().contains(&STANDARD.encode(b"hello\n")));
        assert!(!encoded.to_string().contains("[104,101,108,108,111"));
    }
}
