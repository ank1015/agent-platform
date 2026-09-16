use crate::state::{ToolCallKind, ToolScheduling};
use agent_contracts::{CustomToolFormat, GrammarSyntax, ToolDefinition};
use serde_json::{Map, Value, json};

const APPLY_PATCH_LARK_GRAMMAR: &str = include_str!("../assets/apply_patch.lark");

pub(crate) struct ToolDescriptor {
    pub(crate) definition: ToolDefinition,
    pub(crate) call_kind: ToolCallKind,
    pub(crate) scheduling: ToolScheduling,
}

pub(crate) fn descriptors() -> Vec<ToolDescriptor> {
    vec![
        function(
            "exec_command",
            "Runs a command in a PTY, returning output or a session ID for ongoing interaction.",
            exec_command_parameters(),
            ToolScheduling::ParallelSafe,
        ),
        function(
            "write_stdin",
            "Writes characters to an existing unified exec session and returns recent output.",
            write_stdin_parameters(),
            ToolScheduling::ParallelSafe,
        ),
        ToolDescriptor {
            definition: ToolDefinition::Custom {
                name: "apply_patch".into(),
                description: "The `apply_patch` tool can be used to edit files. This is a FREEFORM tool, so do not wrap the patch in JSON.".into(),
                format: CustomToolFormat {
                    syntax: GrammarSyntax::Lark,
                    definition: APPLY_PATCH_LARK_GRAMMAR.into(),
                },
            },
            call_kind: ToolCallKind::Custom,
            scheduling: ToolScheduling::Exclusive,
        },
        function(
            "view_image",
            "View a local image file from the filesystem when visual inspection is needed. Use this for images already available on disk.",
            view_image_parameters(),
            ToolScheduling::ParallelSafe,
        ),
    ]
}

pub(crate) fn definitions() -> Vec<ToolDefinition> {
    descriptors()
        .into_iter()
        .map(|descriptor| descriptor.definition)
        .collect()
}

pub(crate) fn descriptor(name: &str) -> Option<ToolDescriptor> {
    descriptors()
        .into_iter()
        .find(|descriptor| tool_name(&descriptor.definition) == name)
}

fn function(
    name: &str,
    description: &str,
    parameters: Map<String, Value>,
    scheduling: ToolScheduling,
) -> ToolDescriptor {
    ToolDescriptor {
        definition: ToolDefinition::Function {
            name: name.into(),
            description: description.into(),
            parameters,
            output_schema: None,
            strict: Some(false),
        },
        call_kind: ToolCallKind::Function,
        scheduling,
    }
}

pub(crate) fn tool_name(definition: &ToolDefinition) -> &str {
    match definition {
        ToolDefinition::Function { name, .. } | ToolDefinition::Custom { name, .. } => name,
    }
}

pub(crate) fn parameters(descriptor: &ToolDescriptor) -> Option<&Map<String, Value>> {
    match &descriptor.definition {
        ToolDefinition::Function { parameters, .. } => Some(parameters),
        ToolDefinition::Custom { .. } => None,
    }
}

fn object(value: Value) -> Map<String, Value> {
    value.as_object().expect("tool schema is an object").clone()
}

fn exec_command_parameters() -> Map<String, Value> {
    object(json!({
        "type": "object",
        "properties": {
            "cmd": {
                "type": "string",
                "description": "Shell command to execute."
            },
            "workdir": {
                "type": "string",
                "description": "Working directory for the command. Defaults to the turn cwd."
            },
            "tty": {
                "type": "boolean",
                "description": "True allocates a PTY for the command; false or omitted uses plain pipes."
            },
            "yield_time_ms": {
                "type": "number",
                "description": "Wait before yielding output. Defaults to 10000 ms; effective range is 250-30000 ms."
            },
            "max_output_tokens": {
                "type": "number",
                "description": "Output token budget. Defaults to 10000 tokens; larger requests may be capped by policy."
            },
            "shell": {
                "type": "string",
                "description": "Shell binary to launch. Defaults to the user's default shell."
            },
            "login": {
                "type": "boolean",
                "description": "True runs the shell with -l/-i semantics; false disables them. Defaults to true."
            },
            "sandbox_permissions": {
                "type": "string",
                "enum": ["use_default", "require_escalated"],
                "description": "Per-command sandbox override. Defaults to `use_default`; use `require_escalated` for unsandboxed execution."
            },
            "justification": {
                "type": "string",
                "description": "User-facing approval question for `require_escalated`; omit otherwise."
            },
            "prefix_rule": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Reusable approval prefix for `cmd`, only with `sandbox_permissions: \"require_escalated\"`; for example [\"git\", \"pull\"]."
            }
        },
        "required": ["cmd"],
        "additionalProperties": false
    }))
}

fn write_stdin_parameters() -> Map<String, Value> {
    object(json!({
        "type": "object",
        "properties": {
            "session_id": {
                "type": "number",
                "description": "Identifier of the running unified exec session."
            },
            "chars": {
                "type": "string",
                "description": "Bytes to write to stdin. Defaults to empty, which polls without writing."
            },
            "yield_time_ms": {
                "type": "number",
                "description": "Wait before yielding output. Non-empty writes default to 250 ms and cap at 30000 ms; empty polls wait 5000-300000 ms by default."
            },
            "max_output_tokens": {
                "type": "number",
                "description": "Output token budget. Defaults to 10000 tokens; larger requests may be capped by policy."
            }
        },
        "required": ["session_id"],
        "additionalProperties": false
    }))
}

fn view_image_parameters() -> Map<String, Value> {
    object(json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Local filesystem path to an image file."
            },
            "detail": {
                "type": "string",
                "enum": ["high", "original"],
                "description": "Image detail level. Defaults to `high`; use `original` to preserve exact resolution."
            }
        },
        "required": ["path"],
        "additionalProperties": false
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definitions_match_the_selected_codex_shapes() {
        let definitions = definitions();
        assert_eq!(definitions.len(), 4);
        assert_eq!(
            definitions.iter().map(tool_name).collect::<Vec<_>>(),
            ["exec_command", "write_stdin", "apply_patch", "view_image"]
        );
        let serialized = serde_json::to_value(&definitions).unwrap();
        assert_eq!(serialized[0]["type"], "function");
        assert_eq!(serialized[0]["parameters"]["required"], json!(["cmd"]));
        assert_eq!(serialized[0]["strict"], false);
        assert_eq!(
            serialized[1]["parameters"]["required"],
            json!(["session_id"])
        );
        assert_eq!(serialized[2]["type"], "custom");
        assert_eq!(serialized[2]["format"]["syntax"], "lark");
        assert_eq!(
            serialized[2]["format"]["definition"],
            APPLY_PATCH_LARK_GRAMMAR
        );
        assert_eq!(serialized[3]["parameters"]["required"], json!(["path"]));
    }
}
