//! Durable Codex-style harness for the agent platform.

mod cancellation;
mod compaction;
mod config;
mod error;
mod failure;
mod filesystem_tools;
mod handler;
mod harness;
mod llm;
mod model;
mod prompt;
mod shell_tools;
mod state;
mod tool_catalog;
mod tool_execution;
mod tool_plan;

pub use config::{BasicCodexConfig, WorkingDirectory};
pub use error::{ConfigError, ConstructionError};
pub use harness::BasicCodexHarness;
pub use model::{BasicCodexModel, BasicCodexProvider, ModelProfile, ReasoningEffort};

/// Stable identifier used in the platform harness registry.
pub const HARNESS_ID: &str = "basic-codex";
/// Version of the externally visible harness behavior and configuration.
pub const HARNESS_VERSION: &str = "1";
/// Version of the JSON-serialized durable state owned by this harness version.
pub const STATE_SCHEMA_VERSION: u32 = 1;

#[cfg(test)]
mod tests {
    use super::*;
    use agent_contracts::{GatewayConnectionId, HarnessId, HarnessVersion};
    use agent_runtime::HarnessRegistry;
    use serde_json::{json, value::to_raw_value};
    use uuid::Uuid;

    fn configuration() -> BasicCodexConfig {
        BasicCodexConfig {
            account_id: Uuid::new_v4(),
            provider: BasicCodexProvider::Openai,
            model: BasicCodexModel::Terra,
            reasoning_effort: ReasoningEffort::Medium,
            machine_id: Uuid::new_v4(),
            cwd: WorkingDirectory::new("/workspace/project").unwrap(),
            shell: Some("zsh".into()),
            platform: Some("linux".into()),
            additional_instructions: Some("Prefer straightforward Rust.".into()),
        }
    }

    fn harness() -> BasicCodexHarness {
        BasicCodexHarness::new(
            GatewayConnectionId("llm-primary".into()),
            GatewayConnectionId("execution-primary".into()),
        )
        .unwrap()
    }

    #[test]
    fn registers_with_exact_description_and_configuration_schema() {
        let mut registry = HarnessRegistry::new();
        registry.register(harness()).unwrap();

        let registered = registry
            .get(
                &HarnessId(HARNESS_ID.into()),
                &HarnessVersion(HARNESS_VERSION.into()),
            )
            .expect("harness is registered");
        assert_eq!(registered.description().name, "Basic Codex");
        assert_eq!(
            registered.description().description,
            "Durable Codex-style coding harness for GPT-5.6 models."
        );

        let schema = registered.configuration_schema().to_string();
        assert!(!schema.contains("llm_connection"));
        assert!(!schema.contains("execution_connection"));
        assert!(schema.contains("account_id"));
        assert!(schema.contains("machine_id"));
        assert!(schema.contains("shell"));
        assert!(schema.contains("platform"));
    }

    #[test]
    fn registry_validates_and_initializes_an_idle_session() {
        let mut registry = HarnessRegistry::new();
        registry.register(harness()).unwrap();
        let registered = registry
            .get(
                &HarnessId(HARNESS_ID.into()),
                &HarnessVersion(HARNESS_VERSION.into()),
            )
            .unwrap();
        let configuration = to_raw_value(&configuration()).unwrap();

        registered.validate_configuration(&configuration).unwrap();
        let state = registered.initialize(&configuration).unwrap();
        let value: serde_json::Value = serde_json::from_str(state.get()).unwrap();
        assert_eq!(value["schema_version"], STATE_SCHEMA_VERSION);
        assert_eq!(value["active_turn"], serde_json::Value::Null);
        assert_eq!(value["context_window"]["generation"], 1);
        assert_eq!(value["context_window"]["model_history"], json!([]));
    }

    #[test]
    fn unknown_state_schema_is_rejected_during_deserialization() {
        let error = serde_json::from_value::<state::BasicCodexState>(json!({
            "schema_version": 99,
            "active_turn": null,
            "context_window": {
                "generation": 1,
                "estimated_tokens": null,
                "model_history": [],
                "checkpoint": null
            },
            "executions": [],
            "cancellation": null
        }))
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported basic Codex state schema version 99")
        );
    }
}
