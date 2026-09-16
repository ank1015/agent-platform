use crate::{
    BasicCodexConfig, ConstructionError, HARNESS_ID, HARNESS_VERSION, state::BasicCodexState,
};
use agent_contracts::{
    GatewayConnectionId, HandlerError, HandlerOutcome, Harness, HarnessContext, HarnessDescription,
    HarnessId, HarnessVersion, InitializationError, SessionEvent, ValidationError,
};
use async_trait::async_trait;
use std::fmt;

#[derive(Clone)]
pub struct BasicCodexHarness {
    llm_connection_id: GatewayConnectionId,
    execution_connection_id: GatewayConnectionId,
}

impl fmt::Debug for BasicCodexHarness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BasicCodexHarness")
            .field("llm_connection_id", &self.llm_connection_id)
            .field("execution_connection_id", &self.execution_connection_id)
            .finish_non_exhaustive()
    }
}

impl BasicCodexHarness {
    pub fn new(
        llm_connection_id: GatewayConnectionId,
        execution_connection_id: GatewayConnectionId,
    ) -> Result<Self, ConstructionError> {
        if llm_connection_id.0.trim().is_empty() {
            return Err(ConstructionError::EmptyLlmConnectionId);
        }
        if execution_connection_id.0.trim().is_empty() {
            return Err(ConstructionError::EmptyExecutionConnectionId);
        }
        Ok(Self {
            llm_connection_id,
            execution_connection_id,
        })
    }

    /// Server-owned LLM gateway route used for operations from this harness.
    pub fn llm_connection_id(&self) -> &GatewayConnectionId {
        &self.llm_connection_id
    }

    /// Server-owned execution gateway route used for operations from this harness.
    pub fn execution_connection_id(&self) -> &GatewayConnectionId {
        &self.execution_connection_id
    }
}

#[async_trait]
impl Harness for BasicCodexHarness {
    type Config = BasicCodexConfig;
    type State = BasicCodexState;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId(HARNESS_ID.into()),
            version: HarnessVersion(HARNESS_VERSION.into()),
            name: "Basic Codex".into(),
            description: "Durable Codex-style coding harness for GPT-5.6 models.".into(),
        }
    }

    fn validate_config(&self, config: &Self::Config) -> Result<(), ValidationError> {
        config
            .validate()
            .map_err(|error| ValidationError(error.to_string()))
    }

    fn initialize(&self, config: &Self::Config) -> Result<Self::State, InitializationError> {
        config
            .validate()
            .map_err(|error| InitializationError(format!("invalid configuration: {error}")))?;
        Ok(BasicCodexState::initial())
    }

    async fn handle(
        &self,
        config: &Self::Config,
        state: Self::State,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<Self::State>, HandlerError> {
        crate::handler::handle(self, config, state, event, context).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_contracts::Harness;
    use uuid::Uuid;

    fn config() -> BasicCodexConfig {
        BasicCodexConfig {
            account_id: Uuid::new_v4(),
            provider: crate::BasicCodexProvider::Openai,
            model: crate::BasicCodexModel::Sol,
            reasoning_effort: crate::ReasoningEffort::Low,
            machine_id: Uuid::new_v4(),
            cwd: crate::WorkingDirectory::new("/workspace").unwrap(),
            shell: None,
            platform: None,
            additional_instructions: None,
        }
    }

    #[test]
    fn rejects_empty_routing_ids() {
        assert_eq!(
            BasicCodexHarness::new(
                GatewayConnectionId(" ".into()),
                GatewayConnectionId("execution".into())
            )
            .unwrap_err(),
            ConstructionError::EmptyLlmConnectionId
        );
        assert_eq!(
            BasicCodexHarness::new(
                GatewayConnectionId("llm".into()),
                GatewayConnectionId(String::new())
            )
            .unwrap_err(),
            ConstructionError::EmptyExecutionConnectionId
        );
    }

    #[test]
    fn initialization_is_deterministic_and_performs_no_work() {
        let harness = BasicCodexHarness::new(
            GatewayConnectionId("llm".into()),
            GatewayConnectionId("execution".into()),
        )
        .unwrap();
        let first = serde_json::to_value(harness.initialize(&config()).unwrap()).unwrap();
        let second = serde_json::to_value(harness.initialize(&config()).unwrap()).unwrap();
        assert_eq!(first, second);
        assert_eq!(first["active_turn"], serde_json::Value::Null);
        assert_eq!(first["executions"], serde_json::json!([]));
    }

    #[test]
    fn routing_is_owned_by_the_harness_instance() {
        let harness = BasicCodexHarness::new(
            GatewayConnectionId("llm".into()),
            GatewayConnectionId("execution".into()),
        )
        .unwrap();
        assert_eq!(harness.llm_connection_id().0, "llm");
        assert_eq!(harness.execution_connection_id().0, "execution");
    }
}
