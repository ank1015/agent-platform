//! A separate crate proving that a harness needs only the public contracts.

use agent_contracts::{
    EventKind, GatewayConnectionId, HandlerError, HandlerOutcome, Harness, HarnessContext,
    HarnessDescription, HarnessId, HarnessVersion, InitializationError, LlmRequest, OperationId,
    OutcomeBuilder, SessionEvent, SessionStatus, ValidationError,
};
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Map;

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub connection: String,
    pub account_id: String,
    pub model_id: String,
}

#[derive(Deserialize, Serialize)]
pub struct State {
    pub pending: Option<OperationId>,
    pub native_message: agent_contracts::Message,
}

pub struct FixtureHarness;

#[async_trait]
impl Harness for FixtureHarness {
    type Config = Config;
    type State = State;

    fn describe(&self) -> HarnessDescription {
        HarnessDescription {
            id: HarnessId("fixture".into()),
            version: HarnessVersion("1".into()),
            name: "Fixture".into(),
            description: "Contract test harness".into(),
        }
    }

    fn validate_config(&self, config: &Config) -> Result<(), ValidationError> {
        if config.connection.is_empty() {
            return Err(ValidationError("connection is required".into()));
        }
        Ok(())
    }

    fn initialize(&self, _config: &Config) -> Result<State, InitializationError> {
        let native_message = serde_json::from_str(
            r#"{"role":"assistant","provider":"openai","content":[{"text":"\ud800"}]}"#,
        )
        .map_err(|error| InitializationError(error.to_string()))?;
        Ok(State {
            pending: None,
            native_message,
        })
    }

    async fn handle(
        &self,
        config: &Config,
        state: State,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<State>, HandlerError> {
        let mut outcome = OutcomeBuilder::new(state);
        match event.kind {
            EventKind::UserMessage { message } => {
                outcome.append_message(message.clone());
                let account_id = config
                    .account_id
                    .parse()
                    .map_err(|_| HandlerError("invalid account ID".into()))?;
                let operation_id = outcome.request_llm(
                    GatewayConnectionId(config.connection.clone()),
                    LlmRequest::Fresh {
                        account_id,
                        model_id: config.model_id.clone(),
                        instructions: None,
                        messages: vec![message],
                        tools: vec![],
                        provider_options: Map::new(),
                    },
                );
                outcome.state_mut().pending = Some(operation_id);
                outcome.set_status(SessionStatus::Running);
            }
            EventKind::OperationCompleted { operation_id, .. } => {
                if outcome.state_mut().pending != Some(operation_id) {
                    return Ok(outcome.finish());
                }
                let operation = context
                    .operation(operation_id)
                    .await
                    .map_err(|error| HandlerError(error.to_string()))?
                    .ok_or_else(|| HandlerError("completed operation missing".into()))?;
                if operation.outcome.is_none() {
                    return Err(HandlerError("operation has no outcome".into()));
                }
                outcome.state_mut().pending = None;
                outcome.set_status(SessionStatus::Idle);
            }
            EventKind::CancellationRequested { .. } => {
                outcome.state_mut().pending = None;
                outcome.set_status(SessionStatus::Cancelled);
            }
            EventKind::WaitResumed { .. } => {
                if context.session().status != SessionStatus::Cancelled {
                    outcome.set_status(SessionStatus::Idle);
                }
            }
        }
        Ok(outcome.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_contracts::{
        EventId, EventSequence, HistorySequence, Message, OperationKind, OperationOutcome,
        OperationOutcomeStatus, OperationRecord, ProjectId, SessionId, SessionView, StateVersion,
    };
    use agent_runtime::{HarnessRegistry, RuntimeError};
    use agent_test_support::MemoryContext;
    use chrono::Utc;
    use serde_json::json;

    #[derive(Deserialize)]
    struct BadState {
        fail: bool,
    }

    impl Serialize for BadState {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.fail {
                return Err(serde::ser::Error::custom("state cannot be serialized"));
            }
            #[derive(Serialize)]
            struct Wire {
                fail: bool,
            }
            Wire { fail: self.fail }.serialize(serializer)
        }
    }

    struct BadStateHarness;

    #[async_trait]
    impl Harness for BadStateHarness {
        type Config = Config;
        type State = BadState;

        fn describe(&self) -> HarnessDescription {
            HarnessDescription {
                id: HarnessId("bad-state".into()),
                version: HarnessVersion("1".into()),
                name: "Bad state".into(),
                description: "Serialization test".into(),
            }
        }

        fn initialize(&self, _config: &Config) -> Result<BadState, InitializationError> {
            Ok(BadState { fail: false })
        }

        async fn handle(
            &self,
            _config: &Config,
            _state: BadState,
            _event: SessionEvent,
            _context: &dyn HarnessContext,
        ) -> Result<HandlerOutcome<BadState>, HandlerError> {
            Ok(OutcomeBuilder::new(BadState { fail: true }).finish())
        }
    }

    fn context() -> MemoryContext {
        MemoryContext::new(
            SessionView {
                id: SessionId::new(),
                project_id: ProjectId("project-1".into()),
                harness_id: HarnessId("fixture".into()),
                harness_version: HarnessVersion("1".into()),
                status: SessionStatus::Idle,
                state_version: StateVersion(0),
                metadata: json!({}),
            },
            HistorySequence(0),
        )
    }

    fn event(session_id: SessionId, kind: EventKind) -> SessionEvent {
        SessionEvent {
            id: EventId::new(),
            session_id,
            sequence: EventSequence(1),
            created_at: Utc::now(),
            kind,
        }
    }

    #[tokio::test]
    async fn registered_harness_handles_message_and_completion() {
        let mut registry = HarnessRegistry::new();
        registry.register(FixtureHarness).unwrap();
        let registered = registry
            .get(&HarnessId("fixture".into()), &HarnessVersion("1".into()))
            .unwrap();
        let account_id = uuid::Uuid::new_v4();
        let configuration = serde_json::value::to_raw_value(&json!({"connection": "llm-primary", "account_id": account_id.to_string(), "model_id": "test-model"})).unwrap();
        registered.validate_configuration(&configuration).unwrap();
        assert!(registered.configuration_schema()["properties"]["connection"].is_object());
        let state = registered.initialize(&configuration).unwrap();
        assert!(state.get().contains("\\ud800"));
        let mut context = context();
        let message = Message::user_text("Hello");
        let first = registered
            .handle(
                &configuration,
                state,
                event(context.session().id, EventKind::UserMessage { message }),
                &context,
            )
            .await
            .unwrap();
        assert_eq!(first.history.len(), 1);
        assert_eq!(first.operations.len(), 1);
        assert!(matches!(first.status, Some(SessionStatus::Running)));
        let operation_id = first.operations[0].id;
        assert!(first.state.get().contains("\\ud800"));
        let checkpoint: State = serde_json::from_str(first.state.get()).unwrap();
        assert_eq!(checkpoint.pending, Some(operation_id));
        context
            .add_operation(&OperationRecord {
                id: operation_id,
                kind: OperationKind::Llm,
                gateway_job_id: None,
                outcome: Some(OperationOutcome::Cancelled),
            })
            .unwrap();
        let second = registered
            .handle(
                &configuration,
                first.state,
                event(
                    context.session().id,
                    EventKind::OperationCompleted {
                        operation_id,
                        operation_kind: OperationKind::Llm,
                        outcome_status: OperationOutcomeStatus::Cancelled,
                    },
                ),
                &context,
            )
            .await
            .unwrap();
        let checkpoint: State = serde_json::from_str(second.state.get()).unwrap();
        assert_eq!(checkpoint.pending, None);
        assert!(matches!(second.status, Some(SessionStatus::Idle)));
    }

    #[test]
    fn duplicate_registration_and_semantic_validation() {
        let mut registry = HarnessRegistry::new();
        registry.register(FixtureHarness).unwrap();
        assert!(matches!(
            registry.register(FixtureHarness),
            Err(RuntimeError::DuplicateHarness(_, _))
        ));
        let registered = registry
            .get(&HarnessId("fixture".into()), &HarnessVersion("1".into()))
            .unwrap();
        assert!(matches!(
            registered.initialize(
                serde_json::value::to_raw_value(
                    &json!({"connection":"", "account_id":"x", "model_id":"m"})
                )
                .unwrap()
                .as_ref()
            ),
            Err(RuntimeError::InvalidConfiguration(_))
        ));
        assert!(
            registry
                .get(&HarnessId("fixture".into()), &HarnessVersion("2".into()))
                .is_none()
        );
    }

    #[tokio::test]
    async fn handler_and_checkpoint_errors_are_distinct() {
        let mut registry = HarnessRegistry::new();
        registry.register(FixtureHarness).unwrap();
        registry.register(BadStateHarness).unwrap();
        let invalid_account = serde_json::value::to_raw_value(
            &json!({"connection":"llm", "account_id":"invalid", "model_id":"model"}),
        )
        .unwrap();
        let context = context();
        let fixture = registry
            .get(&HarnessId("fixture".into()), &HarnessVersion("1".into()))
            .unwrap();
        let state = fixture.initialize(&invalid_account).unwrap();
        assert!(matches!(
            fixture
                .handle(
                    &invalid_account,
                    state,
                    event(
                        context.session().id,
                        EventKind::UserMessage {
                            message: Message::user_text("hello")
                        }
                    ),
                    &context
                )
                .await,
            Err(RuntimeError::Handler(_))
        ));

        let bad = registry
            .get(&HarnessId("bad-state".into()), &HarnessVersion("1".into()))
            .unwrap();
        let state = bad.initialize(&invalid_account).unwrap();
        assert!(matches!(
            bad.handle(
                &invalid_account,
                state,
                event(
                    context.session().id,
                    EventKind::CancellationRequested { reason: None }
                ),
                &context
            )
            .await,
            Err(RuntimeError::Serialization(_))
        ));
    }
}
