//! Harness registration and durable session execution.

mod completion;
mod context;
mod dispatch;
mod outcome;
mod request_cleanup;
mod scheduler;
mod wait_expiration;
mod work_signal;

pub use completion::*;
pub use dispatch::*;
pub use request_cleanup::*;
pub use scheduler::*;
pub use wait_expiration::*;
pub use work_signal::*;

use agent_contracts::{
    HandlerOutcome, Harness, HarnessContext, HarnessDescription, HarnessId, HarnessVersion,
    Message, SessionEvent, SessionStatus, StagedOperation, StagedWait, WaitId,
};
use async_trait::async_trait;
use serde_json::{Value, value::RawValue};
use std::collections::HashMap;

#[derive(Debug)]
pub enum RuntimeError {
    DuplicateHarness(HarnessId, HarnessVersion),
    InvalidConfiguration(String),
    InvalidState(String),
    Initialization(String),
    Handler(String),
    Serialization(String),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateHarness(id, version) => {
                write!(f, "duplicate harness {} version {}", id.0, version.0)
            }
            Self::InvalidConfiguration(error) => write!(f, "invalid configuration: {error}"),
            Self::InvalidState(error) => write!(f, "invalid saved state: {error}"),
            Self::Initialization(error) => write!(f, "initialization failed: {error}"),
            Self::Handler(error) => write!(f, "handler failed: {error}"),
            Self::Serialization(error) => write!(f, "outcome serialization failed: {error}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

pub struct SerializedOutcome {
    pub state: Box<RawValue>,
    pub history: Vec<Message>,
    pub operations: Vec<StagedOperation>,
    pub waits: Vec<StagedWait>,
    pub cancelled_waits: Vec<WaitId>,
    pub progress: Vec<Value>,
    pub status: Option<SessionStatus>,
}

impl SerializedOutcome {
    fn from_typed<S: serde::Serialize>(outcome: HandlerOutcome<S>) -> Result<Self, RuntimeError> {
        Ok(Self {
            state: serde_json::value::to_raw_value(&outcome.state)
                .map_err(|error| RuntimeError::Serialization(error.to_string()))?,
            history: outcome.history,
            operations: outcome.operations,
            waits: outcome.waits,
            cancelled_waits: outcome.cancelled_waits,
            progress: outcome.progress,
            status: outcome.status,
        })
    }
}

#[async_trait]
pub trait RegisteredHarness: Send + Sync {
    fn description(&self) -> &HarnessDescription;
    fn configuration_schema(&self) -> &Value;
    fn validate_configuration(&self, configuration: &RawValue) -> Result<(), RuntimeError>;
    fn initialize(&self, configuration: &RawValue) -> Result<Box<RawValue>, RuntimeError>;
    async fn handle(
        &self,
        configuration: &RawValue,
        state: Box<RawValue>,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<SerializedOutcome, RuntimeError>;
}

struct Adapter<H: Harness> {
    harness: H,
    description: HarnessDescription,
    configuration_schema: Value,
}

#[async_trait]
impl<H: Harness> RegisteredHarness for Adapter<H> {
    fn description(&self) -> &HarnessDescription {
        &self.description
    }

    fn configuration_schema(&self) -> &Value {
        &self.configuration_schema
    }

    fn validate_configuration(&self, configuration: &RawValue) -> Result<(), RuntimeError> {
        let config: H::Config = serde_json::from_str(configuration.get())
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        self.harness
            .validate_config(&config)
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))
    }

    fn initialize(&self, configuration: &RawValue) -> Result<Box<RawValue>, RuntimeError> {
        let config: H::Config = serde_json::from_str(configuration.get())
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        self.harness
            .validate_config(&config)
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        let state = self
            .harness
            .initialize(&config)
            .map_err(|error| RuntimeError::Initialization(error.to_string()))?;
        serde_json::value::to_raw_value(&state)
            .map_err(|error| RuntimeError::Serialization(error.to_string()))
    }

    async fn handle(
        &self,
        configuration: &RawValue,
        state: Box<RawValue>,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<SerializedOutcome, RuntimeError> {
        let config: H::Config = serde_json::from_str(configuration.get())
            .map_err(|error| RuntimeError::InvalidConfiguration(error.to_string()))?;
        let state: H::State = serde_json::from_str(state.get())
            .map_err(|error| RuntimeError::InvalidState(error.to_string()))?;
        let outcome = self
            .harness
            .handle(&config, state, event, context)
            .await
            .map_err(|error| RuntimeError::Handler(error.to_string()))?;
        SerializedOutcome::from_typed(outcome)
    }
}

#[derive(Default)]
pub struct HarnessRegistry {
    harnesses: HashMap<(HarnessId, HarnessVersion), Box<dyn RegisteredHarness>>,
}

impl HarnessRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<H: Harness>(&mut self, harness: H) -> Result<(), RuntimeError> {
        let description = harness.describe();
        let key = (description.id.clone(), description.version.clone());
        if self.harnesses.contains_key(&key) {
            return Err(RuntimeError::DuplicateHarness(key.0, key.1));
        }
        let configuration_schema = serde_json::to_value(schemars::schema_for!(H::Config))
            .map_err(|error| RuntimeError::Serialization(error.to_string()))?;
        self.harnesses.insert(
            key,
            Box::new(Adapter {
                harness,
                description,
                configuration_schema,
            }),
        );
        Ok(())
    }

    pub fn get(&self, id: &HarnessId, version: &HarnessVersion) -> Option<&dyn RegisteredHarness> {
        self.harnesses
            .get(&(id.clone(), version.clone()))
            .map(Box::as_ref)
    }

    pub fn descriptions(&self) -> Vec<&HarnessDescription> {
        let mut descriptions: Vec<_> = self.harnesses.values().map(|h| h.description()).collect();
        descriptions.sort_by(|a, b| (&a.id.0, &a.version.0).cmp(&(&b.id.0, &b.version.0)));
        descriptions
    }
}
