use crate::{HandlerOutcome, HarnessContext, HarnessId, HarnessVersion, SessionEvent};
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Serialize, de::DeserializeOwned};

#[derive(Clone, Debug)]
pub struct HarnessDescription {
    pub id: HarnessId,
    pub version: HarnessVersion,
    pub name: String,
    pub description: String,
}

macro_rules! harness_error {
    ($name:ident) => {
        #[derive(Clone, Debug)]
        pub struct $name(pub String);

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl std::error::Error for $name {}
    };
}

harness_error!(ValidationError);
harness_error!(InitializationError);
harness_error!(HandlerError);

#[async_trait]
pub trait Harness: Send + Sync + 'static {
    type Config: Serialize + DeserializeOwned + JsonSchema + Send + Sync;
    type State: Serialize + DeserializeOwned + Send;

    fn describe(&self) -> HarnessDescription;

    fn validate_config(&self, _config: &Self::Config) -> Result<(), ValidationError> {
        Ok(())
    }

    /// Called once on session creation. It must not start platform operations.
    fn initialize(&self, config: &Self::Config) -> Result<Self::State, InitializationError>;

    async fn handle(
        &self,
        config: &Self::Config,
        state: Self::State,
        event: SessionEvent,
        context: &dyn HarnessContext,
    ) -> Result<HandlerOutcome<Self::State>, HandlerError>;
}
