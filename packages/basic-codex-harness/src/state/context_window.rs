use agent_contracts::{HistorySequence, Message, Provider, Usage};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContextWindowState {
    pub(crate) generation: u32,
    pub(crate) estimated_tokens: Option<u64>,
    /// The causal, provider-visible conversation projection. This can differ
    /// from chronological platform history when steering arrives while an LLM
    /// operation is in flight.
    #[serde(default)]
    pub(crate) model_history: Vec<Message>,
    pub(crate) checkpoint: Option<CompactionCheckpoint>,
}

impl ContextWindowState {
    pub(crate) fn initial() -> Self {
        Self {
            generation: 1,
            estimated_tokens: None,
            model_history: Vec::new(),
            checkpoint: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompactionCheckpoint {
    pub(crate) provider: Provider,
    pub(crate) response_id: String,
    pub(crate) native_item: Box<RawValue>,
    pub(crate) source_through_sequence: HistorySequence,
    pub(crate) source_projection_sha256: String,
    pub(crate) retained_user_messages: Vec<Message>,
    pub(crate) generation: u32,
    pub(crate) usage: Option<Usage>,
}
