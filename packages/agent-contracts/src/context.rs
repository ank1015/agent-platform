use crate::{
    HistoryEntryId, HistorySequence, Message, OperationId, OperationKind, OperationRecord,
    SessionView, WaitId, WaitRecord, WaitStatus,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct AssetUpload {
    pub content_type: String,
    pub bytes: Arc<[u8]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishedAsset {
    pub url: String,
    pub sha256: String,
    pub size_bytes: usize,
}

#[async_trait]
pub trait AssetPublisher: Send + Sync {
    /// Publishes immutable content and returns a URL usable in model input.
    /// Implementations must make retries with identical bytes safe.
    async fn publish(&self, asset: AssetUpload) -> Result<PublishedAsset, String>;
}

#[derive(Debug, Default)]
pub struct UnavailableAssetPublisher;

#[async_trait]
impl AssetPublisher for UnavailableAssetPublisher {
    async fn publish(&self, _asset: AssetUpload) -> Result<PublishedAsset, String> {
        Err("no asset publisher is configured for this platform".into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub id: HistoryEntryId,
    pub sequence: HistorySequence,
    pub source_event_id: Option<crate::EventId>,
    pub created_at: DateTime<Utc>,
    pub message: Message,
}

#[derive(Clone, Copy, Debug)]
pub struct HistoryQuery {
    pub after_sequence: HistorySequence,
    pub limit: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct OperationQuery {
    pub kind: Option<OperationKind>,
    pub limit: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct WaitQuery {
    pub status: Option<WaitStatus>,
    pub limit: usize,
}

#[derive(Clone, Debug)]
pub enum ContextError {
    Unavailable(String),
    Read(String),
    Publish(String),
}

impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(message) | Self::Read(message) | Self::Publish(message) => {
                message.fmt(f)
            }
        }
    }
}

impl std::error::Error for ContextError {}

/// All reads are scoped to one session. History is bounded at invocation start.
#[async_trait]
pub trait HarnessContext: Send + Sync {
    fn session(&self) -> &SessionView;
    fn history_through_sequence(&self) -> HistorySequence;
    fn stop_requested(&self) -> bool;

    async fn publish_asset(&self, _asset: AssetUpload) -> Result<PublishedAsset, ContextError> {
        Err(ContextError::Unavailable(
            "no asset publisher is configured for this platform".into(),
        ))
    }

    async fn history(&self, query: HistoryQuery) -> Result<Vec<HistoryEntry>, ContextError>;
    async fn history_entry(&self, id: HistoryEntryId)
    -> Result<Option<HistoryEntry>, ContextError>;
    async fn operation(&self, id: OperationId) -> Result<Option<OperationRecord>, ContextError>;
    async fn operations(&self, query: OperationQuery)
    -> Result<Vec<OperationRecord>, ContextError>;
    async fn wait(&self, id: WaitId) -> Result<Option<WaitRecord>, ContextError>;
    async fn waits(&self, query: WaitQuery) -> Result<Vec<WaitRecord>, ContextError>;
}
