use agent_contracts::OperationId;
use agent_store::{Store, StoreError};
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct RequestCleanupSettings {
    pub enabled: bool,
    pub poll_interval: Duration,
    pub batch_size: i64,
}

impl Default for RequestCleanupSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval: Duration::from_secs(60),
            batch_size: 100,
        }
    }
}

impl RequestCleanupSettings {
    pub fn validate(&self) -> Result<(), RequestCleanupError> {
        if self.poll_interval.is_zero() || !(1..=1000).contains(&self.batch_size) {
            return Err(RequestCleanupError::InvalidSettings);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RequestCleanupError {
    #[error("invalid request cleanup settings")]
    InvalidSettings,
    #[error(transparent)]
    Store(#[from] StoreError),
}

pub struct RequestCleanupWorker {
    store: Store,
    settings: RequestCleanupSettings,
    cursor: Mutex<Option<(DateTime<Utc>, OperationId)>>,
}

impl RequestCleanupWorker {
    pub fn new(
        store: Store,
        settings: RequestCleanupSettings,
    ) -> Result<Self, RequestCleanupError> {
        settings.validate()?;
        Ok(Self {
            store,
            settings,
            cursor: Mutex::new(None),
        })
    }

    pub async fn process_batch(&self) -> Result<usize, RequestCleanupError> {
        Ok(self
            .process_batch_inner(None)
            .await?
            .expect("batch without shutdown"))
    }

    async fn process_batch_inner(
        &self,
        shutdown: Option<&CancellationToken>,
    ) -> Result<Option<usize>, RequestCleanupError> {
        let mut cursor = self.cursor.lock().await;
        let scan = self
            .store
            .expired_operation_request_candidates(*cursor, self.settings.batch_size);
        let candidates = if let Some(stop) = shutdown {
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(None),
                result = scan => result?,
            }
        } else {
            scan.await?
        };
        *cursor = candidates.last().copied();
        let scanned = candidates.len();
        let mut first_error = None;
        let mut removed = 0;
        for (_, id) in candidates {
            let deletion = self.store.delete_expired_operation_request(id);
            let result = if let Some(stop) = shutdown {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => return Ok(None),
                    result = deletion => result,
                }
            } else {
                deletion.await
            };
            match result {
                Ok(true) => removed += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(operation_id=%id, %error, "could not remove expired request");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error.into());
        }
        if removed > 0 {
            tracing::info!(removed, "expired operation requests removed");
        }
        Ok(Some(scanned))
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), RequestCleanupError> {
        if !self.settings.enabled {
            shutdown.cancelled().await;
            return Ok(());
        }
        loop {
            let delay = match self.process_batch_inner(Some(&shutdown)).await {
                Ok(None) => return Ok(()),
                Ok(Some(count)) if count == self.settings.batch_size as usize => Duration::ZERO,
                Ok(Some(_)) => self.settings.poll_interval,
                Err(error) => {
                    tracing::warn!(%error, "could not clean expired operation requests");
                    self.settings.poll_interval
                }
            };
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }
}
