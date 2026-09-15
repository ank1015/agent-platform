use agent_store::{DueWait, Store, StoreError};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
pub struct WaitExpirationSettings {
    pub enabled: bool,
    pub poll_interval: Duration,
    pub batch_size: i64,
}

impl Default for WaitExpirationSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval: Duration::from_millis(100),
            batch_size: 100,
        }
    }
}

impl WaitExpirationSettings {
    pub fn validate(&self) -> Result<(), WaitExpirationError> {
        if self.poll_interval.is_zero() || !(1..=1000).contains(&self.batch_size) {
            return Err(WaitExpirationError::InvalidSettings);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WaitExpirationError {
    #[error("invalid wait expiration worker settings")]
    InvalidSettings,
    #[error(transparent)]
    Store(#[from] StoreError),
}

pub struct WaitExpirationWorker {
    store: Store,
    settings: WaitExpirationSettings,
    cursor: Mutex<Option<DueWait>>,
}

impl WaitExpirationWorker {
    pub fn new(
        store: Store,
        settings: WaitExpirationSettings,
    ) -> Result<Self, WaitExpirationError> {
        settings.validate()?;
        Ok(Self {
            store,
            settings,
            cursor: Mutex::new(None),
        })
    }

    pub async fn process_batch(&self) -> Result<usize, WaitExpirationError> {
        Ok(self
            .process_batch_inner(None)
            .await?
            .expect("batch without shutdown"))
    }

    async fn process_batch_inner(
        &self,
        shutdown: Option<&CancellationToken>,
    ) -> Result<Option<usize>, WaitExpirationError> {
        let mut cursor = self.cursor.lock().await;
        let scan = self
            .store
            .due_waits_after(*cursor, self.settings.batch_size);
        let waits = if let Some(stop) = shutdown {
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(None),
                result = scan => result?,
            }
        } else {
            scan.await?
        };
        *cursor = waits.last().copied();
        let mut first_error = None;
        for wait in &waits {
            let expiration = self.store.expire_wait(wait.id);
            let result = if let Some(stop) = shutdown {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => return Ok(None),
                    result = expiration => result,
                }
            } else {
                expiration.await
            };
            if let Err(error) = result {
                tracing::warn!(wait_id=%wait.id, %error, "could not expire due wait");
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error.into());
        }
        Ok(Some(waits.len()))
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), WaitExpirationError> {
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
                    tracing::warn!(%error, "could not expire due waits");
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
