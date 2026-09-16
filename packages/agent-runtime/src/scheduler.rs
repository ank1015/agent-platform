use crate::{
    HarnessRegistry, RuntimeError,
    context::StoreContext,
    outcome::{OutcomePreparationError, prepare_outcome},
};
use agent_contracts::{AssetPublisher, SessionEvent, SessionView, UnavailableAssetPublisher};
use agent_store::{
    ClaimedEvent, HandlerAttemptStatus, HandlerClaim, HandlerCommitError, OutcomeCommit, Store,
    StoreError,
};
use chrono::Utc;
use futures_util::FutureExt;
use serde::Serialize;
use serde_json::{json, value::RawValue};
use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

pub const DEFAULT_MAX_CONCURRENT_HANDLERS: usize = 32;
pub const DEFAULT_SCHEDULER_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const DEFAULT_HANDLER_LEASE: Duration = Duration::from_secs(30);
pub const DEFAULT_HANDLER_LEASE_RENEWAL: Duration = Duration::from_secs(10);
pub const DEFAULT_HANDLER_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
pub const DEFAULT_OWNERSHIP_LOSS_GRACE: Duration = Duration::from_millis(100);
pub const DEFAULT_INFRASTRUCTURE_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub retry_delays: Vec<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            retry_delays: vec![Duration::from_secs(1), Duration::from_secs(5)],
        }
    }
}

#[derive(Clone, Debug)]
pub struct SchedulerSettings {
    pub enabled: bool,
    pub max_concurrent_handlers: usize,
    pub poll_interval: Duration,
    pub lease_duration: Duration,
    pub lease_renewal_interval: Duration,
    pub retry_policy: RetryPolicy,
    pub commit_retry_delays: Vec<Duration>,
    pub infrastructure_retry_delay: Duration,
    pub ownership_loss_grace: Duration,
    pub shutdown_grace: Duration,
}

impl Default for SchedulerSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_handlers: DEFAULT_MAX_CONCURRENT_HANDLERS,
            poll_interval: DEFAULT_SCHEDULER_POLL_INTERVAL,
            lease_duration: DEFAULT_HANDLER_LEASE,
            lease_renewal_interval: DEFAULT_HANDLER_LEASE_RENEWAL,
            retry_policy: RetryPolicy::default(),
            commit_retry_delays: vec![Duration::from_millis(10), Duration::from_millis(50)],
            infrastructure_retry_delay: DEFAULT_INFRASTRUCTURE_RETRY_DELAY,
            ownership_loss_grace: DEFAULT_OWNERSHIP_LOSS_GRACE,
            shutdown_grace: DEFAULT_HANDLER_SHUTDOWN_GRACE,
        }
    }
}

impl SchedulerSettings {
    pub fn validate(&self) -> Result<(), SchedulerError> {
        if !(1..=1024).contains(&self.max_concurrent_handlers) {
            return Err(SchedulerError::InvalidSettings(
                "maximum concurrent handlers must be 1..1024".into(),
            ));
        }
        if self.poll_interval.is_zero() {
            return Err(SchedulerError::InvalidSettings(
                "scheduler poll interval must be greater than zero".into(),
            ));
        }
        if self.lease_duration.is_zero()
            || self.lease_renewal_interval.is_zero()
            || self.lease_renewal_interval >= self.lease_duration
        {
            return Err(SchedulerError::InvalidSettings(
                "lease renewal interval must be greater than zero and shorter than the lease"
                    .into(),
            ));
        }
        if self.shutdown_grace.is_zero() {
            return Err(SchedulerError::InvalidSettings(
                "handler shutdown grace must be greater than zero".into(),
            ));
        }
        if self.retry_policy.retry_delays.iter().any(Duration::is_zero) {
            return Err(SchedulerError::InvalidSettings(
                "handler retry delays must be greater than zero".into(),
            ));
        }
        if self.commit_retry_delays.iter().any(Duration::is_zero) {
            return Err(SchedulerError::InvalidSettings(
                "handler outcome commit retry delays must be greater than zero".into(),
            ));
        }
        if self.infrastructure_retry_delay.is_zero() {
            return Err(SchedulerError::InvalidSettings(
                "infrastructure retry delay must be greater than zero".into(),
            ));
        }
        if self.ownership_loss_grace.is_zero() {
            return Err(SchedulerError::InvalidSettings(
                "ownership-loss grace must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("invalid scheduler settings: {0}")]
    InvalidSettings(String),
    #[error("a scheduler worker stopped unexpectedly: {0}")]
    Worker(String),
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Clone)]
pub struct SessionScheduler {
    store: Store,
    registry: Arc<HarnessRegistry>,
    settings: SchedulerSettings,
    asset_publisher: Arc<dyn AssetPublisher>,
}

struct LeaseKeeper {
    stop: CancellationToken,
    lost: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl LeaseKeeper {
    fn lost(&self) -> CancellationToken {
        self.lost.clone()
    }

    async fn stop(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl SessionScheduler {
    pub fn new(
        store: Store,
        registry: Arc<HarnessRegistry>,
        settings: SchedulerSettings,
    ) -> Result<Self, SchedulerError> {
        settings.validate()?;
        Ok(Self {
            store,
            registry,
            settings,
            asset_publisher: Arc::new(UnavailableAssetPublisher),
        })
    }

    pub fn with_asset_publisher(mut self, asset_publisher: Arc<dyn AssetPublisher>) -> Self {
        self.asset_publisher = asset_publisher;
        self
    }

    pub async fn process_one(&self, shutdown: CancellationToken) -> Result<bool, SchedulerError> {
        if !self.settings.enabled || shutdown.is_cancelled() {
            return Ok(false);
        }
        let Some(claimed) = self
            .store
            .claim_next_event(self.settings.lease_duration)
            .await?
        else {
            return Ok(false);
        };
        self.clone().process_claim(claimed, shutdown).await;
        Ok(true)
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), SchedulerError> {
        if !self.settings.enabled {
            shutdown.cancelled().await;
            return Ok(());
        }

        let mut workers = JoinSet::new();
        'scheduling: loop {
            while workers.len() < self.settings.max_concurrent_handlers {
                if shutdown.is_cancelled() {
                    break 'scheduling;
                }
                let claim = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break 'scheduling,
                    result = self.store.claim_next_event(self.settings.lease_duration) => result,
                };
                match claim {
                    Ok(Some(claimed)) => {
                        let scheduler = self.clone();
                        let shutdown = shutdown.clone();
                        workers.spawn(async move {
                            scheduler.process_claim(claimed, shutdown).await;
                        });
                    }
                    Ok(None) => break,
                    Err(error) => {
                        tracing::warn!(%error, "could not scan for session events");
                        break;
                    }
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                result = workers.join_next(), if !workers.is_empty() => {
                    if let Some(Err(error)) = result {
                        return Err(SchedulerError::Worker(error.to_string()));
                    }
                }
                _ = tokio::time::sleep(self.settings.poll_interval) => {}
            }
        }

        if workers.is_empty() {
            return Ok(());
        }
        let grace = tokio::time::sleep(self.settings.shutdown_grace);
        tokio::pin!(grace);
        loop {
            tokio::select! {
                _ = &mut grace => {
                    workers.abort_all();
                    while workers.join_next().await.is_some() {}
                    return Ok(());
                }
                result = workers.join_next() => match result {
                    Some(Ok(())) => {}
                    Some(Err(error)) if error.is_cancelled() => {}
                    Some(Err(error)) => return Err(SchedulerError::Worker(error.to_string())),
                    None => return Ok(()),
                }
            }
        }
    }

    async fn process_claim(self, claimed: ClaimedEvent, shutdown: CancellationToken) {
        let claim = HandlerClaim::from(&claimed);
        let Some(harness) = self.registry.get(
            &claimed.session.harness_id,
            &claimed.session.harness_version,
        ) else {
            self.block(
                &claim,
                "harness_not_registered",
                format!(
                    "harness {} version {} is not registered",
                    claimed.session.harness_id.0, claimed.session.harness_version.0
                ),
            )
            .await;
            return;
        };
        match std::panic::catch_unwind(AssertUnwindSafe(|| {
            harness.validate_configuration(&claimed.session.configuration)
        })) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.block(&claim, "invalid_configuration", error.to_string())
                    .await;
                return;
            }
            Err(_) => {
                self.block(
                    &claim,
                    "harness_validation_panicked",
                    "harness configuration validation panicked".into(),
                )
                .await;
                return;
            }
        }
        let event = match decode_event(&claimed) {
            Ok(event) => event,
            Err(error) => {
                self.block(&claim, "invalid_event", error).await;
                return;
            }
        };
        let ownership_lost = Arc::new(AtomicBool::new(false));
        let lease_keeper = self.start_lease_keeper(claim.clone(), ownership_lost.clone());
        let lease_lost = lease_keeper.lost();
        let context = StoreContext::new(
            self.store.clone(),
            session_view(&claimed),
            claimed.history_through_sequence,
            shutdown,
            ownership_lost.clone(),
            self.asset_publisher.clone(),
        );
        let handler = AssertUnwindSafe(harness.handle(
            &claimed.session.configuration,
            claimed.session.state,
            event,
            &context,
        ))
        .catch_unwind();
        tokio::pin!(handler);

        let handled = tokio::select! {
            result = &mut handler => Some(result),
            _ = lease_lost.cancelled() => {
                let _ = tokio::time::timeout(self.settings.ownership_loss_grace, &mut handler).await;
                None
            }
        };
        let Some(handled) = handled else {
            lease_keeper.stop().await;
            return;
        };

        if let Ok(Err(RuntimeError::Handler(_))) = &handled
            && let Some(error) = context.take_infrastructure_failure()
        {
            lease_keeper.stop().await;
            if self.renew_authority(&claim, &ownership_lost).await {
                self.abandon_infrastructure(&claim, "context_unavailable", error)
                    .await;
            }
            return;
        }

        let outcome = match handled {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(RuntimeError::Handler(message))) => {
                lease_keeper.stop().await;
                if self.renew_authority(&claim, &ownership_lost).await {
                    self.retry_handler(&claim, "handler_failed", message).await;
                }
                return;
            }
            Ok(Err(error)) => {
                lease_keeper.stop().await;
                if self.renew_authority(&claim, &ownership_lost).await {
                    self.block(&claim, "invalid_handler_data", error.to_string())
                        .await;
                }
                return;
            }
            Err(_) => {
                lease_keeper.stop().await;
                if self.renew_authority(&claim, &ownership_lost).await {
                    self.retry_handler(&claim, "handler_panicked", "handler panicked".into())
                        .await;
                }
                return;
            }
        };

        let preparation = prepare_outcome(&self.store, claim.session_id, outcome);
        tokio::pin!(preparation);
        let prepared = tokio::select! {
            result = &mut preparation => Some(result),
            _ = lease_lost.cancelled() => None,
        };
        let Some(prepared) = prepared else {
            lease_keeper.stop().await;
            return;
        };
        if ownership_lost.load(Ordering::Acquire) {
            lease_keeper.stop().await;
            return;
        }

        let outcome = match prepared {
            Ok(outcome) => outcome,
            Err(OutcomePreparationError::Invalid(error)) => {
                lease_keeper.stop().await;
                if self.renew_authority(&claim, &ownership_lost).await {
                    self.block(&claim, "invalid_handler_outcome", error).await;
                }
                return;
            }
            Err(OutcomePreparationError::Store(error)) => {
                lease_keeper.stop().await;
                if self.renew_authority(&claim, &ownership_lost).await {
                    self.abandon_infrastructure(
                        &claim,
                        "outcome_preparation_unavailable",
                        error.to_string(),
                    )
                    .await;
                }
                return;
            }
        };
        self.commit_prepared(&claim, &ownership_lost, outcome, lease_keeper)
            .await;
    }

    fn start_lease_keeper(
        &self,
        claim: HandlerClaim,
        ownership_lost: Arc<AtomicBool>,
    ) -> LeaseKeeper {
        let stop = CancellationToken::new();
        let lost = CancellationToken::new();
        let task_stop = stop.clone();
        let task_lost = lost.clone();
        let store = self.store.clone();
        let lease_duration = self.settings.lease_duration;
        let renewal_interval = self.settings.lease_renewal_interval;
        let task = tokio::spawn(async move {
            let mut renewal = tokio::time::interval_at(
                tokio::time::Instant::now() + renewal_interval,
                renewal_interval,
            );
            loop {
                tokio::select! {
                    _ = task_stop.cancelled() => return,
                    _ = renewal.tick() => {
                        let renewal = store.renew_event_claim(&claim, lease_duration);
                        tokio::pin!(renewal);
                        let result = tokio::select! {
                            _ = task_stop.cancelled() => return,
                            result = &mut renewal => result,
                        };
                        if let Err(error) = result {
                            ownership_lost.store(true, Ordering::Release);
                            task_lost.cancel();
                            match error {
                                StoreError::StaleClaim => tracing::info!(
                                    session_id=%claim.session_id,
                                    event_id=%claim.event_id,
                                    "handler lost its session claim"
                                ),
                                error => tracing::warn!(
                                    session_id=%claim.session_id,
                                    event_id=%claim.event_id,
                                    %error,
                                    "could not renew the session claim"
                                ),
                            }
                            return;
                        }
                    }
                }
            }
        });
        LeaseKeeper {
            stop,
            lost,
            task: Some(task),
        }
    }

    async fn commit_prepared(
        &self,
        claim: &HandlerClaim,
        ownership_lost: &AtomicBool,
        outcome: OutcomeCommit,
        lease_keeper: LeaseKeeper,
    ) {
        let mut retry = 0;
        let lease_lost = lease_keeper.lost();
        loop {
            let commit = self
                .store
                .commit_handler_outcome_classified(claim, outcome.clone());
            tokio::pin!(commit);
            let result = tokio::select! {
                result = &mut commit => Some(result),
                _ = lease_lost.cancelled() => None,
            };
            let Some(result) = result else {
                lease_keeper.stop().await;
                return;
            };
            match result {
                Ok(()) | Err(HandlerCommitError::StaleClaim) => {
                    lease_keeper.stop().await;
                    return;
                }
                Err(HandlerCommitError::Retryable(error)) => {
                    let Some(delay) = self.settings.commit_retry_delays.get(retry).copied() else {
                        lease_keeper.stop().await;
                        if self.renew_authority(claim, ownership_lost).await {
                            self.abandon_infrastructure(
                                claim,
                                "outcome_commit_unavailable",
                                error.to_string(),
                            )
                            .await;
                        }
                        return;
                    };
                    retry += 1;
                    tracing::warn!(
                        session_id=%claim.session_id,
                        event_id=%claim.event_id,
                        %error,
                        retry,
                        "handler outcome transaction rolled back; retrying the prepared outcome"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = lease_lost.cancelled() => {
                            lease_keeper.stop().await;
                            return;
                        }
                    }
                    if ownership_lost.load(Ordering::Acquire) {
                        lease_keeper.stop().await;
                        return;
                    }
                }
                Err(HandlerCommitError::Rejected(error)) => {
                    lease_keeper.stop().await;
                    if self.renew_authority(claim, ownership_lost).await {
                        self.block(claim, "invalid_handler_outcome", error.to_string())
                            .await;
                    }
                    return;
                }
                Err(HandlerCommitError::Uncertain(error)) => {
                    let status = self.store.handler_attempt_status(claim.attempt_id).await;
                    lease_keeper.stop().await;
                    match status {
                        Ok(Some(HandlerAttemptStatus::Committed)) => {}
                        Ok(_) => tracing::warn!(
                            session_id=%claim.session_id,
                            event_id=%claim.event_id,
                            %error,
                            "handler outcome commit is uncertain; the lease will recover it"
                        ),
                        Err(status_error) => tracing::warn!(
                            session_id=%claim.session_id,
                            event_id=%claim.event_id,
                            %error,
                            %status_error,
                            "handler outcome commit could not be reconciled"
                        ),
                    }
                    return;
                }
            }
        }
    }

    async fn renew_authority(&self, claim: &HandlerClaim, ownership_lost: &AtomicBool) -> bool {
        match self
            .store
            .renew_event_claim(claim, self.settings.lease_duration)
            .await
        {
            Ok(()) => true,
            Err(error) => {
                ownership_lost.store(true, Ordering::Release);
                match error {
                    StoreError::StaleClaim => tracing::info!(
                        session_id=%claim.session_id,
                        event_id=%claim.event_id,
                        "handler lost its session claim"
                    ),
                    error => tracing::warn!(
                        session_id=%claim.session_id,
                        event_id=%claim.event_id,
                        %error,
                        "could not renew the session claim"
                    ),
                }
                false
            }
        }
    }

    async fn retry_handler(&self, claim: &HandlerClaim, code: &str, message: String) {
        let prior_failures = match self.store.handler_failure_count(claim.event_id).await {
            Ok(count) => count as usize,
            Err(error) => {
                tracing::warn!(session_id=%claim.session_id, event_id=%claim.event_id, %error, "could not read handler retry count");
                return;
            }
        };
        let blocked = prior_failures >= self.settings.retry_policy.retry_delays.len();
        let delay = self
            .settings
            .retry_policy
            .retry_delays
            .get(prior_failures)
            .copied()
            .unwrap_or_default();
        self.record_failure(claim, code, message, delay, blocked)
            .await;
    }

    async fn block(&self, claim: &HandlerClaim, code: &str, message: String) {
        self.record_failure(claim, code, message, Duration::ZERO, true)
            .await;
    }

    async fn abandon_infrastructure(&self, claim: &HandlerClaim, code: &str, message: String) {
        let retry_at = Utc::now()
            + chrono::Duration::from_std(self.settings.infrastructure_retry_delay)
                .expect("validated infrastructure retry delay fits chrono");
        let error = json!({"code": code, "message": message});
        match self
            .store
            .abandon_handler_attempt(claim, error, retry_at)
            .await
        {
            Ok(()) | Err(StoreError::StaleClaim) => {}
            Err(error) => tracing::warn!(
                session_id=%claim.session_id,
                event_id=%claim.event_id,
                %error,
                "could not persist infrastructure interruption"
            ),
        }
    }

    async fn record_failure(
        &self,
        claim: &HandlerClaim,
        code: &str,
        message: String,
        delay: Duration,
        blocked: bool,
    ) {
        let retry_at = Utc::now()
            + chrono::Duration::from_std(delay).expect("validated retry delay fits chrono");
        let error = json!({"code": code, "message": message});
        match self
            .store
            .record_handler_failure(claim, error, retry_at, blocked)
            .await
        {
            Ok(()) | Err(StoreError::StaleClaim) => {}
            Err(error) => tracing::warn!(
                session_id=%claim.session_id,
                event_id=%claim.event_id,
                %error,
                "could not persist handler failure"
            ),
        }
    }
}

fn session_view(claimed: &ClaimedEvent) -> SessionView {
    SessionView {
        id: claimed.session.id,
        project_id: claimed.session.project_id.clone(),
        harness_id: claimed.session.harness_id.clone(),
        harness_version: claimed.session.harness_version.clone(),
        status: claimed.session.status,
        state_version: claimed.session.state_version,
        metadata: claimed.session.metadata.clone(),
    }
}

#[derive(Serialize)]
struct EventEnvelope<'a> {
    id: agent_contracts::EventId,
    session_id: agent_contracts::SessionId,
    sequence: agent_contracts::EventSequence,
    created_at: chrono::DateTime<Utc>,
    #[serde(rename = "type")]
    event_type: &'static str,
    payload: &'a RawValue,
}

fn decode_event(claimed: &ClaimedEvent) -> Result<SessionEvent, String> {
    let json = serde_json::to_string(&EventEnvelope {
        id: claimed.event.id,
        session_id: claimed.event.session_id,
        sequence: claimed.event.sequence,
        created_at: claimed.event.created_at,
        event_type: claimed.event.event_type.as_str(),
        payload: &claimed.event.payload,
    })
    .map_err(|error| error.to_string())?;
    serde_json::from_str(&json).map_err(|error| error.to_string())
}
