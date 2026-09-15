use agent_contracts::{
    FailureSource, LlmRequest, OperationFailure, OperationKind, OperationRequest, OperationResult,
};
use agent_gateways::{GatewayClient, GatewayError, GatewayKind, GatewayRegistry};
use agent_store::{ClaimedOperation, OperationPhase, OperationStatus, Store, StoreError};
use chrono::Utc;
use serde::Serialize;
use serde_json::{json, value::RawValue};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub const DEFAULT_MAX_CONCURRENT_DISPATCHES: usize = 32;
pub const DEFAULT_DISPATCH_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const DEFAULT_OPERATION_LEASE: Duration = Duration::from_secs(30);
pub const DEFAULT_OPERATION_LEASE_RENEWAL: Duration = Duration::from_secs(10);
pub const DEFAULT_OPERATION_DEPENDENCY_DELAY: Duration = Duration::from_millis(250);

#[derive(Clone, Debug)]
pub struct DispatcherSettings {
    pub enabled: bool,
    pub max_concurrent_dispatches: usize,
    pub poll_interval: Duration,
    pub lease_duration: Duration,
    pub lease_renewal_interval: Duration,
    pub retry_delays: Vec<Duration>,
    pub dependency_delay: Duration,
    pub request_retention: Duration,
    pub result_check_delay: Duration,
    pub shutdown_grace: Duration,
}

impl Default for DispatcherSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_dispatches: DEFAULT_MAX_CONCURRENT_DISPATCHES,
            poll_interval: DEFAULT_DISPATCH_POLL_INTERVAL,
            lease_duration: DEFAULT_OPERATION_LEASE,
            lease_renewal_interval: DEFAULT_OPERATION_LEASE_RENEWAL,
            retry_delays: vec![
                Duration::from_secs(1),
                Duration::from_secs(5),
                Duration::from_secs(30),
            ],
            dependency_delay: DEFAULT_OPERATION_DEPENDENCY_DELAY,
            request_retention: Duration::from_secs(7 * 24 * 60 * 60),
            result_check_delay: Duration::from_secs(1),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

impl DispatcherSettings {
    pub fn validate(&self) -> Result<(), DispatcherError> {
        if !(1..=1024).contains(&self.max_concurrent_dispatches) {
            return Err(DispatcherError::InvalidSettings(
                "maximum concurrent dispatches must be 1..1024",
            ));
        }
        if [
            self.poll_interval,
            self.lease_duration,
            self.lease_renewal_interval,
            self.dependency_delay,
            self.request_retention,
            self.result_check_delay,
            self.shutdown_grace,
        ]
        .into_iter()
        .any(|duration| duration.is_zero())
        {
            return Err(DispatcherError::InvalidSettings(
                "dispatcher durations must be greater than zero",
            ));
        }
        if self.retry_delays.is_empty() || self.retry_delays.iter().any(Duration::is_zero) {
            return Err(DispatcherError::InvalidSettings(
                "operation retry delays must be non-empty and greater than zero",
            ));
        }
        if self.lease_renewal_interval >= self.lease_duration {
            return Err(DispatcherError::InvalidSettings(
                "operation lease renewal must be shorter than the lease",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DispatcherError {
    #[error("invalid dispatcher settings: {0}")]
    InvalidSettings(&'static str),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("a dispatch worker stopped unexpectedly: {0}")]
    Worker(String),
}

#[derive(Clone)]
pub struct OperationDispatcher {
    store: Store,
    gateways: Arc<GatewayRegistry>,
    settings: DispatcherSettings,
    phase_cursor: Arc<AtomicUsize>,
}

struct OperationLeaseKeeper {
    stop: CancellationToken,
    lost: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl OperationLeaseKeeper {
    async fn stop(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for OperationLeaseKeeper {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl OperationDispatcher {
    pub fn new(
        store: Store,
        gateways: Arc<GatewayRegistry>,
        settings: DispatcherSettings,
    ) -> Result<Self, DispatcherError> {
        settings.validate()?;
        Ok(Self {
            store,
            gateways,
            settings,
            phase_cursor: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn process_one(&self) -> Result<bool, DispatcherError> {
        if !self.settings.enabled {
            return Ok(false);
        }
        let Some(claim) = self.claim_next().await? else {
            return Ok(false);
        };
        self.clone().process_claim(claim).await;
        Ok(true)
    }

    async fn claim_next(&self) -> Result<Option<ClaimedOperation>, StoreError> {
        const PHASES: [OperationPhase; 3] = [
            OperationPhase::Withdrawal,
            OperationPhase::Cancellation,
            OperationPhase::Submission,
        ];
        let start = self.phase_cursor.fetch_add(1, Ordering::Relaxed) % PHASES.len();
        for offset in 0..PHASES.len() {
            if let Some(claim) = self
                .store
                .claim_operation(
                    PHASES[(start + offset) % PHASES.len()],
                    self.settings.lease_duration,
                )
                .await?
            {
                return Ok(Some(claim));
            }
        }
        Ok(None)
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), DispatcherError> {
        if !self.settings.enabled {
            shutdown.cancelled().await;
            return Ok(());
        }
        let mut workers = JoinSet::new();
        'run: loop {
            while workers.len() < self.settings.max_concurrent_dispatches {
                if shutdown.is_cancelled() {
                    break 'run;
                }
                let claim = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break 'run,
                    result = self.claim_next() => result,
                };
                match claim {
                    Ok(Some(claim)) => {
                        let dispatcher = self.clone();
                        workers.spawn(async move {
                            dispatcher.process_claim(claim).await;
                        });
                    }
                    Ok(None) => break,
                    Err(error) => {
                        tracing::warn!(%error, "could not scan for operations");
                        break;
                    }
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => break,
                result = workers.join_next(), if !workers.is_empty() => {
                    if let Some(Err(error)) = result { return Err(DispatcherError::Worker(error.to_string())); }
                }
                _ = tokio::time::sleep(self.settings.poll_interval) => {}
            }
        }
        let deadline = tokio::time::sleep(self.settings.shutdown_grace);
        tokio::pin!(deadline);
        while !workers.is_empty() {
            tokio::select! {
                _ = &mut deadline => {
                    workers.abort_all();
                    while workers.join_next().await.is_some() {}
                    break;
                }
                result = workers.join_next() => {
                    if let Some(Err(error)) = result && !error.is_cancelled() {
                        return Err(DispatcherError::Worker(error.to_string()));
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_claim(self, claim: ClaimedOperation) {
        let claim = Arc::new(claim);
        let renewal = self.start_renewal(claim.clone());
        let lost = renewal.lost.clone();
        let work = async {
            match claim.phase {
                OperationPhase::Submission => self.submit(&claim).await,
                OperationPhase::Cancellation => self.cancel(&claim).await,
                OperationPhase::Withdrawal => self.withdraw(&claim).await,
                OperationPhase::ResultRetrieval => {
                    unreachable!("result delivery is handled separately")
                }
            }
        };
        tokio::pin!(work);
        tokio::select! {
            _ = &mut work => {}
            _ = lost.cancelled() => {}
        }
        renewal.stop().await;
    }

    fn start_renewal(&self, claim: Arc<ClaimedOperation>) -> OperationLeaseKeeper {
        let stop = CancellationToken::new();
        let lost = CancellationToken::new();
        let task_stop = stop.clone();
        let task_lost = lost.clone();
        let store = self.store.clone();
        let interval = self.settings.lease_renewal_interval;
        let lease = self.settings.lease_duration;
        let task = tokio::spawn(async move {
            let mut ticks =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            loop {
                tokio::select! {
                    _ = task_stop.cancelled() => return,
                    _ = ticks.tick() => {
                        if let Err(error) = store.renew_operation_claim(&claim, lease).await {
                            tracing::warn!(operation_id=%claim.operation.id, %error, "operation claim renewal failed");
                            task_lost.cancel();
                            return;
                        }
                    }
                }
            }
        });
        OperationLeaseKeeper {
            stop,
            lost,
            task: Some(task),
        }
    }

    async fn submit(&self, claim: &ClaimedOperation) {
        let request = match claim
            .request
            .as_deref()
            .ok_or("stored operation request is missing")
            .and_then(|raw| {
                serde_json::from_str::<OperationRequest>(raw.get())
                    .map_err(|_| "stored operation request is invalid")
            }) {
            Ok(request) => request,
            Err(message) => {
                return self
                    .fail(
                        claim,
                        FailureSource::Platform,
                        "invalid_operation_request",
                        message,
                        None,
                    )
                    .await;
            }
        };
        let (connection, kind, body) = match self.submission_body(claim, request).await {
            Ok(Some(value)) => value,
            Ok(None) => return,
            Err(failure) => return self.fail_value(claim, failure, None).await,
        };
        let Some(client) = self.gateways.get(&connection) else {
            return self
                .fail(
                    claim,
                    FailureSource::Platform,
                    "gateway_connection_not_configured",
                    "gateway connection is not configured",
                    None,
                )
                .await;
        };
        if client.kind() != kind {
            return self
                .fail(
                    claim,
                    FailureSource::Platform,
                    "gateway_connection_kind_mismatch",
                    "gateway connection has the wrong kind",
                    None,
                )
                .await;
        }
        self.finish_submission(claim, client, body).await;
    }

    async fn submission_body(
        &self,
        claim: &ClaimedOperation,
        request: OperationRequest,
    ) -> Result<
        Option<(
            agent_contracts::GatewayConnectionId,
            GatewayKind,
            Box<RawValue>,
        )>,
        OperationFailure,
    > {
        match request {
            OperationRequest::Llm {
                connection,
                request,
            } => {
                let body = match request {
                    LlmRequest::Fresh { account_id, model_id, instructions, messages, tools, provider_options } => {
                        #[derive(Serialize)] #[serde(rename_all="camelCase")]
                        struct Fresh<'a> {
                            idempotency_key: &'a str, account_id: uuid::Uuid, model_id: &'a str,
                            #[serde(skip_serializing_if="Option::is_none")] instructions: Option<&'a str>,
                            messages: &'a [agent_contracts::Message], tools: &'a [agent_contracts::ToolDefinition],
                            provider_options: &'a agent_contracts::ProviderOptions,
                        }
                        serde_json::value::to_raw_value(&Fresh { idempotency_key: &claim.operation.gateway_idempotency_key,
                            account_id, model_id: &model_id, instructions: instructions.as_deref(), messages: &messages,
                            tools: &tools, provider_options: &provider_options })
                    }
                    LlmRequest::Continuation { previous_operation_id, messages } => {
                        let parent = match self.store.operation_for_session(claim.operation.session_id, previous_operation_id).await {
                            Ok(parent) => parent,
                            Err(error) => {
                                tracing::warn!(operation_id=%claim.operation.id, %error, "could not read continuation dependency");
                                self.retry(claim, claim.recovering_uncertain, None, "dependency_read_failed", "could not read continuation dependency").await;
                                return Ok(None);
                            }
                        };
                        let Some(parent) = parent else {
                            return Err(platform_failure("continuation_not_found", "continuation parent does not exist in the session"));
                        };
                        if parent.kind != OperationKind::Llm || parent.gateway_connection_id.as_ref() != Some(&connection) {
                            return Err(platform_failure("invalid_continuation", "continuation parent must be an LLM operation on the same connection"));
                        }
                        match parent.status {
                            OperationStatus::Pending | OperationStatus::Submitting | OperationStatus::Accepted => {
                                self.defer(claim).await;
                                return Ok(None);
                            }
                            OperationStatus::Failed | OperationStatus::Cancelled | OperationStatus::Unknown => {
                                return Err(platform_failure("continuation_parent_failed", "continuation parent did not succeed"));
                            }
                            OperationStatus::Succeeded => {}
                        }
                        let Some(previous_job_id) = parent.gateway_job_id else {
                            return Err(platform_failure("continuation_parent_unmapped", "continuation parent has no gateway job"));
                        };
                        #[derive(Serialize)] #[serde(rename_all="camelCase")]
                        struct Continuation<'a> { idempotency_key: &'a str, previous_job_id: uuid::Uuid, messages: &'a [agent_contracts::Message] }
                        serde_json::value::to_raw_value(&Continuation { idempotency_key: &claim.operation.gateway_idempotency_key, previous_job_id, messages: &messages })
                    }
                }.map_err(|_| platform_failure("request_serialization_failed", "could not serialize LLM request"))?;
                Ok(Some((connection, GatewayKind::Llm, body)))
            }
            OperationRequest::Execution {
                connection,
                machine_id,
                expected_generation_id,
                request,
            } => {
                #[derive(Serialize)]
                struct ExecutionRequest<'a> {
                    #[serde(skip_serializing_if = "Option::is_none")]
                    expected_generation_id: Option<uuid::Uuid>,
                    #[serde(flatten)]
                    request: &'a agent_contracts::execution::Payload,
                }
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                struct ExecutionBody<'a> {
                    machine_id: uuid::Uuid,
                    idempotency_key: &'a str,
                    request: ExecutionRequest<'a>,
                }
                let body = serde_json::value::to_raw_value(&ExecutionBody {
                    machine_id,
                    idempotency_key: &claim.operation.gateway_idempotency_key,
                    request: ExecutionRequest {
                        expected_generation_id,
                        request: &request,
                    },
                })
                .map_err(|_| {
                    platform_failure(
                        "request_serialization_failed",
                        "could not serialize execution request",
                    )
                })?;
                Ok(Some((connection, GatewayKind::Execution, body)))
            }
            _ => Err(platform_failure(
                "operation_kind_mismatch",
                "operation request does not match submission phase",
            )),
        }
    }

    async fn finish_submission(
        &self,
        claim: &ClaimedOperation,
        client: Arc<dyn GatewayClient>,
        body: Box<RawValue>,
    ) {
        if claim.recovering_uncertain {
            match client
                .find_by_idempotency_key(&claim.operation.gateway_idempotency_key)
                .await
            {
                Ok(Some(ack)) => {
                    self.accept(claim, ack, 200).await;
                    return;
                }
                Ok(None) => {}
                Err(error) => {
                    self.gateway_failure(claim, error).await;
                    return;
                }
            }
        }
        match client.submit(&body).await {
            Ok(ack) => self.accept(claim, ack, 202).await,
            Err(error) => self.gateway_failure(claim, error).await,
        }
    }

    async fn accept(
        &self,
        claim: &ClaimedOperation,
        ack: agent_gateways::JobAcknowledgement,
        http_status: i32,
    ) {
        let check_at = after(self.settings.result_check_delay);
        if let Err(error) = self
            .store
            .record_operation_accepted(claim, ack.id, check_at, Some(http_status))
            .await
        {
            tracing::warn!(operation_id=%claim.operation.id, gateway_job_id=%ack.id, gateway_status=%ack.status, %error, "could not persist gateway acceptance");
        }
    }

    async fn cancel(&self, claim: &ClaimedOperation) {
        let Some(target_id) = claim.operation.target_operation_id else {
            return self
                .fail(
                    claim,
                    FailureSource::Platform,
                    "cancellation_target_not_found",
                    "cancellation target does not exist",
                    None,
                )
                .await;
        };
        let target = match self
            .store
            .operation_for_session(claim.operation.session_id, target_id)
            .await
        {
            Ok(Some(target)) => target,
            Ok(None) => {
                return self
                    .fail(
                        claim,
                        FailureSource::Platform,
                        "cancellation_target_not_found",
                        "cancellation target does not exist",
                        None,
                    )
                    .await;
            }
            Err(error) => {
                tracing::warn!(operation_id=%claim.operation.id, %error, "could not read cancellation target");
                return self
                    .retry(
                        claim,
                        claim.recovering_uncertain,
                        None,
                        "dependency_read_failed",
                        "could not read cancellation target",
                    )
                    .await;
            }
        };
        if target.kind != OperationKind::Llm {
            return self
                .fail(
                    claim,
                    FailureSource::Platform,
                    "invalid_cancellation_target",
                    "only an LLM operation can be cancelled",
                    None,
                )
                .await;
        }
        match target.status {
            OperationStatus::Pending | OperationStatus::Submitting => {
                return self.defer(claim).await;
            }
            OperationStatus::Failed
            | OperationStatus::Cancelled
            | OperationStatus::Unknown
            | OperationStatus::Succeeded => {
                if !claim.recovering_uncertain {
                    return self
                        .complete_result(
                            claim,
                            &OperationResult::LlmCancellation { accepted: false },
                            None,
                        )
                        .await;
                }
            }
            OperationStatus::Accepted => {}
        }
        let Some(job_id) = target.gateway_job_id else {
            return self.defer(claim).await;
        };
        let Some(connection) = target.gateway_connection_id else {
            return self
                .fail(
                    claim,
                    FailureSource::Platform,
                    "cancellation_target_unmapped",
                    "cancellation target has no gateway connection",
                    None,
                )
                .await;
        };
        let Some(client) = self.gateways.get(&connection) else {
            return self
                .fail(
                    claim,
                    FailureSource::Platform,
                    "gateway_connection_not_configured",
                    "gateway connection is not configured",
                    None,
                )
                .await;
        };
        if client.kind() != GatewayKind::Llm {
            return self
                .fail(
                    claim,
                    FailureSource::Platform,
                    "gateway_connection_kind_mismatch",
                    "cancellation target connection is not an LLM gateway",
                    None,
                )
                .await;
        }
        match client.cancel(job_id).await {
            Ok(ack) => {
                self.complete_result(
                    claim,
                    &OperationResult::LlmCancellation {
                        accepted: ack.accepted,
                    },
                    Some(200),
                )
                .await
            }
            Err(error) => self.gateway_failure(claim, error).await,
        }
    }

    async fn withdraw(&self, claim: &ClaimedOperation) {
        if let Err(error) = self
            .store
            .complete_withdrawal(claim, Some(after(self.settings.request_retention)))
            .await
        {
            tracing::warn!(operation_id=%claim.operation.id, %error, "could not complete operation withdrawal");
        }
    }

    async fn gateway_failure(&self, claim: &ClaimedOperation, error: GatewayError) {
        match error {
            GatewayError::Rejected {
                status,
                code,
                message,
            } if !claim.recovering_uncertain => {
                self.fail(
                    claim,
                    FailureSource::Admission,
                    &code,
                    &message,
                    Some(status as i32),
                )
                .await;
            }
            GatewayError::Rejected {
                status,
                code,
                message,
            } => {
                self.retry(claim, true, Some(status as i32), &code, &message)
                    .await;
            }
            GatewayError::Retryable(failure) => {
                self.retry(
                    claim,
                    claim.recovering_uncertain,
                    failure.status.map(i32::from),
                    &failure.code,
                    &failure.message,
                )
                .await;
            }
            GatewayError::Uncertain(failure) => {
                self.retry(
                    claim,
                    true,
                    failure.status.map(i32::from),
                    &failure.code,
                    &failure.message,
                )
                .await;
            }
            GatewayError::InvalidRequest { code, message } => {
                self.fail(claim, FailureSource::Platform, &code, &message, None)
                    .await;
            }
        }
    }

    async fn retry(
        &self,
        claim: &ClaimedOperation,
        uncertain: bool,
        http_status: Option<i32>,
        code: &str,
        message: &str,
    ) {
        let error = json!({"code": code, "message": message});
        if let Err(store_error) = self
            .store
            .record_operation_retry(
                claim,
                uncertain,
                after(self.retry_delay(claim.attempt_number)),
                http_status,
                error,
            )
            .await
        {
            tracing::warn!(operation_id=%claim.operation.id, %store_error, "could not schedule operation retry");
        }
    }

    fn retry_delay(&self, attempt_number: i32) -> Duration {
        let index = usize::try_from(attempt_number.saturating_sub(1)).unwrap_or_default();
        self.settings.retry_delays[index.min(self.settings.retry_delays.len() - 1)]
    }

    async fn defer(&self, claim: &ClaimedOperation) {
        if let Err(error) = self
            .store
            .defer_operation(claim, after(self.settings.dependency_delay))
            .await
        {
            tracing::warn!(operation_id=%claim.operation.id, %error, "could not defer operation dependency");
        }
    }

    async fn fail(
        &self,
        claim: &ClaimedOperation,
        source: FailureSource,
        code: &str,
        message: &str,
        http_status: Option<i32>,
    ) {
        self.fail_value(
            claim,
            OperationFailure {
                source,
                code: code.into(),
                message: message.into(),
                execution_response: None,
            },
            http_status,
        )
        .await;
    }

    async fn fail_value(
        &self,
        claim: &ClaimedOperation,
        failure: OperationFailure,
        http_status: Option<i32>,
    ) {
        if claim.recovering_uncertain {
            return self
                .retry(claim, true, http_status, &failure.code, &failure.message)
                .await;
        }
        let error = serde_json::to_value(&failure).expect("operation failure is JSON serializable");
        let expires = after(self.settings.request_retention);
        if let Err(store_error) = self
            .store
            .complete_operation_with_http_status(
                claim,
                OperationStatus::Failed,
                None,
                Some(error),
                Some(expires),
                http_status,
            )
            .await
        {
            tracing::warn!(operation_id=%claim.operation.id, %store_error, "could not persist operation rejection");
        }
    }

    async fn complete_result(
        &self,
        claim: &ClaimedOperation,
        result: &OperationResult,
        http_status: Option<i32>,
    ) {
        let result =
            serde_json::value::to_raw_value(result).expect("operation result is JSON serializable");
        if let Err(error) = self
            .store
            .complete_operation_with_http_status(
                claim,
                OperationStatus::Succeeded,
                Some(&result),
                None,
                Some(after(self.settings.request_retention)),
                http_status,
            )
            .await
        {
            tracing::warn!(operation_id=%claim.operation.id, %error, "could not persist operation result");
        }
    }
}

fn platform_failure(code: &str, message: &str) -> OperationFailure {
    OperationFailure {
        source: FailureSource::Platform,
        code: code.into(),
        message: message.into(),
        execution_response: None,
    }
}

fn after(duration: Duration) -> chrono::DateTime<Utc> {
    Utc::now() + chrono::Duration::from_std(duration).expect("validated duration fits chrono")
}
