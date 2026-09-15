use agent_contracts::{
    AssistantResponse, FailureSource, OperationFailure, OperationKind, OperationRequest,
    OperationResult,
};
use agent_gateways::{GatewayError, GatewayJobOutcome, GatewayKind, GatewayRegistry};
use agent_store::{
    ClaimedCallbackReceipt, ClaimedOperation, OperationPhase, OperationStatus, Store, StoreError,
};
use chrono::Utc;
use serde_json::{Value, json, value::RawValue};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

pub const DEFAULT_RESULT_POLL_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct CompletionSettings {
    pub enabled: bool,
    pub max_concurrent_results: usize,
    pub poll_interval: Duration,
    pub lease_duration: Duration,
    pub lease_renewal_interval: Duration,
    pub retry_delay: Duration,
    pub fallback_interval: Duration,
    pub unmatched_retry_delay: Duration,
    pub unmatched_retention: Duration,
    pub request_retention: Duration,
    pub infrastructure_retry_delay: Duration,
    pub shutdown_grace: Duration,
}

impl Default for CompletionSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_results: 32,
            poll_interval: Duration::from_millis(100),
            lease_duration: Duration::from_secs(30),
            lease_renewal_interval: Duration::from_secs(10),
            retry_delay: Duration::from_secs(5),
            fallback_interval: DEFAULT_RESULT_POLL_INTERVAL,
            unmatched_retry_delay: Duration::from_secs(1),
            unmatched_retention: Duration::from_secs(24 * 60 * 60),
            request_retention: Duration::from_secs(7 * 24 * 60 * 60),
            infrastructure_retry_delay: Duration::from_secs(1),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

impl CompletionSettings {
    pub fn validate(&self) -> Result<(), CompletionError> {
        if !(1..=1024).contains(&self.max_concurrent_results)
            || [
                self.poll_interval,
                self.lease_duration,
                self.lease_renewal_interval,
                self.retry_delay,
                self.fallback_interval,
                self.unmatched_retry_delay,
                self.unmatched_retention,
                self.request_retention,
                self.infrastructure_retry_delay,
                self.shutdown_grace,
            ]
            .into_iter()
            .any(|value| value.is_zero())
            || self.lease_renewal_interval >= self.lease_duration
        {
            return Err(CompletionError::InvalidSettings);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CompletionError {
    #[error("invalid completion worker settings")]
    InvalidSettings,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("a completion worker stopped unexpectedly: {0}")]
    Worker(String),
}

#[derive(Clone)]
pub struct CompletionRuntime {
    store: Store,
    gateways: Arc<GatewayRegistry>,
    settings: CompletionSettings,
    claim_cursor: Arc<AtomicUsize>,
}

enum CompletionClaim {
    Receipt(ClaimedCallbackReceipt),
    Result(ClaimedOperation),
}

struct ResultLeaseKeeper {
    stop: CancellationToken,
    lost: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl ResultLeaseKeeper {
    async fn stop(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ResultLeaseKeeper {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl CompletionRuntime {
    pub fn new(
        store: Store,
        gateways: Arc<GatewayRegistry>,
        settings: CompletionSettings,
    ) -> Result<Self, CompletionError> {
        settings.validate()?;
        Ok(Self {
            store,
            gateways,
            settings,
            claim_cursor: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn process_one_receipt(&self) -> Result<bool, CompletionError> {
        let Some(claim) = self
            .store
            .claim_callback_receipt(self.settings.lease_duration)
            .await?
        else {
            return Ok(false);
        };
        self.process_receipt(&claim).await;
        Ok(true)
    }

    pub async fn process_one_result(&self) -> Result<bool, CompletionError> {
        let Some(claim) = self
            .store
            .claim_operation(
                OperationPhase::ResultRetrieval,
                self.settings.lease_duration,
            )
            .await?
        else {
            return Ok(false);
        };
        self.process_result(claim).await;
        Ok(true)
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), CompletionError> {
        if !self.settings.enabled {
            shutdown.cancelled().await;
            return Ok(());
        }
        let mut workers = JoinSet::new();
        'run: loop {
            while workers.len() < self.settings.max_concurrent_results {
                let claim = tokio::select! {
                    _ = shutdown.cancelled() => break 'run,
                    result = self.claim_next() => result,
                };
                match claim {
                    Ok(Some(CompletionClaim::Receipt(claim))) => {
                        let runtime = self.clone();
                        workers.spawn(async move {
                            runtime.process_receipt(&claim).await;
                        });
                    }
                    Ok(Some(CompletionClaim::Result(claim))) => {
                        let runtime = self.clone();
                        workers.spawn(async move {
                            runtime.process_result(claim).await;
                        });
                    }
                    Ok(None) => break,
                    Err(error) => {
                        tracing::warn!(%error, "could not scan for callbacks or operation results");
                        tokio::select! {
                            _ = shutdown.cancelled() => break 'run,
                            _ = tokio::time::sleep(self.settings.infrastructure_retry_delay) => {}
                        }
                        break;
                    }
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => break,
                result = workers.join_next(), if !workers.is_empty() => {
                    if let Some(Err(error)) = result { return Err(CompletionError::Worker(error.to_string())); }
                }
                _ = tokio::time::sleep(self.settings.poll_interval) => {}
            }
        }
        let _ = tokio::time::timeout(self.settings.shutdown_grace, async {
            while workers.join_next().await.is_some() {}
        })
        .await;
        workers.abort_all();
        Ok(())
    }

    async fn claim_next(&self) -> Result<Option<CompletionClaim>, StoreError> {
        let receipt_first = self
            .claim_cursor
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(2);
        for receipt in [receipt_first, !receipt_first] {
            if receipt {
                if let Some(claim) = self
                    .store
                    .claim_callback_receipt(self.settings.lease_duration)
                    .await?
                {
                    return Ok(Some(CompletionClaim::Receipt(claim)));
                }
            } else if let Some(claim) = self
                .store
                .claim_operation(
                    OperationPhase::ResultRetrieval,
                    self.settings.lease_duration,
                )
                .await?
            {
                return Ok(Some(CompletionClaim::Result(claim)));
            }
        }
        Ok(None)
    }

    async fn process_receipt(&self, claim: &ClaimedCallbackReceipt) {
        let retry_at = after(self.settings.unmatched_retry_delay);
        match self.store.match_callback_receipt(claim, retry_at).await {
            Ok(Some(_)) => {
                if let Err(error) = self.store.complete_callback_receipt(claim).await {
                    tracing::warn!(receipt_id=%claim.receipt.id, %error, "could not finish callback receipt");
                }
            }
            Ok(None) => {
                if Utc::now() - claim.receipt.received_at
                    > chrono::Duration::from_std(self.settings.unmatched_retention)
                        .expect("validated duration")
                    && let Err(error) = self.store.block_unmatched_callback_receipt(
                        claim.receipt.id,
                        json!({"code":"operation_mapping_not_found","message":"callback remained unmatched beyond its retention window"}),
                    ).await
                {
                    tracing::warn!(receipt_id=%claim.receipt.id, %error, "could not block expired unmatched callback receipt");
                }
            }
            Err(error) => {
                tracing::warn!(receipt_id=%claim.receipt.id, %error, "could not reconcile callback receipt")
            }
        }
    }

    async fn process_result(&self, claim: ClaimedOperation) {
        let claim = Arc::new(claim);
        let renewal = self.start_result_renewal(claim.clone());
        let lost = renewal.lost.clone();
        let work = self.process_result_claim(&claim);
        tokio::pin!(work);
        tokio::select! {
            _ = &mut work => {}
            _ = lost.cancelled() => {}
        }
        renewal.stop().await;
    }

    fn start_result_renewal(&self, claim: Arc<ClaimedOperation>) -> ResultLeaseKeeper {
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
                            if !task_stop.is_cancelled() {
                                tracing::warn!(operation_id=%claim.operation.id, %error, "result-retrieval claim renewal failed");
                                task_lost.cancel();
                            }
                            return;
                        }
                    }
                }
            }
        });
        ResultLeaseKeeper {
            stop,
            lost,
            task: Some(task),
        }
    }

    async fn process_result_claim(&self, claim: &ClaimedOperation) {
        let Some(connection) = claim.operation.gateway_connection_id.as_ref() else {
            return self
                .retry_result(
                    claim,
                    None,
                    "gateway_connection_missing",
                    "accepted operation has no gateway connection",
                )
                .await;
        };
        let Some(job_id) = claim.operation.gateway_job_id else {
            return self
                .retry_result(
                    claim,
                    None,
                    "gateway_job_missing",
                    "accepted operation has no gateway job",
                )
                .await;
        };
        let Some(client) = self.gateways.get(connection) else {
            return self
                .retry_result(
                    claim,
                    None,
                    "gateway_connection_not_configured",
                    "gateway connection is not configured",
                )
                .await;
        };
        let expected = if claim.operation.kind == OperationKind::Llm {
            GatewayKind::Llm
        } else {
            GatewayKind::Execution
        };
        if client.kind() != expected {
            return self
                .retry_result(
                    claim,
                    None,
                    "gateway_connection_kind_mismatch",
                    "gateway connection has the wrong kind",
                )
                .await;
        }
        let execution_identity = if claim.operation.kind == OperationKind::Execution {
            let Some(identity) = self.execution_request_identity(claim).await else {
                return;
            };
            Some(identity)
        } else {
            None
        };
        match client.get_job(job_id).await {
            Ok(job) => {
                if let Some((machine_id, expected_generation_id)) = execution_identity
                    && let Err(error) =
                        job.validate_execution_identity(machine_id, expected_generation_id)
                {
                    return self
                        .retry_result(
                            claim,
                            Some(200),
                            "gateway_job_identity_mismatch",
                            &error.to_string(),
                        )
                        .await;
                }
                match job.outcome {
                    GatewayJobOutcome::Pending => {
                        if let Err(error) = self
                            .store
                            .record_operation_pending_result(
                                claim,
                                after(self.settings.fallback_interval),
                                Some(200),
                            )
                            .await
                        {
                            tracing::warn!(operation_id=%claim.operation.id, %error, "could not schedule fallback result retrieval");
                        }
                    }
                    GatewayJobOutcome::Succeeded(response) => {
                        self.complete_success(claim, response.as_ref()).await
                    }
                    GatewayJobOutcome::Failed { response, error } => {
                        self.complete_failure(claim, response.as_deref(), error)
                            .await
                    }
                    GatewayJobOutcome::Cancelled => {
                        self.complete(claim, OperationStatus::Cancelled, None, None)
                            .await
                    }
                    GatewayJobOutcome::Unknown(error) => {
                        let failure = failure(
                            FailureSource::Gateway,
                            &error,
                            "unknown",
                            "gateway could not recover the operation result",
                        );
                        self.complete(claim, OperationStatus::Unknown, None, Some(failure))
                            .await;
                    }
                }
            }
            Err(error) => {
                let (status, code, message) = gateway_error(error);
                self.retry_result(claim, status, &code, &message).await;
            }
        }
    }

    async fn execution_request_identity(
        &self,
        claim: &ClaimedOperation,
    ) -> Option<(uuid::Uuid, Option<uuid::Uuid>)> {
        let request = match self.store.operation_request(claim.operation.id).await {
            Ok(Some(request)) => request,
            Ok(None) => {
                self.retry_result(
                    claim,
                    None,
                    "operation_request_missing",
                    "stored execution request is missing",
                )
                .await;
                return None;
            }
            Err(error) => {
                tracing::warn!(operation_id=%claim.operation.id, %error, "could not read stored execution request");
                self.retry_result(
                    claim,
                    None,
                    "operation_request_read_failed",
                    "could not read stored execution request",
                )
                .await;
                return None;
            }
        };
        match serde_json::from_str::<OperationRequest>(request.get()) {
            Ok(OperationRequest::Execution {
                connection,
                machine_id,
                expected_generation_id,
                ..
            }) if Some(&connection) == claim.operation.gateway_connection_id.as_ref() => {
                Some((machine_id, expected_generation_id))
            }
            _ => {
                self.retry_result(
                    claim,
                    None,
                    "invalid_operation_request",
                    "stored execution request is invalid or inconsistent",
                )
                .await;
                None
            }
        }
    }

    async fn complete_success(&self, claim: &ClaimedOperation, response: &RawValue) {
        let result = match claim.operation.kind {
            OperationKind::Llm => serde_json::from_str::<AssistantResponse>(response.get())
                .map(|value| OperationResult::Llm(Box::new(value))),
            OperationKind::Execution => {
                serde_json::from_str(response.get()).map(OperationResult::Execution)
            }
            _ => return,
        };
        match result {
            Ok(result) => {
                let raw =
                    serde_json::value::to_raw_value(&result).expect("typed result serializes");
                self.complete(claim, OperationStatus::Succeeded, Some(raw.as_ref()), None)
                    .await;
            }
            Err(_) => {
                self.retry_result(
                    claim,
                    Some(200),
                    "invalid_gateway_result",
                    "gateway returned an invalid typed result",
                )
                .await
            }
        }
    }

    async fn complete_failure(
        &self,
        claim: &ClaimedOperation,
        response: Option<&RawValue>,
        error: Option<Value>,
    ) {
        let mut result = None;
        let source = if claim.operation.kind == OperationKind::Execution && response.is_some() {
            FailureSource::Protocol
        } else {
            FailureSource::Gateway
        };
        if let Some(response) = response
            && claim.operation.kind == OperationKind::Execution
        {
            let Ok(response) = serde_json::from_str(response.get()) else {
                return self
                    .retry_result(
                        claim,
                        Some(200),
                        "invalid_gateway_result",
                        "gateway returned an invalid execution response",
                    )
                    .await;
            };
            let operation_result = OperationResult::Execution(response);
            result = Some(
                serde_json::value::to_raw_value(&operation_result)
                    .expect("typed result serializes"),
            );
        }
        let error = error.unwrap_or_else(
            || json!({"code":"operation_failed","message":"gateway operation failed"}),
        );
        let failure = failure(
            source,
            &error,
            "operation_failed",
            "gateway operation failed",
        );
        self.complete(
            claim,
            OperationStatus::Failed,
            result.as_deref(),
            Some(failure),
        )
        .await;
    }

    async fn complete(
        &self,
        claim: &ClaimedOperation,
        status: OperationStatus,
        result: Option<&RawValue>,
        error: Option<Value>,
    ) {
        if let Err(error) = self
            .store
            .complete_operation_with_http_status(
                claim,
                status,
                result,
                error,
                Some(after(self.settings.request_retention)),
                Some(200),
            )
            .await
        {
            tracing::warn!(operation_id=%claim.operation.id, %error, "could not persist operation completion");
        }
    }

    async fn retry_result(
        &self,
        claim: &ClaimedOperation,
        http_status: Option<i32>,
        code: &str,
        message: &str,
    ) {
        if let Err(error) = self
            .store
            .record_operation_retry(
                claim,
                false,
                after(self.settings.retry_delay),
                http_status,
                json!({"code":code,"message":message}),
            )
            .await
        {
            tracing::warn!(operation_id=%claim.operation.id, %error, "could not reschedule result retrieval");
        }
    }
}

fn failure(
    source: FailureSource,
    value: &Value,
    default_code: &str,
    default_message: &str,
) -> Value {
    serde_json::to_value(OperationFailure {
        source,
        code: value
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or(default_code)
            .into(),
        message: value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or(default_message)
            .into(),
        execution_response: None,
    })
    .expect("failure serializes")
}

fn gateway_error(error: GatewayError) -> (Option<i32>, String, String) {
    match error {
        GatewayError::Rejected {
            status,
            code,
            message,
        } => (Some(status.into()), code, message),
        GatewayError::Retryable(value) | GatewayError::Uncertain(value) => {
            (value.status.map(i32::from), value.code, value.message)
        }
        GatewayError::InvalidRequest { code, message } => (None, code, message),
    }
}

fn after(duration: Duration) -> chrono::DateTime<Utc> {
    Utc::now() + chrono::Duration::from_std(duration).expect("validated duration")
}
