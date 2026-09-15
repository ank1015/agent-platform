//! Configured clients for the LLM and execution gateways.

use agent_contracts::{AssistantResponse, GatewayConnectionId, execution};
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use reqwest::{StatusCode, Url, header::HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};
use sha2::Sha256;
use std::{collections::HashMap, fmt, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayKind {
    Llm,
    Execution,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConnectionConfig {
    pub id: GatewayConnectionId,
    pub kind: GatewayKind,
    pub base_url: Url,
    pub bearer_token: String,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

impl fmt::Debug for GatewayConnectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let base_url = match (self.base_url.host_str(), self.base_url.port()) {
            (Some(host), Some(port)) => format!(
                "{}://{host}:{port}{}",
                self.base_url.scheme(),
                self.base_url.path()
            ),
            (Some(host), None) => format!(
                "{}://{host}{}",
                self.base_url.scheme(),
                self.base_url.path()
            ),
            (None, _) => "[invalid URL]".into(),
        };
        formatter
            .debug_struct("GatewayConnectionConfig")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("base_url", &base_url)
            .field("bearer_token", &"[redacted]")
            .field("timeout_ms", &self.timeout_ms)
            .finish()
    }
}

const fn default_timeout_ms() -> u64 {
    30_000
}

impl GatewayConnectionConfig {
    pub fn validate(&self) -> Result<(), GatewayConfigError> {
        if self.id.0.is_empty()
            || self.id.0.len() > 200
            || self.id.0.chars().any(char::is_whitespace)
        {
            return Err(GatewayConfigError::Invalid(
                self.id.clone(),
                "connection ID must contain 1 to 200 non-whitespace characters",
            ));
        }
        if self.bearer_token.is_empty()
            || self.bearer_token.len() > 512
            || self.bearer_token.chars().any(char::is_whitespace)
            || HeaderValue::try_from(format!("Bearer {}", self.bearer_token)).is_err()
        {
            return Err(GatewayConfigError::Invalid(
                self.id.clone(),
                "bearer token must contain 1 to 512 non-whitespace characters",
            ));
        }
        if self.timeout_ms == 0 {
            return Err(GatewayConfigError::Invalid(
                self.id.clone(),
                "timeout must be greater than zero",
            ));
        }
        if !matches!(self.base_url.scheme(), "http" | "https")
            || self.base_url.cannot_be_a_base()
            || !self.base_url.username().is_empty()
            || self.base_url.password().is_some()
            || !self.base_url.path().ends_with('/')
            || self.base_url.query().is_some()
            || self.base_url.fragment().is_some()
        {
            return Err(GatewayConfigError::Invalid(
                self.id.clone(),
                "base URL must be an HTTP or HTTPS directory URL without credentials, query, or fragment",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayConfigError {
    #[error("duplicate gateway connection {0}")]
    Duplicate(GatewayConnectionId),
    #[error("invalid gateway connection {0}: {1}")]
    Invalid(GatewayConnectionId, &'static str),
    #[error("could not build gateway connection {0}: {1}")]
    Client(GatewayConnectionId, reqwest::Error),
}

#[derive(Clone, Debug)]
pub struct JobAcknowledgement {
    pub id: Uuid,
    pub status: GatewayJobStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayJobStatus {
    Llm(LlmJobStatus),
    Execution(ExecutionJobStatus),
}

impl fmt::Display for GatewayJobStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Llm(status) => status.as_str(),
            Self::Execution(status) => status.as_str(),
        };
        formatter.write_str(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LlmJobStatus {
    Queued,
    Running,
    RetryWait,
    Succeeded,
    Failed,
    Cancelled,
}

impl LlmJobStatus {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "retry_wait" => Some(Self::RetryWait),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::RetryWait => "retry_wait",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionJobStatus {
    Queued,
    Dispatching,
    WaitingResponse,
    Succeeded,
    Failed,
    Unknown,
}

impl ExecutionJobStatus {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "dispatching" => Some(Self::Dispatching),
            "waiting_response" => Some(Self::WaitingResponse),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Dispatching => "dispatching",
            Self::WaitingResponse => "waiting_response",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug)]
pub struct CancellationAcknowledgement {
    pub accepted: bool,
}

#[derive(Debug)]
pub struct GatewayJob {
    pub id: Uuid,
    pub status: GatewayJobStatus,
    pub outcome: GatewayJobOutcome,
    pub execution: Option<ExecutionJobMetadata>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionJobMetadata {
    pub machine_id: Uuid,
    pub runtime_generation_id: Option<Uuid>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GatewayJobIdentityError {
    #[error("execution job metadata is missing")]
    MissingExecutionMetadata,
    #[error("execution job identifies a different machine")]
    MachineMismatch,
    #[error("execution job identifies a different runtime generation")]
    GenerationMismatch,
}

impl GatewayJob {
    pub fn validate_execution_identity(
        &self,
        machine_id: Uuid,
        expected_generation_id: Option<Uuid>,
    ) -> Result<(), GatewayJobIdentityError> {
        let metadata = self
            .execution
            .ok_or(GatewayJobIdentityError::MissingExecutionMetadata)?;
        if metadata.machine_id != machine_id {
            return Err(GatewayJobIdentityError::MachineMismatch);
        }
        if let (Some(expected), Some(actual)) =
            (expected_generation_id, metadata.runtime_generation_id)
            && expected != actual
        {
            return Err(GatewayJobIdentityError::GenerationMismatch);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum GatewayJobOutcome {
    Pending,
    Succeeded(Box<RawValue>),
    Failed {
        response: Option<Box<RawValue>>,
        error: Option<Value>,
    },
    Cancelled,
    Unknown(Value),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobWire {
    id: Uuid,
    status: String,
    response: Option<Box<RawValue>>,
    error: Option<Value>,
    machine_id: Option<Uuid>,
    runtime_generation_id: Option<Uuid>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayCallbackConfig {
    pub id: GatewayConnectionId,
    pub kind: GatewayKind,
    pub secrets: Vec<String>,
}

impl GatewayCallbackConfig {
    pub fn validate(&self) -> Result<(), GatewayConfigError> {
        if self.id.0.is_empty()
            || self.secrets.is_empty()
            || self.secrets.iter().any(String::is_empty)
        {
            return Err(GatewayConfigError::Invalid(
                self.id.clone(),
                "callback verification requires a connection ID and at least one non-empty secret",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct CallbackNotification {
    pub event_id: Uuid,
    pub job_id: Uuid,
    pub event_type: String,
}

#[derive(Clone, Copy, Debug)]
pub struct CallbackHeaders<'a> {
    pub event_id: &'a str,
    pub timestamp: &'a str,
    pub signature: &'a str,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum CallbackVerificationError {
    #[error("callback verification is not configured")]
    NotConfigured,
    #[error("callback headers are invalid")]
    InvalidHeaders,
    #[error("callback timestamp is outside the accepted window")]
    Stale,
    #[error("callback signature is invalid")]
    InvalidSignature,
    #[error("callback payload is invalid")]
    InvalidPayload,
}

#[derive(Clone, Default)]
pub struct CallbackVerifierRegistry {
    entries: HashMap<GatewayConnectionId, GatewayCallbackConfig>,
}

impl CallbackVerifierRegistry {
    pub fn from_configs(configs: Vec<GatewayCallbackConfig>) -> Result<Self, GatewayConfigError> {
        let mut entries = HashMap::new();
        for config in configs {
            config.validate()?;
            let id = config.id.clone();
            if entries.insert(id.clone(), config).is_some() {
                return Err(GatewayConfigError::Duplicate(id));
            }
        }
        Ok(Self { entries })
    }

    pub fn verify(
        &self,
        connection: &GatewayConnectionId,
        kind: GatewayKind,
        headers: CallbackHeaders<'_>,
        body: &[u8],
        now_unix: i64,
        tolerance: Duration,
    ) -> Result<CallbackNotification, CallbackVerificationError> {
        let config = self
            .entries
            .get(connection)
            .ok_or(CallbackVerificationError::NotConfigured)?;
        if config.kind != kind {
            return Err(CallbackVerificationError::NotConfigured);
        }
        let event_id: Uuid = headers
            .event_id
            .parse()
            .map_err(|_| CallbackVerificationError::InvalidHeaders)?;
        let timestamp: i64 = headers
            .timestamp
            .parse()
            .map_err(|_| CallbackVerificationError::InvalidHeaders)?;
        if now_unix.abs_diff(timestamp) > tolerance.as_secs() {
            return Err(CallbackVerificationError::Stale);
        }
        let presented = headers
            .signature
            .strip_prefix("v1=")
            .and_then(|value| hex::decode(value).ok())
            .filter(|value| value.len() == 32)
            .ok_or(CallbackVerificationError::InvalidHeaders)?;
        let valid = config.secrets.iter().fold(false, |matched, secret| {
            let mut mac =
                Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key");
            mac.update(format!("{}.{}.", headers.timestamp, headers.event_id).as_bytes());
            mac.update(body);
            matched | bool::from(mac.finalize().into_bytes().as_slice().ct_eq(&presented))
        });
        if !valid {
            return Err(CallbackVerificationError::InvalidSignature);
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct LlmWire {
            event_id: Uuid,
            #[serde(rename = "type")]
            event_type: String,
            job_id: Uuid,
            completed_at: chrono::DateTime<chrono::Utc>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct ExecutionWire {
            schema_version: u32,
            event_id: Uuid,
            #[serde(rename = "type")]
            event_type: String,
            job_id: Uuid,
            machine_id: Uuid,
            completed_at: chrono::DateTime<chrono::Utc>,
        }
        let (payload_event_id, job_id, event_type) = match kind {
            GatewayKind::Llm => {
                let wire: LlmWire = serde_json::from_slice(body)
                    .map_err(|_| CallbackVerificationError::InvalidPayload)?;
                let _ = wire.completed_at;
                if !matches!(
                    wire.event_type.as_str(),
                    "job.succeeded" | "job.failed" | "job.cancelled"
                ) {
                    return Err(CallbackVerificationError::InvalidPayload);
                }
                (wire.event_id, wire.job_id, wire.event_type)
            }
            GatewayKind::Execution => {
                let wire: ExecutionWire = serde_json::from_slice(body)
                    .map_err(|_| CallbackVerificationError::InvalidPayload)?;
                let _ = (wire.machine_id, wire.completed_at);
                if wire.schema_version != 2
                    || !matches!(
                        wire.event_type.as_str(),
                        "job.succeeded" | "job.failed" | "job.unknown"
                    )
                {
                    return Err(CallbackVerificationError::InvalidPayload);
                }
                (wire.event_id, wire.job_id, wire.event_type)
            }
        };
        if payload_event_id != event_id {
            return Err(CallbackVerificationError::InvalidPayload);
        }
        Ok(CallbackNotification {
            event_id,
            job_id,
            event_type,
        })
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("gateway rejected the request: {code}: {message}")]
    Rejected {
        status: u16,
        code: String,
        message: String,
    },
    #[error("gateway request may be retried: {0}")]
    Retryable(GatewayFailure),
    #[error("gateway acceptance is uncertain: {0}")]
    Uncertain(GatewayFailure),
    #[error("gateway request could not be constructed: {code}: {message}")]
    InvalidRequest { code: String, message: String },
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("{code}: {message}")]
pub struct GatewayFailure {
    pub status: Option<u16>,
    pub code: String,
    pub message: String,
}

impl GatewayFailure {
    fn new(status: Option<u16>, code: &str, message: &str) -> Self {
        Self {
            status,
            code: code.to_owned(),
            message: message.to_owned(),
        }
    }
}

#[async_trait]
pub trait GatewayClient: Send + Sync {
    fn kind(&self) -> GatewayKind;
    async fn submit(&self, request: &RawValue) -> Result<JobAcknowledgement, GatewayError>;
    async fn find_by_idempotency_key(
        &self,
        _idempotency_key: &str,
    ) -> Result<Option<JobAcknowledgement>, GatewayError> {
        Ok(None)
    }
    async fn cancel(&self, _job_id: Uuid) -> Result<CancellationAcknowledgement, GatewayError> {
        Err(GatewayError::Rejected {
            status: 400,
            code: "unsupported_operation".into(),
            message: "this gateway does not support job cancellation".into(),
        })
    }
    async fn get_job(&self, _job_id: Uuid) -> Result<GatewayJob, GatewayError> {
        Err(GatewayError::InvalidRequest {
            code: "job_retrieval_unsupported".into(),
            message: "this gateway client does not support job retrieval".into(),
        })
    }
}

#[derive(Clone, Default)]
pub struct GatewayRegistry {
    clients: HashMap<GatewayConnectionId, Arc<dyn GatewayClient>>,
}

impl GatewayRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_configs(configs: Vec<GatewayConnectionConfig>) -> Result<Self, GatewayConfigError> {
        let mut registry = Self::new();
        for config in configs {
            let id = config.id.clone();
            let client = Arc::new(HttpGatewayClient::new(config)?);
            registry.register(id, client)?;
        }
        Ok(registry)
    }

    pub fn register(
        &mut self,
        id: GatewayConnectionId,
        client: Arc<dyn GatewayClient>,
    ) -> Result<(), GatewayConfigError> {
        if self.clients.contains_key(&id) {
            return Err(GatewayConfigError::Duplicate(id));
        }
        self.clients.insert(id, client);
        Ok(())
    }

    pub fn get(&self, id: &GatewayConnectionId) -> Option<Arc<dyn GatewayClient>> {
        self.clients.get(id).cloned()
    }
}

struct HttpGatewayClient {
    kind: GatewayKind,
    base_url: Url,
    bearer_token: String,
    client: reqwest::Client,
}

impl HttpGatewayClient {
    fn new(config: GatewayConnectionConfig) -> Result<Self, GatewayConfigError> {
        config.validate()?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| GatewayConfigError::Client(config.id.clone(), error))?;
        Ok(Self {
            kind: config.kind,
            base_url: config.base_url,
            bearer_token: config.bearer_token,
            client,
        })
    }

    fn endpoint(&self, path: &str) -> Result<Url, GatewayError> {
        self.base_url
            .join(path)
            .map_err(|_| GatewayError::InvalidRequest {
                code: "invalid_gateway_endpoint".into(),
                message: "gateway endpoint could not be constructed".into(),
            })
    }

    async fn response_value(
        response: reqwest::Response,
    ) -> Result<(StatusCode, Value), GatewayError> {
        let status = response.status();
        let bytes = response.bytes().await.map_err(|_| {
            uncertain(
                None,
                "response_read_failed",
                "gateway response could not be read",
            )
        })?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
            if status.is_success() {
                uncertain(
                    Some(status.as_u16()),
                    "invalid_success_response",
                    "gateway returned an invalid success response",
                )
            } else {
                classify_status(
                    status,
                    "invalid_error_response",
                    "gateway returned an invalid error response",
                )
            }
        })?;
        if status.is_success() {
            return Ok((status, value));
        }
        let code = value
            .pointer("/error/code")
            .and_then(Value::as_str)
            .unwrap_or("gateway_error");
        let message = value
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("gateway rejected the request");
        Err(classify_status(status, code, message))
    }
}

#[async_trait]
impl GatewayClient for HttpGatewayClient {
    fn kind(&self) -> GatewayKind {
        self.kind
    }

    async fn submit(&self, request: &RawValue) -> Result<JobAcknowledgement, GatewayError> {
        let response = self
            .client
            .post(self.endpoint("v1/jobs")?)
            .bearer_auth(&self.bearer_token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(request.get().to_owned())
            .send()
            .await
            .map_err(classify_transport)?;
        let (status, value) = Self::response_value(response).await?;
        acknowledgement(self.kind, status, &value)
    }

    async fn find_by_idempotency_key(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<JobAcknowledgement>, GatewayError> {
        let response = self
            .client
            .get(self.endpoint("v1/jobs")?)
            .bearer_auth(&self.bearer_token)
            .query(&[("idempotencyKey", idempotency_key), ("limit", "1")])
            .send()
            .await
            .map_err(classify_transport)?;
        let (status, value) = Self::response_value(response).await?;
        let jobs = value.get("data").and_then(Value::as_array).ok_or_else(|| {
            uncertain(
                Some(status.as_u16()),
                "invalid_lookup_response",
                "gateway lookup response has no job array",
            )
        })?;
        if jobs.is_empty() {
            return Ok(None);
        }
        if jobs.len() != 1 {
            return Err(uncertain(
                Some(status.as_u16()),
                "ambiguous_lookup_response",
                "gateway lookup returned more than one job",
            ));
        }
        let job = &jobs[0];
        if job.get("idempotencyKey").and_then(Value::as_str) != Some(idempotency_key) {
            return Err(uncertain(
                Some(status.as_u16()),
                "lookup_identity_mismatch",
                "gateway lookup returned a job for a different idempotency key",
            ));
        }
        acknowledgement(self.kind, status, job).map(Some)
    }

    async fn cancel(&self, job_id: Uuid) -> Result<CancellationAcknowledgement, GatewayError> {
        if self.kind != GatewayKind::Llm {
            return Err(GatewayError::Rejected {
                status: 400,
                code: "unsupported_operation".into(),
                message: "this gateway does not support job cancellation".into(),
            });
        }
        let response = self
            .client
            .post(self.endpoint(&format!("v1/jobs/{job_id}/cancel"))?)
            .bearer_auth(&self.bearer_token)
            .send()
            .await
            .map_err(classify_transport)?;
        let (http_status, value) = Self::response_value(response).await?;
        let response_id = value
            .get("id")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<Uuid>().ok())
            .ok_or_else(|| {
                uncertain(
                    Some(http_status.as_u16()),
                    "invalid_cancellation_response",
                    "gateway cancellation response has no valid job ID",
                )
            })?;
        if response_id != job_id {
            return Err(uncertain(
                Some(http_status.as_u16()),
                "cancellation_identity_mismatch",
                "gateway cancellation response identifies a different job",
            ));
        }
        let status = value
            .get("status")
            .and_then(Value::as_str)
            .and_then(LlmJobStatus::parse)
            .ok_or_else(|| {
                uncertain(
                    Some(http_status.as_u16()),
                    "invalid_cancellation_response",
                    "gateway cancellation response has no recognized job status",
                )
            })?;
        let cancel_requested_at = value.get("cancelRequestedAt").ok_or_else(|| {
            uncertain(
                Some(http_status.as_u16()),
                "invalid_cancellation_response",
                "gateway cancellation response has no cancellation timestamp",
            )
        })?;
        let accepted = match cancel_requested_at {
            Value::Null if status.is_terminal() => false,
            Value::String(value) if chrono::DateTime::parse_from_rfc3339(value).is_ok() => true,
            _ => {
                return Err(uncertain(
                    Some(http_status.as_u16()),
                    "invalid_cancellation_response",
                    "gateway cancellation response has an invalid cancellation timestamp",
                ));
            }
        };
        Ok(CancellationAcknowledgement { accepted })
    }

    async fn get_job(&self, job_id: Uuid) -> Result<GatewayJob, GatewayError> {
        let response = self
            .client
            .get(self.endpoint(&format!("v1/jobs/{job_id}"))?)
            .bearer_auth(&self.bearer_token)
            .send()
            .await
            .map_err(classify_transport)?;
        let http_status = response.status();
        let bytes = response.bytes().await.map_err(|_| {
            uncertain(
                None,
                "response_read_failed",
                "gateway response could not be read",
            )
        })?;
        if !http_status.is_success() {
            let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            return Err(classify_status(
                http_status,
                value
                    .pointer("/error/code")
                    .and_then(Value::as_str)
                    .unwrap_or("gateway_error"),
                value
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("gateway rejected the request"),
            ));
        }
        let mut wire: JobWire = serde_json::from_slice(&bytes).map_err(|_| {
            uncertain(
                Some(http_status.as_u16()),
                "invalid_job_response",
                "gateway returned an invalid job response",
            )
        })?;
        if wire.id != job_id {
            return Err(uncertain(
                Some(http_status.as_u16()),
                "job_identity_mismatch",
                "gateway returned a different job",
            ));
        }
        let status = match self.kind {
            GatewayKind::Llm => LlmJobStatus::parse(&wire.status).map(GatewayJobStatus::Llm),
            GatewayKind::Execution => {
                ExecutionJobStatus::parse(&wire.status).map(GatewayJobStatus::Execution)
            }
        }
        .ok_or_else(|| {
            uncertain(
                Some(http_status.as_u16()),
                "invalid_job_response",
                "gateway returned an unknown job status",
            )
        })?;
        let execution = match self.kind {
            GatewayKind::Llm => None,
            GatewayKind::Execution => Some(ExecutionJobMetadata {
                machine_id: wire.machine_id.ok_or_else(|| {
                    uncertain(
                        Some(http_status.as_u16()),
                        "invalid_job_response",
                        "execution job has no machine identity",
                    )
                })?,
                runtime_generation_id: wire.runtime_generation_id,
            }),
        };
        let invalid =
            |message| uncertain(Some(http_status.as_u16()), "invalid_job_response", message);
        let outcome = match status {
            GatewayJobStatus::Llm(
                LlmJobStatus::Queued | LlmJobStatus::Running | LlmJobStatus::RetryWait,
            )
            | GatewayJobStatus::Execution(
                ExecutionJobStatus::Queued
                | ExecutionJobStatus::Dispatching
                | ExecutionJobStatus::WaitingResponse,
            ) => {
                if wire.response.is_some() || wire.error.is_some() {
                    return Err(invalid("pending job has a terminal outcome"));
                }
                GatewayJobOutcome::Pending
            }
            GatewayJobStatus::Llm(LlmJobStatus::Succeeded) => {
                if wire.error.is_some() {
                    return Err(invalid("succeeded LLM job has an error"));
                }
                let response = wire
                    .response
                    .ok_or_else(|| invalid("succeeded LLM job has no response"))?;
                serde_json::from_str::<AssistantResponse>(response.get())
                    .map_err(|_| invalid("succeeded LLM job has an invalid response"))?;
                GatewayJobOutcome::Succeeded(response)
            }
            GatewayJobStatus::Execution(ExecutionJobStatus::Succeeded) => {
                if wire.error.is_some() {
                    return Err(invalid("succeeded execution job has an error"));
                }
                let response = wire
                    .response
                    .take()
                    .ok_or_else(|| invalid("succeeded execution job has no response"))?;
                let response = validate_execution_response(
                    response,
                    job_id,
                    wire.machine_id,
                    wire.runtime_generation_id,
                    true,
                    http_status,
                )?;
                GatewayJobOutcome::Succeeded(response)
            }
            GatewayJobStatus::Llm(LlmJobStatus::Failed) => {
                if wire.response.is_some() || !valid_terminal_error(wire.error.as_ref()) {
                    return Err(invalid("failed LLM job has an invalid outcome"));
                }
                GatewayJobOutcome::Failed {
                    response: None,
                    error: wire.error,
                }
            }
            GatewayJobStatus::Execution(ExecutionJobStatus::Failed) => {
                if wire.response.is_some() == wire.error.is_some() {
                    return Err(invalid(
                        "failed execution job must have exactly one response or error",
                    ));
                }
                if let Some(response) = wire.response {
                    let response = validate_execution_response(
                        response,
                        job_id,
                        wire.machine_id,
                        wire.runtime_generation_id,
                        false,
                        http_status,
                    )?;
                    GatewayJobOutcome::Failed {
                        response: Some(response),
                        error: None,
                    }
                } else {
                    if !valid_terminal_error(wire.error.as_ref()) {
                        return Err(invalid("failed execution job has an invalid error"));
                    }
                    GatewayJobOutcome::Failed {
                        response: None,
                        error: wire.error,
                    }
                }
            }
            GatewayJobStatus::Llm(LlmJobStatus::Cancelled) => {
                if wire.response.is_some() || wire.error.is_some() {
                    return Err(invalid("cancelled LLM job has a result or error"));
                }
                GatewayJobOutcome::Cancelled
            }
            GatewayJobStatus::Execution(ExecutionJobStatus::Unknown) => {
                if wire.response.is_some() || !valid_terminal_error(wire.error.as_ref()) {
                    return Err(invalid("unknown execution job has an invalid outcome"));
                }
                if wire.runtime_generation_id.is_none() {
                    return Err(invalid(
                        "unknown execution job has no runtime generation identity",
                    ));
                }
                GatewayJobOutcome::Unknown(wire.error.expect("validated as present"))
            }
        };
        Ok(GatewayJob {
            id: wire.id,
            status,
            outcome,
            execution,
        })
    }
}

fn valid_terminal_error(error: Option<&Value>) -> bool {
    error.is_some_and(|value| {
        value
            .get("code")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
            && value
                .get("message")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
    })
}

fn validate_execution_response(
    response: Box<RawValue>,
    job_id: Uuid,
    machine_id: Option<Uuid>,
    runtime_generation_id: Option<Uuid>,
    succeeded: bool,
    http_status: StatusCode,
) -> Result<Box<RawValue>, GatewayError> {
    let invalid = |message| uncertain(Some(http_status.as_u16()), "invalid_job_response", message);
    if machine_id.is_none() {
        return Err(invalid("execution job has no machine identity"));
    }
    let generation = runtime_generation_id
        .ok_or_else(|| invalid("execution job has no runtime generation identity"))?;
    let typed: execution::Response = serde_json::from_str(response.get())
        .map_err(|_| invalid("execution job has an invalid protocol response"))?;
    if typed.protocol_version != execution::VERSION {
        return Err(invalid(
            "execution response uses an unsupported protocol version",
        ));
    }
    let expected_request_id = job_id.to_string();
    if typed.request_id.as_deref() != Some(expected_request_id.as_str()) {
        return Err(invalid("execution response identifies a different request"));
    }
    if typed.generation_id != generation {
        return Err(invalid(
            "execution response identifies a different runtime generation",
        ));
    }
    if typed.succeeded() != succeeded {
        return Err(invalid(
            "execution job status disagrees with its protocol response",
        ));
    }
    Ok(response)
}

fn classify_transport(error: reqwest::Error) -> GatewayError {
    if error.is_connect() {
        retryable(
            None,
            "gateway_connect_failed",
            "could not connect to gateway",
        )
    } else if error.is_builder() {
        GatewayError::InvalidRequest {
            code: "gateway_request_invalid".into(),
            message: "gateway request could not be constructed".into(),
        }
    } else {
        uncertain(
            None,
            "gateway_transport_failed",
            "gateway request outcome is unknown",
        )
    }
}

fn classify_status(status: StatusCode, code: &str, message: &str) -> GatewayError {
    if status == StatusCode::TOO_MANY_REQUESTS {
        retryable(Some(status.as_u16()), code, message)
    } else if status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT {
        uncertain(Some(status.as_u16()), code, message)
    } else {
        GatewayError::Rejected {
            status: status.as_u16(),
            code: code.to_owned(),
            message: message.to_owned(),
        }
    }
}

fn acknowledgement(
    kind: GatewayKind,
    http_status: StatusCode,
    value: &Value,
) -> Result<JobAcknowledgement, GatewayError> {
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<Uuid>().ok())
        .ok_or_else(|| {
            uncertain(
                Some(http_status.as_u16()),
                "invalid_job_acknowledgement",
                "gateway response has no valid job ID",
            )
        })?;
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .and_then(|value| match kind {
            GatewayKind::Llm => LlmJobStatus::parse(value).map(GatewayJobStatus::Llm),
            GatewayKind::Execution => {
                ExecutionJobStatus::parse(value).map(GatewayJobStatus::Execution)
            }
        })
        .ok_or_else(|| {
            uncertain(
                Some(http_status.as_u16()),
                "invalid_job_acknowledgement",
                "gateway response has no recognized job status",
            )
        })?;
    Ok(JobAcknowledgement { id, status })
}

fn retryable(status: Option<u16>, code: &str, message: &str) -> GatewayError {
    GatewayError::Retryable(GatewayFailure::new(status, code, message))
}

fn uncertain(status: Option<u16>, code: &str, message: &str) -> GatewayError {
    GatewayError::Uncertain(GatewayFailure::new(status, code, message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Bytes, extract::OriginalUri, http::HeaderMap, routing::post};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn config(id: &str) -> GatewayConnectionConfig {
        GatewayConnectionConfig {
            id: GatewayConnectionId(id.into()),
            kind: GatewayKind::Llm,
            base_url: "https://gateway.example.test/".parse().unwrap(),
            bearer_token: "secret-token".into(),
            timeout_ms: 1000,
        }
    }

    fn callback_signature(secret: &str, timestamp: &str, event_id: Uuid, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(format!("{timestamp}.{event_id}.").as_bytes());
        mac.update(body);
        format!("v1={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn configuration_is_validated_and_secrets_are_redacted() {
        let valid = config("llm");
        assert!(valid.validate().is_ok());
        assert!(!format!("{valid:?}").contains("secret-token"));

        let mut invalid = config("llm");
        invalid.base_url = "https://gateway.example.test/api".parse().unwrap();
        assert!(invalid.validate().is_err());

        let mut invalid = config("llm");
        invalid.bearer_token = "invalid\0token".into();
        assert!(invalid.validate().is_err());

        let mut invalid = config("llm");
        invalid.base_url = "https://name:password@gateway.example.test/"
            .parse()
            .unwrap();
        assert!(invalid.validate().is_err());
        let debug = format!("{invalid:?}");
        assert!(!debug.contains("name:password"));
        assert!(!debug.contains("password"));
    }

    #[test]
    fn response_classes_preserve_submission_uncertainty() {
        assert!(matches!(
            classify_status(StatusCode::CONFLICT, "machine_offline", "offline"),
            GatewayError::Rejected { .. }
        ));
        let GatewayError::Retryable(failure) =
            classify_status(StatusCode::TOO_MANY_REQUESTS, "resource_limit", "busy")
        else {
            panic!("429 must be retryable")
        };
        assert_eq!(failure.status, Some(429));
        assert_eq!(failure.code, "resource_limit");
        let GatewayError::Uncertain(failure) =
            classify_status(StatusCode::SERVICE_UNAVAILABLE, "internal_error", "failed")
        else {
            panic!("503 must be uncertain")
        };
        assert_eq!(failure.status, Some(503));
        assert_eq!(failure.code, "internal_error");
    }

    #[test]
    fn callbacks_verify_exact_gateway_contracts_and_rotated_secrets() {
        let connection = GatewayConnectionId("execution".into());
        let registry = CallbackVerifierRegistry::from_configs(vec![GatewayCallbackConfig {
            id: connection.clone(),
            kind: GatewayKind::Execution,
            secrets: vec!["current".into(), "previous".into()],
        }])
        .unwrap();
        let event_id = Uuid::new_v4();
        let job_id = Uuid::new_v4();
        let body = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "eventId": event_id,
            "type": "job.succeeded",
            "jobId": job_id,
            "machineId": Uuid::new_v4(),
            "completedAt": "2026-09-15T00:00:00Z"
        }))
        .unwrap();
        let timestamp = "1789430400";
        let signature = callback_signature("previous", timestamp, event_id, &body);
        let notification = registry
            .verify(
                &connection,
                GatewayKind::Execution,
                CallbackHeaders {
                    event_id: &event_id.to_string(),
                    timestamp,
                    signature: &signature,
                },
                &body,
                1_789_430_400,
                Duration::from_secs(300),
            )
            .unwrap();
        assert_eq!(notification.job_id, job_id);

        let mut changed_body = body.clone();
        changed_body.push(b' ');
        assert!(matches!(
            registry.verify(
                &connection,
                GatewayKind::Execution,
                CallbackHeaders {
                    event_id: &event_id.to_string(),
                    timestamp,
                    signature: &signature,
                },
                &changed_body,
                1_789_430_400,
                Duration::from_secs(300),
            ),
            Err(CallbackVerificationError::InvalidSignature)
        ));
        assert!(matches!(
            registry.verify(
                &connection,
                GatewayKind::Execution,
                CallbackHeaders {
                    event_id: &event_id.to_string(),
                    timestamp,
                    signature: &signature,
                },
                &body,
                1_789_431_000,
                Duration::from_secs(300),
            ),
            Err(CallbackVerificationError::Stale)
        ));

        let legacy = serde_json::to_vec(&json!({
            "eventId": event_id,
            "type": "job.succeeded",
            "jobId": job_id,
            "machineId": Uuid::new_v4(),
            "completedAt": "2026-09-15T00:00:00Z",
            "response": null,
            "error": null
        }))
        .unwrap();
        let legacy_signature = callback_signature("current", timestamp, event_id, &legacy);
        assert!(matches!(
            registry.verify(
                &connection,
                GatewayKind::Execution,
                CallbackHeaders {
                    event_id: &event_id.to_string(),
                    timestamp,
                    signature: &legacy_signature,
                },
                &legacy,
                1_789_430_400,
                Duration::from_secs(300),
            ),
            Err(CallbackVerificationError::InvalidPayload)
        ));
    }

    #[tokio::test]
    async fn http_client_uses_gateway_routes_authentication_and_lookup() {
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured = requests.clone();
        let job_id = Uuid::new_v4();
        let app = Router::new()
            .route(
                "/v1/jobs",
                post({
                    let captured = captured.clone();
                    move |headers: HeaderMap, body: Bytes| async move {
                        captured.lock().unwrap().push(format!(
                            "{} {}",
                            headers.get("authorization").unwrap().to_str().unwrap(),
                            String::from_utf8(body.to_vec()).unwrap()
                        ));
                        axum::Json(json!({"id": job_id, "status": "queued"}))
                    }
                })
                .get({
                    let captured = captured.clone();
                    move |OriginalUri(uri): OriginalUri| async move {
                        captured.lock().unwrap().push(uri.to_string());
                        axum::Json(json!({"data":[{
                            "id":job_id,
                            "idempotencyKey":"operation-1",
                            "status":"running"
                        }], "nextCursor": null}))
                    }
                }),
            )
            .route(
                "/v1/jobs/{id}/cancel",
                post(move || async move {
                    axum::Json(json!({
                        "id": job_id,
                        "status": "running",
                        "cancelRequestedAt": "2026-09-15T00:00:00Z"
                    }))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HttpGatewayClient::new(GatewayConnectionConfig {
            id: GatewayConnectionId("llm".into()),
            kind: GatewayKind::Llm,
            base_url: format!("http://{address}/").parse().unwrap(),
            bearer_token: "secret-token".into(),
            timeout_ms: 1000,
        })
        .unwrap();
        let request = RawValue::from_string(r#"{"idempotencyKey":"operation-1"}"#.into()).unwrap();
        assert_eq!(client.submit(&request).await.unwrap().id, job_id);
        assert_eq!(
            client
                .find_by_idempotency_key("operation-1")
                .await
                .unwrap()
                .unwrap()
                .id,
            job_id
        );
        assert!(client.cancel(job_id).await.unwrap().accepted);
        let requests = requests.lock().unwrap();
        assert!(requests[0].starts_with("Bearer secret-token "));
        assert!(requests[1].contains("idempotencyKey=operation-1"));
        server.abort();
    }

    async fn fixture_client_for(
        kind: GatewayKind,
        response: Value,
    ) -> (HttpGatewayClient, tokio::task::JoinHandle<()>) {
        let app = Router::new().fallback(move || {
            let response = response.clone();
            async move { axum::Json(response) }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HttpGatewayClient::new(GatewayConnectionConfig {
            id: GatewayConnectionId("llm".into()),
            kind,
            base_url: format!("http://{address}/").parse().unwrap(),
            bearer_token: "secret-token".into(),
            timeout_ms: 1000,
        })
        .unwrap();
        (client, server)
    }

    async fn fixture_client(response: Value) -> (HttpGatewayClient, tokio::task::JoinHandle<()>) {
        fixture_client_for(GatewayKind::Llm, response).await
    }

    async fn fixture_raw_client_for(
        kind: GatewayKind,
        response: String,
    ) -> (HttpGatewayClient, tokio::task::JoinHandle<()>) {
        let app = Router::new().fallback(move || {
            let response = response.clone();
            async move {
                (
                    [(reqwest::header::CONTENT_TYPE, "application/json")],
                    response,
                )
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HttpGatewayClient::new(GatewayConnectionConfig {
            id: GatewayConnectionId("fixture".into()),
            kind,
            base_url: format!("http://{address}/").parse().unwrap(),
            bearer_token: "secret-token".into(),
            timeout_ms: 1000,
        })
        .unwrap();
        (client, server)
    }

    #[tokio::test]
    async fn job_retrieval_maps_authoritative_terminal_outcomes() {
        let job_id = Uuid::new_v4();
        let (client, server) = fixture_client(json!({
            "id": job_id,
            "status": "succeeded",
            "response": {
                "id": "response-1",
                "modelId": "model",
                "message": {"role":"assistant","provider":"openai","content":[]},
                "stopReason": "stop",
                "durationMs": 1,
                "timestamp": 1
            },
            "error": null
        }))
        .await;
        assert!(matches!(
            client.get_job(job_id).await.unwrap().outcome,
            GatewayJobOutcome::Succeeded(_)
        ));
        server.abort();

        let generation = Uuid::new_v4();
        let machine = Uuid::new_v4();
        let response = json!({
            "protocol_version": 1,
            "request_id": job_id,
            "generation_id": generation,
            "status": "error",
            "error": {"code":"invalid_argument","message":"bad command"}
        });
        let (client, server) = fixture_client_for(
            GatewayKind::Execution,
            json!({
                "id": job_id,
                "status": "failed",
                "machineId": machine,
                "runtimeGenerationId": generation,
                "response": response,
                "error": null
            }),
        )
        .await;
        let GatewayJobOutcome::Failed { response, error } =
            client.get_job(job_id).await.unwrap().outcome
        else {
            panic!("execution failure was not preserved")
        };
        assert!(response.is_some());
        assert!(error.is_none());
        server.abort();
    }

    #[test]
    fn execution_job_metadata_correlates_with_the_immutable_request() {
        let machine_id = Uuid::new_v4();
        let generation_id = Uuid::new_v4();
        let job = GatewayJob {
            id: Uuid::new_v4(),
            status: GatewayJobStatus::Execution(ExecutionJobStatus::Succeeded),
            outcome: GatewayJobOutcome::Pending,
            execution: Some(ExecutionJobMetadata {
                machine_id,
                runtime_generation_id: Some(generation_id),
            }),
        };
        assert!(
            job.validate_execution_identity(machine_id, Some(generation_id))
                .is_ok()
        );
        assert_eq!(
            job.validate_execution_identity(Uuid::new_v4(), Some(generation_id)),
            Err(GatewayJobIdentityError::MachineMismatch)
        );
        assert_eq!(
            job.validate_execution_identity(machine_id, Some(Uuid::new_v4())),
            Err(GatewayJobIdentityError::GenerationMismatch)
        );
        let job_without_generation = GatewayJob {
            id: Uuid::new_v4(),
            status: GatewayJobStatus::Execution(ExecutionJobStatus::Failed),
            outcome: GatewayJobOutcome::Pending,
            execution: Some(ExecutionJobMetadata {
                machine_id,
                runtime_generation_id: None,
            }),
        };
        assert!(
            job_without_generation
                .validate_execution_identity(machine_id, Some(generation_id))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn terminal_job_shapes_are_gateway_specific() {
        let job_id = Uuid::new_v4();
        for response in [
            json!({"id":job_id,"status":"failed","response":null,"error":"bad"}),
            json!({"id":job_id,"status":"failed","response":{"unexpected":true},"error":{"code":"failed","message":"failed"}}),
            json!({"id":job_id,"status":"cancelled","response":null,"error":{"code":"failed","message":"failed"}}),
            json!({"id":job_id,"status":"running","response":{"unexpected":true},"error":null}),
        ] {
            let (client, server) = fixture_client(response).await;
            assert!(matches!(
                client.get_job(job_id).await,
                Err(GatewayError::Uncertain(GatewayFailure {
                    code,
                    ..
                })) if code == "invalid_job_response"
            ));
            server.abort();
        }

        let (client, server) = fixture_client(json!({
            "id":job_id,
            "status":"failed",
            "response":null,
            "error":{"code":"provider_error","message":"Provider request failed.","retryable":false}
        }))
        .await;
        assert!(matches!(
            client.get_job(job_id).await.unwrap().outcome,
            GatewayJobOutcome::Failed {
                response: None,
                error: Some(_)
            }
        ));
        server.abort();

        for response in [
            json!({
                "id":job_id,"status":"failed","machineId":Uuid::new_v4(),
                "runtimeGenerationId":null,"response":null,"error":["bad"]
            }),
            json!({
                "id":job_id,"status":"unknown","machineId":Uuid::new_v4(),
                "runtimeGenerationId":Uuid::new_v4(),"response":null,
                "error":{"code":"recovery_unavailable"}
            }),
        ] {
            let (client, server) = fixture_client_for(GatewayKind::Execution, response).await;
            assert!(matches!(
                client.get_job(job_id).await,
                Err(GatewayError::Uncertain(_))
            ));
            server.abort();
        }
    }

    #[tokio::test]
    async fn execution_results_require_protocol_correlation_generation_and_status_agreement() {
        let job_id = Uuid::new_v4();
        let machine = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let valid = |status: &str, response_status: &str, result: Value| {
            json!({
                "id":job_id,
                "status":status,
                "machineId":machine,
                "runtimeGenerationId":generation,
                "response":{
                    "protocol_version":execution::VERSION,
                    "request_id":job_id,
                    "generation_id":generation,
                    "status":response_status,
                    "result":result,
                    "error":{"code":"invalid_argument","message":"fixture"}
                },
                "error":null
            })
        };

        let invalid_responses = [
            json!({
                "id":job_id,"status":"succeeded","machineId":machine,
                "runtimeGenerationId":generation,"response":{
                    "protocol_version":999,"request_id":job_id,"generation_id":generation,
                    "status":"ok","result":{}
                },"error":null
            }),
            json!({
                "id":job_id,"status":"succeeded","machineId":machine,
                "runtimeGenerationId":generation,"response":{
                    "protocol_version":execution::VERSION,"request_id":Uuid::new_v4(),
                    "generation_id":generation,"status":"ok","result":{}
                },"error":null
            }),
            json!({
                "id":job_id,"status":"succeeded","machineId":machine,
                "runtimeGenerationId":Uuid::new_v4(),"response":{
                    "protocol_version":execution::VERSION,"request_id":job_id,
                    "generation_id":generation,"status":"ok","result":{}
                },"error":null
            }),
            valid("succeeded", "error", json!({})),
            valid(
                "failed",
                "ok",
                json!({"execution":{"state":"finished","exit":{"code":7}}}),
            ),
        ];
        for response in invalid_responses {
            let (client, server) = fixture_client_for(GatewayKind::Execution, response).await;
            assert!(matches!(
                client.get_job(job_id).await,
                Err(GatewayError::Uncertain(_))
            ));
            server.abort();
        }

        let partial = valid(
            "failed",
            "ok",
            json!({
                "succeeded":false,
                "results":[
                    {"request_id":"one","status":"ok","result":{}},
                    {"request_id":"two","status":"error","error":{"code":"invalid_argument","message":"fixture"}}
                ]
            }),
        );
        let (client, server) = fixture_client_for(GatewayKind::Execution, partial).await;
        assert!(matches!(
            client.get_job(job_id).await.unwrap().outcome,
            GatewayJobOutcome::Failed {
                response: Some(_),
                error: None
            }
        ));
        server.abort();

        let nonzero_exit = valid(
            "succeeded",
            "ok",
            json!({"execution":{"state":"finished","exit":{"code":7}}}),
        );
        let (client, server) = fixture_client_for(GatewayKind::Execution, nonzero_exit).await;
        assert!(matches!(
            client.get_job(job_id).await.unwrap().outcome,
            GatewayJobOutcome::Succeeded(_)
        ));
        server.abort();
    }

    #[tokio::test]
    async fn successful_llm_response_validation_preserves_raw_provider_content() {
        let job_id = Uuid::new_v4();
        let body = format!(
            r#"{{"id":"{job_id}","status":"succeeded","response":{{"id":"response-1","modelId":"model","message":{{"role":"assistant","provider":"openai","content":[{{"type":"text","text":"raw\u0000escape"}}]}},"stopReason":"stop","durationMs":1,"timestamp":1}},"error":null}}"#
        );
        let (client, server) = fixture_raw_client_for(GatewayKind::Llm, body).await;
        let GatewayJobOutcome::Succeeded(response) = client.get_job(job_id).await.unwrap().outcome
        else {
            panic!("valid LLM response was not returned")
        };
        assert!(response.get().contains(r#"raw\u0000escape"#));
        server.abort();

        let (client, server) = fixture_client(json!({
            "id":job_id,
            "status":"succeeded",
            "response":{
                "id":"response-1","modelId":"model",
                "message":{"role":"user","content":[]},
                "stopReason":"stop","durationMs":1,"timestamp":1
            },
            "error":null
        }))
        .await;
        assert!(matches!(
            client.get_job(job_id).await,
            Err(GatewayError::Uncertain(_))
        ));
        server.abort();
    }

    #[tokio::test]
    async fn malformed_success_documents_are_never_authoritative() {
        let (client, server) = fixture_client(json!({})).await;
        assert!(matches!(
            client.cancel(Uuid::new_v4()).await,
            Err(GatewayError::Uncertain(_))
        ));
        assert!(matches!(
            client.find_by_idempotency_key("operation-1").await,
            Err(GatewayError::Uncertain(_))
        ));
        server.abort();

        let (client, server) = fixture_client(json!({
            "id": Uuid::new_v4(),
            "status": "not-a-gateway-status"
        }))
        .await;
        let request = RawValue::from_string("{}".into()).unwrap();
        assert!(matches!(
            client.submit(&request).await,
            Err(GatewayError::Uncertain(_))
        ));
        server.abort();
    }

    #[tokio::test]
    async fn lookup_requires_exact_identity_and_cancellation_requires_a_typed_acknowledgement() {
        let job_id = Uuid::new_v4();
        let (client, server) = fixture_client(json!({"data":[]})).await;
        assert!(
            client
                .find_by_idempotency_key("operation-1")
                .await
                .unwrap()
                .is_none()
        );
        server.abort();

        let (client, server) = fixture_client(json!({"data":[{
            "id":job_id,
            "idempotencyKey":"another-operation",
            "status":"running"
        }]}))
        .await;
        assert!(matches!(
            client.find_by_idempotency_key("operation-1").await,
            Err(GatewayError::Uncertain(_))
        ));
        server.abort();

        let (client, server) = fixture_client(json!({
            "id":job_id,
            "status":"succeeded",
            "cancelRequestedAt":null
        }))
        .await;
        assert!(!client.cancel(job_id).await.unwrap().accepted);
        server.abort();

        let (client, server) = fixture_client(json!({
            "id":job_id,
            "status":"running",
            "cancelRequestedAt":"2026-09-15T00:00:00Z"
        }))
        .await;
        assert!(client.cancel(job_id).await.unwrap().accepted);
        server.abort();

        let (client, server) = fixture_client(json!({
            "id":job_id,
            "status":"running",
            "cancelRequestedAt":null
        }))
        .await;
        assert!(matches!(
            client.cancel(job_id).await,
            Err(GatewayError::Uncertain(_))
        ));
        server.abort();
    }
}
