use crate::{GatewayConnectionId, Message, OperationId};
use process_execution_protocol as execution;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use serde_json::{Map, Value, value::RawValue};
use uuid::Uuid;

pub type ProviderOptions = Map<String, Value>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Llm,
    Execution,
    LlmCancellation,
    Withdraw,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationOutcomeStatus {
    Succeeded,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperationRequest {
    Llm {
        connection: GatewayConnectionId,
        request: LlmRequest,
    },
    Execution {
        connection: GatewayConnectionId,
        machine_id: Uuid,
        #[serde(skip_serializing_if = "Option::is_none")]
        expected_generation_id: Option<Uuid>,
        request: execution::Payload,
    },
    LlmCancellation {
        target: OperationId,
    },
    Withdraw {
        target: OperationId,
    },
}

#[derive(Deserialize)]
struct OperationRequestWire {
    kind: OperationKind,
    connection: Option<GatewayConnectionId>,
    machine_id: Option<Uuid>,
    expected_generation_id: Option<Uuid>,
    request: Option<Box<RawValue>>,
    target: Option<OperationId>,
}

impl<'de> Deserialize<'de> for OperationRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let wire: OperationRequestWire =
            serde_json::from_str(raw.get()).map_err(D::Error::custom)?;
        match wire.kind {
            OperationKind::Llm => Ok(Self::Llm {
                connection: wire
                    .connection
                    .ok_or_else(|| D::Error::missing_field("connection"))?,
                request: parse_raw(
                    wire.request
                        .ok_or_else(|| D::Error::missing_field("request"))?
                        .as_ref(),
                )?,
            }),
            OperationKind::Execution => Ok(Self::Execution {
                connection: wire
                    .connection
                    .ok_or_else(|| D::Error::missing_field("connection"))?,
                machine_id: wire
                    .machine_id
                    .ok_or_else(|| D::Error::missing_field("machine_id"))?,
                expected_generation_id: wire.expected_generation_id,
                request: parse_raw(
                    wire.request
                        .ok_or_else(|| D::Error::missing_field("request"))?
                        .as_ref(),
                )?,
            }),
            OperationKind::LlmCancellation => Ok(Self::LlmCancellation {
                target: wire
                    .target
                    .ok_or_else(|| D::Error::missing_field("target"))?,
            }),
            OperationKind::Withdraw => Ok(Self::Withdraw {
                target: wire
                    .target
                    .ok_or_else(|| D::Error::missing_field("target"))?,
            }),
        }
    }
}

impl OperationRequest {
    pub fn kind(&self) -> OperationKind {
        match self {
            Self::Llm { .. } => OperationKind::Llm,
            Self::Execution { .. } => OperationKind::Execution,
            Self::LlmCancellation { .. } => OperationKind::LlmCancellation,
            Self::Withdraw { .. } => OperationKind::Withdraw,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum LlmRequest {
    Fresh {
        account_id: Uuid,
        model_id: String,
        instructions: Option<String>,
        messages: Vec<Message>,
        tools: Vec<ToolDefinition>,
        provider_options: ProviderOptions,
    },
    Continuation {
        previous_operation_id: OperationId,
        messages: Vec<Message>,
    },
}

#[derive(Deserialize)]
struct LlmRequestWire {
    mode: LlmRequestMode,
    account_id: Option<Uuid>,
    model_id: Option<String>,
    instructions: Option<String>,
    messages: Box<RawValue>,
    tools: Option<Vec<ToolDefinition>>,
    provider_options: Option<ProviderOptions>,
    previous_operation_id: Option<OperationId>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum LlmRequestMode {
    Fresh,
    Continuation,
}

impl<'de> Deserialize<'de> for LlmRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let wire: LlmRequestWire = serde_json::from_str(raw.get()).map_err(D::Error::custom)?;
        let messages = parse_raw(wire.messages.as_ref())?;
        match wire.mode {
            LlmRequestMode::Fresh => Ok(Self::Fresh {
                account_id: wire
                    .account_id
                    .ok_or_else(|| D::Error::missing_field("account_id"))?,
                model_id: wire
                    .model_id
                    .ok_or_else(|| D::Error::missing_field("model_id"))?,
                instructions: wire.instructions,
                messages,
                tools: wire.tools.ok_or_else(|| D::Error::missing_field("tools"))?,
                provider_options: wire
                    .provider_options
                    .ok_or_else(|| D::Error::missing_field("provider_options"))?,
            }),
            LlmRequestMode::Continuation => Ok(Self::Continuation {
                previous_operation_id: wire
                    .previous_operation_id
                    .ok_or_else(|| D::Error::missing_field("previous_operation_id"))?,
                messages,
            }),
        }
    }
}

fn parse_raw<T: serde::de::DeserializeOwned, E: serde::de::Error>(raw: &RawValue) -> Result<T, E> {
    serde_json::from_str(raw.get()).map_err(E::custom)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolDefinition {
    Function {
        name: String,
        description: String,
        parameters: Map<String, Value>,
        #[serde(rename = "outputSchema", skip_serializing_if = "Option::is_none")]
        output_schema: Option<Map<String, Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        strict: Option<bool>,
    },
    Custom {
        name: String,
        description: String,
        format: CustomToolFormat,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CustomToolFormat {
    pub syntax: GrammarSyntax,
    pub definition: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrammarSyntax {
    Lark,
}

/// A platform operation. `outcome` is absent while it is outstanding.
#[derive(Debug, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: OperationId,
    pub kind: OperationKind,
    pub gateway_job_id: Option<Uuid>,
    pub outcome: Option<OperationOutcome>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum OperationOutcome {
    Succeeded(OperationResult),
    Failed(OperationFailure),
    Cancelled,
    Unknown(OperationFailure),
}

#[derive(Deserialize)]
struct OperationOutcomeWire {
    status: OperationOutcomeStatus,
    value: Option<Box<RawValue>>,
}

impl<'de> Deserialize<'de> for OperationOutcome {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = OperationOutcomeWire::deserialize(deserializer)?;
        let value = || {
            wire.value
                .as_deref()
                .ok_or_else(|| D::Error::missing_field("value"))
        };
        match wire.status {
            OperationOutcomeStatus::Succeeded => Ok(Self::Succeeded(parse_raw(value()?)?)),
            OperationOutcomeStatus::Failed => Ok(Self::Failed(parse_raw(value()?)?)),
            OperationOutcomeStatus::Cancelled => Ok(Self::Cancelled),
            OperationOutcomeStatus::Unknown => Ok(Self::Unknown(parse_raw(value()?)?)),
        }
    }
}

impl OperationOutcome {
    pub fn status(&self) -> OperationOutcomeStatus {
        match self {
            Self::Succeeded(_) => OperationOutcomeStatus::Succeeded,
            Self::Failed(_) => OperationOutcomeStatus::Failed,
            Self::Cancelled => OperationOutcomeStatus::Cancelled,
            Self::Unknown(_) => OperationOutcomeStatus::Unknown,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum OperationResult {
    Llm(Box<AssistantResponse>),
    Execution(execution::Response),
    LlmCancellation { accepted: bool },
    Withdraw { withdrawn: bool },
}

#[derive(Deserialize)]
struct OperationResultWire {
    kind: OperationKind,
    value: Box<RawValue>,
}

impl<'de> Deserialize<'de> for OperationResult {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = OperationResultWire::deserialize(deserializer)?;
        match wire.kind {
            OperationKind::Llm => Ok(Self::Llm(parse_raw(wire.value.as_ref())?)),
            OperationKind::Execution => Ok(Self::Execution(parse_raw(wire.value.as_ref())?)),
            OperationKind::LlmCancellation => {
                #[derive(Deserialize)]
                struct ResultValue {
                    accepted: bool,
                }
                let value: ResultValue = parse_raw(wire.value.as_ref())?;
                Ok(Self::LlmCancellation {
                    accepted: value.accepted,
                })
            }
            OperationKind::Withdraw => {
                #[derive(Deserialize)]
                struct ResultValue {
                    withdrawn: bool,
                }
                let value: ResultValue = parse_raw(wire.value.as_ref())?;
                Ok(Self::Withdraw {
                    withdrawn: value.withdrawn,
                })
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OperationFailure {
    pub source: FailureSource,
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_response: Option<execution::Response>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureSource {
    Admission,
    Gateway,
    Protocol,
    Platform,
}

/// JSON-compatible shape of the LLM gateway's AssistantResponse.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssistantResponse {
    pub id: String,
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_model_id: Option<String>,
    pub message: Message,
    pub stop_reason: StopReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    pub duration_ms: u64,
    pub timestamp: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssistantResponseWire {
    id: String,
    model_id: String,
    resolved_model_id: Option<String>,
    message: Message,
    stop_reason: StopReason,
    usage: Option<Usage>,
    duration_ms: u64,
    timestamp: i64,
}

impl<'de> Deserialize<'de> for AssistantResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = AssistantResponseWire::deserialize(deserializer)?;
        if !matches!(wire.message, Message::Assistant { .. }) {
            return Err(D::Error::custom(
                "assistant response message must have the assistant role",
            ));
        }
        Ok(Self {
            id: wire.id,
            model_id: wire.model_id,
            resolved_model_id: wire.resolved_model_id,
            message: wire.message,
            stop_reason: wire.stop_reason,
            usage: wire.usage,
            duration_ms: wire.duration_ms,
            timestamp: wire.timestamp,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    Stop,
    Length,
    ToolUse,
    Refusal,
    ContentFilter,
    PauseTurn,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<UsageCost>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageCost {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write: Option<f64>,
    pub total: f64,
}
