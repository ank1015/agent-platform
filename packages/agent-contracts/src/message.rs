use serde::{
    Deserialize, Deserializer, Serialize,
    de::{DeserializeOwned, Error as _},
};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

pub type Metadata = Map<String, Value>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    Text {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<Metadata>,
    },
    Image {
        url: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<Metadata>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageDetail {
    Auto,
    Low,
    High,
    Original,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    User {
        #[serde(flatten)]
        base: MessageBase,
        content: Vec<ContentPart>,
    },
    System {
        #[serde(flatten)]
        base: MessageBase,
        content: Vec<TextContent>,
    },
    Assistant {
        #[serde(flatten)]
        base: MessageBase,
        provider: Provider,
        content: Vec<Box<RawValue>>,
    },
    ToolResult {
        #[serde(flatten)]
        base: MessageBase,
        #[serde(rename = "toolName")]
        tool_name: String,
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        content: Vec<ContentPart>,
        outcome: ToolResultOutcome,
        #[serde(skip_serializing_if = "Option::is_none")]
        details: Option<Box<RawValue>>,
    },
    Custom {
        #[serde(flatten)]
        base: MessageBase,
        tag: String,
        data: Box<RawValue>,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageWire {
    role: MessageRole,
    id: Option<String>,
    timestamp: Option<i64>,
    metadata: Option<Metadata>,
    content: Option<Box<RawValue>>,
    provider: Option<Provider>,
    tool_name: Option<String>,
    tool_call_id: Option<String>,
    outcome: Option<ToolResultOutcome>,
    #[serde(default, deserialize_with = "present_raw")]
    details: Option<Box<RawValue>>,
    tag: Option<String>,
    #[serde(default, deserialize_with = "present_raw")]
    data: Option<Box<RawValue>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum MessageRole {
    User,
    System,
    Assistant,
    ToolResult,
    Custom,
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = MessageWire::deserialize(deserializer)?;
        let base = MessageBase {
            id: wire.id,
            timestamp: wire.timestamp,
            metadata: wire.metadata,
        };
        let required = |field: &'static str| D::Error::missing_field(field);
        match wire.role {
            MessageRole::User => Ok(Self::User {
                base,
                content: parse_content(
                    wire.content.as_deref().ok_or_else(|| required("content"))?,
                )?,
            }),
            MessageRole::System => Ok(Self::System {
                base,
                content: parse_content(
                    wire.content.as_deref().ok_or_else(|| required("content"))?,
                )?,
            }),
            MessageRole::Assistant => Ok(Self::Assistant {
                base,
                provider: wire.provider.ok_or_else(|| required("provider"))?,
                content: parse_content(
                    wire.content.as_deref().ok_or_else(|| required("content"))?,
                )?,
            }),
            MessageRole::ToolResult => Ok(Self::ToolResult {
                base,
                tool_name: wire.tool_name.ok_or_else(|| required("toolName"))?,
                tool_call_id: wire.tool_call_id.ok_or_else(|| required("toolCallId"))?,
                content: parse_content(
                    wire.content.as_deref().ok_or_else(|| required("content"))?,
                )?,
                outcome: wire.outcome.ok_or_else(|| required("outcome"))?,
                details: wire.details,
            }),
            MessageRole::Custom => Ok(Self::Custom {
                base,
                tag: wire.tag.ok_or_else(|| required("tag"))?,
                data: wire.data.ok_or_else(|| required("data"))?,
            }),
        }
    }
}

fn parse_content<T: DeserializeOwned, E: serde::de::Error>(raw: &RawValue) -> Result<T, E> {
    serde_json::from_str(raw.get()).map_err(E::custom)
}

fn present_raw<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Box<RawValue>>, D::Error> {
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MessageBase {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Unix timestamp in milliseconds, matching the LLM contracts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextContent {
    #[serde(rename = "type")]
    pub kind: TextContentKind,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextContentKind {
    Text,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Openai,
    Chatgpt,
    Fireworks,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ToolResultOutcome {
    Success,
    Error { error: ToolResultError },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolResultError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Message {
    pub fn user_text(text: impl Into<String>) -> Self {
        Self::User {
            base: MessageBase::default(),
            content: vec![ContentPart::Text {
                text: text.into(),
                metadata: None,
            }],
        }
    }
}
