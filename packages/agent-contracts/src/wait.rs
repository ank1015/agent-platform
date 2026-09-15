use crate::WaitId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitMode {
    External,
    Expiration,
    Either,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WaitSpec {
    pub mode: WaitMode,
    pub payload: Value,
    pub response_schema: Option<Value>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl WaitSpec {
    pub fn validate(&self) -> Result<(), WaitSpecError> {
        match self.mode {
            WaitMode::External if self.expires_at.is_some() => {
                return Err(WaitSpecError::UnexpectedExpiration);
            }
            WaitMode::Expiration | WaitMode::Either if self.expires_at.is_none() => {
                return Err(WaitSpecError::MissingExpiration);
            }
            WaitMode::Expiration if self.response_schema.is_some() => {
                return Err(WaitSpecError::UnexpectedResponseSchema);
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitSpecError {
    MissingExpiration,
    UnexpectedExpiration,
    UnexpectedResponseSchema,
}

impl std::fmt::Display for WaitSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingExpiration => write!(f, "wait mode requires expires_at"),
            Self::UnexpectedExpiration => write!(f, "external-only wait cannot expire"),
            Self::UnexpectedResponseSchema => {
                write!(f, "expiration-only wait cannot have a response schema")
            }
        }
    }
}

impl std::error::Error for WaitSpecError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitStatus {
    Pending,
    Resolved,
    Expired,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WaitRecord {
    pub id: WaitId,
    pub spec: WaitSpec,
    pub status: WaitStatus,
    pub resolution: Option<Value>,
    pub finished_at: Option<DateTime<Utc>>,
}
