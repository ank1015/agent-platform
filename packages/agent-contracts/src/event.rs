use crate::{
    EventId, EventSequence, Message, OperationId, OperationKind, OperationOutcomeStatus, SessionId,
    WaitId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use serde_json::{Value, value::RawValue};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Running,
    Waiting,
    Cancelling,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct SessionEvent {
    pub id: EventId,
    pub session_id: SessionId,
    pub sequence: EventSequence,
    pub created_at: DateTime<Utc>,
    #[serde(flatten)]
    pub kind: EventKind,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum EventKind {
    UserMessage {
        message: Message,
    },
    OperationCompleted {
        operation_id: OperationId,
        operation_kind: OperationKind,
        outcome_status: OperationOutcomeStatus,
    },
    CancellationRequested {
        reason: Option<String>,
    },
    WaitResumed {
        wait_id: WaitId,
        reason: WaitResumeReason,
    },
}

#[derive(Deserialize)]
struct EventWire {
    id: Option<EventId>,
    session_id: Option<SessionId>,
    sequence: Option<EventSequence>,
    created_at: Option<DateTime<Utc>>,
    #[serde(rename = "type")]
    event_type: EventType,
    payload: Box<RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum EventType {
    UserMessage,
    OperationCompleted,
    CancellationRequested,
    WaitResumed,
}

impl EventKind {
    fn from_wire<E: serde::de::Error>(wire: &EventWire) -> Result<Self, E> {
        match wire.event_type {
            EventType::UserMessage => {
                #[derive(Deserialize)]
                struct Payload {
                    message: Message,
                }
                let payload: Payload =
                    serde_json::from_str(wire.payload.get()).map_err(E::custom)?;
                Ok(Self::UserMessage {
                    message: payload.message,
                })
            }
            EventType::OperationCompleted => {
                #[derive(Deserialize)]
                struct Payload {
                    operation_id: OperationId,
                    operation_kind: OperationKind,
                    outcome_status: OperationOutcomeStatus,
                }
                let payload: Payload =
                    serde_json::from_str(wire.payload.get()).map_err(E::custom)?;
                Ok(Self::OperationCompleted {
                    operation_id: payload.operation_id,
                    operation_kind: payload.operation_kind,
                    outcome_status: payload.outcome_status,
                })
            }
            EventType::CancellationRequested => {
                #[derive(Deserialize)]
                struct Payload {
                    reason: Option<String>,
                }
                let payload: Payload =
                    serde_json::from_str(wire.payload.get()).map_err(E::custom)?;
                Ok(Self::CancellationRequested {
                    reason: payload.reason,
                })
            }
            EventType::WaitResumed => {
                #[derive(Deserialize)]
                struct Payload {
                    wait_id: WaitId,
                    reason: WaitResumeReason,
                }
                let payload: Payload =
                    serde_json::from_str(wire.payload.get()).map_err(E::custom)?;
                Ok(Self::WaitResumed {
                    wait_id: payload.wait_id,
                    reason: payload.reason,
                })
            }
        }
    }
}

impl<'de> Deserialize<'de> for EventKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let wire: EventWire = serde_json::from_str(raw.get()).map_err(D::Error::custom)?;
        Self::from_wire(&wire)
    }
}

impl<'de> Deserialize<'de> for SessionEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = EventWire::deserialize(deserializer)?;
        let kind = EventKind::from_wire(&wire)?;
        Ok(Self {
            id: wire.id.ok_or_else(|| D::Error::missing_field("id"))?,
            session_id: wire
                .session_id
                .ok_or_else(|| D::Error::missing_field("session_id"))?,
            sequence: wire
                .sequence
                .ok_or_else(|| D::Error::missing_field("sequence"))?,
            created_at: wire
                .created_at
                .ok_or_else(|| D::Error::missing_field("created_at"))?,
            kind,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitResumeReason {
    Resolved,
    Expired,
    Cancelled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionView {
    pub id: SessionId,
    pub project_id: crate::ProjectId,
    pub harness_id: crate::HarnessId,
    pub harness_version: crate::HarnessVersion,
    pub status: SessionStatus,
    pub state_version: crate::StateVersion,
    pub metadata: Value,
}
