use crate::{
    Message, OperationId, OperationRequest, SessionStatus, WaitId, WaitSpec, WaitSpecError,
};
use process_execution_protocol::{Operation as ExecutionOperation, Payload};
use serde_json::Value;
use uuid::Uuid;

pub struct HandlerOutcome<S> {
    pub state: S,
    pub history: Vec<Message>,
    pub operations: Vec<StagedOperation>,
    pub waits: Vec<StagedWait>,
    pub cancelled_waits: Vec<WaitId>,
    pub progress: Vec<Value>,
    pub status: Option<SessionStatus>,
}

pub struct StagedOperation {
    pub id: OperationId,
    pub request: OperationRequest,
}

pub struct StagedWait {
    pub id: WaitId,
    pub spec: WaitSpec,
}

pub struct OutcomeBuilder<S> {
    outcome: HandlerOutcome<S>,
}

impl<S> OutcomeBuilder<S> {
    pub fn new(state: S) -> Self {
        Self {
            outcome: HandlerOutcome {
                state,
                history: Vec::new(),
                operations: Vec::new(),
                waits: Vec::new(),
                cancelled_waits: Vec::new(),
                progress: Vec::new(),
                status: None,
            },
        }
    }

    pub fn state_mut(&mut self) -> &mut S {
        &mut self.outcome.state
    }

    pub fn append_message(&mut self, message: Message) {
        self.outcome.history.push(message);
    }

    pub fn set_status(&mut self, status: SessionStatus) {
        self.outcome.status = Some(status);
    }

    pub fn request_llm(
        &mut self,
        connection: crate::GatewayConnectionId,
        request: crate::LlmRequest,
    ) -> OperationId {
        self.push_operation(OperationRequest::Llm {
            connection,
            request,
        })
    }

    pub fn request_execution(
        &mut self,
        connection: crate::GatewayConnectionId,
        machine_id: Uuid,
        request: Payload,
    ) -> Result<OperationId, OutcomeError> {
        validate_execution_payload(&request)?;
        Ok(self.push_operation(OperationRequest::Execution {
            connection,
            machine_id,
            expected_generation_id: None,
            request,
        }))
    }

    pub fn request_execution_for_generation(
        &mut self,
        connection: crate::GatewayConnectionId,
        machine_id: Uuid,
        expected_generation_id: Uuid,
        request: Payload,
    ) -> Result<OperationId, OutcomeError> {
        validate_execution_payload(&request)?;
        Ok(self.push_operation(OperationRequest::Execution {
            connection,
            machine_id,
            expected_generation_id: Some(expected_generation_id),
            request,
        }))
    }

    pub fn start_execution(
        &mut self,
        connection: crate::GatewayConnectionId,
        machine_id: Uuid,
        params: crate::execution::StartParams,
    ) -> Result<OperationId, OutcomeError> {
        self.request_execution(
            connection,
            machine_id,
            Payload::Single(ExecutionOperation::Start(params)),
        )
    }

    pub fn observe_execution(
        &mut self,
        connection: crate::GatewayConnectionId,
        machine_id: Uuid,
        params: crate::execution::ObserveParams,
    ) -> Result<OperationId, OutcomeError> {
        self.request_execution(
            connection,
            machine_id,
            Payload::Single(ExecutionOperation::Observe(params)),
        )
    }

    pub fn control_execution(
        &mut self,
        connection: crate::GatewayConnectionId,
        machine_id: Uuid,
        operation: ExecutionOperation,
    ) -> Result<OperationId, OutcomeError> {
        self.request_execution(connection, machine_id, Payload::Single(operation))
    }

    pub fn request_llm_cancellation(&mut self, target: OperationId) -> OperationId {
        self.push_operation(OperationRequest::LlmCancellation { target })
    }

    pub fn withdraw_operation(&mut self, target: OperationId) -> OperationId {
        self.push_operation(OperationRequest::Withdraw { target })
    }

    pub fn create_wait(&mut self, spec: WaitSpec) -> Result<WaitId, OutcomeError> {
        spec.validate().map_err(OutcomeError::InvalidWait)?;
        let id = WaitId::new();
        self.outcome.waits.push(StagedWait { id, spec });
        Ok(id)
    }

    pub fn cancel_wait(&mut self, wait_id: WaitId) {
        self.outcome.cancelled_waits.push(wait_id);
    }

    pub fn emit_progress(&mut self, payload: Value) {
        self.outcome.progress.push(payload);
    }

    pub fn finish(self) -> HandlerOutcome<S> {
        self.outcome
    }

    fn push_operation(&mut self, request: OperationRequest) -> OperationId {
        let id = OperationId::new();
        self.outcome
            .operations
            .push(StagedOperation { id, request });
        id
    }
}

fn validate_execution_payload(request: &Payload) -> Result<(), OutcomeError> {
    let shutdown = match request {
        Payload::Single(operation) => matches!(operation, ExecutionOperation::Shutdown),
        Payload::Batch(batch) => batch
            .operations
            .iter()
            .any(|item| matches!(item.operation, ExecutionOperation::Shutdown)),
    };
    if shutdown {
        Err(OutcomeError::UnsupportedExecutionOperation)
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub enum OutcomeError {
    InvalidWait(WaitSpecError),
    UnsupportedExecutionOperation,
}

impl std::fmt::Display for OutcomeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidWait(error) => error.fmt(f),
            Self::UnsupportedExecutionOperation => write!(
                f,
                "runtime.shutdown is not supported by the execution gateway"
            ),
        }
    }
}

impl std::error::Error for OutcomeError {}
