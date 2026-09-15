use crate::SerializedOutcome;
use agent_contracts::{
    LlmRequest, OperationId, OperationKind, OperationRequest, SessionId, StagedOperation,
};
use agent_store::{NewHistoryEntry, NewOperation, NewWait, OutcomeCommit, Store};
use std::collections::{HashMap, HashSet};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OutcomePreparationError {
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Store(#[from] agent_store::StoreError),
}

impl From<String> for OutcomePreparationError {
    fn from(message: String) -> Self {
        Self::Invalid(message)
    }
}

pub(crate) async fn prepare_outcome(
    store: &Store,
    session_id: SessionId,
    outcome: SerializedOutcome,
) -> Result<OutcomeCommit, OutcomePreparationError> {
    let mut operation_ids = HashSet::new();
    let mut staged_kinds = HashMap::new();
    let mut operations = Vec::with_capacity(outcome.operations.len());
    for staged in outcome.operations {
        if !operation_ids.insert(staged.id) {
            return Err(format!("duplicate operation ID {}", staged.id).into());
        }
        match store.operation(staged.id).await {
            Ok(_) => {
                return Err(format!("operation ID {} already exists", staged.id).into());
            }
            Err(agent_store::StoreError::OperationNotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        validate_execution_request(&staged)?;
        validate_references(store, session_id, &staged, &staged_kinds).await?;
        let operation = new_operation(&staged)?;
        staged_kinds.insert(operation.id, operation.kind);
        operations.push(operation);
    }

    let mut wait_ids = HashSet::new();
    let mut waits = Vec::with_capacity(outcome.waits.len());
    for staged in outcome.waits {
        if !wait_ids.insert(staged.id) {
            return Err(format!("duplicate wait ID {}", staged.id).into());
        }
        match store.wait(staged.id).await {
            Ok(_) => return Err(format!("wait ID {} already exists", staged.id).into()),
            Err(agent_store::StoreError::WaitNotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
        staged.spec.validate().map_err(|error| error.to_string())?;
        if let Some(schema) = &staged.spec.response_schema {
            jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .build(schema)
                .map_err(|error| format!("invalid wait response schema: {error}"))?;
        }
        waits.push(NewWait {
            id: staged.id,
            mode: staged.spec.mode,
            payload: serde_json::value::to_raw_value(&staged.spec.payload)
                .map_err(|error| error.to_string())?,
            response_schema: staged
                .spec
                .response_schema
                .as_ref()
                .map(serde_json::value::to_raw_value)
                .transpose()
                .map_err(|error| error.to_string())?,
            expires_at: staged.spec.expires_at,
        });
    }

    let history = outcome
        .history
        .iter()
        .map(|message| {
            Ok(NewHistoryEntry {
                id: agent_contracts::HistoryEntryId::new(),
                message: serde_json::value::to_raw_value(message)
                    .map_err(|error| error.to_string())?,
            })
        })
        .collect::<Result<_, String>>()?;

    let mut cancelled_wait_ids = HashSet::new();
    for wait_id in &outcome.cancelled_waits {
        if !cancelled_wait_ids.insert(*wait_id) {
            return Err(format!("duplicate cancelled wait ID {wait_id}").into());
        }
    }

    Ok(OutcomeCommit {
        state: outcome.state,
        status: outcome.status,
        history,
        operations,
        waits,
        cancelled_waits: outcome.cancelled_waits,
        progress: outcome.progress,
    })
}

fn new_operation(staged: &StagedOperation) -> Result<NewOperation, String> {
    let (gateway_connection_id, target_operation_id, previous_operation_id) = match &staged.request
    {
        OperationRequest::Llm {
            connection,
            request,
        } => (
            Some(connection.clone()),
            None,
            match request {
                LlmRequest::Fresh { .. } => None,
                LlmRequest::Continuation {
                    previous_operation_id,
                    ..
                } => Some(*previous_operation_id),
            },
        ),
        OperationRequest::Execution { connection, .. } => (Some(connection.clone()), None, None),
        OperationRequest::LlmCancellation { target } | OperationRequest::Withdraw { target } => {
            (None, Some(*target), None)
        }
    };
    Ok(NewOperation {
        id: staged.id,
        kind: staged.request.kind(),
        gateway_connection_id,
        target_operation_id,
        previous_operation_id,
        request: serde_json::value::to_raw_value(&staged.request)
            .map_err(|error| error.to_string())?,
    })
}

async fn validate_references(
    store: &Store,
    session_id: SessionId,
    staged: &StagedOperation,
    staged_kinds: &HashMap<OperationId, OperationKind>,
) -> Result<(), OutcomePreparationError> {
    let (reference, required_kind) = match &staged.request {
        OperationRequest::Llm {
            request:
                LlmRequest::Continuation {
                    previous_operation_id,
                    ..
                },
            ..
        }
        | OperationRequest::LlmCancellation {
            target: previous_operation_id,
        } => (Some(*previous_operation_id), Some(OperationKind::Llm)),
        OperationRequest::Withdraw { target } => (Some(*target), None),
        _ => (None, None),
    };
    let Some(reference) = reference else {
        return Ok(());
    };
    if reference == staged.id {
        return Err(format!("operation {} cannot reference itself", staged.id).into());
    }
    let kind = if let Some(kind) = staged_kinds.get(&reference) {
        *kind
    } else {
        store
            .operation_for_session(session_id, reference)
            .await?
            .map(|operation| operation.kind)
            .ok_or_else(|| {
                OutcomePreparationError::Invalid(format!(
                    "referenced operation {reference} does not exist in the session"
                ))
            })?
    };
    if required_kind.is_some_and(|required| required != kind) {
        return Err(format!("referenced operation {reference} is not an LLM operation").into());
    }
    Ok(())
}

fn validate_execution_request(staged: &StagedOperation) -> Result<(), String> {
    let OperationRequest::Execution { request, .. } = &staged.request else {
        return Ok(());
    };
    let has_shutdown = match request {
        agent_contracts::execution::Payload::Single(operation) => {
            matches!(operation, agent_contracts::execution::Operation::Shutdown)
        }
        agent_contracts::execution::Payload::Batch(batch) => batch.operations.iter().any(|item| {
            matches!(
                item.operation,
                agent_contracts::execution::Operation::Shutdown
            )
        }),
    };
    if has_shutdown {
        return Err("runtime.shutdown is not supported by the execution gateway".into());
    }
    Ok(())
}
