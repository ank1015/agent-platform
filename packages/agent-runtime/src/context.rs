use agent_contracts::{
    ContextError, HarnessContext, HistoryEntry, HistoryEntryId, HistoryQuery, HistorySequence,
    OperationFailure, OperationId, OperationOutcome, OperationQuery,
    OperationRecord as ContractOperation, SessionView, WaitId, WaitQuery,
    WaitRecord as ContractWait,
};
use agent_store::{HistoryRecord, OperationRecord, OperationStatus, Store, WaitRecord, WaitStatus};
use async_trait::async_trait;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio_util::sync::CancellationToken;

pub(crate) struct StoreContext {
    store: Store,
    session: SessionView,
    history_through: HistorySequence,
    shutdown: CancellationToken,
    ownership_lost: Arc<AtomicBool>,
    infrastructure_failure: Mutex<Option<String>>,
}

impl StoreContext {
    pub(crate) fn new(
        store: Store,
        session: SessionView,
        history_through: HistorySequence,
        shutdown: CancellationToken,
        ownership_lost: Arc<AtomicBool>,
    ) -> Self {
        Self {
            store,
            session,
            history_through,
            shutdown,
            ownership_lost,
            infrastructure_failure: Mutex::new(None),
        }
    }

    pub(crate) fn take_infrastructure_failure(&self) -> Option<String> {
        self.infrastructure_failure.lock().unwrap().take()
    }

    fn store_error(&self, error: agent_store::StoreError) -> ContextError {
        match error {
            agent_store::StoreError::Database(_) | agent_store::StoreError::Migration(_) => {
                let message = error.to_string();
                *self.infrastructure_failure.lock().unwrap() = Some(message.clone());
                ContextError::Unavailable(message)
            }
            error => ContextError::Read(error.to_string()),
        }
    }
}

#[async_trait]
impl HarnessContext for StoreContext {
    fn session(&self) -> &SessionView {
        &self.session
    }

    fn history_through_sequence(&self) -> HistorySequence {
        self.history_through
    }

    fn stop_requested(&self) -> bool {
        self.shutdown.is_cancelled() || self.ownership_lost.load(Ordering::Acquire)
    }

    async fn history(&self, query: HistoryQuery) -> Result<Vec<HistoryEntry>, ContextError> {
        let limit = limit(query.limit)?;
        self.store
            .history(
                self.session.id,
                query.after_sequence,
                self.history_through,
                limit,
            )
            .await
            .map_err(|error| self.store_error(error))?
            .into_iter()
            .map(history_entry)
            .collect()
    }

    async fn history_entry(
        &self,
        id: HistoryEntryId,
    ) -> Result<Option<HistoryEntry>, ContextError> {
        self.store
            .history_entry(self.session.id, id, self.history_through)
            .await
            .map_err(|error| self.store_error(error))?
            .map(history_entry)
            .transpose()
    }

    async fn operation(&self, id: OperationId) -> Result<Option<ContractOperation>, ContextError> {
        self.store
            .operation_for_session(self.session.id, id)
            .await
            .map_err(|error| self.store_error(error))?
            .map(operation)
            .transpose()
    }

    async fn operations(
        &self,
        query: OperationQuery,
    ) -> Result<Vec<ContractOperation>, ContextError> {
        self.store
            .operations(self.session.id, query.kind, limit(query.limit)?)
            .await
            .map_err(|error| self.store_error(error))?
            .into_iter()
            .map(operation)
            .collect()
    }

    async fn wait(&self, id: WaitId) -> Result<Option<ContractWait>, ContextError> {
        self.store
            .wait_for_session(self.session.id, id)
            .await
            .map_err(|error| self.store_error(error))?
            .map(wait)
            .transpose()
    }

    async fn waits(&self, query: WaitQuery) -> Result<Vec<ContractWait>, ContextError> {
        self.store
            .waits(
                self.session.id,
                query.status.map(store_wait_status),
                limit(query.limit)?,
            )
            .await
            .map_err(|error| self.store_error(error))?
            .into_iter()
            .map(wait)
            .collect()
    }
}

fn limit(limit: usize) -> Result<i64, ContextError> {
    if !(1..=1000).contains(&limit) {
        return Err(ContextError::Read(
            "query limit must be between 1 and 1000".into(),
        ));
    }
    Ok(limit as i64)
}

fn history_entry(record: HistoryRecord) -> Result<HistoryEntry, ContextError> {
    Ok(HistoryEntry {
        id: record.id,
        sequence: record.sequence,
        source_event_id: record.source_event_id,
        created_at: record.created_at,
        message: serde_json::from_str(record.message.get())
            .map_err(|error| ContextError::Read(error.to_string()))?,
    })
}

fn operation(record: OperationRecord) -> Result<ContractOperation, ContextError> {
    let outcome = match record.status {
        OperationStatus::Pending | OperationStatus::Submitting | OperationStatus::Accepted => None,
        OperationStatus::Succeeded => Some(OperationOutcome::Succeeded(parse_result(&record)?)),
        OperationStatus::Failed => Some(OperationOutcome::Failed(parse_failure(&record)?)),
        OperationStatus::Cancelled => Some(OperationOutcome::Cancelled),
        OperationStatus::Unknown => Some(OperationOutcome::Unknown(parse_failure(&record)?)),
    };
    Ok(ContractOperation {
        id: record.id,
        kind: record.kind,
        gateway_job_id: record.gateway_job_id,
        outcome,
    })
}

fn parse_result(
    record: &OperationRecord,
) -> Result<agent_contracts::OperationResult, ContextError> {
    let result = record
        .result
        .as_deref()
        .ok_or_else(|| ContextError::Read("succeeded operation has no result".into()))?;
    serde_json::from_str(result.get()).map_err(|error| ContextError::Read(error.to_string()))
}

fn parse_failure(record: &OperationRecord) -> Result<OperationFailure, ContextError> {
    let error = record
        .error
        .as_ref()
        .ok_or_else(|| ContextError::Read("failed operation has no error".into()))?;
    let mut failure: OperationFailure = serde_json::from_value(error.clone())
        .map_err(|error| ContextError::Read(error.to_string()))?;
    if record.kind == agent_contracts::OperationKind::Execution
        && let Some(result) = record.result.as_deref()
    {
        let result: agent_contracts::OperationResult = serde_json::from_str(result.get())
            .map_err(|error| ContextError::Read(error.to_string()))?;
        let agent_contracts::OperationResult::Execution(response) = result else {
            return Err(ContextError::Read(
                "failed execution has a non-execution result".into(),
            ));
        };
        failure.execution_response = Some(response);
    }
    Ok(failure)
}

fn wait(record: WaitRecord) -> Result<ContractWait, ContextError> {
    Ok(ContractWait {
        id: record.id,
        spec: agent_contracts::WaitSpec {
            mode: record.mode,
            payload: serde_json::from_str(record.payload.get())
                .map_err(|error| ContextError::Read(error.to_string()))?,
            response_schema: record
                .response_schema
                .as_deref()
                .map(|schema| serde_json::from_str(schema.get()))
                .transpose()
                .map_err(|error| ContextError::Read(error.to_string()))?,
            expires_at: record.expires_at,
        },
        status: contract_wait_status(record.status),
        resolution: record
            .resolution_payload
            .as_deref()
            .map(|resolution| serde_json::from_str(resolution.get()))
            .transpose()
            .map_err(|error| ContextError::Read(error.to_string()))?,
        finished_at: record.finished_at,
    })
}

fn store_wait_status(status: agent_contracts::WaitStatus) -> WaitStatus {
    match status {
        agent_contracts::WaitStatus::Pending => WaitStatus::Pending,
        agent_contracts::WaitStatus::Resolved => WaitStatus::Resolved,
        agent_contracts::WaitStatus::Expired => WaitStatus::Expired,
        agent_contracts::WaitStatus::Cancelled => WaitStatus::Cancelled,
    }
}

fn contract_wait_status(status: WaitStatus) -> agent_contracts::WaitStatus {
    match status {
        WaitStatus::Pending => agent_contracts::WaitStatus::Pending,
        WaitStatus::Resolved => agent_contracts::WaitStatus::Resolved,
        WaitStatus::Expired => agent_contracts::WaitStatus::Expired,
        WaitStatus::Cancelled => agent_contracts::WaitStatus::Cancelled,
    }
}
