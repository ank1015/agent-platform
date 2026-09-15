//! In-memory session-scoped context for testing independent harness crates.

use agent_contracts::{
    ContextError, HarnessContext, HistoryEntry, HistoryEntryId, HistoryQuery, HistorySequence,
    OperationId, OperationQuery, OperationRecord, SessionView, WaitId, WaitQuery, WaitRecord,
};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct MemoryContext {
    session: SessionView,
    history_through: HistorySequence,
    history: Vec<String>,
    operations: Vec<(OperationId, String)>,
    waits: Vec<(WaitId, String)>,
    stop_requested: AtomicBool,
}

impl MemoryContext {
    pub fn new(session: SessionView, history_through: HistorySequence) -> Self {
        Self {
            session,
            history_through,
            history: Vec::new(),
            operations: Vec::new(),
            waits: Vec::new(),
            stop_requested: AtomicBool::new(false),
        }
    }

    pub fn add_history(&mut self, entry: &HistoryEntry) -> Result<(), serde_json::Error> {
        self.history.push(serde_json::to_string(entry)?);
        Ok(())
    }

    pub fn add_operation(&mut self, record: &OperationRecord) -> Result<(), serde_json::Error> {
        let json = serde_json::to_string(record)?;
        if let Some((_, existing)) = self.operations.iter_mut().find(|(id, _)| *id == record.id) {
            *existing = json;
        } else {
            self.operations.push((record.id, json));
        }
        Ok(())
    }

    pub fn add_wait(&mut self, record: &WaitRecord) -> Result<(), serde_json::Error> {
        let json = serde_json::to_string(record)?;
        if let Some((_, existing)) = self.waits.iter_mut().find(|(id, _)| *id == record.id) {
            *existing = json;
        } else {
            self.waits.push((record.id, json));
        }
        Ok(())
    }

    pub fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::Relaxed);
    }
}

fn decode<T: DeserializeOwned>(json: &str) -> Result<T, ContextError> {
    serde_json::from_str(json).map_err(|error| ContextError::Read(error.to_string()))
}

#[async_trait]
impl HarnessContext for MemoryContext {
    fn session(&self) -> &SessionView {
        &self.session
    }

    fn history_through_sequence(&self) -> HistorySequence {
        self.history_through
    }

    fn stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::Relaxed)
    }

    async fn history(&self, query: HistoryQuery) -> Result<Vec<HistoryEntry>, ContextError> {
        let mut entries: Vec<HistoryEntry> = self
            .history
            .iter()
            .map(|json| decode(json))
            .collect::<Result<_, _>>()?;
        entries.retain(|entry| {
            entry.sequence > query.after_sequence && entry.sequence <= self.history_through
        });
        entries.sort_by_key(|entry| entry.sequence);
        entries.truncate(query.limit);
        Ok(entries)
    }

    async fn history_entry(
        &self,
        id: HistoryEntryId,
    ) -> Result<Option<HistoryEntry>, ContextError> {
        for json in &self.history {
            let entry: HistoryEntry = decode(json)?;
            if entry.id == id && entry.sequence <= self.history_through {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    async fn operation(&self, id: OperationId) -> Result<Option<OperationRecord>, ContextError> {
        for (_, json) in &self.operations {
            let record: OperationRecord = decode(json)?;
            if record.id == id {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    async fn operations(
        &self,
        query: OperationQuery,
    ) -> Result<Vec<OperationRecord>, ContextError> {
        let mut records: Vec<OperationRecord> = self
            .operations
            .iter()
            .map(|(_, json)| decode(json))
            .collect::<Result<_, _>>()?;
        records.retain(|record| query.kind.is_none_or(|kind| kind == record.kind));
        records.truncate(query.limit);
        Ok(records)
    }

    async fn wait(&self, id: WaitId) -> Result<Option<WaitRecord>, ContextError> {
        for (_, json) in &self.waits {
            let record: WaitRecord = decode(json)?;
            if record.id == id {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    async fn waits(&self, query: WaitQuery) -> Result<Vec<WaitRecord>, ContextError> {
        let mut records: Vec<WaitRecord> = self
            .waits
            .iter()
            .map(|(_, json)| decode(json))
            .collect::<Result<_, _>>()?;
        records.retain(|record| query.status.is_none_or(|status| status == record.status));
        records.truncate(query.limit);
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_contracts::{
        EventId, FailureSource, HarnessId, HarnessVersion, HistorySequence, Message,
        OperationFailure, OperationKind, OperationOutcome, OperationResult, OutcomeBuilder,
        ProjectId, SessionId, SessionStatus, StateVersion, WaitMode, WaitSpec, WaitStatus,
    };
    use chrono::Utc;
    use serde_json::json;

    fn context(through: i64) -> MemoryContext {
        MemoryContext::new(
            SessionView {
                id: SessionId::new(),
                project_id: ProjectId("project".into()),
                harness_id: HarnessId("fixture".into()),
                harness_version: HarnessVersion("1".into()),
                status: SessionStatus::Idle,
                state_version: StateVersion(0),
                metadata: json!({}),
            },
            HistorySequence(through),
        )
    }

    #[tokio::test]
    async fn history_boundary_pagination_and_staged_visibility() {
        let mut context = context(3);
        let mut ids = vec![];
        for sequence in 1..=4 {
            let entry = HistoryEntry {
                id: HistoryEntryId::new(),
                sequence: HistorySequence(sequence),
                source_event_id: Some(EventId::new()),
                created_at: Utc::now(),
                message: Message::user_text(format!("message {sequence}")),
            };
            ids.push(entry.id);
            context.add_history(&entry).unwrap();
        }
        let page = context
            .history(HistoryQuery {
                after_sequence: HistorySequence(1),
                limit: 1,
            })
            .await
            .unwrap();
        assert_eq!(page[0].sequence, HistorySequence(2));
        let page = context
            .history(HistoryQuery {
                after_sequence: HistorySequence(2),
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].sequence, HistorySequence(3));
        assert!(context.history_entry(ids[3]).await.unwrap().is_none());

        let mut builder = OutcomeBuilder::new(());
        builder.append_message(Message::user_text("staged"));
        assert_eq!(builder.finish().history.len(), 1);
        assert_eq!(
            context
                .history(HistoryQuery {
                    after_sequence: HistorySequence(3),
                    limit: 10
                })
                .await
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn operation_wait_and_stop_reads_reflect_recorded_state() {
        let mut context = context(0);
        let id = OperationId::new();
        let mut record = OperationRecord {
            id,
            kind: OperationKind::Llm,
            gateway_job_id: None,
            outcome: None,
        };
        context.add_operation(&record).unwrap();
        assert!(
            context
                .operation(id)
                .await
                .unwrap()
                .unwrap()
                .outcome
                .is_none()
        );
        record.outcome = Some(OperationOutcome::Failed(OperationFailure {
            source: FailureSource::Admission,
            code: "rejected".into(),
            message: "no job".into(),
            execution_response: None,
        }));
        context.add_operation(&record).unwrap();
        let result = context.operation(id).await.unwrap().unwrap();
        assert!(result.gateway_job_id.is_none());
        assert!(matches!(result.outcome, Some(OperationOutcome::Failed(_))));
        assert_eq!(
            context
                .operations(OperationQuery {
                    kind: Some(OperationKind::Llm),
                    limit: 10
                })
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            context
                .operation(OperationId::new())
                .await
                .unwrap()
                .is_none()
        );

        let unknown = OperationRecord {
            id: OperationId::new(),
            kind: OperationKind::Execution,
            gateway_job_id: None,
            outcome: Some(OperationOutcome::Unknown(OperationFailure {
                source: FailureSource::Gateway,
                code: "unknown".into(),
                message: "uncertain".into(),
                execution_response: None,
            })),
        };
        context.add_operation(&unknown).unwrap();
        let cancelled = OperationRecord {
            id: OperationId::new(),
            kind: OperationKind::Llm,
            gateway_job_id: None,
            outcome: Some(OperationOutcome::Cancelled),
        };
        context.add_operation(&cancelled).unwrap();
        let withdrawn = OperationRecord {
            id: OperationId::new(),
            kind: OperationKind::Withdraw,
            gateway_job_id: None,
            outcome: Some(OperationOutcome::Succeeded(OperationResult::Withdraw {
                withdrawn: true,
            })),
        };
        context.add_operation(&withdrawn).unwrap();
        assert!(matches!(
            context
                .operation(unknown.id)
                .await
                .unwrap()
                .unwrap()
                .outcome,
            Some(OperationOutcome::Unknown(_))
        ));
        assert!(matches!(
            context
                .operation(cancelled.id)
                .await
                .unwrap()
                .unwrap()
                .outcome,
            Some(OperationOutcome::Cancelled)
        ));
        assert!(matches!(
            context
                .operation(withdrawn.id)
                .await
                .unwrap()
                .unwrap()
                .outcome,
            Some(OperationOutcome::Succeeded(OperationResult::Withdraw {
                withdrawn: true
            }))
        ));

        let mut wait = WaitRecord {
            id: WaitId::new(),
            spec: WaitSpec {
                mode: WaitMode::External,
                payload: json!({"prompt":"reply"}),
                response_schema: None,
                expires_at: None,
            },
            status: WaitStatus::Pending,
            resolution: None,
            finished_at: None,
        };
        context.add_wait(&wait).unwrap();
        assert!(matches!(
            context.wait(wait.id).await.unwrap().unwrap().status,
            WaitStatus::Pending
        ));
        assert_eq!(
            context
                .waits(WaitQuery {
                    status: Some(WaitStatus::Pending),
                    limit: 1
                })
                .await
                .unwrap()
                .len(),
            1
        );
        wait.status = WaitStatus::Resolved;
        wait.resolution = Some(json!({"answer":"yes"}));
        context.add_wait(&wait).unwrap();
        assert_eq!(
            context
                .waits(WaitQuery {
                    status: Some(WaitStatus::Pending),
                    limit: 10
                })
                .await
                .unwrap()
                .len(),
            0
        );
        assert!(matches!(
            context.wait(wait.id).await.unwrap().unwrap().status,
            WaitStatus::Resolved
        ));
        assert!(context.wait(WaitId::new()).await.unwrap().is_none());
        assert!(!context.stop_requested());
        context.request_stop();
        assert!(context.stop_requested());
    }
}
