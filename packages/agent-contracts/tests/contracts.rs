use agent_contracts::{
    AssistantResponse, EventId, EventKind, EventSequence, FailureSource, GatewayConnectionId,
    HistoryEntry, HistoryEntryId, HistorySequence, LlmRequest, Message, OperationFailure,
    OperationId, OperationKind, OperationOutcome, OperationOutcomeStatus, OperationRecord,
    OperationRequest, OperationResult, OutcomeBuilder, SessionEvent, SessionId, SessionStatus,
    StopReason, Usage, UsageCost, WaitId, WaitMode, WaitResumeReason, WaitSpec, execution,
    execution_core,
};
use chrono::Utc;
use serde_json::{Value, json};

#[test]
fn conversation_messages_match_llm_wire_shapes() {
    let examples = [
        json!({"role":"user","content":[{"type":"text","text":"hello"},{"type":"image","url":"https://example.test/image.png","detail":"high"}]}),
        json!({"role":"system","content":[{"type":"text","text":"instructions"}]}),
        json!({"role":"assistant","provider":"openai","content":[{"type":"opaque","nested":{"value":"α"}}]}),
        json!({"role":"tool_result","toolName":"shell","toolCallId":"call-1","content":[{"type":"text","text":"done"}],"outcome":{"status":"error","error":{"message":"failed"}},"details":{"exitCode":1}}),
        json!({"role":"custom","tag":"harness.note","data":{"count":2}}),
    ];
    for example in examples {
        let message: Message = serde_json::from_value(example.clone()).unwrap();
        assert_eq!(serde_json::to_value(message).unwrap(), example);
    }
}

#[test]
fn outcome_stages_ids_and_waits_without_dispatch() {
    let mut builder = OutcomeBuilder::new(0_u32);
    builder.append_message(Message::user_text("go"));
    let operation_id = builder.request_llm(
        GatewayConnectionId("primary".into()),
        LlmRequest::Fresh {
            account_id: uuid::Uuid::new_v4(),
            model_id: "model".into(),
            instructions: None,
            messages: vec![],
            tools: vec![],
            provider_options: Default::default(),
        },
    );
    let wait_id = builder
        .create_wait(WaitSpec {
            mode: WaitMode::External,
            payload: json!({"prompt":"reply"}),
            response_schema: None,
            expires_at: None,
        })
        .unwrap();
    *builder.state_mut() = 1;
    builder.set_status(SessionStatus::Waiting);
    let outcome = builder.finish();
    assert_eq!(outcome.state, 1);
    assert_eq!(outcome.operations[0].id, operation_id);
    assert_eq!(outcome.waits[0].id, wait_id);
    assert_eq!(outcome.history.len(), 1);
    assert!(matches!(outcome.status, Some(SessionStatus::Waiting)));
    assert!(
        WaitSpec {
            mode: WaitMode::Expiration,
            payload: Value::Null,
            response_schema: None,
            expires_at: None
        }
        .validate()
        .is_err()
    );
    assert!(
        WaitSpec {
            mode: WaitMode::Either,
            payload: Value::Null,
            response_schema: None,
            expires_at: Some(Utc::now())
        }
        .validate()
        .is_ok()
    );
    assert!(
        WaitSpec {
            mode: WaitMode::External,
            payload: Value::Null,
            response_schema: None,
            expires_at: Some(Utc::now())
        }
        .validate()
        .is_err()
    );
}

#[test]
fn builder_preserves_order_and_does_not_change_status_implicitly() {
    let mut builder = OutcomeBuilder::new(());
    builder.append_message(Message::user_text("first"));
    let first = builder.request_llm_cancellation(OperationId::new());
    builder.append_message(Message::user_text("second"));
    let second = builder.withdraw_operation(OperationId::new());
    let outcome = builder.finish();
    assert_eq!(
        outcome
            .operations
            .iter()
            .map(|operation| operation.id)
            .collect::<Vec<_>>(),
        vec![first, second]
    );
    assert_eq!(outcome.history.len(), 2);
    assert!(outcome.status.is_none());
}

#[test]
fn execution_result_is_distinct_from_gateway_failure() {
    let response: agent_contracts::execution::Response = serde_json::from_value(json!({
        "protocol_version":1,
        "request_id":"start",
        "generation_id":uuid::Uuid::new_v4(),
        "status":"ok",
        "result":{"handle":{"id":"process"},"exit_code":7}
    }))
    .unwrap();
    let result = OperationOutcome::Succeeded(OperationResult::Execution(response));
    assert_eq!(serde_json::to_value(result).unwrap()["status"], "succeeded");
}

#[test]
fn execution_observation_is_an_explicit_staged_operation() {
    let handle = execution_core::ExecutionHandle {
        id: uuid::Uuid::new_v4(),
        generation_id: uuid::Uuid::new_v4(),
    };
    let mut builder = OutcomeBuilder::new(());
    let operation_id = builder
        .observe_execution(
            GatewayConnectionId("execution-primary".into()),
            uuid::Uuid::new_v4(),
            execution::ObserveParams {
                handle,
                after_cursor: None,
                wait_ms: 0,
                return_when: execution::WaitMode::Activity,
                max_output_bytes: None,
            },
        )
        .unwrap();
    let outcome = builder.finish();
    assert_eq!(outcome.operations[0].id, operation_id);
    assert!(matches!(
        outcome.operations[0].request,
        OperationRequest::Execution { .. }
    ));
}

#[test]
fn raw_json_can_preserve_opaque_provider_escape_sequences() {
    let message = serde_json::from_str::<Message>(
        r#"{"role":"assistant","provider":"openai","content":[{"text":"\ud800"}]}"#,
    )
    .unwrap();
    let Message::Assistant { content, .. } = message else {
        panic!("expected assistant message")
    };
    assert_eq!(content[0].get(), r#"{"text":"\ud800"}"#);
}

fn roundtrip<T: serde::Serialize + serde::de::DeserializeOwned>(value: &T) -> String {
    let json = serde_json::to_string(value).unwrap();
    let _: T = serde_json::from_str(&json).unwrap();
    json
}

#[test]
fn message_survives_all_enclosing_contracts() {
    let assistant: Message = serde_json::from_str(
        r#"{"role":"assistant","provider":"openai","content":[{"text":"\ud800"}]}"#,
    )
    .unwrap();
    let user = Message::user_text("hello");
    let account_id = uuid::Uuid::new_v4();
    let fresh = LlmRequest::Fresh {
        account_id,
        model_id: "model".into(),
        instructions: None,
        messages: vec![user.clone(), assistant.clone()],
        tools: vec![],
        provider_options: Default::default(),
    };
    let continuation = LlmRequest::Continuation {
        previous_operation_id: OperationId::new(),
        messages: vec![assistant.clone()],
    };
    assert!(roundtrip(&fresh).contains("\\ud800"));
    roundtrip(&continuation);
    roundtrip(&OperationRequest::Llm {
        connection: GatewayConnectionId("llm".into()),
        request: fresh,
    });
    roundtrip(&OperationRequest::Llm {
        connection: GatewayConnectionId("llm".into()),
        request: continuation,
    });
    roundtrip(&OperationRequest::LlmCancellation {
        target: OperationId::new(),
    });
    roundtrip(&OperationRequest::Withdraw {
        target: OperationId::new(),
    });
    roundtrip(&OperationRequest::Execution {
        connection: GatewayConnectionId("execution".into()),
        machine_id: uuid::Uuid::new_v4(),
        expected_generation_id: None,
        request: execution::Payload::Single(execution::Operation::Info),
    });

    let session_id = SessionId::new();
    for kind in [
        EventKind::UserMessage { message: user },
        EventKind::UserMessage {
            message: assistant.clone(),
        },
        EventKind::OperationCompleted {
            operation_id: OperationId::new(),
            operation_kind: OperationKind::Llm,
            outcome_status: OperationOutcomeStatus::Succeeded,
        },
        EventKind::CancellationRequested {
            reason: Some("stop".into()),
        },
        EventKind::WaitResumed {
            wait_id: WaitId::new(),
            reason: WaitResumeReason::Expired,
        },
        EventKind::WaitResumed {
            wait_id: WaitId::new(),
            reason: WaitResumeReason::Cancelled,
        },
    ] {
        roundtrip(&SessionEvent {
            id: EventId::new(),
            session_id,
            sequence: EventSequence(1),
            created_at: Utc::now(),
            kind,
        });
    }
    roundtrip(&HistoryEntry {
        id: HistoryEntryId::new(),
        sequence: HistorySequence(1),
        source_event_id: None,
        created_at: Utc::now(),
        message: assistant.clone(),
    });

    let response = AssistantResponse {
        id: "response".into(),
        model_id: "model".into(),
        resolved_model_id: None,
        message: assistant,
        stop_reason: StopReason::ToolUse,
        usage: Some(Usage {
            input: Some(10),
            output: Some(2),
            cache_read: Some(1),
            cache_write: None,
            cost: Some(UsageCost {
                input: Some(0.1),
                output: Some(0.2),
                cache_read: None,
                cache_write: None,
                total: 0.3,
            }),
        }),
        duration_ms: 10,
        timestamp: 100,
    };
    let result_json = roundtrip(&OperationRecord {
        id: OperationId::new(),
        kind: OperationKind::Llm,
        gateway_job_id: Some(uuid::Uuid::new_v4()),
        outcome: Some(OperationOutcome::Succeeded(OperationResult::Llm(Box::new(
            response,
        )))),
    });
    assert!(result_json.contains("\\ud800"));
    roundtrip(&OperationOutcome::Failed(OperationFailure {
        source: FailureSource::Admission,
        code: "rejected".into(),
        message: "no job".into(),
        execution_response: None,
    }));
    roundtrip(&OperationOutcome::Unknown(OperationFailure {
        source: FailureSource::Gateway,
        code: "unknown".into(),
        message: "uncertain".into(),
        execution_response: None,
    }));
    roundtrip(&OperationOutcome::Cancelled);
    roundtrip(&OperationOutcome::Succeeded(
        OperationResult::LlmCancellation { accepted: true },
    ));
    roundtrip(&OperationOutcome::Succeeded(OperationResult::Withdraw {
        withdrawn: false,
    }));
}

#[test]
fn assistant_response_requires_an_assistant_message() {
    let response = serde_json::json!({
        "id": "response",
        "modelId": "model",
        "message": {"role": "user", "content": []},
        "stopReason": "stop",
        "durationMs": 1,
        "timestamp": 1
    });
    assert!(serde_json::from_value::<AssistantResponse>(response).is_err());
}

#[test]
fn nested_message_field_order_and_nulls() {
    let llm = format!(
        r#"{{"messages":[{{"content":[{{"text":"hello","type":"text"}}],"role":"user"}}],"previous_operation_id":"{}","mode":"continuation"}}"#,
        OperationId::new()
    );
    serde_json::from_str::<LlmRequest>(&llm).unwrap();
    let operation = format!(r#"{{"request":{llm},"connection":"llm","kind":"llm"}}"#);
    serde_json::from_str::<OperationRequest>(&operation).unwrap();
    let event = r#"{"payload":{"message":{"content":[{"type":"text","text":"hello"}],"role":"user"}},"created_at":"2026-09-14T00:00:00Z","sequence":1,"session_id":"00000000-0000-0000-0000-000000000002","type":"user_message","id":"00000000-0000-0000-0000-000000000001"}"#;
    serde_json::from_str::<SessionEvent>(event).unwrap();
    let message: Message =
        serde_json::from_str(r#"{"data":null,"tag":"test","role":"custom"}"#).unwrap();
    assert_eq!(serde_json::to_value(&message).unwrap()["data"], Value::Null);
    let message: Message = serde_json::from_str(r#"{"details":null,"outcome":{"status":"success"},"content":[],"toolCallId":"call","toolName":"tool","role":"tool_result"}"#).unwrap();
    assert_eq!(
        serde_json::to_value(&message).unwrap()["details"],
        Value::Null
    );
}
