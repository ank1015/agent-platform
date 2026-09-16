use crate::{
    BasicCodexConfig, BasicCodexProvider, llm, prompt, state::CompactionTrigger, tool_catalog,
};
use agent_contracts::{
    AssistantResponse, ContentPart, HistorySequence, ImageDetail, Message, MessageBase, Provider,
    StopReason, Usage,
};
use serde::Serialize;
use serde_json::{Value, json, value::RawValue, value::to_raw_value};
use sha2::{Digest, Sha256};

pub(crate) const RETAINED_USER_TOKEN_BUDGET: u64 = 64_000;
pub(crate) const CHECKPOINT_TAG: &str = "basic_codex_compaction_checkpoint";

pub(crate) struct ValidatedCompaction {
    pub(crate) provider: Provider,
    pub(crate) response_id: String,
    pub(crate) native_item: Box<RawValue>,
    pub(crate) usage: Option<Usage>,
}

pub(crate) fn validate_response(
    config: &BasicCodexConfig,
    response: AssistantResponse,
) -> Result<ValidatedCompaction, String> {
    let provider = llm::response_provider(config, &response)?;
    if response.stop_reason != StopReason::Stop {
        return Err(format!(
            "compaction response must stop normally, received {:?}",
            response.stop_reason
        ));
    }
    let Message::Assistant { content, .. } = response.message else {
        return Err("compaction response does not contain an assistant message".into());
    };
    let mut native_item = None;
    for item in content {
        let value: Value = serde_json::from_str(item.get())
            .map_err(|failure| format!("compaction output contains invalid JSON: {failure}"))?;
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            continue;
        };
        if kind != "compaction" && kind != "compaction_summary" {
            continue;
        }
        if native_item.is_some() {
            return Err("compaction response contains more than one compaction item".into());
        }
        let encrypted = value
            .get("encrypted_content")
            .and_then(Value::as_str)
            .ok_or_else(|| "compaction item requires encrypted_content".to_owned())?;
        if encrypted.trim().is_empty() {
            return Err("compaction item encrypted_content must not be empty".into());
        }
        native_item = Some(item);
    }
    let native_item = native_item
        .ok_or_else(|| "compaction response does not contain a compaction item".to_owned())?;
    Ok(ValidatedCompaction {
        provider,
        response_id: response.id,
        native_item,
        usage: response.usage,
    })
}

pub(crate) fn provider_message(
    provider: BasicCodexProvider,
    native_item: &RawValue,
) -> Result<Message, String> {
    let data = to_raw_value(&json!({
        "content": [serde_json::from_str::<Value>(native_item.get())
            .map_err(|failure| format!("saved compaction item is invalid JSON: {failure}"))?]
    }))
    .map_err(|failure| format!("could not serialize provider compaction message: {failure}"))?;
    Ok(Message::Custom {
        base: MessageBase::default(),
        tag: match provider {
            BasicCodexProvider::Openai => "openai_custom_item",
            BasicCodexProvider::Chatgpt => "chatgpt_custom_item",
        }
        .into(),
        data,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn checkpoint_message(
    provider: Provider,
    response_id: &str,
    native_item: &RawValue,
    source_through_sequence: HistorySequence,
    source_projection_sha256: &str,
    retained_user_messages: usize,
    generation: u32,
    trigger: CompactionTrigger,
) -> Result<Message, String> {
    let native_item: Value = serde_json::from_str(native_item.get())
        .map_err(|failure| format!("saved compaction item is invalid JSON: {failure}"))?;
    let data = to_raw_value(&json!({
        "version": 1,
        "provider": provider,
        "response_id": response_id,
        "native_item": native_item,
        "source_through_sequence": source_through_sequence,
        "source_projection_sha256": source_projection_sha256,
        "retained_user_messages": retained_user_messages,
        "generation": generation,
        "trigger": trigger,
    }))
    .map_err(|failure| format!("could not serialize compaction checkpoint: {failure}"))?;
    Ok(Message::Custom {
        base: MessageBase::default(),
        tag: CHECKPOINT_TAG.into(),
        data,
    })
}

pub(crate) fn projection_sha256(messages: &[Message]) -> Result<String, String> {
    let bytes = serde_json::to_vec(messages)
        .map_err(|failure| format!("could not fingerprint the model projection: {failure}"))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub(crate) fn retained_user_messages(messages: &[Message]) -> Vec<Message> {
    let mut remaining = RETAINED_USER_TOKEN_BUDGET;
    let mut retained = Vec::new();
    for message in messages.iter().rev() {
        let Message::User { .. } = message else {
            continue;
        };
        let tokens = estimate_message_tokens(message);
        if tokens <= remaining {
            retained.push(message.clone());
            remaining -= tokens;
            continue;
        }
        if remaining > 0
            && let Some(truncated) = truncate_user_message(message, remaining)
        {
            retained.push(truncated);
        }
        break;
    }
    retained.reverse();
    retained
}

pub(crate) fn estimate_request_tokens(config: &BasicCodexConfig, messages: &[Message]) -> u64 {
    let instructions = llm::instructions(config);
    estimate_bytes(instructions.len())
        .saturating_add(estimate_serialized_tokens(&tool_catalog::definitions()))
        .saturating_add(estimate_message_tokens(&prompt::environment_message(
            config,
            "0000-00-00",
        )))
        .saturating_add(messages.iter().map(estimate_message_tokens).sum::<u64>())
}

pub(crate) fn estimate_message_tokens(message: &Message) -> u64 {
    match message {
        Message::User { content, .. } => {
            4_u64.saturating_add(content.iter().map(estimate_content_tokens).sum::<u64>())
        }
        Message::Custom { tag, data, .. }
            if tag == "openai_custom_item" || tag == "chatgpt_custom_item" =>
        {
            estimate_native_compaction_tokens(data.as_ref())
                .unwrap_or_else(|| estimate_bytes(data.get().len()))
        }
        _ => estimate_serialized_tokens(message),
    }
}

fn estimate_content_tokens(content: &ContentPart) -> u64 {
    match content {
        ContentPart::Text { text, .. } => estimate_bytes(text.len()),
        ContentPart::Image { detail, .. } => match detail.unwrap_or(ImageDetail::Auto) {
            ImageDetail::Low => 85,
            ImageDetail::Auto | ImageDetail::High => 1_844,
            ImageDetail::Original => 10_000,
        },
    }
}

fn estimate_native_compaction_tokens(data: &RawValue) -> Option<u64> {
    let value: Value = serde_json::from_str(data.get()).ok()?;
    let encrypted = value
        .get("content")?
        .as_array()?
        .iter()
        .find_map(|item| item.get("encrypted_content").and_then(Value::as_str))?;
    let visible_bytes = encrypted.len().saturating_mul(3) / 4;
    Some(estimate_bytes(visible_bytes.saturating_sub(650)))
}

fn truncate_user_message(message: &Message, budget: u64) -> Option<Message> {
    let Message::User { base, content } = message else {
        return None;
    };
    let mut remaining = budget.saturating_sub(4);
    let mut selected = Vec::new();
    for part in content.iter().rev() {
        let tokens = estimate_content_tokens(part);
        if tokens <= remaining {
            selected.push(part.clone());
            remaining -= tokens;
            continue;
        }
        if let ContentPart::Text { text, metadata } = part
            && remaining > 0
        {
            let byte_budget = remaining.saturating_mul(4).min(usize::MAX as u64) as usize;
            let start = text.len().saturating_sub(byte_budget);
            let start = next_char_boundary(text, start);
            if start < text.len() {
                selected.push(ContentPart::Text {
                    text: text[start..].to_owned(),
                    metadata: metadata.clone(),
                });
            }
        }
        break;
    }
    selected.reverse();
    (!selected.is_empty()).then(|| Message::User {
        base: base.clone(),
        content: selected,
    })
}

fn next_char_boundary(text: &str, mut index: usize) -> usize {
    while index < text.len() && !text.is_char_boundary(index) {
        index += 1;
    }
    index
}

fn estimate_serialized_tokens(value: &impl Serialize) -> u64 {
    serde_json::to_vec(value)
        .map(|bytes| estimate_bytes(bytes.len()))
        .unwrap_or(u64::MAX)
}

const fn estimate_bytes(bytes: usize) -> u64 {
    (bytes as u64).saturating_add(3) / 4
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_contracts::{MessageBase, Provider};
    use serde_json::value::to_raw_value;

    #[test]
    fn retains_recent_user_messages_in_original_order_with_a_hard_budget() {
        let oldest = Message::user_text("old");
        let middle = Message::user_text("m".repeat(300_000));
        let newest = Message::user_text("new");
        let retained = retained_user_messages(&[
            oldest,
            Message::Assistant {
                base: MessageBase::default(),
                provider: Provider::Openai,
                content: vec![],
            },
            middle,
            newest,
        ]);
        assert_eq!(retained.len(), 2);
        let encoded = serde_json::to_value(&retained).unwrap();
        assert!(encoded[0]["content"][0]["text"].as_str().unwrap().len() < 300_000);
        assert_eq!(encoded[1]["content"][0]["text"], "new");
        assert!(
            retained.iter().map(estimate_message_tokens).sum::<u64>() <= RETAINED_USER_TOKEN_BUDGET
        );
    }

    #[test]
    fn validates_exactly_one_nonempty_native_compaction_item() {
        let response = AssistantResponse {
            id: "response-1".into(),
            model_id: "gpt-5.6-luna".into(),
            resolved_model_id: None,
            message: Message::Assistant {
                base: MessageBase::default(),
                provider: Provider::Openai,
                content: vec![
                    to_raw_value(&json!({
                        "type": "compaction",
                        "encrypted_content": "opaque"
                    }))
                    .unwrap(),
                ],
            },
            stop_reason: StopReason::Stop,
            usage: None,
            duration_ms: 1,
            timestamp: 1,
        };
        let config = BasicCodexConfig {
            account_id: uuid::Uuid::new_v4(),
            provider: BasicCodexProvider::Openai,
            model: crate::BasicCodexModel::Luna,
            reasoning_effort: crate::ReasoningEffort::Medium,
            machine_id: uuid::Uuid::new_v4(),
            cwd: crate::WorkingDirectory::new("/workspace").unwrap(),
            shell: None,
            platform: None,
            additional_instructions: None,
        };
        let validated = validate_response(&config, response).unwrap();
        assert_eq!(validated.provider, Provider::Openai);
        assert!(validated.native_item.get().contains("encrypted_content"));
    }
}
