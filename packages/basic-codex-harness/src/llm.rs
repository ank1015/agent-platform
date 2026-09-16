use crate::{BasicCodexConfig, prompt, tool_catalog};
use agent_contracts::{
    AssistantResponse, LlmRequest, Message, MessageBase, Provider, ProviderOptions, Usage,
};
use serde_json::{json, value::to_raw_value};

pub(crate) fn fresh_request(
    config: &BasicCodexConfig,
    context: prompt::PromptContext<'_>,
    messages: Vec<Message>,
) -> LlmRequest {
    LlmRequest::Fresh {
        account_id: config.account_id,
        model_id: config.model.as_str().to_owned(),
        instructions: Some(prompt::instructions(config)),
        messages: prompt::project_messages(config, context.current_date, messages),
        tools: tool_catalog::definitions(),
        provider_options: provider_options(config, context.session_id),
    }
}

pub(crate) fn compaction_request(
    config: &BasicCodexConfig,
    context: prompt::PromptContext<'_>,
    mut messages: Vec<Message>,
) -> Result<LlmRequest, String> {
    let data = to_raw_value(&json!({ "content": [{ "type": "compaction_trigger" }] }))
        .map_err(|failure| format!("could not serialize compaction trigger: {failure}"))?;
    messages.push(Message::Custom {
        base: MessageBase::default(),
        tag: match config.provider {
            crate::BasicCodexProvider::Openai => "openai_custom_item",
            crate::BasicCodexProvider::Chatgpt => "chatgpt_custom_item",
        }
        .into(),
        data,
    });
    Ok(fresh_request(config, context, messages))
}

pub(crate) fn response_provider(
    config: &BasicCodexConfig,
    response: &AssistantResponse,
) -> Result<Provider, String> {
    if response.id.trim().is_empty() {
        return Err("LLM response ID must not be empty".into());
    }
    if response.model_id != config.model.as_str() {
        return Err(format!(
            "LLM response model {} does not match configured model {}",
            response.model_id, config.model
        ));
    }
    let Message::Assistant { provider, .. } = &response.message else {
        return Err("LLM response does not contain an assistant message".into());
    };
    let Message::Assistant { content, .. } = &response.message else {
        unreachable!("assistant message was checked above")
    };
    for (index, item) in content.iter().enumerate() {
        let value: serde_json::Value = serde_json::from_str(item.get())
            .map_err(|failure| format!("assistant item {index} is invalid JSON: {failure}"))?;
        let object = value
            .as_object()
            .ok_or_else(|| format!("assistant item {index} must be an object"))?;
        if object
            .get("type")
            .and_then(serde_json::Value::as_str)
            .is_none_or(|kind| kind.trim().is_empty())
        {
            return Err(format!("assistant item {index} requires a nonempty type"));
        }
    }
    let configured = config.provider.contract_provider();
    if *provider != configured {
        return Err(format!(
            "LLM response provider {provider:?} does not match configured provider {}",
            config.provider
        ));
    }
    Ok(*provider)
}

pub(crate) fn estimated_context_tokens(usage: Option<&Usage>) -> Option<u64> {
    let usage = usage?;
    let input = usage.input?;
    let output = usage.output?;
    Some(
        input
            .saturating_add(usage.cache_read.unwrap_or(0))
            .saturating_add(usage.cache_write.unwrap_or(0))
            .saturating_add(output),
    )
}

pub(crate) fn instructions(config: &BasicCodexConfig) -> String {
    prompt::instructions(config)
}

fn provider_options(
    config: &BasicCodexConfig,
    session_id: agent_contracts::SessionId,
) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    options.insert(
        "reasoning".into(),
        json!({ "effort": config.reasoning_effort.as_str() }),
    );
    options.insert("parallel_tool_calls".into(), json!(true));
    options.insert("store".into(), json!(false));
    options.insert("include".into(), json!(["reasoning.encrypted_content"]));
    options.insert("codex_remote_compaction_v2".into(), json!(true));
    options.insert(
        "prompt_cache_key".into(),
        json!(prompt::prompt_cache_key(session_id)),
    );
    options
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BasicCodexModel, BasicCodexProvider, ReasoningEffort, WorkingDirectory};
    use uuid::Uuid;

    fn config() -> BasicCodexConfig {
        BasicCodexConfig {
            account_id: Uuid::new_v4(),
            provider: BasicCodexProvider::Openai,
            model: BasicCodexModel::Terra,
            reasoning_effort: ReasoningEffort::High,
            machine_id: Uuid::new_v4(),
            cwd: WorkingDirectory::new("/workspace").unwrap(),
            shell: Some("zsh".into()),
            platform: Some("macos".into()),
            additional_instructions: Some("Use Rust.".into()),
        }
    }

    fn context(session_id: agent_contracts::SessionId) -> prompt::PromptContext<'static> {
        prompt::PromptContext {
            session_id,
            current_date: "2026-09-15",
        }
    }

    #[test]
    fn fresh_request_has_stable_minimal_settings() {
        let session_id = agent_contracts::SessionId::new();
        let request = fresh_request(
            &config(),
            context(session_id),
            vec![Message::user_text("hello")],
        );
        let LlmRequest::Fresh {
            model_id,
            instructions,
            messages,
            tools,
            provider_options,
            ..
        } = request
        else {
            panic!("expected fresh request");
        };
        assert_eq!(model_id, "gpt-5.6-terra");
        assert_eq!(messages.len(), 2);
        assert_eq!(tools.len(), 4);
        assert_eq!(provider_options["reasoning"], json!({"effort":"high"}));
        assert_eq!(provider_options["parallel_tool_calls"], true);
        assert_eq!(provider_options["store"], false);
        assert_eq!(
            provider_options["include"],
            json!(["reasoning.encrypted_content"])
        );
        assert_eq!(provider_options["codex_remote_compaction_v2"], true);
        assert_eq!(provider_options["prompt_cache_key"], session_id.to_string());
        let instructions = instructions.unwrap();
        assert!(instructions.starts_with(prompt::BASE_INSTRUCTIONS.trim_end()));
        assert!(instructions.ends_with("# Session instructions\n\nUse Rust."));
    }

    #[test]
    fn compaction_request_appends_a_provider_native_trigger() {
        let session_id = agent_contracts::SessionId::new();
        let request = compaction_request(
            &config(),
            context(session_id),
            vec![Message::user_text("hello")],
        )
        .unwrap();
        let LlmRequest::Fresh {
            messages,
            provider_options,
            ..
        } = request
        else {
            panic!("expected fresh request");
        };
        assert_eq!(messages.len(), 3);
        let Message::Custom { tag, data, .. } = &messages[2] else {
            panic!("expected a native custom item");
        };
        assert_eq!(tag, "openai_custom_item");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(data.get()).unwrap(),
            json!({"content":[{"type":"compaction_trigger"}]})
        );
        assert_eq!(provider_options["prompt_cache_key"], session_id.to_string());
    }

    #[test]
    fn normal_and_compaction_requests_share_the_same_stable_prefix() {
        let config = config();
        let session_id = agent_contracts::SessionId::new();
        let history = vec![Message::user_text("hello")];
        let normal = fresh_request(&config, context(session_id), history.clone());
        let compacting = compaction_request(&config, context(session_id), history).unwrap();
        let (
            LlmRequest::Fresh {
                instructions: normal_instructions,
                messages: normal_messages,
                tools: normal_tools,
                provider_options: normal_options,
                ..
            },
            LlmRequest::Fresh {
                instructions: compacting_instructions,
                messages: compacting_messages,
                tools: compacting_tools,
                provider_options: compacting_options,
                ..
            },
        ) = (normal, compacting)
        else {
            panic!("expected fresh requests");
        };
        assert_eq!(normal_instructions, compacting_instructions);
        assert_eq!(
            serde_json::to_value(normal_tools).unwrap(),
            serde_json::to_value(compacting_tools).unwrap()
        );
        assert_eq!(normal_options, compacting_options);
        assert_eq!(
            serde_json::to_value(&normal_messages[0]).unwrap(),
            serde_json::to_value(&compacting_messages[0]).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&normal_messages[1]).unwrap(),
            serde_json::to_value(&compacting_messages[1]).unwrap()
        );
    }

    #[test]
    fn openai_and_chatgpt_receive_the_same_codex_model_options() {
        let session_id = agent_contracts::SessionId::new();
        let openai = fresh_request(&config(), context(session_id), Vec::new());
        let mut chatgpt_config = config();
        chatgpt_config.provider = BasicCodexProvider::Chatgpt;
        let chatgpt = fresh_request(&chatgpt_config, context(session_id), Vec::new());
        let (
            LlmRequest::Fresh {
                provider_options: openai_options,
                ..
            },
            LlmRequest::Fresh {
                provider_options: chatgpt_options,
                ..
            },
        ) = (openai, chatgpt)
        else {
            panic!("expected fresh requests");
        };
        assert_eq!(openai_options, chatgpt_options);
        assert_eq!(openai_options["reasoning"], json!({"effort":"high"}));
        assert_eq!(openai_options["parallel_tool_calls"], true);
        assert_eq!(openai_options["store"], false);
        assert_eq!(
            openai_options["include"],
            json!(["reasoning.encrypted_content"])
        );
        assert_eq!(openai_options["codex_remote_compaction_v2"], true);
        assert_eq!(openai_options["prompt_cache_key"], session_id.to_string());
    }
}
