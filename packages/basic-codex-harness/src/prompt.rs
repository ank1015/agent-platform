use crate::BasicCodexConfig;
use agent_contracts::{ContentPart, Message, MessageBase, SessionId};

pub(crate) const BASE_INSTRUCTIONS: &str = include_str!("../assets/base_instructions.md");

pub(crate) struct PromptContext<'a> {
    pub(crate) session_id: SessionId,
    pub(crate) current_date: &'a str,
}

pub(crate) fn instructions(config: &BasicCodexConfig) -> String {
    let base = BASE_INSTRUCTIONS.trim_end();
    match &config.additional_instructions {
        Some(additional) => format!("{base}\n\n# Session instructions\n\n{additional}"),
        None => base.to_owned(),
    }
}

pub(crate) fn project_messages(
    config: &BasicCodexConfig,
    current_date: &str,
    messages: Vec<Message>,
) -> Vec<Message> {
    let mut projected = Vec::with_capacity(messages.len().saturating_add(1));
    projected.push(environment_message(config, current_date));
    projected.extend(messages);
    projected
}

pub(crate) fn environment_message(config: &BasicCodexConfig, current_date: &str) -> Message {
    let mut lines = vec![
        "<environment_context>".to_owned(),
        format!("  <cwd>{}</cwd>", escape_xml(config.cwd.as_str())),
        format!(
            "  <current_date>{}</current_date>",
            escape_xml(current_date)
        ),
    ];
    if let Some(shell) = &config.shell {
        lines.push(format!("  <shell>{}</shell>", escape_xml(shell)));
    }
    if let Some(platform) = &config.platform {
        lines.push(format!("  <platform>{}</platform>", escape_xml(platform)));
    }
    lines.push("</environment_context>".to_owned());
    Message::User {
        base: MessageBase::default(),
        content: vec![ContentPart::Text {
            text: lines.join("\n"),
            metadata: None,
        }],
    }
}

pub(crate) fn prompt_cache_key(session_id: SessionId) -> String {
    session_id.to_string()
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
            cwd: WorkingDirectory::new("/workspace/a&b").unwrap(),
            shell: Some("zsh <login>".into()),
            platform: Some("macOS".into()),
            additional_instructions: Some("Use Rust.".into()),
        }
    }

    #[test]
    fn appends_session_instructions_after_the_stable_base_prompt() {
        let instructions = instructions(&config());
        assert!(instructions.starts_with(BASE_INSTRUCTIONS.trim_end()));
        assert!(instructions.ends_with("# Session instructions\n\nUse Rust."));
        assert!(!instructions.contains("AGENTS.md"));
        assert!(!instructions.contains("sandbox"));
        assert!(!instructions.contains("approval"));
    }

    #[test]
    fn prepends_one_escaped_environment_message_without_changing_history() {
        let history = vec![Message::user_text("hello")];
        let projected = project_messages(&config(), "2026-09-15", history.clone());
        assert_eq!(projected.len(), 2);
        assert_eq!(
            serde_json::to_value(&projected[1]).unwrap(),
            serde_json::to_value(&history[0]).unwrap()
        );
        let Message::User { content, .. } = &projected[0] else {
            panic!("environment context must be a user message");
        };
        let ContentPart::Text { text, .. } = &content[0] else {
            panic!("environment context must be text");
        };
        assert!(text.contains("<cwd>/workspace/a&amp;b</cwd>"));
        assert!(text.contains("<current_date>2026-09-15</current_date>"));
        assert!(text.contains("<shell>zsh &lt;login&gt;</shell>"));
        assert!(text.contains("<platform>macOS</platform>"));
    }

    #[test]
    fn omits_optional_environment_fields_when_unset() {
        let mut config = config();
        config.shell = None;
        config.platform = None;
        let Message::User { content, .. } = environment_message(&config, "2026-09-15") else {
            panic!("environment context must be a user message");
        };
        let ContentPart::Text { text, .. } = &content[0] else {
            panic!("environment context must be text");
        };
        assert!(!text.contains("<shell>"));
        assert!(!text.contains("<platform>"));
    }

    #[test]
    fn cache_key_is_the_stable_platform_session_id() {
        let session_id = SessionId::new();
        assert_eq!(prompt_cache_key(session_id), session_id.to_string());
    }
}
