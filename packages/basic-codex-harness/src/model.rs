use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

use agent_contracts::Provider;

pub const ACTIVE_CONTEXT_WINDOW: u64 = 272_000;
pub const AUTO_COMPACT_LIMIT: u64 = 244_800;

const SUPPORTED_REASONING_EFFORTS: &[ReasoningEffort] = &[
    ReasoningEffort::Low,
    ReasoningEffort::Medium,
    ReasoningEffort::High,
    ReasoningEffort::Xhigh,
    ReasoningEffort::Max,
];

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[schemars(rename_all = "snake_case")]
pub enum BasicCodexProvider {
    Openai,
    Chatgpt,
}

impl BasicCodexProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Chatgpt => "chatgpt",
        }
    }

    pub const fn contract_provider(self) -> Provider {
        match self {
            Self::Openai => Provider::Openai,
            Self::Chatgpt => Provider::Chatgpt,
        }
    }
}

impl fmt::Display for BasicCodexProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
pub enum BasicCodexModel {
    #[serde(rename = "gpt-5.6-sol")]
    #[schemars(rename = "gpt-5.6-sol")]
    Sol,
    #[serde(rename = "gpt-5.6-terra")]
    #[schemars(rename = "gpt-5.6-terra")]
    Terra,
    #[serde(rename = "gpt-5.6-luna")]
    #[schemars(rename = "gpt-5.6-luna")]
    Luna,
}

impl BasicCodexModel {
    pub const ALL: [Self; 3] = [Self::Sol, Self::Terra, Self::Luna];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sol => "gpt-5.6-sol",
            Self::Terra => "gpt-5.6-terra",
            Self::Luna => "gpt-5.6-luna",
        }
    }

    pub const fn profile(self) -> ModelProfile {
        ModelProfile {
            model_id: self.as_str(),
            default_reasoning_effort: match self {
                Self::Sol => ReasoningEffort::Low,
                Self::Terra | Self::Luna => ReasoningEffort::Medium,
            },
            allowed_reasoning_efforts: SUPPORTED_REASONING_EFFORTS,
            context_window: ACTIVE_CONTEXT_WINDOW,
            auto_compact_limit: AUTO_COMPACT_LIMIT,
            supports_images: true,
            supports_parallel_tools: true,
        }
    }

    pub fn supports(self, effort: ReasoningEffort) -> bool {
        self.profile().allowed_reasoning_efforts.contains(&effort)
    }
}

impl fmt::Display for BasicCodexModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[schemars(rename_all = "snake_case")]
pub enum ReasoningEffort {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl ReasoningEffort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

impl fmt::Display for ReasoningEffort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelProfile {
    pub model_id: &'static str,
    pub default_reasoning_effort: ReasoningEffort,
    pub allowed_reasoning_efforts: &'static [ReasoningEffort],
    pub context_window: u64,
    pub auto_compact_limit: u64,
    pub supports_images: bool,
    pub supports_parallel_tools: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_slugs_round_trip_and_reject_unknown_models() {
        for model in BasicCodexModel::ALL {
            let json = serde_json::to_string(&model).unwrap();
            assert_eq!(json, format!("\"{}\"", model.as_str()));
            assert_eq!(
                serde_json::from_str::<BasicCodexModel>(&json).unwrap(),
                model
            );
        }
        assert!(serde_json::from_str::<BasicCodexModel>("\"gpt-5.5\"").is_err());
    }

    #[test]
    fn reasoning_efforts_have_exact_wire_names() {
        for (effort, wire) in [
            (ReasoningEffort::Low, "low"),
            (ReasoningEffort::Medium, "medium"),
            (ReasoningEffort::High, "high"),
            (ReasoningEffort::Xhigh, "xhigh"),
            (ReasoningEffort::Max, "max"),
        ] {
            assert_eq!(
                serde_json::to_string(&effort).unwrap(),
                format!("\"{wire}\"")
            );
        }
        assert!(serde_json::from_str::<ReasoningEffort>("\"ultra\"").is_err());
    }

    #[test]
    fn providers_have_exact_wire_names_and_exclude_fireworks() {
        for (provider, wire) in [
            (BasicCodexProvider::Openai, "openai"),
            (BasicCodexProvider::Chatgpt, "chatgpt"),
        ] {
            assert_eq!(
                serde_json::to_string(&provider).unwrap(),
                format!("\"{wire}\"")
            );
        }
        assert!(serde_json::from_str::<BasicCodexProvider>("\"fireworks\"").is_err());
    }

    #[test]
    fn profiles_match_agreed_defaults_and_limits() {
        assert_eq!(
            BasicCodexModel::Sol.profile().default_reasoning_effort,
            ReasoningEffort::Low
        );
        for model in [BasicCodexModel::Terra, BasicCodexModel::Luna] {
            assert_eq!(
                model.profile().default_reasoning_effort,
                ReasoningEffort::Medium
            );
        }
        for model in BasicCodexModel::ALL {
            let profile = model.profile();
            assert_eq!(profile.context_window, 272_000);
            assert_eq!(profile.auto_compact_limit, 244_800);
            assert!(profile.supports_images);
            assert!(profile.supports_parallel_tools);
            assert!(model.supports(ReasoningEffort::Max));
        }
    }
}
