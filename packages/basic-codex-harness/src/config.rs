use crate::{BasicCodexModel, BasicCodexProvider, ConfigError, ReasoningEffort};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

const MAX_WORKING_DIRECTORY_BYTES: usize = 4 * 1024;
const MAX_ADDITIONAL_INSTRUCTIONS_BYTES: usize = 64 * 1024;
const MAX_ENVIRONMENT_LABEL_BYTES: usize = 256;

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasicCodexConfig {
    pub account_id: Uuid,
    pub provider: BasicCodexProvider,
    pub model: BasicCodexModel,
    pub reasoning_effort: ReasoningEffort,
    pub machine_id: Uuid,
    pub cwd: WorkingDirectory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_instructions: Option<String>,
}

impl BasicCodexConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.account_id.is_nil() {
            return Err(ConfigError::NilAccountId);
        }
        if self.machine_id.is_nil() {
            return Err(ConfigError::NilMachineId);
        }
        self.cwd.validate()?;
        validate_environment_label(self.shell.as_deref(), "shell")?;
        validate_environment_label(self.platform.as_deref(), "platform")?;
        if !self.model.supports(self.reasoning_effort) {
            return Err(ConfigError::UnsupportedReasoningEffort);
        }
        if let Some(instructions) = &self.additional_instructions {
            if instructions.trim().is_empty() {
                return Err(ConfigError::EmptyAdditionalInstructions);
            }
            if instructions.len() > MAX_ADDITIONAL_INSTRUCTIONS_BYTES {
                return Err(ConfigError::AdditionalInstructionsTooLarge);
            }
        }
        Ok(())
    }
}

fn validate_environment_label(value: Option<&str>, field: &'static str) -> Result<(), ConfigError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        return Err(ConfigError::EmptyEnvironmentLabel(field));
    }
    if value.len() > MAX_ENVIRONMENT_LABEL_BYTES {
        return Err(ConfigError::EnvironmentLabelTooLarge(field));
    }
    if value.contains('\0') {
        return Err(ConfigError::EnvironmentLabelContainsNul(field));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkingDirectory(String);

impl WorkingDirectory {
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigError> {
        let directory = Self(value.into());
        directory.validate()?;
        Ok(directory)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.0.trim().is_empty() {
            return Err(ConfigError::EmptyWorkingDirectory);
        }
        if self.0.len() > MAX_WORKING_DIRECTORY_BYTES {
            return Err(ConfigError::WorkingDirectoryTooLong);
        }
        if self.0.contains('\0') {
            return Err(ConfigError::WorkingDirectoryContainsNul);
        }
        if !is_platform_absolute(&self.0) {
            return Err(ConfigError::WorkingDirectoryNotAbsolute);
        }
        Ok(())
    }
}

impl AsRef<str> for WorkingDirectory {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for WorkingDirectory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn is_platform_absolute(path: &str) -> bool {
    if path.starts_with('/') || path.starts_with(r"\\") {
        return true;
    }
    let bytes = path.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

#[cfg(test)]
mod tests {
    use super::*;
    use schemars::schema_for;
    use serde_json::json;

    fn valid() -> BasicCodexConfig {
        BasicCodexConfig {
            account_id: Uuid::new_v4(),
            provider: BasicCodexProvider::Openai,
            model: BasicCodexModel::Terra,
            reasoning_effort: ReasoningEffort::Medium,
            machine_id: Uuid::new_v4(),
            cwd: WorkingDirectory::new("/workspace/project").unwrap(),
            shell: None,
            platform: None,
            additional_instructions: None,
        }
    }

    #[test]
    fn accepts_posix_windows_and_unc_absolute_paths() {
        for path in [
            "/workspace/project",
            r"C:\workspace\project",
            r"\\server\share",
        ] {
            assert_eq!(WorkingDirectory::new(path).unwrap().as_str(), path);
        }
    }

    #[test]
    fn rejects_invalid_working_directories() {
        assert_eq!(
            WorkingDirectory::new("relative/path").unwrap_err(),
            ConfigError::WorkingDirectoryNotAbsolute
        );
        assert_eq!(
            WorkingDirectory::new(" ").unwrap_err(),
            ConfigError::EmptyWorkingDirectory
        );
        assert_eq!(
            WorkingDirectory::new("/bad\0path").unwrap_err(),
            ConfigError::WorkingDirectoryContainsNul
        );
        assert_eq!(
            WorkingDirectory::new(format!("/{}", "a".repeat(4096))).unwrap_err(),
            ConfigError::WorkingDirectoryTooLong
        );
    }

    #[test]
    fn validates_ids_and_additional_instructions() {
        let mut config = valid();
        config.account_id = Uuid::nil();
        assert_eq!(config.validate().unwrap_err(), ConfigError::NilAccountId);

        let mut config = valid();
        config.machine_id = Uuid::nil();
        assert_eq!(config.validate().unwrap_err(), ConfigError::NilMachineId);

        let mut config = valid();
        config.additional_instructions = Some("  ".into());
        assert_eq!(
            config.validate().unwrap_err(),
            ConfigError::EmptyAdditionalInstructions
        );

        let mut config = valid();
        config.additional_instructions = Some("x".repeat(65_537));
        assert_eq!(
            config.validate().unwrap_err(),
            ConfigError::AdditionalInstructionsTooLarge
        );
    }

    #[test]
    fn validates_optional_shell_and_platform() {
        let mut config = valid();
        config.shell = Some("  ".into());
        assert_eq!(
            config.validate().unwrap_err(),
            ConfigError::EmptyEnvironmentLabel("shell")
        );

        let mut config = valid();
        config.platform = Some("x".repeat(257));
        assert_eq!(
            config.validate().unwrap_err(),
            ConfigError::EnvironmentLabelTooLarge("platform")
        );

        let mut config = valid();
        config.shell = Some("zsh\0bad".into());
        assert_eq!(
            config.validate().unwrap_err(),
            ConfigError::EnvironmentLabelContainsNul("shell")
        );
    }

    #[test]
    fn configuration_has_strict_json_shape_and_schema() {
        let value = serde_json::to_value(valid()).unwrap();
        let mut object = value.as_object().unwrap().clone();
        object.insert("llm_connection_id".into(), json!("not-session-owned"));
        assert!(serde_json::from_value::<BasicCodexConfig>(object.into()).is_err());

        let schema = serde_json::to_value(schema_for!(BasicCodexConfig)).unwrap();
        let schema = schema.to_string();
        for required in [
            "account_id",
            "provider",
            "model",
            "reasoning_effort",
            "machine_id",
            "cwd",
            "shell",
            "platform",
        ] {
            assert!(schema.contains(required), "schema is missing {required}");
        }
        assert!(!schema.contains("llm_connection_id"));
        assert!(!schema.contains("execution_connection_id"));
    }
}
