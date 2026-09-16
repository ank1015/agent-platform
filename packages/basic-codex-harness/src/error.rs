use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    NilAccountId,
    NilMachineId,
    EmptyWorkingDirectory,
    WorkingDirectoryTooLong,
    WorkingDirectoryContainsNul,
    WorkingDirectoryNotAbsolute,
    EmptyEnvironmentLabel(&'static str),
    EnvironmentLabelTooLarge(&'static str),
    EnvironmentLabelContainsNul(&'static str),
    EmptyAdditionalInstructions,
    AdditionalInstructionsTooLarge,
    UnsupportedReasoningEffort,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::NilAccountId => "account_id must not be the nil UUID",
            Self::NilMachineId => "machine_id must not be the nil UUID",
            Self::EmptyWorkingDirectory => "cwd must not be empty",
            Self::WorkingDirectoryTooLong => "cwd must not exceed 4096 bytes",
            Self::WorkingDirectoryContainsNul => "cwd must not contain a NUL character",
            Self::WorkingDirectoryNotAbsolute => {
                "cwd must be an absolute POSIX, Windows drive, or UNC path"
            }
            Self::EmptyEnvironmentLabel(field) => {
                return write!(formatter, "{field} must not be empty or only whitespace");
            }
            Self::EnvironmentLabelTooLarge(field) => {
                return write!(formatter, "{field} must not exceed 256 bytes");
            }
            Self::EnvironmentLabelContainsNul(field) => {
                return write!(formatter, "{field} must not contain a NUL character");
            }
            Self::EmptyAdditionalInstructions => {
                "additional_instructions must not be empty or only whitespace"
            }
            Self::AdditionalInstructionsTooLarge => {
                "additional_instructions must not exceed 65536 bytes"
            }
            Self::UnsupportedReasoningEffort => {
                "reasoning_effort is not supported by the selected model"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ConfigError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstructionError {
    EmptyLlmConnectionId,
    EmptyExecutionConnectionId,
}

impl fmt::Display for ConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyLlmConnectionId => "LLM gateway connection ID must not be empty",
            Self::EmptyExecutionConnectionId => "execution gateway connection ID must not be empty",
        })
    }
}

impl std::error::Error for ConstructionError {}
