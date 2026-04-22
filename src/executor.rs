use philharmonic_types::JsonValue;

use async_trait::async_trait;

/// Execution backend for one workflow step.
#[async_trait]
pub trait StepExecutor: Send + Sync {
    /// Execute the workflow script with one input argument and concrete config.
    async fn execute(
        &self,
        script: &str,
        arg: &JsonValue,
        config: &JsonValue,
    ) -> Result<JsonValue, StepExecutionError>;
}

/// Step execution failure categories.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum StepExecutionError {
    /// Execution transport was inconclusive (retry may succeed).
    #[error("executor transport failure: {0}")]
    Transport(String),

    /// Script execution completed with an explicit script-level error.
    #[error("script execution failed: {0}")]
    ScriptError(String),
}

impl StepExecutionError {
    /// Whether this error is potentially retryable.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Transport(_))
    }
}
