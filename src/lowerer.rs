use crate::{SubjectContext, WorkflowInstance};

use philharmonic_types::{EntityId, JsonValue};

use async_trait::async_trait;

/// Abstract-to-concrete config lowering for a single step.
#[async_trait]
pub trait ConfigLowerer: Send + Sync {
    /// Lower abstract template config into concrete step config.
    async fn lower(
        &self,
        abstract_config: &JsonValue,
        instance_id: EntityId<WorkflowInstance>,
        step_seq: u64,
        subject: &SubjectContext,
    ) -> Result<JsonValue, ConfigLoweringError>;
}

/// Config lowering failure categories.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigLoweringError {
    /// Lowerer backend was unreachable or transiently unavailable.
    #[error("lowerer backend failure: {0}")]
    Backend(String),

    /// Lowerer rejected the abstract configuration as invalid.
    #[error("invalid abstract config: {0}")]
    InvalidConfig(String),
}

impl ConfigLoweringError {
    /// Whether this error is potentially retryable.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Backend(_))
    }
}
