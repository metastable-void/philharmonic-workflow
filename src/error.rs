use crate::{ConfigLoweringError, InstanceStatus};

use philharmonic_store::StoreError;
use philharmonic_types::{CanonError, Sha256, Uuid};

/// Workflow-engine errors.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    /// Storage substrate failure.
    #[error("store error: {0}")]
    Store(#[from] StoreError),

    /// Config lowering failure.
    #[error("config lowering failed: {0}")]
    ConfigLowering(#[from] ConfigLoweringError),

    /// Template entity does not exist.
    #[error("workflow template not found: {template_id}")]
    TemplateNotFound { template_id: Uuid },

    /// Template revision does not exist.
    #[error("workflow template revision not found: {template_id}@{revision_seq}")]
    TemplateRevisionNotFound {
        template_id: Uuid,
        revision_seq: u64,
    },

    /// Instance entity does not exist.
    #[error("workflow instance not found: {instance_id}")]
    InstanceNotFound { instance_id: Uuid },

    /// Instance has no revision yet.
    #[error("workflow instance has no revisions: {instance_id}")]
    InstanceRevisionMissing { instance_id: Uuid },

    /// Revision is missing a required content attribute.
    #[error("missing content attribute '{attribute}' on {entity_name}")]
    MissingContentAttribute {
        entity_name: &'static str,
        attribute: &'static str,
    },

    /// Revision is missing a required entity attribute.
    #[error("missing entity attribute '{attribute}' on {entity_name}")]
    MissingEntityAttribute {
        entity_name: &'static str,
        attribute: &'static str,
    },

    /// Revision is missing a required scalar attribute.
    #[error("missing scalar attribute '{attribute}' on {entity_name}")]
    MissingScalarAttribute {
        entity_name: &'static str,
        attribute: &'static str,
    },

    /// Referenced content hash is absent from the content store.
    #[error("missing content blob for {entity_name}.{attribute}: {hash}")]
    MissingContentBlob {
        entity_name: &'static str,
        attribute: &'static str,
        hash: Sha256,
    },

    /// Scalar value has unexpected type.
    #[error(
        "invalid scalar type for '{attribute}' on {entity_name}: expected {expected}, found {actual}"
    )]
    InvalidScalarType {
        entity_name: &'static str,
        attribute: &'static str,
        expected: &'static str,
        actual: &'static str,
    },

    /// Instance status value in storage is unknown.
    #[error("invalid instance status discriminant: {value}")]
    InvalidInstanceStatusDiscriminant { value: i64 },

    /// Transition is disallowed by the lifecycle state machine.
    #[error("invalid workflow transition for {instance_id}: {from:?} -> {to:?}")]
    InvalidTransition {
        instance_id: Uuid,
        from: InstanceStatus,
        to: InstanceStatus,
    },

    /// Operation attempted on terminal instance.
    #[error("workflow instance is terminal: {instance_id} ({status:?})")]
    InstanceTerminal {
        instance_id: Uuid,
        status: InstanceStatus,
    },

    /// Template and instance tenant bindings disagree.
    #[error(
        "template tenant mismatch for instance {instance_id}: template {template_id} has tenant {template_tenant}, instance has tenant {instance_tenant}"
    )]
    TemplateTenantMismatch {
        template_id: Uuid,
        instance_id: Uuid,
        template_tenant: Uuid,
        instance_tenant: Uuid,
    },

    /// Template or instance entity-ref expected pinned revision but got latest reference.
    #[error("expected pinned entity reference for {entity_name}.{attribute}")]
    MissingPinnedReference {
        entity_name: &'static str,
        attribute: &'static str,
    },

    /// Referenced entity kind does not match expected entity kind.
    #[error("entity kind mismatch for {entity_name}: expected {expected}, found {actual}")]
    EntityKindMismatch {
        entity_name: &'static str,
        expected: Uuid,
        actual: Uuid,
    },

    /// Script source bytes are not valid UTF-8.
    #[error("invalid UTF-8 in script content")]
    ScriptUtf8(#[from] std::str::Utf8Error),

    /// JSON serialization or parsing failed.
    #[error("json error: {detail}")]
    Json { detail: String },

    /// Canonical JSON conversion failed.
    #[error("canonical JSON error: {0}")]
    Canonical(#[from] CanonError),

    /// Executor transport failed before a conclusive script result.
    #[error("executor unreachable: {detail}")]
    ExecutorUnreachable { detail: String },

    /// Step failed with a script-level error category.
    #[error("step execution failed: {detail}")]
    StepExecutionFailed { detail: String },

    /// Numeric overflow while computing next sequence numbers.
    #[error("integer overflow while computing {field}")]
    IntegerOverflow { field: &'static str },

    /// Numeric conversion failed at the API/storage boundary.
    #[error("integer conversion failed for {field}: {detail}")]
    IntegerConversion { field: &'static str, detail: String },
}

impl WorkflowError {
    /// Whether this failure is potentially retryable.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Store(error) => error.is_retryable(),
            Self::ConfigLowering(error) => error.is_retryable(),
            Self::ExecutorUnreachable { .. } => true,
            Self::TemplateNotFound { .. }
            | Self::TemplateRevisionNotFound { .. }
            | Self::InstanceNotFound { .. }
            | Self::InstanceRevisionMissing { .. }
            | Self::MissingContentAttribute { .. }
            | Self::MissingEntityAttribute { .. }
            | Self::MissingScalarAttribute { .. }
            | Self::MissingContentBlob { .. }
            | Self::InvalidScalarType { .. }
            | Self::InvalidInstanceStatusDiscriminant { .. }
            | Self::InvalidTransition { .. }
            | Self::InstanceTerminal { .. }
            | Self::TemplateTenantMismatch { .. }
            | Self::MissingPinnedReference { .. }
            | Self::EntityKindMismatch { .. }
            | Self::ScriptUtf8(_)
            | Self::Json { .. }
            | Self::Canonical(_)
            | Self::StepExecutionFailed { .. }
            | Self::IntegerOverflow { .. }
            | Self::IntegerConversion { .. } => false,
        }
    }

    pub(crate) fn json(detail: impl Into<String>) -> Self {
        Self::Json {
            detail: detail.into(),
        }
    }
}
