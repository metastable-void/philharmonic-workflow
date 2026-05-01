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
    TemplateNotFound {
        /// Looked-up template UUID.
        template_id: Uuid,
    },

    /// Template revision does not exist.
    #[error("workflow template revision not found: {template_id}@{revision_seq}")]
    TemplateRevisionNotFound {
        /// Template UUID.
        template_id: Uuid,
        /// Expected revision sequence number.
        revision_seq: u64,
    },

    /// Instance entity does not exist.
    #[error("workflow instance not found: {instance_id}")]
    InstanceNotFound {
        /// Looked-up instance UUID.
        instance_id: Uuid,
    },

    /// Instance has no revision yet.
    #[error("workflow instance has no revisions: {instance_id}")]
    InstanceRevisionMissing {
        /// Instance UUID.
        instance_id: Uuid,
    },

    /// Revision is missing a required content attribute.
    #[error("missing content attribute '{attribute}' on {entity_name}")]
    MissingContentAttribute {
        /// Entity type name.
        entity_name: &'static str,
        /// Missing attribute name.
        attribute: &'static str,
    },

    /// Revision is missing a required entity attribute.
    #[error("missing entity attribute '{attribute}' on {entity_name}")]
    MissingEntityAttribute {
        /// Entity type name.
        entity_name: &'static str,
        /// Missing attribute name.
        attribute: &'static str,
    },

    /// Revision is missing a required scalar attribute.
    #[error("missing scalar attribute '{attribute}' on {entity_name}")]
    MissingScalarAttribute {
        /// Entity type name.
        entity_name: &'static str,
        /// Missing attribute name.
        attribute: &'static str,
    },

    /// Referenced content hash is absent from the content store.
    #[error("missing content blob for {entity_name}.{attribute}: {hash}")]
    MissingContentBlob {
        /// Entity type name.
        entity_name: &'static str,
        /// Attribute name.
        attribute: &'static str,
        /// Expected content hash.
        hash: Sha256,
    },

    /// Scalar value has unexpected type.
    #[error(
        "invalid scalar type for '{attribute}' on {entity_name}: expected {expected}, found {actual}"
    )]
    InvalidScalarType {
        /// Entity type name.
        entity_name: &'static str,
        /// Attribute name.
        attribute: &'static str,
        /// Expected scalar type.
        expected: &'static str,
        /// Actual scalar type.
        actual: &'static str,
    },

    /// Instance status value in storage is unknown.
    #[error("invalid instance status discriminant: {value}")]
    InvalidInstanceStatusDiscriminant {
        /// Unrecognised discriminant value.
        value: i64,
    },

    /// Transition is disallowed by the lifecycle state machine.
    #[error("invalid workflow transition for {instance_id}: {from:?} -> {to:?}")]
    InvalidTransition {
        /// Instance UUID.
        instance_id: Uuid,
        /// Current status.
        from: InstanceStatus,
        /// Attempted target status.
        to: InstanceStatus,
    },

    /// Operation attempted on terminal instance.
    #[error("workflow instance is terminal: {instance_id} ({status:?})")]
    InstanceTerminal {
        /// Instance UUID.
        instance_id: Uuid,
        /// Terminal status.
        status: InstanceStatus,
    },

    /// Template and instance tenant bindings disagree.
    #[error(
        "template tenant mismatch for instance {instance_id}: template {template_id} has tenant {template_tenant}, instance has tenant {instance_tenant}"
    )]
    TemplateTenantMismatch {
        /// Template UUID.
        template_id: Uuid,
        /// Instance UUID.
        instance_id: Uuid,
        /// Tenant bound to the template.
        template_tenant: Uuid,
        /// Tenant bound to the instance.
        instance_tenant: Uuid,
    },

    /// Template or instance entity-ref expected pinned revision but got latest reference.
    #[error("expected pinned entity reference for {entity_name}.{attribute}")]
    MissingPinnedReference {
        /// Entity type name.
        entity_name: &'static str,
        /// Attribute name.
        attribute: &'static str,
    },

    /// Referenced entity kind does not match expected entity kind.
    #[error("entity kind mismatch for {entity_name}: expected {expected}, found {actual}")]
    EntityKindMismatch {
        /// Entity type name.
        entity_name: &'static str,
        /// Expected kind UUID.
        expected: Uuid,
        /// Actual kind UUID.
        actual: Uuid,
    },

    /// Script source bytes are not valid UTF-8.
    #[error("invalid UTF-8 in script content")]
    ScriptUtf8(#[from] std::str::Utf8Error),

    /// JSON serialization or parsing failed.
    #[error("json error: {detail}")]
    Json {
        /// Error detail.
        detail: String,
    },

    /// Canonical JSON conversion failed.
    #[error("canonical JSON error: {0}")]
    Canonical(#[from] CanonError),

    /// Executor transport failed before a conclusive script result.
    #[error("executor unreachable: {detail}")]
    ExecutorUnreachable {
        /// Transport error detail.
        detail: String,
    },

    /// Step failed with a script-level error category.
    #[error("step execution failed: {detail}")]
    StepExecutionFailed {
        /// Script error detail.
        detail: String,
    },

    /// Numeric overflow while computing next sequence numbers.
    #[error("integer overflow while computing {field}")]
    IntegerOverflow {
        /// Field name that overflowed.
        field: &'static str,
    },

    /// Numeric conversion failed at the API/storage boundary.
    #[error("integer conversion failed for {field}: {detail}")]
    IntegerConversion {
        /// Field name.
        field: &'static str,
        /// Conversion error detail.
        detail: String,
    },
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
