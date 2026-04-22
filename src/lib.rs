//! Workflow orchestration for Philharmonic.
//!
//! This crate defines three entity kinds (`WorkflowTemplate`,
//! `WorkflowInstance`, `StepRecord`), subject-context plumbing, the
//! execution/lowering trait boundaries, and the generic
//! `WorkflowEngine<S, E, L>` orchestration engine.

mod engine;
mod entities;
mod error;
mod executor;
mod lowerer;
mod status;
mod subject;

pub use engine::{StepResult, WorkflowEngine};
pub use entities::{StepRecord, WorkflowInstance, WorkflowTemplate};
pub use error::WorkflowError;
pub use executor::{StepExecutionError, StepExecutor};
pub use lowerer::{ConfigLowerer, ConfigLoweringError};
pub use status::InstanceStatus;
pub use subject::{StepRecordSubject, SubjectContext, SubjectKind};

pub use philharmonic_policy::{MintingAuthority, Tenant};
pub use philharmonic_types::{CanonicalJson, EntityId, JsonValue, Uuid};
