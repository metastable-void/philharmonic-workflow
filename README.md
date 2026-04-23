# philharmonic-workflow

Workflow orchestration for the Philharmonic crate family.

This crate provides:

- Entity kinds: `WorkflowTemplate`, `WorkflowInstance`, `StepRecord`.
- Subject threading with `SubjectContext` / `SubjectKind`.
- Async trait boundaries: `StepExecutor` and `ConfigLowerer`.
- `WorkflowEngine<S, E, L>` with `create_instance`, `execute_step`, `complete`, and `cancel`.

## Quick Start

```rust
use philharmonic_workflow::{
    CanonicalJson, ConfigLowerer, ConfigLoweringError, EntityId, JsonValue,
    StepExecutionError, StepExecutor, SubjectContext, WorkflowEngine,
    WorkflowInstance,
};

use async_trait::async_trait;

struct MyExecutor;

#[async_trait]
impl StepExecutor for MyExecutor {
    async fn execute(
        &self,
        _script: &str,
        _arg: &JsonValue,
        _config: &JsonValue,
    ) -> Result<JsonValue, StepExecutionError> {
        Ok(serde_json::json!({
            "context": {"state": "next"},
            "output": {"ok": true}
        }))
    }
}

struct MyLowerer;

#[async_trait]
impl ConfigLowerer for MyLowerer {
    async fn lower(
        &self,
        _abstract_config: &JsonValue,
        _instance_id: EntityId<WorkflowInstance>,
        _step_seq: u64,
        _subject: &SubjectContext,
    ) -> Result<JsonValue, ConfigLoweringError> {
        Ok(serde_json::json!({"endpoint": "resolved"}))
    }
}

# async fn run<S>(store: S,
# template_id: EntityId<philharmonic_workflow::WorkflowTemplate>,
# subject: SubjectContext)
# where S: philharmonic_store::ContentStore + philharmonic_store::IdentityStore + philharmonic_store::EntityStore {
let engine = WorkflowEngine::new(store, MyExecutor, MyLowerer);

let instance_id = engine
    .create_instance(template_id, CanonicalJson::from_value(&serde_json::json!({"arg": 1})).unwrap(), subject.clone())
    .await
    .unwrap();

let result = engine
    .execute_step(instance_id, CanonicalJson::from_value(&serde_json::json!({"input": "go"})).unwrap(), subject)
    .await
    .unwrap();

if result.is_terminal() {
    // completed/failed/cancelled
}
# }
```

## Lifecycle

`InstanceStatus` values are persisted as stable `i64` discriminants:

- `Pending`
- `Running`
- `Completed`
- `Failed`
- `Cancelled`

Terminal instances are immutable: `execute_step`, `complete`, and `cancel`
return `WorkflowError::InstanceTerminal` when called after termination.

## Audit Discipline

`StepRecord.subject` persists only:

- `kind`
- `id`
- `authority_id`

`SubjectContext.claims` are intentionally never persisted in step records.

## Contributing

This crate is developed as a submodule of the Philharmonic
workspace. Workspace-wide development conventions — git workflow,
script wrappers, Rust code rules, versioning, terminology — live
in the workspace meta-repo at
[metastable-void/philharmonic-workspace](https://github.com/metastable-void/philharmonic-workspace),
authoritatively in its
[`CONTRIBUTING.md`](https://github.com/metastable-void/philharmonic-workspace/blob/main/CONTRIBUTING.md).

SPDX-License-Identifier: Apache-2.0 OR MPL-2.0
