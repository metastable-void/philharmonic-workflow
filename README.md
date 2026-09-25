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

`StepRecord.subject` persists:

- `kind`
- `id`
- `authority_id`
- `claims: Option<JsonValue>` — the caller's `SubjectContext.claims`,
  persisted verbatim when non-empty (`null` and `{}` both persist as
  `None`). This is deliberately opaque: the framework makes no
  assumption about claim shape or meaning, and does not select,
  filter, or interpret any specific key. A consumer that wants to
  surface one particular claim (e.g. a display-only contact address)
  owns that convention entirely in its own code — encoding a
  consumer-specific field name into this shared type would be a
  framework/workflow layering violation (see the root workspace
  `HUMANS.md`'s "Clean separation of concerns" note).

Revised 2026-09-25: earlier revisions of this crate never persisted
claims at all (see `docs/design/14-open-questions.md` and
`docs/design/15-v1-scope.md` in the parent workspace for the prior
decision and why it changed).

**Security note — read-permission granularity.** `workflow:instance_read`
(`philharmonic-api`'s `GET /v1/workflows/instances/{id}/steps`) is
scoped to the *instance*, not to the individual step or the
principal that executed it: any caller holding that permission on
an instance can read every step's `subject` — including `claims` —
regardless of which principal ran that step. This matters for any
workflow with more than one principal sharing an instance (e.g.
`philharmonic-chat` mints both an agent-side and a customer-side
ephemeral token against the same `instance_id`, and its own guide
at `docs/guide/end-user-session-tokens.md` grants the customer's
token `workflow:instance_read` so its browser can poll the
transcript). A principal could in principle read another
co-principal's injected claims on the same instance via this raw
API, bypassing whatever access control a consuming bin's own UI
applies.

This is accepted for now (2026-09-25) specifically because no
current consumer of this engine injects non-empty claims for a
principal on a shared, multi-principal instance —
`philharmonic-chat`'s own minting code injects empty claims for
both ends of every chat it creates. If a future consumer *does*
inject sensitive claims into a multi-principal instance, this
persistence contract and the engine's instance-level (rather than
per-step/per-principal) read-permission granularity need to be
revisited together before that ships.

## Contributing

This crate is developed as a submodule of the Philharmonic
workspace. Workspace-wide development conventions — git workflow,
script wrappers, Rust code rules, versioning, terminology — live
in the workspace meta-repo at
[metastable-void/philharmonic-workspace](https://github.com/metastable-void/philharmonic-workspace),
authoritatively in its
[`CONTRIBUTING.md`](https://github.com/metastable-void/philharmonic-workspace/blob/main/CONTRIBUTING.md).

SPDX-License-Identifier: Apache-2.0 OR MPL-2.0
