mod common;

use common::{
    MockExecutor, MockLowerer, MockStore, StoreCall, canonical_json, ephemeral_subject,
    load_canonical_json, load_step_records, principal_subject, read_instance_status, seed_template,
    seed_tenant,
};

use philharmonic_store::EntityStoreExt;
use philharmonic_types::{Entity, JsonValue, ScalarValue};
use philharmonic_workflow::{
    InstanceStatus, StepExecutionError, StepRecord, StepRecordSubject, WorkflowEngine,
    WorkflowError, WorkflowInstance,
};

use serde_json::json;

fn new_engine(
    store: &MockStore,
    executor: &MockExecutor,
    lowerer: &MockLowerer,
) -> WorkflowEngine<MockStore, MockExecutor, MockLowerer> {
    WorkflowEngine::new(store.clone(), executor.clone(), lowerer.clone())
}

fn step_seq(row: &philharmonic_store::RevisionRow) -> i64 {
    match row.scalar_attrs.get("step_seq").expect("step_seq scalar") {
        ScalarValue::I64(value) => *value,
        ScalarValue::Bool(_) => panic!("step_seq must be i64"),
    }
}

fn outcome(row: &philharmonic_store::RevisionRow) -> i64 {
    match row.scalar_attrs.get("outcome").expect("outcome scalar") {
        ScalarValue::I64(value) => *value,
        ScalarValue::Bool(_) => panic!("outcome must be i64"),
    }
}

fn script_arg_object<'a>(
    arg: &'a JsonValue,
    field: &str,
) -> &'a serde_json::Map<String, JsonValue> {
    arg.get(field)
        .and_then(JsonValue::as_object)
        .unwrap_or_else(|| panic!("script arg field {field} must be an object"))
}

#[tokio::test(flavor = "current_thread")]
async fn execute_step_writes_step_record_before_instance_revision() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-order").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-a" }),
    )
    .await;

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(
        json!({ "context": { "step": 1 }, "output": { "ok": true } }),
    ));

    let creator = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(template_id, canonical_json(&json!({ "arg": 1 })), creator)
        .await
        .unwrap();

    store.clear_calls();

    let actor = ephemeral_subject(&store, tenant_id, "ephemeral-a", json!({ "trace": "x" })).await;
    let _ = engine
        .execute_step(instance_id, canonical_json(&json!({ "input": 1 })), actor)
        .await
        .unwrap();

    let calls = store.calls();

    let step_append_idx = calls
        .iter()
        .position(|call| {
            matches!(
                call,
                StoreCall::AppendRevision {
                    kind,
                    revision_seq: 0,
                    ..
                } if *kind == StepRecord::KIND
            )
        })
        .expect("step-record append revision call");

    let instance_append_idx = calls
        .iter()
        .position(|call| {
            matches!(
                call,
                StoreCall::AppendRevision {
                    kind,
                    revision_seq: 1,
                    ..
                } if *kind == WorkflowInstance::KIND
            )
        })
        .expect("instance append revision call");

    assert!(step_append_idx < instance_append_idx);
}

#[tokio::test(flavor = "current_thread")]
async fn subject_from_execute_step_is_recorded_on_step_not_creator_subject() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-subject").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-b" }),
    )
    .await;

    let creator = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "value" })),
            creator.clone(),
        )
        .await
        .unwrap();

    let step_subject = ephemeral_subject(
        &store,
        tenant_id,
        "ephemeral-user-42",
        json!({ "user_id": "u42", "locale": "ja-JP" }),
    )
    .await;

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(
        json!({ "context": { "state": "next" }, "output": { "ok": true } }),
    ));

    engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "go" })),
            step_subject.clone(),
        )
        .await
        .unwrap();

    let rows = load_step_records(&store, instance_id).await;
    assert_eq!(rows.len(), 1);

    let subject_hash = rows[0].content_attrs.get("subject").copied().unwrap();
    let subject_content = load_canonical_json(&store, subject_hash).await;
    let persisted: StepRecordSubject = subject_content.to_deserializable().unwrap();

    assert_eq!(persisted, step_subject.to_step_record_subject());
    assert_ne!(persisted.id, creator.id);
}

#[tokio::test(flavor = "current_thread")]
async fn step_record_subject_never_persists_claims() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-audit").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-c" }),
    )
    .await;

    let creator = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": true })),
            creator,
        )
        .await
        .unwrap();

    let claims = json!({
        "marker": "SHOULD_NOT_PERSIST",
        "nested": { "plan": "enterprise", "locale": "en-US" }
    });
    let step_subject = ephemeral_subject(&store, tenant_id, "ephemeral-claims", claims).await;

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(json!({ "context": { "n": 1 }, "output": { "ok": 1 } })));

    engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": 1 })),
            step_subject,
        )
        .await
        .unwrap();

    let rows = load_step_records(&store, instance_id).await;
    assert_eq!(rows.len(), 1);

    let subject_hash = rows[0].content_attrs.get("subject").copied().unwrap();
    let subject_content = load_canonical_json(&store, subject_hash).await;
    let subject_json: serde_json::Value = subject_content.to_deserializable().unwrap();

    let object = subject_json.as_object().expect("subject object");
    assert!(!object.contains_key("claims"));
    assert!(!object.contains_key("tenant_id"));
}

#[tokio::test(flavor = "current_thread")]
async fn script_arg_includes_public_instance_and_flat_principal_subject() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-script-arg-principal").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-script-arg-principal" }),
    )
    .await;

    let principal = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "value" })),
            principal.clone(),
        )
        .await
        .unwrap();

    lowerer.push_response(Ok(json!({ "config": "first" })));
    executor.push_response(Ok(
        json!({ "context": { "step": 1 }, "output": { "ok": 1 } }),
    ));
    let first = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "first" })),
            principal.clone(),
        )
        .await
        .unwrap();

    lowerer.push_response(Ok(json!({ "config": "second" })));
    executor.push_response(Ok(
        json!({ "context": { "step": 2 }, "output": { "ok": 2 } }),
    ));
    let second = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "second" })),
            principal,
        )
        .await
        .unwrap();

    let calls = executor.calls();
    assert_eq!(calls.len(), 2);

    let public_instance = instance_id.public().as_uuid().to_string();
    let internal_instance = instance_id.internal().as_uuid().to_string();
    let public_tenant = tenant_id.public().as_uuid().to_string();
    let internal_tenant = tenant_id.internal().as_uuid().to_string();

    let first_arg = &calls[0].arg;
    assert!(first_arg.get("context").is_some());
    assert!(first_arg.get("args").is_some());
    assert!(first_arg.get("input").is_some());
    assert!(first_arg.get("data").is_some());
    assert_eq!(first_arg["context"], json!({}));
    assert_eq!(first_arg["args"], json!({ "arg": "value" }));
    assert_eq!(first_arg["input"], json!({ "input": "first" }));
    assert_eq!(first_arg["data"], json!({}));

    let first_instance = script_arg_object(first_arg, "instance");
    assert_eq!(first_instance.len(), 2);
    assert_eq!(
        first_instance.get("id").and_then(JsonValue::as_str),
        Some(public_instance.as_str())
    );
    assert_ne!(
        first_instance.get("id").and_then(JsonValue::as_str),
        Some(internal_instance.as_str())
    );
    assert_eq!(
        first_instance.get("step").and_then(JsonValue::as_u64),
        Some(first.step_seq)
    );
    assert_eq!(first.step_seq, 1);

    let second_arg = &calls[1].arg;
    let second_instance = script_arg_object(second_arg, "instance");
    assert_eq!(
        second_instance.get("id").and_then(JsonValue::as_str),
        Some(public_instance.as_str())
    );
    assert_eq!(
        second_instance.get("step").and_then(JsonValue::as_u64),
        Some(second.step_seq)
    );
    assert_eq!(second.step_seq, first.step_seq + 1);

    let records = load_step_records(&store, instance_id).await;
    assert_eq!(records.len(), 2);
    let first_record_seq = i64::try_from(first.step_seq).expect("step seq fits i64");
    let second_record_seq = i64::try_from(second.step_seq).expect("step seq fits i64");
    assert!(records.iter().any(|row| step_seq(row) == first_record_seq));
    assert!(records.iter().any(|row| step_seq(row) == second_record_seq));

    let subject = script_arg_object(first_arg, "subject");
    assert_eq!(subject.len(), 5);
    assert_eq!(subject.get("kind"), Some(&json!("principal")));
    assert_eq!(subject.get("id"), Some(&json!("principal-subject")));
    assert_eq!(
        subject.get("tenant_id").and_then(JsonValue::as_str),
        Some(public_tenant.as_str())
    );
    assert_ne!(
        subject.get("tenant_id").and_then(JsonValue::as_str),
        Some(internal_tenant.as_str())
    );
    assert!(subject.get("authority_id").is_some_and(JsonValue::is_null));
    assert_eq!(subject.get("claims"), Some(&json!({})));
}

#[tokio::test(flavor = "current_thread")]
async fn script_arg_flattens_ephemeral_subject() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-script-arg-ephemeral").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-script-arg-ephemeral" }),
    )
    .await;

    let principal = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(template_id, canonical_json(&json!({})), principal)
        .await
        .unwrap();

    let claims = json!({ "user_id": "u42", "locale": "ja-JP" });
    let step_subject =
        ephemeral_subject(&store, tenant_id, "ephemeral-user-42", claims.clone()).await;
    let authority_id = step_subject.authority_id.expect("ephemeral authority");

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(json!({ "context": {}, "output": { "ok": true } })));
    engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "go" })),
            step_subject,
        )
        .await
        .unwrap();

    let calls = executor.calls();
    assert_eq!(calls.len(), 1);

    let subject = script_arg_object(&calls[0].arg, "subject");
    assert_eq!(subject.len(), 5);
    assert_eq!(subject.get("kind"), Some(&json!("ephemeral")));
    assert_eq!(subject.get("id"), Some(&json!("ephemeral-user-42")));
    assert_eq!(subject.get("claims"), Some(&claims));

    let public_tenant = tenant_id.public().as_uuid().to_string();
    let internal_tenant = tenant_id.internal().as_uuid().to_string();
    assert_eq!(
        subject.get("tenant_id").and_then(JsonValue::as_str),
        Some(public_tenant.as_str())
    );
    assert_ne!(
        subject.get("tenant_id").and_then(JsonValue::as_str),
        Some(internal_tenant.as_str())
    );

    let public_authority = authority_id.public().as_uuid().to_string();
    let internal_authority = authority_id.internal().as_uuid().to_string();
    assert_eq!(
        subject.get("authority_id").and_then(JsonValue::as_str),
        Some(public_authority.as_str())
    );
    assert_ne!(
        subject.get("authority_id").and_then(JsonValue::as_str),
        Some(internal_authority.as_str())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_result_is_recorded_as_script_failure() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-malformed").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-d" }),
    )
    .await;

    let creator = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(template_id, canonical_json(&json!({ "arg": "x" })), creator)
        .await
        .unwrap();

    let subject = ephemeral_subject(&store, tenant_id, "ephemeral-malformed", json!({})).await;

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(
        json!({ "context": { "step": 1 }, "output": { "first": true } }),
    ));

    engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "first" })),
            subject.clone(),
        )
        .await
        .unwrap();

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(json!({ "output": "missing-context" })));

    let error = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "second" })),
            subject,
        )
        .await
        .unwrap_err();

    match error {
        WorkflowError::StepExecutionFailed { .. } => {}
        other => panic!("expected StepExecutionFailed, got {other:?}"),
    }

    let latest = store
        .get_latest_revision_typed::<WorkflowInstance>(instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_instance_status(&latest), InstanceStatus::Failed);

    let rows = load_step_records(&store, instance_id).await;
    assert_eq!(rows.len(), 2);

    let failed_row = rows
        .iter()
        .find(|row| step_seq(row) == 2)
        .expect("step 2 record");
    assert_eq!(outcome(failed_row), 1);
    assert!(failed_row.content_attrs.contains_key("error"));
    assert!(!failed_row.content_attrs.contains_key("output"));
}

#[tokio::test(flavor = "current_thread")]
async fn transport_failure_creates_no_step_record_or_instance_revision() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-transport").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-e" }),
    )
    .await;

    let creator = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(template_id, canonical_json(&json!({ "arg": 3 })), creator)
        .await
        .unwrap();

    let subject = ephemeral_subject(&store, tenant_id, "ephemeral-transport", json!({})).await;

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Err(StepExecutionError::Transport(
        "dial timeout".to_string(),
    )));

    let error = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "x" })),
            subject,
        )
        .await
        .unwrap_err();

    match error {
        WorkflowError::ExecutorUnreachable { .. } => {}
        other => panic!("expected ExecutorUnreachable, got {other:?}"),
    }
    assert!(error.is_retryable());

    let rows = load_step_records(&store, instance_id).await;
    assert!(rows.is_empty());

    let latest = store
        .get_latest_revision_typed::<WorkflowInstance>(instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.revision_seq, 0);
    assert_eq!(read_instance_status(&latest), InstanceStatus::Pending);
}

#[tokio::test(flavor = "current_thread")]
async fn done_true_transitions_instance_to_completed() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-done").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-f" }),
    )
    .await;

    let creator = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(template_id, canonical_json(&json!({ "arg": 5 })), creator)
        .await
        .unwrap();

    let subject = ephemeral_subject(&store, tenant_id, "ephemeral-done", json!({})).await;

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(json!({
        "context": { "step": 1 },
        "output": { "ok": true },
        "done": true
    })));

    let result = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "go" })),
            subject,
        )
        .await
        .unwrap();

    assert_eq!(result.status, InstanceStatus::Completed);
    assert!(result.is_terminal());

    let latest = store
        .get_latest_revision_typed::<WorkflowInstance>(instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_instance_status(&latest), InstanceStatus::Completed);
}

#[tokio::test(flavor = "current_thread")]
async fn complete_and_cancel_handle_pending_running_and_terminal_states() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-terminal").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-g" }),
    )
    .await;

    let principal = principal_subject(tenant_id);

    let pending_complete = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "pending-complete" })),
            principal.clone(),
        )
        .await
        .unwrap();
    engine
        .complete(pending_complete, principal.clone())
        .await
        .unwrap();

    let completed_latest = store
        .get_latest_revision_typed::<WorkflowInstance>(pending_complete)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read_instance_status(&completed_latest),
        InstanceStatus::Completed
    );

    let execute_on_completed = engine
        .execute_step(
            pending_complete,
            canonical_json(&json!({ "input": "after-complete" })),
            principal.clone(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        execute_on_completed,
        WorkflowError::InstanceTerminal {
            status: InstanceStatus::Completed,
            ..
        }
    ));

    let pending_cancel = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "pending-cancel" })),
            principal.clone(),
        )
        .await
        .unwrap();
    engine
        .cancel(pending_cancel, principal.clone())
        .await
        .unwrap();

    let cancelled_latest = store
        .get_latest_revision_typed::<WorkflowInstance>(pending_cancel)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read_instance_status(&cancelled_latest),
        InstanceStatus::Cancelled
    );

    let execute_on_cancelled = engine
        .execute_step(
            pending_cancel,
            canonical_json(&json!({ "input": "after-cancel" })),
            principal.clone(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        execute_on_cancelled,
        WorkflowError::InstanceTerminal {
            status: InstanceStatus::Cancelled,
            ..
        }
    ));

    let running_complete = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "running-complete" })),
            principal.clone(),
        )
        .await
        .unwrap();

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(
        json!({ "context": { "running": true }, "output": { "ok": true } }),
    ));

    engine
        .execute_step(
            running_complete,
            canonical_json(&json!({ "input": "run" })),
            principal.clone(),
        )
        .await
        .unwrap();

    engine
        .complete(running_complete, principal.clone())
        .await
        .unwrap();

    let complete_again = engine
        .complete(running_complete, principal.clone())
        .await
        .unwrap_err();
    assert!(matches!(
        complete_again,
        WorkflowError::InstanceTerminal {
            status: InstanceStatus::Completed,
            ..
        }
    ));

    let running_cancel = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "running-cancel" })),
            principal,
        )
        .await
        .unwrap();

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(
        json!({ "context": { "running": true }, "output": { "ok": true } }),
    ));

    engine
        .execute_step(
            running_cancel,
            canonical_json(&json!({ "input": "run" })),
            principal_subject(tenant_id),
        )
        .await
        .unwrap();

    engine
        .cancel(running_cancel, principal_subject(tenant_id))
        .await
        .unwrap();

    let cancel_again = engine
        .cancel(running_cancel, principal_subject(tenant_id))
        .await
        .unwrap_err();
    assert!(matches!(
        cancel_again,
        WorkflowError::InstanceTerminal {
            status: InstanceStatus::Cancelled,
            ..
        }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn execute_step_on_failed_instance_returns_instance_terminal() {
    let store = MockStore::new();
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);

    let tenant_id = seed_tenant(&store, "tenant-failed").await;
    let template_id = seed_template(
        &store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-h" }),
    )
    .await;

    let principal = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "running" })),
            principal.clone(),
        )
        .await
        .unwrap();

    let subject = ephemeral_subject(&store, tenant_id, "ephemeral-fail", json!({})).await;

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(
        json!({ "context": { "running": true }, "output": { "ok": true } }),
    ));

    engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "step-1" })),
            subject.clone(),
        )
        .await
        .unwrap();

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Err(StepExecutionError::ScriptError(
        "step failed".to_string(),
    )));

    let failed = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "step-2" })),
            subject.clone(),
        )
        .await
        .unwrap_err();
    assert!(matches!(failed, WorkflowError::StepExecutionFailed { .. }));

    let execute_again = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "step-3" })),
            subject,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        execute_again,
        WorkflowError::InstanceTerminal {
            status: InstanceStatus::Failed,
            ..
        }
    ));
}
