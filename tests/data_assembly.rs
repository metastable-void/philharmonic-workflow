mod common;

use common::{
    MockExecutor, MockLowerer, MockStore, canonical_json, principal_subject, seed_tenant,
};

use philharmonic_policy::{
    CorpusItem, EmbeddingDataset, EmbeddingDatasetStatus, SourceItem, Tenant, encode_corpus,
    encode_source_items,
};
use philharmonic_store::{
    ContentStore, ContentStoreExt, EntityRefValue, EntityStoreExt, RevisionInput, StoreExt,
};
use philharmonic_types::{CanonicalJson, ContentValue, EntityId, JsonValue, ScalarValue, Uuid};
use philharmonic_workflow::{WorkflowEngine, WorkflowError, WorkflowTemplate};
use serde_json::json;

fn new_engine(
    store: &MockStore,
    executor: &MockExecutor,
    lowerer: &MockLowerer,
) -> WorkflowEngine<MockStore, MockExecutor, MockLowerer> {
    WorkflowEngine::new(store.clone(), executor.clone(), lowerer.clone())
}

async fn put_json(store: &MockStore, value: &JsonValue) -> philharmonic_types::Sha256 {
    store
        .put_typed(&CanonicalJson::from_value(value).expect("canonical JSON"))
        .await
        .unwrap()
        .as_digest()
}

async fn put_bytes(store: &MockStore, bytes: &[u8]) -> philharmonic_types::Sha256 {
    let value = ContentValue::new(bytes.to_vec());
    let hash = value.digest();
    store.put(&value).await.unwrap();
    hash
}

async fn seed_template_with_data_config(
    store: &MockStore,
    tenant_id: EntityId<Tenant>,
    data_config: Option<JsonValue>,
) -> EntityId<WorkflowTemplate> {
    let template_id = store
        .create_entity_minting::<WorkflowTemplate>()
        .await
        .unwrap();
    let script_hash = put_bytes(store, b"export default async function main() {}").await;
    let config_hash = put_json(store, &json!({})).await;

    let mut revision = RevisionInput::new()
        .with_content("script", script_hash)
        .with_content("config", config_hash)
        .with_entity(
            "tenant",
            EntityRefValue::pinned(tenant_id.internal().as_uuid(), 0),
        )
        .with_scalar("is_retired", ScalarValue::Bool(false));
    if let Some(data_config) = data_config {
        let data_config_hash = put_json(store, &data_config).await;
        revision = revision.with_content("data_config", data_config_hash);
    }

    store
        .append_revision_typed::<WorkflowTemplate>(template_id, 0, &revision)
        .await
        .unwrap();
    template_id
}

async fn seed_dataset(
    store: &MockStore,
    tenant_id: EntityId<Tenant>,
    is_retired: bool,
    corpus: Option<Vec<CorpusItem>>,
) -> EntityId<EmbeddingDataset> {
    let dataset_id = store
        .create_entity_minting::<EmbeddingDataset>()
        .await
        .unwrap();
    let display_name_hash = put_json(store, &json!("Knowledge base")).await;
    let source_items_hash = put_bytes(
        store,
        &encode_source_items(&[SourceItem {
            id: "doc-1".to_string(),
            text: "Example".to_string(),
            payload: Some(json!({ "title": "Example" })),
        }])
        .unwrap(),
    )
    .await;
    let embed_endpoint_hash = put_json(store, &json!(Uuid::new_v4().to_string())).await;

    let mut revision = RevisionInput::new()
        .with_content("display_name", display_name_hash)
        .with_content("source_items", source_items_hash)
        .with_content("embed_endpoint_id", embed_endpoint_hash)
        .with_entity(
            "tenant",
            EntityRefValue::pinned(tenant_id.internal().as_uuid(), 0),
        )
        .with_scalar(
            "status",
            ScalarValue::I64(EmbeddingDatasetStatus::Ready.as_i64()),
        )
        .with_scalar("is_retired", ScalarValue::Bool(is_retired))
        .with_scalar("item_count", ScalarValue::I64(1));
    if let Some(corpus) = corpus {
        let corpus_hash = put_bytes(store, &encode_corpus(&corpus).unwrap()).await;
        revision = revision.with_content("corpus", corpus_hash);
    }

    store
        .append_revision_typed::<EmbeddingDataset>(dataset_id, 0, &revision)
        .await
        .unwrap();
    dataset_id
}

async fn execute_and_capture_arg(
    store: &MockStore,
    template_id: EntityId<WorkflowTemplate>,
    tenant_id: EntityId<Tenant>,
) -> JsonValue {
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(store, &executor, &lowerer);
    lowerer.push_response(Ok(json!({})));
    executor.push_response(Ok(json!({ "context": {}, "output": { "ok": true } })));
    let instance_id = engine
        .create_instance(
            template_id,
            canonical_json(&json!({})),
            principal_subject(tenant_id),
        )
        .await
        .unwrap();
    engine
        .execute_step(
            instance_id,
            canonical_json(&json!({})),
            principal_subject(tenant_id),
        )
        .await
        .unwrap();
    executor.calls()[0].arg.clone()
}

#[tokio::test(flavor = "current_thread")]
async fn execute_step_includes_ready_embedding_dataset_corpus() {
    let store = MockStore::new();
    let tenant_id = seed_tenant(&store, "tenant-data").await;
    let dataset_id = seed_dataset(
        &store,
        tenant_id,
        false,
        Some(vec![CorpusItem {
            id: "doc-1".to_string(),
            vector: vec![1.0, 2.0],
            payload: Some(json!({ "title": "Example" })),
        }]),
    )
    .await;
    let template_id = seed_template_with_data_config(
        &store,
        tenant_id,
        Some(json!({ "embed_datasets": { "kb": dataset_id.public().as_uuid() } })),
    )
    .await;

    let arg = execute_and_capture_arg(&store, template_id, tenant_id).await;

    assert_eq!(
        arg["data"]["embed_datasets"]["kb"][0]["id"],
        JsonValue::String("doc-1".to_string())
    );
    assert_eq!(
        arg["data"]["embed_datasets"]["kb"][0]["vector"],
        json!([1.0, 2.0])
    );
    assert_eq!(
        arg["data"]["embed_datasets"]["kb"][0]["payload"],
        json!({ "title": "Example" })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn execute_step_omits_retired_dataset() {
    let store = MockStore::new();
    let tenant_id = seed_tenant(&store, "tenant-retired").await;
    let dataset_id = seed_dataset(
        &store,
        tenant_id,
        true,
        Some(vec![CorpusItem {
            id: "doc-1".to_string(),
            vector: vec![1.0],
            payload: None,
        }]),
    )
    .await;
    let template_id = seed_template_with_data_config(
        &store,
        tenant_id,
        Some(json!({ "embed_datasets": { "kb": dataset_id.public().as_uuid() } })),
    )
    .await;

    let arg = execute_and_capture_arg(&store, template_id, tenant_id).await;

    assert!(arg["data"]["embed_datasets"].get("kb").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn execute_step_omits_dataset_without_corpus() {
    let store = MockStore::new();
    let tenant_id = seed_tenant(&store, "tenant-no-corpus").await;
    let dataset_id = seed_dataset(&store, tenant_id, false, None).await;
    let template_id = seed_template_with_data_config(
        &store,
        tenant_id,
        Some(json!({ "embed_datasets": { "kb": dataset_id.public().as_uuid() } })),
    )
    .await;

    let arg = execute_and_capture_arg(&store, template_id, tenant_id).await;

    assert!(arg["data"]["embed_datasets"].get("kb").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn execute_step_rejects_cross_tenant_dataset_binding() {
    let store = MockStore::new();
    let tenant_id = seed_tenant(&store, "tenant-a").await;
    let other_tenant_id = seed_tenant(&store, "tenant-b").await;
    let dataset_id = seed_dataset(
        &store,
        other_tenant_id,
        false,
        Some(vec![CorpusItem {
            id: "doc-1".to_string(),
            vector: vec![1.0],
            payload: None,
        }]),
    )
    .await;
    let template_id = seed_template_with_data_config(
        &store,
        tenant_id,
        Some(json!({ "embed_datasets": { "kb": dataset_id.public().as_uuid() } })),
    )
    .await;
    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = new_engine(&store, &executor, &lowerer);
    let instance_id = engine
        .create_instance(
            template_id,
            canonical_json(&json!({})),
            principal_subject(tenant_id),
        )
        .await
        .unwrap();

    let error = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({})),
            principal_subject(tenant_id),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, WorkflowError::DataConfigInvalid { .. }));
}

#[tokio::test(flavor = "current_thread")]
async fn execute_step_omits_missing_public_dataset_reference() {
    let store = MockStore::new();
    let tenant_id = seed_tenant(&store, "tenant-missing").await;
    let template_id = seed_template_with_data_config(
        &store,
        tenant_id,
        Some(json!({ "embed_datasets": { "kb": Uuid::new_v4() } })),
    )
    .await;

    let arg = execute_and_capture_arg(&store, template_id, tenant_id).await;

    assert!(arg["data"]["embed_datasets"].get("kb").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn execute_step_without_data_config_includes_empty_data_object() {
    let store = MockStore::new();
    let tenant_id = seed_tenant(&store, "tenant-empty").await;
    let template_id = seed_template_with_data_config(&store, tenant_id, None).await;

    let arg = execute_and_capture_arg(&store, template_id, tenant_id).await;

    assert_eq!(arg["data"], json!({}));
}
