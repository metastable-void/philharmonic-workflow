mod common;

use common::{MockExecutor, MockLowerer, canonical_json, principal_subject};

use philharmonic_policy::{Tenant, TenantStatus};
use philharmonic_store::{
    ContentStore, ContentStoreExt, EntityRefValue, EntityStore, EntityStoreExt, IdentityStore,
    RevisionInput, RevisionRef, RevisionRow, StoreError, StoreExt,
};
use philharmonic_store_sqlx_mysql::{SinglePool, SqlStore, migrate};
use philharmonic_types::{ContentValue, Entity, EntityId, Identity, ScalarValue, Sha256, Uuid};
use philharmonic_workflow::{
    InstanceStatus, StepRecord, WorkflowEngine, WorkflowError, WorkflowInstance, WorkflowTemplate,
};

use async_trait::async_trait;

use serde_json::json;

use sqlx::{MySqlPool, mysql::MySqlPoolOptions};

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use testcontainers_modules::{
    mysql::Mysql,
    testcontainers::{ContainerAsync, ImageExt, core::IntoContainerPort, runners::AsyncRunner},
};

type ContainerHandle = ContainerAsync<Mysql>;

#[derive(Clone)]
struct SharedSqlStore(Arc<SqlStore<SinglePool>>);

impl SharedSqlStore {
    fn new(inner: SqlStore<SinglePool>) -> Self {
        Self(Arc::new(inner))
    }
}

#[async_trait]
impl ContentStore for SharedSqlStore {
    async fn put(&self, value: &ContentValue) -> Result<(), StoreError> {
        self.0.put(value).await
    }

    async fn get(&self, hash: Sha256) -> Result<Option<ContentValue>, StoreError> {
        self.0.get(hash).await
    }

    async fn exists(&self, hash: Sha256) -> Result<bool, StoreError> {
        self.0.exists(hash).await
    }
}

#[async_trait]
impl IdentityStore for SharedSqlStore {
    async fn mint(&self) -> Result<Identity, StoreError> {
        self.0.mint().await
    }

    async fn resolve_public(&self, public: Uuid) -> Result<Option<Identity>, StoreError> {
        self.0.resolve_public(public).await
    }

    async fn resolve_internal(&self, internal: Uuid) -> Result<Option<Identity>, StoreError> {
        self.0.resolve_internal(internal).await
    }
}

#[async_trait]
impl EntityStore for SharedSqlStore {
    async fn create_entity(&self, identity: Identity, kind: Uuid) -> Result<(), StoreError> {
        self.0.create_entity(identity, kind).await
    }

    async fn get_entity(
        &self,
        entity_id: Uuid,
    ) -> Result<Option<philharmonic_store::EntityRow>, StoreError> {
        self.0.get_entity(entity_id).await
    }

    async fn append_revision(
        &self,
        entity_id: Uuid,
        revision_seq: u64,
        input: &RevisionInput,
    ) -> Result<(), StoreError> {
        self.0.append_revision(entity_id, revision_seq, input).await
    }

    async fn get_revision(
        &self,
        entity_id: Uuid,
        revision_seq: u64,
    ) -> Result<Option<RevisionRow>, StoreError> {
        self.0.get_revision(entity_id, revision_seq).await
    }

    async fn get_latest_revision(
        &self,
        entity_id: Uuid,
    ) -> Result<Option<RevisionRow>, StoreError> {
        self.0.get_latest_revision(entity_id).await
    }

    async fn list_revisions_referencing(
        &self,
        target_entity_id: Uuid,
        attribute_name: &str,
    ) -> Result<Vec<RevisionRef>, StoreError> {
        self.0
            .list_revisions_referencing(target_entity_id, attribute_name)
            .await
    }

    async fn find_by_scalar(
        &self,
        kind: Uuid,
        attribute_name: &str,
        value: &ScalarValue,
    ) -> Result<Vec<philharmonic_store::EntityRow>, StoreError> {
        self.0.find_by_scalar(kind, attribute_name, value).await
    }
}

struct TestContext {
    _serial_guard: MutexGuard<'static, ()>,
    _container: ContainerHandle,
    _pool: MySqlPool,
    store: SharedSqlStore,
}

static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn test_mutex() -> &'static Mutex<()> {
    TEST_MUTEX.get_or_init(|| Mutex::new(()))
}

async fn setup() -> TestContext {
    let serial_guard = test_mutex().lock().await;

    let container = Mysql::default()
        .with_startup_timeout(Duration::from_secs(180))
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(3306.tcp()).await.unwrap();

    let database_url = format!("mysql://root@{}:{}/test", host, port);
    let pool = MySqlPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await
        .unwrap();

    migrate(&pool).await.unwrap();

    let store = SharedSqlStore::new(SqlStore::from_pool(pool.clone()));

    TestContext {
        _serial_guard: serial_guard,
        _container: container,
        _pool: pool,
        store,
    }
}

async fn put_content(store: &SharedSqlStore, bytes: &[u8]) -> Sha256 {
    let value = ContentValue::new(bytes.to_vec());
    let hash = value.digest();
    store.put(&value).await.unwrap();
    hash
}

async fn seed_tenant(store: &SharedSqlStore, display_name: &str) -> EntityId<Tenant> {
    let tenant_id = store.create_entity_minting::<Tenant>().await.unwrap();

    let display_name = canonical_json(&json!({ "display_name": display_name }));
    let settings = canonical_json(&json!({ "settings": {} }));
    let display_hash = store.put_typed(&display_name).await.unwrap();
    let settings_hash = store.put_typed(&settings).await.unwrap();

    let revision = RevisionInput::new()
        .with_content("display_name", display_hash.as_digest())
        .with_content("settings", settings_hash.as_digest())
        .with_scalar("status", ScalarValue::I64(TenantStatus::Active.as_i64()));
    store
        .append_revision_typed::<Tenant>(tenant_id, 0, &revision)
        .await
        .unwrap();

    tenant_id
}

async fn seed_template(
    store: &SharedSqlStore,
    tenant_id: EntityId<Tenant>,
    script: &str,
    config: serde_json::Value,
) -> EntityId<WorkflowTemplate> {
    let template_id = store
        .create_entity_minting::<WorkflowTemplate>()
        .await
        .unwrap();

    let script_hash = put_content(store, script.as_bytes()).await;
    let config_canon = canonical_json(&config);
    let config_hash = store.put_typed(&config_canon).await.unwrap();

    let revision = RevisionInput::new()
        .with_content("script", script_hash)
        .with_content("config", config_hash.as_digest())
        .with_entity(
            "tenant",
            EntityRefValue::pinned(tenant_id.internal().as_uuid(), 0),
        )
        .with_scalar("is_retired", ScalarValue::Bool(false));

    store
        .append_revision_typed::<WorkflowTemplate>(template_id, 0, &revision)
        .await
        .unwrap();

    template_id
}

async fn load_step_records(
    store: &SharedSqlStore,
    instance_id: EntityId<WorkflowInstance>,
) -> Vec<RevisionRow> {
    let refs = store
        .list_revisions_referencing(instance_id.internal().as_uuid(), "instance")
        .await
        .unwrap();

    let mut rows = Vec::new();
    for reference in refs {
        let entity = store
            .get_entity(reference.entity_id)
            .await
            .unwrap()
            .unwrap();
        if entity.kind != StepRecord::KIND {
            continue;
        }
        rows.push(
            store
                .get_revision(reference.entity_id, reference.revision_seq)
                .await
                .unwrap()
                .unwrap(),
        );
    }

    rows.sort_by_key(|row| match row.scalar_attrs.get("step_seq").unwrap() {
        ScalarValue::I64(value) => *value,
        ScalarValue::Bool(_) => panic!("step_seq must be i64"),
    });

    rows
}

fn read_status(revision: &RevisionRow) -> InstanceStatus {
    let value = match revision.scalar_attrs.get("status").unwrap() {
        ScalarValue::I64(value) => *value,
        ScalarValue::Bool(_) => panic!("status must be i64"),
    };
    InstanceStatus::try_from_i64(value).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn mysql_end_to_end_create_execute_complete() {
    let ctx = setup().await;

    let tenant_id = seed_tenant(&ctx.store, "tenant-mysql-e2e").await;
    let template_id = seed_template(
        &ctx.store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-mysql-1" }),
    )
    .await;

    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = WorkflowEngine::new(ctx.store.clone(), executor.clone(), lowerer.clone());

    let subject = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "x" })),
            subject.clone(),
        )
        .await
        .unwrap();

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(
        json!({ "context": { "step": 1 }, "output": { "ok": 1 } }),
    ));

    let first = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "first" })),
            subject.clone(),
        )
        .await
        .unwrap();
    assert_eq!(first.status, InstanceStatus::Running);

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(
        json!({ "context": { "step": 2 }, "output": { "ok": 2 }, "done": true }),
    ));

    let second = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "second" })),
            subject,
        )
        .await
        .unwrap();
    assert_eq!(second.status, InstanceStatus::Completed);
    assert!(second.is_terminal());

    let latest = ctx
        .store
        .get_latest_revision_typed::<WorkflowInstance>(instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.revision_seq, 2);
    assert_eq!(read_status(&latest), InstanceStatus::Completed);

    let steps = load_step_records(&ctx.store, instance_id).await;
    assert_eq!(steps.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn mysql_step_records_pin_pre_step_instance_revisions() {
    let ctx = setup().await;

    let tenant_id = seed_tenant(&ctx.store, "tenant-mysql-pinning").await;
    let template_id = seed_template(
        &ctx.store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-mysql-2" }),
    )
    .await;

    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = WorkflowEngine::new(ctx.store.clone(), executor.clone(), lowerer.clone());

    let subject = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "x" })),
            subject.clone(),
        )
        .await
        .unwrap();

    lowerer.push_response(Ok(json!({ "config": "resolved" })));
    executor.push_response(Ok(
        json!({ "context": { "step": 1 }, "output": { "ok": 1 } }),
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
    executor.push_response(Ok(
        json!({ "context": { "step": 2 }, "output": { "ok": 2 } }),
    ));
    engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "second" })),
            subject,
        )
        .await
        .unwrap();

    let steps = load_step_records(&ctx.store, instance_id).await;
    assert_eq!(steps.len(), 2);

    let first_ref = steps[0].entity_attrs.get("instance").unwrap();
    let second_ref = steps[1].entity_attrs.get("instance").unwrap();

    assert_eq!(first_ref.target_revision_seq, Some(0));
    assert_eq!(second_ref.target_revision_seq, Some(1));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires MySQL testcontainer"]
async fn mysql_terminal_instance_rejects_execute_step() {
    let ctx = setup().await;

    let tenant_id = seed_tenant(&ctx.store, "tenant-mysql-terminal").await;
    let template_id = seed_template(
        &ctx.store,
        tenant_id,
        "export default async function main() {}",
        json!({ "endpoint": "cfg-mysql-3" }),
    )
    .await;

    let executor = MockExecutor::new();
    let lowerer = MockLowerer::new();
    let engine = WorkflowEngine::new(ctx.store.clone(), executor, lowerer);

    let subject = principal_subject(tenant_id);
    let instance_id = engine
        .create_instance(
            template_id,
            canonical_json(&json!({ "arg": "x" })),
            subject.clone(),
        )
        .await
        .unwrap();

    engine.complete(instance_id, subject.clone()).await.unwrap();

    let error = engine
        .execute_step(
            instance_id,
            canonical_json(&json!({ "input": "after-complete" })),
            subject,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        WorkflowError::InstanceTerminal {
            status: InstanceStatus::Completed,
            ..
        }
    ));
}
