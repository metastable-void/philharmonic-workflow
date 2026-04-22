#![allow(dead_code)]

use philharmonic_policy::{MintingAuthority, Tenant, TenantStatus};
use philharmonic_store::{
    ContentStore, ContentStoreExt, EntityRefValue, EntityRow, EntityStore, EntityStoreExt,
    IdentityStore, IdentityStoreExt, RevisionInput, RevisionRef, RevisionRow, StoreError, StoreExt,
};
use philharmonic_types::{
    CanonicalJson, ContentHash, ContentValue, Entity, EntityId, Identity, JsonValue, ScalarValue,
    Sha256, UnixMillis, Uuid,
};
use philharmonic_workflow::{
    ConfigLowerer, ConfigLoweringError, InstanceStatus, StepExecutionError, StepExecutor,
    StepRecord, SubjectContext, SubjectKind, WorkflowTemplate,
};

use async_trait::async_trait;

use serde_json::json;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub enum StoreCall {
    MintIdentity {
        internal: Uuid,
        public: Uuid,
    },
    CreateEntity {
        entity_id: Uuid,
        kind: Uuid,
    },
    AppendRevision {
        entity_id: Uuid,
        kind: Uuid,
        revision_seq: u64,
    },
    PutContent {
        hash: Sha256,
    },
}

#[derive(Default)]
struct State {
    content: HashMap<Sha256, ContentValue>,
    identities_by_internal: HashMap<Uuid, Uuid>,
    identities_by_public: HashMap<Uuid, Uuid>,
    entities: HashMap<Uuid, EntityRow>,
    revisions: HashMap<(Uuid, u64), RevisionRow>,
    calls: Vec<StoreCall>,
    next_timestamp: i64,
    next_identity_seed: u64,
}

impl State {
    fn next_unix_millis(&mut self) -> UnixMillis {
        self.next_timestamp += 1;
        UnixMillis(self.next_timestamp)
    }

    fn next_identity(&mut self) -> Identity {
        self.next_identity_seed += 1;
        fixed_identity(self.next_identity_seed)
    }
}

#[derive(Clone, Default)]
pub struct MockStore {
    state: Arc<Mutex<State>>,
}

impl MockStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn calls(&self) -> Vec<StoreCall> {
        self.state.lock().expect("lock state").calls.clone()
    }

    pub fn clear_calls(&self) {
        self.state.lock().expect("lock state").calls.clear();
    }

    pub fn revision_count_for_entity(&self, entity_id: Uuid) -> usize {
        self.state
            .lock()
            .expect("lock state")
            .revisions
            .keys()
            .filter(|(id, _)| *id == entity_id)
            .count()
    }
}

#[async_trait]
impl IdentityStore for MockStore {
    async fn mint(&self) -> Result<Identity, StoreError> {
        let mut state = self.state.lock().expect("lock state");
        let identity = state.next_identity();
        state
            .identities_by_internal
            .insert(identity.internal, identity.public);
        state
            .identities_by_public
            .insert(identity.public, identity.internal);
        state.calls.push(StoreCall::MintIdentity {
            internal: identity.internal,
            public: identity.public,
        });
        Ok(identity)
    }

    async fn resolve_public(&self, public: Uuid) -> Result<Option<Identity>, StoreError> {
        let state = self.state.lock().expect("lock state");
        let Some(internal) = state.identities_by_public.get(&public).copied() else {
            return Ok(None);
        };
        Ok(Some(Identity { internal, public }))
    }

    async fn resolve_internal(&self, internal: Uuid) -> Result<Option<Identity>, StoreError> {
        let state = self.state.lock().expect("lock state");
        let Some(public) = state.identities_by_internal.get(&internal).copied() else {
            return Ok(None);
        };
        Ok(Some(Identity { internal, public }))
    }
}

#[async_trait]
impl ContentStore for MockStore {
    async fn put(&self, value: &ContentValue) -> Result<(), StoreError> {
        let mut state = self.state.lock().expect("lock state");
        state.content.insert(value.digest(), value.clone());
        state.calls.push(StoreCall::PutContent {
            hash: value.digest(),
        });
        Ok(())
    }

    async fn get(&self, hash: Sha256) -> Result<Option<ContentValue>, StoreError> {
        let state = self.state.lock().expect("lock state");
        Ok(state.content.get(&hash).cloned())
    }

    async fn exists(&self, hash: Sha256) -> Result<bool, StoreError> {
        let state = self.state.lock().expect("lock state");
        Ok(state.content.contains_key(&hash))
    }
}

#[async_trait]
impl EntityStore for MockStore {
    async fn create_entity(&self, identity: Identity, kind: Uuid) -> Result<(), StoreError> {
        let mut state = self.state.lock().expect("lock state");
        if state.entities.contains_key(&identity.internal) {
            return Err(StoreError::IdentityCollision {
                uuid: identity.internal,
            });
        }

        let created_at = state.next_unix_millis();
        state.entities.insert(
            identity.internal,
            EntityRow {
                identity,
                kind,
                created_at,
            },
        );
        state.calls.push(StoreCall::CreateEntity {
            entity_id: identity.internal,
            kind,
        });
        Ok(())
    }

    async fn get_entity(&self, entity_id: Uuid) -> Result<Option<EntityRow>, StoreError> {
        let state = self.state.lock().expect("lock state");
        Ok(state.entities.get(&entity_id).cloned())
    }

    async fn append_revision(
        &self,
        entity_id: Uuid,
        revision_seq: u64,
        input: &RevisionInput,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().expect("lock state");

        let Some(kind) = state.entities.get(&entity_id).map(|row| row.kind) else {
            return Err(StoreError::EntityNotFound { entity_id });
        };

        if state.revisions.contains_key(&(entity_id, revision_seq)) {
            return Err(StoreError::RevisionConflict {
                entity_id,
                revision_seq,
            });
        }

        let created_at = state.next_unix_millis();
        state.revisions.insert(
            (entity_id, revision_seq),
            RevisionRow {
                entity_id,
                revision_seq,
                created_at,
                content_attrs: input.content_attrs.clone(),
                entity_attrs: input.entity_attrs.clone(),
                scalar_attrs: input.scalar_attrs.clone(),
            },
        );
        state.calls.push(StoreCall::AppendRevision {
            entity_id,
            kind,
            revision_seq,
        });
        Ok(())
    }

    async fn get_revision(
        &self,
        entity_id: Uuid,
        revision_seq: u64,
    ) -> Result<Option<RevisionRow>, StoreError> {
        let state = self.state.lock().expect("lock state");
        Ok(state.revisions.get(&(entity_id, revision_seq)).cloned())
    }

    async fn get_latest_revision(
        &self,
        entity_id: Uuid,
    ) -> Result<Option<RevisionRow>, StoreError> {
        let state = self.state.lock().expect("lock state");
        let latest = state
            .revisions
            .values()
            .filter(|row| row.entity_id == entity_id)
            .max_by_key(|row| row.revision_seq)
            .cloned();
        Ok(latest)
    }

    async fn list_revisions_referencing(
        &self,
        target_entity_id: Uuid,
        attribute_name: &str,
    ) -> Result<Vec<RevisionRef>, StoreError> {
        let state = self.state.lock().expect("lock state");
        let mut refs = state
            .revisions
            .values()
            .filter_map(|row| {
                let reference = row.entity_attrs.get(attribute_name)?;
                if reference.target_entity_id != target_entity_id {
                    return None;
                }
                Some(RevisionRef::new(row.entity_id, row.revision_seq))
            })
            .collect::<Vec<_>>();
        refs.sort_by_key(|reference| (reference.entity_id.as_u128(), reference.revision_seq));
        Ok(refs)
    }

    async fn find_by_scalar(
        &self,
        kind: Uuid,
        attribute_name: &str,
        value: &ScalarValue,
    ) -> Result<Vec<EntityRow>, StoreError> {
        let state = self.state.lock().expect("lock state");
        let mut rows = Vec::new();

        for entity in state.entities.values() {
            if entity.kind != kind {
                continue;
            }

            let latest = state
                .revisions
                .values()
                .filter(|row| row.entity_id == entity.identity.internal)
                .max_by_key(|row| row.revision_seq);
            let Some(latest) = latest else {
                continue;
            };

            if latest.scalar_attrs.get(attribute_name) == Some(value) {
                rows.push(entity.clone());
            }
        }

        rows.sort_by_key(|row| row.identity.internal.as_u128());
        Ok(rows)
    }
}

#[derive(Clone, Debug)]
pub struct ExecutorCall {
    pub script: String,
    pub arg: JsonValue,
    pub config: JsonValue,
}

#[derive(Clone, Default)]
pub struct MockExecutor {
    state: Arc<Mutex<ExecutorState>>,
}

#[derive(Default)]
struct ExecutorState {
    responses: VecDeque<Result<JsonValue, StepExecutionError>>,
    calls: Vec<ExecutorCall>,
}

impl MockExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_response(&self, response: Result<JsonValue, StepExecutionError>) {
        self.state
            .lock()
            .expect("lock executor state")
            .responses
            .push_back(response);
    }

    pub fn calls(&self) -> Vec<ExecutorCall> {
        self.state
            .lock()
            .expect("lock executor state")
            .calls
            .clone()
    }
}

#[async_trait]
impl StepExecutor for MockExecutor {
    async fn execute(
        &self,
        script: &str,
        arg: &JsonValue,
        config: &JsonValue,
    ) -> Result<JsonValue, StepExecutionError> {
        let mut state = self.state.lock().expect("lock executor state");
        state.calls.push(ExecutorCall {
            script: script.to_string(),
            arg: arg.clone(),
            config: config.clone(),
        });
        state.responses.pop_front().unwrap_or_else(|| {
            Err(StepExecutionError::Transport(
                "mock executor response queue is empty".to_string(),
            ))
        })
    }
}

#[derive(Clone, Debug)]
pub struct LowererCall {
    pub abstract_config: JsonValue,
    pub instance_id: Uuid,
    pub step_seq: u64,
    pub subject: SubjectContext,
}

#[derive(Clone, Default)]
pub struct MockLowerer {
    state: Arc<Mutex<LowererState>>,
}

#[derive(Default)]
struct LowererState {
    responses: VecDeque<Result<JsonValue, ConfigLoweringError>>,
    calls: Vec<LowererCall>,
}

impl MockLowerer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_response(&self, response: Result<JsonValue, ConfigLoweringError>) {
        self.state
            .lock()
            .expect("lock lowerer state")
            .responses
            .push_back(response);
    }

    pub fn calls(&self) -> Vec<LowererCall> {
        self.state.lock().expect("lock lowerer state").calls.clone()
    }
}

#[async_trait]
impl ConfigLowerer for MockLowerer {
    async fn lower(
        &self,
        abstract_config: &JsonValue,
        instance_id: EntityId<philharmonic_workflow::WorkflowInstance>,
        step_seq: u64,
        subject: &SubjectContext,
    ) -> Result<JsonValue, ConfigLoweringError> {
        let mut state = self.state.lock().expect("lock lowerer state");
        state.calls.push(LowererCall {
            abstract_config: abstract_config.clone(),
            instance_id: instance_id.internal().as_uuid(),
            step_seq,
            subject: subject.clone(),
        });
        state.responses.pop_front().unwrap_or_else(|| {
            Err(ConfigLoweringError::Backend(
                "mock lowerer response queue is empty".to_string(),
            ))
        })
    }
}

pub fn canonical_json(value: &JsonValue) -> CanonicalJson {
    CanonicalJson::from_value(value).expect("canonical JSON")
}

pub async fn seed_tenant(store: &MockStore, display_name: &str) -> EntityId<Tenant> {
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

pub async fn seed_template(
    store: &MockStore,
    tenant_id: EntityId<Tenant>,
    script: &str,
    config: JsonValue,
) -> EntityId<WorkflowTemplate> {
    let template_id = store
        .create_entity_minting::<WorkflowTemplate>()
        .await
        .unwrap();

    let script_value = ContentValue::new(script.as_bytes().to_vec());
    let script_hash = script_value.digest();
    store.put(&script_value).await.unwrap();

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

pub fn principal_subject(tenant_id: EntityId<Tenant>) -> SubjectContext {
    SubjectContext {
        kind: SubjectKind::Principal,
        id: "principal-subject".to_string(),
        tenant_id,
        authority_id: None,
        claims: json!({}),
    }
}

pub async fn ephemeral_subject(
    store: &MockStore,
    tenant_id: EntityId<Tenant>,
    id: &str,
    claims: JsonValue,
) -> SubjectContext {
    let authority_id = store.mint_typed::<MintingAuthority>().await.unwrap();
    SubjectContext {
        kind: SubjectKind::Ephemeral,
        id: id.to_string(),
        tenant_id,
        authority_id: Some(authority_id),
        claims,
    }
}

pub async fn load_canonical_json(store: &MockStore, hash: Sha256) -> CanonicalJson {
    let typed_hash = ContentHash::<CanonicalJson>::from_digest_unchecked(hash);
    store
        .get_typed::<CanonicalJson>(typed_hash)
        .await
        .unwrap()
        .unwrap()
}

pub async fn load_step_records(
    store: &MockStore,
    instance_id: EntityId<philharmonic_workflow::WorkflowInstance>,
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

        let row = store
            .get_revision(reference.entity_id, reference.revision_seq)
            .await
            .unwrap()
            .unwrap();
        rows.push(row);
    }

    rows.sort_by_key(|row| (row.entity_id.as_u128(), row.revision_seq));
    rows
}

pub fn read_instance_status(revision: &RevisionRow) -> InstanceStatus {
    let value = match revision.scalar_attrs.get("status").expect("status slot") {
        ScalarValue::I64(value) => *value,
        ScalarValue::Bool(_) => panic!("status must be i64"),
    };
    InstanceStatus::try_from_i64(value).expect("valid status")
}

fn fixed_identity(seed: u64) -> Identity {
    let internal =
        Uuid::parse_str(&format!("00000000-0000-7000-8000-{seed:012x}")).expect("valid UUIDv7");
    let public =
        Uuid::parse_str(&format!("00000000-0000-4000-8000-{seed:012x}")).expect("valid UUIDv4");
    Identity { internal, public }
}
