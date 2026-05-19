use crate::{
    ConfigLowerer, InstanceStatus, StepExecutionError, StepExecutor, StepRecord, SubjectContext,
    WorkflowError, WorkflowInstance, WorkflowTemplate,
};

use philharmonic_policy::{CorpusItem, EmbeddingDataset, decode_corpus};
use philharmonic_store::{
    ContentStore, ContentStoreExt, EntityRefValue, EntityStore, EntityStoreExt, IdentityStore,
    RevisionInput, RevisionRow, StoreExt,
};
use philharmonic_types::{
    CanonicalJson, ContentHash, Entity, EntityId, JsonValue, ScalarValue, Sha256, Uuid,
};

use serde_json::{Map as JsonMap, json};

/// Result of one `execute_step` call.
#[derive(Clone, Debug, PartialEq)]
pub struct StepResult {
    /// Output value returned by the script.
    pub output: JsonValue,
    /// Updated workflow context persisted for the next step.
    pub context: JsonValue,
    /// Instance status after this step.
    pub status: InstanceStatus,
    /// Step sequence number that was executed.
    pub step_seq: u64,
}

impl StepResult {
    /// Whether the instance is terminal after this step.
    pub const fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

#[derive(Clone, Copy, Debug)]
enum StepOutcome {
    Success,
    Failure,
}

impl StepOutcome {
    const fn as_i64(self) -> i64 {
        match self {
            Self::Success => 0,
            Self::Failure => 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct StepRecordWrite {
    instance_id: EntityId<WorkflowInstance>,
    instance_revision_seq: u64,
    step_seq: u64,
    input_hash: Sha256,
    subject_hash: Sha256,
    outcome: StepOutcome,
    output_hash: Option<Sha256>,
    error_hash: Option<Sha256>,
}

/// Workflow orchestration engine.
pub struct WorkflowEngine<S, E, L> {
    store: S,
    executor: E,
    lowerer: L,
}

impl<S, E, L> WorkflowEngine<S, E, L>
where
    S: ContentStore + IdentityStore + EntityStore,
    E: StepExecutor,
    L: ConfigLowerer,
{
    /// Construct an engine from its substrate, executor, and lowerer.
    pub fn new(store: S, executor: E, lowerer: L) -> Self {
        Self {
            store,
            executor,
            lowerer,
        }
    }

    /// Create a workflow instance bound to the current template revision.
    pub async fn create_instance(
        &self,
        template_id: EntityId<WorkflowTemplate>,
        args: CanonicalJson,
        _subject: SubjectContext,
    ) -> Result<EntityId<WorkflowInstance>, WorkflowError> {
        let template_revision = self
            .store
            .get_latest_revision_typed::<WorkflowTemplate>(template_id)
            .await?
            .ok_or(WorkflowError::TemplateNotFound {
                template_id: template_id.internal().as_uuid(),
            })?;

        let template_tenant =
            required_entity_attr(&template_revision, WorkflowTemplate::NAME, "tenant")?;

        let instance_id = self
            .store
            .create_entity_minting::<WorkflowInstance>()
            .await?;

        let empty_context = CanonicalJson::from_value(&JsonValue::Object(JsonMap::new()))?;
        let context_hash = self.store.put_typed(&empty_context).await?;
        let args_hash = self.store.put_typed(&args).await?;

        let revision = RevisionInput::new()
            .with_content("context", context_hash.as_digest())
            .with_content("args", args_hash.as_digest())
            .with_entity(
                "template",
                EntityRefValue::pinned(
                    template_id.internal().as_uuid(),
                    template_revision.revision_seq,
                ),
            )
            .with_entity("tenant", template_tenant)
            .with_scalar("status", ScalarValue::I64(InstanceStatus::Pending.as_i64()));

        self.store
            .append_revision_typed::<WorkflowInstance>(instance_id, 0, &revision)
            .await?;

        Ok(instance_id)
    }

    /// Execute one workflow step.
    pub async fn execute_step(
        &self,
        instance_id: EntityId<WorkflowInstance>,
        input: CanonicalJson,
        subject: SubjectContext,
    ) -> Result<StepResult, WorkflowError> {
        let latest = self.load_latest_instance_revision(instance_id).await?;
        let status = read_status(&latest)?;

        if status.is_terminal() {
            return Err(WorkflowError::InstanceTerminal {
                instance_id: instance_id.internal().as_uuid(),
                status,
            });
        }

        let step_seq = latest
            .revision_seq
            .checked_add(1)
            .ok_or(WorkflowError::IntegerOverflow { field: "step_seq" })?;

        let template_ref = required_entity_attr(&latest, WorkflowInstance::NAME, "template")?;
        let template_seq =
            template_ref
                .target_revision_seq
                .ok_or(WorkflowError::MissingPinnedReference {
                    entity_name: WorkflowInstance::NAME,
                    attribute: "template",
                })?;

        let instance_tenant_ref = required_entity_attr(&latest, WorkflowInstance::NAME, "tenant")?;

        let template_entity = self
            .store
            .get_entity(template_ref.target_entity_id)
            .await?
            .ok_or(WorkflowError::TemplateNotFound {
                template_id: template_ref.target_entity_id,
            })?;

        if template_entity.kind != WorkflowTemplate::KIND {
            return Err(WorkflowError::EntityKindMismatch {
                entity_name: WorkflowTemplate::NAME,
                expected: WorkflowTemplate::KIND,
                actual: template_entity.kind,
            });
        }

        let template_revision = self
            .store
            .get_revision(template_ref.target_entity_id, template_seq)
            .await?
            .ok_or(WorkflowError::TemplateRevisionNotFound {
                template_id: template_ref.target_entity_id,
                revision_seq: template_seq,
            })?;

        let template_tenant_ref =
            required_entity_attr(&template_revision, WorkflowTemplate::NAME, "tenant")?;
        if template_tenant_ref.target_entity_id != instance_tenant_ref.target_entity_id {
            return Err(WorkflowError::TemplateTenantMismatch {
                template_id: template_ref.target_entity_id,
                instance_id: instance_id.internal().as_uuid(),
                template_tenant: template_tenant_ref.target_entity_id,
                instance_tenant: instance_tenant_ref.target_entity_id,
            });
        }

        let script_hash =
            required_content_attr(&template_revision, WorkflowTemplate::NAME, "script")?;
        let abstract_config_hash =
            required_content_attr(&template_revision, WorkflowTemplate::NAME, "config")?;

        let script_content =
            self.store
                .get(script_hash)
                .await?
                .ok_or(WorkflowError::MissingContentBlob {
                    entity_name: WorkflowTemplate::NAME,
                    attribute: "script",
                    hash: script_hash,
                })?;
        let script = std::str::from_utf8(script_content.bytes())?;

        let abstract_config = self
            .load_json_content(abstract_config_hash, WorkflowTemplate::NAME, "config")
            .await?;

        let context_hash = required_content_attr(&latest, WorkflowInstance::NAME, "context")?;
        let args_hash = required_content_attr(&latest, WorkflowInstance::NAME, "args")?;
        let context = self
            .load_json_content(context_hash, WorkflowInstance::NAME, "context")
            .await?;
        let args = self
            .load_json_content(args_hash, WorkflowInstance::NAME, "args")
            .await?;

        let input_hash = self.store.put_typed(&input).await?;

        let subject_record = subject.to_step_record_subject();
        let subject_canon = CanonicalJson::from_serializable(&subject_record)?;
        let subject_hash = self.store.put_typed(&subject_canon).await?;

        let input_json = canonical_to_json(&input)?;
        let subject_json = subject.to_script_value().map_err(|error| {
            WorkflowError::json(format!("failed to serialize subject: {error}"))
        })?;
        let data = build_script_data(&self.store, &template_revision, &instance_tenant_ref).await?;

        let script_arg = json!({
            "context": context,
            "args": args,
            "input": input_json,
            "subject": subject_json,
            "data": data,
            "instance": {
                "id": instance_id.public().as_uuid().to_string(),
                "step": step_seq,
            },
        });

        let concrete_config = self
            .lowerer
            .lower(&abstract_config, instance_id, step_seq, &subject)
            .await?;

        match self
            .executor
            .execute(script, &script_arg, &concrete_config)
            .await
        {
            Err(StepExecutionError::Transport(detail)) => {
                Err(WorkflowError::ExecutorUnreachable { detail })
            }
            Err(StepExecutionError::ScriptError(detail)) => {
                let write = StepRecordWrite {
                    instance_id,
                    instance_revision_seq: latest.revision_seq,
                    step_seq,
                    input_hash: input_hash.as_digest(),
                    subject_hash: subject_hash.as_digest(),
                    outcome: StepOutcome::Failure,
                    output_hash: None,
                    error_hash: None,
                };
                self.record_failed_step(&latest, write, &detail).await?;
                Err(WorkflowError::StepExecutionFailed { detail })
            }
            Ok(result_json) => {
                let parsed = match parse_executor_result(&result_json) {
                    Ok(parsed) => parsed,
                    Err(detail) => {
                        let write = StepRecordWrite {
                            instance_id,
                            instance_revision_seq: latest.revision_seq,
                            step_seq,
                            input_hash: input_hash.as_digest(),
                            subject_hash: subject_hash.as_digest(),
                            outcome: StepOutcome::Failure,
                            output_hash: None,
                            error_hash: None,
                        };
                        self.record_failed_step(&latest, write, &detail).await?;
                        return Err(WorkflowError::StepExecutionFailed { detail });
                    }
                };

                let next_status = if parsed.done {
                    InstanceStatus::Completed
                } else {
                    InstanceStatus::Running
                };
                ensure_transition(status, next_status, instance_id.internal().as_uuid())?;

                let context_canon = CanonicalJson::from_value(&parsed.context)?;
                let output_canon = CanonicalJson::from_value(&parsed.output)?;

                let next_context_hash = self.store.put_typed(&context_canon).await?;
                let output_hash = self.store.put_typed(&output_canon).await?;

                let write = StepRecordWrite {
                    instance_id,
                    instance_revision_seq: latest.revision_seq,
                    step_seq,
                    input_hash: input_hash.as_digest(),
                    subject_hash: subject_hash.as_digest(),
                    outcome: StepOutcome::Success,
                    output_hash: Some(output_hash.as_digest()),
                    error_hash: None,
                };
                self.write_step_record(write).await?;

                self.append_instance_revision(
                    instance_id,
                    &latest,
                    next_context_hash.as_digest(),
                    next_status,
                )
                .await?;

                Ok(StepResult {
                    output: parsed.output,
                    context: parsed.context,
                    status: next_status,
                    step_seq,
                })
            }
        }
    }

    /// Mark an instance completed.
    pub async fn complete(
        &self,
        instance_id: EntityId<WorkflowInstance>,
        _subject: SubjectContext,
    ) -> Result<(), WorkflowError> {
        self.transition_instance(instance_id, InstanceStatus::Completed)
            .await
    }

    /// Cancel an instance.
    pub async fn cancel(
        &self,
        instance_id: EntityId<WorkflowInstance>,
        _subject: SubjectContext,
    ) -> Result<(), WorkflowError> {
        self.transition_instance(instance_id, InstanceStatus::Cancelled)
            .await
    }

    async fn transition_instance(
        &self,
        instance_id: EntityId<WorkflowInstance>,
        next_status: InstanceStatus,
    ) -> Result<(), WorkflowError> {
        let latest = self.load_latest_instance_revision(instance_id).await?;
        let current = read_status(&latest)?;

        if current.is_terminal() {
            return Err(WorkflowError::InstanceTerminal {
                instance_id: instance_id.internal().as_uuid(),
                status: current,
            });
        }

        ensure_transition(current, next_status, instance_id.internal().as_uuid())?;

        let current_context = required_content_attr(&latest, WorkflowInstance::NAME, "context")?;
        self.append_instance_revision(instance_id, &latest, current_context, next_status)
            .await
    }

    async fn load_latest_instance_revision(
        &self,
        instance_id: EntityId<WorkflowInstance>,
    ) -> Result<RevisionRow, WorkflowError> {
        let Some(entity) = self
            .store
            .get_entity(instance_id.internal().as_uuid())
            .await?
        else {
            return Err(WorkflowError::InstanceNotFound {
                instance_id: instance_id.internal().as_uuid(),
            });
        };

        if entity.kind != WorkflowInstance::KIND {
            return Err(WorkflowError::EntityKindMismatch {
                entity_name: WorkflowInstance::NAME,
                expected: WorkflowInstance::KIND,
                actual: entity.kind,
            });
        }

        self.store
            .get_latest_revision_typed::<WorkflowInstance>(instance_id)
            .await?
            .ok_or(WorkflowError::InstanceRevisionMissing {
                instance_id: instance_id.internal().as_uuid(),
            })
    }

    async fn load_json_content(
        &self,
        hash: Sha256,
        entity_name: &'static str,
        attribute: &'static str,
    ) -> Result<JsonValue, WorkflowError> {
        let typed_hash = ContentHash::<CanonicalJson>::from_digest_unchecked(hash);
        let canonical = self
            .store
            .get_typed::<CanonicalJson>(typed_hash)
            .await?
            .ok_or(WorkflowError::MissingContentBlob {
                entity_name,
                attribute,
                hash,
            })?;
        canonical_to_json(&canonical)
    }

    async fn record_failed_step(
        &self,
        latest: &RevisionRow,
        mut write: StepRecordWrite,
        detail: &str,
    ) -> Result<(), WorkflowError> {
        let current_status = read_status(latest)?;
        let next_status = InstanceStatus::Failed;
        ensure_transition(
            current_status,
            next_status,
            write.instance_id.internal().as_uuid(),
        )?;

        let error_payload = CanonicalJson::from_value(&json!({ "message": detail }))?;
        let error_hash = self.store.put_typed(&error_payload).await?;

        write.error_hash = Some(error_hash.as_digest());
        self.write_step_record(write).await?;

        let current_context = required_content_attr(latest, WorkflowInstance::NAME, "context")?;
        self.append_instance_revision(write.instance_id, latest, current_context, next_status)
            .await
    }

    async fn write_step_record(&self, write: StepRecordWrite) -> Result<(), WorkflowError> {
        let step_record_id = self.store.create_entity_minting::<StepRecord>().await?;

        let step_seq_i64 =
            i64::try_from(write.step_seq).map_err(|error| WorkflowError::IntegerConversion {
                field: "step_seq",
                detail: error.to_string(),
            })?;

        let mut revision = RevisionInput::new()
            .with_content("input", write.input_hash)
            .with_content("subject", write.subject_hash)
            .with_entity(
                "instance",
                EntityRefValue::pinned(
                    write.instance_id.internal().as_uuid(),
                    write.instance_revision_seq,
                ),
            )
            .with_scalar("step_seq", ScalarValue::I64(step_seq_i64))
            .with_scalar("outcome", ScalarValue::I64(write.outcome.as_i64()));

        if let Some(hash) = write.output_hash {
            revision = revision.with_content("output", hash);
        }
        if let Some(hash) = write.error_hash {
            revision = revision.with_content("error", hash);
        }

        self.store
            .append_revision_typed::<StepRecord>(step_record_id, 0, &revision)
            .await?;
        Ok(())
    }

    async fn append_instance_revision(
        &self,
        instance_id: EntityId<WorkflowInstance>,
        latest: &RevisionRow,
        context_hash: Sha256,
        status: InstanceStatus,
    ) -> Result<(), WorkflowError> {
        let args_hash = required_content_attr(latest, WorkflowInstance::NAME, "args")?;
        let template_ref = required_entity_attr(latest, WorkflowInstance::NAME, "template")?;
        let tenant_ref = required_entity_attr(latest, WorkflowInstance::NAME, "tenant")?;

        let next_revision_seq =
            latest
                .revision_seq
                .checked_add(1)
                .ok_or(WorkflowError::IntegerOverflow {
                    field: "instance_revision_seq",
                })?;

        let revision = RevisionInput::new()
            .with_content("context", context_hash)
            .with_content("args", args_hash)
            .with_entity("template", template_ref)
            .with_entity("tenant", tenant_ref)
            .with_scalar("status", ScalarValue::I64(status.as_i64()));

        self.store
            .append_revision_typed::<WorkflowInstance>(instance_id, next_revision_seq, &revision)
            .await?;
        Ok(())
    }
}

fn required_content_attr(
    revision: &RevisionRow,
    entity_name: &'static str,
    attribute: &'static str,
) -> Result<Sha256, WorkflowError> {
    revision
        .content_attrs
        .get(attribute)
        .copied()
        .ok_or(WorkflowError::MissingContentAttribute {
            entity_name,
            attribute,
        })
}

fn optional_content_attr(revision: &RevisionRow, attribute: &'static str) -> Option<Sha256> {
    revision.content_attrs.get(attribute).copied()
}

fn required_entity_attr(
    revision: &RevisionRow,
    entity_name: &'static str,
    attribute: &'static str,
) -> Result<EntityRefValue, WorkflowError> {
    revision
        .entity_attrs
        .get(attribute)
        .copied()
        .ok_or(WorkflowError::MissingEntityAttribute {
            entity_name,
            attribute,
        })
}

async fn build_script_data<S>(
    store: &S,
    template_revision: &RevisionRow,
    template_tenant_ref: &EntityRefValue,
) -> Result<JsonValue, WorkflowError>
where
    S: ContentStore + IdentityStore + EntityStore,
{
    let Some(data_config_hash) = optional_content_attr(template_revision, "data_config") else {
        return Ok(JsonValue::Object(JsonMap::new()));
    };
    let data_config = load_json_content(
        store,
        data_config_hash,
        WorkflowTemplate::NAME,
        "data_config",
    )
    .await?;
    let data_config_object =
        data_config
            .as_object()
            .ok_or_else(|| WorkflowError::DataConfigInvalid {
                detail: "data_config must be a JSON object".to_string(),
            })?;

    let mut embed_datasets = JsonMap::new();
    if let Some(value) = data_config_object.get("embed_datasets") {
        let bindings = value
            .as_object()
            .ok_or_else(|| WorkflowError::DataConfigInvalid {
                detail: "data_config.embed_datasets must be a JSON object".to_string(),
            })?;
        for (name, public_value) in bindings {
            let public_string =
                public_value
                    .as_str()
                    .ok_or_else(|| WorkflowError::DataConfigInvalid {
                        detail: format!("data_config.embed_datasets.{name} must be a UUID string"),
                    })?;
            let public_uuid = public_string.parse::<Uuid>().map_err(|error| {
                WorkflowError::DataConfigInvalid {
                    detail: format!(
                        "data_config.embed_datasets.{name} is not a valid UUID: {error}"
                    ),
                }
            })?;
            let Some(identity) = store.resolve_public(public_uuid).await? else {
                tracing::warn!(
                    dataset_name = %name,
                    public_uuid = %public_uuid,
                    "embedding dataset referenced by data_config was not found"
                );
                continue;
            };
            let dataset_id = identity.typed::<EmbeddingDataset>().map_err(|error| {
                WorkflowError::DataConfigInvalid {
                    detail: format!("{public_uuid} is not an embedding dataset identity: {error}"),
                }
            })?;
            let entity = store
                .get_entity(dataset_id.internal().as_uuid())
                .await?
                .ok_or(WorkflowError::DataConfigInvalid {
                    detail: format!("embedding dataset {public_uuid} entity row is missing"),
                })?;
            if entity.kind != EmbeddingDataset::KIND {
                return Err(WorkflowError::DataConfigInvalid {
                    detail: format!(
                        "{public_uuid} kind mismatch: expected {}, found {}",
                        EmbeddingDataset::KIND,
                        entity.kind
                    ),
                });
            }
            let latest = store
                .get_latest_revision_typed::<EmbeddingDataset>(dataset_id)
                .await?
                .ok_or(WorkflowError::DataConfigInvalid {
                    detail: format!("embedding dataset {public_uuid} has no revisions"),
                })?;
            let dataset_tenant = required_entity_attr(&latest, EmbeddingDataset::NAME, "tenant")?;
            if dataset_tenant.target_entity_id != template_tenant_ref.target_entity_id {
                return Err(WorkflowError::DataConfigInvalid {
                    detail: format!("embedding dataset {public_uuid} belongs to another tenant"),
                });
            }
            if bool_attr(&latest, EmbeddingDataset::NAME, "is_retired")? {
                continue;
            }
            let Some(corpus_hash) = optional_content_attr(&latest, "corpus") else {
                continue;
            };
            let corpus_bytes =
                load_content_blob(store, corpus_hash, EmbeddingDataset::NAME, "corpus").await?;
            let corpus = decode_corpus(corpus_bytes.bytes()).map_err(|error| {
                WorkflowError::DataConfigInvalid {
                    detail: format!("failed to decode corpus for {public_uuid}: {error}"),
                }
            })?;
            embed_datasets.insert(name.clone(), corpus_items_to_json(&corpus)?);
        }
    }

    let mut data = JsonMap::new();
    data.insert(
        "embed_datasets".to_string(),
        JsonValue::Object(embed_datasets),
    );
    Ok(JsonValue::Object(data))
}

async fn load_json_content<S>(
    store: &S,
    hash: Sha256,
    entity_name: &'static str,
    attribute: &'static str,
) -> Result<JsonValue, WorkflowError>
where
    S: ContentStore,
{
    let typed_hash = ContentHash::<CanonicalJson>::from_digest_unchecked(hash);
    let canonical = store.get_typed::<CanonicalJson>(typed_hash).await?.ok_or(
        WorkflowError::MissingContentBlob {
            entity_name,
            attribute,
            hash,
        },
    )?;
    canonical_to_json(&canonical)
}

async fn load_content_blob<S>(
    store: &S,
    hash: Sha256,
    entity_name: &'static str,
    attribute: &'static str,
) -> Result<philharmonic_types::ContentValue, WorkflowError>
where
    S: ContentStore,
{
    store
        .get(hash)
        .await?
        .ok_or(WorkflowError::MissingContentBlob {
            entity_name,
            attribute,
            hash,
        })
}

fn bool_attr(
    revision: &RevisionRow,
    entity_name: &'static str,
    attribute: &'static str,
) -> Result<bool, WorkflowError> {
    match revision.scalar_attrs.get(attribute) {
        Some(ScalarValue::Bool(value)) => Ok(*value),
        Some(ScalarValue::I64(_)) => Err(WorkflowError::InvalidScalarType {
            entity_name,
            attribute,
            expected: "bool",
            actual: "i64",
        }),
        None => Err(WorkflowError::MissingScalarAttribute {
            entity_name,
            attribute,
        }),
    }
}

fn corpus_items_to_json(items: &[CorpusItem]) -> Result<JsonValue, WorkflowError> {
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        let mut object = JsonMap::new();
        object.insert("id".to_string(), JsonValue::String(item.id.clone()));
        let mut vector = Vec::with_capacity(item.vector.len());
        for value in &item.vector {
            let number = serde_json::Number::from_f64(f64::from(*value)).ok_or_else(|| {
                WorkflowError::DataConfigInvalid {
                    detail: format!("corpus item {} contains a non-finite vector value", item.id),
                }
            })?;
            vector.push(JsonValue::Number(number));
        }
        object.insert("vector".to_string(), JsonValue::Array(vector));
        if let Some(payload) = item.payload.as_ref() {
            object.insert("payload".to_string(), payload.clone());
        }
        values.push(JsonValue::Object(object));
    }
    Ok(JsonValue::Array(values))
}

fn read_status(revision: &RevisionRow) -> Result<InstanceStatus, WorkflowError> {
    let scalar =
        revision
            .scalar_attrs
            .get("status")
            .ok_or(WorkflowError::MissingScalarAttribute {
                entity_name: WorkflowInstance::NAME,
                attribute: "status",
            })?;

    let value = match scalar {
        ScalarValue::I64(value) => *value,
        ScalarValue::Bool(_) => {
            return Err(WorkflowError::InvalidScalarType {
                entity_name: WorkflowInstance::NAME,
                attribute: "status",
                expected: "i64",
                actual: "bool",
            });
        }
    };

    InstanceStatus::try_from_i64(value)
        .ok_or(WorkflowError::InvalidInstanceStatusDiscriminant { value })
}

fn ensure_transition(
    from: InstanceStatus,
    to: InstanceStatus,
    instance_id: Uuid,
) -> Result<(), WorkflowError> {
    if from.can_transition_to(to) {
        Ok(())
    } else {
        Err(WorkflowError::InvalidTransition {
            instance_id,
            from,
            to,
        })
    }
}

fn canonical_to_json(value: &CanonicalJson) -> Result<JsonValue, WorkflowError> {
    serde_json::from_slice(value.as_bytes())
        .map_err(|error| WorkflowError::json(format!("failed to parse canonical JSON: {error}")))
}

struct ParsedExecutorResult {
    context: JsonValue,
    output: JsonValue,
    done: bool,
}

fn parse_executor_result(value: &JsonValue) -> Result<ParsedExecutorResult, String> {
    let context = value
        .get("context")
        .cloned()
        .ok_or_else(|| "executor result missing required field 'context'".to_string())?;
    let output = value
        .get("output")
        .cloned()
        .ok_or_else(|| "executor result missing required field 'output'".to_string())?;

    let done = match value.get("done") {
        None => false,
        Some(JsonValue::Bool(value)) => *value,
        Some(_) => {
            return Err("executor result field 'done' must be boolean".to_string());
        }
    };

    Ok(ParsedExecutorResult {
        context,
        output,
        done,
    })
}
