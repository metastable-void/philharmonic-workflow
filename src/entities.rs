use philharmonic_policy::Tenant;
use philharmonic_types::{
    ContentSlot, Entity, EntitySlot, ScalarSlot, ScalarType, SlotPinning, Uuid,
};

/// Reusable workflow definition.
pub struct WorkflowTemplate;

impl Entity for WorkflowTemplate {
    // Generated via `./scripts/xtask.sh gen-uuid -- --v4` on 2026-04-22.
    const KIND: Uuid = Uuid::from_u128(0xbf6b3627572b4727935567304ab8a0e9);
    const NAME: &'static str = "workflow_template";
    const CONTENT_SLOTS: &'static [ContentSlot] =
        &[ContentSlot::new("script"), ContentSlot::new("config")];
    const ENTITY_SLOTS: &'static [EntitySlot] =
        &[EntitySlot::of::<Tenant>("tenant", SlotPinning::Pinned)];
    const SCALAR_SLOTS: &'static [ScalarSlot] =
        &[ScalarSlot::new("is_retired", ScalarType::Bool, true)];
}

/// Executing workflow instance.
pub struct WorkflowInstance;

impl Entity for WorkflowInstance {
    // Generated via `./scripts/xtask.sh gen-uuid -- --v4` on 2026-04-22.
    const KIND: Uuid = Uuid::from_u128(0xec5459ac44db45519464e6c6dad8f58f);
    const NAME: &'static str = "workflow_instance";
    const CONTENT_SLOTS: &'static [ContentSlot] =
        &[ContentSlot::new("context"), ContentSlot::new("args")];
    const ENTITY_SLOTS: &'static [EntitySlot] = &[
        EntitySlot::of::<WorkflowTemplate>("template", SlotPinning::Pinned),
        EntitySlot::of::<Tenant>("tenant", SlotPinning::Pinned),
    ];
    const SCALAR_SLOTS: &'static [ScalarSlot] = &[ScalarSlot::new("status", ScalarType::I64, true)];
}

/// Immutable execution record for one step run.
pub struct StepRecord;

impl Entity for StepRecord {
    // Generated via `./scripts/xtask.sh gen-uuid -- --v4` on 2026-04-22.
    const KIND: Uuid = Uuid::from_u128(0x586f37e343a74f66b7abd4e9b7ac123a);
    const NAME: &'static str = "step_record";
    const CONTENT_SLOTS: &'static [ContentSlot] = &[
        ContentSlot::new("input"),
        ContentSlot::new("output"),
        ContentSlot::new("error"),
        ContentSlot::new("subject"),
    ];
    const ENTITY_SLOTS: &'static [EntitySlot] = &[EntitySlot::of::<WorkflowInstance>(
        "instance",
        SlotPinning::Pinned,
    )];
    const SCALAR_SLOTS: &'static [ScalarSlot] = &[
        ScalarSlot::new("step_seq", ScalarType::I64, true),
        ScalarSlot::new("outcome", ScalarType::I64, true),
    ];
}
