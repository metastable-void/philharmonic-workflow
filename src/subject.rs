use philharmonic_policy::{MintingAuthority, Tenant};
use philharmonic_types::{EntityId, JsonValue};

use serde::{Deserialize, Serialize};
use serde_json::json;

/// Authenticated caller category for workflow operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubjectKind {
    /// Persistent principal identity.
    Principal,
    /// Ephemeral subject minted by a minting authority.
    Ephemeral,
}

/// Caller context threaded through engine operations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubjectContext {
    /// Caller kind.
    pub kind: SubjectKind,
    /// Opaque caller identifier.
    pub id: String,
    /// Tenant owning the request scope.
    pub tenant_id: EntityId<Tenant>,
    /// Minting authority, required for ephemeral subjects.
    pub authority_id: Option<EntityId<MintingAuthority>>,
    /// Free-form claims from the authenticated context.
    pub claims: JsonValue,
}

impl SubjectContext {
    /// Serialize subject context for script arguments.
    pub fn to_script_value(&self) -> Result<JsonValue, serde_json::Error> {
        Ok(json!({
            "kind": self.kind,
            "id": self.id,
            "tenant_id": self.tenant_id.public().as_uuid().to_string(),
            "authority_id": self
                .authority_id
                .map(|authority_id| authority_id.public().as_uuid().to_string()),
            "claims": self.claims,
        }))
    }

    /// Convert to the persisted step-record subject shape, retaining non-empty claims verbatim.
    pub fn to_step_record_subject(&self) -> StepRecordSubject {
        StepRecordSubject {
            kind: self.kind,
            id: self.id.clone(),
            authority_id: self.authority_id,
            claims: match &self.claims {
                JsonValue::Null => None,
                JsonValue::Object(claims) if claims.is_empty() => None,
                claims => Some(claims.clone()),
            },
        }
    }
}

/// Persisted subject content for a step record.
///
/// Tenant fields are excluded. Caller-supplied claims are persisted verbatim
/// when present; consumers interpret their own claim conventions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepRecordSubject {
    /// Caller kind.
    pub kind: SubjectKind,
    /// Opaque caller identifier.
    pub id: String,
    /// Minting authority for ephemeral subjects.
    pub authority_id: Option<EntityId<MintingAuthority>>,
    /// Arbitrary caller-supplied claims, persisted verbatim when present.
    /// This framework makes no assumptions about claim shape or meaning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claims: Option<JsonValue>,
}
