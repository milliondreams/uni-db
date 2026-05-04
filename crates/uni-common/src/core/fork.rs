// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Type definitions for the fork feature.
//!
//! `Fork` = a named, durable, isolated branch of the graph. Each fork is
//! backed by one Lance branch per dataset (vertex, edge-delta, adjacency).
//! These types are persisted to `catalog/fork_registry.json` and
//! `catalog/fork_schemas/{fork_id}.json`. See
//! `docs/proposals/graph_fork_plan.md` §Phase 1 for the spec they
//! implement and the 2PC state machines that govern their lifecycle.
//!
//! `SchemaDelta` is wired through Phase 1 with all instances empty —
//! the merge infrastructure exists so Phase 2's on-the-fly label
//! creation has a populated path to land into.

// Rust guideline compliant

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::core::schema::{DataType, EdgeTypeMeta, LabelMeta};

/// Stable identifier for a fork. Display format is base32 ULID.
///
/// Newtype around [`ulid::Ulid`]; preserves time-ordering across
/// processes and avoids the random-distribution drawbacks of UUIDv4.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ForkId(pub Ulid);

impl ForkId {
    /// Allocate a fresh ForkId using the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// Parse a ForkId from its canonical string form.
    ///
    /// # Errors
    ///
    /// Returns an error if `s` is not a valid 26-character base32 ULID.
    pub fn parse(s: &str) -> Result<Self, ulid::DecodeError> {
        Ulid::from_string(s).map(Self)
    }
}

impl Default for ForkId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ForkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Lifecycle status of a fork in the registry.
///
/// State machine: `Pending` → `Active` (create commit point); `Active` →
/// `Tombstoned` → removed (drop commit point). Recovery resumes any
/// non-`Active` state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum ForkStatus {
    /// Registry entry persisted; some Lance branches not yet created.
    Pending,
    /// All Lance branches present; fork is reachable via `session.fork`.
    Active,
    /// Drop initiated; recovery will finish removing branches.
    Tombstoned,
}

/// Metadata for a single fork.
///
/// One [`ForkInfo`] per fork in `catalog/fork_registry.json`. The
/// `datasets` map is filled in step 4 of the create 2PC and is
/// authoritative for which Lance branches the fork owns.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkInfo {
    /// Stable identifier; persists across rename if rename ever lands.
    pub id: ForkId,

    /// Human-readable name (unique per database).
    pub name: String,

    /// Parent fork in a nested-fork chain. `None` ⇒ parent is primary.
    /// Phase 1 always sets `None`.
    #[serde(default)]
    pub parent_fork_id: Option<ForkId>,

    /// Snapshot id of primary at the moment the fork was created.
    pub parent_snapshot_id: String,

    /// Wall-clock UTC at fork creation.
    pub created_at: DateTime<Utc>,

    /// Wall-clock TTL expiry. `None` ⇒ never expires. Phase 1 always
    /// `None`; the sweeper lands in Phase 4.
    #[serde(default)]
    pub ttl_expires_at: Option<DateTime<Utc>>,

    /// Schema version (`Schema::schema_version`) at fork creation.
    /// Captured day-one even though only Phase 7's schema-evolution
    /// spike consumes it; backfilling later is impossible.
    pub schema_version_at_creation: u32,

    /// Map of `dataset_name` → `branch_name` for every Lance dataset
    /// this fork owns. Branch names live under the dataset's `tree/`
    /// directory in Lance's on-disk layout.
    pub datasets: BTreeMap<String, String>,

    /// Lifecycle state. See [`ForkStatus`].
    pub status: ForkStatus,
}

impl ForkInfo {
    /// Convenience: build a `Pending` info ready for create 2PC step 2.
    #[must_use]
    pub fn new_pending(
        id: ForkId,
        name: impl Into<String>,
        parent_snapshot_id: impl Into<String>,
        schema_version: u32,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            parent_fork_id: None,
            parent_snapshot_id: parent_snapshot_id.into(),
            created_at: Utc::now(),
            ttl_expires_at: None,
            schema_version_at_creation: schema_version,
            datasets: BTreeMap::new(),
            status: ForkStatus::Pending,
        }
    }
}

/// Adds a property to an existing label or edge type via the fork
/// schema overlay. Phase 1 has no producer; Phase 2 fills this when
/// `tx.execute("CREATE (n:Person {phone: ...})")` introduces a
/// previously-unknown property on a fork.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PropertyAddition {
    /// Owning label or edge-type name.
    pub owner: String,
    /// Whether `owner` is a label or an edge type.
    pub owner_kind: PropertyOwnerKind,
    /// New property name.
    pub property: String,
    /// Declared type.
    pub data_type: DataType,
    /// Whether the new property may be null.
    pub nullable: bool,
}

/// Discriminator for [`PropertyAddition::owner_kind`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PropertyOwnerKind {
    /// Property attaches to a vertex label.
    Label,
    /// Property attaches to an edge type.
    EdgeType,
}

/// Schema additions on top of primary, owned by a single fork.
///
/// Only *additions* — renames, drops, and type changes are spec
/// non-goals (§14). Always read together with primary's schema:
/// `merged = primary ⊕ delta`. The merge implementation lives in
/// [`crate::core::schema::SchemaManager::with_overlay`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SchemaDelta {
    /// Vertex labels new to this fork's schema.
    #[serde(default)]
    pub added_labels: Vec<(String, LabelMeta)>,

    /// Edge types new to this fork's schema.
    #[serde(default)]
    pub added_edge_types: Vec<(String, EdgeTypeMeta)>,

    /// Properties added to existing labels or edge types.
    #[serde(default)]
    pub added_properties: Vec<PropertyAddition>,
}

impl SchemaDelta {
    /// Convenience: empty delta (the only valid Phase 1 value).
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// `true` if the delta contributes nothing on top of primary.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.added_labels.is_empty()
            && self.added_edge_types.is_empty()
            && self.added_properties.is_empty()
    }
}

/// Top-level on-disk shape of `catalog/fork_registry.json`.
///
/// Concurrent updates are serialized at the registry-handle layer in
/// `uni-store`; this struct is just the wire format.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ForkRegistryFile {
    /// Fork name → metadata.
    #[serde(default)]
    pub forks: BTreeMap<String, ForkInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_id_roundtrip() {
        let id = ForkId::new();
        let s = id.to_string();
        let parsed = ForkId::parse(&s).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn fork_info_serde_roundtrip() {
        let info = ForkInfo::new_pending(ForkId::new(), "scenario_1", "snap-abc", 17);
        let json = serde_json::to_string(&info).unwrap();
        let parsed: ForkInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, info.id);
        assert_eq!(parsed.name, "scenario_1");
        assert_eq!(parsed.parent_snapshot_id, "snap-abc");
        assert_eq!(parsed.schema_version_at_creation, 17);
        assert_eq!(parsed.status, ForkStatus::Pending);
        assert!(parsed.datasets.is_empty());
        assert!(parsed.parent_fork_id.is_none());
        assert!(parsed.ttl_expires_at.is_none());
    }

    #[test]
    fn registry_file_default_empty() {
        let file = ForkRegistryFile::default();
        let json = serde_json::to_string(&file).unwrap();
        let parsed: ForkRegistryFile = serde_json::from_str(&json).unwrap();
        assert!(parsed.forks.is_empty());
    }

    #[test]
    fn schema_delta_default_is_empty() {
        let d = SchemaDelta::default();
        assert!(d.is_empty());
    }
}
