// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Per-`StorageManager` cache of materialised graph projections.
//!
//! Backs the `uni.graph.{project, drop, list, exists}` procedure family
//! and the V2 `(graphRef, config)` `Named` projection variant. Each
//! [`ProjectionStore`] is scoped to a `StorageManager` instance — that
//! is the closest available proxy for "this database" in the current
//! architecture (the proposal calls for a per-`Database`
//! [`ProjectionStore`]; uni-db's `Database` type hangs off
//! `StorageManager`, so we use the manager's Arc pointer identity as
//! the cache key in the process-global registry below).
//!
//! v1 caveats (per proposal §4.10.3): in-memory only (not persisted),
//! no eviction policy other than `drop`, no LRU. The `bytes` field on
//! [`ProjectionEntry`] exists for a future LRU policy. Staleness is
//! explicit — a projection is frozen at materialisation time;
//! recomputing it requires a `drop` + `project` cycle.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};
use std::time::SystemTime;

use uni_algo::algo::GraphProjection;
use uni_store::storage::manager::StorageManager;

/// How a projection was materialised. Surfaced through
/// `uni.graph.list` so operators can tell at a glance which
/// projections came from native label/edge-type scans vs inner Cypher
/// queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionSourceKind {
    /// Materialised from native labels + edge types
    /// (`graphRef = {nodeLabels, edgeTypes, ...}`).
    Native,
    /// Materialised from two inner Cypher queries
    /// (`graphRef = {nodeQuery, edgeQuery, ...}`).
    Cypher,
}

impl ProjectionSourceKind {
    /// Stable string label for diagnostic output (`uni.graph.list`).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Native => "Native",
            Self::Cypher => "Cypher",
        }
    }
}

/// One stored projection plus the bookkeeping `uni.graph.list` needs.
#[derive(Clone)]
pub struct ProjectionEntry {
    /// The materialised projection. `Arc` so callers can take cheap
    /// clones for repeated algorithm invocations.
    pub projection: Arc<GraphProjection>,
    /// Vertex count at materialisation time.
    pub node_count: usize,
    /// Edge count at materialisation time.
    pub edge_count: usize,
    /// Approximate memory footprint in bytes (advisory — used by a
    /// future LRU policy that v1 does not implement).
    pub bytes: usize,
    /// Wall-clock instant the projection was materialised.
    pub created_at: SystemTime,
    /// Where the projection's rows came from.
    pub source_kind: ProjectionSourceKind,
}

impl std::fmt::Debug for ProjectionEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectionEntry")
            .field("node_count", &self.node_count)
            .field("edge_count", &self.edge_count)
            .field("bytes", &self.bytes)
            .field("source_kind", &self.source_kind)
            .finish_non_exhaustive()
    }
}

/// In-memory cache of named graph projections, keyed by user-chosen
/// name strings. See module docs for scope and eviction semantics.
#[derive(Default)]
pub struct ProjectionStore {
    entries: RwLock<HashMap<String, ProjectionEntry>>,
}

impl std::fmt::Debug for ProjectionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.entries.read().map(|g| g.len()).unwrap_or(0);
        f.debug_struct("ProjectionStore")
            .field("entries", &len)
            .finish()
    }
}

impl ProjectionStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a projection. Returns `Err` if a projection with the same
    /// name already exists (caller must `drop` first — the proposal
    /// rejects implicit replace).
    ///
    /// # Errors
    ///
    /// Returns the duplicate name as `Err(String)` so the calling
    /// procedure can surface it through `FnError`.
    pub fn insert(&self, name: String, entry: ProjectionEntry) -> Result<(), String> {
        let mut g = self
            .entries
            .write()
            .map_err(|_| "store lock poisoned".to_owned())?;
        if g.contains_key(&name) {
            return Err(name);
        }
        g.insert(name, entry);
        Ok(())
    }

    /// Look up a projection by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<ProjectionEntry> {
        self.entries.read().ok()?.get(name).cloned()
    }

    /// Remove a projection by name. Returns `true` if a projection was
    /// removed, `false` if no such projection existed.
    ///
    /// Named `drop_by_name` (not just `drop`) because `drop` is a
    /// reserved method-name slot in Rust's `Drop` trait resolution
    /// and the compiler refuses ambient destructor calls.
    pub fn drop_by_name(&self, name: &str) -> bool {
        self.entries
            .write()
            .map(|mut g| g.remove(name).is_some())
            .unwrap_or(false)
    }

    /// List every stored projection as `(name, entry)` pairs.
    #[must_use]
    pub fn list(&self) -> Vec<(String, ProjectionEntry)> {
        self.entries
            .read()
            .map(|g| g.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    /// Probe membership without cloning the entry.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.entries
            .read()
            .map(|g| g.contains_key(name))
            .unwrap_or(false)
    }
}

/// One process-global projection-registry row: a weak handle to a
/// database's schema manager paired with its projection store.
///
/// The [`Weak`] lets dropped databases be pruned so a recycled heap
/// address is never matched to a stale store.
type ProjectionRegistryEntry = (
    Weak<uni_common::core::schema::SchemaManager>,
    Arc<ProjectionStore>,
);

/// Look up (or create) the [`ProjectionStore`] for the given
/// `StorageManager`. Identifies the owning database by the backing
/// `Arc<SchemaManager>` *allocation identity* — callers sharing the
/// same `schema_manager` Arc (e.g. a pinned transaction and the live
/// session) see the same store, while a fork (which holds a distinct
/// `schema_manager`) gets an isolated store.
///
/// The registry holds a [`Weak`] reference per database and compares
/// entries with [`Arc::ptr_eq`] on the *live* upgraded Arc rather than
/// a raw address. This makes stale reuse impossible: a dropped
/// database's `Weak` can never upgrade to a live Arc, so a later
/// database that happens to reuse a freed heap address is never
/// matched to the dead database's store. Dead entries are pruned on
/// each lookup, so the registry stays bounded.
pub fn for_storage(storage: &Arc<StorageManager>) -> Arc<ProjectionStore> {
    static REGISTRY: OnceLock<Mutex<Vec<ProjectionRegistryEntry>>> = OnceLock::new();

    let schema_arc = storage.schema_manager_arc_ref();
    let reg = REGISTRY.get_or_init(|| Mutex::new(Vec::new()));
    let mut g = match reg.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    // Drop entries whose `SchemaManager` has been dropped so a freed heap
    // address cannot be re-associated with a stale store.
    g.retain(|(weak, _)| weak.strong_count() > 0);
    if let Some((_, store)) = g
        .iter()
        .find(|(weak, _)| weak.upgrade().is_some_and(|s| Arc::ptr_eq(&s, schema_arc)))
    {
        return store.clone();
    }
    let store = Arc::new(ProjectionStore::new());
    g.push((Arc::downgrade(schema_arc), store.clone()));
    store
}

/// Best-effort byte-size estimate for a [`GraphProjection`]. Used by
/// [`ProjectionEntry::bytes`] — informational only (a future LRU
/// policy will consult it).
#[must_use]
pub fn estimate_bytes(p: &GraphProjection) -> usize {
    use std::mem::size_of;
    let v = p.vertex_count();
    // Approximate: CSR offsets (V+1)*4 each (out + in) + neighbors
    // count*4 each. We don't have direct accessors for the edge
    // count, but `out_offsets[V]` would tell us; for v1 estimate
    // very loosely as 32 bytes per vertex.
    v * 32 + size_of::<GraphProjection>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn empty_entry() -> ProjectionEntry {
        ProjectionEntry {
            projection: Arc::new(GraphProjection::from_rows(&[], &[], None, false).unwrap()),
            node_count: 0,
            edge_count: 0,
            bytes: 0,
            created_at: SystemTime::now(),
            source_kind: ProjectionSourceKind::Native,
        }
    }

    #[test]
    fn insert_get_drop_round_trip() {
        let s = ProjectionStore::new();
        s.insert("g".to_owned(), empty_entry()).unwrap();
        assert!(s.contains("g"));
        assert!(s.get("g").is_some());
        assert!(s.drop_by_name("g"));
        assert!(!s.contains("g"));
        assert!(!s.drop_by_name("g"));
    }

    #[test]
    fn duplicate_insert_rejected() {
        let s = ProjectionStore::new();
        s.insert("g".to_owned(), empty_entry()).unwrap();
        let err = s.insert("g".to_owned(), empty_entry()).unwrap_err();
        assert_eq!(err, "g");
    }

    #[test]
    fn list_returns_all_entries() {
        let s = ProjectionStore::new();
        s.insert("a".to_owned(), empty_entry()).unwrap();
        s.insert("b".to_owned(), empty_entry()).unwrap();
        let l = s.list();
        assert_eq!(l.len(), 2);
        let names: Vec<&str> = l.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }
}
