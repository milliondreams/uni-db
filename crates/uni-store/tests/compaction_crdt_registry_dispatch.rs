#![allow(clippy::all)]
//! WS-D (P0.4): end-to-end proof that vertex **compaction** routes CRDT
//! merges through the plugin registry stamped on the `StorageManager`.
//!
//! The merge helpers (`Compactor::merge_crdt_values`, `Crdt::merge_via_registry`)
//! are already unit-tested. The GAP this file closes is a *runtime* test: does
//! the registry stamped via `StorageManager::set_plugin_registry` actually
//! survive the `StorageManager` -> `Compactor::compact_vertices` path and reach
//! `merge_crdt_values` during a real compaction of two L1 delta rows?
//!
//! Strategy: declare a vertex label with a `DataType::Crdt(GCounter)` property,
//! write the same vid twice (two flushes => two compactable L1 delta rows),
//! stamp an invocation-counting `CrdtKindProvider` onto the owned
//! `StorageManager` BEFORE Arc-wrapping it, run `Compactor::compact_vertices`,
//! and assert the provider's `from_persisted` fired. A companion test with NO
//! provider registered proves the native-fallback path still compacts.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::schema::{CrdtType, DataType, SchemaManager};
use uni_crdt::{Crdt, GCounter};
use uni_plugin::traits::crdt::{CrdtKind, CrdtKindProvider, CrdtOp, CrdtState, ScalarValue};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, PluginRegistry};
use uni_store::runtime::writer::Writer;
use uni_store::storage::compaction::Compactor;
use uni_store::storage::manager::StorageManager;

// ── Invocation-counting GCounter provider (mirrors l0_crdt_registry_dispatch) ─

#[derive(Default)]
struct CountingProvider {
    from_persisted_calls: AtomicUsize,
}

impl CrdtKindProvider for CountingProvider {
    fn kind(&self) -> CrdtKind {
        // Matches `uni_crdt::Crdt::kind()` for `Crdt::GCounter(_)`.
        CrdtKind::new("uni-crdt:g-counter")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(NativeState {
            inner: Crdt::GCounter(GCounter::new()),
        })
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        self.from_persisted_calls.fetch_add(1, Ordering::SeqCst);
        let inner = Crdt::from_msgpack(bytes)
            .map_err(|e| FnError::new(0xA01, format!("from_persisted: {e}")))?;
        Ok(Box::new(NativeState { inner }))
    }
}

struct NativeState {
    inner: Crdt,
}

impl CrdtState for NativeState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, _op: &CrdtOp) -> Result<(), FnError> {
        Ok(())
    }
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
        let other = other
            .as_any()
            .downcast_ref::<NativeState>()
            .ok_or_else(|| FnError::new(0xA10, "merge: type mismatch"))?;
        self.inner
            .try_merge(&other.inner)
            .map_err(|e| FnError::new(0xA11, format!("native merge: {e}")))
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Utf8(Some(self.inner.type_name().to_owned())))
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        self.inner
            .to_msgpack()
            .map_err(|e| FnError::new(0xA12, format!("persist: {e}")))
    }
}

fn registry_with_provider() -> (Arc<PluginRegistry>, Arc<CountingProvider>) {
    let registry = Arc::new(PluginRegistry::new());
    let provider = Arc::new(CountingProvider::default());
    let caps = CapabilitySet::from_iter_of([Capability::Crdt]);
    let mut r = PluginRegistrar::new(PluginId::new("test.counting-gcounter"), &caps, &registry);
    r.crdt_kind(
        CrdtKind::new("uni-crdt:g-counter"),
        Arc::clone(&provider) as Arc<dyn CrdtKindProvider>,
    )
    .expect("register provider");
    r.commit_to_registry().expect("commit");
    (registry, provider)
}

/// Build a `uni_common::Value` holding a GCounter with `actor += by`.
fn gcounter_val(actor: &str, by: u64) -> Value {
    let mut gc = GCounter::new();
    gc.increment(actor, by);
    serde_json::to_value(Crdt::GCounter(gc))
        .expect("to_value")
        .into()
}

/// Set up a `StorageManager` on a tempdir with a `Counter` label that owns a
/// `count` GCounter CRDT property. Optionally stamps `registry` onto the owned
/// manager *before* Arc-wrapping (`set_plugin_registry` needs `&mut self`).
///
/// Returns the tempdir guard (kept alive), the Arc storage, and a `Writer`.
async fn setup(
    registry: Option<Arc<PluginRegistry>>,
) -> anyhow::Result<(tempfile::TempDir, Arc<StorageManager>, Writer)> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_owned();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

    schema_manager.add_label("Counter")?;
    schema_manager.add_property("Counter", "count", DataType::Crdt(CrdtType::GCounter), true)?;
    schema_manager.save().await?;

    // `set_plugin_registry` needs `&mut self`, so stamp the registry on the
    // OWNED StorageManager before Arc-wrapping — exactly how uni/api/mod.rs
    // wires it (owned `storage.set_plugin_registry(...)` then `Arc::new(storage)`).
    let mut storage = StorageManager::new(&path, schema_manager.clone()).await?;
    if let Some(registry) = registry {
        storage.set_plugin_registry(registry);
    }
    let storage = Arc::new(storage);

    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;
    Ok((dir, storage, writer))
}

/// Write the same vid's `count` CRDT property at two increasing versions so
/// vertex compaction must merge two L1 delta rows.
async fn write_two_versions(writer: &Writer) -> anyhow::Result<()> {
    let vid = writer.next_vid().await?;

    // Version 1: actor "r1" += 5.
    let props1 = HashMap::from([("count".to_string(), gcounter_val("r1", 5))]);
    writer
        .insert_vertex_with_labels(vid, props1, &["Counter".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // Version 2 (same vid): actor "r2" += 7 — forces a two-sided merge.
    let props2 = HashMap::from([("count".to_string(), gcounter_val("r2", 7))]);
    writer
        .insert_vertex_with_labels(vid, props2, &["Counter".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    Ok(())
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// Compaction of two CRDT-property versions routes the merge through the
/// registry stamped on the `StorageManager`.
#[tokio::test]
async fn compaction_routes_crdt_merge_through_registry() -> anyhow::Result<()> {
    let (registry, provider) = registry_with_provider();

    let (_dir, storage, writer) = setup(Some(Arc::clone(&registry))).await?;
    write_two_versions(&writer).await?;

    // Sanity: the registry really is reachable from the StorageManager the
    // Compactor will read (this is the WS-D wiring under test).
    assert!(
        storage.plugin_registry().is_some(),
        "StorageManager must expose the stamped registry to the Compactor"
    );

    // Run REAL vertex compaction of the two L1 delta rows for the vid.
    Compactor::new(storage.clone())
        .compact_vertices("Counter")
        .await?;

    // The registry path ran: `from_persisted` fired during compaction's merge.
    let calls = provider.from_persisted_calls.load(Ordering::SeqCst);
    assert!(
        calls > 0,
        "compaction must route the CRDT merge through the registered \
         CrdtKindProvider (from_persisted was never called => registry \
         did not survive StorageManager -> Compactor path)"
    );

    Ok(())
}

/// With NO provider registered, compaction still succeeds via native fallback
/// (behaviour-preserving).
#[tokio::test]
async fn compaction_falls_back_to_native_without_provider() -> anyhow::Result<()> {
    let (_dir, storage, writer) = setup(None).await?;
    write_two_versions(&writer).await?;

    assert!(
        storage.plugin_registry().is_none(),
        "no registry stamped => native fallback path"
    );

    // Must not error — native `Crdt::try_merge` handles the merge.
    Compactor::new(storage.clone())
        .compact_vertices("Counter")
        .await?;

    Ok(())
}
