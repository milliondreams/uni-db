#![allow(dead_code, unused_imports, clippy::all)]
//! Proof-of-life test for the M10 follow-up migration:
//! `PropertyManager::merge_crdt_values` now consults the plugin
//! registry via [`uni_crdt::Crdt::merge_via_registry`].
//!
//! The test registers an invocation-counting `CrdtKindProvider` over
//! `uni_crdt::GCounter`, builds a `PropertyManager` against that
//! registry, drives a merge through it, and asserts both (a) the
//! merged value matches what native `try_merge` would produce and
//! (b) the registry's `from_persisted` was actually called — which
//! is what proves the funnel went through the registry rather than
//! the native fallback.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::core::schema::SchemaManager;
use uni_crdt::{Crdt, GCounter};
use uni_plugin::traits::crdt::{CrdtKind, CrdtKindProvider, CrdtOp, CrdtState, ScalarValue};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, PluginRegistry};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

// ── Invocation-counting GCounter provider ───────────────────────────

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
        Box::new(NativeGCounterState {
            inner: Crdt::GCounter(GCounter::new()),
        })
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        self.from_persisted_calls.fetch_add(1, Ordering::SeqCst);
        let inner = Crdt::from_msgpack(bytes)
            .map_err(|e| FnError::new(0xA01, format!("gcounter from_persisted: {e}")))?;
        Ok(Box::new(NativeGCounterState { inner }))
    }
}

/// `CrdtState` adapter that holds any `Crdt` variant and routes
/// through the native dispatch under the hood. This is the shape
/// other registry-dispatched native providers would take.
struct NativeGCounterState {
    inner: Crdt,
}

impl CrdtState for NativeGCounterState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, _op: &CrdtOp) -> Result<(), FnError> {
        Ok(())
    }
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
        let other = other
            .as_any()
            .downcast_ref::<NativeGCounterState>()
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
            .map_err(|e| FnError::new(0xA12, format!("native persist: {e}")))
    }
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Register `provider` under `uni-crdt:g-counter`.
fn register_counting_provider(registry: &PluginRegistry, provider: Arc<CountingProvider>) {
    let caps = CapabilitySet::from_iter_of([Capability::Crdt]);
    let mut r = PluginRegistrar::new(PluginId::new("test.counting-gcounter"), &caps, registry);
    r.crdt_kind(
        CrdtKind::new("uni-crdt:g-counter"),
        provider as Arc<dyn CrdtKindProvider>,
    )
    .expect("register provider");
    r.commit_to_registry().expect("commit");
}

/// Construct a real `PropertyManager` against a temp `StorageManager`
/// wired to `registry`.
async fn build_pm(
    registry: Arc<PluginRegistry>,
) -> anyhow::Result<(tempfile::TempDir, PropertyManager)> {
    let dir = tempdir()?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    let path = dir.path().to_str().unwrap();
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let pm = PropertyManager::with_plugin_registry(storage, schema_manager, 0, registry);
    Ok((dir, pm))
}

// ── Tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn merge_crdt_values_invokes_registered_provider() -> anyhow::Result<()> {
    let registry = Arc::new(PluginRegistry::new());
    let provider = Arc::new(CountingProvider::default());
    register_counting_provider(&registry, Arc::clone(&provider));

    let (_dir, pm) = build_pm(Arc::clone(&registry)).await?;

    let mut a = GCounter::new();
    a.increment("r1", 5);
    let mut b = GCounter::new();
    b.increment("r2", 7);

    let av = uni_common::Value::from(serde_json::to_value(Crdt::GCounter(a.clone()))?);
    let bv = uni_common::Value::from(serde_json::to_value(Crdt::GCounter(b.clone()))?);

    let merged = pm.merge_crdt_values(&av, &bv)?;

    // (a) The merge produced the expected value (matches native).
    let merged_json: serde_json::Value = merged.into();
    let merged_crdt: Crdt = serde_json::from_value(merged_json)?;
    match merged_crdt {
        Crdt::GCounter(g) => assert_eq!(g.value(), 12, "5 + 7 = 12"),
        other => panic!("expected GCounter, got {other:?}"),
    }

    // (b) The registry path actually ran: `from_persisted` was called
    //     twice (once for `lhs`, once for `rhs`).
    let invocations = provider.from_persisted_calls.load(Ordering::SeqCst);
    assert_eq!(
        invocations, 2,
        "from_persisted should be called once per operand"
    );
    Ok(())
}

#[tokio::test]
async fn merge_crdt_values_falls_back_to_native_with_empty_registry() -> anyhow::Result<()> {
    // Empty registry → `merge_via_registry` falls back to native
    // dispatch. This is the semantics-preserving path for callers
    // using the legacy 3-arg `PropertyManager::new`.
    let registry = Arc::new(PluginRegistry::new());
    let (_dir, pm) = build_pm(registry).await?;

    let mut a = GCounter::new();
    a.increment("r1", 3);
    let mut b = GCounter::new();
    b.increment("r2", 4);

    let av = uni_common::Value::from(serde_json::to_value(Crdt::GCounter(a))?);
    let bv = uni_common::Value::from(serde_json::to_value(Crdt::GCounter(b))?);

    let merged = pm.merge_crdt_values(&av, &bv)?;
    let merged_json: serde_json::Value = merged.into();
    let merged_crdt: Crdt = serde_json::from_value(merged_json)?;
    match merged_crdt {
        Crdt::GCounter(g) => assert_eq!(g.value(), 7),
        other => panic!("expected GCounter, got {other:?}"),
    }
    Ok(())
}
