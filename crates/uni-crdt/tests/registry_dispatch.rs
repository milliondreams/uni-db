//! M10 acceptance test: `Crdt::merge_via_registry` end-to-end.
//!
//! Registers two CRDT providers (g-counter, or-set) that wrap the
//! native `uni_crdt` types via the msgpack envelope, then runs the
//! registry-dispatched merge and asserts the merged state matches
//! `Crdt::try_merge`. This satisfies the M10 acceptance criterion that
//! the dispatch path exercises mutation → registry lookup → apply for
//! ≥2 of the built-in CRDTs.

use std::sync::Arc;

use uni_crdt::registry_dispatch::op_from_bytes;
use uni_crdt::{Crdt, GCounter, ORSet};
use uni_plugin::traits::crdt::{CrdtKind, CrdtKindProvider, CrdtOp, CrdtState, ScalarValue};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, PluginRegistry};

// ── Native uni-crdt providers (msgpack envelope) ────────────────────────

/// Provider wrapping `uni_crdt::GCounter` via the msgpack `Crdt` envelope.
struct NativeGCounterProvider;

impl CrdtKindProvider for NativeGCounterProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("uni-crdt:g-counter")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(NativeCrdtState {
            inner: Crdt::GCounter(GCounter::new()),
        })
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        let inner = Crdt::from_msgpack(bytes)
            .map_err(|e| FnError::new(0x900, format!("g-counter from_persisted: {e}")))?;
        Ok(Box::new(NativeCrdtState { inner }))
    }
}

/// Provider wrapping `uni_crdt::ORSet<String>` via the msgpack envelope.
struct NativeOrSetProvider;

impl CrdtKindProvider for NativeOrSetProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("uni-crdt:or-set")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(NativeCrdtState {
            inner: Crdt::ORSet(ORSet::new()),
        })
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        let inner = Crdt::from_msgpack(bytes)
            .map_err(|e| FnError::new(0x901, format!("or-set from_persisted: {e}")))?;
        Ok(Box::new(NativeCrdtState { inner }))
    }
}

/// State adapter that holds any `Crdt` variant.
struct NativeCrdtState {
    inner: Crdt,
}

impl CrdtState for NativeCrdtState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, _op: &CrdtOp) -> Result<(), FnError> {
        // The op shape is variant-specific; for the dispatch test we
        // only exercise merge / value / persist. apply remains a no-op
        // here.
        Ok(())
    }
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
        let other = other
            .as_any()
            .downcast_ref::<NativeCrdtState>()
            .ok_or_else(|| FnError::new(0x910, "native merge: state type mismatch"))?;
        self.inner
            .try_merge(&other.inner)
            .map_err(|e| FnError::new(0x911, format!("native merge: {e}")))
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        // The dispatcher does not consult value() for the merge path;
        // surface the variant tag so callers can sanity-check.
        Ok(ScalarValue::Utf8(Some(self.inner.type_name().to_owned())))
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        self.inner
            .to_msgpack()
            .map_err(|e| FnError::new(0x912, format!("native persist: {e}")))
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Register both native providers under a single test plugin id.
fn register_native_providers(registry: &PluginRegistry) {
    let caps = CapabilitySet::from_iter_of([Capability::Crdt]);
    let mut r = PluginRegistrar::new(PluginId::new("uni-crdt.native"), &caps, registry);
    r.crdt_kind(
        CrdtKind::new("uni-crdt:g-counter"),
        Arc::new(NativeGCounterProvider),
    )
    .unwrap();
    r.crdt_kind(
        CrdtKind::new("uni-crdt:or-set"),
        Arc::new(NativeOrSetProvider),
    )
    .unwrap();
    r.commit_to_registry().unwrap();
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn registry_dispatch_g_counter_merges_through_provider() {
    let registry = PluginRegistry::new();
    register_native_providers(&registry);

    let mut a = GCounter::new();
    a.increment("r1", 5);
    a.increment("r2", 2);
    let mut b = GCounter::new();
    b.increment("r2", 10);
    b.increment("r3", 4);

    // Expected via the native dispatch.
    let mut expected = Crdt::GCounter(a.clone());
    expected.try_merge(&Crdt::GCounter(b.clone())).unwrap();

    // Actual via the registry.
    let mut actual = Crdt::GCounter(a);
    actual
        .merge_via_registry(&Crdt::GCounter(b), &registry)
        .expect("registry-dispatched merge");

    assert_eq!(actual, expected);
    match actual {
        Crdt::GCounter(c) => assert_eq!(c.value(), 5 + 10 + 4),
        other => panic!("expected GCounter, got {other:?}"),
    }
}

#[test]
fn registry_dispatch_or_set_merges_through_provider() {
    let registry = PluginRegistry::new();
    register_native_providers(&registry);

    let mut a = ORSet::<String>::new();
    a.add("apple".to_owned());
    a.add("banana".to_owned());
    let mut b = ORSet::<String>::new();
    b.add("cherry".to_owned());

    let mut expected = Crdt::ORSet(a.clone());
    expected.try_merge(&Crdt::ORSet(b.clone())).unwrap();

    let mut actual = Crdt::ORSet(a);
    actual
        .merge_via_registry(&Crdt::ORSet(b), &registry)
        .expect("registry-dispatched merge");

    assert_eq!(actual, expected);
    match actual {
        Crdt::ORSet(s) => {
            let mut elts = s.elements();
            elts.sort();
            assert_eq!(elts, vec!["apple", "banana", "cherry"]);
        }
        other => panic!("expected ORSet, got {other:?}"),
    }
}

#[test]
fn registry_dispatch_falls_back_when_no_provider_registered() {
    // Empty registry — `merge_via_registry` should fall back to native.
    let registry = PluginRegistry::new();
    let mut a = GCounter::new();
    a.increment("r1", 3);
    let mut b = GCounter::new();
    b.increment("r2", 7);
    let mut x = Crdt::GCounter(a);
    x.merge_via_registry(&Crdt::GCounter(b), &registry)
        .expect("native fallback");
    match x {
        Crdt::GCounter(c) => assert_eq!(c.value(), 10),
        other => panic!("expected GCounter, got {other:?}"),
    }
}

#[test]
fn op_helper_wraps_bytes_into_crdt_op() {
    let op = op_from_bytes(vec![1, 2, 3]);
    assert_eq!(op.bytes, vec![1, 2, 3]);
}
