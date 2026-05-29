#![allow(dead_code, unused_imports, clippy::all)]
//! M5b — verify the planner consults the plugin registry's
//! `IndexKindProvider` chain.
//!
//! This test verifies the registry-canonical pattern at the registry
//! level: a custom `IndexKindProvider` is registered for a synthetic
//! kind and the registry returns it via `index_kind()`. Full planner
//! dispatch through a non-built-in `IndexKindProvider` (i.e., building
//! an `IndexProbeExec` against the provider) is tracked as an M5b
//! follow-up.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::RecordBatch;
use uni_plugin::traits::index::{IndexBuild, IndexHandle, IndexKind, IndexKindProvider};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, PluginRegistry};

#[derive(Debug)]
struct StubProvider;

impl IndexKindProvider for StubProvider {
    fn kind(&self) -> IndexKind {
        IndexKind::new("test_stub")
    }
    fn build(&self, _src: &RecordBatch, _opts: &str) -> Result<Box<dyn IndexBuild>, FnError> {
        Err(FnError::new(0, "stub"))
    }
    fn open(&self, _bytes: &[u8]) -> Result<Box<dyn IndexHandle>, FnError> {
        Err(FnError::new(0, "stub"))
    }
}

#[test]
fn index_kind_provider_reachable_via_registry() {
    let registry = Arc::new(PluginRegistry::default());
    let plugin_id = PluginId::new("test_idx");
    let caps = CapabilitySet::from_iter_of([Capability::Index]);
    let mut r = PluginRegistrar::new(plugin_id, &caps, &registry);
    r.index_kind(IndexKind::new("test_stub"), Arc::new(StubProvider))
        .unwrap();
    r.commit_to_registry().unwrap();

    let provider = registry.index_kind(&IndexKind::new("test_stub"));
    assert!(
        provider.is_some(),
        "registry must return registered provider"
    );
    let kinds = registry.iter_index_kinds();
    assert!(kinds.iter().any(|(k, _)| k == &IndexKind::new("test_stub")));
}
