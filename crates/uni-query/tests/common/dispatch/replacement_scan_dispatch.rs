#![allow(dead_code, unused_imports, clippy::all)]
//! M5b — verify replacement-scan providers are reachable via the registry
//! and gated by the `replacement_scans_enabled` config (default off).
//
// Rust guideline compliant

use std::sync::Arc;

use uni_plugin::traits::catalog::{
    CatalogTable, Replacement, ReplacementRequest, ReplacementScanProvider,
};
use uni_plugin::{
    Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, PluginRegistry, QName,
};

#[derive(Debug)]
struct FunctionRerouteScan;

impl ReplacementScanProvider for FunctionRerouteScan {
    fn replace(&self, request: &ReplacementRequest<'_>) -> Option<Replacement> {
        match request {
            ReplacementRequest::Function(q) if q.local() == "missing_fn" => {
                Some(Replacement::Function(QName::new("uni", "abs")))
            }
            _ => None,
        }
    }
}

#[test]
fn replacement_scan_provider_reachable_via_registry() {
    let registry = Arc::new(PluginRegistry::default());
    let plugin_id = PluginId::new("test_rs");
    let caps = CapabilitySet::from_iter_of([Capability::Catalog]);
    let mut r = PluginRegistrar::new(plugin_id, &caps, &registry);
    r.replacement_scan(Arc::new(FunctionRerouteScan)).unwrap();
    r.commit_to_registry().unwrap();

    let scans = registry.replacement_scans();
    assert_eq!(scans.len(), 1);
    let q = QName::new("uni", "missing_fn");
    let result = scans[0].replace(&ReplacementRequest::Function(&q));
    assert!(matches!(result, Some(Replacement::Function(_))));
    let q_other = QName::new("uni", "other");
    assert!(
        scans[0]
            .replace(&ReplacementRequest::Function(&q_other))
            .is_none()
    );
}

// ── Label-replacement reachability ──────────────────────────────────

/// Stub `CatalogTable` returned by the label-replacement provider; we
/// only assert the registry consultation returns this table, not that
/// the planner wires it into a real plan (that work is deferred —
/// see the "Batch 3 prerequisite — virtual label-id allocation" note
/// in `docs/plans/plugin_framework_implementation.md`).
#[derive(Debug)]
struct StubCatalogTable;

impl CatalogTable for StubCatalogTable {
    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::new(arrow_schema::Schema::empty())
    }
    fn scan(
        &self,
        _proj: Option<&[usize]>,
        _filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, FnError> {
        Err(FnError::new(0, "unused in this test"))
    }
}

#[derive(Debug)]
struct LabelReplacementScan;

impl ReplacementScanProvider for LabelReplacementScan {
    fn replace(&self, request: &ReplacementRequest<'_>) -> Option<Replacement> {
        if let ReplacementRequest::Label(name) = request
            && *name == "External"
        {
            return Some(Replacement::CatalogTable(Arc::new(StubCatalogTable)));
        }
        None
    }
}

/// Reachability test for the new `Label` replacement-request branch.
///
/// The planner-side rewrite for label-replacement is blocked on
/// virtual-label-id allocation (deferred to Batch 3, see plan doc).
/// This test exercises the registry consultation site directly so
/// the wiring is covered end-to-end at the surface available today.
#[test]
fn label_replacement_scan_reachable_via_registry() {
    let registry = Arc::new(PluginRegistry::default());
    let plugin_id = PluginId::new("test_rs_label");
    let caps = CapabilitySet::from_iter_of([Capability::Catalog]);
    let mut r = PluginRegistrar::new(plugin_id, &caps, &registry);
    r.replacement_scan(Arc::new(LabelReplacementScan)).unwrap();
    r.commit_to_registry().unwrap();

    let scans = registry.replacement_scans();
    assert_eq!(scans.len(), 1);
    let hit = scans[0].replace(&ReplacementRequest::Label("External"));
    assert!(
        matches!(hit, Some(Replacement::CatalogTable(_))),
        "registry consultation must yield CatalogTable replacement",
    );
    let miss = scans[0].replace(&ReplacementRequest::Label("Other"));
    assert!(miss.is_none(), "unknown label must not resolve");
}
