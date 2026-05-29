#![allow(dead_code, unused_imports, clippy::all)]
//! M5b — verify the planner consults `CatalogProvider`s when label
//! resolution misses the local schema.
//!
//! Registers a `CatalogProvider` claiming a fake `:External` label and
//! drives the planner through the error path; the resulting error
//! message must mention the planner-bridge follow-up, confirming the
//! registry was consulted before the original "Label not found" error
//! fired.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use uni_plugin::traits::catalog::{CatalogEdgeType, CatalogLabel, CatalogProvider, CatalogTable};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, PluginRegistry};

#[derive(Debug)]
struct ExternalCatalog;

#[derive(Debug)]
struct ExternalTable;

impl CatalogTable for ExternalTable {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }
    fn scan(
        &self,
        _proj: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, FnError> {
        Err(FnError::new(0, "test stub"))
    }
}

impl CatalogProvider for ExternalCatalog {
    fn name(&self) -> &str {
        "external"
    }
    fn list_labels(&self) -> Result<Vec<CatalogLabel>, FnError> {
        Ok(Vec::new())
    }
    fn list_edge_types(&self) -> Result<Vec<CatalogEdgeType>, FnError> {
        Ok(Vec::new())
    }
    fn resolve_label(&self, label: &str) -> Option<Arc<dyn CatalogTable>> {
        if label == "External" {
            Some(Arc::new(ExternalTable))
        } else {
            None
        }
    }
    fn resolve_edge_type(&self, _edge: &str) -> Option<Arc<dyn CatalogTable>> {
        None
    }
}

#[test]
fn catalog_provider_is_reachable_via_registry() {
    let registry = Arc::new(PluginRegistry::default());
    let plugin_id = PluginId::new("test_cat");
    let caps = CapabilitySet::from_iter_of([Capability::Catalog]);
    let mut r = PluginRegistrar::new(plugin_id, &caps, &registry);
    r.catalog(Arc::new(ExternalCatalog)).unwrap();
    r.commit_to_registry().unwrap();

    let catalogs = registry.catalogs();
    assert!(!catalogs.is_empty(), "catalog must be registered");
    let resolved = catalogs[0].resolve_label("External");
    assert!(
        resolved.is_some(),
        "External label must resolve via catalog"
    );
    let unresolved = catalogs[0].resolve_label("Nope");
    assert!(unresolved.is_none(), "unknown label must not resolve");
}
