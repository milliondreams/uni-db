#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/impl_query.rs:757 (finding [9]).
//!
//! `execute_internal_with_config_and_token` builds its `QueryPlanner` WITHOUT
//! `.with_plugin_registry(...)` (line 756-757); every other planner
//! construction in the file attaches it. The plugin-catalog features
//! (`allocate_virtual_label`, replacement scans, virtual-label write
//! rejection) all guard on `self.plugin_registry` — with it `None`, a virtual
//! (catalog-backed) label never resolves. The session task-local registry does
//! NOT compensate because the planner reads only `self.plugin_registry`.
//!
//! This path is reached whenever a query sets a timeout / max_memory /
//! cancellation token (`has_overrides`). So the SAME `MATCH (n:External)` that
//! returns catalog rows on the default cached path returns ZERO rows once a
//! `.timeout(...)` override forces the config-aware path.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use uni_db::Uni;
use uni_plugin::traits::catalog::{CatalogEdgeType, CatalogLabel, CatalogProvider, CatalogTable};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};

#[derive(Debug)]
struct InMemoryCatalogTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl InMemoryCatalogTable {
    fn vertex_fixture() -> Arc<Self> {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("foo", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    Some("beta"),
                    Some("gamma"),
                ])),
            ],
        )
        .expect("vertex fixture batch");
        Arc::new(Self {
            schema,
            batches: vec![batch],
        })
    }
}

impl CatalogTable for InMemoryCatalogTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        _projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, FnError> {
        let batches: Vec<_> = self.batches.iter().cloned().map(Ok).collect();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream::iter(batches),
        )))
    }
}

#[derive(Debug)]
struct ExternalCatalog {
    vertex: Arc<InMemoryCatalogTable>,
}

impl CatalogProvider for ExternalCatalog {
    fn name(&self) -> &str {
        "external"
    }
    fn list_labels(&self) -> Result<Vec<CatalogLabel>, FnError> {
        Ok(vec![CatalogLabel {
            name: "External".into(),
            doc: String::new(),
        }])
    }
    fn list_edge_types(&self) -> Result<Vec<CatalogEdgeType>, FnError> {
        Ok(vec![])
    }
    fn resolve_label(&self, label: &str) -> Option<Arc<dyn CatalogTable>> {
        if label == "External" {
            Some(Arc::clone(&self.vertex) as Arc<dyn CatalogTable>)
        } else {
            None
        }
    }
    fn resolve_edge_type(&self, _edge: &str) -> Option<Arc<dyn CatalogTable>> {
        None
    }
}

async fn db_with_catalog() -> Uni {
    let db = Uni::temporary().build().await.expect("temporary db builds");
    let catalog = Arc::new(ExternalCatalog {
        vertex: InMemoryCatalogTable::vertex_fixture(),
    });
    let caps = CapabilitySet::from_iter_of([Capability::Catalog]);
    let mut r = PluginRegistrar::new(
        PluginId::new("test_external_catalog_9"),
        &caps,
        db.plugin_registry(),
    );
    r.catalog(catalog as Arc<dyn CatalogProvider>)
        .expect("catalog cap satisfies catalog()");
    r.commit_to_registry().expect("registry accepts catalog");
    db
}

#[tokio::test]
async fn config_override_path_drops_virtual_label_resolution() -> anyhow::Result<()> {
    let db = db_with_catalog().await;

    // Default (cached) path: planner is built WITH the plugin registry, so the
    // virtual label "External" resolves to catalog rows.
    let default_path = db
        .session()
        .query("MATCH (n:External) RETURN n.foo AS f")
        .await?;
    assert_eq!(
        default_path.len(),
        3,
        "control: default path resolves the virtual label (3 catalog rows)"
    );

    // Config-override path: a `.timeout(...)` forces
    // execute_internal_with_config_and_token, which builds the planner WITHOUT
    // `.with_plugin_registry(...)`.
    let override_path = db
        .session()
        .query_with("MATCH (n:External) RETURN n.foo AS f")
        .timeout(Duration::from_secs(30))
        .fetch_all()
        .await;

    let override_rows = override_path
        .expect("config-override path must succeed and resolve the virtual label")
        .len();

    // FIXED (impl_query.rs): the config-override planner now attaches the plugin
    // registry, so the virtual label resolves to the same 3 catalog rows as the
    // default cached path.
    assert_eq!(
        override_rows, 3,
        "config-override path must resolve the virtual label to 3 catalog rows, got {override_rows}"
    );

    db.shutdown().await?;
    Ok(())
}
