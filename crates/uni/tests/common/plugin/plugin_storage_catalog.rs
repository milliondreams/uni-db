#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5h end-to-end — register a plugin `Storage` via the
//! `StorageCatalogTable` adapter and reach it through a Cypher
//! `MATCH (n:External) RETURN n.foo`.
//!
//! Confirms the bridge added in `uni-plugin::adapters::catalog_from_storage`
//! actually makes plugin `Storage::read_batch` reachable from the
//! graph planner via the existing virtual-label catalog dispatch.
//! Without the adapter, plugin storage is dead weight; this test is
//! the load-bearing acceptance for the workstream.

// Rust guideline compliant

use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use uni_db::Uni;
use uni_plugin::adapters::StorageCatalogTable;
use uni_plugin::traits::catalog::{CatalogEdgeType, CatalogLabel, CatalogProvider, CatalogTable};
use uni_plugin::traits::storage::{Storage, WriteHandle};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};

// ── Fake plugin Storage ────────────────────────────────────────────────

/// Records each `read_batch` call so the test can assert the planner
/// reached the plugin Storage rather than serving from a stub.
#[derive(Debug, Default)]
struct StorageCalls {
    read_batch_count: usize,
    last_table: Option<String>,
}

struct FakeStorage {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    calls: Mutex<StorageCalls>,
}

impl std::fmt::Debug for FakeStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeStorage")
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl Storage for FakeStorage {
    async fn read_batch(
        &self,
        table: &str,
        _predicate: Option<&Expr>,
    ) -> Result<SendableRecordBatchStream, FnError> {
        {
            let mut g = self.calls.lock().expect("calls mutex");
            g.read_batch_count += 1;
            g.last_table = Some(table.to_owned());
        }
        let batches: Vec<_> = self.batches.iter().cloned().map(Ok).collect();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream::iter(batches),
        )))
    }

    async fn write_batch(
        &self,
        _table: &str,
        _batch: &RecordBatch,
    ) -> Result<WriteHandle, FnError> {
        Err(FnError::new(1, "fake storage is read-only"))
    }

    async fn list_tables(&self) -> Result<Vec<String>, FnError> {
        Ok(vec!["people".to_owned()])
    }

    async fn delete(&self, _table: &str, _predicate: &Expr) -> Result<u64, FnError> {
        Err(FnError::new(1, "fake storage is read-only"))
    }
}

// ── CatalogProvider that wraps the FakeStorage via the adapter ─────────

struct StorageBackedCatalog {
    table: Arc<dyn CatalogTable>,
    storage: Arc<FakeStorage>,
}

impl std::fmt::Debug for StorageBackedCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageBackedCatalog")
            .field("storage", &self.storage)
            .finish()
    }
}

impl StorageBackedCatalog {
    fn new() -> Arc<Self> {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("foo", DataType::Utf8, true),
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
        .expect("fixture batch builds");

        let storage = Arc::new(FakeStorage {
            schema: schema.clone(),
            batches: vec![batch],
            calls: Mutex::new(StorageCalls::default()),
        });
        let adapter = Arc::new(StorageCatalogTable::new(
            (Arc::clone(&storage)) as Arc<dyn Storage>,
            "people".to_owned(),
            schema,
        ));
        Arc::new(Self {
            table: adapter as Arc<dyn CatalogTable>,
            storage,
        })
    }
}

impl CatalogProvider for StorageBackedCatalog {
    fn name(&self) -> &str {
        "storage_backed"
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
            Some(Arc::clone(&self.table))
        } else {
            None
        }
    }
    fn resolve_edge_type(&self, _edge: &str) -> Option<Arc<dyn CatalogTable>> {
        None
    }
}

// ── Test harness ───────────────────────────────────────────────────────

fn register_catalog(
    uni: &Uni,
    catalog: Arc<dyn CatalogProvider>,
) -> std::result::Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Catalog]);
    let mut r = PluginRegistrar::new(PluginId::new("test-storage-catalog"), &caps, registry);
    r.catalog(catalog)?;
    r.commit_to_registry()?;
    Ok(())
}

#[tokio::test]
async fn match_against_storage_backed_virtual_label_streams_rows() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let catalog = StorageBackedCatalog::new();
    let storage = Arc::clone(&catalog.storage);
    register_catalog(&db, catalog as Arc<dyn CatalogProvider>)?;

    let rows = db
        .session()
        .query("MATCH (n:External) RETURN n.foo AS foo")
        .await?;

    let foos: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("foo").ok())
        .collect();
    assert_eq!(foos, vec!["alpha", "beta", "gamma"]);

    let calls = storage.calls.lock().expect("calls mutex");
    assert!(
        calls.read_batch_count >= 1,
        "planner must have reached Storage::read_batch (count = {})",
        calls.read_batch_count
    );
    assert_eq!(calls.last_table.as_deref(), Some("people"));
    Ok(())
}
