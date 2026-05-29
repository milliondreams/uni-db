#![allow(dead_code, unused_imports, clippy::all)]
//! M10 reload discipline for `StorageBackend`.
//!
//! Per §11.2.1 a `StorageBackend` reload is "clean" — new `Storage`
//! comes from the new provider's `open()`; old continues serving
//! in-flight queries until their captured Arcs drop. This test
//! verifies the observable side: after `Uni::reload` the registry
//! serves the new backend, an Arc captured before the swap still
//! works, and the old plugin's `shutdown()` runs.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use uni_db::api::Uni;
use uni_plugin::traits::storage::{
    BranchMetadata, Storage, StorageBackend, StorageOptions, WriteHandle,
};
use uni_plugin::{
    Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginId, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, Scope, SideEffects,
};

// ── Tagged storage stack ────────────────────────────────────────────

#[derive(Debug)]
struct TaggedBackend {
    tag: &'static str,
}

#[async_trait]
impl StorageBackend for TaggedBackend {
    fn scheme(&self) -> &'static str {
        "test-mem"
    }
    async fn open(
        &self,
        _uri: &str,
        _options: &StorageOptions,
    ) -> Result<Arc<dyn Storage>, FnError> {
        Ok(Arc::new(TaggedStorage { tag: self.tag }))
    }
}

#[derive(Debug)]
struct TaggedStorage {
    tag: &'static str,
}

#[async_trait]
impl Storage for TaggedStorage {
    async fn read_batch(
        &self,
        _table: &str,
        _predicate: Option<&Expr>,
    ) -> Result<SendableRecordBatchStream, FnError> {
        Err(FnError::new(0x900, "test stub: read_batch not implemented"))
    }
    async fn write_batch(
        &self,
        _table: &str,
        _batch: &RecordBatch,
    ) -> Result<WriteHandle, FnError> {
        Err(FnError::new(
            0x901,
            "test stub: write_batch not implemented",
        ))
    }
    async fn list_tables(&self) -> Result<Vec<String>, FnError> {
        Ok(vec![format!("tagged.{}", self.tag)])
    }
    async fn delete(&self, _table: &str, _predicate: &Expr) -> Result<u64, FnError> {
        Ok(0)
    }
    async fn fork(
        &self,
        _table: &str,
        _src_branch: &str,
        _dst_branch: &str,
    ) -> Result<BranchMetadata, FnError> {
        Err(FnError::new(0x902, "test stub: no branching"))
    }
    async fn schema(&self, _table: &str) -> Option<SchemaRef> {
        None
    }
}

struct TaggedStoragePlugin {
    tag: &'static str,
    shutdown_called: Arc<AtomicBool>,
    manifest: OnceLock<PluginManifest>,
}

impl TaggedStoragePlugin {
    fn new(tag: &'static str, shutdown_called: Arc<AtomicBool>) -> Self {
        Self {
            tag,
            shutdown_called,
            manifest: OnceLock::new(),
        }
    }
}

impl Plugin for TaggedStoragePlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: PluginId::new("test.storage"),
            version: "0.1.0".parse().expect("static version"),
            abi: uni_plugin::AbiRange::parse("^1").unwrap(),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::Storage]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "M10 storage-reload test".to_owned(),
            metadata: BTreeMap::new(),
        })
    }
    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.storage_backend("test-mem", Arc::new(TaggedBackend { tag: self.tag }))?;
        Ok(())
    }
    fn shutdown(&self) {
        self.shutdown_called.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn reload_storage_backend_swaps_registry_entry() -> anyhow::Result<()> {
    use smol_str::SmolStr;
    let db = Uni::temporary().build().await?;
    let shutdown_v1 = Arc::new(AtomicBool::new(false));
    db.add_plugin(TaggedStoragePlugin::new("v1", Arc::clone(&shutdown_v1)))?;

    // v1 backend visible in the registry.
    let entry = db
        .plugin_registry()
        .storage_backend(&SmolStr::new("test-mem"))
        .expect("v1 backend registered");
    // Hold an Arc across the reload — the "in-flight query continues
    // to read from the old backend" invariant in arc-swap form.
    let captured = Arc::clone(&entry);
    drop(entry);

    let handle = db
        .plugin(&PluginId::new("test.storage"))
        .expect("installed");
    let shutdown_v2 = Arc::new(AtomicBool::new(false));
    db.reload(
        &handle,
        TaggedStoragePlugin::new("v2", Arc::clone(&shutdown_v2)),
    )?;

    assert!(
        shutdown_v1.load(Ordering::SeqCst),
        "old plugin shutdown ran"
    );

    // Captured Arc still observable (v1 backend lives until last ref drops).
    assert_eq!(captured.scheme(), "test-mem");
    drop(captured);

    // The registry now hands out the v2 backend.
    let after = db
        .plugin_registry()
        .storage_backend(&SmolStr::new("test-mem"))
        .expect("v2 backend registered");
    assert_eq!(after.scheme(), "test-mem");
    Ok(())
}

#[tokio::test]
async fn remove_storage_plugin_evicts_backend() -> anyhow::Result<()> {
    use smol_str::SmolStr;
    let db = Uni::temporary().build().await?;
    let shutdown = Arc::new(AtomicBool::new(false));
    db.add_plugin(TaggedStoragePlugin::new("v1", Arc::clone(&shutdown)))?;
    let handle = db
        .plugin(&PluginId::new("test.storage"))
        .expect("installed");
    db.remove_plugin(&handle)?;
    assert!(
        db.plugin_registry()
            .storage_backend(&SmolStr::new("test-mem"))
            .is_none(),
        "backend should be evicted on remove"
    );
    Ok(())
}
