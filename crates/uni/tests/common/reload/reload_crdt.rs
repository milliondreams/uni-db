#![allow(dead_code, unused_imports, clippy::all)]
//! M10 reload discipline for `CrdtKindProvider`.
//!
//! Per §11.2.1 a CRDT reload requires schema compatibility: the new
//! provider's `from_persisted()` must accept bytes produced by the
//! old's `persist()`. An incompatible new provider is a hard reload
//! error.
//!
//! This test installs a CRDT plugin, then reloads to an incompatible
//! plugin and asserts the typed `UniError::InvalidArgument` carrying
//! the SchemaIncompat reason surfaces and the original plugin remains
//! installed.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;

use datafusion::scalar::ScalarValue;
use uni_db::api::Uni;
use uni_plugin::traits::crdt::{CrdtKind, CrdtKindProvider, CrdtOp, CrdtState};
use uni_plugin::{
    Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginId, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, Scope, SideEffects,
};

// ── Two providers under the same kind ─────────────────────────────

#[derive(Default)]
struct EmptyState;

impl CrdtState for EmptyState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, _op: &CrdtOp) -> Result<(), FnError> {
        Ok(())
    }
    fn merge(&mut self, _other: &dyn CrdtState) -> Result<(), FnError> {
        Ok(())
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Null)
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        Ok(vec![1, 2, 3])
    }
}

struct AcceptProvider;
impl CrdtKindProvider for AcceptProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("test-incompat")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(EmptyState)
    }
    fn from_persisted(&self, _bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        Ok(Box::new(EmptyState))
    }
}

struct RejectProvider;
impl CrdtKindProvider for RejectProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("test-incompat")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(EmptyState)
    }
    fn from_persisted(&self, _bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        Err(FnError::new(0x701, "incompatible schema"))
    }
}

struct CrdtPlugin<P: CrdtKindProvider + 'static> {
    provider: Arc<P>,
    manifest: OnceLock<PluginManifest>,
}

impl<P: CrdtKindProvider + 'static> CrdtPlugin<P> {
    fn new(provider: P) -> Self {
        Self {
            provider: Arc::new(provider),
            manifest: OnceLock::new(),
        }
    }
}

impl<P: CrdtKindProvider + 'static> Plugin for CrdtPlugin<P> {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: PluginId::new("test.crdt-reload"),
            version: "0.1.0".parse().expect("static version"),
            abi: uni_plugin::AbiRange::parse("^1").unwrap(),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::Crdt]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "M10 crdt-reload test".to_owned(),
            metadata: BTreeMap::new(),
        })
    }
    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.crdt_kind(
            CrdtKind::new("test-incompat"),
            Arc::clone(&self.provider) as Arc<dyn CrdtKindProvider>,
        )?;
        Ok(())
    }
}

#[tokio::test]
async fn incompatible_crdt_reload_is_rejected() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    db.add_plugin(CrdtPlugin::new(AcceptProvider))?;
    let handle = db
        .plugin(&PluginId::new("test.crdt-reload"))
        .expect("installed");

    // Reload to the rejecting provider: schema_compat_check fails.
    let res = db.reload(&handle, CrdtPlugin::new(RejectProvider));
    let err = res.err().expect("reload should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("compat") || msg.contains("incompatible") || msg.contains("incompat"),
        "expected compat rejection, got: {msg}"
    );

    // Old plugin remains active.
    let still_installed = db
        .plugin(&PluginId::new("test.crdt-reload"))
        .expect("old plugin should still be installed");
    assert_eq!(still_installed.generation, 0);
    assert!(
        db.plugin_registry()
            .crdt_kind(&CrdtKind::new("test-incompat"))
            .is_some(),
        "CRDT provider should still be registered after rejected reload"
    );
    Ok(())
}

#[tokio::test]
async fn compatible_crdt_reload_swaps_provider() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    db.add_plugin(CrdtPlugin::new(AcceptProvider))?;
    let handle = db
        .plugin(&PluginId::new("test.crdt-reload"))
        .expect("installed");

    // Reload to a second accepting provider — round-trip passes.
    let new_handle = db.reload(&handle, CrdtPlugin::new(AcceptProvider))?;
    assert_eq!(new_handle.generation, 1);
    Ok(())
}
