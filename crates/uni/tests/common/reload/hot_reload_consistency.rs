#![allow(dead_code, unused_imports, clippy::all)]
//! M10 acceptance test: `Uni::reload` swaps a plugin's surface
//! atomically without breaking queries already in flight.
//!
//! The "10s wall-clock in-flight unaffected" requirement is guaranteed
//! by arc-swap: any caller that captured `Arc<dyn ScalarPluginFn>`
//! before the registry swap continues to execute against that Arc, so
//! mid-flight observers cannot tear. This test exercises the
//! observable side of that contract — pre-reload lookups still resolve
//! to the old function object even after the registry has handed out
//! the new one — and verifies the new plugin's `shutdown()` runs on
//! removal.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::scalar::ScalarValue;
use uni_db::api::Uni;
use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling, ScalarPluginFn};
use uni_plugin::{
    Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginId, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, QName, Scope, SideEffects,
};

// ── Mock plugin: a scalar fn returning a configurable constant ──────────

struct ConstScalar {
    value: i64,
    signature: OnceLock<FnSignature>,
}

impl ConstScalar {
    fn new(value: i64) -> Self {
        Self {
            value,
            signature: OnceLock::new(),
        }
    }
}

impl ScalarPluginFn for ConstScalar {
    fn signature(&self) -> &FnSignature {
        self.signature.get_or_init(|| FnSignature {
            args: vec![ArgType::Primitive(DataType::Int64)],
            returns: ArgType::Primitive(DataType::Int64),
            volatility: Volatility::Immutable,
            null_handling: NullHandling::PropagateNulls,
        })
    }
    fn invoke(&self, _args: &[ColumnarValue], _rows: usize) -> Result<ColumnarValue, FnError> {
        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(self.value))))
    }
}

struct ConstPlugin {
    value: i64,
    shutdown_called: Arc<AtomicBool>,
    manifest: OnceLock<PluginManifest>,
}

impl ConstPlugin {
    fn new(value: i64, shutdown_called: Arc<AtomicBool>) -> Self {
        Self {
            value,
            shutdown_called,
            manifest: OnceLock::new(),
        }
    }
}

impl Plugin for ConstPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: PluginId::new("test.const"),
            version: "0.1.0".parse().expect("static version"),
            abi: uni_plugin::AbiRange::parse("^1").unwrap(),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::ScalarFn]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "M10 hot-reload consistency test plugin".to_owned(),
            metadata: BTreeMap::new(),
        })
    }
    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        let sig = FnSignature {
            args: vec![ArgType::Primitive(DataType::Int64)],
            returns: ArgType::Primitive(DataType::Int64),
            volatility: Volatility::Immutable,
            null_handling: NullHandling::PropagateNulls,
        };
        r.scalar_fn(
            QName::new("test.const", "value"),
            sig,
            Arc::new(ConstScalar::new(self.value)),
        )?;
        Ok(())
    }
    fn shutdown(&self) {
        self.shutdown_called.store(true, Ordering::SeqCst);
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn add_plugin_then_lookup_resolves() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let shutdown_called = Arc::new(AtomicBool::new(false));
    db.add_plugin(ConstPlugin::new(1, Arc::clone(&shutdown_called)))?;
    let handle = db.plugin(&PluginId::new("test.const")).expect("installed");
    assert_eq!(handle.generation, 0);
    let entry = db
        .plugin_registry()
        .scalar_fn(&QName::new("test.const", "value"))
        .expect("scalar fn registered");
    assert_eq!(entry.plugin.as_str(), "test.const");
    Ok(())
}

#[tokio::test]
async fn reload_bumps_generation_and_runs_old_shutdown() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let shutdown_v1 = Arc::new(AtomicBool::new(false));
    db.add_plugin(ConstPlugin::new(1, Arc::clone(&shutdown_v1)))?;

    let handle = db.plugin(&PluginId::new("test.const")).expect("installed");
    assert_eq!(handle.generation, 0);

    let shutdown_v2 = Arc::new(AtomicBool::new(false));
    let new_handle = db.reload(&handle, ConstPlugin::new(2, Arc::clone(&shutdown_v2)))?;
    assert_eq!(new_handle.generation, 1, "generation increments per reload");
    assert!(
        shutdown_v1.load(Ordering::SeqCst),
        "v1 plugin's shutdown should have run after reload"
    );
    assert!(
        !shutdown_v2.load(Ordering::SeqCst),
        "v2 plugin's shutdown should NOT have run (it's the live one)"
    );

    // New plugin's scalar fn entry is what the registry now serves.
    let entry = db
        .plugin_registry()
        .scalar_fn(&QName::new("test.const", "value"))
        .expect("scalar fn still registered after reload");
    assert_eq!(entry.plugin.as_str(), "test.const");
    Ok(())
}

#[tokio::test]
async fn reload_rejects_stale_handle() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let shutdown = Arc::new(AtomicBool::new(false));
    db.add_plugin(ConstPlugin::new(1, Arc::clone(&shutdown)))?;
    let handle = db.plugin(&PluginId::new("test.const")).expect("installed");
    let _new = db.reload(
        &handle,
        ConstPlugin::new(2, Arc::new(AtomicBool::new(false))),
    )?;
    // `handle` is now stale (generation 0; live is 1).
    let stale_attempt = db.reload(
        &handle,
        ConstPlugin::new(3, Arc::new(AtomicBool::new(false))),
    );
    assert!(stale_attempt.is_err(), "reload should reject stale handle");
    Ok(())
}

#[tokio::test]
async fn remove_plugin_evicts_surface_and_runs_shutdown() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let shutdown = Arc::new(AtomicBool::new(false));
    db.add_plugin(ConstPlugin::new(1, Arc::clone(&shutdown)))?;
    let handle = db.plugin(&PluginId::new("test.const")).expect("installed");
    db.remove_plugin(&handle)?;
    assert!(shutdown.load(Ordering::SeqCst));
    assert!(
        db.plugin_registry()
            .scalar_fn(&QName::new("test.const", "value"))
            .is_none(),
        "scalar fn should be evicted on remove"
    );
    assert!(db.plugin(&PluginId::new("test.const")).is_none());
    Ok(())
}

#[tokio::test]
async fn in_flight_arc_keeps_old_function_alive_through_reload() -> anyhow::Result<()> {
    // The arc-swap guarantee: an Arc captured before the swap continues
    // to point at the old function object. This is the load-bearing
    // invariant for "10s wall-clock query unaffected by reload".
    let db = Uni::temporary().build().await?;
    db.add_plugin(ConstPlugin::new(1, Arc::new(AtomicBool::new(false))))?;
    let handle = db.plugin(&PluginId::new("test.const")).expect("installed");

    // Capture the old entry before reload.
    let captured = db
        .plugin_registry()
        .scalar_fn(&QName::new("test.const", "value"))
        .expect("entry present");
    let captured_fn = Arc::clone(&captured.function);

    // Reload to value=2.
    db.reload(
        &handle,
        ConstPlugin::new(2, Arc::new(AtomicBool::new(false))),
    )?;

    // The captured Arc still resolves to value=1 (the original
    // function object). Any in-flight query that already had this Arc
    // sees consistent output.
    let result = captured_fn
        .invoke(&[ColumnarValue::Scalar(ScalarValue::Int64(Some(0)))], 1)
        .expect("invoke");
    match result {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
            assert_eq!(v, 1, "captured Arc should still produce v1's output");
        }
        other => panic!("unexpected result: {other:?}"),
    }

    // The registry now hands out the new function object.
    let after = db
        .plugin_registry()
        .scalar_fn(&QName::new("test.const", "value"))
        .expect("entry present after reload");
    let result = after
        .function
        .invoke(&[ColumnarValue::Scalar(ScalarValue::Int64(Some(0)))], 1)
        .expect("invoke");
    match result {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
            assert_eq!(v, 2, "new lookup should produce v2's output");
        }
        other => panic!("unexpected result: {other:?}"),
    }
    Ok(())
}
