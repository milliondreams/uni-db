// Rust guideline compliant
//! Shadow-native test (forward direction).
//!
//! When a native scalar is already registered under qname `mycorp.foo`
//! and the user calls `uni.plugin.declareFunction('mycorp.foo', ...)`,
//! the declaration should still succeed but be marked `active=false`
//! in the [`DeclaredPluginStore`]; the native registration must continue
//! to resolve as the live scalar.
//!
//! # Reverse direction not covered
//!
//! The opposite direction (declare first, then a native plugin loaded
//! later claims the same qname) is *not* tested because the registry
//! has no `mark_declared_inactive` hook today — it would reject the
//! second registration with `DuplicateRegistration`, leaving the
//! declared record `active=true`. Wiring that direction is net-new
//! infrastructure outside M9 scope (see
//! `docs/plans/plugin_framework_implementation.md` M9 status block).

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use arrow_array::{BooleanArray, StringArray};
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::scalar::ScalarValue;
use semver::Version;
use uni_plugin::traits::procedure::{ProcedureContext, ProcedurePlugin};
use uni_plugin::traits::scalar::{ArgType, FnSignature, ScalarPluginFn};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginId,
    PluginManifest, PluginRegistrar, PluginRegistry, ProvidedSurfaces, QName, Scope, SideEffects,
};
use uni_plugin_custom::procedures::DeclareFunctionProcedure;
use uni_plugin_custom::{CustomPlugin, DeclaredPluginStore, NullPersistence, Persistence};

/// A native scalar that returns the literal `"native"` regardless of
/// inputs — used to verify it keeps dispatching after a declaration
/// attempts to shadow it.
#[derive(Debug)]
struct NativeMarker;

impl ScalarPluginFn for NativeMarker {
    fn signature(&self) -> &FnSignature {
        static SIG: OnceLock<FnSignature> = OnceLock::new();
        SIG.get_or_init(|| {
            FnSignature::new(
                vec![ArgType::Primitive(DataType::Utf8)],
                ArgType::Primitive(DataType::Utf8),
                Volatility::Immutable,
            )
        })
    }

    fn invoke(&self, _args: &[ColumnarValue], _rows: usize) -> Result<ColumnarValue, FnError> {
        Ok(ColumnarValue::Array(Arc::new(StringArray::from(vec![
            "native",
        ]))))
    }
}

struct NativePlugin {
    manifest: OnceLock<PluginManifest>,
}

impl NativePlugin {
    fn new() -> Self {
        Self {
            manifest: OnceLock::new(),
        }
    }

    fn manifest_value() -> PluginManifest {
        PluginManifest {
            id: PluginId::new("mycorp"),
            version: Version::new(0, 1, 0),
            abi: AbiRange::parse("^1").unwrap(),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::ScalarFn]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "Native shadow test plugin.".to_owned(),
            metadata: BTreeMap::new(),
        }
    }
}

impl std::fmt::Debug for NativePlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativePlugin").finish()
    }
}

impl Plugin for NativePlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(Self::manifest_value)
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.scalar_fn(
            QName::new("mycorp", "foo"),
            FnSignature::new(
                vec![ArgType::Primitive(DataType::Utf8)],
                ArgType::Primitive(DataType::Utf8),
                Volatility::Immutable,
            ),
            Arc::new(NativeMarker),
        )?;
        Ok(())
    }
}

fn collect_registered(
    stream_result: Result<datafusion::execution::SendableRecordBatchStream, FnError>,
) -> bool {
    let stream = stream_result.expect("declareFunction invoke");
    // We can't easily await here without runtime; instead spin up tokio.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    rt.block_on(async move {
        use futures::StreamExt;
        let mut stream = stream;
        let batch = stream
            .next()
            .await
            .expect("at least one batch")
            .expect("batch ok");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("bool col");
        col.value(0)
    })
}

#[test]
fn declared_function_shadowed_by_native() {
    let registry = Arc::new(PluginRegistry::new());

    // 1. Register the native scalar `mycorp.foo` first.
    let native = NativePlugin::new();
    let manifest = native.manifest().clone();
    let mut r = PluginRegistrar::new(manifest.id.clone(), &manifest.capabilities, &registry);
    native.register(&mut r).expect("native register");
    r.commit_to_registry().expect("native commit");

    // 2. Try to declare a function under the same qname.
    let persistence: Arc<dyn Persistence> = Arc::new(NullPersistence);
    let custom =
        CustomPlugin::new(Arc::clone(&registry), Arc::clone(&persistence)).expect("custom plugin");
    let declare = DeclareFunctionProcedure::new(
        Arc::clone(custom.store()),
        Arc::clone(&persistence),
        Arc::clone(custom.registry()),
    );
    let args: Vec<ColumnarValue> = vec![
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("mycorp.foo".to_owned()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("$x".to_owned()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("string".to_owned()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"["x"]"#.to_owned()))),
    ];
    let ctx = ProcedureContext::new();
    let registered = collect_registered(declare.invoke(ctx, &args));

    // Declare should report `registered=false` because the qname is
    // shadowed.
    assert!(!registered, "expected registered=false when native shadows");

    // Store should still hold the declaration as `active=false`.
    let entry = custom
        .store()
        .get("mycorp.foo")
        .expect("declaration recorded");
    assert!(!entry.active, "expected active=false; got {entry:?}");

    // Native scalar must continue to be the live registration.
    let qn = QName::new("mycorp", "foo");
    let live = registry.scalar_fn(&qn).expect("native still live");
    let out = live
        .function
        .invoke(
            &[ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "ignored".to_owned(),
            )))],
            1,
        )
        .expect("invoke native");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(_) => panic!("expected array from native"),
    };
    let s = arr.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s.value(0), "native");
}

// Use a tiny private store import just to ensure `DeclaredPluginStore`
// re-export keeps working — pure compile-time hygiene check.
#[allow(dead_code)]
fn _doc_store_re_export() -> Arc<DeclaredPluginStore> {
    Arc::new(DeclaredPluginStore::new())
}
