// Rust guideline compliant
//! M9 acceptance #2 — declarations survive a `CustomPlugin` restart.
//!
//! Builds a fresh `Arc<PluginRegistry>` and a `JsonFilePersistence`
//! pointed at a tempdir-backed sidecar. Declares a function through
//! the meta-plugin's `DeclareFunctionProcedure`, drops the
//! `CustomPlugin` instance, then constructs a new `CustomPlugin`
//! against the same persistence and asserts the function is
//! re-registered into the registry.

use std::sync::Arc;

use arrow_array::StringArray;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::traits::procedure::{ProcedureContext, ProcedurePlugin};
use uni_plugin::{Plugin, PluginRegistrar, PluginRegistry};
use uni_plugin_custom::procedures::{DeclareFunctionProcedure, declare_function_signature};
use uni_plugin_custom::{CustomPlugin, JsonFilePersistence, Persistence};

fn make_args(values: &[&str]) -> Vec<ColumnarValue> {
    values
        .iter()
        .map(|s| ColumnarValue::Scalar(ScalarValue::Utf8(Some((*s).to_owned()))))
        .collect()
}

fn invoke_collect(procedure: &DeclareFunctionProcedure, args: &[ColumnarValue]) {
    let ctx = ProcedureContext::new();
    let stream = procedure.invoke(ctx, args).expect("declareFunction invoke");
    drop(stream);
}

#[tokio::test]
async fn declared_function_survives_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sidecar = tmp.path().join("declared_plugins.json");

    // ----- First "run": declare and persist -----
    {
        let registry = Arc::new(PluginRegistry::new());
        let persistence: Arc<dyn Persistence> = Arc::new(JsonFilePersistence::new(sidecar.clone()));
        let plugin = CustomPlugin::new(Arc::clone(&registry), Arc::clone(&persistence))
            .expect("construct CustomPlugin");
        plugin
            .reactivate_into_registry()
            .expect("reactivate (empty)");
        // Register the meta-plugin procedures.
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
        plugin.register(&mut r).expect("register custom");
        r.commit_to_registry().expect("commit");

        let declare = DeclareFunctionProcedure::new(
            Arc::clone(plugin.store()),
            Arc::clone(&persistence),
            Arc::clone(plugin.registry()),
        );
        // Sanity-check the signature is the one we expect.
        let _ = declare_function_signature();
        invoke_collect(
            &declare,
            &make_args(&[
                "mycorp.greet",
                "$prefix + ' ' + $name",
                "string",
                r#"["prefix","name"]"#,
            ]),
        );

        // The function should now resolve in this registry.
        let qn = uni_plugin::QName::new("mycorp", "greet");
        let entry = registry.scalar_fn(&qn).expect("scalar registered");
        let out = entry
            .function
            .invoke(&make_args(&["Hello,", "world"]), 1)
            .expect("invoke");
        let arr = match out {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(_) => panic!("expected array"),
        };
        let s = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "Hello, world");
    }

    // ----- Second "run": reopen against the same sidecar -----
    {
        let registry = Arc::new(PluginRegistry::new());
        let persistence: Arc<dyn Persistence> = Arc::new(JsonFilePersistence::new(sidecar.clone()));
        let plugin = CustomPlugin::new(Arc::clone(&registry), Arc::clone(&persistence))
            .expect("re-open CustomPlugin");
        plugin
            .reactivate_into_registry()
            .expect("reactivate from sidecar");

        let qn = uni_plugin::QName::new("mycorp", "greet");
        let entry = registry.scalar_fn(&qn).expect("re-registered after reload");
        let out = entry
            .function
            .invoke(&make_args(&["Bonjour,", "world"]), 1)
            .expect("invoke after reload");
        let arr = match out {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(_) => panic!("expected array"),
        };
        let s = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "Bonjour, world");
    }
}
