// Rust guideline compliant
//! Verify the `declared_by` snapshot M9 advertises as "capability
//! inheritance via declarer id snapshot."
//!
//! `Principal` (`crates/uni-plugin/src/traits/connector.rs:54`) today
//! carries only `id` + `groups` — no capability set. The corresponding
//! enforced-denial path ("principal lacks `Capability::ProcedureWrites`
//! → declareProcedure denied") needs Principal-side infrastructure
//! that does not exist; that lane is **outside M9 scope** (the same
//! constraint is noted in the M9 status block).
//!
//! What M9 actually delivers is the *snapshot*: the declaring
//! principal's `id` is recorded in `DeclaredPlugin.declared_by` so
//! audit / future enforcement layers can trace who declared what. This
//! file pins that contract.

use std::sync::Arc;

use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use uni_plugin::PluginRegistry;
use uni_plugin::traits::connector::Principal;
use uni_plugin::traits::procedure::{ProcedureContext, ProcedurePlugin};
use uni_plugin_custom::procedures::DeclareFunctionProcedure;
use uni_plugin_custom::{CustomPlugin, NullPersistence, Persistence};

async fn drive(
    procedure: &DeclareFunctionProcedure,
    args: Vec<ColumnarValue>,
    ctx: ProcedureContext<'_>,
) {
    let mut stream = procedure.invoke(ctx, &args).expect("invoke");
    while let Some(b) = stream.next().await {
        b.expect("batch");
    }
}

fn args_for(qname: &str) -> Vec<ColumnarValue> {
    vec![
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(qname.to_owned()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("$x".to_owned()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("string".to_owned()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"["x"]"#.to_owned()))),
    ]
}

#[tokio::test]
async fn declared_by_records_principal_id() {
    let registry = Arc::new(PluginRegistry::new());
    let persistence: Arc<dyn Persistence> = Arc::new(NullPersistence);
    let custom = CustomPlugin::new(Arc::clone(&registry), Arc::clone(&persistence)).unwrap();
    let declare = DeclareFunctionProcedure::new(
        Arc::clone(custom.store()),
        Arc::clone(&persistence),
        Arc::clone(custom.registry()),
    );

    let alice = Principal {
        id: "alice".to_owned(),
        groups: vec![],
        capabilities: uni_plugin::CapabilitySet::default(),
    };
    let ctx = ProcedureContext::new().with_principal(&alice);
    drive(&declare, args_for("alice.greet"), ctx).await;

    let entry = custom
        .store()
        .get("alice.greet")
        .expect("declaration recorded");
    assert_eq!(entry.declared_by, "alice");
}

#[tokio::test]
async fn declared_by_defaults_to_anonymous_without_principal() {
    let registry = Arc::new(PluginRegistry::new());
    let persistence: Arc<dyn Persistence> = Arc::new(NullPersistence);
    let custom = CustomPlugin::new(Arc::clone(&registry), Arc::clone(&persistence)).unwrap();
    let declare = DeclareFunctionProcedure::new(
        Arc::clone(custom.store()),
        Arc::clone(&persistence),
        Arc::clone(custom.registry()),
    );

    let ctx = ProcedureContext::new();
    drive(&declare, args_for("anon.greet"), ctx).await;

    let entry = custom
        .store()
        .get("anon.greet")
        .expect("declaration recorded");
    assert_eq!(entry.declared_by, "anonymous");
}
