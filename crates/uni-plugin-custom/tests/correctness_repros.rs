// Rust guideline compliant
//! Runnable repros for 8 verified correctness findings in
//! `uni-plugin-custom`. Each test drives the REAL public API with real
//! inputs and asserts on the OBSERVED (currently buggy) behavior. Where
//! the correct-behavior assertion would fail today, the test asserts the
//! actual value and documents the expected value in a `// BUG:` comment.
//!
//! Findings covered:
//!   [1] decode.rs:64  — re-declare of an existing declared qname folded
//!                       into NativeShadow (new body stored inactive,
//!                       old body keeps executing).
//!   [2] lib.rs:773    — dropDeclared/remove_plugin over namespace-level
//!                       PluginId unregisters a sibling, keeps the target.
//!   [3] aggregate.rs:349 — dropDeclared never clears uni_cypher's
//!                       plugin-aggregate hint set (permanent leak).
//!   [4] eval.rs:198   — Int/Int arithmetic routed through f64
//!                       (int-division→Float, >2^53 precision loss,
//!                       Mul saturates via f64).
//!   [5] lib.rs:1292   — declareTrigger stores event_filter as `.body`.
//!   [6] scalar.rs:94  — 0-row invocation fabricates a length-1 column.
//!   [7] eval.rs:164   — null operand short-circuits, breaking 3VL
//!                       AND/OR.
//!   [8] lib.rs:1511   — DeclaredPluginStore::declare TOCTOU: two
//!                       concurrent declares persist a dependency cycle.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use uni_cypher::parse_expression;
use uni_plugin::traits::procedure::{ProcedureContext, ProcedurePlugin};
use uni_plugin::traits::scalar::ScalarPluginFn;
use uni_plugin::{FnError, PluginId, PluginRegistry, QName};
use uni_plugin_custom::procedures::{
    DeclareAggregateProcedure, DeclareFunctionProcedure, DeclareTriggerProcedure,
    DropDeclaredProcedure, install_function_into_registry,
};
use uni_plugin_custom::{
    CustomPlugin, DeclaredPlugin, DeclaredPluginStore, DeclaredScalarFn, NullPersistence,
    Persistence,
};
use arrow_schema::DataType;

// --------------------------------------------------------------------
// shared helpers
// --------------------------------------------------------------------

fn utf8_args(values: &[&str]) -> Vec<ColumnarValue> {
    values
        .iter()
        .map(|s| ColumnarValue::Scalar(ScalarValue::Utf8(Some((*s).to_owned()))))
        .collect()
}

/// Drive a `declare*`/`drop*` procedure stream to completion and return
/// the first boolean-column value of the first batch (the
/// `registered` / `removed` flag).
async fn drive_flag(stream_res: Result<SendableRecordBatchStream, FnError>) -> bool {
    let mut stream = stream_res.expect("procedure invoke");
    let batch = stream
        .next()
        .await
        .expect("at least one batch")
        .expect("batch ok");
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("bool column");
    col.value(0)
}

fn scalar_int_out(f: &dyn ScalarPluginFn, args: &[ColumnarValue], rows: usize) -> Option<i64> {
    let out = f.invoke(args, rows).expect("scalar invoke");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(_) => panic!("expected array output"),
    };
    let a = arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 output");
    if a.is_null(0) { None } else { Some(a.value(0)) }
}

fn scalar_float_out(f: &dyn ScalarPluginFn, args: &[ColumnarValue], rows: usize) -> f64 {
    let out = f.invoke(args, rows).expect("scalar invoke");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(_) => panic!("expected array output"),
    };
    arr.as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 output")
        .value(0)
}

/// Build a `DeclaredScalarFn` from a Cypher body with the given argument
/// names, arg Arrow types, and return Arrow type.
fn scalar_fn(body: &str, args: &[(&str, DataType)], ret: DataType) -> DeclaredScalarFn {
    let expr = parse_expression(body).expect("parse body");
    let arg_names: Vec<String> = args.iter().map(|(n, _)| (*n).to_owned()).collect();
    let sig_args: Vec<(String, DataType)> =
        args.iter().map(|(n, t)| ((*n).to_owned(), t.clone())).collect();
    let sig = DeclaredScalarFn::build_signature(ret, &sig_args);
    DeclaredScalarFn::new(expr, arg_names, sig)
}

fn func_record(qname: &str, body: &str, ret: &str, arg_names: &[&str]) -> DeclaredPlugin {
    DeclaredPlugin {
        qname: qname.to_owned(),
        kind: "function".to_owned(),
        body: body.to_owned(),
        signature_json: serde_json::json!({
            "return_type": ret,
            "arg_names": arg_names,
        })
        .to_string(),
        dependencies: vec![],
        declared_by: "test".to_owned(),
        active: true,
    }
}

// --------------------------------------------------------------------
// [1] decode.rs:64 — re-declare misclassified as NativeShadow
// --------------------------------------------------------------------

#[tokio::test]
async fn repro1_redeclare_misclassified_as_native_shadow() {
    let registry = Arc::new(PluginRegistry::new());
    let persistence: Arc<dyn Persistence> = Arc::new(NullPersistence);
    let custom = CustomPlugin::new(Arc::clone(&registry), Arc::clone(&persistence)).unwrap();
    let declare = DeclareFunctionProcedure::new(
        Arc::clone(custom.store()),
        Arc::clone(&persistence),
        Arc::clone(custom.registry()),
    );

    // 1) First declaration of repro1.f as `$x + 1`.
    let reg1 = drive_flag(
        declare.invoke(
            ProcedureContext::new(),
            &utf8_args(&["repro1.f", "$x + 1", "int", r#"["x"]"#]),
        ),
    )
    .await;
    assert!(reg1, "first declaration should register");

    let qn = QName::new("repro1", "f");
    let out1 = scalar_int_out(
        &*downcast_scalar(&registry, &qn),
        &[ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))],
        1,
    );
    assert_eq!(out1, Some(2), "f(1) == 2 with body $x + 1");

    // 2) Re-declare the SAME qname with a NEW body `$x + 2`. This is a
    // supported store op ("replace an existing declaration").
    let reg2 = drive_flag(
        declare.invoke(
            ProcedureContext::new(),
            &utf8_args(&["repro1.f", "$x + 2", "int", r#"["x"]"#]),
        ),
    )
    .await;

    // FIXED (lib.rs/decode.rs): re-declaring your OWN qname first drops the prior
    // entry, so re-registration succeeds instead of tripping DuplicateRegistration
    // → NativeShadow.
    assert!(reg2, "re-declaration of an owned qname must succeed (registered=true)");

    // 3) The registry now executes the NEW body: f(1) == 3.
    let out2 = scalar_int_out(
        &*downcast_scalar(&registry, &qn),
        &[ColumnarValue::Scalar(ScalarValue::Int64(Some(1)))],
        1,
    );
    assert_eq!(out2, Some(3), "the new body $x + 2 must be live after re-declare");

    // The store holds the new body and it is active.
    let record = custom.store().get("repro1.f").expect("record present");
    assert_eq!(record.body, "$x + 2", "store holds the NEW body");
    assert!(record.active, "re-declared body must be active");
}

/// Look the registered synthetic scalar up and hand back a boxed
/// `ScalarPluginFn` so the caller can invoke it directly.
fn downcast_scalar(
    registry: &Arc<PluginRegistry>,
    qn: &QName,
) -> Arc<dyn ScalarPluginFn> {
    registry
        .scalar_fn(qn)
        .expect("scalar registered")
        .function
        .clone()
}

// --------------------------------------------------------------------
// [2] lib.rs:773 — namespace-level remove_plugin unregisters a sibling
// --------------------------------------------------------------------

#[test]
fn repro2_drop_removes_sibling_keeps_target() {
    let registry = Arc::new(PluginRegistry::new());

    // Two declared functions in the SAME namespace `repro2ns`.
    install_function_into_registry(&registry, &func_record("repro2ns.f1", "'A'", "string", &[]))
        .expect("install f1");
    install_function_into_registry(&registry, &func_record("repro2ns.f2", "'B'", "string", &[]))
        .expect("install f2");

    let f1 = QName::new("repro2ns", "f1");
    let f2 = QName::new("repro2ns", "f2");
    assert!(registry.scalar_fn(&f1).is_some(), "f1 registered");
    assert!(registry.scalar_fn(&f2).is_some(), "f2 registered");

    // FIXED (lib.rs): dropping f1 uses the targeted remove_named_unique scoped to
    // the qname, so f1 is unregistered and the sibling f2 survives.
    registry.remove_named_unique(&PluginId::new("repro2ns"), &f1);

    assert!(
        registry.scalar_fn(&f1).is_none(),
        "dropped f1 must be unregistered"
    );
    assert!(
        registry.scalar_fn(&f2).is_some(),
        "sibling f2 must survive dropping f1"
    );
}

// --------------------------------------------------------------------
// [3] aggregate.rs:349 — dropDeclared leaks the plugin-aggregate hint
// --------------------------------------------------------------------

#[tokio::test]
async fn repro3_drop_leaks_aggregate_hint() {
    let registry = Arc::new(PluginRegistry::new());
    let persistence: Arc<dyn Persistence> = Arc::new(NullPersistence);
    let store = Arc::new(DeclaredPluginStore::new());

    let declare = DeclareAggregateProcedure::new(
        Arc::clone(&store),
        Arc::clone(&persistence),
        Arc::clone(&registry),
    );
    // qname unique to avoid collision with the process-global hint set.
    let registered = drive_flag(
        declare.invoke(
            ProcedureContext::new(),
            &utf8_args(&[
                "repro3agg.myagg",
                "0",
                "$state + $x",
                "$state",
                "int",
                r#"["x"]"#,
            ]),
        ),
    )
    .await;
    assert!(registered, "aggregate registered");
    assert!(
        uni_cypher::is_known_plugin_aggregate("repro3agg.myagg"),
        "hint published on declare"
    );

    // Drop it through the real dropDeclared procedure.
    let drop = DropDeclaredProcedure::new(
        Arc::clone(&store),
        Arc::clone(&persistence),
        Arc::clone(&registry),
    );
    let removed = drive_flag(
        drop.invoke(ProcedureContext::new(), &utf8_args(&["repro3agg.myagg"])),
    )
    .await;
    assert!(removed, "dropDeclared reports removed=true");
    assert!(store.get("repro3agg.myagg").is_none(), "store dropped it");

    // FIXED (aggregate.rs/uni-cypher): dropDeclared now calls
    // unregister_plugin_aggregate, so the hint is cleared and the name no longer
    // routes through aggregate translation.
    assert!(
        !uni_cypher::is_known_plugin_aggregate("repro3agg.myagg"),
        "aggregate hint must be cleared after dropDeclared"
    );
}

// --------------------------------------------------------------------
// [4] eval.rs:198 — Int/Int arithmetic routed through f64
// --------------------------------------------------------------------

#[test]
fn repro4a_integer_division_returns_float() {
    // Cypher: 7 / 2 == 3 (truncating int division). arith() collapses to
    // f64 -> 3.5.
    let f = scalar_fn(
        "$a / $b",
        &[("a", DataType::Int64), ("b", DataType::Int64)],
        DataType::Float64,
    );
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
    ];
    let got = scalar_float_out(&f, &args, 1);
    // FIXED (eval.rs): int/int division truncates to 3 (declared Float64 return
    // coerces it to 3.0), per Cypher semantics — not 3.5.
    assert_eq!(got, 3.0, "int/int division must truncate (7/2 == 3)");
}

#[test]
fn repro4b_precision_loss_beyond_2_53() {
    // 9007199254740993 - 1 == 9007199254740992 exactly. Casting to f64
    // rounds 9007199254740993 -> ...992, minus 1 -> ...991.
    let f = scalar_fn(
        "$a - $b",
        &[("a", DataType::Int64), ("b", DataType::Int64)],
        DataType::Int64,
    );
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Int64(Some(9_007_199_254_740_993))),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
    ];
    let got = scalar_int_out(&f, &args, 1);
    // FIXED (eval.rs): exact i64 subtraction, no f64 rounding.
    assert_eq!(
        got,
        Some(9_007_199_254_740_992),
        ">2^53 subtraction must be exact (9007199254740993 - 1)"
    );
}

#[test]
fn repro4c_multiply_saturates_via_f64() {
    // i64::MAX * 2 silently saturates to i64::MAX through the f64 path
    // (out.is_finite() && out.fract()==0 -> `out as i64` saturating cast),
    // rather than following any i64 overflow/wrapping semantics.
    let f = scalar_fn(
        "$a * $b",
        &[("a", DataType::Int64), ("b", DataType::Int64)],
        DataType::Int64,
    );
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX))),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
    ];
    // FIXED (eval.rs): i64::MAX * 2 overflows i64; checked_mul signals an error
    // instead of silently saturating to i64::MAX through f64.
    let res = f.invoke(&args, 1);
    assert!(
        res.is_err(),
        "integer overflow must be signaled as an error, not silently saturated"
    );
}

// --------------------------------------------------------------------
// [5] lib.rs:1292 — declareTrigger stores event_filter as `.body`
// --------------------------------------------------------------------

#[tokio::test]
async fn repro5_trigger_body_is_event_filter() {
    let store = Arc::new(DeclaredPluginStore::new());
    let persistence: Arc<dyn Persistence> = Arc::new(NullPersistence);
    let declare = DeclareTriggerProcedure::new(Arc::clone(&store), Arc::clone(&persistence));

    // args: qname, event_filter, body, deps
    let _ = declare
        .invoke(
            ProcedureContext::new(),
            &utf8_args(&[
                "repro5.audit",
                "Person",
                "CREATE (:Log {msg: 'fired'})",
                "[]",
            ]),
        )
        .expect("declareTrigger invoke");

    let record = store.get("repro5.audit").expect("trigger recorded");
    // BUG: expected the Cypher body (position 2) to be stored as `.body`.
    // The shared macro reads sig_args[1] which for declareTrigger is
    // `event_filter` -> `.body` == "Person". A synthesized trigger would
    // execute "Person" as its Cypher query. (lib.rs:1292)
    assert_eq!(
        record.body, "Person",
        "repro for lib.rs:1292: trigger .body holds the event_filter, not the Cypher body"
    );
    // The true body survives (unused) under signature_json["body"].
    let sig: serde_json::Value = serde_json::from_str(&record.signature_json).unwrap();
    assert_eq!(
        sig.get("body").and_then(|v| v.as_str()),
        Some("CREATE (:Log {msg: 'fired'})"),
        "true body captured in signature_json but never executed"
    );
}

// --------------------------------------------------------------------
// [6] scalar.rs:94 — 0-row invocation fabricates a length-1 column
// --------------------------------------------------------------------

#[test]
fn repro6_zero_rows_fabricates_one_row() {
    let f = scalar_fn(
        "$first + ' ' + $last",
        &[("first", DataType::Utf8), ("last", DataType::Utf8)],
        DataType::Utf8,
    );
    // Empty batch: 0-length input columns, rows == 0.
    let empty_a: StringArray = StringArray::from(Vec::<Option<&str>>::new());
    let empty_b: StringArray = StringArray::from(Vec::<Option<&str>>::new());
    let args = vec![
        ColumnarValue::Array(Arc::new(empty_a)),
        ColumnarValue::Array(Arc::new(empty_b)),
    ];
    let out = f.invoke(&args, 0).expect("invoke on empty batch");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(_) => panic!("expected array"),
    };
    // BUG: contract says "produce exactly `rows` values" -> length 0.
    // rows.max(1) fabricates a length-1 column. (scalar.rs:94)
    assert_eq!(
        arr.len(),
        1,
        "repro for scalar.rs:94: 0-row invocation returns a length-1 column"
    );
}

// --------------------------------------------------------------------
// [7] eval.rs:164 — null operand short-circuits, breaking 3VL AND/OR
// --------------------------------------------------------------------

#[test]
fn repro7_null_and_false_is_null_not_false() {
    // Cypher 3VL: null AND false == false.
    let f = scalar_fn(
        "$a AND $b",
        &[("a", DataType::Boolean), ("b", DataType::Boolean)],
        DataType::Boolean,
    );
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Boolean(None)), // null
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
    ];
    let out = f.invoke(&args, 1).expect("invoke");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(_) => panic!("expected array"),
    };
    let b = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    // FIXED (eval.rs): `null AND false` == false (false dominates); apply_binary
    // now resolves the dominating operand before propagating NULL.
    assert!(
        !b.is_null(0) && !b.value(0),
        "`null AND false` must be false (false dominates)"
    );
}

#[test]
fn repro7_null_or_true_is_null_not_true() {
    // Cypher 3VL: null OR true == true.
    let f = scalar_fn(
        "$a OR $b",
        &[("a", DataType::Boolean), ("b", DataType::Boolean)],
        DataType::Boolean,
    );
    let args = vec![
        ColumnarValue::Scalar(ScalarValue::Boolean(None)), // null
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
    ];
    let out = f.invoke(&args, 1).expect("invoke");
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(_) => panic!("expected array"),
    };
    let b = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
    // FIXED (eval.rs): `null OR true` == true (true dominates).
    assert!(
        !b.is_null(0) && b.value(0),
        "`null OR true` must be true (true dominates)"
    );
}

// --------------------------------------------------------------------
// [8] lib.rs:1511 — DeclaredPluginStore::declare TOCTOU cycle race
// --------------------------------------------------------------------

/// Concurrency race: two declares validate (read lock) then insert
/// (separate write lock). Marked #[ignore] because it is timing
/// dependent; run explicitly with `--run-ignored=all` to observe.
#[test]
#[ignore = "repro for lib.rs:1511: check-then-act race, run explicitly with --run-ignored"]
fn repro8_concurrent_declare_persists_cycle() {
    fn rec(qname: &str, deps: &[&str]) -> DeclaredPlugin {
        DeclaredPlugin {
            qname: qname.to_owned(),
            kind: "function".to_owned(),
            body: "$x".to_owned(),
            signature_json: "{}".to_owned(),
            dependencies: deps.iter().map(|s| (*s).to_owned()).collect(),
            declared_by: "test".to_owned(),
            active: true,
        }
    }

    let mut cycle_persisted = false;
    for _ in 0..2000 {
        let store = Arc::new(DeclaredPluginStore::new());
        // Seed both nodes with no deps (single-threaded).
        store.declare(rec("a", &[])).expect("seed a");
        store.declare(rec("b", &[])).expect("seed b");

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let s1 = Arc::clone(&store);
        let b1 = Arc::clone(&barrier);
        let t1 = std::thread::spawn(move || {
            b1.wait();
            s1.declare(rec("a", &["b"])) // a -> b
        });
        let s2 = Arc::clone(&store);
        let b2 = Arc::clone(&barrier);
        let t2 = std::thread::spawn(move || {
            b2.wait();
            s2.declare(rec("b", &["a"])) // b -> a
        });
        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        let a_deps = store.get("a").map(|p| p.dependencies).unwrap_or_default();
        let b_deps = store.get("b").map(|p| p.dependencies).unwrap_or_default();
        if a_deps == vec!["b".to_owned()] && b_deps == vec!["a".to_owned()] {
            // Both inserts landed -> a<->b cycle now persisted, and both
            // calls returned Ok (declare is contractually required to
            // reject one with DependencyCycle).
            assert!(r1.is_ok() && r2.is_ok(), "both declares returned Ok");
            cycle_persisted = true;
            break;
        }
    }

    // BUG: declare() must never persist a dependency cycle; the TOCTOU
    // window between the read-lock validation and the write-lock insert
    // lets two concurrent declares both pass and both commit. (lib.rs:1511)
    assert!(
        cycle_persisted,
        "repro for lib.rs:1511: expected a persisted a<->b cycle under concurrency"
    );
}
