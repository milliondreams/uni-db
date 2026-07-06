//! Runnable repros for 5 verified correctness findings in `uni-plugin-rhai`.
//!
//! Each test drives the REAL public API with REAL inputs and asserts on the
//! actually-observed (buggy) behavior. Where the correct-behavior assertion
//! would fail while the bug is present, the test asserts the buggy value and
//! documents the expected-correct value in a `// BUG:` comment.

#![cfg(feature = "rhai-runtime")]

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field};
use datafusion::scalar::ScalarValue;

use uni_plugin::traits::aggregate::{AggSignature, AggregatePluginFn, PluginAccumulator};
use uni_plugin::traits::procedure::{
    ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::capability::SideEffects;
use uni_plugin::{
    Capability, CapabilitySet, KmsProvider, PluginId, PluginRegistrar, PluginRegistry, QName,
};

use datafusion::logical_expr::Volatility;
use futures::StreamExt;
use rhai::Dynamic;
use uni_plugin_rhai::adapter_aggregate::rhai_state_fields;
use uni_plugin_rhai::host_fn_impls::register_default_host_fns;
use uni_plugin_rhai::manifest::compile;
use uni_plugin_rhai::{
    RhaiAggregateFn, RhaiHostFnRegistry, RhaiLoader, RhaiPluginRuntime, RhaiProcedure, build_engine,
};

fn build_runtime(script: &str) -> Arc<RhaiPluginRuntime> {
    let engine = build_engine(&CapabilitySet::new(), &RhaiHostFnRegistry::new());
    let ast = compile(&engine, script).expect("script compiles");
    RhaiPluginRuntime::new(PluginId::new("test.repro"), engine, ast)
}

// ---------------------------------------------------------------------------
// [1] adapter_aggregate.rs:291 — evaluate() ignores signature.returns type.
//
// The aggregate declares `returns: Float64`, but finalize returns a Rhai INT.
// dynamic_to_scalar_loose is value-directed, so evaluate() emits Int64 rather
// than the declared Float64 — a type/schema mismatch vs what the DataFusion
// UDAF adapter advertised via return_type().
// ---------------------------------------------------------------------------
#[test]
fn agg_evaluate_ignores_declared_return_type_int_for_float() {
    let script = r#"
        fn cnt_init() { #{ n: 0 } }
        fn cnt_accumulate(s, x) { s.n += 1; s }
        fn cnt_merge(a, b) { #{ n: a.n + b.n } }
        fn cnt_finalize(s) { s.n }
    "#;
    let runtime = build_runtime(script);
    let sig = AggSignature {
        args: vec![ArgType::Primitive(DataType::Float64)],
        // Declared return type is Float64.
        returns: ArgType::Primitive(DataType::Float64),
        state_fields: rhai_state_fields(),
        volatility: Volatility::Immutable,
        supports_partial: true,
    };
    let agg = RhaiAggregateFn::new(runtime, "cnt", sig);
    let mut acc = agg.create_accumulator();
    let xs: arrow_array::ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
    acc.update_batch(&[xs]).unwrap();
    let result = acc.evaluate().unwrap();

    // BUG: declared returns=Float64 → expected ScalarValue::Float64(Some(3.0));
    // got ScalarValue::Int64(Some(3)) because dynamic_to_scalar_loose is
    // value-directed (adapter_aggregate.rs:291).
    match result {
        ScalarValue::Int64(Some(3)) => { /* buggy path confirmed */ }
        other => panic!("expected buggy Int64(3); got {other:?}"),
    }
}

#[test]
fn agg_evaluate_unit_finalize_becomes_untyped_null() {
    let script = r#"
        fn u_init() { #{ n: 0 } }
        fn u_accumulate(s, x) { s.n += 1; s }
        fn u_merge(a, b) { #{ n: a.n + b.n } }
        fn u_finalize(s) { () }
    "#;
    let runtime = build_runtime(script);
    let sig = AggSignature {
        args: vec![ArgType::Primitive(DataType::Float64)],
        returns: ArgType::Primitive(DataType::Float64),
        state_fields: rhai_state_fields(),
        volatility: Volatility::Immutable,
        supports_partial: true,
    };
    let agg = RhaiAggregateFn::new(runtime, "u", sig);
    let mut acc = agg.create_accumulator();
    let xs: arrow_array::ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
    acc.update_batch(&[xs]).unwrap();
    let result = acc.evaluate().unwrap();

    // BUG: declared returns=Float64 → expected a typed Float64 null
    // (ScalarValue::Float64(None)); got untyped ScalarValue::Null whose
    // DataType is DataType::Null (adapter_aggregate.rs:292-294).
    assert_eq!(result, ScalarValue::Null);
    assert_eq!(result.data_type(), DataType::Null);
}

// ---------------------------------------------------------------------------
// [4] adapter_aggregate.rs:245 — serde_json encodes NaN/Inf as JSON null,
// so a peer NaN partial silently becomes Dynamic::UNIT after decode_state.
//
// Driven through the public state()/merge_batch() round-trip (the real merge
// path), so no private helpers are touched.
// ---------------------------------------------------------------------------
#[test]
fn agg_nan_partial_state_silently_lost_on_serialize_merge() {
    // State is a bare float. init produces NaN via float 0.0/0.0.
    let script = r#"
        fn s_init() { 0.0 / 0.0 }
        fn s_accumulate(state, x) { state }
        fn s_merge(a, b) { b }
        fn s_finalize(s) { s.type_of() }
    "#;
    let runtime = build_runtime(script);
    let sig = AggSignature {
        args: vec![ArgType::Primitive(DataType::Float64)],
        returns: ArgType::Primitive(DataType::LargeUtf8),
        state_fields: rhai_state_fields(),
        volatility: Volatility::Immutable,
        supports_partial: true,
    };
    let agg = RhaiAggregateFn::new(runtime, "s", sig);

    // Partition A: state is NaN. Serialize it (this is what a peer ships).
    let a = agg.create_accumulator();
    let state_vec = a.state().unwrap();
    let state_bytes = match &state_vec[0] {
        ScalarValue::LargeBinary(Some(b)) => b.clone(),
        other => panic!("expected LargeBinary state, got {other:?}"),
    };
    // BUG: encode_state (serde_json::to_vec) emits the JSON token `null` for
    // NaN — the float is gone. Expected: a representation preserving NaN (or
    // an error), not b"null".
    assert_eq!(
        state_bytes, b"null",
        "NaN state should not silently serialize to JSON null"
    );

    // Partition B: merge partition A's peer state, then finalize the type.
    let mut b = agg.create_accumulator();
    let peer_arr: arrow_array::ArrayRef =
        Arc::new(arrow_array::LargeBinaryArray::from(vec![state_bytes.as_slice()]));
    b.merge_batch(&[peer_arr]).unwrap();
    let finalized = b.evaluate().unwrap();

    // BUG: decode_state turned the peer NaN into Dynamic::UNIT, so the merged
    // state's runtime type is unit `()` — not a float. Expected: "f64".
    match finalized {
        ScalarValue::Utf8(Some(ref t)) => assert_eq!(
            t, "()",
            "peer NaN should not decode to unit; got type {t}"
        ),
        other => panic!("unexpected finalize result: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// [3] loader.rs:294 — loader fabricates yield field names col0..colN, so a
// procedure returning natural-key row maps (id/name) yields all-NULL columns.
//
// Driven through the REAL loader + registry + invoke path.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn procedure_loader_fabricated_yield_names_drop_natural_keys() {
    let script = r#"
        fn uni_manifest() {
            #{
                id: "ai.example.rows",
                version: "0.1.0",
                determinism: "pure",
                procedures: [
                    #{ name: "rows", args: [], yields: ["int","string"], mode: "read" },
                ],
            }
        }

        fn rows() {
            [
                #{ id: 1, name: "alice" },
                #{ id: 2, name: "bob" },
            ]
        }
    "#;

    let loader = RhaiLoader::new();
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::Procedure]);
    let mut r = PluginRegistrar::new(PluginId::new("rhai.rows"), &caps, &registry);
    loader.load(script, &mut r, &caps).expect("load succeeds");
    r.commit_to_registry().expect("commits");

    let qname = QName::new("ai.example.rows", "rows");
    let entry = registry.procedure(&qname).expect("procedure registered");

    // The loader named the yield columns positionally.
    let schema = &entry.signature.yields;
    assert_eq!(schema[0].name(), "col0");
    assert_eq!(schema[1].name(), "col1");

    let mut stream = entry
        .procedure
        .invoke(ProcedureContext::new(), &[])
        .expect("invoke");
    let batch = stream.next().await.unwrap().unwrap();

    // 2 rows produced, but every value is NULL: dynamic_to_record_batch keyed
    // on col0/col1, which the row maps (id/name) do not contain.
    assert_eq!(batch.num_rows(), 2);
    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 col");
    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 col");

    // BUG: expected id=[1,2], name=["alice","bob"]; got all-NULL because the
    // fabricated col0/col1 names (loader.rs:294) don't match the map keys.
    assert_eq!(id_col.null_count(), 2, "expected all id values NULL (bug)");
    assert_eq!(name_col.null_count(), 2, "expected all name values NULL (bug)");
}

// ---------------------------------------------------------------------------
// [5] adapter_procedure.rs:146 — coerce_for casts a Rhai float into an
// Int64-declared column with a bare `as i64`: NaN→0, fractional truncates,
// out-of-range saturates — all silently.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn procedure_float_into_int64_silently_casts() {
    let script = r#"
        fn rows() {
            [
                #{ v: 0.0 / 0.0 },
                #{ v: 3.9 },
                #{ v: 1e30 },
                #{ v: -1e30 },
            ]
        }
    "#;
    let runtime = build_runtime(script);
    let sig = ProcedureSignature {
        args: vec![],
        yields: vec![Field::new("v", DataType::Int64, true)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: String::new(),
    };
    let proc = RhaiProcedure::new(runtime, "rows", sig);
    let mut stream = proc.invoke(ProcedureContext::new(), &[]).expect("invoke");
    let batch = stream.next().await.unwrap().unwrap();
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 col");

    // FIXED (adapter_procedure.rs): only a finite, in-range float coerces (3.9 ->
    // 3). NaN / ±1e30 are NOT silently `as i64`-cast to 0 / i64::MAX / i64::MIN —
    // they fall out as NULL instead.
    assert!(col.is_null(0), "NaN must not silently become 0");
    assert_eq!(col.value(1), 3, "3.9 truncates to 3");
    assert!(col.is_null(2), "1e30 must not saturate to i64::MAX");
    assert!(col.is_null(3), "-1e30 must not saturate to i64::MIN");
    assert!(col.null_count() >= 3, "non-finite/out-of-range floats are nulled");
}

// ---------------------------------------------------------------------------
// [2] host_fn_impls/kms.rs:99 — from_hex slices &s[i..i+2] on byte offsets
// with no char-boundary check, so a multi-byte char in the signature string
// panics the host thread instead of returning a graceful hex error.
//
// Driven end-to-end through uni_kms_verify with a configured KmsProvider and
// a granted Kms capability, so the private from_hex is reached the real way.
// ---------------------------------------------------------------------------
struct NoopKms;
impl KmsProvider for NoopKms {
    fn sign(&self, _key_id: &str, data: &[u8]) -> Result<Vec<u8>, uni_plugin::FnError> {
        Ok(data.to_vec())
    }
    fn verify(
        &self,
        _key_id: &str,
        _data: &[u8],
        _signature: &[u8],
    ) -> Result<bool, uni_plugin::FnError> {
        Ok(true)
    }
}

#[test]
fn kms_verify_multibyte_hex_returns_error_not_panic() {
    let mut loader = RhaiLoader::new().with_kms(Arc::new(NoopKms));
    register_default_host_fns(&mut loader);
    let caps = CapabilitySet::from_iter_of([Capability::Kms {
        key_ids: vec!["*".into()],
    }]);
    let engine = build_engine(&caps, loader.host_fns());

    // "a€" is 4 bytes (even length → passes the parity check), but '€'
    // occupies bytes 1..4, which byte-index slicing would split.
    let script = r#"uni_kms_verify("k1", "hello", "a€")"#;

    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = catch_unwind(AssertUnwindSafe(|| engine.eval::<bool>(script)));
    std::panic::set_hook(prev);

    // FIXED (kms.rs): from_hex decodes on raw bytes, so a multibyte hex string
    // surfaces as a graceful Err — it must NOT panic/unwind the host thread.
    let eval_res = outcome.expect("uni_kms_verify must not panic the host thread");
    assert!(
        eval_res.is_err(),
        "invalid (multibyte) signature hex must surface as an Err, got {eval_res:?}"
    );
}
