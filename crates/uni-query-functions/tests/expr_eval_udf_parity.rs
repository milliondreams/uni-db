// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Differential test pinning the two Cypher scalar-function evaluation paths
//! against each other.
//!
//! `uni-query-functions` evaluates scalar functions twice over:
//!
//! * the interpreter — [`expr_eval::eval_scalar_function`], reached from
//!   `executor/read.rs`, `df_graph/locy_eval.rs` and `df_graph/unwind.rs`;
//! * the DataFusion UDFs registered by [`df_udfs::register_cypher_udfs`].
//!
//! Both are live in production, so any disagreement is a query answer that
//! depends on which plan shape the optimizer happened to pick. This harness
//! drives one input table through both and asserts they agree.
//!
//! Comparison rule: if both sides succeed the `Value`s must be equal; if both
//! fail that counts as agreement (the exact message text is reconciled
//! separately, since only the openCypher error-code prefix is TCK-visible).
//! A success on one side and a failure on the other is a divergence.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::config::ConfigOptions;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use uni_common::Value;
use uni_common::cypher_value_codec::encode;
use uni_query_functions::df_udfs::register_cypher_udfs;
use uni_query_functions::expr_eval::eval_scalar_function;

/// Outcome of evaluating one expression on one engine.
#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Value(Value),
    Error,
}

/// One differential case: the same call expressed for both engines.
struct Case {
    /// Function name as written in Cypher (the interpreter uppercases it).
    cypher: &'static str,
    /// Name the UDF is registered under in the `SessionContext`.
    udf: &'static str,
    args: Vec<Value>,
    /// Human-readable label used in assertion messages.
    label: &'static str,
}

fn case(cypher: &'static str, udf: &'static str, label: &'static str, args: Vec<Value>) -> Case {
    Case {
        cypher,
        udf,
        args,
        label,
    }
}

/// Evaluate through the interpreter.
fn eval_interpreter(c: &Case) -> Outcome {
    match eval_scalar_function(c.cypher, &c.args, None) {
        Ok(v) => Outcome::Value(v),
        Err(_) => Outcome::Error,
    }
}

/// Evaluate through the registered DataFusion UDF, passing every argument as a
/// CypherValue-encoded `LargeBinary` scalar (the encoding the query engine uses
/// for heterogeneous Cypher values).
fn eval_udf(ctx: &SessionContext, c: &Case) -> Outcome {
    let Ok(udf) = ctx.udf(c.udf) else {
        panic!("UDF `{}` is not registered", c.udf);
    };
    let arg_types = vec![DataType::LargeBinary; c.args.len()];
    let Ok(return_type) = udf.return_type(&arg_types) else {
        return Outcome::Error;
    };

    let args = ScalarFunctionArgs {
        args: c
            .args
            .iter()
            .map(|v| ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(encode(v)))))
            .collect(),
        arg_fields: arg_types
            .iter()
            .enumerate()
            .map(|(i, t)| Arc::new(Field::new(format!("a{i}"), t.clone(), true)))
            .collect(),
        number_rows: 1,
        return_field: Arc::new(Field::new("res", return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };

    match udf.invoke_with_args(args) {
        Ok(cv) => match cv.to_array(1) {
            Ok(arr) => Outcome::Value(decode_arrow_scalar(&arr)),
            Err(_) => Outcome::Error,
        },
        Err(_) => Outcome::Error,
    }
}

/// Decode row 0 of a single-row result array back into a `Value`.
fn decode_arrow_scalar(arr: &dyn Array) -> Value {
    if arr.is_null(0) {
        return Value::Null;
    }
    macro_rules! at {
        ($ty:ty) => {
            arr.as_any().downcast_ref::<$ty>().unwrap().value(0)
        };
    }
    match arr.data_type() {
        DataType::LargeBinary => {
            uni_common::cypher_value_codec::decode(at!(LargeBinaryArray)).unwrap_or(Value::Null)
        }
        DataType::Utf8 => Value::String(at!(StringArray).to_string()),
        DataType::LargeUtf8 => Value::String(at!(LargeStringArray).to_string()),
        DataType::Int64 => Value::Int(at!(Int64Array)),
        DataType::Float64 => Value::Float(at!(Float64Array)),
        DataType::Boolean => Value::Bool(at!(BooleanArray)),
        other => panic!("parity harness cannot decode result type {other:?}"),
    }
}

/// Cases where the two engines are expected to agree today. These guard the
/// harness itself and protect the common path when the UDF shells are collapsed
/// onto the interpreter bodies.
fn agreeing_cases() -> Vec<Case> {
    vec![
        case("toString", "tostring", "toString(42)", vec![Value::Int(42)]),
        case(
            "toString",
            "tostring",
            "toString(true)",
            vec![Value::Bool(true)],
        ),
        case(
            "toString",
            "tostring",
            "toString('hi')",
            vec![Value::String("hi".into())],
        ),
        case(
            "toInteger",
            "tointeger",
            "toInteger('42')",
            vec![Value::String("42".into())],
        ),
        case(
            "toInteger",
            "tointeger",
            "toInteger('abc')",
            vec![Value::String("abc".into())],
        ),
        case(
            "toBoolean",
            "toboolean",
            "toBoolean('true')",
            vec![Value::String("true".into())],
        ),
        case(
            "toBoolean",
            "toboolean",
            "toBoolean(false)",
            vec![Value::Bool(false)],
        ),
        case(
            "substring",
            "_cypher_substring",
            "substring('hello', 1)",
            vec![Value::String("hello".into()), Value::Int(1)],
        ),
        case(
            "substring",
            "_cypher_substring",
            "substring('hello', 1, 3)",
            vec![Value::String("hello".into()), Value::Int(1), Value::Int(3)],
        ),
        case(
            "split",
            "_cypher_split",
            "split('a,b,c', ',')",
            vec![Value::String("a,b,c".into()), Value::String(",".into())],
        ),
        case(
            "split",
            "_cypher_split",
            "split(null, ',')",
            vec![Value::Null, Value::String(",".into())],
        ),
    ]
}

/// The seven known divergences from the 2026-07-30 audit. Each is a live wrong
/// answer on one of the two paths; see `docs/proposals/dup_dead_simplify_audit_2026-07-30.md` §0.1.
fn known_divergence_cases() -> Vec<Case> {
    vec![
        // expr_eval "1.0" (format!("{f:.1}")) vs df_udfs "1" (f.to_string()).
        case(
            "toString",
            "tostring",
            "toString(1.0)",
            vec![Value::Float(1.0)],
        ),
        // expr_eval stringifies the list; df_udfs raises TypeError.
        case(
            "toString",
            "tostring",
            "toString([1, 2])",
            vec![Value::List(vec![Value::Int(1), Value::Int(2)])],
        ),
        // expr_eval Null (no f64 fallback) vs df_udfs Int(3).
        case(
            "toInteger",
            "tointeger",
            "toInteger('3.7')",
            vec![Value::String("3.7".into())],
        ),
        // expr_eval errors on a non-string/bool; df_udfs coerces `i != 0`.
        case(
            "toBoolean",
            "toboolean",
            "toBoolean(1)",
            vec![Value::Int(1)],
        ),
        // expr_eval errors on a negative start; df_udfs clamps to 0.
        case(
            "substring",
            "_cypher_substring",
            "substring('hello', -1, 2)",
            vec![Value::String("hello".into()), Value::Int(-1), Value::Int(2)],
        ),
        // expr_eval silently clamps a negative length; df_udfs errors.
        case(
            "substring",
            "_cypher_substring",
            "substring('hello', 0, -1)",
            vec![Value::String("hello".into()), Value::Int(0), Value::Int(-1)],
        ),
        // expr_eval only null-checks arg 0, so a null delimiter errors;
        // df_udfs propagates null from any argument.
        case(
            "split",
            "_cypher_split",
            "split('a', null)",
            vec![Value::String("a".into()), Value::Null],
        ),
    ]
}

fn check(cases: Vec<Case>) -> Vec<String> {
    let ctx = SessionContext::new();
    register_cypher_udfs(&ctx).expect("UDF registration must succeed");

    let mut mismatches = Vec::new();
    for c in &cases {
        let interp = eval_interpreter(c);
        let udf = eval_udf(&ctx, c);
        if interp != udf {
            mismatches.push(format!(
                "  {:<26} expr_eval={:?}  df_udfs={:?}",
                c.label, interp, udf
            ));
        }
    }
    mismatches
}

#[test]
fn interpreter_and_udfs_agree_on_the_common_path() {
    let mismatches = check(agreeing_cases());
    assert!(
        mismatches.is_empty(),
        "interpreter and UDF paths disagree:\n{}",
        mismatches.join("\n")
    );
}

/// The seven audited divergences must be reconciled — both engines returning the
/// same answer for each. Until §0.1 lands this test documents the live drift.
#[test]
fn interpreter_and_udfs_agree_on_audited_divergences() {
    let mismatches = check(known_divergence_cases());
    assert!(
        mismatches.is_empty(),
        "{} of the 7 audited divergences are still live:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}
