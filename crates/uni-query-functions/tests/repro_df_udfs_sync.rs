// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runnable repros for verified findings in the DataFusion UDF layer that are
//! reachable synchronously (direct `invoke_with_args` / public helpers, no
//! query engine / async runtime).

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array, LargeBinaryArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;

use uni_common::Value;
use uni_common::cypher_value_codec::encode_int;

use uni_query_functions::df_udfs::{
    create_cypher_equal_udf, create_cypher_lt_eq_udf, create_range_udf, encode_cypher_sort_key,
    invoke_cypher_string_op,
};

/// Build `ScalarFunctionArgs` from two ready-made `ColumnarValue`s.
fn make_args(
    a: ColumnarValue,
    a_ty: DataType,
    b: ColumnarValue,
    b_ty: DataType,
    number_rows: usize,
) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        args: vec![a, b],
        arg_fields: vec![
            Arc::new(Field::new("l", a_ty, true)),
            Arc::new(Field::new("r", b_ty, true)),
        ],
        number_rows,
        return_field: Arc::new(Field::new("res", DataType::Boolean, true)),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

/// Finding [2] df_udfs.rs:4226 — the LargeBinary-vs-Int64 fast-compare branch
/// routes the native i64 RHS through f64, losing precision above 2^53 and
/// yielding wrong equality/ordering for large integers.
#[test]
fn repro_finding_02_fast_compare_i64_precision_loss() {
    // 2^62 + 1: exactly representable as i64 but NOT as f64.
    let big: i64 = 4_611_686_018_427_387_905;

    let make_operands = || {
        let lhs = LargeBinaryArray::from_iter_values([encode_int(big)]);
        let rhs = Int64Array::from(vec![big]);
        (
            ColumnarValue::Array(Arc::new(lhs)),
            ColumnarValue::Array(Arc::new(rhs)),
        )
    };

    // Equality: genuinely-equal values.
    let eq_udf = create_cypher_equal_udf();
    let (l, r) = make_operands();
    let out = eq_udf
        .invoke_with_args(make_args(l, DataType::LargeBinary, r, DataType::Int64, 1))
        .unwrap();
    let eq_result = match out {
        ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0),
        other => panic!("unexpected output: {other:?}"),
    };
    // FIXED (df_udfs.rs): the RHS i64 is compared exactly (no f64 round-trip),
    // so big == big is true.
    assert!(eq_result, "large-int equality must be exact (big == big)");

    // Ordering: `big <= big` should be true.
    let le_udf = create_cypher_lt_eq_udf();
    let (l, r) = make_operands();
    let out = le_udf
        .invoke_with_args(make_args(l, DataType::LargeBinary, r, DataType::Int64, 1))
        .unwrap();
    let le_result = match out {
        ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0),
        other => panic!("unexpected output: {other:?}"),
    };
    // FIXED (df_udfs.rs): big <= big is true.
    assert!(le_result, "large-int <= must be exact (big <= big)");
}

/// Finding [3] df_udfs.rs:4030 — the array-vs-array branch of
/// `invoke_cypher_string_op` never checks `is_null` for StringArray, so a null
/// slot decodes as "" instead of None, breaking 3-valued logic.
#[test]
fn repro_finding_03_string_op_array_null_becomes_empty() {
    // l[0] is a NULL string; l[1] is "hi".
    let l = StringArray::from(vec![None, Some("hi")]);
    let r = StringArray::from(vec![Some("h"), Some("h")]);

    let args = make_args(
        ColumnarValue::Array(Arc::new(l)),
        DataType::Utf8,
        ColumnarValue::Array(Arc::new(r)),
        DataType::Utf8,
        2,
    );

    let out = invoke_cypher_string_op(&args, "_cypher_contains", |a, b| a.contains(b)).unwrap();
    let bools = match out {
        ColumnarValue::Array(arr) => arr.as_any().downcast_ref::<BooleanArray>().unwrap().clone(),
        other => panic!("unexpected output: {other:?}"),
    };

    // Correct 3VL: `null CONTAINS 'h'` must be NULL.
    // BUG: null slot decodes to "" so result[0] = Some(false), not NULL
    // (repro for df_udfs.rs:4030).
    assert!(
        !bools.is_null(0),
        "null string slot should yield NULL but yields a concrete boolean"
    );
    assert!(
        !bools.value(0),
        "null CONTAINS 'h' wrongly evaluates to false (empty-string decode)"
    );
}

/// Finding [7] df_udfs.rs:1434 — `RangeUdf` advances with unchecked `current +=
/// step`, so a range ending at `i64::MAX` overflows (panics in debug).
#[test]
fn repro_finding_07_range_udf_overflow() {
    let result = std::panic::catch_unwind(|| {
        let udf = create_range_udf();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX - 1))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("start", DataType::Int64, true)),
                Arc::new(Field::new("end", DataType::Int64, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new(
                "res",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            )),
            config_options: Arc::new(ConfigOptions::default()),
        };
        udf.invoke_with_args(args)
    });

    // FIXED (df_udfs.rs): RangeUdf uses checked_add, so it terminates cleanly at
    // the i64 boundary instead of panicking (debug) / overflowing (release).
    let inner = result.expect("RangeUdf must not panic at the i64 boundary");
    assert!(inner.is_ok(), "range at the i64 boundary must produce a terminating list");
}

/// Finding [13] df_udfs.rs:2956 — `encode_sort_key_to_buf` casts `Value::Int`
/// to f64 for the ORDER BY sort key, collapsing distinct i64 values above 2^53.
#[test]
fn repro_finding_13_sort_key_int_collapse() {
    // 2^53 and 2^53 + 1 differ by 1 but both round to the same f64.
    let k_lo = encode_cypher_sort_key(&Value::Int(9_007_199_254_740_992));
    let k_hi = encode_cypher_sort_key(&Value::Int(9_007_199_254_740_993));

    // BUG: distinct integers produce byte-identical sort keys (repro for
    // df_udfs.rs:2956); a correct encoder would make these differ.
    assert_eq!(
        k_lo, k_hi,
        "distinct i64 values above 2^53 collapse to the same sort key"
    );
}
