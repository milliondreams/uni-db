// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runnable repros for verified correctness findings that are reachable
//! through the crate's pure `Value`-level public API (no DataFusion runtime).
//!
//! Each test drives a REAL public function with REAL inputs and asserts on the
//! OBSERVED (currently buggy) behavior. Where the correct-behavior assertion
//! would fail today, the buggy value is asserted with a `// BUG:` comment
//! pinning the source `file:line`.

use std::collections::HashMap;

use uni_common::core::schema::DistanceMetric;
use uni_common::{TemporalValue, Value};

use uni_query_functions::datetime::eval_temporal_accessor_value;
use uni_query_functions::expr_eval::eval_scalar_function;
use uni_query_functions::similar_to::{calculate_score, eval_sparse_similar_to_pure};
use uni_query_functions::spatial::eval_spatial_function;

/// Finding [4] similar_to.rs:288 — `calculate_score` passes the Dot-metric
/// distance through unchanged, but callers feed it `compute_distance` = `-dot`,
/// so the returned "similarity" is sign-inverted for Dot indexes.
#[test]
fn repro_finding_04_calculate_score_dot_sign_inverted() {
    // Two parallel vectors: true dot product = 8.0 (highly similar).
    let a = [8.0_f32, 0.0, 0.0];
    let b = [1.0_f32, 0.0, 0.0];

    let metric = DistanceMetric::Dot;
    let distance = metric.compute_distance(&a, &b); // = -dot = -8.0
    assert_eq!(distance, -8.0, "compute_distance(Dot) must return -dot");

    let score = calculate_score(distance, &metric);

    // Correct similarity for a Dot index is +8.0 (the actual dot product), as
    // the sibling `score_vectors` computes by negating. Instead we observe:
    // BUG: expected score == +8.0, got -8.0 (repro for similar_to.rs:288).
    assert_eq!(
        score, -8.0,
        "calculate_score returns sign-inverted similarity for Dot metric"
    );
}

/// Finding [5] datetime.rs:1826 — `format_timezone_offset` loses the sign for
/// negative sub-hour offsets: -1800s (-00:30) renders as "+00:30".
#[test]
fn repro_finding_05_timezone_offset_negative_subhour_sign_lost() {
    let dt = Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch: 0,
        offset_seconds: -1800, // -00:30
        timezone_name: None,
    });

    let result = eval_temporal_accessor_value(&dt, "offset").unwrap();

    // BUG: expected Value::String("-00:30"), got "+00:30" (repro for datetime.rs:1826).
    assert_eq!(
        result,
        Value::String("+00:30".to_string()),
        "negative sub-hour offset loses its sign"
    );
}

/// Finding [9] expr_eval.rs:1363 — `eval_sign` maps Float via `f.signum() as
/// i64`; `(0.0f64).signum() == 1.0`, so `sign(0.0)` returns 1 instead of 0.
#[test]
fn repro_finding_09_sign_of_float_zero() {
    let result = eval_scalar_function("sign", &[Value::Float(0.0)], None).unwrap();

    // BUG: expected Value::Int(0), got Value::Int(1) (repro for expr_eval.rs:1363).
    assert_eq!(
        result,
        Value::Int(1),
        "sign(0.0) returns 1 because f64::signum(+0.0) == 1.0"
    );

    // Negative zero yields -1 for the same reason.
    let neg = eval_scalar_function("sign", &[Value::Float(-0.0)], None).unwrap();
    assert_eq!(neg, Value::Int(-1), "sign(-0.0) returns -1");
}

/// Finding [10] expr_eval.rs:1139 — `eval_size`/`eval_length` return the UTF-8
/// byte length instead of the character count for non-ASCII strings.
#[test]
fn repro_finding_10_size_length_byte_count_not_char_count() {
    // "café" = 4 characters, 5 UTF-8 bytes (é = 2 bytes).
    let size = eval_scalar_function("size", &[Value::String("café".to_string())], None).unwrap();
    // FIXED (expr_eval.rs): size() counts codepoints, not bytes.
    assert_eq!(size, Value::Int(4), "size() must return char count");

    // "naïve" = 5 characters, 6 UTF-8 bytes (ï = 2 bytes).
    let length =
        eval_scalar_function("length", &[Value::String("naïve".to_string())], None).unwrap();
    // FIXED (expr_eval.rs): length() counts codepoints, not bytes.
    assert_eq!(length, Value::Int(5), "length() must return char count");
}

/// Finding [12] datetime.rs:877 — `epochSeconds`/`epochMillis` use truncating
/// division instead of floor, giving off-by-one for pre-1970 sub-second instants.
#[test]
fn repro_finding_12_epoch_seconds_truncation_pre_1970() {
    // 1969-12-31T23:59:59.5Z => -500_000_000 ns since epoch. Floor => -1s.
    let dt = Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch: -500_000_000,
        offset_seconds: 0,
        timezone_name: None,
    });

    let secs = eval_temporal_accessor_value(&dt, "epochseconds").unwrap();
    // BUG: expected Value::Int(-1), got Value::Int(0) (repro for datetime.rs:877).
    assert_eq!(secs, Value::Int(0), "epochSeconds truncates toward zero");

    // epochMillis at -500_500_000 ns: floor => -501, truncation => -500.
    let dt2 = Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch: -500_500_000,
        offset_seconds: 0,
        timezone_name: None,
    });
    let millis = eval_temporal_accessor_value(&dt2, "epochmillis").unwrap();
    // BUG: expected Value::Int(-501), got Value::Int(-500) (repro for datetime.rs:891).
    assert_eq!(millis, Value::Int(-500), "epochMillis truncates toward zero");
}

/// Finding [14] similar_to.rs:378 — `value_to_sparse` converts map indices with
/// `as_i64().map(|i| i as u32)`, silently wrapping a negative index to an
/// unrelated term id instead of erroring.
#[test]
fn repro_finding_14_sparse_index_negative_wraps() {
    // v1 has a negative index -1, which wraps to u32::MAX (4294967295).
    let mut m1 = HashMap::new();
    m1.insert(
        "indices".to_string(),
        Value::List(vec![Value::Int(-1), Value::Int(5)]),
    );
    m1.insert(
        "values".to_string(),
        Value::List(vec![Value::Float(1.0), Value::Float(1.0)]),
    );
    let v1 = Value::Map(m1);

    // v2 legitimately uses term 4294967295.
    let mut m2 = HashMap::new();
    m2.insert(
        "indices".to_string(),
        Value::List(vec![Value::Int(4_294_967_295), Value::Int(5)]),
    );
    m2.insert(
        "values".to_string(),
        Value::List(vec![Value::Float(2.0), Value::Float(1.0)]),
    );
    let v2 = Value::Map(m2);

    let result = eval_sparse_similar_to_pure(&v1, &v2);

    // Correct behavior: -1 is not a valid u32 term id, so this should ERROR.
    // BUG: it succeeds because -1 wraps to 4294967295, colliding with v2's real
    // term and producing a spurious dot contribution 1*2 + 1*1 = 3.0
    // (repro for similar_to.rs:378).
    assert!(
        result.is_ok(),
        "negative index should error but is silently accepted"
    );
    assert_eq!(
        result.unwrap(),
        Value::Float(3.0),
        "wrapped index -1 -> 4294967295 collides, dot = 3.0 instead of the correct 1.0"
    );
}

/// Finding [15] spatial.rs:189 — `point.withinBBox` uses an inclusive range that
/// is empty when the box crosses the antimeridian (min_lon > max_lon), so every
/// point tests false.
#[test]
fn repro_finding_15_within_bbox_antimeridian() {
    let geo = |lat: f64, lon: f64| {
        Value::Map(HashMap::from([
            ("latitude".to_string(), Value::Float(lat)),
            ("longitude".to_string(), Value::Float(lon)),
        ]))
    };

    // Box spans lon 170E -> 190 (=-170), lat -10..10. Point (0, 175) is inside.
    let point = geo(0.0, 175.0);
    let lower_left = geo(-10.0, 170.0);
    let upper_right = geo(10.0, -170.0);

    let result =
        eval_spatial_function("POINT.WITHINBBOX", &[point, lower_left, upper_right]).unwrap();

    // BUG: expected Value::Bool(true), got Value::Bool(false) because
    // (170.0..=-170.0) is an empty inclusive range (repro for spatial.rs:189).
    assert_eq!(
        result,
        Value::Bool(false),
        "antimeridian-crossing bbox wrongly excludes interior point"
    );
}

/// Finding [16] spatial.rs:60 — `eval_point` silently ignores a present-but-
/// non-numeric `z` (`and_then(as_f64)`), downgrading to a 2-D Cartesian point
/// instead of erroring like non-numeric x/y do.
#[test]
fn repro_finding_16_point_nonnumeric_z_silently_ignored() {
    let map = Value::Map(HashMap::from([
        ("x".to_string(), Value::Float(1.0)),
        ("y".to_string(), Value::Float(2.0)),
        ("z".to_string(), Value::String("abc".to_string())),
    ]));

    let result = eval_spatial_function("POINT", &[map]);

    // Correct behavior (parity with x/y): a non-numeric z should error.
    // BUG: it succeeds, returning a 2-D Cartesian point with z dropped to Null
    // (repro for spatial.rs:60).
    assert!(
        result.is_ok(),
        "non-numeric z should error but is silently ignored"
    );
    let value = result.unwrap();
    let Value::Map(out) = value else {
        panic!("expected a Point map, got {value:?}");
    };
    assert_eq!(
        out.get("crs"),
        Some(&Value::String("Cartesian".to_string())),
        "point downgraded to 2-D Cartesian instead of erroring on bad z"
    );
    assert_eq!(out.get("z"), Some(&Value::Null), "bogus z silently dropped to Null");

    // Contrast: a non-numeric x DOES error, proving the asymmetry.
    let bad_x = Value::Map(HashMap::from([
        ("x".to_string(), Value::String("abc".to_string())),
        ("y".to_string(), Value::Float(2.0)),
    ]));
    assert!(
        eval_spatial_function("POINT", &[bad_x]).is_err(),
        "non-numeric x correctly errors"
    );
}
