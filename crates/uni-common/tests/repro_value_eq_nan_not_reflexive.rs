//! Regression test for crates/uni-common/src/value.rs (Vector/SparseVector Eq).
//!
//! `Value` implements `Eq` (asserting reflexivity: `v == v` for all `v`), and
//! its `Float` arm normalizes NaN so `Float(NaN) == Float(NaN)` is `true`. The
//! `Vector` and `SparseVector` arms formerly compared `Vec<f32>` with the
//! derived IEEE-754 `==`, where `NaN != NaN`, so a `Value::Vector(vec![f32::NAN])`
//! was NOT equal to itself — violating `Eq` reflexivity and disagreeing with the
//! NaN-normalizing `Hash` impl (which silently broke `HashSet`/`HashMap` dedup).
//! The arms now compare element-wise with the same signed-zero/NaN normalization
//! as `Hash`; this test pins the reflexive behavior.

use std::collections::HashSet;
use uni_common::Value;

#[test]
fn vector_nan_is_eq_reflexive() {
    let v = Value::Vector(vec![f32::NAN]);

    // The Float arm is normalized and reflexive.
    assert_eq!(
        Value::Float(f64::NAN),
        Value::Float(f64::NAN),
        "Float(NaN) == Float(NaN) should hold (normalized arm)"
    );

    // Fixed: the Vector arm now normalizes NaN, so `v == v` holds.
    assert_eq!(
        v,
        v.clone(),
        "Value::Vector([NaN]) must be reflexively equal (Value: Eq)"
    );
}

#[test]
fn sparse_vector_nan_is_eq_reflexive() {
    let v = Value::SparseVector {
        indices: vec![0],
        values: vec![f32::NAN],
    };

    // Fixed: the SparseVector arm now normalizes NaN weights.
    assert_eq!(
        v,
        v.clone(),
        "SparseVector with NaN weight must be reflexively equal (Value: Eq)"
    );
}

#[test]
fn hashset_dedup_works_for_nan_vector() {
    // Hash normalizes NaN and Eq now agrees, so a HashSet dedups correctly.
    let mut set: HashSet<Value> = HashSet::new();
    set.insert(Value::Vector(vec![f32::NAN]));
    set.insert(Value::Vector(vec![f32::NAN]));

    assert_eq!(
        set.len(),
        1,
        "two equal NaN vectors must dedup to a single set entry"
    );
    assert!(
        set.contains(&Value::Vector(vec![f32::NAN])),
        "lookup of an inserted NaN vector must succeed (Hash/Eq agree)"
    );
}
