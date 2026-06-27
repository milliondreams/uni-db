// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Value-level assertion helpers for Locy `FactRow` results.
//!
//! The Locy bug cluster #111/#112/#113 shipped past a fully-green suite because
//! the existing tests assert *outcomes* — query succeeds, row count, dotted
//! property access (`n.name`) — but never the actual typed VALUE of a returned
//! KEY column or typed column. A KEY column that silently projects `Value::Null`
//! (#112) or a typed column that loses its logical type (#111/#113) passes every
//! count-based check.
//!
//! These helpers close that blind spot: they assert the concrete `Value`
//! (including its type) of a named column across all rows, and they fail loudly
//! — printing the actual value and the keys present — when a column is missing,
//! `Null`, or the wrong type.
//!
//! A Locy `FactRow` is a plain `HashMap<String, Value>` read via
//! `row.get("col") -> Option<&Value>` (see `locy_issue_94_key_property_repro`);
//! the `.get::<T>()` / `.value()` accessors are on Cypher result rows, not Locy
//! rows, so every helper here operates on `&Value` from a `HashMap`.

// Rust guideline compliant

use std::collections::HashMap;

use uni_db::Value;
use uni_db::common::TemporalValue;

/// A Locy result row: column name → value.
pub type Row = HashMap<String, Value>;

/// Logical type tag for a column, used by [`assert_column_typed`].
///
/// `PointMap` reflects that there is no `Value::Point` variant — a Point
/// surfaces as a `Value::Map` of geo fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeTag {
    /// `Value::Int`.
    Int,
    /// `Value::Float`.
    Float,
    /// `Value::String`.
    Str,
    /// `Value::Bool`.
    Bool,
    /// `Value::Temporal(DateTime)`.
    DateTime,
    /// `Value::Temporal(Time)`.
    Time,
    /// `Value::Temporal(Duration)`.
    Duration,
    /// `Value::Temporal(Btic)`.
    Btic,
    /// `Value::Bytes`.
    Bytes,
    /// `Value::Vector`.
    Vector,
    /// `Value::Map` (Point has no dedicated `Value` variant).
    PointMap,
    /// `Value::Node`.
    Node,
    /// `Value::Edge`.
    Edge,
}

impl TypeTag {
    /// Returns `true` when `value` matches this logical type.
    fn matches(self, value: &Value) -> bool {
        match self {
            TypeTag::Int => matches!(value, Value::Int(_)),
            TypeTag::Float => matches!(value, Value::Float(_)),
            TypeTag::Str => matches!(value, Value::String(_)),
            TypeTag::Bool => matches!(value, Value::Bool(_)),
            TypeTag::DateTime => {
                matches!(value, Value::Temporal(TemporalValue::DateTime { .. }))
            }
            TypeTag::Time => matches!(value, Value::Temporal(TemporalValue::Time { .. })),
            TypeTag::Duration => {
                matches!(value, Value::Temporal(TemporalValue::Duration { .. }))
            }
            TypeTag::Btic => matches!(value, Value::Temporal(TemporalValue::Btic { .. })),
            TypeTag::Bytes => matches!(value, Value::Bytes(_)),
            TypeTag::Vector => matches!(value, Value::Vector(_)),
            TypeTag::PointMap => matches!(value, Value::Map(_)),
            TypeTag::Node => matches!(value, Value::Node(_)),
            TypeTag::Edge => matches!(value, Value::Edge(_)),
        }
    }
}

/// Renders the column keys present in a row for diagnostic failure messages.
fn keys_of(row: &Row) -> Vec<&String> {
    let mut keys: Vec<&String> = row.keys().collect();
    keys.sort();
    keys
}

/// Asserts that `col` in every row equals `expected` (exact value AND type).
///
/// # Panics
///
/// Panics if `rows` is empty, or any row is missing `col`, or any value differs
/// from `expected`.
pub fn assert_column_eq(rows: &[Row], col: &str, expected: &Value) {
    assert!(
        !rows.is_empty(),
        "assert_column_eq({col}): expected at least one row, got none"
    );
    for (i, row) in rows.iter().enumerate() {
        match row.get(col) {
            Some(actual) => assert_eq!(
                actual, expected,
                "row {i} column `{col}`: expected {expected:?}, got {actual:?}"
            ),
            None => panic!(
                "row {i} column `{col}` missing; present keys = {:?}",
                keys_of(row)
            ),
        }
    }
}

/// Asserts that `col` in every row is present, non-`Null`, and matches `want`.
///
/// This is the core guard for the typed-value blind spot: a column that silently
/// collapses to `Value::Null` (the #112 failure mode) or loses its logical type
/// (#111/#113) fails here even when the row count is correct.
///
/// # Panics
///
/// Panics if `rows` is empty, or any row's `col` is missing, `Null`, or a value
/// whose type does not match `want`.
pub fn assert_column_typed(rows: &[Row], col: &str, want: TypeTag) {
    assert!(
        !rows.is_empty(),
        "assert_column_typed({col}, {want:?}): expected at least one row, got none"
    );
    for (i, row) in rows.iter().enumerate() {
        match row.get(col) {
            Some(Value::Null) => panic!(
                "row {i} column `{col}`: expected {want:?}, got Value::Null \
                 (typed value lost at the Locy boundary)"
            ),
            Some(actual) => assert!(
                want.matches(actual),
                "row {i} column `{col}`: expected {want:?}, got {actual:?}"
            ),
            None => panic!(
                "row {i} column `{col}` missing; present keys = {:?}",
                keys_of(row)
            ),
        }
    }
}

/// Asserts that the multiset of `col` values equals `expected` (order-free).
///
/// Matches the dominant assertion shape in `locy_issue_94_key_property_repro`:
/// rule output order is not guaranteed, so compare value sets.
///
/// # Panics
///
/// Panics if the sorted-by-debug value lists differ.
pub fn assert_column_value_set(rows: &[Row], col: &str, expected: &[Value]) {
    let mut actual: Vec<String> = rows
        .iter()
        .map(|row| match row.get(col) {
            Some(v) => format!("{v:?}"),
            None => format!("<missing `{col}`; keys={:?}>", keys_of(row)),
        })
        .collect();
    let mut want: Vec<String> = expected.iter().map(|v| format!("{v:?}")).collect();
    actual.sort();
    want.sort();
    assert_eq!(
        actual, want,
        "column `{col}` value set mismatch: got {actual:?}, expected {want:?}"
    );
}

/// Asserts that no row has `col == Value::Null` (and that `col` is present).
///
/// The minimal blind-spot guard: cheaper than [`assert_column_typed`] when the
/// concrete type is not the point, only that the value materialized at all.
///
/// # Panics
///
/// Panics if `rows` is empty, or any row's `col` is missing or `Null`.
pub fn assert_column_non_null(rows: &[Row], col: &str) {
    assert!(
        !rows.is_empty(),
        "assert_column_non_null({col}): expected at least one row, got none"
    );
    for (i, row) in rows.iter().enumerate() {
        match row.get(col) {
            Some(Value::Null) => panic!("row {i} column `{col}`: unexpected Value::Null"),
            Some(_) => {}
            None => panic!(
                "row {i} column `{col}` missing; present keys = {:?}",
                keys_of(row)
            ),
        }
    }
}
