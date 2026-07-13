// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for arrow_convert.rs:1428 (finding [5]) and arrow_convert.rs:1512
//! (finding [6]).
//!
//! `build_timestamp_column` and `build_date32_column` coerce a LIVE row that
//! is simply MISSING the property to `Some(0)` (1970-01-01T00:00:00Z / epoch
//! day 0) instead of appending NULL, because the `is_deleted || val.is_none()`
//! guard folds "missing" into the deleted-padding branch. Every sibling
//! builder appends NULL for a live-missing value.

use arrow_array::{Array, Date32Array, TimestampNanosecondArray};
use uni_common::DataType;
use uni_store::storage::arrow_convert::PropertyExtractor;

#[test]
fn repro_timestamp_missing_prop_stored_as_epoch_not_null() {
    let dt = DataType::Timestamp;
    let ex = PropertyExtractor::new("seen_at", &dt);
    // One live (not deleted) row whose property is absent.
    let arr = ex
        .build_column(1, &[false], |_| None)
        .expect("build_column");
    let ts = arr
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("timestamp array");

    // FIXED (arrow_convert.rs): a live-missing Timestamp is NULL, not epoch 0.
    assert!(
        ts.is_null(0),
        "live-missing Timestamp must be NULL, not 1970-01-01T00:00:00Z"
    );
}

#[test]
fn repro_date_missing_prop_stored_as_epoch_not_null() {
    let dt = DataType::Date;
    let ex = PropertyExtractor::new("birthday", &dt);
    let arr = ex
        .build_column(1, &[false], |_| None)
        .expect("build_column");
    let d = arr
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date32 array");

    // FIXED (arrow_convert.rs): a live-missing Date is NULL, not day 0.
    assert!(
        d.is_null(0),
        "live-missing Date must be NULL, not 1970-01-01"
    );
}

/// Control: the sibling Int64 builder correctly appends NULL for a
/// live-missing value, confirming the Timestamp/Date behaviour is the outlier.
#[test]
fn control_int64_missing_prop_is_null() {
    let dt = DataType::Int64;
    let ex = PropertyExtractor::new("count", &dt);
    let arr = ex
        .build_column(1, &[false], |_| None)
        .expect("build_column");
    assert!(
        arr.is_null(0),
        "control: live-missing Int64 must be NULL (sibling correct behaviour)"
    );
}
