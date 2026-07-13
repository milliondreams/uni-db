//! Repro for crates/uni-common/src/value.rs:396
//!
//! `Display for TemporalValue::Date` uses the unchecked `epoch +
//! chrono::Duration::days(days_since_epoch)`. chrono's `Add<TimeDelta> for
//! NaiveDate` calls `.checked_add_signed(rhs).expect("`NaiveDate + TimeDelta`
//! overflowed")`, so an out-of-range `days_since_epoch` (the field is a plain
//! `i32`, unconstrained — e.g. decoded from a raw Arrow Date32 column) PANICS
//! inside a `Display` impl. The sibling `to_date()` uses `checked_add_signed`
//! and returns `None` gracefully.

use uni_common::value::TemporalValue;

#[test]
fn date_display_panics_on_out_of_range_days() {
    // A value far outside chrono's valid NaiveDate range.
    let v = TemporalValue::Date {
        days_since_epoch: i32::MAX,
    };

    // Sibling conversion degrades gracefully (returns None), proving the value
    // is merely out of range, not otherwise malformed.
    assert!(
        v.to_date().is_none(),
        "to_date() should return None on overflow (graceful)"
    );

    // FIXED: Display now degrades gracefully like to_date() by using a checked
    // add and saturating to chrono's representable range on overflow, so it
    // never panics. (fix for crates/uni-common/src/value.rs:396)
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {})); // silence any panic backtrace noise
    let result = std::panic::catch_unwind(|| format!("{}", v));
    std::panic::set_hook(prev_hook);

    let rendered = result.expect(
        "formatting Date{days_since_epoch: i32::MAX} must not panic (should saturate gracefully)",
    );
    // A positive out-of-range value saturates to chrono's maximum date.
    assert_eq!(
        rendered,
        format!("{}", chrono::NaiveDate::MAX.format("%Y-%m-%d")),
        "out-of-range positive days should saturate to NaiveDate::MAX"
    );
}
