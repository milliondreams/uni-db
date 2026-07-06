//! Runnable repros for two verified correctness findings in `uni-btic`.
//!
//! Each test exercises the REAL public API with REAL inputs and asserts on the
//! OBSERVED (buggy) behavior so CI stays green. The correct-behavior expectation
//! is documented inline. Remove the `// BUG:` assertion and restore the
//! commented "correct" assertion once each defect is fixed.

use uni_btic::btic::Btic;
use uni_btic::parse::parse_btic_literal;

/// Repro for crates/uni-btic/src/parse.rs:174
///
/// The formats_and_gran table registers ("%Y-%m-%dT%H", Granularity::Hour), but
/// chrono's NaiveDateTime::parse_from_str cannot build a datetime from an
/// hour-only pattern (it returns Err(NOT_ENOUGH) because the minute field is
/// unset). So the Hour-granularity branch is dead: an hour-only datetime literal
/// can never parse.
///
/// Correct behavior: parse_btic_literal("2024-06-15T14") should return Ok with
/// Granularity::Hour anchored to 2024-06-15T14:00:00 UTC.
/// Actual behavior: Err(ParseError("cannot parse datetime '2024-06-15T14'")).
#[test]
fn repro_hour_granularity_datetime_never_parses() {
    let result = parse_btic_literal("2024-06-15T14");

    // Sanity: a minute-anchored input DOES parse, proving only hour-only is broken.
    let minute = parse_btic_literal("2024-06-15T14:30");
    assert!(
        minute.is_ok(),
        "minute-anchored literal should parse; got {minute:?}"
    );

    // BUG: expected Ok(Granularity::Hour), got Err (repro for parse.rs:174).
    assert!(
        result.is_err(),
        "hour-only literal currently fails to parse; observed {result:?}"
    );
    let err = format!("{:?}", result.unwrap_err());
    assert!(
        err.contains("cannot parse datetime"),
        "expected 'cannot parse datetime' error, got: {err}"
    );

    // Correct behavior (uncomment once fixed):
    // let b = result.unwrap();
    // assert_eq!(b.lo_granularity(), uni_btic::Granularity::Hour);
}

/// Repro for crates/uni-btic/src/btic.rs:152
///
/// duration_ms() guards only against the exact sentinels NEG_INF / POS_INF, then
/// computes `hi - lo` with unchecked i64 subtraction. A fully valid interval with
/// near-sentinel finite bounds (lo = i64::MIN+1, hi = i64::MAX-1) passes every
/// construction invariant but overflows the subtraction: in debug builds this
/// panics ("attempt to subtract with overflow"); in release it wraps to a
/// negative, nonsensical duration.
///
/// Correct behavior: duration_ms() should return a checked/saturated width or a
/// documented None, never panic or return a negative duration.
#[test]
fn repro_duration_ms_overflow_on_wide_valid_interval() {
    // A valid Btic: lo < hi, meta=0 passes all invariants, neither bound is an
    // exact sentinel so INV-6 does not apply.
    let b = Btic::new(i64::MIN + 1, i64::MAX - 1, 0).expect("wide interval is valid");
    assert!(b.is_finite(), "both bounds finite, so callers expect Some");

    // FIXED (btic.rs): duration_ms uses checked_sub, so an i64-overflowing width
    // returns None instead of panicking (debug) or wrapping negative (release).
    assert!(
        matches!(b.duration_ms(), Some(v) if v >= 0) || b.duration_ms().is_none(),
        "duration_ms must be overflow-safe: non-negative Some or None"
    );
    assert_eq!(
        b.duration_ms(),
        None,
        "a width exceeding i64::MAX must return None, not a wrapped value"
    );
}
