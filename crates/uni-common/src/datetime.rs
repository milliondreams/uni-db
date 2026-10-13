// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Canonical datetime parsing shared across the workspace.
//!
//! This lives in `uni-common` rather than beside the other temporal helpers in
//! `uni-query-functions` because the storage layer needs it too, and
//! `uni-query-functions` depends on `uni-store` — putting it there would make
//! `uni-store` -> `uni-query-functions` a dependency cycle. It is pure chrono,
//! so the lowest crate is a natural home.

use anyhow::{Result, anyhow};
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};

/// Parse a datetime string to UTC.
///
/// Supports multiple formats:
/// - RFC3339 (e.g., "2023-01-01T00:00:00Z")
/// - "%Y-%m-%d %H:%M:%S %z" (e.g., "2023-01-01 00:00:00 +0000")
/// - "%Y-%m-%d %H:%M:%S" naive (assumed UTC)
///
/// This is the canonical datetime parsing function for temporal operations
/// like `validAt`. Using a single implementation ensures consistent behavior.
///
/// # Errors
///
/// Returns an error when the string matches none of the supported formats.
pub fn parse_datetime_utc(s: &str) -> Result<DateTime<Utc>> {
    // Temporal string renderings in the engine can include a bracketed timezone
    // suffix (e.g. "2020-01-01T00:00Z[UTC]"). Strip it for parsing while keeping
    // the explicit offset/UTC marker in the base datetime.
    let s = s.trim();
    let parse_input = match s.rfind('[') {
        Some(pos) if s.ends_with(']') => &s[..pos],
        _ => s,
    };

    DateTime::parse_from_rfc3339(parse_input)
        .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
        .or_else(|_| {
            // Handle formats without seconds (e.g., "2023-01-01T00:00Z")
            if let Some(base) = parse_input.strip_suffix('Z') {
                NaiveDateTime::parse_from_str(base, "%Y-%m-%dT%H:%M")
                    .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
            } else {
                // Handle formats without seconds with offset (e.g., "2023-01-01T00:00+05:00")
                DateTime::parse_from_str(parse_input, "%Y-%m-%dT%H:%M%:z")
                    .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
            }
        })
        .or_else(|_| {
            DateTime::parse_from_str(parse_input, "%Y-%m-%d %H:%M:%S %z")
                .map(|dt: DateTime<FixedOffset>| dt.with_timezone(&Utc))
        })
        .or_else(|_| {
            NaiveDateTime::parse_from_str(parse_input, "%Y-%m-%d %H:%M:%S")
                .map(|ndt| DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc))
        })
        .map_err(|_| anyhow!("Invalid datetime format: {}", s))
}

/// Scales a duration by `factor`, as `duration * n` and `duration / n` do.
///
/// Months and days scale separately; the fraction of a month cascades into
/// days and seconds at the average Gregorian month (2 629 746 s), and the
/// fraction of a day into nanoseconds. Returns `(months, days, nanos)`.
///
/// # Examples
///
/// ```
/// use uni_common::datetime::scale_duration;
///
/// // Half of 1 month and 1 day: 15 days, 5 h 14 min 33 s of the month, 12 h.
/// assert_eq!(scale_duration(1, 1, 0, 0.5), (0, 15, 62_073_000_000_000));
/// ```
#[must_use]
pub fn scale_duration(months: i64, days: i64, nanos: i64, factor: f64) -> (i64, i64, i64) {
    const SECONDS_PER_DAY: f64 = 86_400.0;
    const NANOS_PER_SECOND: f64 = 1_000_000_000.0;
    const NANOS_PER_DAY: f64 = SECONDS_PER_DAY * NANOS_PER_SECOND;

    let months_f = months as f64 * factor;
    let whole_months = months_f.trunc() as i64;
    let frac_months = months_f.fract();

    // Cascade fractional months via average Gregorian month (2629746 seconds).
    let frac_month_seconds = frac_months * 2_629_746.0;
    let extra_days_from_months = (frac_month_seconds / SECONDS_PER_DAY).trunc();
    let remaining_secs_from_months = frac_month_seconds - extra_days_from_months * SECONDS_PER_DAY;

    let days_f = days as f64 * factor + extra_days_from_months;
    let whole_days = days_f.trunc() as i64;
    let frac_days = days_f.fract();

    let nanos_f = nanos as f64 * factor
        + remaining_secs_from_months * NANOS_PER_SECOND
        + frac_days * NANOS_PER_DAY;

    (whole_months, whole_days, nanos_f.trunc() as i64)
}
