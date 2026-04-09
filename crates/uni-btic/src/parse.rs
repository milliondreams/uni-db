use crate::btic::Btic;
use crate::certainty::Certainty;
use crate::error::BticError;
use crate::granularity::Granularity;
use chrono::{Datelike, NaiveDate, NaiveDateTime};

/// Parse a BTIC literal string into a `Btic` value.
///
/// Supported forms (per spec section 13.5):
/// - Single granular: `"1985"`, `"1985-03"`, `"1985-03-15"`, `"1985-03-15T14:30Z"`
/// - Two-bound solidus: `"1985-03/2024-06"`, `"1985/2024-06-15"`
/// - Unbounded: `"2020-03/"`, `"/2024-06"`, `"/"`
/// - Certainty prefixes: `"~1985"` (approximate), `"?1985"` (uncertain), `"??1985"` (unknown)
/// - BCE dates: `"500 BCE"`
pub fn parse_btic_literal(s: &str) -> Result<Btic, BticError> {
    let s = s.trim();

    if s.is_empty() {
        return Err(BticError::ParseError("empty literal".into()));
    }

    // Check for solidus (interval notation)
    if let Some(slash_pos) = s.find('/') {
        let left = &s[..slash_pos];
        let right = &s[slash_pos + 1..];
        return parse_two_bound(left, right);
    }

    // Single granular expression
    parse_single(s)
}

/// Parse a two-bound interval (e.g., "1985-03/2024-06", "2020-03/", "/2024-06", "/").
fn parse_two_bound(left: &str, right: &str) -> Result<Btic, BticError> {
    let left = left.trim();
    let right = right.trim();

    let (lo, lo_gran, lo_cert) = if left.is_empty() {
        // Left-unbounded
        (i64::MIN, Granularity::Millisecond, Certainty::Definite)
    } else {
        parse_component(left)?
    };

    let (hi_raw, hi_gran, hi_cert) = if right.is_empty() {
        // Right-unbounded
        (i64::MAX, Granularity::Millisecond, Certainty::Definite)
    } else {
        let (lo_ms, gran, cert) = parse_component(right)?;
        let hi_ms = expand_granularity(lo_ms, gran)?;
        (hi_ms, gran, cert)
    };

    // Sentinel bounds already carry zeroed granularity/certainty from the
    // unbounded branches above, so build_meta handles all cases uniformly.
    let meta = Btic::build_meta(lo_gran, hi_gran, lo_cert, hi_cert);
    Btic::new(lo, hi_raw, meta)
}

/// Parse a single granular expression (e.g., "1985", "1985-03-15", "~500 BCE").
/// Both bounds are derived from the same expression.
fn parse_single(s: &str) -> Result<Btic, BticError> {
    let (lo, gran, cert) = parse_component(s)?;
    let hi = expand_granularity(lo, gran)?;

    let meta = Btic::build_meta(gran, gran, cert, cert);
    Btic::new(lo, hi, meta)
}

/// Parse a single temporal component, returning (lo_ms, granularity, certainty).
///
/// Handles certainty prefixes (`~`, `?`, `??`) and BCE suffix.
fn parse_component(s: &str) -> Result<(i64, Granularity, Certainty), BticError> {
    let s = s.trim();
    let (s, certainty) = strip_certainty_prefix(s);
    let s = s.trim();

    // Check for BCE suffix
    if let Some(bce_s) = strip_bce_suffix(s) {
        return parse_bce_year(bce_s.trim(), certainty);
    }

    parse_iso_component(s, certainty)
}

/// Strip certainty prefix from a string, returning (remaining, certainty).
fn strip_certainty_prefix(s: &str) -> (&str, Certainty) {
    if let Some(rest) = s.strip_prefix("??") {
        (rest, Certainty::Unknown)
    } else if let Some(rest) = s.strip_prefix('~') {
        (rest, Certainty::Approximate)
    } else if let Some(rest) = s.strip_prefix('?') {
        (rest, Certainty::Uncertain)
    } else {
        (s, Certainty::Definite)
    }
}

/// Check for and strip " BCE" suffix (case-insensitive).
fn strip_bce_suffix(s: &str) -> Option<&str> {
    if s.len() >= 4 && s[s.len() - 4..].eq_ignore_ascii_case(" BCE") {
        Some(&s[..s.len() - 4])
    } else if s.len() > 3 && s[s.len() - 3..].eq_ignore_ascii_case("BCE") {
        Some(&s[..s.len() - 3])
    } else {
        None
    }
}

/// Parse a BCE year like "500" into astronomical year -499.
fn parse_bce_year(
    s: &str,
    certainty: Certainty,
) -> Result<(i64, Granularity, Certainty), BticError> {
    let year: i32 = s
        .trim()
        .parse()
        .map_err(|e| BticError::ParseError(format!("invalid BCE year '{s}': {e}")))?;
    if year <= 0 {
        return Err(BticError::ParseError(format!(
            "BCE year must be positive, got {year}"
        )));
    }
    // Astronomical year: 1 BCE = year 0, 2 BCE = year -1, etc.
    let astro_year = -(year - 1);
    let lo_ms = year_to_ms(astro_year)?;
    Ok((lo_ms, Granularity::Year, certainty))
}

/// Parse an ISO 8601 component and determine its granularity.
fn parse_iso_component(
    s: &str,
    certainty: Certainty,
) -> Result<(i64, Granularity, Certainty), BticError> {
    // Try from most specific to least specific

    // Full datetime with time component (contains 'T')
    if s.contains('T') {
        return parse_datetime_component(s, certainty);
    }

    // Date-only forms: YYYY-MM-DD, YYYY-MM, YYYY
    parse_date_only_component(s, certainty)
}

/// Parse a datetime string (contains 'T').
fn parse_datetime_component(
    s: &str,
    certainty: Certainty,
) -> Result<(i64, Granularity, Certainty), BticError> {
    // Strip trailing 'Z' or timezone offset for parsing
    let (s_clean, _tz_offset_secs) = strip_timezone(s);

    // Try parsing with various precision levels
    // Full: 2024-06-15T14:30:00.000
    // Seconds: 2024-06-15T14:30:00
    // Minutes: 2024-06-15T14:30
    // Hours: 2024-06-15T14

    let formats_and_gran = [
        ("%Y-%m-%dT%H:%M:%S%.3f", Granularity::Millisecond),
        ("%Y-%m-%dT%H:%M:%S%.f", Granularity::Millisecond),
        ("%Y-%m-%dT%H:%M:%S", Granularity::Second),
        ("%Y-%m-%dT%H:%M", Granularity::Minute),
        ("%Y-%m-%dT%H", Granularity::Hour),
    ];

    for (fmt, gran) in &formats_and_gran {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s_clean, fmt) {
            let ms = datetime_to_ms(ndt);
            // Determine if this is second vs millisecond granularity
            let actual_gran = if *gran == Granularity::Millisecond {
                infer_sub_second_granularity(s_clean)
            } else {
                *gran
            };
            return Ok((ms, actual_gran, certainty));
        }
    }

    Err(BticError::ParseError(format!(
        "cannot parse datetime '{s}'"
    )))
}

/// Determine whether a datetime string has sub-second precision.
///
/// Expects a string containing `T` (the caller guarantees this).
/// Returns `Millisecond` if a decimal point appears in the time part,
/// otherwise `Second`.
fn infer_sub_second_granularity(s: &str) -> Granularity {
    let time_part = s.split_once('T').map(|(_, t)| t).unwrap_or("");
    if time_part.contains('.') {
        Granularity::Millisecond
    } else {
        Granularity::Second
    }
}

/// Parse a date-only component: YYYY-MM-DD, YYYY-MM, YYYY.
fn parse_date_only_component(
    s: &str,
    certainty: Certainty,
) -> Result<(i64, Granularity, Certainty), BticError> {
    let parts: Vec<&str> = s.split('-').collect();

    match parts.len() {
        3 => {
            // YYYY-MM-DD
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| BticError::ParseError(format!("invalid date '{s}': {e}")))?;
            let ms = date_to_ms(date);
            Ok((ms, Granularity::Day, certainty))
        }
        2 => {
            // YYYY-MM
            let year: i32 = parts[0]
                .parse()
                .map_err(|e| BticError::ParseError(format!("invalid year in '{s}': {e}")))?;
            let month: u32 = parts[1]
                .parse()
                .map_err(|e| BticError::ParseError(format!("invalid month in '{s}': {e}")))?;
            if !(1..=12).contains(&month) {
                return Err(BticError::ParseError(format!(
                    "month {month} out of range 1-12"
                )));
            }
            let date = NaiveDate::from_ymd_opt(year, month, 1).ok_or_else(|| {
                BticError::ParseError(format!("invalid date {year}-{month:02}-01"))
            })?;
            let ms = date_to_ms(date);
            Ok((ms, Granularity::Month, certainty))
        }
        1 => {
            // YYYY (just a year)
            let year: i32 = parts[0]
                .parse()
                .map_err(|e| BticError::ParseError(format!("invalid year '{s}': {e}")))?;
            let ms = year_to_ms(year)?;
            Ok((ms, Granularity::Year, certainty))
        }
        _ => Err(BticError::ParseError(format!(
            "cannot parse date component '{s}'"
        ))),
    }
}

/// Strip timezone suffix from a datetime string, returning (cleaned, offset_secs).
fn strip_timezone(s: &str) -> (&str, i32) {
    if let Some(stripped) = s.strip_suffix('Z') {
        return (stripped, 0);
    }
    if let Some(stripped) = s.strip_suffix('z') {
        return (stripped, 0);
    }

    // Look for +HH:MM or -HH:MM at the end
    let bytes = s.as_bytes();
    if bytes.len() >= 6 {
        let sign_pos = bytes.len() - 6;
        if (bytes[sign_pos] == b'+' || bytes[sign_pos] == b'-') && bytes[sign_pos + 3] == b':' {
            let sign = if bytes[sign_pos] == b'+' { 1 } else { -1 };
            if let (Ok(h), Ok(m)) = (
                s[sign_pos + 1..sign_pos + 3].parse::<i32>(),
                s[sign_pos + 4..sign_pos + 6].parse::<i32>(),
            ) {
                let offset = sign * (h * 3600 + m * 60);
                return (&s[..sign_pos], offset);
            }
        }
    }

    (s, 0)
}

/// Convert a NaiveDate to milliseconds since epoch.
fn date_to_ms(date: NaiveDate) -> i64 {
    let dt = date.and_hms_opt(0, 0, 0).unwrap();
    datetime_to_ms(dt)
}

/// Convert a NaiveDateTime to milliseconds since epoch.
fn datetime_to_ms(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_millis()
}

/// Convert an astronomical year to milliseconds since epoch (start of year).
fn year_to_ms(year: i32) -> Result<i64, BticError> {
    let date = NaiveDate::from_ymd_opt(year, 1, 1)
        .ok_or_else(|| BticError::ParseError(format!("year {year} out of range")))?;
    Ok(date_to_ms(date))
}

/// Expand a lower-bound ms timestamp by one unit of the given granularity
/// to produce the upper bound. Uses calendar-aware arithmetic for variable-width units.
fn expand_granularity(lo_ms: i64, gran: Granularity) -> Result<i64, BticError> {
    match gran {
        Granularity::Millisecond => Ok(lo_ms + 1),
        Granularity::Second => Ok(lo_ms + 1_000),
        Granularity::Minute => Ok(lo_ms + 60_000),
        Granularity::Hour => Ok(lo_ms + 3_600_000),
        Granularity::Day => Ok(lo_ms + 86_400_000),
        // Variable-width calendar units require chrono
        Granularity::Month => expand_months(lo_ms, 1),
        Granularity::Quarter => expand_months(lo_ms, 3),
        Granularity::Year => expand_years(lo_ms, 1),
        Granularity::Decade => expand_years(lo_ms, 10),
        Granularity::Century => expand_years(lo_ms, 100),
        Granularity::Millennium => expand_years(lo_ms, 1000),
    }
}

/// Add N months to a timestamp (calendar-aware).
fn expand_months(lo_ms: i64, months: i32) -> Result<i64, BticError> {
    let dt = ms_to_datetime(lo_ms)?;
    let date = dt.date();

    let mut year = date.year();
    let mut month = date.month() as i32 + months;
    while month > 12 {
        month -= 12;
        year += 1;
    }
    while month < 1 {
        month += 12;
        year -= 1;
    }

    let next_date = NaiveDate::from_ymd_opt(year, month as u32, 1)
        .ok_or_else(|| BticError::ParseError(format!("date overflow: {year}-{month:02}-01")))?;
    Ok(date_to_ms(next_date))
}

/// Add N years to a timestamp (calendar-aware).
fn expand_years(lo_ms: i64, years: i32) -> Result<i64, BticError> {
    let dt = ms_to_datetime(lo_ms)?;
    let date = dt.date();
    let next_date = NaiveDate::from_ymd_opt(date.year() + years, 1, 1).ok_or_else(|| {
        BticError::ParseError(format!("date overflow: year {}", date.year() + years))
    })?;
    Ok(date_to_ms(next_date))
}

/// Convert milliseconds since epoch to a NaiveDateTime.
fn ms_to_datetime(ms: i64) -> Result<NaiveDateTime, BticError> {
    let secs = ms.div_euclid(1000);
    let nsecs = (ms.rem_euclid(1000) * 1_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs)
        .map(|dt| dt.naive_utc())
        .ok_or_else(|| BticError::ParseError(format!("timestamp {ms}ms out of range")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_btic(
        s: &str,
        expected_lo: i64,
        expected_hi: i64,
        lo_gran: Granularity,
        hi_gran: Granularity,
    ) {
        let b = parse_btic_literal(s).unwrap_or_else(|e| panic!("parse '{s}' failed: {e}"));
        assert_eq!(b.lo(), expected_lo, "lo mismatch for '{s}'");
        assert_eq!(b.hi(), expected_hi, "hi mismatch for '{s}'");
        assert_eq!(b.lo_granularity(), lo_gran, "lo_gran mismatch for '{s}'");
        assert_eq!(b.hi_granularity(), hi_gran, "hi_gran mismatch for '{s}'");
    }

    #[test]
    fn year_1985() {
        assert_btic(
            "1985",
            473_385_600_000,
            504_921_600_000,
            Granularity::Year,
            Granularity::Year,
        );
    }

    #[test]
    fn month_march_1985() {
        assert_btic(
            "1985-03",
            478_483_200_000,
            481_161_600_000,
            Granularity::Month,
            Granularity::Month,
        );
    }

    #[test]
    fn day_1985_03_15() {
        assert_btic(
            "1985-03-15",
            479_692_800_000,
            479_779_200_000,
            Granularity::Day,
            Granularity::Day,
        );
    }

    #[test]
    fn epoch_instant() {
        let b = parse_btic_literal("1970-01-01T00:00:00.000Z").unwrap();
        assert_eq!(b.lo(), 0);
        assert_eq!(b.hi(), 1);
        assert!(b.is_instant());
        assert_eq!(b.lo_granularity(), Granularity::Millisecond);
    }

    #[test]
    fn two_bound_solidus() {
        let b = parse_btic_literal("1985-03/2024-06").unwrap();
        assert_eq!(b.lo(), 478_483_200_000); // 1985-03-01
        assert_eq!(b.hi(), 1_719_792_000_000); // 2024-07-01
        assert_eq!(b.lo_granularity(), Granularity::Month);
        assert_eq!(b.hi_granularity(), Granularity::Month);
    }

    #[test]
    fn mixed_granularity_solidus() {
        let b = parse_btic_literal("1985-03/2024-06-15").unwrap();
        assert_eq!(b.lo(), 478_483_200_000); // 1985-03-01
        assert_eq!(b.hi(), 1_718_496_000_000); // 2024-06-16
        assert_eq!(b.lo_granularity(), Granularity::Month);
        assert_eq!(b.hi_granularity(), Granularity::Day);
    }

    #[test]
    fn right_unbounded() {
        let b = parse_btic_literal("2020-03/").unwrap();
        assert_eq!(b.lo(), 1_583_020_800_000); // 2020-03-01
        assert_eq!(b.hi(), i64::MAX);
        assert!(b.is_unbounded());
        assert_eq!(b.lo_granularity(), Granularity::Month);
    }

    #[test]
    fn left_unbounded() {
        let b = parse_btic_literal("/2024-06").unwrap();
        assert_eq!(b.lo(), i64::MIN);
        assert_eq!(b.hi(), 1_719_792_000_000); // 2024-07-01
    }

    #[test]
    fn fully_unbounded() {
        let b = parse_btic_literal("/").unwrap();
        assert_eq!(b.lo(), i64::MIN);
        assert_eq!(b.hi(), i64::MAX);
        assert_eq!(b.meta(), 0);
    }

    #[test]
    fn certainty_approximate() {
        let b = parse_btic_literal("~1985").unwrap();
        assert_eq!(b.lo_certainty(), Certainty::Approximate);
        assert_eq!(b.hi_certainty(), Certainty::Approximate);
    }

    #[test]
    fn certainty_uncertain() {
        let b = parse_btic_literal("?1985").unwrap();
        assert_eq!(b.lo_certainty(), Certainty::Uncertain);
        assert_eq!(b.hi_certainty(), Certainty::Uncertain);
    }

    #[test]
    fn certainty_unknown() {
        let b = parse_btic_literal("??1985").unwrap();
        assert_eq!(b.lo_certainty(), Certainty::Unknown);
        assert_eq!(b.hi_certainty(), Certainty::Unknown);
    }

    #[test]
    fn mixed_certainty_solidus() {
        let b = parse_btic_literal("~1985/2024-06").unwrap();
        assert_eq!(b.lo_certainty(), Certainty::Approximate);
        assert_eq!(b.hi_certainty(), Certainty::Definite);
    }

    #[test]
    fn bce_date() {
        let b = parse_btic_literal("500 BCE").unwrap();
        // Astronomical year -499
        assert_eq!(b.lo_granularity(), Granularity::Year);
        assert_eq!(b.hi_granularity(), Granularity::Year);
        // Verify it's a year-long interval
        assert!(b.duration_ms().unwrap() > 0);
    }

    #[test]
    fn approximate_bce() {
        let b = parse_btic_literal("~500 BCE").unwrap();
        assert_eq!(b.lo_certainty(), Certainty::Approximate);
        assert_eq!(b.hi_certainty(), Certainty::Approximate);
        assert_eq!(b.lo_granularity(), Granularity::Year);
    }

    #[test]
    fn second_granularity() {
        let b = parse_btic_literal("1985-03-15T14:30:00Z").unwrap();
        assert_eq!(b.lo_granularity(), Granularity::Second);
        assert_eq!(b.duration_ms(), Some(1000));
    }

    #[test]
    fn minute_granularity() {
        let b = parse_btic_literal("1985-03-15T14:30Z").unwrap();
        assert_eq!(b.lo_granularity(), Granularity::Minute);
        assert_eq!(b.duration_ms(), Some(60_000));
    }

    #[test]
    fn empty_literal_rejected() {
        assert!(parse_btic_literal("").is_err());
    }

    #[test]
    fn invalid_literal_rejected() {
        assert!(parse_btic_literal("not-a-date").is_err());
    }
}
