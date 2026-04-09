use crate::certainty::Certainty;
use crate::error::BticError;
use crate::granularity::Granularity;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

/// Sentinel value representing negative infinity (lower bound unbounded).
pub const NEG_INF: i64 = i64::MIN;
/// Sentinel value representing positive infinity (upper bound unbounded).
pub const POS_INF: i64 = i64::MAX;
/// XOR mask to flip sign bit for unsigned/memcmp-compatible ordering.
pub const SIGN_FLIP: u64 = 0x8000_0000_0000_0000;

/// A Binary Temporal Interval Codec value.
///
/// Represents a half-open temporal interval `[lo, hi)` in milliseconds since
/// the Unix epoch (1970-01-01T00:00:00.000Z), with per-bound granularity and
/// certainty metadata packed into a 64-bit meta word.
///
/// The 24-byte packed canonical form supports `memcmp`-based ordering for
/// B-tree and LSM key storage.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Btic {
    lo: i64,
    hi: i64,
    meta: u64,
}

impl Btic {
    /// Construct a new BTIC value, validating all invariants (INV-1 through INV-7).
    pub fn new(lo: i64, hi: i64, meta: u64) -> Result<Self, BticError> {
        let btic = Self { lo, hi, meta };
        btic.validate()?;
        Ok(btic)
    }

    /// Construct without validation. Caller must guarantee all invariants hold.
    ///
    /// # Safety (logical)
    /// This is not `unsafe` in the Rust memory-safety sense, but producing an
    /// invalid BTIC can cause incorrect comparison results and storage corruption.
    #[allow(dead_code)]
    pub(crate) fn new_unchecked(lo: i64, hi: i64, meta: u64) -> Self {
        Self { lo, hi, meta }
    }

    /// Validate all BTIC invariants on this value.
    pub fn validate(&self) -> Result<(), BticError> {
        // INV-1: lo < hi
        if self.lo >= self.hi {
            return Err(BticError::BoundOrdering {
                lo: self.lo,
                hi: self.hi,
            });
        }

        // INV-5: granularity codes in range
        let lo_gran_code = ((self.meta >> 60) & 0xF) as u8;
        let hi_gran_code = ((self.meta >> 56) & 0xF) as u8;
        if lo_gran_code > 0xA {
            return Err(BticError::GranularityRange(lo_gran_code));
        }
        if hi_gran_code > 0xA {
            return Err(BticError::GranularityRange(hi_gran_code));
        }

        // INV-4: version=0, flags=0, reserved=0
        let version = ((self.meta >> 48) & 0xF) as u8;
        let flags = ((self.meta >> 32) & 0xFFFF) as u16;
        let reserved = (self.meta & 0xFFFF_FFFF) as u32;
        if version != 0 || flags != 0 || reserved != 0 {
            return Err(BticError::ReservedBits);
        }

        // INV-6: sentinel bounds must have zeroed granularity and certainty
        if self.lo == NEG_INF {
            let lo_cert_code = ((self.meta >> 54) & 0x3) as u8;
            if lo_gran_code != 0 || lo_cert_code != 0 {
                return Err(BticError::SentinelMetadata);
            }
        }
        if self.hi == POS_INF {
            let hi_cert_code = ((self.meta >> 52) & 0x3) as u8;
            if hi_gran_code != 0 || hi_cert_code != 0 {
                return Err(BticError::SentinelMetadata);
            }
        }

        // INV-2 (sentinel exclusivity) is implied by INV-1 + INV-6:
        // INV-1 prevents lo=POS_INF and hi=NEG_INF; INV-6 ensures sentinel metadata is zeroed.

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Meta word construction
    // -----------------------------------------------------------------------

    /// Build a meta word from granularity and certainty values.
    pub fn build_meta(
        lo_gran: Granularity,
        hi_gran: Granularity,
        lo_cert: Certainty,
        hi_cert: Certainty,
    ) -> u64 {
        ((lo_gran.code() as u64) << 60)
            | ((hi_gran.code() as u64) << 56)
            | ((lo_cert.code() as u64) << 54)
            | ((hi_cert.code() as u64) << 52)
        // version=0, flags=0, reserved=0 → remaining bits are 0
    }

    // -----------------------------------------------------------------------
    // Meta word extraction
    // -----------------------------------------------------------------------

    /// Lower bound granularity.
    pub fn lo_granularity(&self) -> Granularity {
        Granularity::from_code(((self.meta >> 60) & 0xF) as u8).expect("validated on construction")
    }

    /// Upper bound granularity.
    pub fn hi_granularity(&self) -> Granularity {
        Granularity::from_code(((self.meta >> 56) & 0xF) as u8).expect("validated on construction")
    }

    /// Lower bound certainty.
    pub fn lo_certainty(&self) -> Certainty {
        Certainty::from_code(((self.meta >> 54) & 0x3) as u8).expect("validated on construction")
    }

    /// Upper bound certainty.
    pub fn hi_certainty(&self) -> Certainty {
        Certainty::from_code(((self.meta >> 52) & 0x3) as u8).expect("validated on construction")
    }

    /// Version field (must be 0 in v1).
    pub fn version(&self) -> u8 {
        ((self.meta >> 48) & 0xF) as u8
    }

    // -----------------------------------------------------------------------
    // Field accessors
    // -----------------------------------------------------------------------

    /// Lower bound in milliseconds since epoch. `i64::MIN` means -infinity.
    pub fn lo(&self) -> i64 {
        self.lo
    }

    /// Upper bound in milliseconds since epoch. `i64::MAX` means +infinity.
    pub fn hi(&self) -> i64 {
        self.hi
    }

    /// Raw meta word.
    pub fn meta(&self) -> u64 {
        self.meta
    }

    /// Duration in milliseconds. `None` if either bound is infinite.
    pub fn duration_ms(&self) -> Option<i64> {
        if self.lo == NEG_INF || self.hi == POS_INF {
            None
        } else {
            Some(self.hi - self.lo)
        }
    }

    /// True if this is an instant (one millisecond wide).
    pub fn is_instant(&self) -> bool {
        self.hi == self.lo + 1
    }

    /// True if either bound is infinite.
    pub fn is_unbounded(&self) -> bool {
        self.lo == NEG_INF || self.hi == POS_INF
    }

    /// True if both bounds are finite.
    pub fn is_finite(&self) -> bool {
        !self.is_unbounded()
    }
}

// ---------------------------------------------------------------------------
// Ordering: (lo, hi, meta) lexicographic — matches memcmp on packed form
// ---------------------------------------------------------------------------

impl PartialOrd for Btic {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Btic {
    fn cmp(&self, other: &Self) -> Ordering {
        self.lo
            .cmp(&other.lo)
            .then(self.hi.cmp(&other.hi))
            .then(self.meta.cmp(&other.meta))
    }
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

impl fmt::Display for Btic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let lo_str = if self.lo == NEG_INF {
            "-inf".to_string()
        } else {
            format_ms_as_datetime(self.lo)
        };
        let hi_str = if self.hi == POS_INF {
            "+inf".to_string()
        } else {
            format_ms_as_datetime(self.hi)
        };

        write!(f, "[{lo_str}, {hi_str})")?;

        // Append granularity info
        if self.lo != NEG_INF && self.hi != POS_INF {
            let lg = self.lo_granularity();
            let hg = self.hi_granularity();
            if lg == hg {
                write!(f, " ~{}", lg.name())?;
            } else {
                write!(f, " {}/{}", lg.name(), hg.name())?;
            }
        } else if self.lo != NEG_INF {
            write!(f, " {}/", self.lo_granularity().name())?;
        } else if self.hi != POS_INF {
            write!(f, " /{}", self.hi_granularity().name())?;
        }

        // Append certainty if non-definite
        let lc = self.lo_certainty();
        let hc = self.hi_certainty();
        if lc != Certainty::Definite || hc != Certainty::Definite {
            if lc == hc {
                write!(f, " [{}]", lc.name())?;
            } else {
                write!(f, " [{}/{}]", lc.name(), hc.name())?;
            }
        }

        Ok(())
    }
}

impl fmt::Debug for Btic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Btic")
            .field("lo", &self.lo)
            .field("hi", &self.hi)
            .field("meta", &format_args!("{:#018x}", self.meta))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format milliseconds since epoch as an ISO 8601 datetime string.
fn format_ms_as_datetime(ms: i64) -> String {
    use chrono::{DateTime, Utc};

    let secs = ms.div_euclid(1000);
    let nanos = (ms.rem_euclid(1000) * 1_000_000) as u32;
    match DateTime::<Utc>::from_timestamp(secs, nanos) {
        Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        None => format!("{ms}ms"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_instant_at_epoch() {
        let meta = Btic::build_meta(
            Granularity::Millisecond,
            Granularity::Millisecond,
            Certainty::Definite,
            Certainty::Definite,
        );
        let b = Btic::new(0, 1, meta).unwrap();
        assert!(b.is_instant());
        assert!(b.is_finite());
        assert!(!b.is_unbounded());
        assert_eq!(b.duration_ms(), Some(1));
    }

    #[test]
    fn inv1_lo_ge_hi_rejected() {
        let meta = Btic::build_meta(
            Granularity::Day,
            Granularity::Day,
            Certainty::Definite,
            Certainty::Definite,
        );
        assert!(Btic::new(100, 100, meta).is_err());
        assert!(Btic::new(200, 100, meta).is_err());
    }

    #[test]
    fn inv5_bad_granularity_rejected() {
        // Manually set granularity code 0xF in the meta word
        let bad_meta = 0xF700_0000_0000_0000u64; // lo_gran=0xF
        assert!(Btic::new(0, 1, bad_meta).is_err());
    }

    #[test]
    fn inv6_sentinel_metadata_must_be_zero() {
        // NEG_INF lo with non-zero lo_granularity
        let bad_meta = Btic::build_meta(
            Granularity::Year,
            Granularity::Month,
            Certainty::Definite,
            Certainty::Definite,
        );
        assert!(Btic::new(NEG_INF, 1000, bad_meta).is_err());
    }

    #[test]
    fn unbounded_intervals() {
        let meta_lo_zero = Btic::build_meta(
            Granularity::Millisecond,
            Granularity::Month,
            Certainty::Definite,
            Certainty::Definite,
        );
        let left_unbounded = Btic::new(NEG_INF, 1000, meta_lo_zero).unwrap();
        assert!(left_unbounded.is_unbounded());
        assert!(!left_unbounded.is_finite());
        assert_eq!(left_unbounded.duration_ms(), None);

        let meta_hi_zero = Btic::build_meta(
            Granularity::Month,
            Granularity::Millisecond,
            Certainty::Definite,
            Certainty::Definite,
        );
        let right_unbounded = Btic::new(1000, POS_INF, meta_hi_zero).unwrap();
        assert!(right_unbounded.is_unbounded());
        assert_eq!(right_unbounded.duration_ms(), None);
    }

    #[test]
    fn ordering_lo_first() {
        let meta = Btic::build_meta(
            Granularity::Day,
            Granularity::Day,
            Certainty::Definite,
            Certainty::Definite,
        );
        let a = Btic::new(100, 200, meta).unwrap();
        let b = Btic::new(150, 200, meta).unwrap();
        assert!(a < b);
    }

    #[test]
    fn ordering_hi_second() {
        let meta = Btic::build_meta(
            Granularity::Day,
            Granularity::Day,
            Certainty::Definite,
            Certainty::Definite,
        );
        let a = Btic::new(100, 200, meta).unwrap();
        let b = Btic::new(100, 300, meta).unwrap();
        assert!(a < b);
    }

    #[test]
    fn ordering_meta_third() {
        let meta_a = Btic::build_meta(
            Granularity::Day,
            Granularity::Day,
            Certainty::Definite,
            Certainty::Definite,
        );
        let meta_b = Btic::build_meta(
            Granularity::Year,
            Granularity::Year,
            Certainty::Definite,
            Certainty::Definite,
        );
        let a = Btic::new(100, 200, meta_a).unwrap();
        let b = Btic::new(100, 200, meta_b).unwrap();
        assert!(a < b); // Day(0x4) < Year(0x7) in meta
    }

    #[test]
    fn display_finite_interval() {
        let meta = Btic::build_meta(
            Granularity::Year,
            Granularity::Year,
            Certainty::Definite,
            Certainty::Definite,
        );
        // 1985-01-01 to 1986-01-01
        let b = Btic::new(473_385_600_000, 504_921_600_000, meta).unwrap();
        let s = b.to_string();
        assert!(s.contains("1985-01-01"));
        assert!(s.contains("1986-01-01"));
        assert!(s.contains("~year"));
    }

    #[test]
    fn build_meta_roundtrip() {
        let meta = Btic::build_meta(
            Granularity::Month,
            Granularity::Day,
            Certainty::Approximate,
            Certainty::Uncertain,
        );
        let b = Btic::new(0, 1000, meta).unwrap();
        assert_eq!(b.lo_granularity(), Granularity::Month);
        assert_eq!(b.hi_granularity(), Granularity::Day);
        assert_eq!(b.lo_certainty(), Certainty::Approximate);
        assert_eq!(b.hi_certainty(), Certainty::Uncertain);
        assert_eq!(b.version(), 0);
    }
}
