use crate::error::BticError;

/// Granularity of a BTIC bound — the calendrical unit from which it was derived.
///
/// Granularity is metadata only; it does not affect comparison or ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum Granularity {
    Millisecond = 0,
    Second = 1,
    Minute = 2,
    Hour = 3,
    Day = 4,
    Month = 5,
    Quarter = 6,
    Year = 7,
    Decade = 8,
    Century = 9,
    Millennium = 10,
}

impl Granularity {
    /// Construct from a 4-bit code (0x0..=0xA). Returns error for reserved codes.
    pub fn from_code(code: u8) -> Result<Self, BticError> {
        match code {
            0 => Ok(Self::Millisecond),
            1 => Ok(Self::Second),
            2 => Ok(Self::Minute),
            3 => Ok(Self::Hour),
            4 => Ok(Self::Day),
            5 => Ok(Self::Month),
            6 => Ok(Self::Quarter),
            7 => Ok(Self::Year),
            8 => Ok(Self::Decade),
            9 => Ok(Self::Century),
            10 => Ok(Self::Millennium),
            _ => Err(BticError::GranularityRange(code)),
        }
    }

    /// The numeric code for this granularity (0x0..=0xA).
    pub fn code(self) -> u8 {
        self as u8
    }

    /// Human-readable name for this granularity.
    pub fn name(self) -> &'static str {
        match self {
            Self::Millisecond => "millisecond",
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
            Self::Decade => "decade",
            Self::Century => "century",
            Self::Millennium => "millennium",
        }
    }

    /// Look up by name (case-insensitive).
    pub fn from_name(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "millisecond" | "ms" => Some(Self::Millisecond),
            "second" | "s" => Some(Self::Second),
            "minute" | "min" => Some(Self::Minute),
            "hour" | "h" => Some(Self::Hour),
            "day" | "d" => Some(Self::Day),
            "month" | "mon" => Some(Self::Month),
            "quarter" | "q" => Some(Self::Quarter),
            "year" | "y" => Some(Self::Year),
            "decade" => Some(Self::Decade),
            "century" => Some(Self::Century),
            "millennium" => Some(Self::Millennium),
            _ => None,
        }
    }

    /// Returns the finer (more precise) of two granularities (lower code = finer).
    pub fn finer(self, other: Self) -> Self {
        std::cmp::min(self, other)
    }
}

impl std::fmt::Display for Granularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_codes() {
        for code in 0..=10u8 {
            let g = Granularity::from_code(code).unwrap();
            assert_eq!(g.code(), code);
        }
    }

    #[test]
    fn reserved_codes_rejected() {
        for code in 11..=15u8 {
            assert!(Granularity::from_code(code).is_err());
        }
    }

    #[test]
    fn name_roundtrip() {
        for code in 0..=10u8 {
            let g = Granularity::from_code(code).unwrap();
            let g2 = Granularity::from_name(g.name()).unwrap();
            assert_eq!(g, g2);
        }
    }

    #[test]
    fn finer_picks_lower_code() {
        assert_eq!(Granularity::Day.finer(Granularity::Year), Granularity::Day);
        assert_eq!(
            Granularity::Year.finer(Granularity::Month),
            Granularity::Month
        );
        assert_eq!(
            Granularity::Millisecond.finer(Granularity::Millennium),
            Granularity::Millisecond
        );
    }
}
