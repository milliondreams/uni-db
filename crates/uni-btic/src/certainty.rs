use crate::error::BticError;

/// Epistemic certainty of a BTIC bound value.
///
/// Certainty is metadata only; it does not affect comparison or ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum Certainty {
    /// Bound is known precisely to stated granularity.
    Definite = 0,
    /// Bound is estimated; true value may differ by +/-1 unit of granularity.
    Approximate = 1,
    /// Bound is weakly estimated; true value may differ significantly.
    Uncertain = 2,
    /// Bound is not meaningfully known; stored value is a best guess.
    Unknown = 3,
}

impl Certainty {
    /// Construct from a 2-bit code (0b00..=0b11).
    pub fn from_code(code: u8) -> Result<Self, BticError> {
        match code {
            0 => Ok(Self::Definite),
            1 => Ok(Self::Approximate),
            2 => Ok(Self::Uncertain),
            3 => Ok(Self::Unknown),
            _ => Err(BticError::ParseError(format!(
                "certainty code {code} out of range 0..=3"
            ))),
        }
    }

    /// The numeric code for this certainty level.
    pub fn code(self) -> u8 {
        self as u8
    }

    /// Human-readable name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Definite => "definite",
            Self::Approximate => "approximate",
            Self::Uncertain => "uncertain",
            Self::Unknown => "unknown",
        }
    }

    /// Returns the least certain of two values (max of the codes).
    pub fn least_certain(self, other: Self) -> Self {
        std::cmp::max(self, other)
    }
}

impl std::fmt::Display for Certainty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_codes() {
        for code in 0..=3u8 {
            let c = Certainty::from_code(code).unwrap();
            assert_eq!(c.code(), code);
        }
    }

    #[test]
    fn least_certain_picks_higher() {
        assert_eq!(
            Certainty::Definite.least_certain(Certainty::Approximate),
            Certainty::Approximate
        );
        assert_eq!(
            Certainty::Unknown.least_certain(Certainty::Definite),
            Certainty::Unknown
        );
        assert_eq!(
            Certainty::Uncertain.least_certain(Certainty::Uncertain),
            Certainty::Uncertain
        );
    }
}
