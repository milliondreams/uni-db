use thiserror::Error;

/// Errors produced by BTIC construction, encoding, decoding, and parsing.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BticError {
    #[error("INV-1: lo ({lo}) must be less than hi ({hi})")]
    BoundOrdering { lo: i64, hi: i64 },

    #[error("INV-2: sentinel value used as finite bound")]
    SentinelExclusivity,

    #[error("INV-4: non-zero reserved/version/flags bits in meta word")]
    ReservedBits,

    #[error("INV-5: granularity code {0:#x} out of valid range 0x0..=0xA")]
    GranularityRange(u8),

    #[error("INV-6: sentinel bound must have zeroed granularity and certainty")]
    SentinelMetadata,

    #[error("invalid BTIC literal: {0}")]
    ParseError(String),

    #[error("BTIC buffer must be exactly 24 bytes, got {0}")]
    InvalidLength(usize),
}
