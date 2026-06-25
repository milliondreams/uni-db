use thiserror::Error;

/// Errors produced by `SparseVector` construction, encoding, and decoding.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum SparseError {
    /// `indices` and `values` had different lengths.
    #[error("SV-1: indices ({indices}) and values ({values}) must have equal length")]
    LengthMismatch { indices: usize, values: usize },

    /// `indices` were not in strictly ascending order (which also forbids duplicates).
    #[error(
        "SV-2: indices must be strictly ascending (sorted + unique); violation at position {position} ({prev} >= {curr})"
    )]
    UnsortedIndices {
        position: usize,
        prev: u32,
        curr: u32,
    },

    /// A weight was non-finite (NaN or ±infinity).
    #[error("SV-3: weight at position {position} is non-finite ({value})")]
    NonFiniteWeight { position: usize, value: f32 },

    /// An encoded buffer was too short to contain its declared payload.
    #[error("SV-4: encoded buffer truncated: need {need} bytes, got {got}")]
    Truncated { need: usize, got: usize },

    /// An encoded buffer carried trailing bytes after its declared payload.
    #[error("SV-5: encoded buffer has {trailing} unexpected trailing byte(s)")]
    TrailingBytes { trailing: usize },
}
