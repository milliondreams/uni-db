use crate::btic::{Btic, SIGN_FLIP};
use crate::error::BticError;

/// Encode a BTIC value into its 24-byte packed canonical form.
///
/// The packed format uses sign-bit-flipped big-endian encoding so that
/// `memcmp` on the raw bytes produces the same order as `Btic::cmp`.
pub fn encode(btic: &Btic) -> [u8; 24] {
    let mut buf = [0u8; 24];
    let lo_encoded = (btic.lo() as u64) ^ SIGN_FLIP;
    let hi_encoded = (btic.hi() as u64) ^ SIGN_FLIP;
    buf[0..8].copy_from_slice(&lo_encoded.to_be_bytes());
    buf[8..16].copy_from_slice(&hi_encoded.to_be_bytes());
    buf[16..24].copy_from_slice(&btic.meta().to_be_bytes());
    buf
}

/// Decode a 24-byte packed canonical form into a BTIC value.
///
/// Validates all invariants after decoding.
pub fn decode(bytes: &[u8; 24]) -> Result<Btic, BticError> {
    let lo_encoded = u64::from_be_bytes(
        bytes[0..8]
            .try_into()
            .expect("infallible: 8-byte slice from 24-byte array"),
    );
    let hi_encoded = u64::from_be_bytes(
        bytes[8..16]
            .try_into()
            .expect("infallible: 8-byte slice from 24-byte array"),
    );
    let meta = u64::from_be_bytes(
        bytes[16..24]
            .try_into()
            .expect("infallible: 8-byte slice from 24-byte array"),
    );

    let lo = (lo_encoded ^ SIGN_FLIP) as i64;
    let hi = (hi_encoded ^ SIGN_FLIP) as i64;

    Btic::new(lo, hi, meta)
}

/// Decode from a byte slice, checking that the length is exactly 24.
pub fn decode_slice(bytes: &[u8]) -> Result<Btic, BticError> {
    if bytes.len() != 24 {
        return Err(BticError::InvalidLength(bytes.len()));
    }
    let arr: &[u8; 24] = bytes
        .try_into()
        .expect("infallible: length validated above");
    decode(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btic::NEG_INF;
    use crate::certainty::Certainty;
    use crate::granularity::Granularity;

    #[test]
    fn roundtrip_basic() {
        let meta = Btic::build_meta(
            Granularity::Year,
            Granularity::Year,
            Certainty::Definite,
            Certainty::Definite,
        );
        let original = Btic::new(473_385_600_000, 504_921_600_000, meta).unwrap();
        let packed = encode(&original);
        let decoded = decode(&packed).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn roundtrip_negative_lo() {
        let meta = Btic::build_meta(
            Granularity::Year,
            Granularity::Year,
            Certainty::Approximate,
            Certainty::Approximate,
        );
        // Negative timestamp (before epoch)
        let original = Btic::new(-77_914_137_600_000, -77_882_601_600_000, meta).unwrap();
        let packed = encode(&original);
        let decoded = decode(&packed).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn roundtrip_sentinel_lo() {
        let meta = Btic::build_meta(
            Granularity::Millisecond,
            Granularity::Month,
            Certainty::Definite,
            Certainty::Definite,
        );
        let original = Btic::new(NEG_INF, 481_161_600_000, meta).unwrap();
        let packed = encode(&original);
        let decoded = decode(&packed).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn invalid_length_rejected() {
        assert!(decode_slice(&[0u8; 23]).is_err());
        assert!(decode_slice(&[0u8; 25]).is_err());
    }

    #[test]
    fn memcmp_matches_ord() {
        let meta = Btic::build_meta(
            Granularity::Day,
            Granularity::Day,
            Certainty::Definite,
            Certainty::Definite,
        );
        let a = Btic::new(100, 200, meta).unwrap();
        let b = Btic::new(150, 200, meta).unwrap();
        let packed_a = encode(&a);
        let packed_b = encode(&b);

        // memcmp ordering should match Ord
        assert!(packed_a < packed_b);
        assert!(a < b);
    }
}
