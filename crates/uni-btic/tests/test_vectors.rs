//! Test vectors from BTIC Specification Appendix B.
//!
//! Each test verifies exact byte-level packed representations.

use uni_btic::btic::{Btic, NEG_INF, POS_INF};
use uni_btic::encode;
use uni_btic::parse::parse_btic_literal;

/// Helper: assert packed bytes match expected hex.
fn assert_packed(btic: &Btic, expected: &[u8; 24]) {
    let actual = encode::encode(btic);
    assert_eq!(
        &actual, expected,
        "\nExpected: {:02x?}\nActual:   {:02x?}",
        expected, actual
    );
}

#[test]
fn b1_unix_epoch_instant() {
    // Input: '1970-01-01T00:00:00.000Z'
    let b = parse_btic_literal("1970-01-01T00:00:00.000Z").unwrap();
    assert_eq!(b.lo(), 0);
    assert_eq!(b.hi(), 1);
    assert_eq!(b.meta(), 0x0000_0000_0000_0000);

    let expected: [u8; 24] = [
        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // lo
        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // hi
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // meta
    ];
    assert_packed(&b, &expected);
}

#[test]
fn b2_year_1985() {
    // Input: '1985'
    let b = parse_btic_literal("1985").unwrap();
    assert_eq!(b.lo(), 473_385_600_000);
    assert_eq!(b.hi(), 504_921_600_000);
    assert_eq!(b.meta(), 0x7700_0000_0000_0000);

    let expected: [u8; 24] = [
        0x80, 0x00, 0x00, 0x6E, 0x37, 0xFB, 0x04, 0x00, // lo
        0x80, 0x00, 0x00, 0x75, 0x8F, 0xAC, 0x30, 0x00, // hi
        0x77, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // meta
    ];
    assert_packed(&b, &expected);
}

#[test]
fn b3_neg_inf_to_march_1985() {
    // Input: '/1985-03'
    let b = parse_btic_literal("/1985-03").unwrap();
    assert_eq!(b.lo(), NEG_INF);
    assert_eq!(b.hi(), 481_161_600_000);
    assert_eq!(b.meta(), 0x0500_0000_0000_0000);

    let expected: [u8; 24] = [
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // lo (NEG_INF sign-flipped)
        0x80, 0x00, 0x00, 0x70, 0x07, 0x77, 0x5C, 0x00, // hi
        0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // meta
    ];
    assert_packed(&b, &expected);
}

#[test]
fn b4_500_bce_approximate() {
    // Input: '~500 BCE'
    let b = parse_btic_literal("~500 BCE").unwrap();

    // Astronomical year -499: lo=-0499-01-01, hi=-0498-01-01
    assert_eq!(b.lo(), -77_914_137_600_000);
    assert_eq!(b.hi(), -77_882_601_600_000);
    assert_eq!(b.meta(), 0x7750_0000_0000_0000);

    // Duration should be 365 days (not a leap year)
    assert_eq!(b.duration_ms(), Some(31_536_000_000));

    let expected: [u8; 24] = [
        0x7F, 0xFF, 0xB9, 0x23, 0x33, 0x81, 0x60, 0x00, // lo
        0x7F, 0xFF, 0xB9, 0x2A, 0x8B, 0x32, 0x8C, 0x00, // hi
        0x77, 0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // meta
    ];
    assert_packed(&b, &expected);
}

#[test]
fn b5_fully_unbounded() {
    // Input: '/'
    let b = parse_btic_literal("/").unwrap();
    assert_eq!(b.lo(), NEG_INF);
    assert_eq!(b.hi(), POS_INF);
    assert_eq!(b.meta(), 0x0000_0000_0000_0000);

    let expected: [u8; 24] = [
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // lo
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // hi
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // meta
    ];
    assert_packed(&b, &expected);
}

#[test]
fn b6_mixed_granularity() {
    // Input: '1985-03/2024-06-15'
    let b = parse_btic_literal("1985-03/2024-06-15").unwrap();
    assert_eq!(b.lo(), 478_483_200_000); // 1985-03-01
    assert_eq!(b.hi(), 1_718_496_000_000); // 2024-06-16
    assert_eq!(b.meta(), 0x5400_0000_0000_0000);

    let expected: [u8; 24] = [
        0x80, 0x00, 0x00, 0x6F, 0x67, 0xD2, 0x38, 0x00, // lo
        0x80, 0x00, 0x01, 0x90, 0x1E, 0x57, 0xF8, 0x00, // hi
        0x54, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // meta
    ];
    assert_packed(&b, &expected);
}

#[test]
fn b7_ongoing_event() {
    // Input: '2020-03/'
    let b = parse_btic_literal("2020-03/").unwrap();
    assert_eq!(b.lo(), 1_583_020_800_000); // 2020-03-01
    assert_eq!(b.hi(), POS_INF);
    assert_eq!(b.meta(), 0x5000_0000_0000_0000);

    let expected: [u8; 24] = [
        0x80, 0x00, 0x01, 0x70, 0x93, 0x64, 0x78, 0x00, // lo
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // hi
        0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // meta
    ];
    assert_packed(&b, &expected);
}
