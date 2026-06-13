//! Fuzz the BTIC binary codec (24-byte index-key encoding) and the BTIC
//! literal parser — both consume untrusted bytes from storage / queries.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = uni_btic::encode::decode_slice(data);
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = uni_btic::parse::parse_btic_literal(s);
    }
});
