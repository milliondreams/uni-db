//! Fuzz the WAL segment decoder (v2 checksummed envelope + legacy JSON):
//! arbitrary on-disk bytes must yield `Ok`/`Err(reason)`, never panic —
//! a torn segment is an expected crash artifact, not a parser stress test.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = uni_store::runtime::wal::decode_segment(data);
});
