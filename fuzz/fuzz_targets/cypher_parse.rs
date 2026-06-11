//! Fuzz the openCypher pest grammar + walker: parsing arbitrary text must
//! return `Ok`/`Err`, never panic, hang, or blow the stack.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = uni_cypher::parse(s);
    }
});
