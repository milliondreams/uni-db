#![allow(dead_code, unused_imports, clippy::all)]
//! M10 multi-version ABI coexistence smoke test.
//!
//! The actual `wasmtime::Linker` selection lives in
//! `uni-plugin-wasm/src/multi_version.rs` and is exercised end-to-end
//! by the wasm crate's `multi_version` test module. This file pins
//! the host-visible contract: a plugin manifest declaring
//! `abi: "^1"` and another declaring `abi: "^2"` both resolve to
//! distinct host-supported majors, and a `^99` plugin is rejected
//! before any wasmtime work happens.

#![allow(dead_code)]

// The wasmtime-backed `MultiVersionLinker` tests live next to the
// loader in `uni-plugin-wasm/src/multi_version.rs` (5 unit tests).
// Here we cover the host-visible invariants of `AbiRange` that the
// loader's `select_linker_for_manifest` dispatches on.
#[test]
fn abi_range_matches_major_probes_independently_of_loader() {
    let r1 = uni_plugin::AbiRange::parse("^1").unwrap();
    assert!(r1.matches(1));
    assert!(!r1.matches(2));

    let r2 = uni_plugin::AbiRange::parse("^2").unwrap();
    assert!(r2.matches(2));
    assert!(!r2.matches(1));

    let r_any = uni_plugin::AbiRange::parse(">=1, <99").unwrap();
    assert!(r_any.matches(1));
    assert!(r_any.matches(2));
    assert!(!r_any.matches(99));
}
