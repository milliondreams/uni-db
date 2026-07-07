// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-cypher/src/grammar/mod.rs:113
//
// check_nesting_depth treats every bare word `end` (case-insensitive) as
// CLOSING a nesting level: `depth = (depth - 1).max(0)`. But `end` is a
// NON-reserved keyword, so `end(...)` is a legal recursive function call.
// For each `end(` the guard does `end` -> depth=(d-1).max(0) then `(` ->
// depth+=1, so depth merely oscillates 0<->1 and max_depth stays ~1 — far
// under MAX_NESTING_DEPTH=200. The guard is defeated and pest recurses to
// native-stack exhaustion (uncatchable abort of the host process).
//
// This violates the documented invariant (mod.rs:45-46): the check "may
// over-count, but never under-counts the nesting the parser would recurse
// into." Here it under-counts massively.

use uni_cypher::parse;

/// CONTROL: a neutral function name (`abs`) IS counted by the guard. At depth
/// 400 (> MAX_NESTING_DEPTH=200) the query must be rejected with the
/// NestingTooDeep guard error. This proves the guard works for normal names.
#[test]
fn control_abs_nesting_is_rejected_by_guard() {
    let depth = 400;
    let q = format!("RETURN {}1{}", "abs(".repeat(depth), ")".repeat(depth));
    let res = parse(&q);
    let msg = res.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        msg.contains("NestingTooDeep"),
        "control: abs(...) nested {depth} deep should hit the depth guard; got: {msg:?}"
    );
}

/// FIXED: the identical structure using `end(` instead of `abs(` — 400 levels
/// of real recursion — is now rejected by the depth guard. `end` immediately
/// followed by `(` is recognized as a function call, not a `CASE` close, so its
/// spurious decrement no longer cancels the `(`-increment. The guard fires with
/// NestingTooDeep exactly as it does for `abs(`.
///
/// Parsed on a generous 256 MiB stack so that, were the guard still evaded, pest
/// could complete rather than abort the test binary — but the guard now rejects
/// the input before any recursion.
#[test]
fn end_nesting_is_rejected_by_guard() {
    let depth = 400;
    let q = format!("RETURN {}1{}", "end(".repeat(depth), ")".repeat(depth));
    let msg = std::thread::Builder::new()
        .stack_size(256 * 1024 * 1024)
        .spawn(move || parse(&q).err().map(|e| e.to_string()).unwrap_or_default())
        .expect("spawn")
        .join()
        .expect("parse thread must not abort");
    eprintln!("end(...) x{depth} parse error message = {msg:?}");
    assert!(
        msg.contains("NestingTooDeep"),
        "`end(` nested {depth} deep must hit the depth guard, like `abs(`; got: {msg:?}"
    );
}

/// FIXED: deep `end(...)` nesting that previously overflowed the native stack is
/// now rejected by the guard before any recursion begins. On a small (512 KiB)
/// stack the guard returns Err(NestingTooDeep) instead of aborting the process.
#[test]
fn end_nesting_deep_is_rejected_not_overflowing() {
    let depth = 50_000;
    let q = format!("RETURN {}1{}", "end(".repeat(depth), ")".repeat(depth));
    // Small stack: if the guard were still evaded, unbounded recursion would
    // abort the whole process. A correct guard returns Err(NestingTooDeep).
    let handle = std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(move || parse(&q).map(|_| ()).map_err(|e| e.to_string()))
        .expect("spawn");
    let joined = handle.join();
    eprintln!("end-nesting parse joined = {joined:?}");
    assert!(
        matches!(&joined, Ok(Err(m)) if m.contains("NestingTooDeep")),
        "expected Err(NestingTooDeep) before any recursion; observed {joined:?}"
    );
}
