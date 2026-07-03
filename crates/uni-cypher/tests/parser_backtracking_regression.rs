//! Regression guard: the Cypher/Locy grammar must not backtrack exponentially.
//!
//! The nightly `locy_parse` fuzz target timed out on a 130-byte input —
//! `unwind\r\r` followed by runs of unmatched `[` interleaved with `a` — which
//! took ~1.5s locally and exceeded the 10s per-input fuzz budget on CI. Root
//! cause: in `cypher.pest`, `postfix_suffix`'s index (`[e]`) and slice
//! (`[e?..e?]`) alternatives both re-parsed `expression` after `[`, so every `[`
//! doubled the work — O(2^N) on N stacked brackets. Factoring the shared
//! `[ expression` prefix into the `index_or_slice` rule made it linear.
//!
//! These tests assert the pathological inputs parse near-instantly and that the
//! index/slice/comprehension surface still parses, so a future grammar edit that
//! reintroduces the ambiguity fails loudly instead of silently hanging the fuzzer.

use std::time::{Duration, Instant};

/// Run `parse_locy(input)` on a worker thread and fail if it does not finish
/// within `budget`. Using a thread (rather than a bare wall-clock assertion on
/// the calling thread) means a regression to exponential time fails cleanly
/// instead of hanging the whole test binary.
fn assert_parses_within(input: &str, budget: Duration) {
    let owned = input.to_string();
    let handle = std::thread::spawn(move || {
        let start = Instant::now();
        let _ = uni_cypher::parse_locy(&owned);
        start.elapsed()
    });

    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if handle.is_finished() {
            let elapsed = handle.join().expect("parse thread panicked");
            assert!(
                elapsed < budget,
                "parse took {elapsed:?}, over budget {budget:?}"
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "parse did not finish within {budget:?} for input of {} bytes — \
         exponential backtracking regressed",
        input.len()
    );
}

#[test]
fn fuzz_timeout_artifact_parses_fast() {
    // The exact libFuzzer `locy_parse` timeout artifact
    // (timeout-226c0fb00a6e5a350dbcbb24062758309afd0b67).
    let bytes: &[u8] = &[
        117, 110, 119, 105, 110, 100, 13, 13, 91, 91, 91, 91, 91, 91, 91, 97, 91, 91, 91, 91, 91,
        91, 91, 91, 91, 91, 91, 97, 91, 91, 91, 91, 110, 119, 105, 110, 100, 13, 13, 91, 91, 91,
        91, 91, 91, 91, 97, 91, 91, 91, 75, 91, 91, 97, 91, 91, 110, 119, 105, 110, 100, 13, 13,
        91, 91, 91, 91, 91, 91, 91, 97, 91, 91, 91, 91, 91, 97, 91, 91, 91, 91, 110, 119, 105, 110,
        100, 13, 13, 91, 91, 91, 91, 91, 91, 91, 97, 91, 91, 91, 75, 91, 91, 97, 91, 91, 110, 119,
        105, 110, 100, 13, 13, 91, 91, 91, 91, 91, 91, 75, 91, 91, 97, 91, 91, 91, 75, 62, 61, 120,
        79,
    ];
    let input = std::str::from_utf8(bytes).unwrap();
    assert_parses_within(input, Duration::from_secs(5));
}

#[test]
fn stacked_unmatched_brackets_parse_fast() {
    // Far beyond the artifact: pre-fix this would take longer than the age of
    // the universe; post-fix it is microseconds.
    let input = format!("unwind\r\r{}", "[a".repeat(120));
    assert_parses_within(&input, Duration::from_secs(5));
}

#[test]
fn index_and_slice_surface_still_parses() {
    // The grammar factoring must not change the accepted language.
    let queries = [
        "RETURN xs[0]",
        "RETURN xs[1 + 2]",
        "RETURN xs[1..3]",
        "RETURN xs[..3]",
        "RETURN xs[1..]",
        "RETURN xs[..]",
        "RETURN xs[0][1]",
        "RETURN [1, 2, 3]",
        "RETURN []",
        "RETURN m['key']",
        "RETURN [x IN range(0, 10) WHERE x > 2 | x * 2]",
    ];
    for q in queries {
        assert!(
            uni_cypher::parse_locy(q).is_ok(),
            "expected {q:?} to parse after the index_or_slice factoring"
        );
    }
}
