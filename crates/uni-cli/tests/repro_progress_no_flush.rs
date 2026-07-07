// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for `crates/uni-cli/src/demo/semantic_scholar.rs:168`.
//!
//! The per-1000-record progress indicator runs `print!("\rProcessed ...")` with
//! no trailing newline and no `stdout().flush()`. On a TTY, `std::io::Stdout`
//! wraps a `LineWriter`, which only flushes on a `\n` (or a ~1KB overflow). So
//! the progress counter stays buffered and is not shown during the loop — it
//! only surfaces when the post-loop `println!("\nFlushing ...")` writes a
//! newline.
//!
//! Driving a real TTY from a test harness is not portable, so this test
//! reproduces the exact buffering mechanism deterministically: it writes the
//! literal progress string through the same `LineWriter` type that
//! `std::io::Stdout` uses on a terminal, and observes that the bytes are NOT
//! visible in the underlying sink until a newline is written. It then shows
//! that an explicit `flush()` (the fix) makes them visible immediately.

use std::io::{LineWriter, Write};

/// The exact `print!` payload emitted at `semantic_scholar.rs:168` for the
/// first progress tick.
const PROGRESS_LINE: &str = "\rProcessed 1000 papers";

/// A `\r`-prefixed progress write is invisible on a line-buffered sink until a
/// newline flushes it; an explicit flush would make it visible immediately.
#[test]
fn progress_line_is_buffered_until_newline() {
    // `std::io::Stdout` wraps a `LineWriter` when attached to a TTY. Model that
    // sink with an in-memory `Vec<u8>` we can inspect via `get_ref()`.
    let mut line_writer = LineWriter::new(Vec::<u8>::new());

    // This is what the demo does: `print!("\rProcessed {count} {label}")` with
    // no trailing '\n' and no flush.
    write!(line_writer, "{PROGRESS_LINE}").unwrap();

    // BUG: nothing has reached the sink — on a TTY the user sees no progress.
    // Repro for crates/uni-cli/src/demo/semantic_scholar.rs:168.
    assert!(
        line_writer.get_ref().is_empty(),
        "expected the progress line to be buffered (invisible); sink held: {:?}",
        String::from_utf8_lossy(line_writer.get_ref())
    );

    // The post-loop `println!("\nFlushing {label}...")` finally writes a
    // newline, which flushes the previously-invisible progress text.
    write!(line_writer, "\nFlushing papers...").unwrap();

    let flushed = String::from_utf8(line_writer.get_ref().clone()).unwrap();
    assert!(
        flushed.contains("Processed 1000 papers"),
        "progress text should surface only after the newline; sink held: {flushed:?}"
    );

    // The fix: an explicit `stdout().flush()` right after the `print!` makes the
    // progress visible without waiting for a newline. Demonstrate that a flush
    // pushes a subsequent no-newline write straight through.
    let mut fixed = LineWriter::new(Vec::<u8>::new());
    write!(fixed, "\rProcessed 2000 papers").unwrap();
    fixed.flush().unwrap();
    assert!(
        String::from_utf8_lossy(fixed.get_ref()).contains("Processed 2000 papers"),
        "with an explicit flush the progress line is visible immediately"
    );
}
