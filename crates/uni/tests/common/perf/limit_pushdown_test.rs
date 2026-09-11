// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A `LIMIT` over a bare labelled scan bounds what storage reads — issue #239.
//!
//! # What was actually wrong
//!
//! #239 reports that `ScanRequest::limit` is never set and concludes `LIMIT 1`
//! "reads the whole table". Measured on LDBC SF1, neither half held exactly:
//!
//! | label | vids begin at | table rows | `LIMIT 1` scanned |
//! |---|---|---|---|
//! | `Person`  | 0         | 9 892     | 8 192   |
//! | `Post`    | 100 384   | 1 003 605 | 22 496  |
//! | `Comment` | 1 103 989 | 2 052 169 | 984 971 |
//!
//! Not the whole table — #214's range walk already bounds it — but `LIMIT 1` on
//! `Comment` still read 48% of it. `rows_scanned` was *identical* for `LIMIT 1`,
//! `10` and `1000` at every label, which is the direct evidence the limit
//! reached nothing.
//!
//! The cost tracks where a label's vids **begin**, not its size. Vids are
//! allocated globally while each label gets its own table, so the walk starts at
//! vid 0 and doubles (8192, 16384, 32768, …) through vid space owned by other
//! labels. `8192·(2^k − 1) > 1 103 989` gives k = 8 — matching the eight scans
//! measured for `Comment` exactly. By the time it arrives the width has doubled
//! to ~1M, so the first *productive* range is enormous.
//!
//! # Why the fix is not `ScanRequest::limit`
//!
//! That field truncates raw Lance rows below MVCC dedup and can return a
//! superseded value — `bugs::issue_239_scan_limit_precedes_mvcc_dedup` in
//! `uni-store` demonstrates a `limit(2)` returning a vertex with `n = 1` whose
//! committed value is `100`. The same mechanism already shipped once as #211.
//! `GraphScanExec::with_fetch` narrows the `_vid` **range walk** instead, above
//! dedup and the L0 merge, so it can only change how much is read.
//!
//! # This fixture
//!
//! Reproduces the shape in miniature: `Filler` consumes a block of vids so
//! `Late` begins well above zero, which is the condition that made `Comment`
//! expensive. Asserted on `rows_scanned`, which is deterministic and
//! cache-independent — wall time here would measure the page cache.
//!
//! # Cost, and why it cannot be much smaller
//!
//! Every threshold in the walk is denominated in the 8192-row slice, so a
//! smaller graph cannot reach the state under test: below `SLICE` rows the walk
//! is skipped entirely, and below a ~123 000-vid gap the seek never gates open.
//! `Filler` is therefore property-less and written in committed, flushed chunks
//! — it exists only to move the global vid allocator, and holding it all in L0
//! before one flush is what made an earlier draft heavy.
//!
//! **Memory note.** `Uni::in_memory()` is `Uni::temporary()`, which writes a
//! real store under `TMPDIR`. Where `/tmp` is tmpfs — it is on the usual dev
//! box — that store is resident in RAM, and nextest runs one process per test.
//! These two tests are deliberately merged from four for that reason. If the
//! suite is OOM-killed here, point `TMPDIR` at a real disk rather than trimming
//! the constants, which would stop the fixture reaching the walk state.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// The walk's slice size, which every threshold here scales off.
///
/// This is DataFusion's `session_config().batch_size()`, **not**
/// `UniConfig::batch_size` — an earlier version of this fixture set the latter
/// to 512 hoping to shrink the constants below, and the scan still reported a
/// slice of 8192. The two are different knobs, and the fixture has to be sized
/// against the real one.
const SLICE: i64 = 8_192;

/// Vids consumed before `Late` starts.
///
/// Has to clear the point where the walk's width reaches
/// `SLICE * SEEK_MIN_WIDTH_FACTOR`, since that gates seeking over continuing to
/// double: the widths run 8192, 16384, 32768, 65536 across vids 0…122 880, so
/// the gap must be past that for a seek to engage at all.
const FILLER: i64 = 130_000;

/// Rows of the label under test.
///
/// Must exceed `SLICE`, or `Sizing` skips the range walk altogether and reads
/// the label whole — which is exactly how the first version of this fixture
/// reported `limited == unlimited` for a reason that had nothing to do with the
/// pushdown.
const LATE_ROWS: i64 = 40_000;

async fn fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    // `Filler` carries no properties: nothing ever reads it, and it exists
    // only to advance the *global* vid allocator so `Late` starts high. A
    // property would be ~130 000 extra values written and held for no purpose.
    db.schema().label("Filler").apply().await?;
    db.schema()
        .label("Late")
        .property("n", DataType::Int)
        .apply()
        .await?;
    let session = db.session();
    // Built in committed-and-flushed chunks rather than one transaction. The
    // whole point of the fixture is a large vid space, and holding all of it in
    // L0 before a single flush makes the test a memory hog — with one process
    // per test under nextest, four such fixtures at once was enough to get a
    // run OOM-killed.
    const CHUNK: i64 = 25_000;
    let mut written = 0;
    while written < FILLER {
        let n = CHUNK.min(FILLER - written);
        let tx = session.tx().await?;
        tx.query_with("UNWIND range(1, $c) AS i CREATE (:Filler)")
            .param("c", Value::Int(n))
            .fetch_all()
            .await?;
        tx.commit().await?;
        db.flush().await?;
        written += n;
    }
    let tx = session.tx().await?;
    tx.query_with("UNWIND range(1, $c) AS i CREATE (:Late {n: i})")
        .param("c", Value::Int(LATE_ROWS))
        .fetch_all()
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Rows storage reported scanning for `query`.
async fn rows_scanned(db: &Uni, query: &str) -> Result<usize> {
    let r = db.session().query(query).await?;
    Ok(r.metrics().rows_scanned)
}

/// The pushdown fires, is bounded, and does not change the answer.
///
/// Four assertions in one test on purpose. Each needs the same expensive
/// fixture, and nextest runs one process per test — four copies of a
/// 210 000-row graph at once was enough to get a run OOM-killed.
///
/// The bound is asserted against `SLICE`, not merely against the unlimited
/// count. `limited < unlimited` is **not discriminating**: the #214 range walk
/// already stops after the first productive range, so it held before this fix
/// too — on LDBC `Person` (vids from 0) `LIMIT 1` read 8 192 of 9 892 rows and
/// would have passed. What the fix changes is the size of the *first
/// productive* range when the label starts high, so the assertion has to be
/// that the read stays near one slice.
///
/// The two constants are chosen so the arms are far apart. Measured on this
/// fixture: with the seek the read is exactly `SLICE` (8 192). Without it the
/// walk arrives at `lo = 122 880` with its width doubled to 131 072, a range
/// that spans every `Late` row, so it reads all `LATE_ROWS` (40 000). The
/// `SLICE * 2` bound sits cleanly between them. An earlier draft used
/// `LATE_ROWS = 10 000`, where the unfixed read would have been 10 000 — inside
/// the bound, and so a test that passed either way.
#[tokio::test]
async fn a_limit_bounds_what_a_bare_labelled_scan_reads() -> Result<()> {
    let db = fixture().await?;

    // Control: unbounded still reads the label whole. Without this, a small
    // limited number could just mean the label is unreadable.
    let unlimited = rows_scanned(&db, "MATCH (n:Late) RETURN count(n) AS c").await?;
    assert!(
        unlimited >= LATE_ROWS as usize,
        "control: an unbounded scan must read the label whole; got {unlimited} \
         rows for {LATE_ROWS}"
    );

    // The pushdown: bounded to about one slice despite `Late` starting above
    // {FILLER} vids. Unfixed, the walk arrives with its width doubled past
    // 65 536 and reads that whole range.
    let limited = rows_scanned(&db, "MATCH (n:Late) RETURN id(n) AS v LIMIT 1").await?;
    assert!(
        limited <= (SLICE as usize) * 2,
        "`LIMIT 1` must read about one {SLICE}-row slice, not the range the \
         gap-crossing walk had grown to: read {limited} rows (unlimited reads \
         {unlimited}). This is the #239 defect."
    );

    // Correctness: narrowing the walk must not change which rows win.
    for limit in [1usize, 10, 1000] {
        let r = db
            .session()
            .query(&format!("MATCH (n:Late) RETURN id(n) AS v LIMIT {limit}"))
            .await?;
        assert_eq!(
            r.rows().len(),
            limit,
            "LIMIT {limit} must return {limit} rows from a {LATE_ROWS}-row label"
        );
    }
    let r = db
        .session()
        .query("MATCH (n:Late) RETURN count(n) AS c")
        .await?;
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Int(LATE_ROWS),
        "the unbounded count must be unchanged by the pushdown"
    );
    Ok(())
}

/// The negative twin. A limit above an aggregate cannot be pushed into the
/// scan — the aggregate needs every row — so this must read the label whole.
/// Without it, a pushdown that fired unconditionally would satisfy the positive
/// test while silently truncating aggregates.
#[tokio::test]
async fn a_limit_above_an_aggregate_is_not_pushed_into_the_scan() -> Result<()> {
    let db = fixture().await?;
    let scanned = rows_scanned(&db, "MATCH (n:Late) RETURN count(n) AS c LIMIT 1").await?;
    assert!(
        scanned >= LATE_ROWS as usize,
        "`count(n) … LIMIT 1` limits the one output row, not the input: the \
         scan must still read all {LATE_ROWS} rows, got {scanned}"
    );
    let r = db
        .session()
        .query("MATCH (n:Late) RETURN count(n) AS c LIMIT 1")
        .await?;
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Int(LATE_ROWS),
        "and the count itself must be right"
    );
    Ok(())
}
