// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Telemetry assertions: the SSI counters (`uni_ssi_*`,
//! `uni_l0_snapshot_freezes_total`) fire exactly when the design says they do.
//!
//! These pin the observability contract a release-as-default needs: an operator
//! must be able to read abort rate, retry rate, and freeze rate off the metrics.
//! Each test installs an in-process recorder (`ssi_support::metrics`) and asserts
//! the counter DELTA around a scripted scenario.

use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, RetryOptions, Uni};

use crate::ssi_support::metrics::{self, CounterProbe};
use crate::ssi_support::schedule::{assert_committed, assert_serialization_conflict};

/// Skip a telemetry test unless the process is isolated (nextest).
///
/// SSI counters are process-global, so under the shared-process `cargo test`
/// runner a concurrent test pollutes them or wins the recorder install. Returns
/// `true` (and logs) when the caller should bail out early; under nextest it
/// returns `false` so the test runs at full strength. See
/// [`metrics::counters_isolated`].
fn skip_unless_isolated(test: &str) -> bool {
    if metrics::counters_isolated() {
        return false;
    }
    eprintln!(
        "skipping {test}: SSI counter telemetry needs process isolation; run via `cargo nextest`"
    );
    true
}

async fn counter_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("C")
        .property("id", DataType::String)
        .property("n", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:C {id: 'x', n: 0})").await?;
    tx.commit().await?;
    Ok(db)
}

/// A serialization conflict increments the conflict counter and is counted as a
/// validation. (Empty label slice matches the counter regardless of its `kind`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_increments_serialization_counter() -> Result<()> {
    if skip_unless_isolated("conflict_increments_serialization_counter") {
        return Ok(());
    }
    metrics::init();
    let conflicts = CounterProbe::start("uni_ssi_serialization_conflicts_total", &[]);
    let validations = CounterProbe::start("uni_ssi_commit_validations_total", &[]);

    let db = counter_db().await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;
    ta.execute("MATCH (c:C {id: 'x'}) SET c.n = c.n + 1")
        .await?;
    tb.execute("MATCH (c:C {id: 'x'}) SET c.n = c.n + 1")
        .await?;
    assert_committed(ta.commit().await);
    assert_serialization_conflict(tb.commit().await);

    assert!(conflicts.delta() >= 1, "conflict counter did not increment");
    assert!(
        validations.delta() >= 2,
        "both commits should be counted as validations"
    );
    Ok(())
}

/// An uncontended commit performs no clone-on-freeze: the freeze counter stays
/// flat when no concurrent transaction pins the generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncontended_commit_does_not_freeze() -> Result<()> {
    if skip_unless_isolated("uncontended_commit_does_not_freeze") {
        return Ok(());
    }
    metrics::init();
    let freezes = CounterProbe::start("uni_l0_snapshot_freezes_total", &[]);

    let db = counter_db().await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("MATCH (c:C {id: 'x'}) SET c.n = 1").await?;
    assert_committed(tx.commit().await);

    assert_eq!(
        freezes.delta(),
        0,
        "an uncontended commit must not clone-on-freeze"
    );
    Ok(())
}

/// A commit while a concurrent transaction holds a pinned snapshot DOES freeze
/// the generation aside (so the reader stays isolated) — the counter increments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contended_commit_freezes_once() -> Result<()> {
    if skip_unless_isolated("contended_commit_freezes_once") {
        return Ok(());
    }
    metrics::init();
    let freezes = CounterProbe::start("uni_l0_snapshot_freezes_total", &[]);

    let db = counter_db().await?;
    // Reader pins a snapshot by beginning a transaction and reading.
    let reader_s = db.session();
    let reader = reader_s.tx().await?;
    reader.query("MATCH (c:C {id: 'x'}) RETURN c.n").await?;

    // A concurrent committer writes while the reader's snapshot is pinned.
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("MATCH (c:C {id: 'x'}) SET c.n = 5").await?;
        assert_committed(tx.commit().await);
    }

    assert!(
        freezes.delta() >= 1,
        "a commit under a pinned snapshot must freeze the generation aside"
    );
    drop(reader);
    Ok(())
}

/// High contention drives retries through the bounded-retry helper, incrementing
/// the retry counter, and still converges to the correct value.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn contention_drives_retries() -> Result<()> {
    if skip_unless_isolated("contention_drives_retries") {
        return Ok(());
    }
    metrics::init();
    let retries = CounterProbe::start("uni_ssi_retries_total", &[]);

    let db = Arc::new(counter_db().await?);
    const WRITERS: i64 = 12;
    let mut handles = Vec::new();
    for _ in 0..WRITERS {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            db.session()
                .transact_with_retry(
                    RetryOptions {
                        max_attempts: 100,
                        ..Default::default()
                    },
                    |tx| {
                        Box::pin(async move {
                            tx.execute("MATCH (c:C {id: 'x'}) SET c.n = c.n + 1")
                                .await?;
                            Ok(())
                        })
                    },
                )
                .await
        }));
    }
    for h in handles {
        h.await.expect("task panicked")?;
    }

    let r = db
        .session()
        .query("MATCH (c:C {id: 'x'}) RETURN c.n AS n")
        .await?;
    assert_eq!(r.rows()[0].value("n"), Some(&uni_db::Value::Int(WRITERS)));
    assert!(
        retries.delta() >= 1,
        "12-way contention should have driven at least one retry"
    );
    Ok(())
}
