// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Resilience: crash / recovery / WAL replay for the sparse-vector index
//! (issue #95, test set F).
//!
//! These extend `ssi_resilience.rs` with a `SparseVector` payload. The
//! sparse-specific durability claim is that a committed sparse mutation is
//! persisted through the explicit Cypher-value codec (`TAG_SPARSE_VECTOR`), so a
//! crash-then-reopen replays it from the WAL and decodes it **losslessly** — the
//! durability fix that closed the lossy untagged-serde path. The crash-injection
//! tests (gated behind the `failpoints` feature) drive a commit / flush to panic
//! at a precise seam, then reopen and assert atomicity and decode fidelity.
//!
//! WAL replay does not repopulate the sparse postings index, but no rebuild is
//! needed: the recovered rows land in L0 and the `sparse_rerank` read path
//! unions live L0 candidates, so the index query is correct with the postings
//! still cold (see `recovery_index_no_rebuild.rs` for the cross-index regression).
//! Run with `--features failpoints`. Each test owns its failpoint and runs in
//! its own process under nextest, so the global registry does not bleed.

#[cfg(feature = "failpoints")]
use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, IndexType, Uni, Value};

use crate::ssi_support::reopen::DiskHarness;

const VOCAB: usize = 1000;

/// The fixed sparse vector seeded as `target` and reused as the query — `target`
/// is its own exact dot-product maximizer.
fn target_emb() -> Value {
    Value::SparseVector {
        indices: vec![1, 5, 9, 42],
        values: vec![1.0, 2.0, 3.0, 0.5],
    }
}

/// `Doc(title, emb: SparseVector)` + a scored sparse index over `emb`.
async fn define_sparse_schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("emb", IndexType::sparse(VOCAB))
        .apply()
        .await?;
    Ok(())
}

/// Insert one `Doc` with the given title and sparse embedding (own transaction).
async fn insert_doc(db: &Uni, title: &str, emb: Value) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: $t, emb: $emb})")
        .param("t", Value::String(title.to_string()))
        .param("emb", emb)
        .run()
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Read back the `emb` of the doc titled `title`, if it survived recovery.
async fn read_emb(db: &Uni, title: &str) -> Result<Option<Value>> {
    let r = db
        .session()
        .query_with("MATCH (d:Doc {title: $t}) RETURN d.emb AS emb")
        .param("t", Value::String(title.to_string()))
        .fetch_all()
        .await?;
    Ok(r.rows().first().and_then(|row| row.value("emb").cloned()))
}

/// The title of the top-1 sparse match to `target_emb()` via `uni.sparse.query`.
#[cfg(feature = "failpoints")]
async fn top_sparse_title(db: &Uni) -> Result<Option<String>> {
    let r = db
        .session()
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, 1, null, null, {}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param("q", target_emb())
        .fetch_all()
        .await?;
    Ok(r.rows().first().and_then(|row| match row.value("title") {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }))
}

/// Drive a sparse `CREATE` + commit that is expected to panic at an armed
/// failpoint (the spawned task aborts, so the join returns `Err`).
#[cfg(feature = "failpoints")]
async fn sparse_create_that_crashes(db: Arc<Uni>, title: &'static str) {
    let res = tokio::spawn(async move {
        let s = db.session();
        let tx = s.tx().await.unwrap();
        tx.execute_with("CREATE (:Doc {title: $t, emb: $emb})")
            .param("t", Value::String(title.to_string()))
            .param("emb", target_emb())
            .run()
            .await
            .unwrap();
        tx.commit().await
    })
    .await;
    assert!(
        res.is_err(),
        "commit task should have panicked at the failpoint"
    );
}

/// A sparse mutation made durable in the WAL (committed but not flushed to L1)
/// survives a later mid-commit crash and decodes losslessly through the CV codec
/// on replay. This is the sparse analogue of `crash_after_wal_flush_is_atomic`,
/// targeting the value-fidelity guarantee rather than a scalar count.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_committed_value_survives_crash_recovery() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        define_sparse_schema(&db).await?;
        // A flushed baseline gives recovery a snapshot manifest to replay onto.
        insert_doc(
            &db,
            "base",
            Value::SparseVector {
                indices: vec![2, 3],
                values: vec![0.7, 0.7],
            },
        )
        .await?;
        db.flush().await?;
        // `target` is committed (durable in the WAL) but NOT flushed, so recovery
        // must replay it from the WAL.
        insert_doc(&db, "target", target_emb()).await?;
        // A later transaction crashes after its WAL flush; the already-durable
        // `target` commit must survive the crash + reopen intact.
        let db = Arc::new(db);
        fail::cfg("commit::after-wal-flush", "panic").unwrap();
        sparse_create_that_crashes(db.clone(), "doomed").await;
        fail::remove("commit::after-wal-flush");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_emb(&db, "target").await?,
        Some(target_emb()),
        "committed sparse value corrupted or lost across crash recovery"
    );
    // Usable post-recovery WITHOUT a rebuild: the recovered doc is in L0 and the
    // sparse read path unions it, so it is the top sparse match with the index
    // still cold.
    assert_eq!(
        top_sparse_title(&db).await?.as_deref(),
        Some("target"),
        "recovered sparse doc not retrievable via the L0-union path (no rebuild)"
    );
    Ok(())
}

/// A sparse `CREATE` that crashes AFTER validation but BEFORE the WAL append
/// recovers nothing: the mutation never became durable, so no half-written
/// `SparseVector` resurrects on replay.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_crash_before_wal_recovers_nothing() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        define_sparse_schema(&db).await?;
        insert_doc(
            &db,
            "base",
            Value::SparseVector {
                indices: vec![2, 3],
                values: vec![0.7, 0.7],
            },
        )
        .await?;
        db.flush().await?;
        let db = Arc::new(db);
        fail::cfg("commit::after-validate", "panic").unwrap();
        sparse_create_that_crashes(db.clone(), "doomed").await;
        fail::remove("commit::after-validate");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_emb(&db, "doomed").await?,
        None,
        "a sparse write that crashed before the WAL flush must leave no trace"
    );
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(Value::SparseVector {
            indices: vec![2, 3],
            values: vec![0.7, 0.7],
        }),
        "the durable baseline sparse doc must survive"
    );
    Ok(())
}

/// A crash mid-flush (panic after the L0 rotation, before the Lance write) loses
/// no committed data: both the flushed `base` and the committed-but-unflushed
/// `delta` (which sat in the rotated buffer the crash abandoned) survive reopen,
/// each exactly once, and the index rebuilds cleanly. Targets the
/// `flush::after-rotate-before-lance` seam.
///
/// Regression for the lost-commit durability bug: a graceful close after a
/// failed flush truncated the delta's WAL segment and published a
/// `wal_high_water_mark` past it (WAL truncation + checkpoint keyed off the
/// pending buffer's HIGH watermark instead of its START watermark), so an
/// acknowledged commit vanished on reopen. See `l0_manager::min_pending_wal_lsn_start`.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_crash_during_flush_loses_no_committed_data() -> Result<()> {
    let h = DiskHarness::new()?;
    let delta_emb = Value::SparseVector {
        indices: vec![1, 5],
        values: vec![0.4, 0.4],
    };
    {
        let db = Arc::new(h.open().await?);
        define_sparse_schema(&db).await?;
        // `base` is an exact match for the query, flushed durably before the crash.
        insert_doc(&db, "base", target_emb()).await?;
        db.flush().await?;
        // `delta` is committed (acknowledged) but unflushed — it is the doc the
        // crashing flush is mid-rotating when it panics.
        insert_doc(&db, "delta", delta_emb.clone()).await?;
        // Flush panics at the seam: L0 is rotated to pending but never written to
        // Lance. The rotated buffer's WAL data must NOT be truncated nor
        // checkpointed past on the subsequent graceful close.
        fail::cfg("flush::after-rotate-before-lance", "panic").unwrap();
        let dbf = db.clone();
        let res = tokio::spawn(async move { dbf.flush().await }).await;
        fail::remove("flush::after-rotate-before-lance");
        assert!(res.is_err(), "flush task should have panicked at the seam");
        drop(db);
    }
    let db = h.open().await?;
    // Both the flushed base and the committed-but-unflushed delta survive.
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(target_emb()),
        "flushed sparse doc lost across crash-during-flush"
    );
    assert_eq!(
        read_emb(&db, "delta").await?,
        Some(delta_emb),
        "committed-but-unflushed sparse doc lost across crash-during-flush (lost-commit regression)"
    );
    // Neither is double-applied by the partial-flush + WAL-replay interplay.
    for title in ["base", "delta"] {
        let n = db
            .session()
            .query_with("MATCH (d:Doc {title: $t}) RETURN d.title AS title")
            .param("t", Value::String(title.to_string()))
            .fetch_all()
            .await?
            .rows()
            .len();
        assert_eq!(
            n, 1,
            "{title} double-applied across crash-during-flush recovery"
        );
    }
    // The sparse query is correct over the recovered rows with NO rebuild: both
    // base (flushed, indexed in L1) and delta (recovered into L0) are unioned by
    // the read path.
    assert_eq!(
        top_sparse_title(&db).await?.as_deref(),
        Some("base"),
        "recovered top sparse match wrong after crash-during-flush (no rebuild)"
    );
    Ok(())
}

/// A torn (corrupt) WAL segment at the TAIL must not block reopen: the torn
/// segment belongs to an unacknowledged sparse commit, so recovery skips it and
/// replays everything before it — the flushed baseline sparse doc survives.
/// Mirrors `ssi_resilience::corrupt_wal_tail_does_not_block_reopen` with a
/// sparse payload (no failpoints required).
#[tokio::test]
async fn sparse_corrupt_wal_tail_skipped_on_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    let base_emb = Value::SparseVector {
        indices: vec![2, 3],
        values: vec![0.7, 0.7],
    };
    {
        // Disable time-based auto-flush so the post-flush tail commit cannot be
        // promoted to L1 by a background flush (which would survive tail
        // corruption and make the assertion flaky).
        let cfg = uni_db::UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        };
        let db = h.open_with(cfg).await?;
        define_sparse_schema(&db).await?;
        insert_doc(&db, "base", base_emb.clone()).await?;
        db.flush().await?;
        // A post-flush sparse commit that becomes the WAL tail.
        insert_doc(&db, "tail", target_emb()).await?;
        drop(db);
    }

    // Simulate a torn write: overwrite the highest-LSN WAL segment with garbage.
    let wal_dir = std::path::PathBuf::from(h.uri()).join("wal");
    let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(&wal_dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "wal"))
        .collect();
    segments.sort();
    let tail = segments.last().expect("at least one WAL segment");
    std::fs::write(tail, b"torn-by-power-loss")?;

    let db = h.open().await?;
    assert_eq!(
        read_emb(&db, "base").await?,
        Some(base_emb),
        "the flushed baseline sparse doc before the torn tail must replay"
    );
    assert_eq!(
        read_emb(&db, "tail").await?,
        None,
        "the torn-tail sparse commit must not resurrect"
    );
    Ok(())
}
