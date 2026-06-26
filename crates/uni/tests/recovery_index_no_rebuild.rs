// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: secondary indexes do NOT require a rebuild after WAL recovery.
//!
//! A committed-but-unflushed write is durable in the WAL; on reopen it is
//! replayed into L0. `replay_wal` does not repopulate secondary index
//! structures (it only rebuilds the UNIQUE constraint index), but a rebuild is
//! nonetheless unnecessary because of two independent mechanisms, both pinned
//! here for every index kind (dense vector / full-text / scalar — the sparse
//! analogue lives in `sparse_index.rs::sparse_recovered_delta_queryable_without_rebuild`):
//!
//!   [A] L0-union read path — recovered rows live in L0 and every index read
//!       path unions live L0 candidates, so a query is correct with the index
//!       still cold (`merge_l0_into_*` / `collect_l0_label_candidates`).
//!   [B] flush re-indexes — the next flush recomputes the index delta from a
//!       FULL L0 scan, writing the recovered rows into L1. After a SECOND reopen
//!       (WAL truncated, L0 empty) the query is served purely from the L1 index.
//!
//! This disproves the previously-documented "secondary indexes (all kinds) must
//! be rebuilt after an unflushed-then-recovered write" belief.

use uni_db::{
    DataType, IndexType, ScalarType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric,
};

/// `true` if any result row's `key` column equals `want`.
async fn contains(db: &Uni, query: &str, key: &str, want: &str) -> anyhow::Result<bool> {
    let res = db.session().query(query).await?;
    Ok(res
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>(key).ok())
        .any(|t| t == want))
}

#[tokio::test]
async fn dense_vector_recovered_delta_queryable_without_rebuild() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let query = "CALL uni.vector.query('Doc','emb',[0.0,1.0,0.0,0.0],5,null,null,{}) \
                 YIELD node, score RETURN node.title AS title";
    {
        let db = Uni::open(path).build().await?;
        db.schema()
            .label("Doc")
            .property("title", DataType::String)
            .property("emb", DataType::Vector { dimensions: 4 })
            .index(
                "emb",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: None,
                }),
            )
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 'base', emb: [1.0,0.0,0.0,0.0]})")
            .await?;
        tx.commit().await?;
        db.flush().await?; // -> manifest, base indexed in L1
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 'target', emb: [0.0,1.0,0.0,0.0]})")
            .await?;
        tx.commit().await?; // committed, NOT flushed -> WAL only
        drop(db);
    }

    // [A] L0-union path (no rebuild, no flush).
    let db = Uni::open(path).build().await?;
    assert!(
        contains(&db, query, "title", "target").await?,
        "dense-vector: recovered delta must be visible via L0 union with no rebuild"
    );

    // [B] L1-index only (flush re-indexes, then reopen; no rebuild).
    db.flush().await?;
    drop(db);
    let db = Uni::open(path).build().await?;
    assert!(
        contains(&db, query, "title", "target").await?,
        "dense-vector: flush after recovery must maintain the L1 index (no rebuild)"
    );
    Ok(())
}

#[tokio::test]
async fn fulltext_recovered_delta_queryable_without_rebuild() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let query = "CALL uni.fts.query('Doc','content','medium',5,null,null,{}) \
                 YIELD node RETURN node.title AS title";
    {
        let db = Uni::open(path).build().await?;
        db.schema()
            .label("Doc")
            .property("title", DataType::String)
            .property("content", DataType::String)
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.content]")
            .await?;
        tx.commit().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 'base', content: 'short text'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        db.indexes().rebuild("Doc", false).await?; // index the base corpus
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 'target', content: 'a medium length document'})")
            .await?;
        tx.commit().await?; // committed, NOT flushed -> WAL only
        drop(db);
    }

    // [A] L0-union path (no rebuild, no flush).
    let db = Uni::open(path).build().await?;
    assert!(
        contains(&db, query, "title", "target").await?,
        "full-text: recovered delta must be visible via L0 union with no rebuild"
    );

    // [B] L1-index only (flush re-indexes, then reopen; no rebuild).
    db.flush().await?;
    drop(db);
    let db = Uni::open(path).build().await?;
    assert!(
        contains(&db, query, "title", "target").await?,
        "full-text: flush after recovery must maintain the L1 index (no rebuild)"
    );
    Ok(())
}

#[tokio::test]
async fn scalar_recovered_delta_queryable_without_rebuild() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let query = "MATCH (d:Doc) WHERE d.n = 42 RETURN d.title AS title";
    {
        let db = Uni::open(path).build().await?;
        db.schema()
            .label("Doc")
            .property("title", DataType::String)
            .property("n", DataType::Int64)
            .index("n", IndexType::Scalar(ScalarType::BTree))
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 'base', n: 1})").await?;
        tx.commit().await?;
        db.flush().await?;
        let tx = db.session().tx().await?;
        tx.execute_with("CREATE (:Doc {title: $t, n: 42})")
            .param("t", Value::String("target".into()))
            .run()
            .await?;
        tx.commit().await?; // committed, NOT flushed -> WAL only
        drop(db);
    }

    // [A] L0-union path (no rebuild, no flush).
    let db = Uni::open(path).build().await?;
    assert!(
        contains(&db, query, "title", "target").await?,
        "scalar: recovered delta must be visible via the indexed WHERE lookup with no rebuild"
    );

    // [B] L1-index only (flush re-indexes, then reopen; no rebuild).
    db.flush().await?;
    drop(db);
    let db = Uni::open(path).build().await?;
    assert!(
        contains(&db, query, "title", "target").await?,
        "scalar: flush after recovery must maintain the L1 index (no rebuild)"
    );
    Ok(())
}

/// Scalar UPDATE: a recovered-but-unflushed SET of an indexed property must
/// override the stale L1 index entry without a rebuild. `target` is flushed with
/// n=42 (the indexed value), then SET to n=99 unflushed. After recovery the old
/// value must no longer match and the new value must.
#[tokio::test]
async fn scalar_recovered_update_overrides_stale_entry_without_rebuild() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let q_old = "MATCH (d:Doc) WHERE d.n = 42 RETURN d.title AS title";
    let q_new = "MATCH (d:Doc) WHERE d.n = 99 RETURN d.title AS title";
    {
        let db = Uni::open(path).build().await?;
        db.schema()
            .label("Doc")
            .property("title", DataType::String)
            .property("n", DataType::Int64)
            .index("n", IndexType::Scalar(ScalarType::BTree))
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute_with("CREATE (:Doc {title: $t, n: 42})")
            .param("t", Value::String("target".into()))
            .run()
            .await?;
        tx.commit().await?;
        db.flush().await?; // target indexed at n=42
        let tx = db.session().tx().await?;
        tx.execute("MATCH (d:Doc {title: 'target'}) SET d.n = 99")
            .await?;
        tx.commit().await?; // committed, NOT flushed
        drop(db);
    }

    // [A] L0-union path.
    let db = Uni::open(path).build().await?;
    assert!(
        !contains(&db, q_old, "title", "target").await?,
        "scalar update [L0-union]: stale n=42 entry must not match after recovery"
    );
    assert!(
        contains(&db, q_new, "title", "target").await?,
        "scalar update [L0-union]: recovered n=99 value must match"
    );

    // [B] L1-index only.
    db.flush().await?;
    drop(db);
    let db = Uni::open(path).build().await?;
    assert!(
        !contains(&db, q_old, "title", "target").await?,
        "scalar update [L1-index]: stale n=42 entry must not match after flush+reopen"
    );
    assert!(
        contains(&db, q_new, "title", "target").await?,
        "scalar update [L1-index]: recovered n=99 value must match after flush+reopen"
    );
    Ok(())
}

/// Scalar DELETE: a recovered-but-unflushed tombstone must hide the doc from an
/// indexed WHERE lookup without a rebuild, and the flush must drop it from L1.
#[tokio::test]
async fn scalar_recovered_delete_hides_doc_without_rebuild() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let query = "MATCH (d:Doc) WHERE d.n = 42 RETURN d.title AS title";
    {
        let db = Uni::open(path).build().await?;
        db.schema()
            .label("Doc")
            .property("title", DataType::String)
            .property("n", DataType::Int64)
            .index("n", IndexType::Scalar(ScalarType::BTree))
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute_with("CREATE (:Doc {title: $t, n: 42})")
            .param("t", Value::String("target".into()))
            .run()
            .await?;
        tx.commit().await?;
        db.flush().await?; // target indexed at n=42
        let tx = db.session().tx().await?;
        tx.execute("MATCH (d:Doc {title: 'target'}) DELETE d")
            .await?;
        tx.commit().await?; // committed, NOT flushed
        drop(db);
    }

    // [A] L0-union path: recovered tombstone hides the indexed doc.
    let db = Uni::open(path).build().await?;
    assert!(
        !contains(&db, query, "title", "target").await?,
        "scalar delete: recovered tombstone must hide the doc via the index lookup (no rebuild)"
    );

    // [B] L1-index only.
    db.flush().await?;
    drop(db);
    let db = Uni::open(path).build().await?;
    assert!(
        !contains(&db, query, "title", "target").await?,
        "scalar delete: flush after recovery must drop the doc from the L1 index (no rebuild)"
    );
    Ok(())
}

/// Dense-vector DELETE: a recovered-but-unflushed tombstone must hide the doc
/// from a vector query without a rebuild, and the flush must drop it from L1.
#[tokio::test]
async fn dense_vector_recovered_delete_hides_doc_without_rebuild() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let query = "CALL uni.vector.query('Doc','emb',[0.0,1.0,0.0,0.0],5,null,null,{}) \
                 YIELD node, score RETURN node.title AS title";
    {
        let db = Uni::open(path).build().await?;
        db.schema()
            .label("Doc")
            .property("title", DataType::String)
            .property("emb", DataType::Vector { dimensions: 4 })
            .index(
                "emb",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: None,
                }),
            )
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 'base', emb: [1.0,0.0,0.0,0.0]})")
            .await?;
        tx.execute("CREATE (:Doc {title: 'target', emb: [0.0,1.0,0.0,0.0]})")
            .await?;
        tx.commit().await?;
        db.flush().await?; // target indexed (exact query match)
        let tx = db.session().tx().await?;
        tx.execute("MATCH (d:Doc {title: 'target'}) DELETE d")
            .await?;
        tx.commit().await?; // committed, NOT flushed
        drop(db);
    }

    // [A] L0-union path: recovered tombstone hides the indexed match.
    let db = Uni::open(path).build().await?;
    assert!(
        !contains(&db, query, "title", "target").await?,
        "dense-vector delete: recovered tombstone must hide the doc (no rebuild)"
    );

    // [B] L1-index only.
    db.flush().await?;
    drop(db);
    let db = Uni::open(path).build().await?;
    assert!(
        !contains(&db, query, "title", "target").await?,
        "dense-vector delete: flush after recovery must drop the doc from the L1 index (no rebuild)"
    );
    Ok(())
}
