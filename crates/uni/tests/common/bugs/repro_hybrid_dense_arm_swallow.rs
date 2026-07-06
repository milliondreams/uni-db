// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni-query/src/query/df_graph/search_procedures.rs:1578
//! (finding uni-query[4]).
//!
//! `run_hybrid_search` derived the dense query vector by auto-embedding the
//! query text when no explicit vector was supplied. It swallowed any
//! `auto_embed_text` error with `.unwrap_or_default()`, leaving the query vector
//! empty so the dense arm was silently skipped — hybrid search degraded to
//! FTS-only with no error, inconsistent with the fts/sparse arms which use `?`.
//!
//! Fixed: the auto-embed failure now propagates. With a vector property but no
//! embedding configuration, a text-only hybrid search surfaces the error instead
//! of silently returning a dense-less result.

use uni_db::{DataType, Uni};

#[tokio::test]
async fn hybrid_search_propagates_auto_embed_failure() -> anyhow::Result<()> {
    // No xervo runtime / embedding config is registered.
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'zebra stripes pattern', embedding: [0.9, 0.1]})")
        .await?;
    tx.commit().await?;

    let tx2 = db.session().tx().await?;
    tx2.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.content]")
        .await?;
    tx2.commit().await?;
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;

    // Hybrid search with a query TEXT but NO explicit query vector: the engine
    // must auto-embed 'zebra' for the dense arm. With no embedding config that
    // auto-embed fails.
    let res = db
        .session()
        .query(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content'}, 'zebra', null, 5, null, {}) \
             YIELD node RETURN node.content AS c",
        )
        .await;

    // Fixed (search_procedures.rs:1578): the auto-embed failure propagates as an
    // error instead of being swallowed to an empty vector that silently drops the
    // dense arm and returns an FTS-only result.
    assert!(
        res.is_err(),
        "hybrid search must surface the auto-embed failure, not silently drop the dense arm; got {:?}",
        res.map(|r| r.rows().len())
    );

    Ok(())
}
