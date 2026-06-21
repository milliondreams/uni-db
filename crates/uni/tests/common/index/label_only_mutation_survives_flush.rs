// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Review M8: a label-only mutation (`SET n:Label` / `REMOVE n:Label`)
//! made in a *later* flush window than the vertex's creation must survive
//! its own flush and a restart.
//!
//! Before the fix, the flush built the main table, per-label datasets, and
//! the `VidLabelsIndex` strictly from `vertex_properties`. A pure relabel
//! of a prior-window vid marks `vertex_label_overwrites` but never touches
//! `vertex_properties`, so the relabel was silently dropped at flush and
//! `rebuild_vid_labels_index` read the stale labels on reopen. The
//! same-window create+relabel case worked, which is why this went unseen.

// Rust guideline compliant

use uni_db::{DataType, Uni, UniConfig};

async fn count(session: &uni_db::Session, cypher: &str) -> usize {
    session.query(cypher).await.unwrap().rows().len()
}

#[tokio::test]
async fn label_only_mutation_survives_flush_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // High threshold so only our explicit `flush()` calls flush — this
    // keeps "create" and "relabel" in distinct, deterministic windows.
    // The fork sweeper/index-builder are irrelevant here; disabling them
    // avoids their background-loop sleep delaying `shutdown()`.
    let config = UniConfig {
        auto_flush_threshold: 100_000,
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..Default::default()
    };

    {
        let db = Uni::open(&uri)
            .config(config.clone())
            .build()
            .await
            .unwrap();
        db.schema()
            .label("A")
            .property("val", DataType::Int64)
            .apply()
            .await
            .unwrap();
        db.schema()
            .label("B")
            .property("val", DataType::Int64)
            .apply()
            .await
            .unwrap();

        let session = db.session();

        // Window 1: create (:A {val: 42}) and flush it to L1.
        let tx = session.tx().await.unwrap();
        tx.execute("CREATE (:A {val: 42})").await.unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();
        assert_eq!(count(&session, "MATCH (n:A) RETURN n").await, 1);

        // Window 2: pure relabel A -> B (no property write), then flush.
        let tx = session.tx().await.unwrap();
        tx.execute("MATCH (n:A) SET n:B REMOVE n:A").await.unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();

        // Post-flush, in the same process: B present, A gone, props kept.
        assert_eq!(
            count(&session, "MATCH (n:B) RETURN n").await,
            1,
            "relabel to B was lost at flush (M8)"
        );
        assert_eq!(
            count(&session, "MATCH (n:A) RETURN n").await,
            0,
            "old label A not tombstoned in its per-label dataset (M8)"
        );
        let rows = session
            .query("MATCH (n:B) WHERE n.val = 42 RETURN n")
            .await
            .unwrap();
        assert_eq!(
            rows.rows().len(),
            1,
            "relabeled vertex lost its properties at flush (M8)"
        );

        // Data was made durable by the explicit `flush()` above; drop
        // rather than `shutdown().await` so the test isn't delayed by the
        // shutdown-drain grace period.
        drop(session);
        drop(db);
    }

    // Reopen: `rebuild_vid_labels_index` must read the durable B label,
    // not the stale A from the main table.
    {
        let db = Uni::open(&uri).config(config).build().await.unwrap();
        let session = db.session();
        assert_eq!(
            count(&session, "MATCH (n:B) RETURN n").await,
            1,
            "relabel to B did not survive reopen (M8)"
        );
        assert_eq!(
            count(&session, "MATCH (n:A) RETURN n").await,
            0,
            "stale label A reappeared after reopen (M8)"
        );
        let rows = session
            .query("MATCH (n:B) WHERE n.val = 42 RETURN n")
            .await
            .unwrap();
        assert_eq!(rows.rows().len(), 1, "properties lost after reopen (M8)");
        drop(session);
        drop(db);
    }
}
