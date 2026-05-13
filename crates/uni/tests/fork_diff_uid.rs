// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 6b — UID-keyed diff between unrelated forks.
//!
//! The Phase 6 MVP keyed diffs by VID, which is correct for
//! fork-vs-ancestor comparisons (where inherited VIDs are stable)
//! but breaks for siblings or unrelated forks that independently
//! roll the same VIDs for different content. Phase 6b lifts identity
//! to the content-addressed UID; this test exercises the unrelated-
//! forks case where two siblings both have VIDs at the same low
//! numbers but distinct property bags.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn diff_pairs_by_uid_across_unrelated_forks() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Seed primary with a row so both siblings inherit a non-empty
    // baseline. We'll add a row with the SAME content on both sibling
    // forks — UID is identical → diff sees it as a match (not an add).
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Inherited-Alice'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let sibling_a = session.fork("sibling_a").await?;
        let tx = sibling_a.tx().await?;
        tx.execute("CREATE (:Person {name: 'Shared'})").await?;
        tx.execute("CREATE (:Person {name: 'A-Only'})").await?;
        tx.commit().await?;
    }

    {
        let sibling_b = session.fork("sibling_b").await?;
        let tx = sibling_b.tx().await?;
        tx.execute("CREATE (:Person {name: 'Shared'})").await?;
        tx.execute("CREATE (:Person {name: 'B-Only'})").await?;
        tx.commit().await?;
    }

    let diff = db.diff_forks("sibling_a", "sibling_b").await?;
    // 'Shared' has the same UID on both sides — paired, no diff row.
    // 'A-Only' is on a, not b → deleted in diff(a, b).
    // 'B-Only' is on b, not a → added.
    assert_eq!(
        diff.vertices.added.len(),
        1,
        "expected exactly one fork-b-only row (B-Only), got {:?}",
        diff.vertices
            .added
            .iter()
            .map(|v| v.properties.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        diff.vertices.deleted.len(),
        1,
        "expected exactly one fork-a-only row (A-Only), got {:?}",
        diff.vertices
            .deleted
            .iter()
            .map(|v| v.properties.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        diff.vertices.changed.is_empty(),
        "Shared row should pair cleanly without prop diff: changed = {:?}",
        diff.vertices.changed
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn diff_distinguishes_parallel_edges() -> Result<()> {
    // Phase 7d: two forks with parallel KNOWS edges differing only
    // in the `since` property must show up as one added + one
    // deleted in the diff, not collapsed to "no change". Under
    // Phase 6b's identity (src_uid, dst_uid, type) they would have
    // collapsed to one bucket per side; under Phase 7d the
    // content-addressed edge UID separates them.
    use uni_db::DataType;
    let db = uni_db::Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;

    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'Anchor'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let a = primary.fork("year_a").await?;
        let tx = a.tx().await?;
        tx.execute(
            "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), \
                    (a)-[:KNOWS {since: 2020}]->(b)",
        )
        .await?;
        tx.commit().await?;
        a.flush().await?;
    }
    {
        let b = primary.fork("year_b").await?;
        let tx = b.tx().await?;
        tx.execute(
            "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), \
                    (a)-[:KNOWS {since: 2024}]->(b)",
        )
        .await?;
        tx.commit().await?;
        b.flush().await?;
    }

    let diff = db.diff_forks("year_a", "year_b").await?;
    assert_eq!(
        diff.edges.added.len(),
        1,
        "year_b-only edge expected, got {:?}",
        diff.edges.added
    );
    assert_eq!(
        diff.edges.deleted.len(),
        1,
        "year_a-only edge expected, got {:?}",
        diff.edges.deleted
    );
    // The two edges share the same endpoints + type but differ in
    // properties — under content-addressed identity they have
    // distinct edge_uids.
    assert_ne!(
        diff.edges.added[0].edge_uid, diff.edges.deleted[0].edge_uid,
        "added and deleted edges must have different edge_uids"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn diff_inversion_holds_under_uid_identity() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {name: 'Anchor'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let a = session.fork("left").await?;
        let tx = a.tx().await?;
        tx.execute("CREATE (:Item {name: 'L1'})").await?;
        tx.commit().await?;
    }
    {
        let b = session.fork("right").await?;
        let tx = b.tx().await?;
        tx.execute("CREATE (:Item {name: 'R1'})").await?;
        tx.commit().await?;
    }

    let forward = db.diff_forks("left", "right").await?;
    let reverse = db.diff_forks("right", "left").await?;

    // Swap added/deleted, structure should match. Compare by count
    // (the rows themselves contain UIDs that survive the swap).
    assert_eq!(forward.vertices.added.len(), reverse.vertices.deleted.len());
    assert_eq!(forward.vertices.deleted.len(), reverse.vertices.added.len());

    let inverted = forward.invert();
    assert_eq!(inverted.vertices.added.len(), reverse.vertices.added.len());
    assert_eq!(
        inverted.vertices.deleted.len(),
        reverse.vertices.deleted.len()
    );

    db.shutdown().await?;
    Ok(())
}
