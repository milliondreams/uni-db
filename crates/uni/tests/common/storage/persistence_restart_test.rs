// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Verifies that data written to the database persists to disk and survives restart.

use anyhow::Result;
use tempfile::tempdir;
use uni_db::{DataType, Uni, UniConfig};

#[tokio::test]
async fn test_data_survives_restart() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path().to_str().unwrap().to_string();

    // --- PHASE 1: Write data and shut down ---
    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("Person")
            .property("name", DataType::String)
            .apply()
            .await?;

        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Person {name: 'PersistenceCheck'})")
            .await?;
        tx.commit().await?;

        // Flush to disk
        db.flush().await?;

        // db is dropped here, simulating shutdown
    }

    // --- PHASE 2: Restart and verify data survived ---
    {
        let db = Uni::open(&path).build().await?;

        let result = db
            .session()
            .query("MATCH (n:Person) RETURN n.name AS name")
            .await?;

        assert_eq!(result.len(), 1, "Data should persist after restart");
        assert_eq!(result.rows()[0].get::<String>("name")?, "PersistenceCheck");
    }

    Ok(())
}

/// Regression for review #5: a transaction that commits an edge whose endpoint
/// a concurrent transaction has deleted must be rejected *before* the durable
/// WAL flush, so it never becomes a ghost commit. Before the fix the merge
/// bailed on the issue-#77 endpoint guard *after* the flush, leaving a durable
/// but unmerged transaction that re-bailed on WAL replay — making the database
/// unopenable. This test runs with SSI disabled (so the read/write conflict
/// does not pre-empt the commit) and asserts the database still reopens cleanly.
#[tokio::test]
async fn test_edge_to_concurrently_deleted_endpoint_does_not_ghost_commit() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path().to_str().unwrap().to_string();

    {
        // SSI off: no commit-time read/write conflict detection, so the
        // endpoint-liveness check is the only thing standing between this
        // interleaving and a ghost commit.
        let config = UniConfig {
            ssi_enabled: false,
            ..Default::default()
        };
        let db = Uni::open(&path).config(config).build().await?;
        db.schema()
            .label("N")
            .property("name", DataType::String)
            .done()
            .edge_type("R", &["N"], &["N"])
            .done()
            .apply()
            .await?;

        // Seed two committed vertices.
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:N {name: 'a'}), (:N {name: 'b'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;

        // tx_b binds the existing `a` and `b` and creates an edge between them.
        let tx_b = db.session().tx().await?;
        tx_b.execute("MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) CREATE (a)-[:R]->(b)")
            .await?;

        // Concurrently, tx_a deletes `b` and commits — tombstoning it in main L0.
        let tx_a = db.session().tx().await?;
        tx_a.execute("MATCH (b:N {name: 'b'}) DELETE b").await?;
        tx_a.commit().await?;

        // tx_b now commits an edge to the deleted `b`. It must be rejected, and
        // crucially must NOT have flushed a durable (ghost) record.
        let result = tx_b.commit().await;
        assert!(
            result.is_err(),
            "committing an edge to a concurrently-deleted endpoint must fail"
        );
    }

    // The database must reopen cleanly (no unopenable-DB ghost commit).
    {
        let db = Uni::open(&path).build().await?;
        let count = db
            .session()
            .query("MATCH (n:N) RETURN count(n) AS c")
            .await?
            .rows()[0]
            .get::<i64>("c")?;
        assert_eq!(count, 1, "only `a` should remain; `b` was deleted");

        let edges = db
            .session()
            .query("MATCH ()-[r:R]->() RETURN count(r) AS c")
            .await?
            .rows()[0]
            .get::<i64>("c")?;
        assert_eq!(edges, 0, "the rejected edge must not have been committed");
    }

    Ok(())
}
