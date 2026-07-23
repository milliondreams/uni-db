//! G8/E2 — the cheap write-isolated "scratch" transaction (`Session::scratch`).
//!
//! Design-spike proofs: a scratch transaction is write-isolated from primary,
//! offers read-your-writes (including edge traversal — the adjacency-warm risk),
//! refuses commit, and is discarded on drop with no trace — at pinned-snapshot
//! cost, so thousands of speculative rollouts leave the primary and its durable
//! id allocator untouched.

use uni_db::{DataType, Uni};

async fn count_n(db: &Uni) -> anyhow::Result<i64> {
    Ok(db
        .session()
        .query("MATCH (n:N) RETURN count(n) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?)
}

#[tokio::test]
async fn scratch_tx_is_write_isolated_and_never_commits() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .done()
        .edge_type("R", &["N"], &["N"])
        .done()
        .apply()
        .await?;
    let session = db.session();
    {
        let tx = session.tx().await?;
        tx.execute("CREATE (:N {name: 'seed'})").await?;
        tx.commit().await?;
    }
    assert_eq!(count_n(&db).await?, 1, "primary starts with the seed node");

    // Open a scratch and write nodes + an edge into it.
    let scratch = session.scratch().await?;
    scratch.execute("CREATE (:N {name: 'a'})").await?;
    scratch.execute("CREATE (:N {name: 'b'})").await?;
    scratch
        .execute("MATCH (a:N {name: 'a'}), (b:N {name: 'b'}) CREATE (a)-[:R]->(b)")
        .await?;

    // Read-your-writes: the scratch sees base (seed) + its own 2 nodes.
    let seen = scratch
        .query("MATCH (n:N) RETURN count(n) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(seen, 3, "scratch reads base + its own uncommitted writes");

    // Read-your-writes for EDGE TRAVERSAL — the adjacency-warm risk: the scratch
    // edge (a)-[:R]->(b), written to the private L0, must be traversable over the
    // pinned base's shared adjacency.
    let traversed = scratch
        .query("MATCH (a:N {name: 'a'})-[:R]->(x:N) RETURN x.name AS n")
        .await?;
    assert_eq!(traversed.rows().len(), 1, "the scratch edge is traversable");
    assert_eq!(traversed.rows()[0].get::<String>("n")?, "b");

    // Isolation: a concurrent session sees only the seed, not the scratch writes.
    let other = db.session();
    assert_eq!(
        other
            .query("MATCH (n:N) RETURN count(n) AS c")
            .await?
            .rows()[0]
            .get::<i64>("c")?,
        1,
        "primary is isolated from the scratch's uncommitted writes"
    );

    // Commit is refused (consumes the scratch, releasing the write guard).
    let err = scratch
        .commit()
        .await
        .expect_err("a scratch transaction must refuse commit");
    assert!(
        err.to_string().contains("scratch"),
        "the commit error must name the scratch, got: {err}"
    );

    // No trace: after the scratch is gone, the primary is still just the seed.
    assert_eq!(
        count_n(&db).await?,
        1,
        "no trace of the scratch after it ends"
    );
    Ok(())
}

#[tokio::test]
async fn scratch_tx_many_rollouts_leave_primary_unchanged() -> anyhow::Result<()> {
    // Thousands of open-write-discard rollouts must not touch the primary — no
    // committed rows, and a real transaction afterward still works (the global
    // allocator was never corrupted by the scratches).
    let db = Uni::in_memory().build().await?;
    db.schema().label("N").done().apply().await?;
    let session = db.session();

    for _ in 0..1000 {
        let s = session.scratch().await?;
        s.execute("CREATE (:N)").await?;
        s.execute("CREATE (:N)").await?;
        // Dropped here without commit — the writes vanish.
    }
    assert_eq!(
        count_n(&db).await?,
        0,
        "1000 scratch rollouts leave primary empty"
    );

    // A normal committing transaction still works afterward.
    {
        let tx = session.tx().await?;
        tx.execute("CREATE (:N)").await?;
        tx.commit().await?;
    }
    assert_eq!(
        count_n(&db).await?,
        1,
        "a real tx after the scratches commits normally"
    );
    Ok(())
}
