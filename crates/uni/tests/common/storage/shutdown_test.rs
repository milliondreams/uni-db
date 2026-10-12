use uni_db::Uni;

#[tokio::test(flavor = "multi_thread")]
async fn test_simple_create_drop() {
    println!("Creating DB...");
    let db = Uni::in_memory().build().await.unwrap();
    println!("DB created");
    drop(db);
    println!("DB dropped");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    println!("Test complete");
}

/// Whether a flush has published a snapshot manifest under `root`.
///
/// Only an L0 -> L1 flush writes one (see `first_flush_resilience`), so this
/// reads "has anything flushed" without depending on how datasets are named.
fn has_flushed(root: &std::path::Path) -> bool {
    std::fs::read_dir(root.join("storage/catalog/manifests"))
        .is_ok_and(|mut entries| entries.next().is_some())
}

/// Dropping a database without `shutdown()` does not flush, and loses nothing.
///
/// With the WAL on, every commit is durable before it returns, so the auto-flush
/// task's final flush on the drop signal bought only a shorter replay. Nothing
/// awaits it, so a runtime shutting down right after the drop cut it off midway:
/// Lance panicked on the cancelled task (`Background task failed:
/// JoinError::Cancelled`), and on a temporary database it raced the directory's
/// deletion (#167). The reopen below is the durability half: the row comes back
/// from the WAL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_without_shutdown_leaves_the_data_to_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    {
        let db = Uni::open(&path).build().await.unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (:Kept {n: 1})").await.unwrap();
        tx.commit().await.unwrap();
        assert!(!has_flushed(dir.path()), "flushed before the drop");
        drop(db);
    }
    // Long enough for a final flush, had the drop started one, to land.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(
        !has_flushed(dir.path()),
        "the drop flushed L0 although the WAL already holds it"
    );

    let db = Uni::open(&path).build().await.unwrap();
    let rows = db
        .session()
        .query("MATCH (k:Kept) RETURN k.n AS n")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the committed row did not survive the drop");
    db.shutdown().await.unwrap();
}
