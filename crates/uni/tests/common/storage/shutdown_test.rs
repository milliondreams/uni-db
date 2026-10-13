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

/// A dataset directory with no committed manifest is not a table.
///
/// Lance writes a new dataset's data files before it commits the first
/// manifest, so a create cut off midway (a crash or a runtime shut down during
/// a label's first flush) leaves `<table>.lance/` holding data and no
/// `_versions/`. Listing tables by directory counted it, and every later open
/// failed: `open table 'vertices' at '.../vertices.lance'`, for reads of the
/// label and for the flush that would recreate it. Seen by dropping a `UniSync`
/// whose runtime ended mid-flush; reproduced here by leaving the same remains.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupted_first_create_does_not_wedge_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let quiet = || {
        Uni::open(&path).config(uni_db::UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        })
    };
    {
        let db = quiet().build().await.unwrap();
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (:Kept {n: 1})").await.unwrap();
        tx.commit().await.unwrap();
        drop(db);
    }
    // What an interrupted create leaves: the dataset directory and a data file,
    // no manifest.
    let remains = dir.path().join("storage/vertices.lance/data");
    std::fs::create_dir_all(&remains).unwrap();
    std::fs::write(remains.join("partial.lance"), b"not a finished fragment").unwrap();

    let db = quiet().build().await.unwrap();
    let count = |db: &Uni| {
        let session = db.session();
        async move {
            session
                .query("MATCH (k:Kept) RETURN count(k) AS c")
                .await
                .unwrap()
                .rows()[0]
                .get::<i64>("c")
                .unwrap()
        }
    };
    assert_eq!(count(&db).await, 1, "the WAL-replayed row is readable");
    db.flush()
        .await
        .expect("the first flush recreates the table");
    assert_eq!(count(&db).await, 1, "the flushed row is readable");
    db.shutdown().await.unwrap();

    let db = quiet().build().await.unwrap();
    assert_eq!(count(&db).await, 1, "the row survives a reopen");
    db.shutdown().await.unwrap();
}

/// The fork path's existence check makes the same distinction.
///
/// A fork's first write of a label asks the branching backend whether primary
/// has the dataset, to branch from it or create an empty parent. That check
/// looked only for the directory, so the remains of an interrupted create were
/// taken for a dataset with no version to branch from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fork_writes_past_an_interrupted_first_create() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let remains = dir.path().join("storage/vertices.lance/data");
    std::fs::create_dir_all(&remains).unwrap();
    std::fs::write(remains.join("partial.lance"), b"not a finished fragment").unwrap();

    let db = Uni::open(&path)
        .config(uni_db::UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let fork = db.session().fork("past_remains").await.unwrap();
    let tx = fork.tx().await.unwrap();
    tx.execute("CREATE (:Kept {n: 1})").await.unwrap();
    tx.commit().await.unwrap();
    fork.flush()
        .await
        .expect("the fork's first flush creates the dataset");
    let rows = fork.query("MATCH (k:Kept) RETURN k.n AS n").await.unwrap();
    assert_eq!(rows.len(), 1);
    drop(fork);
    db.shutdown().await.unwrap();
}
