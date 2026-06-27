//! Issue #115 / #117: pin the `open_raw` <-> backend storage-path contract and
//! assert that post-flush index builds actually engage the physical index
//! (mechanism, not just query results).
//!
//! #115 root cause: `VertexDataset::open_raw()` reconstructed the on-disk path
//! as `{base}/vertices_<label>` while the LanceDB backend stores the table at
//! `{base}/vertices_<label>.lance`. Two independent reconstructions of the same
//! external contract drifted, so every raw read of a flushed vertex table
//! silently returned `Err`/0 rows. #117: the prior tests asserted query
//! *results* (which stayed correct via full-scan / brute-force fallback), so
//! the divergence was invisible. These tests assert the *mechanism* instead.

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Build a temporary on-disk `Uni` with `n` `Item` rows flushed to Lance.
async fn db_with_flushed_items(n: usize) -> Result<Uni> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property("tags", DataType::List(Box::new(DataType::String)))
        .property("content", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..n {
        tx.execute(&format!(
            "CREATE (:Item {{name: 'item-{i}', tags: ['tag-{i}'], \
             content: 'body number {i} zebra'}})"
        ))
        .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// THE contract pin (#117's highest-value guard): after a flush, the raw
/// `VertexDataset::open_raw()` path and the `StorageBackend` must agree on the
/// row count for a label. This fails loudly the instant the two on-disk-path
/// reconstructions drift again — the exact root cause of #115.
#[tokio::test]
async fn open_raw_row_count_matches_backend_after_flush() -> Result<()> {
    let db = db_with_flushed_items(50).await?;
    let storage = db.storage();
    let table = uni_db::store::backend::table_names::vertex_table_name("Item");

    let backend_count = storage.backend().count_rows(&table, None).await?;

    let raw_count = storage
        .vertex_dataset("Item")?
        .open_raw()
        .await
        .expect(
            "open_raw must open the flushed vertex table; if this errors, the \
             VertexDataset path and the backend path have drifted (#115)",
        )
        .count_rows(None)
        .await?;

    assert_eq!(
        raw_count, backend_count,
        "open_raw and StorageBackend disagree on row count — the storage-path \
         reconstructions have drifted (root cause of #115)"
    );
    assert_eq!(backend_count, 50, "all 50 rows should be flushed");
    Ok(())
}

/// On-disk layout: the flushed vertex table must live at
/// `{base}/vertices_<label>.lance` (the `.lance` suffix the backend writes and
/// that `open_raw` must target).
#[tokio::test]
async fn vertices_lance_directory_exists_after_flush() -> Result<()> {
    let db = db_with_flushed_items(10).await?;
    let base = db.storage().base_uri().to_string();
    let dir = format!("{base}/vertices_Item.lance");
    assert!(
        std::path::Path::new(&dir).exists(),
        "expected the flushed vertex dataset directory at {dir}"
    );
    Ok(())
}

/// Mechanism for #115: a scalar index created AFTER a flush must physically
/// exist on the Lance table. Before the path fix, the post-flush backfill
/// opened a non-existent path and silently skipped the build, so the physical
/// index was absent (queries still worked via full-scan fallback, masking it).
#[tokio::test]
async fn scalar_index_physically_built_after_flush() -> Result<()> {
    let db = db_with_flushed_items(64).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE INDEX idx_item_name FOR (i:Item) ON (i.name)")
        .await?;
    tx.commit().await?;

    let storage = db.storage();
    let table = uni_db::store::backend::table_names::vertex_table_name("Item");
    let indexes = storage.backend().list_indexes(&table).await?;
    assert!(
        indexes
            .iter()
            .any(|idx| idx.columns.iter().any(|c| c == "name")),
        "no physical Lance index on Item.name after create-after-flush; the \
         backfill silently skipped the build (#115). Indexes present: {indexes:?}"
    );
    Ok(())
}

/// Mechanism for #115: an FTS index created AFTER a flush must physically exist
/// on the Lance table (FTS has a full-scan fallback that otherwise masks a
/// missing physical index).
#[tokio::test]
async fn fts_index_physically_built_after_flush() -> Result<()> {
    let db = db_with_flushed_items(64).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE FULLTEXT INDEX idx_item_content FOR (i:Item) ON EACH [i.content]")
        .await?;
    tx.commit().await?;

    let storage = db.storage();
    let table = uni_db::store::backend::table_names::vertex_table_name("Item");
    let indexes = storage.backend().list_indexes(&table).await?;
    assert!(
        indexes
            .iter()
            .any(|idx| idx.columns.iter().any(|c| c == "content")),
        "no physical Lance FTS index on Item.content after create-after-flush; \
         the backfill silently skipped the build (#115). Indexes present: {indexes:?}"
    );
    Ok(())
}

/// End-to-end regression for the migrated inverted-index backfill: an inverted
/// index created AFTER a flush (now built via `backend.scan` +
/// `InvertedIndex::build_from_batches`) must return the right rows over the
/// already-flushed data.
#[tokio::test]
async fn inverted_index_built_after_flush_returns_results() -> Result<()> {
    let db = db_with_flushed_items(40).await?;

    db.session()
        .query("CALL uni.schema.createIndex('Item', 'tags', {type: 'inverted'})")
        .await?;

    let res = db
        .session()
        .query("MATCH (i:Item) WHERE ANY(t IN i.tags WHERE t IN ['tag-7']) RETURN i.name AS name")
        .await?;
    assert_eq!(
        res.len(),
        1,
        "inverted index over flushed data should match exactly tag-7"
    );
    assert_eq!(
        res.rows()[0].get::<String>("name")?,
        "item-7",
        "inverted index returned the wrong row"
    );
    Ok(())
}
