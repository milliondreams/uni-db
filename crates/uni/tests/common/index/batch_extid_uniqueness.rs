// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A batched vertex insert must enforce `ext_id` uniqueness against
//! flushed storage, matching the single-vertex CREATE path. Before the
//! fix, `validate_vertex_batch_constraints` checked only the in-memory L0
//! buffers, so a batch CREATE whose `ext_id` already existed in L1 was
//! silently accepted as a duplicate.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn batch_create_rejects_storage_duplicate_ext_id() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();

    // Seed `p1` and flush it to L1 (so the L0-only check would miss it).
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'A'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // A batched create (UNWIND … CREATE) where one row reuses the flushed
    // `ext_id` 'p1'. Must error, not silently insert a duplicate.
    let tx = session.tx().await?;
    let result = tx
        .execute(
            "UNWIND [{e: 'p2', n: 'B'}, {e: 'p1', n: 'C'}] AS row \
             CREATE (:Person {ext_id: row.e, name: row.n})",
        )
        .await;
    assert!(
        result.is_err(),
        "batch create reusing a flushed ext_id must error: {result:?}"
    );

    drop(db);
    Ok(())
}
