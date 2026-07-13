// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-bulk/src/bulk.rs:1116 (finding [4], Low).
//
// commit() gates ALL user-index building behind
//   `if self.config.defer_vector_indexes || self.config.defer_scalar_indexes`.
// The only call that materializes user-declared indexes
// (`rebuild_indexes_for_label`) lives inside that block. When BOTH defer flags
// are false the block is skipped, and the flush path only calls
// `ensure_default_indexes` (which builds ONLY the fixed _vid/_uid/ext_id
// BTree indexes) — so user-declared indexes are silently never built by
// either commit or flush. The default config (both flags true) keeps the
// common path safe, making this a config-triggered footgun.

use anyhow::Result;
use std::collections::HashMap;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};
use uni_store::backend::table_names::vertex_table_name;

async fn setup_doc_db() -> Result<(Uni, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let db = Uni::open(temp_dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Doc")
        .property("name", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::BTree))
        .done()
        .apply()
        .await?;
    Ok((db, temp_dir))
}

async fn bulk_load_docs(db: &Uni, defer: bool) -> Result<(u64, Vec<String>)> {
    let tx = db.session().tx().await?;
    let mut bulk = tx
        .bulk_writer()
        .defer_vector_indexes(defer)
        .defer_scalar_indexes(defer)
        .build()?;
    let mut props = Vec::new();
    for i in 0..50 {
        let mut p: HashMap<String, Value> = HashMap::new();
        p.insert("name".to_string(), Value::String(format!("doc_{i}")));
        props.push(p);
    }
    bulk.insert_vertices("Doc", props).await?;
    let stats = bulk.commit().await?;
    drop(tx);

    let idx_names: Vec<String> = db
        .storage()
        .backend()
        .list_indexes(&vertex_table_name("Doc"))
        .await?
        .into_iter()
        .map(|i| i.name)
        .collect();
    Ok((stats.indexes_rebuilt as u64, idx_names))
}

/// With BOTH defer flags = false, commit() skips the index-rebuild block
/// entirely: `indexes_rebuilt == 0`, and no user index is built.
#[tokio::test]
async fn bulk_both_defer_false_skips_user_index_build() -> Result<()> {
    let (db, _temp) = setup_doc_db().await?;
    let (rebuilt, idx_names) = bulk_load_docs(&db, false).await?;
    eprintln!("[both-defer-false] indexes_rebuilt={rebuilt} physical_indexes={idx_names:?}");

    // FIXED (bulk.rs): user indexes are built at commit regardless of the defer
    // flags (they are built nowhere else), so the Doc.name scalar index is
    // (re)built even with both flags false.
    assert!(
        rebuilt >= 1,
        "bulk.rs:1116 — with both defer flags false, commit() must still build \
         the touched label's user indexes (expected >= 1), got {rebuilt}"
    );
    let _ = idx_names;
    Ok(())
}

/// Control: the DEFAULT config (both defer flags = true) DOES rebuild the
/// touched label's indexes at commit — proving the flags, not the data, cause
/// the skip.
#[tokio::test]
async fn bulk_default_defer_rebuilds_index_control() -> Result<()> {
    let (db, _temp) = setup_doc_db().await?;
    let (rebuilt, idx_names) = bulk_load_docs(&db, true).await?;
    eprintln!("[default-defer-true] indexes_rebuilt={rebuilt} physical_indexes={idx_names:?}");

    assert!(
        rebuilt >= 1,
        "control failed — default deferred config should rebuild the touched \
         label's indexes at commit (expected >= 1), got {rebuilt}"
    );
    Ok(())
}
