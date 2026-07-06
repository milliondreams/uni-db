// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-bulk/src/bulk.rs:580 (finding [1], High).
//
// UNIQUE validation in the bulk path (`validate_vertex_batch_constraints`)
// only consults keys seen *within this BulkWriter's lifetime*
// (`seen_unique_keys`, initialized empty at build time) plus intra-batch
// dedup. It never reads committed storage / L0. So bulk-loading a UNIQUE key
// that already exists as a committed row in the DB silently passes validation
// and creates a duplicate — contradicting the builder doc claim that it
// "matches the behavior of regular Writer" (which DOES check L0 + storage).

use anyhow::Result;
use std::collections::HashMap;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType};
use uni_db::{DataType, Uni, Value};

async fn setup_db_with_unique_email() -> Result<(Uni, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let db = Uni::open(temp_dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("User")
        .property("email", DataType::String)
        .done()
        .apply()
        .await?;
    // Declare the UNIQUE constraint on User.email through the shared schema
    // manager (the same Arc the BulkWriter reads via BulkBackend.schema).
    db.schema_manager().add_constraint(Constraint {
        name: "User_email_unique".to_string(),
        constraint_type: ConstraintType::Unique {
            properties: vec!["email".to_string()],
        },
        target: ConstraintTarget::Label("User".to_string()),
        enabled: true,
    })?;
    Ok((db, temp_dir))
}

/// A pre-existing committed `User {email:'a@x.com'}` (flushed to storage) must
/// be seen by the bulk UNIQUE check, which now probes committed storage.
///
/// NOTE: the row is flushed to Lance below. A committed-but-still-in-L0 row on
/// the main write channel remains invisible to the bulk path — the bulk writer
/// holds a StorageManager (Lance) but not the main Writer's L0/constraint index.
/// Closing that cross-channel window is the larger "unified constraint surface"
/// refactor the triage flags (R5); this fix closes the storage-visibility gap.
#[tokio::test]
async fn bulk_unique_ignores_preexisting_committed_row() -> Result<()> {
    let (db, _temp) = setup_db_with_unique_email().await?;

    // 1) Insert + COMMIT a User via the regular write path, then flush to
    //    storage (the realistic "bulk-load onto existing data" scenario).
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:User {email: 'a@x.com'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Sanity: exactly one User exists.
    let pre = db
        .session()
        .query("MATCH (u:User {email: 'a@x.com'}) RETURN count(u) AS c")
        .await?;
    assert_eq!(pre.rows()[0].get::<i64>("c")?, 1, "setup should have 1 User");

    // 2) Fresh BulkWriter (default validate_constraints = true) inserts the
    //    SAME email. A correct impl would reject this as a UNIQUE violation.
    let tx2 = db.session().tx().await?;
    let mut bulk = tx2.bulk_writer().build()?;
    let mut props: HashMap<String, Value> = HashMap::new();
    props.insert("email".to_string(), Value::String("a@x.com".to_string()));

    let insert_res = bulk.insert_vertices("User", vec![props]).await;
    // FIXED (bulk.rs): the bulk UNIQUE check now consults committed storage, so
    // the duplicate email is rejected (like the single-vertex Writer path).
    assert!(
        insert_res.is_err(),
        "bulk insert_vertices must reject a UNIQUE key already committed; got {insert_res:?}"
    );
    drop(tx2);

    // 3) The pre-existing row is intact and no duplicate was created.
    let post = db
        .session()
        .query("MATCH (u:User {email: 'a@x.com'}) RETURN count(u) AS c")
        .await?;
    let cnt = post.rows()[0].get::<i64>("c")?;
    assert_eq!(
        cnt, 1,
        "bulk.rs — UNIQUE must be enforced against committed rows (expected 1 User), got {cnt}"
    );
    Ok(())
}
