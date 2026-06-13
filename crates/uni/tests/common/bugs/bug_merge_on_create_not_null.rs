// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for RC4: `MERGE`'s internal `CREATE` validates NOT NULL *before*
//! applying `ON CREATE SET`.
//!
//! A `MERGE (e:Entity {entity_id: …}) ON CREATE SET e.name = …` should succeed:
//! the `name` supplied by `ON CREATE SET` belongs to the node being created.
//! Today the create path runs `validate_vertex_constraints`
//! (`uni-store/src/runtime/writer.rs`) over the merge key alone, *before* the
//! `ON CREATE SET` assignments are folded in, so a NOT-NULL property that is not
//! in the merge key fails with `"… Property 'name' cannot be null"`.
//!
//! The test asserts the correct behavior (success), so it does not depend on the
//! exact error wording. Fixed by seeding ON CREATE SET props into the
//! MERGE-create node before constraint validation; now a regression guard.
//!
//! Run with:
//!   cargo nextest run -p uni --test integration bug_merge_on_create_not_null

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni};

/// MERGE supplies a NOT-NULL property via ON CREATE SET without a null violation.
#[tokio::test]
async fn merge_on_create_set_supplies_not_null_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // `entity_id` (the merge key) and `name` are both NOT-NULL; `name` is NOT
    // part of the merge key and is supplied only by `ON CREATE SET`.
    db.schema()
        .label("Entity")
        .property("entity_id", DataType::String)
        .property("name", DataType::String)
        .index("entity_id", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await?;

    let session = db.session();

    // First MERGE: no match -> CREATE, with ON CREATE SET providing `name`.
    let tx = session.tx().await?;
    let created = tx
        .query_with(
            "MERGE (e:Entity {entity_id: 'x'}) ON CREATE SET e.name = 'Alice' RETURN e.name AS name",
        )
        .fetch_all()
        .await?;
    let name: String = created.rows()[0].get::<String>("name")?;
    assert_eq!(
        name, "Alice",
        "ON CREATE SET must populate the NOT-NULL `name`"
    );
    tx.commit().await?;

    // Second MERGE with the same key: hits ON MATCH, stays idempotent (one node).
    let tx = session.tx().await?;
    tx.execute("MERGE (e:Entity {entity_id: 'x'}) ON CREATE SET e.name = 'Bob'")
        .await?;
    tx.commit().await?;

    let count = session
        .query_with("MATCH (e:Entity {entity_id: 'x'}) RETURN count(e) AS c")
        .fetch_all()
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(count, 1, "MERGE must be idempotent on the merge key");

    Ok(())
}
