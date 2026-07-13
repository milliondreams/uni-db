// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end tests for RELATIONSHIP (edge-type) unique / node-key constraints
//! (deferred P2 item D2). Mirrors the vertex constraint suite for edges: the L0
//! index, the flush-safe full-horizon LSM probe, the delete-then-reuse case, and
//! the Cypher DDL path `CREATE CONSTRAINT ... ON ()-[r:TYPE]-() ASSERT ...`.

use anyhow::Result;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType};
use uni_db::{DataType, Uni};

/// Person nodes + a KNOWS edge type with a unique `rid` property.
async fn db_with_edge_unique() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("rid", DataType::String)
        .apply()
        .await?;
    db.schema_manager().add_constraint(Constraint {
        name: "knows_rid_unique".to_string(),
        constraint_type: ConstraintType::Unique {
            properties: vec!["rid".to_string()],
        },
        target: ConstraintTarget::EdgeType("KNOWS".to_string()),
        enabled: true,
    })?;
    // Two people to connect.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'a'}), (:Person {name: 'b'}), (:Person {name: 'c'})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

async fn create_edge(db: &Uni, from: &str, to: &str, rid: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    let q = format!(
        "MATCH (a:Person {{name: '{from}'}}), (b:Person {{name: '{to}'}}) \
         CREATE (a)-[:KNOWS {{rid: '{rid}'}}]->(b)"
    );
    tx.execute(&q).await?;
    tx.commit().await?;
    Ok(())
}

/// A duplicate edge `rid` is rejected; a distinct one is accepted.
#[tokio::test]
async fn edge_unique_rejects_duplicate() -> Result<()> {
    let db = db_with_edge_unique().await?;

    create_edge(&db, "a", "b", "r1").await?;
    // Different rid → allowed.
    create_edge(&db, "a", "c", "r2").await?;

    // Duplicate rid → rejected.
    let dup = create_edge(&db, "b", "c", "r1").await;
    assert!(dup.is_err(), "duplicate edge rid should be rejected");
    let err = dup.unwrap_err().to_string().to_lowercase();
    assert!(
        err.contains("constraint") || err.contains("duplicate"),
        "error should reference the constraint: {err}"
    );
    Ok(())
}

/// The uniqueness check must survive a flush — the flushed edge lives only in the
/// LSM delta table, so this exercises the full-horizon storage probe, not just L0.
#[tokio::test]
async fn edge_unique_is_flush_safe() -> Result<()> {
    let db = db_with_edge_unique().await?;

    create_edge(&db, "a", "b", "r1").await?;
    db.flush().await?;

    // After flush, the duplicate must still be rejected (probe reaches storage).
    let dup = create_edge(&db, "b", "c", "r1").await;
    assert!(
        dup.is_err(),
        "duplicate edge rid should still be rejected after flush"
    );
    Ok(())
}

/// Deleting an edge frees its unique value for reuse — the probe must not report a
/// stale conflict from the deleted edge's history (a naive `op = 0` filter would).
#[tokio::test]
async fn edge_unique_value_reusable_after_delete() -> Result<()> {
    let db = db_with_edge_unique().await?;

    create_edge(&db, "a", "b", "r1").await?;
    db.flush().await?;

    // Delete the edge, flush the tombstone.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (:Person {name: 'a'})-[r:KNOWS {rid: 'r1'}]->(:Person {name: 'b'}) DELETE r")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // The value 'r1' is now free — reusing it must succeed (no false conflict).
    create_edge(&db, "b", "c", "r1").await?;
    Ok(())
}

/// The Cypher DDL relationship-pattern path maps to an enforced EdgeType
/// constraint — exercises grammar (`ON ()-[r:TYPE]-()`) → walker → executor.
#[tokio::test]
async fn edge_unique_via_cypher_ddl_is_enforced() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("rid", DataType::String)
        .apply()
        .await?;
    // The relationship-pattern DDL form must parse and install an enforced
    // EdgeType constraint (grammar `ON ()-[r:TYPE]-()` → walker → executor).
    let tx = db.session().tx().await?;
    tx.execute("CREATE CONSTRAINT knows_pk ON ()-[r:KNOWS]-() ASSERT r.rid IS UNIQUE")
        .await?;
    tx.execute("CREATE (:Person {name: 'a'}), (:Person {name: 'b'}), (:Person {name: 'c'})")
        .await?;
    tx.commit().await?;

    create_edge(&db, "a", "b", "x1").await?;
    let dup = create_edge(&db, "b", "c", "x1").await;
    assert!(
        dup.is_err(),
        "duplicate edge rid should be rejected via the DDL-declared constraint"
    );
    Ok(())
}

/// A relationship NODE KEY requires the key property present and non-null, and
/// enforces composite uniqueness.
#[tokio::test]
async fn edge_node_key_requires_present_and_unique() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("RATED", &["Person"], &["Person"])
        .property("scope", DataType::String)
        .property("item", DataType::String)
        .apply()
        .await?;
    db.schema_manager().add_constraint(Constraint {
        name: "rated_key".to_string(),
        constraint_type: ConstraintType::NodeKey {
            properties: vec!["scope".to_string(), "item".to_string()],
        },
        target: ConstraintTarget::EdgeType("RATED".to_string()),
        enabled: true,
    })?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'a'}), (:Person {name: 'b'})")
        .await?;
    tx.commit().await?;

    // Full composite key present → ok.
    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (a:Person {name: 'a'}), (b:Person {name: 'b'}) \
         CREATE (a)-[:RATED {scope: 's', item: 'i'}]->(b)",
    )
    .await?;
    tx.commit().await?;

    // Same composite key → rejected.
    let tx = db.session().tx().await?;
    let dup = tx
        .execute(
            "MATCH (a:Person {name: 'a'}), (b:Person {name: 'b'}) \
             CREATE (a)-[:RATED {scope: 's', item: 'i'}]->(b)",
        )
        .await;
    assert!(
        dup.is_err(),
        "duplicate composite edge key should be rejected"
    );

    // Missing a key property → rejected (NOT-NULL half of node key).
    let tx = db.session().tx().await?;
    let missing = tx
        .execute(
            "MATCH (a:Person {name: 'a'}), (b:Person {name: 'b'}) \
             CREATE (a)-[:RATED {scope: 's'}]->(b)",
        )
        .await;
    assert!(
        missing.is_err(),
        "edge missing a node-key property must be rejected"
    );
    Ok(())
}
