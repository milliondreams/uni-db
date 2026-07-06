// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-bulk/src/bulk.rs:644 (finding [3], Medium).
//
// `compute_unique_key` builds a UNIQUE-constraint key by pushing each
// property's lossy `Value::Display` rendering and joining parts with ':'.
// Value::Bytes(b) renders as "<{len} bytes>" using ONLY the length, so two
// DISTINCT byte arrays of equal length collide to the same key and the second
// row is falsely rejected as a duplicate UNIQUE violation (over-rejection —
// a legitimate insert is refused). The colon join is also ambiguous across
// multiple properties.

use anyhow::Result;
use std::collections::HashMap;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType};
use uni_db::{DataType, Uni, Value};

/// Two DISTINCT Bytes values of the same length both render to "<3 bytes>",
/// so the second is falsely flagged as an intra-batch UNIQUE duplicate.
#[tokio::test]
async fn bulk_unique_key_bytes_length_collision_false_reject() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Uni::open(temp_dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Blob")
        .property("data", DataType::Bytes)
        .done()
        .apply()
        .await?;
    db.schema_manager().add_constraint(Constraint {
        name: "Blob_data_unique".to_string(),
        constraint_type: ConstraintType::Unique {
            properties: vec!["data".to_string()],
        },
        target: ConstraintTarget::Label("Blob".to_string()),
        enabled: true,
    })?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;

    let mut r0: HashMap<String, Value> = HashMap::new();
    r0.insert("data".to_string(), Value::Bytes(vec![1, 2, 3]));
    let mut r1: HashMap<String, Value> = HashMap::new();
    r1.insert("data".to_string(), Value::Bytes(vec![4, 5, 6])); // distinct bytes!

    let res = bulk.insert_vertices("Blob", vec![r0, r1]).await;

    // FIXED (bulk.rs): the codec-based key encodes byte content, so two distinct
    // equal-length byte arrays no longer collide — both rows are accepted.
    res.expect("distinct Bytes must not collide on the UNIQUE key");
    Ok(())
}

/// Composite UNIQUE(a,b) joined with a bare ':' is ambiguous: ("x:y","z") and
/// ("x","y:z") both render "x:y:z", so two distinct rows falsely collide.
#[tokio::test]
async fn bulk_unique_key_colon_join_ambiguity_false_reject() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Uni::open(temp_dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Pair")
        .property("a", DataType::String)
        .property("b", DataType::String)
        .done()
        .apply()
        .await?;
    db.schema_manager().add_constraint(Constraint {
        name: "Pair_ab_unique".to_string(),
        constraint_type: ConstraintType::Unique {
            properties: vec!["a".to_string(), "b".to_string()],
        },
        target: ConstraintTarget::Label("Pair".to_string()),
        enabled: true,
    })?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;

    let mut r0: HashMap<String, Value> = HashMap::new();
    r0.insert("a".to_string(), Value::String("x:y".to_string()));
    r0.insert("b".to_string(), Value::String("z".to_string()));
    let mut r1: HashMap<String, Value> = HashMap::new();
    r1.insert("a".to_string(), Value::String("x".to_string()));
    r1.insert("b".to_string(), Value::String("y:z".to_string()));

    let res = bulk.insert_vertices("Pair", vec![r0, r1]).await;

    // FIXED (bulk.rs): length-prefixed field encoding makes ("x:y","z") and
    // ("x","y:z") distinct composite keys — both rows are accepted.
    res.expect("distinct composite keys must not collide via ':' ambiguity");
    Ok(())
}
