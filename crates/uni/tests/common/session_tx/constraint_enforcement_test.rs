// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType};
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_unique_constraint_enforcement() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Define label with UNIQUE constraint
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (email STRING UNIQUE, name STRING)")
        .await?;
    tx.commit().await?;

    // Insert first user
    let tx = db.session().tx().await?;
    tx.execute("CREATE (u:User {email: 'alice@example.com', name: 'Alice'})")
        .await?;
    tx.commit().await?;

    // Insert second user with DIFFERENT email -> Should succeed
    let tx = db.session().tx().await?;
    tx.execute("CREATE (u:User {email: 'bob@example.com', name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // Insert third user with DUPLICATE email -> Should fail
    let tx = db.session().tx().await?;
    let result = tx
        .execute("CREATE (u:User {email: 'alice@example.com', name: 'Alice2'})")
        .await;

    assert!(result.is_err(), "Duplicate email insert should have failed");
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("constraint"),
        "Error should mention constraint violation: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_not_null_constraint_enforcement() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Define label with NOT NULL constraint
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Product (id STRING, price FLOAT NOT NULL)")
        .await?;
    tx.commit().await?;

    // Insert valid product
    let tx = db.session().tx().await?;
    tx.execute("CREATE (p:Product {id: 'p1', price: 10.0})")
        .await?;
    tx.commit().await?;

    // Insert product with MISSING price -> Should fail
    let tx = db.session().tx().await?;
    let result = tx.execute("CREATE (p:Product {id: 'p2'})").await;

    assert!(
        result.is_err(),
        "Insert with missing NOT NULL property should have failed"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("null"),
        "Error should mention null/missing property: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_check_constraint_enforcement() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Define label
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Adult (age INT, name STRING)")
        .await?;
    // Add CHECK constraint: age > 18
    tx.execute("CREATE CONSTRAINT age_check ON (a:Adult) ASSERT a.age > 18")
        .await?;
    tx.commit().await?;

    // Insert valid adult (age 25 > 18)
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Adult {name: 'Alice', age: 25})")
        .await?;
    tx.commit().await?;

    // Insert invalid adult (age 10 < 18) -> Should fail
    let tx = db.session().tx().await?;
    let result = tx.execute("CREATE (a:Adult {name: 'Bob', age: 10})").await;

    assert!(
        result.is_err(),
        "Insert violating CHECK constraint should have failed"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("CHECK constraint"),
        "Error should mention CHECK constraint violation: {}",
        err
    );

    // Insert edge case (age 18 not > 18) -> Should fail
    let tx = db.session().tx().await?;
    let result_boundary = tx
        .execute("CREATE (a:Adult {name: 'Charlie', age: 18})")
        .await;
    assert!(
        result_boundary.is_err(),
        "Insert violating CHECK constraint (boundary) should have failed"
    );

    Ok(())
}

/// Regression (D5 mirror, writer.rs cross-type Int/Float arm) — FIXED. The
/// single-writer CHECK comparator used to cast `i64 as f64`, so an integer just
/// above 2^53 failed a `> <float>` bound it truly satisfies. It now compares
/// exactly, so the valid row is accepted. (Passing also proves the integer
/// literal survived Cypher parsing without f64 rounding.)
#[tokio::test]
async fn repro_check_constraint_int_float_precision_collapse() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("BigNum")
        .property("val", DataType::Int64)
        .done()
        .apply()
        .await?;
    // Programmatic CHECK (writer's string-expression path): float bound 2^53; the
    // int 2^53+1 is strictly greater and must satisfy `>`.
    db.schema_manager().add_constraint(Constraint {
        name: "BigNum_val_check".to_string(),
        constraint_type: ConstraintType::Check {
            expression: "(n.val > 9007199254740992.0)".to_string(),
        },
        target: ConstraintTarget::Label("BigNum".to_string()),
        enabled: true,
    })?;

    let tx = db.session().tx().await?;
    let result = tx
        .execute("CREATE (b:BigNum {val: 9007199254740993})")
        .await;
    // 9007199254740993 > 9007199254740992.0 is true; exact comparison accepts it.
    assert!(
        result.is_ok(),
        "a valid `>` bound must be accepted after the exact fix; got {result:?}"
    );

    Ok(())
}

/// Declares a composite NODE KEY on `(tenant, email)` for label `User`.
async fn db_with_node_key() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("User")
        .property("tenant", DataType::String)
        .property("email", DataType::String)
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema_manager().add_constraint(Constraint {
        name: "User_pk".to_string(),
        constraint_type: ConstraintType::NodeKey {
            properties: vec!["tenant".to_string(), "email".to_string()],
        },
        target: ConstraintTarget::Label("User".to_string()),
        enabled: true,
    })?;
    Ok(db)
}

/// A NODE KEY enforces composite uniqueness: the same `(tenant, email)` tuple is
/// rejected, but a differing component is accepted.
#[tokio::test]
async fn node_key_enforces_composite_uniqueness() -> Result<()> {
    let db = db_with_node_key().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:User {tenant: 'a', email: 'x@e.com', name: 'A'})")
        .await?;
    tx.commit().await?;

    // Same email under a different tenant → allowed (composite, not per-column).
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:User {tenant: 'b', email: 'x@e.com', name: 'B'})")
        .await?;
    tx.commit().await?;

    // Same (tenant, email) tuple → rejected.
    let tx = db.session().tx().await?;
    let result = tx
        .execute("CREATE (:User {tenant: 'a', email: 'x@e.com', name: 'A2'})")
        .await;
    assert!(
        result.is_err(),
        "duplicate composite node key should be rejected"
    );
    let err = result.unwrap_err().to_string().to_lowercase();
    assert!(
        err.contains("key") || err.contains("constraint"),
        "error should reference the constraint/key: {err}"
    );
    Ok(())
}

/// A NODE KEY requires every key property to be present and non-null — the
/// distinguishing behavior versus UNIQUE (which skips a row missing a key part).
#[tokio::test]
async fn node_key_requires_all_key_properties_non_null() -> Result<()> {
    let db = db_with_node_key().await?;

    // Missing `email` → violation (UNIQUE would have silently accepted this).
    let tx = db.session().tx().await?;
    let result = tx
        .execute("CREATE (:User {tenant: 'a', name: 'NoEmail'})")
        .await;
    assert!(
        result.is_err(),
        "a node with a missing key property must be rejected"
    );
    // A missing key property is rejected — either by the declared-property NOT
    // NULL layer ("cannot be null") or, for a nullable/schemaless key, by the
    // NodeKey arm ("node key … must exist and be non-null"). Both are correct.
    let err = result.unwrap_err().to_string().to_lowercase();
    assert!(
        err.contains("node key")
            || err.contains("non-null")
            || err.contains("must exist")
            || err.contains("cannot be null")
            || err.contains("null"),
        "error should explain the missing key property: {err}"
    );
    Ok(())
}

/// The batch write path (UNWIND … CREATE) must also enforce NODE KEY — guards the
/// `_ => {}` wildcard in `validate_vertex_batch_constraints`.
#[tokio::test]
async fn node_key_enforced_on_batch_insert() -> Result<()> {
    let db = db_with_node_key().await?;

    // Two rows with the same composite key inside one UNWIND batch.
    let tx = db.session().tx().await?;
    let result = tx
        .execute(
            "UNWIND [{t: 'a', e: 'dup@e.com'}, {t: 'a', e: 'dup@e.com'}] AS r \
             CREATE (:User {tenant: r.t, email: r.e})",
        )
        .await;
    assert!(
        result.is_err(),
        "duplicate node key within a batch insert must be rejected"
    );
    Ok(())
}

/// The Cypher DDL path `... IS NODE KEY` maps to a real enforced NodeKey (not a
/// plain Unique) — exercises grammar → AST → `execute_create_constraint`.
#[tokio::test]
async fn node_key_via_cypher_ddl_is_enforced() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Account (tenant STRING, email STRING, name STRING)")
        .await?;
    tx.execute("CREATE CONSTRAINT acct_pk ON (a:Account) ASSERT (tenant, email) IS NODE KEY")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Account {tenant: 'a', email: 'x@e.com', name: 'A'})")
        .await?;
    tx.commit().await?;

    // Missing key property → rejected (proves NOT-NULL half, i.e. it is NodeKey
    // and not a plain Unique that would accept a missing key).
    let tx = db.session().tx().await?;
    let missing = tx
        .execute("CREATE (:Account {tenant: 'a', name: 'B'})")
        .await;
    assert!(
        missing.is_err(),
        "Cypher-declared NODE KEY must enforce NOT NULL on key properties"
    );

    // SHOW CONSTRAINTS renders the NODE KEY type.
    let rows = db.session().query("SHOW CONSTRAINTS").await?;
    let has_node_key = rows.iter().any(|r| {
        r.get::<String>("type")
            .map(|t| t == "NODE KEY")
            .unwrap_or(false)
    });
    assert!(
        has_node_key,
        "SHOW CONSTRAINTS should list the NODE KEY type"
    );
    Ok(())
}
