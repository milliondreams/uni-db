// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for the `strict_schema` configuration flag.

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;

/// Creates an in-memory database with strict_schema enabled and a Person/KNOWS schema.
async fn strict_db() -> Result<Uni> {
    let config = UniConfig {
        strict_schema: true,
        ..UniConfig::default()
    };

    let db = Uni::in_memory().config(config).build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .done()
        .apply()
        .await?;

    Ok(db)
}

/// Creates an in-memory database with default config (schemaless).
async fn schemaless_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .done()
        .apply()
        .await?;

    Ok(db)
}

// ── Default mode (schemaless) ───────────────────────────────────────────────

#[tokio::test]
async fn default_mode_accepts_unknown_label() -> Result<()> {
    let db = schemaless_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    // Animal is not in the schema — should succeed in schemaless mode.
    tx.execute("CREATE (:Animal {species: 'Cat'})").await?;
    tx.commit().await?;

    let res = session.query("MATCH (a:Animal) RETURN a.species").await?;
    assert_eq!(res.len(), 1);

    Ok(())
}

#[tokio::test]
async fn default_mode_accepts_unknown_edge_type() -> Result<()> {
    let db = schemaless_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    tx.execute("CREATE (a:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (b:Person {name: 'Bob'})").await?;
    // LIKES is not in the schema — should succeed in schemaless mode.
    tx.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:LIKES]->(b)",
    )
    .await?;
    tx.commit().await?;

    Ok(())
}

// ── Strict mode: rejections ─────────────────────────────────────────────────

#[tokio::test]
async fn strict_rejects_unknown_label_on_create() -> Result<()> {
    let db = strict_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let err = tx
        .execute("CREATE (:Animal {species: 'Cat'})")
        .await
        .unwrap_err();

    let msg = format!("{err}");
    assert!(
        msg.contains("Animal") && msg.contains("strict_schema"),
        "Error should mention the label and strict_schema: {msg}"
    );

    Ok(())
}

#[tokio::test]
async fn strict_rejects_unknown_edge_type_on_create() -> Result<()> {
    let db = strict_db().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;

    let err = tx
        .execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             CREATE (a)-[:LIKES]->(b)",
        )
        .await
        .unwrap_err();

    let msg = format!("{err}");
    assert!(
        msg.contains("LIKES") && msg.contains("strict_schema"),
        "Error should mention the edge type and strict_schema: {msg}"
    );

    Ok(())
}

#[tokio::test]
async fn strict_rejects_unknown_label_on_merge() -> Result<()> {
    let db = strict_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let err = tx
        .execute("MERGE (:Company {name: 'Acme'})")
        .await
        .unwrap_err();

    let msg = format!("{err}");
    assert!(
        msg.contains("Company") && msg.contains("strict_schema"),
        "Error should mention the label and strict_schema: {msg}"
    );

    Ok(())
}

#[tokio::test]
async fn strict_rejects_unknown_edge_type_on_merge() -> Result<()> {
    let db = strict_db().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;

    let tx = session.tx().await?;
    let err = tx
        .execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
             MERGE (a)-[:LIKES]->(b)",
        )
        .await
        .unwrap_err();

    let msg = format!("{err}");
    assert!(
        msg.contains("LIKES") && msg.contains("strict_schema"),
        "Error should mention the edge type and strict_schema: {msg}"
    );

    Ok(())
}

// ── Strict mode: acceptances ────────────────────────────────────────────────

#[tokio::test]
async fn strict_accepts_declared_label() -> Result<()> {
    let db = strict_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    // Person is declared in the schema — should succeed.
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;

    let res = session.query("MATCH (p:Person) RETURN p.name").await?;
    assert_eq!(res.len(), 1);

    Ok(())
}

#[tokio::test]
async fn strict_accepts_declared_edge_type() -> Result<()> {
    let db = strict_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    // KNOWS is declared in the schema — should succeed.
    tx.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;
    tx.commit().await?;

    let res = session
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .await?;
    assert_eq!(res.len(), 1);

    Ok(())
}

#[tokio::test]
async fn strict_allows_unknown_properties() -> Result<()> {
    let db = strict_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    // "nickname" is not in the Person schema — but strict_schema only gates
    // labels and edge types, not properties.
    tx.execute("CREATE (:Person {name: 'Alice', nickname: 'Al'})")
        .await?;
    tx.commit().await?;

    let res = session.query("MATCH (p:Person) RETURN p.nickname").await?;
    assert_eq!(res.len(), 1);

    Ok(())
}

#[tokio::test]
async fn strict_error_message_is_actionable() -> Result<()> {
    let db = strict_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let err = tx.execute("CREATE (:Widget {size: 42})").await.unwrap_err();

    let msg = format!("{err}");
    assert!(
        msg.contains("db.schema()"),
        "Error should suggest using db.schema() to declare the label: {msg}"
    );

    Ok(())
}
