// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression tests for issue #41: pattern predicates in WHERE clause should
//! use vectorized CSR evaluation, not per-row subquery execution.

use std::time::Instant;

use anyhow::Result;
use uni_common::Value;
use uni_common::core::schema::DataType;
use uni_db::{IndexType, ScalarType, Uni};

/// Sets up the database from the issue reproduction case.
///
/// Creates 100 Message nodes, 2 Participant nodes, 2 Entity nodes, with
/// SENT_BY and MENTIONS edges. Messages alternate between Jon and Gina.
async fn setup_issue41_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Message")
        .property("content", DataType::String)
        .index("content", IndexType::FullText)
        .done()
        .label("Entity")
        .property("name", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::Hash))
        .done()
        .label("Participant")
        .property("name", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::Hash))
        .done()
        .edge_type("MENTIONS", &["Message"], &["Entity"])
        .done()
        .edge_type("SENT_BY", &["Message"], &["Participant"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Entity {name: 'Jon'})").await?;
    tx.execute("CREATE (:Entity {name: 'Gina'})").await?;
    tx.execute("CREATE (:Participant {name: 'Jon'})").await?;
    tx.execute("CREATE (:Participant {name: 'Gina'})").await?;
    tx.commit().await?;

    for i in 0..100 {
        let speaker = if i % 2 == 0 { "Jon" } else { "Gina" };
        let other = if i % 2 == 0 { "Gina" } else { "Jon" };
        let content = format!("Message {i} from {speaker} about business and dance");
        let tx = session.tx().await?;
        tx.execute(&format!(
            "CREATE (m:Message {{content: '{content}'}}) \
             WITH m MATCH (p:Participant {{name: '{speaker}'}}) CREATE (m)-[:SENT_BY]->(p) \
             WITH m MATCH (e:Entity {{name: '{other}'}}) CREATE (m)-[:MENTIONS]->(e)"
        ))
        .await?;
        tx.commit().await?;
    }

    db.flush().await?;
    Ok(db)
}

/// Core issue: WHERE pattern predicate with OR should not be drastically slower
/// than unscoped query.
#[tokio::test]
async fn pattern_in_where_should_not_be_slow() -> Result<()> {
    let db = setup_issue41_db().await?;
    let session = db.session();

    // Warm-up
    let _ = session
        .query("MATCH (m:Message) RETURN count(m) AS c")
        .await?;

    // Q1: Unscoped
    let start = Instant::now();
    for _ in 0..3 {
        let _ = session
            .query_with(
                "MATCH (m:Message) \
                 RETURN m.content AS content, similar_to(m.content, $q) AS score \
                 ORDER BY score DESC LIMIT 15",
            )
            .param("q", Value::String("business dance".into()))
            .fetch_all()
            .await?;
    }
    let unscoped_us = start.elapsed().as_micros() / 3;

    // Q2: Entity-scoped (the bug query)
    let start = Instant::now();
    for _ in 0..3 {
        let _ = session
            .query_with(
                "MATCH (m:Message) \
                 WHERE (m)-[:SENT_BY]->(:Participant {name: $ename}) \
                    OR (m)-[:MENTIONS]->(:Entity {name: $ename}) \
                 RETURN m.content AS content, similar_to(m.content, $q) AS score \
                 ORDER BY score DESC LIMIT 15",
            )
            .param("ename", Value::String("Jon".into()))
            .param("q", Value::String("business dance".into()))
            .fetch_all()
            .await?;
    }
    let scoped_us = start.elapsed().as_micros() / 3;

    eprintln!(
        "Unscoped: {}us, Scoped: {}us, Ratio: {:.1}x",
        unscoped_us,
        scoped_us,
        scoped_us as f64 / unscoped_us.max(1) as f64
    );

    // The scoped query should be within 10x of unscoped (was 15-95x before fix).
    assert!(
        scoped_us < unscoped_us * 10,
        "Scoped ({scoped_us}us) > 10x slower than unscoped ({unscoped_us}us)"
    );

    Ok(())
}

/// Pattern predicate without property filter: `WHERE (m)-[:SENT_BY]->(:Participant)`.
#[tokio::test]
async fn pattern_exists_no_property_filter() -> Result<()> {
    let db = setup_issue41_db().await?;
    let session = db.session();

    let res = session
        .query(
            "MATCH (m:Message) \
             WHERE (m)-[:SENT_BY]->(:Participant) \
             RETURN count(m) AS c",
        )
        .await?;

    let count = res.rows()[0].get::<i64>("c")?;
    assert_eq!(count, 100, "All messages should match (all have SENT_BY)");

    Ok(())
}

/// Pattern predicate with literal property: `{name: 'Jon'}`.
#[tokio::test]
async fn pattern_exists_literal_property() -> Result<()> {
    let db = setup_issue41_db().await?;
    let session = db.session();

    let res = session
        .query(
            "MATCH (m:Message) \
             WHERE (m)-[:SENT_BY]->(:Participant {name: 'Jon'}) \
             RETURN count(m) AS c",
        )
        .await?;

    let count = res.rows()[0].get::<i64>("c")?;
    assert_eq!(count, 50, "Half the messages should be from Jon");

    Ok(())
}

/// Pattern predicate with parameter property: `{name: $ename}`.
#[tokio::test]
async fn pattern_exists_param_property() -> Result<()> {
    let db = setup_issue41_db().await?;
    let session = db.session();

    let res = session
        .query_with(
            "MATCH (m:Message) \
             WHERE (m)-[:SENT_BY]->(:Participant {name: $ename}) \
             RETURN count(m) AS c",
        )
        .param("ename", Value::String("Gina".into()))
        .fetch_all()
        .await?;

    let count = res.rows()[0].get::<i64>("c")?;
    assert_eq!(count, 50, "Half the messages should be from Gina");

    Ok(())
}

/// NOT + pattern predicate.
#[tokio::test]
async fn pattern_exists_negated() -> Result<()> {
    let db = setup_issue41_db().await?;
    let session = db.session();

    let res = session
        .query_with(
            "MATCH (m:Message) \
             WHERE NOT (m)-[:SENT_BY]->(:Participant {name: $ename}) \
             RETURN count(m) AS c",
        )
        .param("ename", Value::String("Jon".into()))
        .fetch_all()
        .await?;

    let count = res.rows()[0].get::<i64>("c")?;
    assert_eq!(count, 50, "Half the messages should NOT be from Jon");

    Ok(())
}

/// OR of two pattern predicates (the exact issue scenario).
#[tokio::test]
async fn pattern_exists_or_combination() -> Result<()> {
    let db = setup_issue41_db().await?;
    let session = db.session();

    let res = session
        .query_with(
            "MATCH (m:Message) \
             WHERE (m)-[:SENT_BY]->(:Participant {name: $ename}) \
                OR (m)-[:MENTIONS]->(:Entity {name: $ename}) \
             RETURN count(m) AS c",
        )
        .param("ename", Value::String("Jon".into()))
        .fetch_all()
        .await?;

    // Jon sends 50 messages (even) and is mentioned in 50 messages (odd) = all 100.
    let count = res.rows()[0].get::<i64>("c")?;
    assert_eq!(
        count, 100,
        "Jon sends 50 and is mentioned in 50 — all 100 should match"
    );

    Ok(())
}

/// Non-matching edge type returns empty results.
#[tokio::test]
async fn pattern_exists_no_matching_edge_type() -> Result<()> {
    let db = setup_issue41_db().await?;
    let session = db.session();

    let res = session
        .query(
            "MATCH (m:Message) \
             WHERE (m)-[:NONEXISTENT]->(:Participant) \
             RETURN count(m) AS c",
        )
        .await?;

    let count = res.rows()[0].get::<i64>("c")?;
    assert_eq!(count, 0, "Non-existent edge type should return 0 matches");

    Ok(())
}
