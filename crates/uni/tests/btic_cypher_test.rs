// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end integration tests for BTIC temporal columns via Cypher.
//! Covers Issues #31-#34 on rustic-ai/uni-db.

use std::collections::HashMap;

use anyhow::Result;
use uni_db::common::TemporalValue;
use uni_db::{DataType, Uni, Value};

fn btic_year_1985() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: 473_385_600_000,         // 1985-01-01T00:00:00Z
        hi: 504_921_600_000,         // 1986-01-01T00:00:00Z
        meta: 0x7700_0000_0000_0000, // year/year, definite/definite
    })
}

fn btic_ongoing_2024() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: 1_705_276_800_000, // 2024-01-15T00:00:00Z
        hi: i64::MAX,
        meta: 0,
    })
}

// ---------------------------------------------------------------------------
// Issue #32: Native BTIC columns can be read via Cypher MATCH/RETURN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_match_return() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticRead")
        .property("name", DataType::String)
        .property("valid_at", DataType::Btic)
        .apply()
        .await?;

    // Write via bulk_insert (known working path)
    let mut props = HashMap::new();
    props.insert("name".into(), Value::String("fact1".into()));
    props.insert("valid_at".into(), btic_year_1985());

    let s = db.session();
    let tx = s.tx().await?;
    tx.bulk_insert_vertices("BticRead", vec![props]).await?;
    tx.commit().await?;

    // Read back via Cypher
    let result = db
        .session()
        .query("MATCH (n:BticRead) WHERE n.name = 'fact1' RETURN n.valid_at AS va")
        .await?;
    assert_eq!(result.len(), 1);
    let va = result.rows()[0].value("va").expect("va column missing");

    match va {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(*lo, 473_385_600_000);
            assert_eq!(*hi, 504_921_600_000);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Issue #33: Cypher SET can write to native BTIC columns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticSet")
        .property("name", DataType::String)
        .property_nullable("valid_at", DataType::Btic)
        .apply()
        .await?;

    // Create node without BTIC
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticSet {name: 'test'})").await?;
    tx.commit().await?;

    // SET BTIC via parameter
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute_with("MATCH (n:BticSet) WHERE n.name = 'test' SET n.valid_at = $btic")
        .param("btic", btic_year_1985())
        .run()
        .await?;
    tx.commit().await?;

    // Read back
    let result = db
        .session()
        .query("MATCH (n:BticSet) WHERE n.name = 'test' RETURN n.valid_at AS va")
        .await?;
    assert_eq!(result.len(), 1);
    let va = result.rows()[0].value("va").expect("va column missing");

    match va {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(*lo, 473_385_600_000);
            assert_eq!(*hi, 504_921_600_000);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Issue #34: CypherValue column preserves BTIC type on round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_cyphervalue_roundtrip() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticCv")
        .property("name", DataType::String)
        .property("valid_at", DataType::CypherValue)
        .apply()
        .await?;

    let mut props = HashMap::new();
    props.insert("name".into(), Value::String("fact1".into()));
    props.insert("valid_at".into(), btic_ongoing_2024());

    let s = db.session();
    let tx = s.tx().await?;
    tx.bulk_insert_vertices("BticCv", vec![props]).await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:BticCv) WHERE n.name = 'fact1' RETURN n.valid_at AS va")
        .await?;
    assert_eq!(result.len(), 1);
    let va = result.rows()[0].value("va").expect("va column missing");

    match va {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(*lo, 1_705_276_800_000);
            assert_eq!(*hi, i64::MAX);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// CREATE with BTIC parameter + MATCH/RETURN round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_create_with_param() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticCreate")
        .property("name", DataType::String)
        .property("valid_at", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute_with("CREATE (n:BticCreate {name: $name, valid_at: $va})")
        .param("name", Value::String("created".into()))
        .param("va", btic_year_1985())
        .run()
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:BticCreate) RETURN n.valid_at AS va")
        .await?;
    assert_eq!(result.len(), 1);

    match result.rows()[0].value("va").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(*lo, 473_385_600_000);
            assert_eq!(*hi, 504_921_600_000);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// BTIC UDFs in RETURN — end-to-end via Cypher
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_udf_in_return() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticUdf")
        .property("name", DataType::String)
        .property("valid_at", DataType::Btic)
        .apply()
        .await?;

    let mut props = HashMap::new();
    props.insert("name".into(), Value::String("udf_test".into()));
    props.insert("valid_at".into(), btic_year_1985());

    let s = db.session();
    let tx = s.tx().await?;
    tx.bulk_insert_vertices("BticUdf", vec![props]).await?;
    tx.commit().await?;

    // btic_duration should return milliseconds between lo and hi
    let result = db
        .session()
        .query(
            "MATCH (n:BticUdf) WHERE n.name = 'udf_test' RETURN btic_duration(n.valid_at) AS dur",
        )
        .await?;
    assert_eq!(result.len(), 1);
    let dur = result.rows()[0].get::<i64>("dur")?;
    // 1985 has 365 days = 365 * 86400 * 1000 ms
    assert_eq!(dur, 365 * 86_400 * 1000);

    Ok(())
}

// ---------------------------------------------------------------------------
// BTIC UDFs in WHERE — end-to-end via Cypher
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_udf_in_where() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticWhere")
        .property("name", DataType::String)
        .property("valid_at", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;

    let mut p1 = HashMap::new();
    p1.insert("name".into(), Value::String("a".into()));
    p1.insert("valid_at".into(), btic_year_1985());

    let mut p2 = HashMap::new();
    p2.insert("name".into(), Value::String("b".into()));
    p2.insert("valid_at".into(), btic_ongoing_2024());

    tx.bulk_insert_vertices("BticWhere", vec![p1, p2]).await?;
    tx.commit().await?;

    // Point in 1985 should match only "a"
    let result = db
        .session()
        .query_with(
            "MATCH (n:BticWhere) WHERE btic_contains_point(n.valid_at, $ts) RETURN n.name AS name",
        )
        .param("ts", Value::Int(486_000_000_000)) // mid-1985
        .fetch_all()
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name")?, "a");

    Ok(())
}

// ---------------------------------------------------------------------------
// MERGE ON MATCH SET for BTIC column
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_merge_on_match_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticMerge")
        .property("name", DataType::String)
        .property_nullable("valid_at", DataType::Btic)
        .apply()
        .await?;

    // Create initial node
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticMerge {name: 'merge_test'})")
        .await?;
    tx.commit().await?;

    // MERGE ON MATCH SET the BTIC property
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute_with("MERGE (n:BticMerge {name: 'merge_test'}) ON MATCH SET n.valid_at = $btic")
        .param("btic", btic_year_1985())
        .run()
        .await?;
    tx.commit().await?;

    // Read back
    let result = db
        .session()
        .query("MATCH (n:BticMerge) WHERE n.name = 'merge_test' RETURN n.valid_at AS va")
        .await?;
    assert_eq!(result.len(), 1);

    match result.rows()[0].value("va").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(*lo, 473_385_600_000);
            assert_eq!(*hi, 504_921_600_000);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Issue #31: `end` as a variable name
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_end_as_variable_name() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("N")
        .property("name", DataType::String)
        .edge_type("LINK", &["N"], &["N"])
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (a:N {name:'a'})-[:LINK]->(b:N {name:'b'})")
        .await?;
    tx.commit().await?;

    // "end" as variable name should work
    let result = db
        .session()
        .query("MATCH (a:N)-[:LINK]->(end:N) WHERE a.name = 'a' RETURN end.name AS name")
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name")?, "b");

    Ok(())
}

// ---------------------------------------------------------------------------
// Regression: CASE ... END still works
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_case_expression_still_works() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("CaseTest")
        .property("val", DataType::Int64)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:CaseTest {val: 1})").await?;
    tx.execute("CREATE (n:CaseTest {val: 2})").await?;
    tx.commit().await?;

    let result = db
        .session()
        .query(
            "MATCH (n:CaseTest) RETURN CASE WHEN n.val = 1 THEN 'one' ELSE 'other' END AS label ORDER BY n.val",
        )
        .await?;
    assert_eq!(result.len(), 2);
    assert_eq!(result.rows()[0].get::<String>("label")?, "one");
    assert_eq!(result.rows()[1].get::<String>("label")?, "other");

    Ok(())
}

// ---------------------------------------------------------------------------
// `end` as variable + CASE expression in same query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_end_in_case_expression_context() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("M")
        .property("val", DataType::Int64)
        .edge_type("REL", &["M"], &["M"])
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (a:M {val: 1})-[:REL]->(b:M {val: 2})")
        .await?;
    tx.commit().await?;

    // Use `end` as variable AND CASE...END in the same query
    let result = db
        .session()
        .query(
            "MATCH (start:M)-[:REL]->(end:M) \
             RETURN CASE WHEN end.val = 2 THEN 'found' ELSE 'miss' END AS status",
        )
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("status")?, "found");

    Ok(())
}

// ---------------------------------------------------------------------------
// btic() constructor function
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_constructor_in_return() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let result = db.session().query("RETURN btic('1985') AS b").await?;
    assert_eq!(result.len(), 1);

    match result.rows()[0].value("b").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(*lo, 473_385_600_000);
            assert_eq!(*hi, 504_921_600_000);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_btic_constructor_with_range() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let result = db
        .session()
        .query("RETURN btic('1985-03/2024-06') AS b")
        .await?;
    assert_eq!(result.len(), 1);

    match result.rows()[0].value("b").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            // 1985-03-01 and 2024-07-01 (month granularity on hi rounds up)
            assert!(*lo > 0);
            assert!(*hi > *lo);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_btic_constructor_unbounded() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let result = db.session().query("RETURN btic('2020-03/') AS b").await?;
    assert_eq!(result.len(), 1);

    match result.rows()[0].value("b").unwrap() {
        Value::Temporal(TemporalValue::Btic { hi, .. }) => {
            assert_eq!(*hi, i64::MAX);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_btic_constructor_null() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let result = db.session().query("RETURN btic(null) AS b").await?;
    assert_eq!(result.len(), 1);
    assert!(matches!(
        result.rows()[0].value("b"),
        Some(Value::Null) | None
    ));

    Ok(())
}

#[tokio::test]
async fn test_btic_constructor_in_create() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticCtor")
        .property("name", DataType::String)
        .property("period", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticCtor {name: 'cold_war', period: btic('1947/1991')})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:BticCtor) RETURN n.period AS p")
        .await?;
    assert_eq!(result.len(), 1);

    match result.rows()[0].value("p").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            // 1947 is before epoch, so lo is negative
            assert_eq!(*lo, -725_846_400_000); // 1947-01-01
            assert!(*hi > *lo);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_btic_set_with_constructor() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticSetCtor")
        .property("name", DataType::String)
        .property_nullable("period", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticSetCtor {name: 'test'})").await?;
    tx.commit().await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("MATCH (n:BticSetCtor) WHERE n.name = 'test' SET n.period = btic('1985')")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:BticSetCtor) WHERE n.name = 'test' RETURN n.period AS p")
        .await?;
    assert_eq!(result.len(), 1);

    match result.rows()[0].value("p").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(*lo, 473_385_600_000);
            assert_eq!(*hi, 504_921_600_000);
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// ORDER BY, DISTINCT, GROUP BY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_order_by() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticOrd")
        .property("name", DataType::String)
        .property("period", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;

    let mut p1 = HashMap::new();
    p1.insert("name".into(), Value::String("c".into()));
    p1.insert("period".into(), btic_year_1985());

    let mut p2 = HashMap::new();
    p2.insert("name".into(), Value::String("a".into()));
    p2.insert("period".into(), btic_ongoing_2024());

    // Insert out of order
    tx.bulk_insert_vertices("BticOrd", vec![p2, p1]).await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:BticOrd) RETURN n.name AS name ORDER BY n.period")
        .await?;
    assert_eq!(result.len(), 2);
    // 1985 < 2024, so "c" (1985) should come first
    assert_eq!(result.rows()[0].get::<String>("name")?, "c");
    assert_eq!(result.rows()[1].get::<String>("name")?, "a");

    Ok(())
}

#[tokio::test]
async fn test_btic_distinct() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticDist")
        .property("period", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;

    // Insert two identical BTIC values
    let mut p1 = HashMap::new();
    p1.insert("period".into(), btic_year_1985());
    let mut p2 = HashMap::new();
    p2.insert("period".into(), btic_year_1985());
    let mut p3 = HashMap::new();
    p3.insert("period".into(), btic_ongoing_2024());

    tx.bulk_insert_vertices("BticDist", vec![p1, p2, p3])
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:BticDist) RETURN DISTINCT n.period AS p ORDER BY p")
        .await?;
    assert_eq!(result.len(), 2); // two distinct values, not three

    Ok(())
}

// ---------------------------------------------------------------------------
// Aggregation UDFs end-to-end
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_aggregation_udfs() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticAgg")
        .property("name", DataType::String)
        .property("period", DataType::CypherValue)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticAgg {name: 'a', period: btic('1985')})")
        .await?;
    tx.execute("CREATE (n:BticAgg {name: 'b', period: btic('2024')})")
        .await?;
    tx.commit().await?;

    // btic_min should return the earliest (1985)
    let result = db
        .session()
        .query("MATCH (n:BticAgg) RETURN btic_min(n.period) AS earliest")
        .await?;
    assert_eq!(result.len(), 1);
    match result.rows()[0].value("earliest").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, .. }) => {
            assert_eq!(*lo, 473_385_600_000); // 1985
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    // btic_max should return the latest (2024)
    let result = db
        .session()
        .query("MATCH (n:BticAgg) RETURN btic_max(n.period) AS latest")
        .await?;
    assert_eq!(result.len(), 1);
    match result.rows()[0].value("latest").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, .. }) => {
            assert_eq!(*lo, 1_704_067_200_000); // 2024-01-01
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    // btic_span_agg should return the bounding interval [1985, 2025)
    let result = db
        .session()
        .query("MATCH (n:BticAgg) RETURN btic_span_agg(n.period) AS span")
        .await?;
    assert_eq!(result.len(), 1);
    match result.rows()[0].value("span").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(*lo, 473_385_600_000); // 1985-01-01
            assert_eq!(*hi, 1_735_689_600_000); // 2025-01-01
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    // btic_count_at: mid-1985 point should be contained in only 1 interval
    let result = db
        .session()
        .query("MATCH (n:BticAgg) RETURN btic_count_at(n.period, 489024000000) AS cnt")
        .await?;
    assert_eq!(result.len(), 1);
    match result.rows()[0].value("cnt").unwrap() {
        Value::Int(cnt) => assert_eq!(*cnt, 1),
        other => panic!("expected Int, got: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Set operations in queries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_set_ops_in_query() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // btic_span of two disjoint intervals should cover both
    let result = db
        .session()
        .query("RETURN btic_span(btic('1985'), btic('1990')) AS span")
        .await?;
    assert_eq!(result.len(), 1);
    match result.rows()[0].value("span").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            assert_eq!(*lo, 473_385_600_000); // 1985-01-01
            // hi should be 1991-01-01
            assert!(*hi > 631_152_000_000); // 1990-01-01
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    // btic_gap between disjoint intervals
    let result = db
        .session()
        .query("RETURN btic_gap(btic('1985'), btic('1990')) AS gap")
        .await?;
    assert_eq!(result.len(), 1);
    match result.rows()[0].value("gap").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, hi, .. }) => {
            // gap should be [1986-01-01, 1990-01-01)
            assert_eq!(*lo, 504_921_600_000); // 1986-01-01
            assert_eq!(*hi, 631_152_000_000); // 1990-01-01
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// DELETE vertex with BTIC property
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_delete_vertex() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticDel")
        .property("name", DataType::String)
        .property("period", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticDel {name: 'target', period: btic('1985')})")
        .await?;
    tx.commit().await?;

    // Verify it exists
    let result = db
        .session()
        .query("MATCH (n:BticDel) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].value("cnt").unwrap(), &Value::Int(1));

    // Delete
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("MATCH (n:BticDel {name: 'target'}) DELETE n")
        .await?;
    tx.commit().await?;

    // Verify gone
    let result = db
        .session()
        .query("MATCH (n:BticDel) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].value("cnt").unwrap(), &Value::Int(0));

    Ok(())
}

// ---------------------------------------------------------------------------
// REMOVE BTIC property
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_remove_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticRem")
        .property("name", DataType::String)
        .property_nullable("period", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticRem {name: 'rm_test', period: btic('1985')})")
        .await?;
    tx.commit().await?;

    // Verify BTIC exists
    let result = db
        .session()
        .query("MATCH (n:BticRem) RETURN n.period AS p")
        .await?;
    assert!(matches!(
        result.rows()[0].value("p").unwrap(),
        Value::Temporal(TemporalValue::Btic { .. })
    ));

    // REMOVE the property
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("MATCH (n:BticRem {name: 'rm_test'}) REMOVE n.period")
        .await?;
    tx.commit().await?;

    // Verify it's null
    let result = db
        .session()
        .query("MATCH (n:BticRem {name: 'rm_test'}) RETURN n.period AS p")
        .await?;
    assert_eq!(result.rows()[0].value("p").unwrap(), &Value::Null);

    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction rollback with BTIC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_transaction_rollback() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticTx")
        .property("name", DataType::String)
        .property("period", DataType::Btic)
        .apply()
        .await?;

    // Commit one node
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticTx {name: 'committed', period: btic('1985')})")
        .await?;
    tx.commit().await?;

    // Create another in a rolled-back transaction
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticTx {name: 'rolled_back', period: btic('2024')})")
        .await?;
    tx.rollback();

    // Verify only the committed node exists
    let result = db
        .session()
        .query("MATCH (n:BticTx) RETURN n.name AS name")
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].value("name").unwrap(),
        &Value::String("committed".into())
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// BTIC survives WITH clause projection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_with_clause() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticWith")
        .property("name", DataType::String)
        .property("period", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticWith {name: 'test', period: btic('1985')})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:BticWith) WITH n.period AS p RETURN p")
        .await?;
    assert_eq!(result.len(), 1);
    match result.rows()[0].value("p").unwrap() {
        Value::Temporal(TemporalValue::Btic { lo, .. }) => {
            assert_eq!(*lo, 473_385_600_000); // 1985-01-01
        }
        other => panic!("expected Temporal(Btic), got: {other:?}"),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// SET BTIC property to null
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_btic_set_null() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("BticSetNull")
        .property("name", DataType::String)
        .property_nullable("period", DataType::Btic)
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (n:BticSetNull {name: 'test', period: btic('1985')})")
        .await?;
    tx.commit().await?;

    // SET to null
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("MATCH (n:BticSetNull {name: 'test'}) SET n.period = null")
        .await?;
    tx.commit().await?;

    // Verify null persisted
    let result = db
        .session()
        .query("MATCH (n:BticSetNull {name: 'test'}) RETURN n.period AS p")
        .await?;
    assert_eq!(result.rows()[0].value("p").unwrap(), &Value::Null);

    Ok(())
}
