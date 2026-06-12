// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for the transaction-write-path plan cache (Lever A).
//
// `UniInner::execute_internal_with_tx_l0` caches the pre-rewrite logical plan
// keyed by query-text hash + schema version. The critical correctness
// invariant: a cached plan is *parameter-value independent* — reused across
// batches of the same statement shape, with the new parameter values binding
// at execution time (the plan must not freeze the first call's values).
// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Regression repro for the stale read-plan cache after a schema DDL change
/// (Bug #2): `schema_version` is initialized to 1 and never incremented by any
/// `SchemaManager` mutator, so the session read-plan cache's version-guard
/// eviction is a dead branch. An untyped traversal `MATCH (a)-[]->(b)` freezes
/// `edge_type_ids = all_edge_type_ids()` at plan time, so a session that cached
/// the plan before a new edge type was added keeps undercounting after.
///
/// RED today: step 6 returns `1` (stale plan excludes the new `LIKES` edge),
/// while a fresh session correctly returns `2` — proving the staleness is in the
/// cache, not the data.
#[tokio::test]
async fn untyped_traversal_plan_is_stale_after_new_edge_type_ddl() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Schema: label `Person`, edge type `KNOWS` (Person -> Person).
    db.schema()
        .label("Person")
        .property("id", DataType::String)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .done()
        .apply()
        .await?;

    // Seed: two `:Person` joined by one `KNOWS` edge.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Person {id: 'a'})-[:KNOWS]->(b:Person {id: 'b'})")
        .await?;
    tx.commit().await?;

    // Reused session: cache the untyped-traversal plan (edge_type_ids = [KNOWS]).
    let session = db.session();
    let r1 = session
        .query("MATCH (a)-[]->(b) RETURN count(*) AS c")
        .await?;
    assert_eq!(
        r1.rows()[0].get::<i64>("c").unwrap(),
        1,
        "one KNOWS edge before the DDL change"
    );

    // DDL on the SAME db: add edge type `LIKES` (Person -> Person)...
    db.schema()
        .edge_type("LIKES", &["Person"], &["Person"])
        .done()
        .apply()
        .await?;

    // ...then create one `LIKES` edge in a fresh transaction.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Person {id: 'a'}), (b:Person {id: 'b'}) CREATE (a)-[:LIKES]->(b)")
        .await?;
    tx.commit().await?;

    // Sanity: a FRESH session re-plans and counts both edges — the data is fine.
    let fresh = db
        .session()
        .query("MATCH (a)-[]->(b) RETURN count(*) AS c")
        .await?;
    assert_eq!(
        fresh.rows()[0].get::<i64>("c").unwrap(),
        2,
        "a fresh session must count both KNOWS and LIKES — proving the data is correct"
    );

    // Re-run the SAME query text on the SAME `session`. Because `schema_version`
    // never incremented, the stale cached plan (edge_type_ids = [KNOWS]) is
    // reused and undercounts the `LIKES` edge.
    let r2 = session
        .query("MATCH (a)-[]->(b) RETURN count(*) AS c")
        .await?;
    assert_eq!(
        r2.rows()[0].get::<i64>("c").unwrap(),
        2,
        "reused session must see the new LIKES edge after the DDL change"
    );
    Ok(())
}

fn row(name: &str, age: i64) -> Value {
    let mut m = std::collections::HashMap::new();
    m.insert("name".to_string(), Value::String(name.to_string()));
    m.insert("age".to_string(), Value::Int(age));
    Value::Map(m)
}

/// Repeated parameterized writes reuse the cached plan, yet each batch's own
/// parameter values land — the plan is not frozen with the first call's values.
#[tokio::test]
async fn parameterized_write_reuses_plan_across_batches() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .done()
        .apply()
        .await?;

    let cypher = "UNWIND $rows AS r CREATE (n:Person {name: r.name, age: r.age})";

    // First batch — cache miss (parse + plan run).
    let tx = db.session().tx().await?;
    let r1 = tx
        .execute_with(cypher)
        .param("rows", Value::List(vec![row("alice", 30)]))
        .run()
        .await?;
    assert!(
        !r1.metrics().plan_cache_hit,
        "first execution should miss the plan cache"
    );
    tx.commit().await?;

    // Second batch — same query string, DIFFERENT parameter values. Must hit
    // the cache AND write the new values (not alice/30 again).
    let tx = db.session().tx().await?;
    let r2 = tx
        .execute_with(cypher)
        .param("rows", Value::List(vec![row("bob", 40)]))
        .run()
        .await?;
    assert!(
        r2.metrics().plan_cache_hit,
        "second execution of the same shape should hit the plan cache"
    );
    assert_eq!(
        r2.metrics().parse_time,
        std::time::Duration::ZERO,
        "a cache hit must skip parsing"
    );
    assert_eq!(
        r2.metrics().plan_time,
        std::time::Duration::ZERO,
        "a cache hit must skip planning"
    );
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY n.age")
        .await?;
    assert_eq!(res.rows().len(), 2, "both batches must have created a row");
    assert_eq!(res.rows()[0].get::<String>("name").unwrap(), "alice");
    assert_eq!(res.rows()[0].get::<i64>("age").unwrap(), 30);
    assert_eq!(
        res.rows()[1].get::<String>("name").unwrap(),
        "bob",
        "second batch's parameter values must land — plan is not frozen"
    );
    assert_eq!(res.rows()[1].get::<i64>("age").unwrap(), 40);
    Ok(())
}

/// Distinct query texts get distinct cache entries — no false sharing.
#[tokio::test]
async fn distinct_query_texts_do_not_collide() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    let a = tx
        .execute_with("CREATE (n:Person {name: $n})")
        .param("n", Value::String("a".to_string()))
        .run()
        .await?;
    assert!(!a.metrics().plan_cache_hit);
    let b = tx
        .execute_with("CREATE (n:Person {name: $n, age: 7})")
        .param("n", Value::String("b".to_string()))
        .run()
        .await?;
    assert!(
        !b.metrics().plan_cache_hit,
        "a different statement shape must not hit the first one's entry"
    );
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:Person) RETURN count(n) AS c")
        .await?;
    assert_eq!(res.rows()[0].get::<i64>("c").unwrap(), 2);
    Ok(())
}

/// A cached write plan survives a later schema change (a new column on the
/// same label) and still produces correct results.
///
/// Note: `add_property`/`add_label` now bump `schema_version` (in `uni-common`
/// schema.rs), so the version guard evicts the cached plan after the DDL and the
/// re-run re-plans against the live schema. Even absent that guard the result
/// would be correct: a cached *write* plan encodes only the CREATE/SET structure
/// from the query text, while constraint validation, type coercion, and index
/// maintenance all re-read the live schema at execution. This test guards that
/// a schema change between executions does not corrupt post-change writes.
#[tokio::test]
async fn cached_write_survives_schema_change() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;

    let name_only = "CREATE (n:Person {name: $n})";

    // Cache the name-only CREATE plan.
    let tx = db.session().tx().await?;
    tx.execute_with(name_only)
        .param("n", Value::String("a".to_string()))
        .run()
        .await?;
    tx.commit().await?;

    // Add a new column to the same label.
    db.schema()
        .label("Person")
        .property_nullable("age", DataType::Int64)
        .done()
        .apply()
        .await?;

    // Re-run the cached name-only plan — must still create a valid Person.
    let tx = db.session().tx().await?;
    tx.execute_with(name_only)
        .param("n", Value::String("b".to_string()))
        .run()
        .await?;
    tx.commit().await?;

    // A new statement shape that uses the new column re-plans (distinct key)
    // and writes the new column correctly.
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (n:Person {name: $n, age: $a})")
        .param("n", Value::String("c".to_string()))
        .param("a", Value::Int(25))
        .run()
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY n.name")
        .await?;
    assert_eq!(res.rows().len(), 3);
    // a and b created before/with the stale plan: name set, age null.
    assert_eq!(res.rows()[0].get::<String>("name").unwrap(), "a");
    assert_eq!(res.rows()[1].get::<String>("name").unwrap(), "b");
    // c used the new shape: age landed.
    assert_eq!(res.rows()[2].get::<String>("name").unwrap(), "c");
    assert_eq!(res.rows()[2].get::<i64>("age").unwrap(), 25);
    Ok(())
}
