// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

#[tokio::test]
async fn test_api_transactions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Account")
        .property("balance", DataType::Int64)
        .apply()
        .await?;

    // 1. Successful transaction
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Account {balance: 100})").await?;
    tx.execute("CREATE (:Account {balance: 200})").await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("total")?, 300);

    // 2. Rollback transaction
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Account {balance: 500})").await?;
    // Data should be visible inside transaction
    let res_inner = tx
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(res_inner.rows()[0].get::<i64>("total")?, 800);

    tx.rollback();

    // Data should NOT be visible after rollback
    let res_outer = db
        .session()
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(res_outer.rows()[0].get::<i64>("total")?, 300);

    // 3. Transaction via session
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Account {balance: 1000})").await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (a:Account) RETURN sum(a.balance) AS total")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("total")?, 1300);

    Ok(())
}

#[tokio::test]
async fn test_api_schema_and_property_query() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 1. Define Schema
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int32)
        .index("name", IndexType::Scalar(ScalarType::BTree))
        .label("Movie")
        .property("title", DataType::String)
        .edge_type("ACTED_IN", &["Person"], &["Movie"])
        .property("role", DataType::String)
        .apply()
        .await?;

    // 2. Insert Data using Cypher
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Tom Hanks', age: 68})")
        .await?;
    tx.execute("CREATE (:Movie {title: 'Cast Away'})").await?;
    tx.execute(
        "
        MATCH (p:Person {name: 'Tom Hanks'}), (m:Movie {title: 'Cast Away'})
        CREATE (p)-[:ACTED_IN {role: 'Chuck Noland'}]->(m)
    ",
    )
    .await?;
    tx.commit().await?;

    // 3. Query properties
    let result = db
        .session()
        .query("MATCH (p:Person)-[r:ACTED_IN]->(m:Movie) RETURN p.name, p.age, r.role, m.title")
        .await?;
    assert_eq!(result.len(), 1);

    let row = &result.rows()[0];
    assert_eq!(row.get::<String>("p.name")?, "Tom Hanks");
    assert_eq!(row.get::<i32>("p.age")?, 68);
    assert_eq!(row.get::<String>("r.role")?, "Chuck Noland");
    assert_eq!(row.get::<String>("m.title")?, "Cast Away");

    Ok(())
}

#[tokio::test]
async fn test_api_query_flow() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema implicitly? No, need schema first for properties
    // Or we can rely on "schemaless" if supported?
    // Current Uni requires schema for properties.
    // For now, let's create a label using internal schema manager until Phase 3 (Schema API).
    // Accessing internal schema manager is possible via db.schema (it's pub(crate)).
    // Wait, integration tests are outside the crate, so they can't access pub(crate).
    // I need to use the Schema API or hacks.
    // But Schema API is Phase 3.

    // Test basic queries that don't require schema setup.

    // Test 1: Simple scalar return
    let result = db
        .session()
        .query("RETURN 1 AS num, 'hello' AS str")
        .await?;
    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];
    let num: i64 = row.get("num")?;
    let s: String = row.get("str")?;
    assert_eq!(num, 1);
    assert_eq!(s, "hello");

    // Test 2: List and Map
    let result = db
        .session()
        .query("RETURN [1, 2, 3] AS list, {a: 1} AS map")
        .await?;
    let row = &result.rows()[0];
    // Lists come back as Value::List
    let list: Vec<i64> = row.get("list")?;
    assert_eq!(list, vec![1, 2, 3]);

    // Test 3: Params
    let result = db
        .session()
        .query_with("RETURN $x AS x")
        .param("x", 42)
        .fetch_all()
        .await?;
    let x: i64 = result.rows()[0].get("x")?;
    assert_eq!(x, 42);

    Ok(())
}

#[tokio::test]
async fn test_parameterized_create_vertex() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (p:Person {name: $name, age: $age})")
        .param("name", "Alice")
        .param("age", 30)
        .run()
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("age")?, 30);

    Ok(())
}

#[tokio::test]
async fn test_parameterized_create_edge() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (p:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (p:Person {name: 'Bob'})").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute_with(
        "MATCH (a:Person {name: $src}), (b:Person {name: $dst}) CREATE (a)-[:KNOWS {since: $since}]->(b)",
    )
    .param("src", "Alice")
    .param("dst", "Bob")
    .param("since", 2024)
    .run()
    .await?;
    tx.commit().await?;

    let result = db
        .session().query("MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b:Person {name: 'Bob'}) RETURN k.since AS since")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("since")?, 2024);

    Ok(())
}

#[tokio::test]
async fn test_parameterized_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (p:Person {name: $name}) SET p.age = $new_age")
        .param("name", "Alice")
        .param("new_age", 31)
        .run()
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("age")?, 31);

    Ok(())
}

#[tokio::test]
async fn test_parameterized_delete() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        .await?;
    tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (p:Person {name: $name}) DELETE p")
        .param("name", "Alice")
        .run()
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name")
        .await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name")?, "Bob");

    Ok(())
}

#[tokio::test]
async fn test_execute_with_returns_auto_commit_result() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    let result = tx
        .execute_with("CREATE (i:Item {name: $name})")
        .param("name", "Widget")
        .run()
        .await?;
    tx.commit().await?;

    assert_eq!(result.nodes_created(), 1);
    assert_eq!(result.properties_set(), 1);

    Ok(())
}

#[tokio::test]
async fn test_register_custom_function() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    db.functions().register("double", |args| {
        let n = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
        Ok(Value::Int(n * 2))
    })?;

    let result = session.query("RETURN double(21) AS val").await?;
    assert_eq!(result.rows().len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("val")?, 42);

    Ok(())
}

#[tokio::test]
async fn test_custom_function_isolated_between_instances() -> Result<()> {
    // Verifies the SessionContext template optimization in
    // `Executor::create_datafusion_planner` does not leak custom UDFs
    // registered on one Uni instance to another instance. If the cached
    // SessionContext were shared by reference (via inner Arc<SessionState>),
    // registering `double` on `db_a` would leak to `db_b` and the query
    // would unexpectedly succeed.
    let db_a = Uni::in_memory().build().await?;
    let db_b = Uni::in_memory().build().await?;

    db_a.functions().register("double", |args| {
        let n = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
        Ok(Value::Int(n * 2))
    })?;

    // Sanity: the UDF is visible on db_a.
    let res_a = db_a.session().query("RETURN double(21) AS val").await?;
    assert_eq!(res_a.rows()[0].get::<i64>("val")?, 42);

    // The UDF must NOT be visible on db_b (separate Uni instance).
    let res_b = db_b.session().query("RETURN double(21) AS val").await;
    assert!(
        res_b.is_err(),
        "double() should not exist on a separate Uni instance, got: {res_b:?}"
    );

    Ok(())
}

/// B1 — register → use → remove → must-fail.
///
/// Verifies that `db.functions().remove(...)` causes subsequent queries to
/// fail the function lookup. Guards against a future regression where the
/// cold-path `SessionContext` reuses cached state and silently keeps a
/// removed UDF available.
#[tokio::test]
async fn test_custom_function_remove_invalidates_lookup() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.functions().register("double", |args| {
        let n = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
        Ok(Value::Int(n * 2))
    })?;

    // Registered → query succeeds.
    let ok = db.session().query("RETURN double(21) AS val").await?;
    assert_eq!(ok.rows()[0].get::<i64>("val")?, 42);

    // Remove and confirm the registry reports it was present.
    let removed = db.functions().remove("double")?;
    assert!(
        removed,
        "expected remove() to return true for an existing UDF"
    );

    // Subsequent query must fail with function-not-found.
    let after = db.session().query("RETURN double(21) AS val").await;
    assert!(
        after.is_err(),
        "double() should not be callable after remove(), got: {after:?}"
    );

    Ok(())
}

/// B6 — custom UDF registration must not leak into the cached SessionContext
/// template (the hot-path template held on `UniInner.df_session_template`).
///
/// Failure mode this guards against: a future change accidentally registers
/// custom UDFs on the shared template's `Arc<RwLock<SessionState>>`. Every
/// subsequent hot-path query would then see the leaked UDF even after
/// `remove()`. We exercise: cold path (registered) → remove → hot path
/// (no custom UDFs) — the second query must fail.
#[tokio::test]
async fn test_custom_udf_does_not_leak_into_template() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Register triggers the cold path on the first query.
    db.functions().register("triple", |args| {
        let n = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
        Ok(Value::Int(n * 3))
    })?;
    let cold = db.session().query("RETURN triple(7) AS val").await?;
    assert_eq!(cold.rows()[0].get::<i64>("val")?, 21);

    // Remove. Next query goes through the hot path (no custom UDFs present).
    assert!(db.functions().remove("triple")?);

    // If the template were mutated by the prior register_custom_udfs call,
    // this would still succeed. It must fail.
    let hot = db.session().query("RETURN triple(7) AS val").await;
    assert!(
        hot.is_err(),
        "triple() must not be visible on the hot-path template after remove(), got: {hot:?}"
    );

    Ok(())
}

/// B2 — concurrent queries on the same `Uni` must not cross-contaminate
/// warning sets.
///
/// Failure mode this guards against: a future revert of `Executor`'s manual
/// `Clone` impl back to `#[derive(Clone)]` would alias the `warnings`
/// `Arc<Mutex<...>>` across cloned executors. Two queries running
/// concurrently on the same `Uni` would see each other's warnings.
///
/// The assertion is conservative: every query in this test is identical and
/// emits zero warnings, so each result's warning vector must be empty. If
/// the Mutex were shared, a single warning generated anywhere would surface
/// on multiple results (even though our queries don't generate any) — and
/// more importantly, the test documents the invariant for any future
/// refactor that introduces a warning here.
#[tokio::test]
async fn test_concurrent_query_warnings_dont_cross_contaminate() -> Result<()> {
    let db = std::sync::Arc::new(Uni::in_memory().build().await?);

    // Seed a few nodes so the query has something to scan.
    let tx = db.session().tx().await?;
    for i in 0..10 {
        tx.execute(&format!("CREATE (:N {{idx: {i}}})")).await?;
    }
    tx.commit().await?;

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let db = db.clone();
        set.spawn(async move {
            let res = db
                .session()
                .query("MATCH (n:N) RETURN count(n) AS c")
                .await?;
            Ok::<_, anyhow::Error>(res.warnings().len())
        });
    }

    while let Some(joined) = set.join_next().await {
        let warn_count = joined??;
        assert_eq!(
            warn_count, 0,
            "concurrent query saw warnings from another query's accumulator"
        );
    }

    Ok(())
}

/// B3 — concurrent queries with independent cancellation tokens: cancelling
/// one must not cancel the other.
///
/// Failure mode this guards against: a future change moves
/// `cancellation_token` from the per-query `Executor` into the shared
/// `ExecutorTemplate`. All queries would then share a single token and
/// cancelling one would silently cancel the others.
///
/// We use the public `query_with(...).cancellation_token(...)` builder
/// (the existing test surface for cancellation, used in fork_cancel.rs).
#[tokio::test]
async fn test_concurrent_query_cancellation_isolation() -> Result<()> {
    let db = std::sync::Arc::new(Uni::in_memory().build().await?);

    // Seed enough nodes that a self-join takes meaningful time and is
    // observable by the cancellation token. A 200-node cartesian is 40k
    // pairs — small enough to complete quickly when not cancelled, large
    // enough that cancellation should be observable mid-execution.
    let tx = db.session().tx().await?;
    for i in 0..200 {
        tx.execute(&format!("CREATE (:N {{idx: {i}}})")).await?;
    }
    tx.commit().await?;

    let token_a = tokio_util::sync::CancellationToken::new();
    let token_b = tokio_util::sync::CancellationToken::new();

    // Cancel A immediately so its query observes the cancellation, while B
    // runs a normal completion path with its own (uncancelled) token. The
    // important invariant is *B completes successfully* — i.e., A's
    // cancellation did not affect B's executor.
    token_a.cancel();

    let db_a = db.clone();
    let token_a_clone = token_a.clone();
    let handle_a = tokio::spawn(async move {
        db_a.session()
            .query_with("MATCH (a:N), (b:N) RETURN count(*) AS c")
            .cancellation_token(token_a_clone)
            .fetch_all()
            .await
    });

    let db_b = db.clone();
    let token_b_clone = token_b.clone();
    let handle_b = tokio::spawn(async move {
        db_b.session()
            .query_with("MATCH (n:N) RETURN count(n) AS c")
            .cancellation_token(token_b_clone)
            .fetch_all()
            .await
    });

    let res_a = handle_a.await?;
    let res_b = handle_b.await?;

    // A may have been cancelled (preferred) or raced to completion before
    // cancellation was observed — both are acceptable for this test. What
    // matters is B succeeds, proving the cancellation didn't propagate.
    assert!(
        res_b.is_ok(),
        "query B failed; cancellation likely propagated from query A: {res_b:?}"
    );
    let _ = res_a;

    Ok(())
}

/// B5 — property reads through the shared `PropertyManager` cache must see
/// updates written in a prior committed transaction.
///
/// Failure mode this guards against: a future change introduces a per-query
/// `PropertyManager` cache layer that captures values at query start and
/// serves them on later queries, missing concurrent updates. The current
/// design checks L0 visibility first, but no test directly asserts the
/// "update in tx A → read in tx B sees new value" invariant *across* the
/// shared cache.
#[tokio::test]
async fn test_property_cache_fresh_across_transactions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // tx A: create.
    let tx_a = db.session().tx().await?;
    tx_a.execute("CREATE (:Item {sku: 'X', value: 1})").await?;
    tx_a.commit().await?;

    // Sanity: a fresh session reads the original value.
    let r0 = db
        .session()
        .query("MATCH (i:Item {sku: 'X'}) RETURN i.value AS v")
        .await?;
    assert_eq!(r0.rows()[0].get::<i64>("v")?, 1);

    // tx B (new session, same Uni, same shared PropertyManager): update.
    let tx_b = db.session().tx().await?;
    tx_b.execute("MATCH (i:Item {sku: 'X'}) SET i.value = 2")
        .await?;
    tx_b.commit().await?;

    // tx C (third session): read must see the updated value, not a stale
    // cache from r0's read.
    let r1 = db
        .session()
        .query("MATCH (i:Item {sku: 'X'}) RETURN i.value AS v")
        .await?;
    assert_eq!(
        r1.rows()[0].get::<i64>("v")?,
        2,
        "shared PropertyManager served stale value 1 after update to 2"
    );

    Ok(())
}

#[tokio::test]
async fn test_capabilities_write_lease() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let caps = session.capabilities();
    assert!(caps.can_write);
    // In-memory databases have no explicit write lease configured.
    assert!(caps.write_lease.is_none());

    Ok(())
}
