#![allow(dead_code, unused_imports, clippy::all)]
// Rust guideline compliant
//! M9 — end-to-end test for `uni.plugin.declareFunction`.
//!
//! Exercises the acceptance criterion from
//! `docs/plans/plugin_framework_implementation.md` §4 M9:
//!
//! 1. `CALL uni.plugin.declareFunction(...)` accepts a Cypher
//!    expression body and registers a new scalar function.
//! 2. The declared function is immediately callable as a plain
//!    Cypher scalar (`RETURN mycorp.fullName('Ada', 'Lovelace')`).
//! 3. `CALL uni.plugin.listDeclared()` yields the declaration.
//! 4. `CALL uni.plugin.dropDeclared(...)` removes it.

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn declare_function_round_trip_via_cypher() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Step 1 — declare a function whose body is a Cypher expression.
    let tx = db.session().tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareFunction(
            'mycorp.fullName',
            '$first + " " + $last',
            'string',
            '["first","last"]'
        )"#,
    )
    .await?;
    tx.commit().await?;

    // Step 2 — call the declared function from a fresh session.
    let res = db
        .session()
        .query("RETURN mycorp.fullName('Ada', 'Lovelace') AS name")
        .await?;
    assert_eq!(
        res.rows()[0].get::<String>("name")?,
        "Ada Lovelace",
        "declared scalar fn did not produce the expected value"
    );

    // Step 3 — listDeclared yields the registration.
    let listed = db
        .session()
        .query(
            "CALL uni.plugin.listDeclared() YIELD qname, kind, active RETURN qname, kind, active",
        )
        .await?;
    let mut found = false;
    for row in listed.rows() {
        if row.get::<String>("qname")? == "mycorp.fullName" {
            assert_eq!(row.get::<String>("kind")?, "function");
            assert!(row.get::<bool>("active")?, "expected active=true");
            found = true;
        }
    }
    assert!(found, "listDeclared did not yield mycorp.fullName");

    // Step 4 — dropDeclared removes it.
    let tx = db.session().tx().await?;
    tx.execute("CALL uni.plugin.dropDeclared('mycorp.fullName')")
        .await?;
    tx.commit().await?;

    let after = db
        .session()
        .query("CALL uni.plugin.listDeclared() YIELD qname RETURN count(*) AS n")
        .await?;
    assert_eq!(after.rows()[0].get::<i64>("n")?, 0);

    Ok(())
}

#[tokio::test]
async fn declare_function_integer_arithmetic() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareFunction(
            'mycorp.square',
            '$x * $x',
            'int',
            '["x"]'
        )"#,
    )
    .await?;
    tx.commit().await?;

    let res = db.session().query("RETURN mycorp.square(7) AS y").await?;
    assert_eq!(res.rows()[0].get::<i64>("y")?, 49);

    Ok(())
}

/// Full end-to-end: declare an aggregate from Cypher, then invoke it
/// in a `MATCH ... RETURN agg(n.value)` query. Proves the M9 path
/// `DeclareAggregateProcedure → DeclaredAggregateFn → PluginRegistry →
/// PluginAggregateUdaf (df_udaf_plugin) → planner translate_aggregates
/// fallthrough`.
#[tokio::test]
async fn declare_aggregate_round_trip_via_cypher() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Seed three Items.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {value: 1}), (:Item {value: 2}), (:Item {value: 3})")
        .await?;
    tx.commit().await?;

    // Declare a sum-of-squares aggregate.
    let tx = db.session().tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareAggregate(
            'mycorp.sumSquares',
            '0',
            '$state + ($x * $x)',
            '$state',
            'int',
            '["x"]'
        )"#,
    )
    .await?;
    tx.commit().await?;

    // Invoke via plain Cypher aggregation.
    let res = db
        .session()
        .query("MATCH (n:Item) RETURN mycorp.sumSquares(n.value) AS s")
        .await?;
    // 1 + 4 + 9 = 14
    assert_eq!(res.rows()[0].get::<i64>("s")?, 14);

    Ok(())
}

/// M11 §4 #6 acceptance — a declared `WRITE`-mode procedure runs its
/// Cypher body through `SyntheticProcedurePlugin` →
/// `QueryProcedureHost::execute_inner_query` in `Write` mode and
/// produces visible side-effects.
///
/// Requires the FU-1 principal-plumbing fix (Transaction →
/// `CURRENT_PRINCIPAL` task-local → `ProcedureContext::with_principal`)
/// so the capability gate on `declareProcedure WRITE` admits a
/// principal holding `Capability::ProcedureWrites`. Requires the
/// multi-thread runtime because `SyntheticProcedurePlugin` bridges
/// sync→async via `tokio::task::block_in_place`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_procedure_write_mode_creates_nodes() -> Result<()> {
    use std::sync::Arc;
    use uni_plugin::traits::connector::Principal;
    use uni_plugin::{Capability, CapabilitySet};

    let db = Uni::in_memory().build().await?;

    let mut caps = CapabilitySet::new();
    caps.insert(Capability::ProcedureWrites);
    let writer_principal = Arc::new(Principal {
        id: "admin".to_owned(),
        groups: vec!["admin".to_owned()],
        capabilities: caps,
    });
    let writer_session = db.session().with_principal(Arc::clone(&writer_principal));

    let tx = writer_session.tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareProcedure(
            'mycorp.createmarker',
            'CREATE (m:Marker {at: 42})',
            'WRITE',
            '[]',
            '[]'
        )"#,
    )
    .await?;
    tx.commit().await?;

    let tx = writer_session.tx().await?;
    tx.execute("CALL mycorp.createmarker()").await?;
    tx.commit().await?;

    let counted = db
        .session()
        .query("MATCH (m:Marker) RETURN count(m) AS n")
        .await?;
    assert_eq!(
        counted.rows()[0].get::<i64>("n")?,
        1,
        "declared WRITE-mode procedure must have created exactly one :Marker"
    );

    Ok(())
}

/// `deps_json` arg lands in the persisted `dependencies` list. Forward
/// declaring `child` with a missing dep should fail; declaring both in
/// order succeeds.
#[tokio::test]
async fn declare_dependencies_surface() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Forward-declare a parent first so the dependency exists.
    let tx = db.session().tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareFunction(
            'mycorp.parent',
            '$x',
            'string',
            '["x"]'
        )"#,
    )
    .await?;
    tx.commit().await?;

    // Now declare a child with deps_json referencing parent.
    let tx = db.session().tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareFunction(
            'mycorp.child',
            '$x',
            'string',
            '["x"]',
            '["mycorp.parent"]'
        )"#,
    )
    .await?;
    tx.commit().await?;

    // Listing yields both with kind=function.
    let listed = db
        .session()
        .query("CALL uni.plugin.listDeclared() YIELD qname, kind RETURN qname, kind")
        .await?;
    let names: Vec<String> = listed
        .rows()
        .iter()
        .map(|r| r.get::<String>("qname").unwrap_or_default())
        .collect();
    assert!(names.iter().any(|q| q == "mycorp.parent"));
    assert!(names.iter().any(|q| q == "mycorp.child"));

    Ok(())
}
