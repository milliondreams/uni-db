// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for the CREATE+SET logical-plan fusion (`fuse_create_set`).
//
// The fusion folds a trailing `SET var.prop = value` on a freshly-created
// entity into the CREATE property map, eliminating the separate
// `MutationSetExec` write pass (~38% of per-edge `UNWIND … CREATE … SET`
// execution). Every test here proves the *observable result is unchanged*;
// the positive cases additionally assert via PROFILE that the `MutationSetExec`
// operator is gone (fusion fired), and the negative cases assert it remains
// (fusion correctly declined) while the SET still applies.
// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Collect the physical operator names of a (rolled-back) write query.
async fn operator_names(db: &Uni, cypher: &str, edges: Option<Value>) -> Result<Vec<String>> {
    let tx = db.session().tx().await?;
    let builder = tx.execute_with(cypher);
    let builder = match edges {
        Some(v) => builder.param("edges", v),
        None => builder,
    };
    let (_res, prof) = builder.profile().await?;
    let names = prof
        .runtime_stats
        .iter()
        .map(|op| op.operator.clone())
        .collect();
    tx.rollback();
    Ok(names)
}

fn edge_param(src: i64, dst: i64, role: &str) -> Value {
    let mut m = std::collections::HashMap::new();
    m.insert("src".to_string(), Value::Int(src));
    m.insert("dst".to_string(), Value::Int(dst));
    m.insert("role".to_string(), Value::String(role.to_string()));
    Value::List(vec![Value::Map(m)])
}

/// The production ingest pattern: `UNWIND … MATCH … CREATE … SET … RETURN id`.
/// The fused plan must drop `MutationSetExec` and still write `r.role`.
#[tokio::test]
async fn edge_unwind_create_set_fuses_and_round_trips() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Ep")
        .property("idx", DataType::Int64)
        .done()
        .edge_type("LINK", &["Ep"], &["Ep"])
        .property_nullable("role", DataType::String)
        .done()
        .apply()
        .await?;

    // Two committed endpoints to address by id().
    let (a, b) = {
        let tx = db.session().tx().await?;
        let mut pa = std::collections::HashMap::new();
        pa.insert("idx".to_string(), Value::Int(0));
        let mut pb = std::collections::HashMap::new();
        pb.insert("idx".to_string(), Value::Int(1));
        let va = tx.bulk_insert_vertices("Ep", vec![pa]).await?;
        let vb = tx.bulk_insert_vertices("Ep", vec![pb]).await?;
        tx.commit().await?;
        (va[0].as_u64() as i64, vb[0].as_u64() as i64)
    };

    let cypher = "UNWIND $edges AS e \
         MATCH (a:Ep) WHERE id(a) = e.src \
         MATCH (b:Ep) WHERE id(b) = e.dst \
         CREATE (a)-[r:LINK]->(b) SET r.role = e.role \
         RETURN id(r) AS eid";

    // Plan shape: SET must be fused away.
    let ops = operator_names(&db, cypher, Some(edge_param(a, b, "member"))).await?;
    assert!(
        !ops.iter().any(|o| o == "MutationSetExec"),
        "expected SET to be fused into CREATE, but MutationSetExec is present: {ops:?}"
    );
    assert!(
        ops.iter().any(|o| o == "MutationCreateExec"),
        "CREATE operator should remain: {ops:?}"
    );

    // Result: the edge property landed.
    let tx = db.session().tx().await?;
    let res = tx
        .execute_with(cypher)
        .param("edges", edge_param(a, b, "member"))
        .run()
        .await?;
    assert_eq!(res.relationships_created(), 1);
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH ()-[r:LINK]->() RETURN r.role AS role")
        .await?;
    assert_eq!(r.rows()[0].get::<String>("role").unwrap(), "member");
    Ok(())
}

/// Node `CREATE (n) SET n.x = lit` fuses; a pre-existing inline value is
/// overridden (SET last-write-wins precedence is preserved).
#[tokio::test]
async fn node_create_set_fuses_with_precedence() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let cypher = "CREATE (n:Person {name: 'p', x: 1}) SET n.x = 9";
    let ops = operator_names(&db, cypher, None).await?;
    assert!(
        !ops.iter().any(|o| o == "MutationSetExec"),
        "node SET should fuse: {ops:?}"
    );

    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Person) RETURN n.x AS x")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("x").unwrap(),
        9,
        "SET must override the inline CREATE value"
    );
    Ok(())
}

/// SET on a MATCHed (not created) variable must NOT fuse — `MutationSetExec`
/// stays and the update still applies.
#[tokio::test]
async fn set_on_matched_var_does_not_fuse() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'p', x: 1})").await?;
    tx.commit().await?;

    let cypher = "MATCH (n:Person {name: 'p'}) SET n.x = 7";
    let ops = operator_names(&db, cypher, None).await?;
    assert!(
        ops.iter().any(|o| o == "MutationSetExec"),
        "SET on a MATCHed var must not fuse: {ops:?}"
    );

    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    let r = db
        .session()
        .query("MATCH (n:Person) RETURN n.x AS x")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("x").unwrap(), 7);
    Ok(())
}

/// SET on a MATCHed variable that is *reused* inside the CREATE pattern
/// (`MATCH (a) CREATE (a)-[r]->(b) SET a.p = 1`) must NOT fuse onto `a`.
///
/// Regression for the 2026-06-10 review #4: the fusion built its `owner` set
/// from *all* CREATE-pattern variables, so the upstream-bound `a` looked
/// freshly created; the SET fused into the `(a)` element, which the executor
/// skips because `a` is already bound — silently dropping the write.
#[tokio::test]
async fn set_on_matched_var_reused_in_create_does_not_fuse() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("p", DataType::Int64)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'a'})").await?;
    tx.commit().await?;

    // `a` is MATCH-bound, then reused as the source of a fresh edge; the SET
    // targets the bound `a`, so it must not fuse into the CREATE element.
    let cypher = "MATCH (a:Person {name: 'a'}) \
         CREATE (a)-[r:KNOWS]->(b:Person {name: 'b'}) SET a.p = 1";
    let ops = operator_names(&db, cypher, None).await?;
    assert!(
        ops.iter().any(|o| o == "MutationSetExec"),
        "SET on a MATCHed var reused in CREATE must not fuse: {ops:?}"
    );

    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (a:Person {name: 'a'}) RETURN a.p AS p")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("p").unwrap(),
        1,
        "SET on the matched var must apply (was silently dropped by fusion)"
    );
    Ok(())
}

/// `SET n += {...}` (map merge) is not the simple property form and must not
/// fuse, but must still apply.
#[tokio::test]
async fn set_map_merge_does_not_fuse() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("x", DataType::Int64)
        .property_nullable("y", DataType::Int64)
        .done()
        .apply()
        .await?;

    let cypher = "CREATE (n:Person {name: 'p', x: 1}) SET n += {x: 5, y: 6}";
    let ops = operator_names(&db, cypher, None).await?;
    assert!(
        ops.iter().any(|o| o == "MutationSetExec"),
        "SET += must not fuse: {ops:?}"
    );

    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    let r = db
        .session()
        .query("MATCH (n:Person) RETURN n.x AS x, n.y AS y")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("x").unwrap(), 5);
    assert_eq!(r.rows()[0].get::<i64>("y").unwrap(), 6);
    Ok(())
}

/// A SET value that references another variable created in the same statement
/// must NOT fuse (evaluating it at create time could differ from SET time),
/// but the result must still be correct.
#[tokio::test]
async fn set_value_referencing_created_var_does_not_fuse() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("x", DataType::Int64)
        .done()
        .apply()
        .await?;

    // b.x is set from a.x — a is created in the same CREATE.
    let cypher = "CREATE (a:Person {name: 'a', x: 42}), (b:Person {name: 'b'}) SET b.x = a.x";
    let ops = operator_names(&db, cypher, None).await?;
    assert!(
        ops.iter().any(|o| o == "MutationSetExec"),
        "SET referencing a created var must not fuse: {ops:?}"
    );

    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    let r = db
        .session()
        .query("MATCH (n:Person {name: 'b'}) RETURN n.x AS x")
        .await?;
    assert_eq!(r.rows()[0].get::<i64>("x").unwrap(), 42);
    Ok(())
}
