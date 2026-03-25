// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for the Locy engine wired to a real database.

use anyhow::Result;
use uni_db::Uni;

// ── Step 1: Skeleton ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_locy_engine_exists() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let _engine = db.locy();
    Ok(())
}

// ── Step 2: compile_only ───────────────────────────────────────────────────

#[tokio::test]
async fn test_compile_only_valid() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let compiled = db
        .locy()
        .compile_only("CREATE RULE r AS MATCH (a)-[:K]->(b) YIELD KEY a, b")?;
    assert_eq!(compiled.strata.len(), 1);
    assert!(compiled.rule_catalog.contains_key("r"));
    Ok(())
}

// ── Step 3: Parse error ────────────────────────────────────────────────────

#[tokio::test]
async fn test_parse_error() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let err = db
        .locy()
        .compile_only("THIS IS NOT VALID LOCY")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("LocyParseError"),
        "expected LocyParseError, got: {msg}"
    );
    assert!(matches!(err, uni_db::UniError::Parse { .. }));
    Ok(())
}

// ── Step 4: Compile error ──────────────────────────────────────────────────

#[tokio::test]
async fn test_compile_error_cyclic_negation() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // a IS NOT b and b IS NOT a → cyclic negation
    let program = "CREATE RULE a AS MATCH (x) WHERE x IS NOT b YIELD KEY x \n\
                    CREATE RULE b AS MATCH (x) WHERE x IS NOT a YIELD KEY x";
    let err = db.locy().compile_only(program).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("LocyCompileError"),
        "expected LocyCompileError, got: {msg}"
    );
    assert!(matches!(err, uni_db::UniError::Query { .. }));
    Ok(())
}

// ── Step 5: Non-recursive evaluation ───────────────────────────────────────

#[tokio::test]
async fn test_evaluate_non_recursive() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;

    let result = db
        .locy()
        .evaluate(
            "CREATE RULE friends AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             YIELD KEY a, b",
        )
        .await?;

    let friends = result
        .derived
        .get("friends")
        .expect("rule 'friends' missing");
    assert_eq!(friends.len(), 1);
    // Keys are the node variables themselves
    assert!(friends[0].contains_key("a"));
    assert!(friends[0].contains_key("b"));
    Ok(())
}

// ── Step 6: Recursive transitive closure ───────────────────────────────────

#[tokio::test]
async fn test_evaluate_recursive_transitive_closure() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;

    let result = db
        .locy()
        .evaluate(
            "CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .await?;

    let reachable = result
        .derived
        .get("reachable")
        .expect("rule 'reachable' missing");
    // A→B, B→C, A→C (transitive)
    assert!(
        reachable.len() >= 3,
        "expected at least 3 reachable facts, got {}",
        reachable.len()
    );
    Ok(())
}

// ── Step 7: DERIVE creates edges ───────────────────────────────────────────

#[tokio::test]
async fn test_derive_creates_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;

    db.locy()
        .evaluate(
            "CREATE RULE inferred AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             DERIVE (a)-[:INFERRED]->(b) \
             DERIVE inferred",
        )
        .await?;

    // Query the newly created edges
    let result = db
        .query("MATCH (a)-[:INFERRED]->(b) RETURN a.name AS src, b.name AS dst")
        .await?;
    assert_eq!(result.rows.len(), 1);
    Ok(())
}

// ── Step 8: ASSUME with savepoint rollback ─────────────────────────────────

#[tokio::test]
async fn test_assume_rollback() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;

    // ASSUME creates a temporary node, THEN re-evaluates rule in mutated state
    let result = db
        .locy()
        .evaluate(
            "CREATE RULE base AS \
             MATCH (p:Person) YIELD KEY p \n\
             ASSUME { CREATE (:Person {name: 'Temp'}) } \
             THEN QUERY base WHERE p = p",
        )
        .await?;

    // The ASSUME command result should show the re-evaluated state
    assert!(!result.command_results.is_empty());

    // After rollback, the temporary node should NOT exist in the real DB
    let check = db.query("MATCH (p:Person {name: 'Temp'}) RETURN p").await?;
    assert_eq!(
        check.rows.len(),
        0,
        "ASSUME should have rolled back the temporary node"
    );
    Ok(())
}

// ── Step 9: Runtime error ──────────────────────────────────────────────────

#[tokio::test]
async fn test_runtime_error_max_iterations() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;

    let config = uni_db::locy::LocyConfig {
        max_iterations: 1,
        timeout: std::time::Duration::from_secs(30),
        ..Default::default()
    };

    let err = db
        .locy()
        .evaluate_with_config(
            "CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
            &config,
        )
        .await
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("LocyRuntimeError"),
        "expected LocyRuntimeError, got: {msg}"
    );
    assert!(matches!(err, uni_db::UniError::Query { .. }));
    Ok(())
}

// ── Step 10: End-to-end smoke test ─────────────────────────────────────────

#[tokio::test]
async fn test_end_to_end_smoke() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create a small social network
    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}), \
         (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)",
    )
    .await?;

    // Compute transitive reachability
    let result = db
        .locy()
        .evaluate(
            "CREATE RULE reachable AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:Person)-[:KNOWS]->(mid:Person) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .await?;

    let reachable = result
        .derived
        .get("reachable")
        .expect("rule 'reachable' missing");

    // Expected: Alice→Bob, Bob→Carol, Alice→Carol (transitive)
    assert!(
        reachable.len() >= 3,
        "expected at least 3 reachable pairs, got {}",
        reachable.len()
    );

    // Verify stats
    assert!(result.stats.total_iterations > 0);
    Ok(())
}

// ── LocyBuilder (evaluate_with) ────────────────────────────────────────────

#[tokio::test]
async fn test_evaluate_with_param_builder() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE (:Episode {agent_id: 'a1', label: 'alpha'})")
        .await?;
    db.execute("CREATE (:Episode {agent_id: 'a2', label: 'beta'})")
        .await?;

    let result = db
        .locy()
        .evaluate_with(
            "CREATE RULE ep AS MATCH (e:Episode) WHERE e.agent_id = $aid \
             YIELD KEY e, e.label AS lbl \
             QUERY ep RETURN lbl",
        )
        .param("aid", "a1")
        .run()
        .await?;

    let rows = match &result.command_results[0] {
        uni_db::locy::CommandResult::Query(rows) => rows,
        other => panic!("expected Query, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("lbl"),
        Some(&uni_db::Value::String("alpha".to_string()))
    );
    Ok(())
}

#[tokio::test]
async fn test_evaluate_with_multiple_params() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.execute("CREATE (:Score {name: 'low',  val: 10})")
        .await?;
    db.execute("CREATE (:Score {name: 'mid',  val: 50})")
        .await?;
    db.execute("CREATE (:Score {name: 'high', val: 90})")
        .await?;

    let result = db
        .locy()
        .evaluate_with(
            "CREATE RULE sc AS MATCH (s:Score) YIELD KEY s, s.val AS v, s.name AS nm \
             QUERY sc WHERE v > $lo AND v < $hi RETURN nm",
        )
        .param("lo", uni_db::Value::Int(20))
        .param("hi", uni_db::Value::Int(80))
        .run()
        .await?;

    let rows = match &result.command_results[0] {
        uni_db::locy::CommandResult::Query(rows) => rows,
        other => panic!("expected Query, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("nm"),
        Some(&uni_db::Value::String("mid".to_string()))
    );
    Ok(())
}
