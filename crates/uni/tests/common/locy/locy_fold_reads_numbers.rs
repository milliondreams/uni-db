// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A numeric FOLD refuses a value that is not a number.
//!
//! `SUM`, `AVG`, `MSUM` and `MNOR` read their input as a number. A Cypher
//! value (a schemaless property) or a duration column was brought to Float64
//! through `_cypher_to_float64`, which turns a non-number into NULL, so a FOLD
//! over strings or durations summed to 0.0, and `AVG` and `MNOR` answered
//! NULL, recursive or not. The input now reads through `_cypher_number`, which
//! keeps NULL and fails on the rest. `MIN`, `MAX`, `COUNT` and `COLLECT` take
//! any value and still do.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_fold_reads_numbers)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Result, bail};
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{DataType, Uni, Value};

/// `a` reaches `b` and `c`, and `b` reaches `c`: `a` folds two edges.
async fn graph(w_type: Option<DataType>, w1: &str, w2: &str) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if let Some(dt) = w_type {
        db.schema()
            .label("N")
            .property("name", DataType::String)
            .done()
            .edge_type("E", &["N"], &["N"])
            .property_nullable("w", dt)
            .done()
            .apply()
            .await?;
    }
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE (a:N {{name: 'a'}}), (b:N {{name: 'b'}}), (c:N {{name: 'c'}}), \
         (a)-[:E {{w: {w1}}}]->(b), (a)-[:E {{w: {w2}}}]->(c), (b)-[:E {{w: {w2}}}]->(c)"
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// A FOLD of `e.w` per source node, recursive for a monotonic aggregate.
fn program(aggregate: &str) -> String {
    let base = format!(
        "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD t = {aggregate}(e.w) YIELD KEY a, t\n"
    );
    let recursive = if aggregate.starts_with('M') {
        format!(
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) WHERE b IS r \
             FOLD t = {aggregate}(e.w) YIELD KEY a, t\n"
        )
    } else {
        String::new()
    };
    format!("{base}{recursive}QUERY r WHERE a.name = 'a' RETURN t")
}

/// The FOLD's value for `a`.
async fn fold(db: &Uni, aggregate: &str) -> Result<Value> {
    let result = db
        .session()
        .locy_with(&program(aggregate))
        .with_config(LocyConfig {
            max_iterations: 1000,
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .run()
        .await?;
    match result.command_results.last() {
        Some(CommandResult::Query(rows)) if rows.len() == 1 => {
            Ok(rows[0].get("t").cloned().unwrap_or(Value::Null))
        }
        other => bail!("{aggregate}: expected one row, got {other:?}"),
    }
}

#[tokio::test]
async fn locy_fold_reads_numbers_or_fails() -> Result<()> {
    let mut failures = Vec::new();
    let non_numbers = [
        ("schemaless strings", None, "'5'", "'7'"),
        (
            "schemaless durations",
            None,
            "duration('P1D')",
            "duration('P2D')",
        ),
        (
            "typed durations",
            Some(DataType::Duration),
            "duration('P1D')",
            "duration('P2D')",
        ),
        ("typed strings", Some(DataType::String), "'5'", "'7'"),
    ];
    for (label, w_type, w1, w2) in non_numbers {
        let db = graph(w_type, w1, w2).await?;
        for aggregate in ["SUM", "AVG", "MSUM", "MNOR"] {
            match fold(&db, aggregate).await {
                Ok(got) => failures.push(format!(
                    "{aggregate} over {label}: expected an error, got {got:?}"
                )),
                Err(e) if e.to_string().contains("as a number") => {}
                Err(e) => failures.push(format!(
                    "{aggregate} over {label}: expected a not-a-number error, got {e}"
                )),
            }
        }
        // Order and counting take any value.
        for aggregate in ["MIN", "MAX", "COUNT", "MMIN"] {
            if let Err(e) = fold(&db, aggregate).await {
                failures.push(format!("{aggregate} over {label}: {e}"));
            }
        }
    }
    // Numbers, schemaless or not, still fold.
    for (label, w_type) in [
        ("schemaless ints", None),
        ("typed int32", Some(DataType::Int32)),
        ("typed floats", Some(DataType::Float64)),
    ] {
        let db = graph(w_type, "5", "7").await?;
        for (aggregate, want) in [("SUM", 12.0), ("AVG", 6.0)] {
            match fold(&db, aggregate).await {
                Ok(Value::Float(got)) if got == want => {}
                other => failures.push(format!(
                    "{aggregate} over {label}: expected {want}, got {other:?}"
                )),
            }
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}
