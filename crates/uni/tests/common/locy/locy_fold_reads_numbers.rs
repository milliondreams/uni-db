// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A numeric FOLD refuses a value that is not a number, and `SUM` / `AVG`
//! add durations.
//!
//! `SUM`, `AVG`, `MSUM` and `MNOR` read their input as a number. A Cypher
//! value (a schemaless property) or a duration column was brought to Float64
//! through `_cypher_to_float64`, which turns a non-number into NULL, so a FOLD
//! over strings or durations summed to 0.0, and `AVG` and `MNOR` answered
//! NULL, recursive or not. A Cypher value now reaches `SUM` / `AVG` as it is
//! and folds as Cypher's `sum` / `avg` do (one `CypherSum`): durations add up,
//! strings are an error. `MSUM` and `MNOR` read through `_cypher_number`,
//! which keeps NULL and fails on anything but a number.
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

/// Cypher's `sum` / `avg` over `a`'s edges: the oracle for FOLD `SUM` / `AVG`.
async fn cypher(db: &Uni, aggregate: &str) -> Result<Value> {
    let result = db
        .session()
        .query(&format!(
            "MATCH (a:N {{name: 'a'}})-[e:E]->(:N) RETURN {aggregate}(e.w)"
        ))
        .await?;
    let [row] = result.rows() else {
        bail!("{aggregate}: expected one row");
    };
    Ok(row.values()[0].clone())
}

#[tokio::test]
async fn locy_fold_reads_numbers_or_fails() -> Result<()> {
    let mut failures = Vec::new();
    let mut refuse = |label: &str, aggregate: &str, got: Result<Value>| match got {
        Ok(got) => failures.push(format!(
            "{aggregate} over {label}: expected an error, got {got:?}"
        )),
        Err(e) if e.to_string().contains("number") => {}
        Err(e) => failures.push(format!(
            "{aggregate} over {label}: expected a not-a-number error, got {e}"
        )),
    };
    let strings = [
        ("schemaless strings", None),
        ("typed strings", Some(DataType::String)),
    ];
    for (label, w_type) in strings {
        let db = graph(w_type, "'5'", "'7'").await?;
        for aggregate in ["SUM", "AVG", "MSUM", "MNOR"] {
            refuse(label, aggregate, fold(&db, aggregate).await);
        }
    }
    let durations = [
        ("schemaless durations", None),
        ("typed durations", Some(DataType::Duration)),
    ];
    let mut answers = Vec::new();
    for (label, w_type) in durations {
        let db = graph(w_type, "duration('P1D')", "duration('PT12H')").await?;
        // A recursive sum, and a probability, take numbers only.
        for aggregate in ["MSUM", "MNOR"] {
            refuse(label, aggregate, fold(&db, aggregate).await);
        }
        // `SUM` / `AVG` add durations, as Cypher's `sum` / `avg` do.
        for aggregate in ["SUM", "AVG"] {
            answers.push((
                format!("{aggregate} over {label}"),
                cypher(&db, aggregate).await?,
                fold(&db, aggregate).await,
            ));
        }
    }
    // Numbers, schemaless or not, still fold to a float.
    for (label, w_type) in [
        ("schemaless ints", None),
        ("typed int32", Some(DataType::Int32)),
        ("typed floats", Some(DataType::Float64)),
    ] {
        let db = graph(w_type, "5", "7").await?;
        for (aggregate, want) in [("SUM", 12.0), ("AVG", 6.0)] {
            answers.push((
                format!("{aggregate} over {label}"),
                Value::Float(want),
                fold(&db, aggregate).await,
            ));
        }
    }
    for (label, want, got) in answers {
        match got {
            Ok(got) if got == want => {}
            other => failures.push(format!("{label}: expected {want:?}, got {other:?}")),
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}
