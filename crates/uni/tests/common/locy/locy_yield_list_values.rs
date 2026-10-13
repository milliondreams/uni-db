// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A Locy YIELD returns the value its expression has in Cypher.
//!
//! The YIELD projection casts each column to a type the planner infers from
//! the expression, and an expression the inference did not know (a list, a
//! comprehension, a map, `reduce`) was typed as a string. So every list a rule
//! yielded came back as its text, `[5, 5]` as the string `"[5, 5]"`; with no
//! schema the list's elements were encoded values and the text was their
//! bytes, `"[0205, 0205]"`, or the cast failed outright ("Encountered non
//! UTF-8 data"). Separately, a relationship property declared nowhere was
//! guessed to be a float, so a stored `5` came back as `5.0`.
//!
//! Plain Cypher is the oracle: for every expression, the value a rule yields
//! must be the value `MATCH ... RETURN` gives, with and without a schema.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(yield_list_values)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Result, bail};
use uni_common::Value;
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{DataType, Uni};

const GRAPH: &str = "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (a)-[:E {w: 5}]->(b)";

const EXPRESSIONS: [&str; 10] = [
    "[e.w, e.w]",
    "[x.name]",
    "[1, e.w]",
    "[v IN [1, 2] | v * 2]",
    "[v IN [1, 2, 3] WHERE v > e.w - 4]",
    "reduce(s = 0, v IN [1, 2] | s + v)",
    "{k: e.w}",
    "e.w",
    "x.name",
    "[[1], [e.w]]",
];

async fn graph(declared: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if declared {
        db.schema()
            .label("N")
            .property("name", DataType::String)
            .done()
            .edge_type("E", &["N"], &["N"])
            .property("w", DataType::Int64)
            .done()
            .apply()
            .await?;
    }
    let tx = db.session().tx().await?;
    tx.execute(GRAPH).await?;
    tx.commit().await?;
    Ok(db)
}

async fn cypher_value(db: &Uni, expr: &str) -> Result<Value> {
    let result = db
        .session()
        .query(&format!("MATCH (x:N)-[e:E]->(y:N) RETURN {expr} AS v"))
        .await?;
    let Some(row) = result.rows().first() else {
        bail!("Cypher returned no row for {expr}");
    };
    let Some(value) = row.value("v") else {
        bail!("Cypher returned no column v for {expr}");
    };
    Ok(value.clone())
}

async fn locy_value(db: &Uni, expr: &str) -> Result<Value> {
    let program = format!(
        "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) YIELD KEY x, {expr} AS v\nQUERY p RETURN v"
    );
    let result = db
        .session()
        .locy_with(&program)
        .with_config(LocyConfig {
            timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        })
        .run()
        .await?;
    let Some(CommandResult::Query(rows)) = result.command_results.last() else {
        bail!("no QUERY result for {expr}");
    };
    let Some(value) = rows.first().and_then(|row| row.get("v")) else {
        bail!("no row for {expr}");
    };
    Ok(value.clone())
}

#[tokio::test]
async fn yield_list_values_match_cypher() -> Result<()> {
    let mut failures = Vec::new();
    for declared in [false, true] {
        let db = graph(declared).await?;
        for expr in EXPRESSIONS {
            let want = cypher_value(&db, expr).await?;
            match locy_value(&db, expr).await {
                Ok(got) if got == want => {}
                Ok(got) => failures.push(format!(
                    "declared={declared} {expr}: Locy {got:?}, Cypher {want:?}"
                )),
                Err(e) => failures.push(format!("declared={declared} {expr}: Locy failed: {e:#}")),
            }
        }
    }
    if !failures.is_empty() {
        bail!(
            "{} of {} differ:\n{}",
            failures.len(),
            2 * EXPRESSIONS.len(),
            failures.join("\n")
        );
    }
    Ok(())
}
