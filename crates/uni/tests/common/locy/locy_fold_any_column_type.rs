// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! FOLD `COLLECT`, `MIN`, `MAX`, `COUNT` and, recursive, `MMIN` and `MMAX`
//! take a column of any type and answer as Cypher's `collect`, `min`, `max`
//! and `count` do.
//!
//! COLLECT read only Int64, Float64, strings, booleans and Cypher values, and
//! refused a declared `Int32`, `Date` or `DateTime` column ("cannot read a
//! non-null cell of type Int32"). Plain Cypher over the same pattern is the
//! oracle, schemaless values included: MIN and MAX compared their encoded
//! bytes, so `MIN(-1, 1)` was 1 and `MIN('b', 'aa')` was `'b'`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_fold_any_column_type)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Context, Result, bail};
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{DataType, Uni, Value};

/// A value rendered so a list compares as a multiset.
fn canon(value: &Value) -> String {
    match value {
        Value::List(items) => {
            let mut items: Vec<String> = items.iter().map(canon).collect();
            items.sort();
            format!("[{}]", items.join(", "))
        }
        other => format!("{other:?}"),
    }
}

/// `w_type` declares `E.w`; `None` leaves the graph schemaless.
async fn graph(w_type: Option<DataType>, w1: &str, w2: &str) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if let Some(w_type) = w_type {
        db.schema()
            .label("N")
            .property("name", DataType::String)
            .done()
            .edge_type("E", &["N"], &["N"])
            .property_nullable("w", w_type)
            .done()
            .apply()
            .await?;
    }
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE (a:N {{name: 'a'}}), (b:N {{name: 'b'}}), (c:N {{name: 'c'}}), \
         (a)-[:E {{w: {w1}}}]->(b), (a)-[:E {{w: {w2}}}]->(c), (a)-[:E]->(c)"
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// The FOLD's value for `a`; a monotonic aggregate (`MMIN`) in a recursive
/// rule, whose second clause folds `a`'s own edges again.
async fn locy_fold(db: &Uni, aggregate: &str) -> Result<String> {
    let recursive = if aggregate.starts_with("MM") {
        format!(
            "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) WHERE a IS r \
             FOLD t = {aggregate}(e.w) YIELD KEY a, t\n"
        )
    } else {
        String::new()
    };
    let program = format!(
        "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) FOLD t = {aggregate}(e.w) YIELD KEY a, t\n\
         {recursive}QUERY r WHERE a.name = 'a' RETURN t"
    );
    let result = db
        .session()
        .locy_with(&program)
        .with_config(LocyConfig {
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .run()
        .await?;
    match result.command_results.last() {
        Some(CommandResult::Query(rows)) if rows.len() == 1 => {
            Ok(canon(rows[0].get("t").unwrap_or(&Value::Null)))
        }
        other => bail!("expected one row, got {other:?}"),
    }
}

async fn cypher(db: &Uni, aggregate: &str) -> Result<String> {
    let result = db
        .session()
        .query(&format!(
            "MATCH (a:N {{name: 'a'}})-[e:E]->(:N) RETURN {aggregate}(e.w)"
        ))
        .await?;
    let [row] = result.rows() else {
        bail!("expected one row");
    };
    Ok(canon(&row.values()[0]))
}

#[tokio::test]
async fn locy_fold_any_column_type_answers_as_cypher() -> Result<()> {
    let columns = [
        (Some(DataType::String), "'p'", "'q'"),
        (Some(DataType::Int32), "5", "7"),
        (Some(DataType::Int64), "5", "7"),
        (Some(DataType::Float32), "1.5", "2.5"),
        (Some(DataType::Float64), "1.5", "2.5"),
        (Some(DataType::Bool), "true", "false"),
        (
            Some(DataType::Date),
            "date('2024-01-01')",
            "date('2024-02-01')",
        ),
        (
            Some(DataType::DateTime),
            "datetime('2024-01-01T00:00:00Z')",
            "datetime('2024-02-01T00:00:00+02:00')",
        ),
        (
            Some(DataType::Duration),
            "duration('P1D')",
            "duration('P2D')",
        ),
        // Schemaless: pairs whose encoded bytes sort the other way round.
        (None, "256", "255"),
        (None, "-1", "1"),
        (None, "-2.5", "1.5"),
        (None, "'b'", "'aa'"),
        (None, "duration('PT48H')", "duration('P1D')"),
        (None, "date('2024-02-01')", "date('2023-12-31')"),
    ];
    let mut failures = Vec::new();
    for (w_type, w1, w2) in columns {
        let label = format!("{w_type:?} {w1}/{w2}");
        let db = graph(w_type, w1, w2).await.with_context(|| label.clone())?;
        for aggregate in ["COLLECT", "MIN", "MAX", "COUNT", "MMIN", "MMAX"] {
            // `MMIN` answers as `min`, `MMAX` as `max`.
            let oracle = aggregate.strip_prefix('M').filter(|a| a.starts_with('M'));
            let oracle = oracle.unwrap_or(aggregate);
            let want = match cypher(&db, oracle).await {
                Ok(want) => want,
                Err(e) => {
                    failures.push(format!(
                        "{label} {aggregate}: the Cypher oracle failed: {e}"
                    ));
                    continue;
                }
            };
            match locy_fold(&db, aggregate).await {
                Ok(got) if got == want => {}
                Ok(got) => failures.push(format!("{label} {aggregate}: Cypher {want}, Locy {got}")),
                Err(e) => failures.push(format!(
                    "{label} {aggregate}: Cypher {want}, Locy error {e}"
                )),
            }
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}
