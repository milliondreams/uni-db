// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A Locy `QUERY ... RETURN` answers entity functions as Cypher does.
//!
//! Locy's row evaluator handled a few functions itself and passed the rest to
//! the shared `eval_scalar_function`, which had no `id`, `elementId`, `type`,
//! `labels` or `properties`: the Cypher executors each kept their own copy.
//! So `type(e)` over a rule's relationship failed with "not implemented",
//! `keys(n)` rejected a hydrated node, and `startNode` / `endNode` were
//! missing. Plain Cypher over the same pattern is the oracle.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_entity_functions)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Result, bail};
use uni_common::Value;
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyConfig};

/// Unrelated nodes first, so node VIDs and edge EIDs share numbers.
const GRAPH: &str = "CREATE (:Other {name: 'x'}), (:Other {name: 'y'}), \
     (a:N:M {name: 'a', k: 2}), (b:N {name: 'b'}), (c:N {name: 'c', k: 1}), \
     (a)-[:E {w: 1}]->(b), (b)-[:F {w: 2, tag: 't'}]->(c)";

/// A value rendered with map keys sorted, so two runs compare equal.
fn canon(value: &Value) -> String {
    match value {
        Value::Map(map) => {
            let mut entries: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{k}: {}", canon(v)))
                .collect();
            entries.sort();
            format!("{{{}}}", entries.join(", "))
        }
        Value::List(items) => {
            format!(
                "[{}]",
                items.iter().map(canon).collect::<Vec<_>>().join(", ")
            )
        }
        other => format!("{other:?}"),
    }
}

#[tokio::test]
async fn locy_entity_functions_answer_as_cypher_does() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(GRAPH).await?;
    tx.commit().await?;

    let expressions = [
        "type(e)",
        "labels(a)",
        "id(a)",
        "id(e)",
        "elementId(a)",
        "elementId(e)",
        "properties(a)",
        "properties(e)",
        "keys(a)",
        "keys(e)",
        "startNode(e).name",
        "endNode(e).name",
    ];
    let mut failures = Vec::new();
    for expr in expressions {
        let want = db
            .session()
            .query(&format!(
                "MATCH (a:N)-[e]->(b:N) RETURN a.name AS a, {expr} AS v ORDER BY a"
            ))
            .await?;
        let want: Vec<String> = want
            .rows()
            .iter()
            .map(|r| {
                format!(
                    "{:?} {}",
                    r.value("a"),
                    canon(r.value("v").unwrap_or(&Value::Null))
                )
            })
            .collect();
        let result = db
            .session()
            .locy_with(&format!(
                "CREATE RULE r AS MATCH (a:N)-[e]->(b:N) YIELD KEY a, KEY e, KEY b\n\
                 QUERY r RETURN a.name AS a, {expr} AS v"
            ))
            .with_config(LocyConfig {
                timeout: Some(Duration::from_secs(60)),
                ..Default::default()
            })
            .run()
            .await;
        let got = match result {
            Ok(result) => match result.command_results.last() {
                Some(CommandResult::Query(rows)) => {
                    let mut got: Vec<String> = rows
                        .iter()
                        .map(|r| {
                            format!(
                                "{:?} {}",
                                r.get("a"),
                                canon(r.get("v").unwrap_or(&Value::Null))
                            )
                        })
                        .collect();
                    got.sort();
                    got
                }
                other => vec![format!("no QUERY result: {other:?}")],
            },
            Err(e) => vec![format!("error: {e}")],
        };
        if got != want {
            failures.push(format!("{expr}: Cypher {want:?}, Locy {got:?}"));
        }
    }
    if !failures.is_empty() {
        bail!(
            "{} of {} differ:\n{}",
            failures.len(),
            expressions.len(),
            failures.join("\n")
        );
    }
    Ok(())
}
