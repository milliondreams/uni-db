// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A Locy rule's facts are rows, its `QUERY` filter is three-valued, and a sum
//! of nothing is 0.
//!
//! * A derived fact is a row (reference §6.2): two derivations of the same row
//!   are one fact. The recursive fixpoint deduplicated through its delta
//!   computation; a non-recursive rule, evaluated in one pass, did not, so three
//!   parallel edges derived `(a, b)` three times.
//! * `QUERY ... WHERE` runs in Locy's in-memory evaluator, where `NULL = x` was
//!   false and `NULL <> x` true, and `AND`/`OR` with a NULL side were NULL even
//!   when the other side decides them. So `QUERY r WHERE a.age <> 0` kept rows
//!   whose `age` is NULL, which the same filter in a rule body (and in Cypher)
//!   rejects.
//! * `SUM` / `MSUM` over a group with no non-null value is 0.0, as Cypher's
//!   `sum` is; it was NULL.
//!
//! Found by the `locy_rule`, `locy_reach` and `locy_fold` relations of
//! `metamorphic::dqp::topo` (W4).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_facts_and_filters)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .property_nullable("age", DataType::Int)
        .edge_type("K", &["P"], &["P"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:P {id: 1, age: 30}), (b:P {id: 2}), \
         (a)-[:K]->(b), (a)-[:K]->(b), (a)-[:K]->(b), (a)-[:K]->(a)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn rows(db: &Uni, program: &str) -> Result<Vec<Vec<Value>>> {
    let result = db.session().locy(program).await?;
    let rows = result
        .command_results()
        .iter()
        .find_map(|c| c.as_query())
        .expect("program has a QUERY");
    let mut out: Vec<Vec<Value>> = rows
        .iter()
        .map(|row| {
            let mut columns: Vec<_> = row.iter().collect();
            columns.sort_by_key(|(name, _)| (*name).clone());
            columns.into_iter().map(|(_, v)| v.clone()).collect()
        })
        .collect();
    out.sort_by_key(|r| format!("{r:?}"));
    Ok(out)
}

#[tokio::test]
async fn locy_facts_and_filters_a_fact_is_a_row() -> Result<()> {
    let db = open().await?;
    let program = "CREATE RULE r AS MATCH (a:P)-[:K]->(b:P) YIELD KEY a, KEY b \
                   QUERY r RETURN a.id AS c0, b.id AS c1";
    assert_eq!(
        rows(&db, program).await?,
        vec![
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(1), Value::Int(2)],
        ]
    );
    let result = db.session().locy(program).await?;
    assert_eq!(result.derived_facts("r").map(Vec::len), Some(2));
    // A FOLD still counts every derivation.
    assert_eq!(
        rows(
            &db,
            "CREATE RULE n AS MATCH (a:P)-[:K]->(b:P) WHERE b.id = 2 FOLD c = COUNT(*) \
             YIELD KEY a, c QUERY n RETURN c AS c0"
        )
        .await?,
        vec![vec![Value::Int(3)]]
    );
    Ok(())
}

#[tokio::test]
async fn locy_facts_and_filters_query_where_is_three_valued() -> Result<()> {
    let db = open().await?;
    let base = "CREATE RULE r AS MATCH (a:P) YIELD KEY a ";
    let one = vec![vec![Value::Int(1)]];
    for (filter, want) in [
        ("a.age <> 0", one.clone()),
        ("NOT (a.age = 99)", one.clone()),
        ("(a.age <> 0) AND (a.id > 0)", one.clone()),
        ("a.age = 30 OR a.age = 31", one.clone()),
        // `false AND NULL` is false and `true OR NULL` is true.
        (
            "NOT (a.id = 1 AND a.age = 0)",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ),
        (
            "a.id = 2 OR a.age = 30",
            vec![vec![Value::Int(1)], vec![Value::Int(2)]],
        ),
        ("a.age IS NULL", vec![vec![Value::Int(2)]]),
    ] {
        let program = format!("{base}QUERY r WHERE {filter} RETURN a.id AS c0");
        assert_eq!(rows(&db, &program).await?, want, "{filter}");
    }
    Ok(())
}

#[tokio::test]
async fn locy_facts_and_filters_sum_of_nothing_is_zero() -> Result<()> {
    let db = open().await?;
    for agg in ["SUM", "MSUM"] {
        let program = format!(
            "CREATE RULE s AS MATCH (a:P)-[:K]->(b:P) WHERE b.id = 2 FOLD t = {agg}(b.age) \
             YIELD KEY a, t QUERY s RETURN t AS c0"
        );
        assert_eq!(
            rows(&db, &program).await?,
            vec![vec![Value::Float(0.0)]],
            "{agg}"
        );
    }
    Ok(())
}

/// A `QUERY ... WHERE` that cannot be evaluated is an error, not an empty
/// result. Evaluation errors and non-boolean conditions used to drop every
/// row silently (W6).
#[tokio::test]
async fn locy_facts_and_filters_query_where_errors_are_reported() -> Result<()> {
    let db = open().await?;
    let base = "CREATE RULE r AS MATCH (a:P) YIELD KEY a ";
    for filter in ["a.id / 0 = 1", "a.id", "NOT a.id"] {
        let program = format!("{base}QUERY r WHERE {filter} RETURN a.id AS c0");
        let err = db
            .session()
            .locy(&program)
            .await
            .expect_err(&format!("`{filter}` cannot be evaluated"));
        let text = err.to_string();
        assert!(
            text.contains("division by zero")
                || text.contains("must be a boolean")
                || text.contains("NOT requires boolean"),
            "{filter}: {text}"
        );
    }
    // Control: NULL is not an error; it drops the row.
    assert_eq!(
        rows(
            &db,
            &format!("{base}QUERY r WHERE a.age > 0 RETURN a.id AS c0")
        )
        .await?,
        vec![vec![Value::Int(1)]]
    );
    Ok(())
}

/// `QUERY ... ORDER BY` an expression that is not a returned column sorts by
/// it. The keys were evaluated on the projected row, where `a.age` no longer
/// existed; the error became NULL, every key tied, and the clause silently did
/// nothing (W6).
#[tokio::test]
async fn locy_facts_and_filters_query_order_by_unreturned_expression() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .property("age", DataType::Int)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {id: 1, age: 20}), (:P {id: 2, age: 40}), (:P {id: 3, age: 30})")
        .await?;
    tx.commit().await?;
    let base = "CREATE RULE r AS MATCH (a:P) YIELD KEY a QUERY r RETURN a.id AS c0 ";
    for (order, want) in [
        ("ORDER BY a.age DESC", [2, 3, 1]),
        ("ORDER BY a.age", [1, 3, 2]),
        ("ORDER BY -a.age", [2, 3, 1]),
        // Control: a returned alias.
        ("ORDER BY c0 DESC", [3, 2, 1]),
    ] {
        let program = format!("{base}{order}");
        let result = db.session().locy(&program).await?;
        let rows = result
            .command_results()
            .iter()
            .find_map(|c| c.as_query())
            .expect("program has a QUERY");
        let got: Vec<Value> = rows.iter().map(|r| r.get("c0").cloned().unwrap()).collect();
        assert_eq!(got, want.map(Value::Int).to_vec(), "{order}");
    }
    // A key that cannot be evaluated is an error.
    let err = db
        .session()
        .locy(&format!("{base}ORDER BY a.id / 0"))
        .await
        .expect_err("ORDER BY a.id / 0");
    assert!(err.to_string().contains("division by zero"), "{err}");
    Ok(())
}

/// A `QUERY ... WHERE` keeps the rows the same filter keeps in Cypher.
/// Comparing values of types with no order between them is NULL, not false;
/// it was false, so under `NOT` every such row was kept (W6).
#[tokio::test]
async fn locy_facts_and_filters_query_where_matches_cypher() -> Result<()> {
    let db = open().await?;
    for filter in [
        "a.id < 'x'",
        "NOT (a.id < 'x')",
        "NOT (a.id >= 'x')",
        "NOT (a.id <= true)",
        "NOT ([a.id, 'x'] > [a.id, 1])",
        // Controls: comparable values.
        "[a.id, 1] < [a.id, 2]",
        "NOT ([a.id] < [a.id, 0])",
        "a.id ^ 2 = 4",
        "a.id >= 2.0",
    ] {
        let cypher = db
            .session()
            .query(&format!("MATCH (a:P) WHERE {filter} RETURN a.id"))
            .await?;
        let mut want: Vec<Vec<Value>> = cypher.rows().iter().map(|r| r.values().to_vec()).collect();
        want.sort_by_key(|r| format!("{r:?}"));
        let program = format!(
            "CREATE RULE r AS MATCH (a:P) YIELD KEY a QUERY r WHERE {filter} RETURN a.id AS c0"
        );
        assert_eq!(rows(&db, &program).await?, want, "{filter}");
    }
    // A power of a non-number is an error, not 0.
    let err = db
        .session()
        .locy(
            "CREATE RULE r AS MATCH (a:P) YIELD KEY a QUERY r WHERE 'x' ^ 2 = 0 RETURN a.id AS c0",
        )
        .await
        .expect_err("'x' ^ 2");
    assert!(err.to_string().contains("pow requires numeric"), "{err}");
    Ok(())
}

/// The `WHERE` of `DERIVE`, `EXPLAIN RULE` and `ABDUCE` reports a condition it
/// cannot evaluate. Each dropped the row instead: `DERIVE` wrote fewer facts,
/// `EXPLAIN` reported no match, and `ABDUCE` a conclusion that no longer held
/// (W6).
#[tokio::test]
async fn locy_facts_and_filters_command_where_errors_are_reported() -> Result<()> {
    let db = open().await?;
    let rule = "CREATE RULE r AS MATCH (a:P)-[:K]->(b:P) DERIVE (a)-[:LINKED]->(b) ";
    let rule_yield = "CREATE RULE r AS MATCH (a:P)-[:K]->(b:P) YIELD KEY a, KEY b ";
    for (program, command) in [
        (rule, "DERIVE r WHERE {f}"),
        (rule_yield, "EXPLAIN RULE r WHERE {f}"),
        (rule_yield, "ABDUCE NOT r WHERE {f}"),
    ] {
        for filter in ["a.id / 0 = 1", "a.id"] {
            let text = format!("{program}{}", command.replace("{f}", filter));
            let err = db
                .session()
                .locy(&text)
                .await
                .expect_err(&format!("`{text}` cannot be evaluated"));
            let msg = err.to_string();
            assert!(
                msg.contains("division by zero") || msg.contains("must be a boolean"),
                "{text}: {msg}"
            );
        }
        // Control: an evaluable filter runs.
        let text = format!("{program}{}", command.replace("{f}", "a.id = 1"));
        db.session()
            .locy(&text)
            .await
            .map_err(|e| anyhow::anyhow!("{text}: {e}"))?;
    }
    Ok(())
}

/// An ALONG rule's facts are per path (#159), recursive or not. Deduplicating a
/// non-recursive rule's rows (the "a fact is a row" fix above) collapsed two
/// parallel edges carrying the same value into one fact, so a downstream SUM
/// read 2.0 where the same rule with a recursive clause added reads 4.0.
#[tokio::test]
async fn locy_facts_and_filters_along_facts_are_per_path() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Q {id: 1}), (b:Q {id: 2}), (a)-[:E {q: 2}]->(b), (a)-[:E {q: 2}]->(b)")
        .await?;
    tx.commit().await?;
    let base = "CREATE RULE x AS MATCH (p:Q)-[e:E]->(c:Q) ALONG q = e.q YIELD KEY p, KEY c, q ";
    let recursive = "CREATE RULE x AS MATCH (p:Q)-[e:E]->(m:Q) WHERE m IS x TO c \
                     ALONG q = prev.q * e.q YIELD KEY p, KEY c, q ";
    let sum = "CREATE RULE s AS MATCH (p:Q) WHERE p IS x TO c FOLD t = SUM(q) YIELD KEY p, t \
               QUERY s RETURN t AS c0";
    for program in [format!("{base}{sum}"), format!("{base}{recursive}{sum}")] {
        assert_eq!(
            rows(&db, &program).await?,
            vec![vec![Value::Float(4.0)]],
            "{program}"
        );
        let result = db.session().locy(&program).await?;
        assert_eq!(
            result.derived_facts("x").map(Vec::len),
            Some(2),
            "{program}"
        );
    }
    Ok(())
}
