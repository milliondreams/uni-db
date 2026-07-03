// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end guardrails for the issue #134 follow-ups: DISTINCT-on-identity and
//! WITH-passthrough projection narrowing.
//!
//! Every `Doc` carries narrow properties (`title`, `author`, `x`) plus an unread
//! wide `colbert` multi-vector column. The queries here never reference
//! `colbert`, so the planner narrows the scan — but the results must be
//! byte-identical to a run without the wide column. Crucially these assert
//! actual property *values*: a narrowing bug would surface as a silent NULL,
//! not an error.

use uni_db::{DataType, Uni};

async fn setup() -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("author", DataType::String)
        .property("x", DataType::Int)
        // Unread wide column: a variable-count multi-vector per row.
        .property(
            "colbert",
            DataType::List(Box::new(DataType::Vector { dimensions: 4 })),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    // A wide colbert literal the queries never touch.
    let cb = "[[0.1,0.2,0.3,0.4],[0.5,0.6,0.7,0.8],[0.9,0.1,0.2,0.3]]";
    for (t, a, x) in [
        ("Alpha", "Ann", 10),
        ("Beta", "Bob", 20),
        ("Gamma", "Cara", 30),
    ] {
        tx.execute(&format!(
            "CREATE (:Doc {{title: '{t}', author: '{a}', x: {x}, colbert: {cb}}})"
        ))
        .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Fetch a typed column value from a specific result row.
fn col<T: uni_db::FromValue>(r: &uni_db::QueryResult, row: usize, name: &str) -> T {
    r.rows()[row].get::<T>(name).unwrap()
}

#[tokio::test]
async fn with_passthrough_same_name_returns_real_values() -> anyhow::Result<()> {
    let db = setup().await?;
    // Forward n, filter on x, return a narrow property. n is narrowed to {x,title}
    // — the wide colbert column must not be read, and title must be the real value.
    let r = db
        .session()
        .query(
            "MATCH (n:Doc) WITH n WHERE n.x > 15 \
             RETURN n.title AS title ORDER BY title",
        )
        .await?;
    let titles: Vec<String> = r
        .rows()
        .iter()
        .map(|row| row.get("title").unwrap())
        .collect();
    assert_eq!(titles, vec!["Beta".to_string(), "Gamma".to_string()]);
    Ok(())
}

#[tokio::test]
async fn with_rename_property_access_is_not_null() -> anyhow::Result<()> {
    let db = setup().await?;
    // The core silent-NULL trap: WITH n AS m RETURN m.title. The alias's
    // accessed property must be folded onto the narrowed source.
    let r = db
        .session()
        .query(
            "MATCH (n:Doc) WITH n AS m \
             RETURN m.title AS title, m.author AS author ORDER BY title",
        )
        .await?;
    assert_eq!(r.len(), 3);
    assert_eq!(col::<String>(&r, 0, "title"), "Alpha");
    assert_eq!(col::<String>(&r, 0, "author"), "Ann");
    assert_eq!(col::<String>(&r, 2, "title"), "Gamma");
    assert_eq!(col::<String>(&r, 2, "author"), "Cara");
    Ok(())
}

#[tokio::test]
async fn with_multi_alias_same_source_resolves_all() -> anyhow::Result<()> {
    let db = setup().await?;
    // Two aliases of the same source, each accessing a different property —
    // both must fold onto n so neither becomes NULL.
    let r = db
        .session()
        .query(
            "MATCH (n:Doc) WITH n AS a, n AS b \
             RETURN a.title AS t, b.author AS au ORDER BY t",
        )
        .await?;
    assert_eq!(col::<String>(&r, 0, "t"), "Alpha");
    assert_eq!(col::<String>(&r, 0, "au"), "Ann");
    Ok(())
}

#[tokio::test]
async fn optional_match_with_forward_nullability() -> anyhow::Result<()> {
    let db = setup().await?;
    // Forwarded optional-unmatched node yields NULL for the property, matched
    // ones the real value.
    let r = db
        .session()
        .query(
            "OPTIONAL MATCH (n:Doc {title: 'Beta'}) WITH n \
             RETURN n.author AS author",
        )
        .await?;
    assert_eq!(col::<String>(&r, 0, "author"), "Bob");
    Ok(())
}

#[tokio::test]
async fn with_grouping_key_and_aggregate() -> anyhow::Result<()> {
    let db = setup().await?;
    let r = db
        .session()
        .query(
            "MATCH (n:Doc) WITH n, count(*) AS c \
             RETURN n.title AS title, c ORDER BY title",
        )
        .await?;
    assert_eq!(col::<String>(&r, 0, "title"), "Alpha");
    assert_eq!(col::<i64>(&r, 0, "c"), 1);
    Ok(())
}

#[tokio::test]
async fn count_distinct_entity_over_wide_column() -> anyhow::Result<()> {
    let db = setup().await?;
    // Same target reached via multiple paths must count once (by identity),
    // and the wide colbert column must not be needed to compute distinctness.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Doc {title:'Alpha'}), (g:Doc {title:'Gamma'}) CREATE (a)-[:CITES]->(g)")
        .await?;
    tx.execute("MATCH (b:Doc {title:'Beta'}), (g:Doc {title:'Gamma'}) CREATE (b)-[:CITES]->(g)")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (:Doc)-[:CITES]->(n:Doc) RETURN count(DISTINCT n) AS c")
        .await?;
    // Gamma is cited twice but is one distinct node.
    assert_eq!(col::<i64>(&r, 0, "c"), 1);

    // collect(DISTINCT n) must dedup by identity → one element for the single
    // distinct target. `size(...)` keeps the assertion an integer.
    let r2 = db
        .session()
        .query("MATCH (:Doc)-[:CITES]->(n:Doc) RETURN size(collect(DISTINCT n)) AS c")
        .await?;
    assert_eq!(col::<i64>(&r2, 0, "c"), 1);
    Ok(())
}

#[tokio::test]
async fn narrowing_matches_unnarrowed_results() -> anyhow::Result<()> {
    // Correctness under narrowing: identical results whether or not the wide
    // column exists on the row.
    let with_wide = setup().await?;
    let narrow = {
        let db = Uni::temporary().build().await?;
        db.schema()
            .label("Doc")
            .property("title", DataType::String)
            .property("author", DataType::String)
            .property("x", DataType::Int)
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        for (t, a, x) in [
            ("Alpha", "Ann", 10),
            ("Beta", "Bob", 20),
            ("Gamma", "Cara", 30),
        ] {
            tx.execute(&format!(
                "CREATE (:Doc {{title: '{t}', author: '{a}', x: {x}}})"
            ))
            .await?;
        }
        tx.commit().await?;
        db.flush().await?;
        db
    };

    let q = "MATCH (n:Doc) WITH n AS m WHERE m.x > 5 RETURN m.title AS t ORDER BY t";
    let a = with_wide.session().query(q).await?;
    let b = narrow.session().query(q).await?;
    let ta: Vec<String> = a.rows().iter().map(|r| r.get("t").unwrap()).collect();
    let tb: Vec<String> = b.rows().iter().map(|r| r.get("t").unwrap()).collect();
    assert_eq!(ta, tb);
    assert_eq!(ta, vec!["Alpha", "Beta", "Gamma"]);
    Ok(())
}
