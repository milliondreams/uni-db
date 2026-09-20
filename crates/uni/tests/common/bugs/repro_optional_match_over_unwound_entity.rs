// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `OPTIONAL MATCH` dropped source rows when its source came through
//! `collect()` + `UNWIND`.
//!
//! An entity reaches an operator in one of two encodings. Natively it arrives
//! with a flat `{var}._vid` column beside it; round-tripped through a list it
//! arrives as a single encoded column named for the variable, with no `_vid`
//! anywhere. Both are ordinary query shapes.
//!
//! `collect_unmatched_optional_group_rows` groups source rows so that a source
//! which fans out to several traversal rows is only null-filled once, and it
//! built that key from columns whose name ends in `._vid`. On the second
//! encoding there are none, so every row keyed to the empty vector, the whole
//! batch became one group, and one matched row marked the group matched —
//! suppressing the NULL rows for every other source.
//!
//! The result was silent: `b` and `c` simply were not in the answer, which is
//! the one thing OPTIONAL MATCH promises cannot happen.
//!
//! The two arms are each other's control. They are the same question over the
//! same data, differing only in how the entity got there, so a disagreement is
//! the bug and neither arm alone would show it.

use uni_db::Uni;

async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'}), (:P {name:'c'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

async fn pairs(db: &Uni, query: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = db
        .session()
        .query(query)
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("nm").unwrap_or_default(),
                r.get::<String>("tn").unwrap_or_else(|_| "NULL".to_string()),
            )
        })
        .collect();
    out.sort();
    out
}

/// Every source row must survive the OPTIONAL MATCH, whichever encoding it
/// arrived in.
#[tokio::test]
async fn optional_match_keeps_every_source_row_for_both_entity_encodings() {
    let db = fixture().await;

    let native = pairs(
        &db,
        "MATCH (n:P) OPTIONAL MATCH (n)-[:KNOWS]->(t) RETURN n.name AS nm, t.name AS tn",
    )
    .await;

    let unwound = pairs(
        &db,
        "MATCH (m:P) WITH collect(m) AS ms UNWIND ms AS n \
         OPTIONAL MATCH (n)-[:KNOWS]->(t) RETURN n.name AS nm, t.name AS tn",
    )
    .await;

    let expected = vec![
        ("a".to_string(), "b".to_string()),
        ("b".to_string(), "NULL".to_string()),
        ("c".to_string(), "NULL".to_string()),
    ];
    assert_eq!(
        native, expected,
        "control: the natively-bound arm is already wrong, so the comparison \
         below would not mean what it claims"
    );
    assert_eq!(
        unwound, expected,
        "OPTIONAL MATCH dropped source rows when the entity arrived through \
         collect()+UNWIND"
    );
}

/// The unwound source really does carry every row, so a shortfall above is the
/// OPTIONAL MATCH losing them rather than the source never having them.
#[tokio::test]
async fn the_unwound_source_carries_every_row() {
    let db = fixture().await;
    let rows = db
        .session()
        .query("MATCH (m:P) WITH collect(m) AS ms UNWIND ms AS n RETURN n.name AS nm")
        .await
        .unwrap();
    let mut names: Vec<String> = rows
        .rows()
        .iter()
        .map(|r| r.get::<String>("nm").unwrap())
        .collect();
    names.sort();
    assert_eq!(names, vec!["a", "b", "c"]);
}
