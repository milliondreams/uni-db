// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for `api/mod.rs:2818` (Tier 1.6).
//!
//! `Uni::get_edge_type_info` counted edges by interpolating the type name into
//! Cypher **unquoted**:
//!
//! ```ignore
//! let query = format!("MATCH ()-[r:{}]->() RETURN count(r) AS cnt", name);
//! ```
//!
//! and then swallowed the result:
//!
//! ```ignore
//! Err(_) => 0,
//! ```
//!
//! Cypher's unquoted relationship-type grammar is `[A-Za-z_][A-Za-z0-9_]*`
//! (`identifier_or_keyword`, `cypher.pest`), but `validate_schema_element_name`
//! accepts far more: punctuation such as `-`, `.` (documented as supported for
//! qualified names), leading digits, and every non-ASCII character. For any of
//! those the generated query is a *parse error*, which `Err(_) => 0` converted
//! into a confident "this edge type has 0 edges".
//!
//! `count` is the only field of `EdgeTypeInfo` derived from the query — every
//! other field comes from schema metadata — so the failure surfaced as a single
//! wrong integer inside an otherwise-correct struct. There was no test coverage
//! at all (`get_label_info` has ~25 call sites; this had none).

use uni_db::Uni;

/// Declare `edge_type` between `Thing` nodes and create `n` edges of it.
async fn db_with_edges(edge_type: &str, n: usize) -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema().label("Thing").apply().await.unwrap();
    db.schema()
        .edge_type(edge_type, &["Thing"], &["Thing"])
        .apply()
        .await
        .unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    for i in 0..n {
        // The type is backtick-quoted here too — the same reason the production
        // code has to quote it.
        tx.execute(&format!(
            "CREATE (:Thing {{k: {i}}})-[:`{edge_type}`]->(:Thing {{k: {}}})",
            i + 1000
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    db
}

/// A hyphenated edge type is accepted by schema validation and breaks the
/// unquoted interpolation.
#[tokio::test]
async fn hyphenated_edge_type_reports_its_real_count() {
    let db = db_with_edges("HAS-PART", 3).await;

    let info = db
        .get_edge_type_info("HAS-PART")
        .await
        .expect("a valid schema name must not error")
        .expect("the edge type is declared");

    assert_eq!(
        info.count, 3,
        "before the fix the generated Cypher failed to parse and `Err(_) => 0` \
         reported an empty edge type"
    );
}

/// Non-ASCII is accepted by validation too — `cypher.pest` matches
/// `ASCII_ALPHA` only, so even a leading letter does not help.
#[tokio::test]
async fn non_ascii_edge_type_reports_its_real_count() {
    let db = db_with_edges("CONNAÎT", 2).await;

    let info = db
        .get_edge_type_info("CONNAÎT")
        .await
        .expect("a valid schema name must not error")
        .expect("the edge type is declared");

    assert_eq!(info.count, 2);
}

/// Control: a plain identifier worked before and must keep working.
///
/// Without this, a fix that broke counting entirely would still satisfy the two
/// tests above only if it happened to return the right number — this pins that
/// the harness really does write edges, so a `0` above is the escaping bug and
/// not an empty graph.
#[tokio::test]
async fn plain_identifier_edge_type_still_counts() {
    let db = db_with_edges("KNOWS", 3).await;

    let info = db
        .get_edge_type_info("KNOWS")
        .await
        .unwrap()
        .expect("the edge type is declared");

    assert_eq!(info.count, 3);
}

/// An unknown edge type is still `Ok(None)`, not an error.
#[tokio::test]
async fn unknown_edge_type_is_still_none() {
    let db = db_with_edges("KNOWS", 1).await;
    assert!(db.get_edge_type_info("NOPE").await.unwrap().is_none());
}

// ── Sibling defect: get_label_info counted superseded MVCC rows ──────────

/// `get_label_info` counted rows with `backend.count_rows(table, None)`, which
/// reads **flushed storage only** and cannot see the L0 buffers.
///
/// So a label whose rows have not yet been flushed reported `count: 0` — the
/// same silent-wrong-answer shape as the edge-type defect above, reached by a
/// different route. A Python test (`test_async_e2e_schema.py:379`) had already
/// been weakened to stop asserting the value, with the comment "count may be 0
/// if get_label_info doesn't reflect recent inserts"; that is this bug, recorded
/// as a tolerance rather than fixed.
///
/// **Two corrections to how this was originally diagnosed**, both established by
/// measuring rather than reading:
///
/// * It is *not* an MVCC overcount. The per-label vertex tables are written
///   through `merge_insert` and `delete_rows` — upsert and physical delete — so
///   `count_rows` does not accumulate superseded versions or tombstones. That
///   inference came from the *main* `vertices`/`edges` tables, which are append
///   only; the per-label tables are not. Flushing first makes this test pass
///   against the unfixed code.
/// * It is *not* a revert of issue #115. That fix moved the count off the
///   raw-dataset `open_raw()` path, whose `.lance` URI was wrong so it reported
///   0 for *flushed* tables. Cypher was never involved, and the edge-type
///   sibling has counted via Cypher throughout.
///
/// The update and delete below are retained as an inverse guard: the Cypher
/// count must also be right *after* mutations, not merely non-zero.
#[tokio::test]
async fn label_count_reflects_unflushed_writes() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema().label("Person").apply().await.unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    for i in 0..5 {
        tx.execute(&format!("CREATE (:Person {{k: {i}, tag: 'a'}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    // Two updates (each appends a superseded row) and one delete (a tombstone).
    let tx = session.tx().await.unwrap();
    tx.execute("MATCH (p:Person) WHERE p.k < 2 SET p.tag = 'b'")
        .await
        .unwrap();
    tx.execute("MATCH (p:Person) WHERE p.k = 4 DELETE p")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let live: usize = session
        .query("MATCH (p:Person) RETURN count(p) AS cnt")
        .await
        .unwrap()
        .rows()
        .first()
        .and_then(|r| r.get::<i64>("cnt").ok())
        .unwrap() as usize;
    assert_eq!(live, 4, "sanity: 5 created, 1 deleted");

    let info = db
        .get_label_info("Person")
        .await
        .unwrap()
        .expect("the label is declared");

    assert_eq!(
        info.count, live,
        "get_label_info must report live cardinality, not raw MVCC row count"
    );
}
