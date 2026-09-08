// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! "Can this query's pattern match anything in this fixture?" (#205)
//!
//! # Why this exists
//!
//! `ldbc_ic14_plans_and_executes` passed for weeks while never running the code
//! it covered. Its query text was the real IC14; its fixture created two
//! `Person`s and one `KNOWS` edge, and IC14's weight term is a pattern
//! comprehension over `Comment`, `Post`, `HAS_CREATOR` and `REPLY_OF` — none of
//! which existed. The comprehension matched nothing, `reduce` never evaluated
//! its body, `startNode(r)` was never called, and the assertions (`!rows
//! .is_empty()` and the id list) all passed honestly.
//!
//! Nothing about the test looked wrong, because the reduction was in the
//! **data** and the data is not visible beside the query. This module makes the
//! dependency statable in one line, as issue #205 asked.
//!
//! ```ignore
//! fixture_shape::assert_pattern_reachable(&session, query).await;
//! ```
//!
//! [`uni_cypher::pattern_requirements`] extracts what the query's `MATCH`
//! patterns and pattern comprehensions need; this side counts rows for each and
//! panics naming the empty ones.
//!
//! # This is a necessary condition, not a sufficient one
//!
//! A clean pass says the **ingredients** exist. It does not say they **compose**
//! into a match: every label and edge type can be non-empty while no path
//! threads them together. The sufficient condition is asserting the value the
//! code under test *computes*, and this module is not a substitute for that. A
//! test that calls this instead of checking its result is still the defect #205
//! describes — the IC14 test carries both, and that pairing is the pattern to
//! copy.
//!
//! # Relationship to `plan_shape`
//!
//! [`super::plan_shape`] answers the same species of question one layer up:
//! *did the operator run?* rather than *could the data have reached it?* Both
//! exist because a result-only assertion cannot distinguish "the code ran and
//! was right" from "the code never ran".

use uni_cypher::pattern_requirements::{self, Requirement};
use uni_db::Session;

/// Rows matching a single label.
async fn label_count(session: &Session, label: &str) -> Option<i64> {
    count(
        session,
        &format!("MATCH (x:`{label}`) RETURN count(x) AS c"),
    )
    .await
}

/// Edges of a single type.
async fn edge_count(session: &Session, edge_type: &str) -> Option<i64> {
    count(
        session,
        &format!("MATCH ()-[r:`{edge_type}`]->() RETURN count(r) AS c"),
    )
    .await
}

/// `None` only when the probe query itself failed.
///
/// Note an *undeclared* label or edge type does **not** land here: measured, a
/// `MATCH (n:Nonexistent)` counts zero rather than erroring, so an empty table
/// and a misspelled name are indistinguishable from this side. The panic text
/// says so rather than claiming a distinction the probe cannot make.
async fn count(session: &Session, probe: &str) -> Option<i64> {
    let r = session.query(probe).await.ok()?;
    r.rows().first()?.get::<i64>("c").ok()
}

/// Asserts every entity type `query`'s patterns require is present in the
/// fixture behind `session`.
///
/// # Panics
///
/// Panics if `query` does not parse, if any required label or edge type is
/// empty or undeclared, or if `query` turns out to require nothing at all —
/// see [`assert_requires_something`] for why that last case is a failure rather
/// than a pass.
pub async fn assert_pattern_reachable(session: &Session, query: &str) {
    let reqs = pattern_requirements::of(query)
        .unwrap_or_else(|e| panic!("fixture_shape: `{query}` does not parse: {e}"));

    assert_requires_something(&reqs, query);

    let mut missing: Vec<String> = Vec::new();
    for req in &reqs {
        if let Some(note) = unmet(session, req).await {
            missing.push(note);
        }
    }

    assert!(
        missing.is_empty(),
        "fixture_shape: this query's pattern cannot match — the fixture is \
         missing:\n  {}\n\nQuery:\n{query}\n\nA pattern that matches nothing \
         still returns cleanly: a comprehension over an empty match yields an \
         empty list and `reduce` returns its seed, so every assertion that \
         avoids the computed value passes without the code under test ever \
         running. That is issue #205.",
        missing.join("\n  ")
    );
}

/// Guards the guard.
///
/// A query whose requirements are empty — every pattern optional, or the whole
/// thing driven by `CREATE` — makes [`assert_pattern_reachable`] a no-op that
/// reads like coverage. Failing here keeps the helper from becoming an instance
/// of the class it exists to catch.
fn assert_requires_something(reqs: &[Requirement], query: &str) {
    assert!(
        !reqs.is_empty(),
        "fixture_shape: `{query}` has no pattern requirements, so this \
         assertion proves nothing. Requirements come from non-optional MATCH \
         patterns and pattern comprehensions only — CREATE, MERGE, OPTIONAL \
         MATCH, EXISTS/COUNT/COLLECT subqueries and `*0..` hops are all \
         excluded because an empty match is legitimate for each. Either assert \
         against the query that actually depends on the data, or drop this call."
    );
}

/// `None` when the requirement is met; otherwise the line to report.
async fn unmet(session: &Session, req: &Requirement) -> Option<String> {
    let is_label = req.is_label();
    let names = req.names();
    let kind = if is_label { "label" } else { "edge type" };

    let mut counts = Vec::with_capacity(names.len());
    for name in names {
        let n = if is_label {
            label_count(session, name).await
        } else {
            edge_count(session, name).await
        };
        counts.push((name.clone(), n));
    }

    // A disjunction (`:A|B`) is satisfied by any one arm.
    if counts.iter().any(|(_, c)| c.is_some_and(|c| c > 0)) {
        return None;
    }

    let detail = counts
        .iter()
        .map(|(name, c)| match c {
            Some(0) => format!("`{name}` (0 rows — the table is empty, or the name is misspelled and no such type is declared)"),
            Some(n) => format!("`{name}` ({n} rows)"),
            None => format!("`{name}` (the count probe itself failed)"),
        })
        .collect::<Vec<_>>()
        .join(" or ");

    Some(if names.len() > 1 {
        format!("at least one of these {kind}s: {detail}")
    } else {
        format!("{kind} {detail}")
    })
}

// ---------------------------------------------------------------------------
// The guard's own guard.
//
// A helper that exists to stop tests passing vacuously has to be shown to fail
// on the shape it targets. These reconstruct the pre-#205 IC14 fixture — two
// `Person`s and one `KNOWS` edge — and run the real weight query against it.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use uni_db::{DataType, Uni};

    /// IC14's weight term. Its pattern comprehension needs `Comment`, `Post`,
    /// `HAS_CREATOR` and `REPLY_OF`; its `MATCH` needs only `Person`, and the
    /// `[:KNOWS*0..]` hop needs no edge at all.
    const IC14_WEIGHT: &str = "
MATCH path = allShortestPaths((person1:Person { id: 1 })-[:KNOWS*0..]-(person2:Person { id: 2 }))
WITH path, relationships(path) as rels_in_path
WITH [r in rels_in_path |
        reduce(w=0.0, v in [
            (a:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(b:Person)
            WHERE a.id = startNode(r).id and b.id = endNode(r).id
            | 1.0] | w+v)
     ] as weight1
RETURN weight1";

    /// Declares every entity type IC14 names, then populates only the ones the
    /// original fixture had. Declaring the rest is deliberate: it proves the
    /// helper fails on *empty* tables, not merely on undeclared names, which is
    /// the harder and more realistic case.
    async fn pre_205_fixture() -> Uni {
        let db = Uni::in_memory().build().await.unwrap();
        db.schema()
            .label("Person")
            .property("id", DataType::Int)
            .label("Comment")
            .property("id", DataType::Int)
            .label("Post")
            .property("id", DataType::Int)
            .apply()
            .await
            .unwrap();
        db.schema()
            .edge_type("KNOWS", &["Person"], &["Person"])
            .edge_type("REPLY_OF", &["Comment"], &["Post"])
            .edge_type("HAS_CREATOR", &["Comment", "Post"], &["Person"])
            .apply()
            .await
            .unwrap();

        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (:Person {id:1}), (:Person {id:2})")
            .await
            .unwrap();
        tx.execute("MATCH (a:Person {id:1}), (b:Person {id:2}) CREATE (a)-[:KNOWS]->(b)")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        db
    }

    /// The control. On the fixture that let the IC14 test pass for weeks, the
    /// helper fails and names what is missing.
    #[tokio::test]
    #[should_panic(expected = "pattern cannot match")]
    async fn the_pre_205_ic14_fixture_is_rejected() {
        let db = pre_205_fixture().await;
        assert_pattern_reachable(&db.session(), IC14_WEIGHT).await;
    }

    /// The positive twin. Adding the missing rows — and nothing else — makes it
    /// pass, so the failure above is attributable to the empty tables rather
    /// than to anything else about the query.
    #[tokio::test]
    async fn adding_the_missing_rows_satisfies_it() {
        let db = pre_205_fixture().await;
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (:Comment {id:10}), (:Post {id:20})")
            .await
            .unwrap();
        tx.execute("MATCH (c:Comment {id:10}), (p:Person {id:1}) CREATE (c)-[:HAS_CREATOR]->(p)")
            .await
            .unwrap();
        tx.execute("MATCH (c:Comment {id:10}), (p:Post {id:20}) CREATE (c)-[:REPLY_OF]->(p)")
            .await
            .unwrap();
        tx.commit().await.unwrap();

        assert_pattern_reachable(&db.session(), IC14_WEIGHT).await;
    }

    /// A query that requires nothing must not read as coverage.
    #[tokio::test]
    #[should_panic(expected = "no pattern requirements")]
    async fn a_query_that_requires_nothing_is_rejected() {
        let db = pre_205_fixture().await;
        assert_pattern_reachable(&db.session(), "CREATE (:Person {id:99})").await;
    }

    /// A misspelled label is caught, and the message does not overclaim.
    ///
    /// An earlier version of this helper reported such a name as "not declared
    /// in the schema", on the assumption that the count query would error.
    /// Measured, it does not — `MATCH (n:Nonexistent)` returns zero rows — so
    /// from the probe's side an empty table and a typo are the same
    /// observation, and the panic text now says exactly that.
    #[tokio::test]
    #[should_panic(expected = "the name is misspelled")]
    async fn a_misspelled_label_is_caught_without_overclaiming_why() {
        let db = pre_205_fixture().await;
        assert_pattern_reachable(&db.session(), "MATCH (n:Nonexistent) RETURN n").await;
    }
}
