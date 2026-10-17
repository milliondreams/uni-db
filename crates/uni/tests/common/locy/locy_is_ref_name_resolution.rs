// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #300: a Locy clause's answer must not depend on how its `IS`
//! conjuncts are ordered or what its variables are called.
//!
//! Each case states a rule body as a list of `WHERE` conjuncts and the answer
//! computed by hand from the fixture. The runner evaluates the body in every
//! permutation of those conjuncts (Heap's algorithm), joined with `", "` and
//! with `" AND "` (the grammar accepts both everywhere these cases use them),
//! before and after `db.flush()`. Every disagreement is collected, then the
//! test fails once with a report naming the case, the order, the joiner, the
//! flush state, and the expected and returned rows.
//!
//! The answers follow the binding semantics decided for #300:
//! - binding is order-independent: one binder per variable, every other
//!   occurrence of it is a test, and an unbound subject at any position binds;
//! - a referenced rule's KEY columns are never exposed by name; a value column
//!   exposed by exactly one reference is readable by its bare name;
//! - reading a value column exposed by two references is a compile error
//!   (`ambiguous`), while exposing it without reading it is legal;
//! - `NOT x IS r` is legal before the conjunct that binds `x`;
//! - a position's kind (node or scalar) follows the referenced rule's yield
//!   expression, never "it is a KEY".
//!
//! The Cypher cases (Q4–Q6) were measured correct; they are guards, not repros.
//!
//! Cells are decoded so the expectations read as names: a node becomes `@`
//! followed by its `uid`/`name` property, an integer its digits, a float its
//! `Debug` form (`60.0`), a string itself. No fixture string looks like a
//! number, so a node or a string where an integer is expected cannot match.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(is_ref_name_resolution)'

// Rust guideline compliant

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::Duration;

use anyhow::{Result, ensure};
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{DataType, Uni, Value};

/// A row as decoded cells, in the case's column order.
type Row = Vec<String>;

/// What a case must produce in every order, joiner and flush state.
enum Expect {
    /// Exactly these rows, each exactly once.
    Rows(BTreeSet<Row>),
    /// A `LocyCompileError` whose message mentions `ambiguous` (any case) and
    /// this column name.
    Compile(&'static str),
}

/// One rule body under test, with its expected answer.
struct Case {
    /// Label used in failure reports.
    id: String,
    /// Rules the body references, as complete `CREATE RULE` statements.
    rules: String,
    /// `CREATE RULE <name> AS MATCH ...`, or empty for a body-less query.
    head: String,
    /// `WHERE` conjuncts, permuted by the runner.
    conjuncts: Vec<String>,
    /// Text between the `WHERE` conjuncts and the `YIELD` (FOLD, ALONG, ...).
    tail: String,
    /// `YIELD ...` plus the `QUERY` that reads the rule.
    query: String,
    /// The `QUERY RETURN` columns, in the order the expected cells use.
    cols: Vec<String>,
    /// The hand-computed answer.
    expect: Expect,
}

/// Builds a [`Case`] from string slices.
#[expect(clippy::too_many_arguments, reason = "a table row, read positionally")]
fn case(
    id: &str,
    rules: &str,
    head: &str,
    conjuncts: &[&str],
    tail: &str,
    query: &str,
    cols: &[&str],
    expect: Expect,
) -> Case {
    Case {
        id: id.to_owned(),
        rules: rules.to_owned(),
        head: head.to_owned(),
        conjuncts: conjuncts.iter().map(|c| (*c).to_owned()).collect(),
        tail: tail.to_owned(),
        query: query.to_owned(),
        cols: cols.iter().map(|c| (*c).to_owned()).collect(),
        expect,
    }
}

/// An expected row set from string-slice rows.
fn rows(rs: &[&[&str]]) -> Expect {
    Expect::Rows(
        rs.iter()
            .map(|r| r.iter().map(|c| (*c).to_owned()).collect())
            .collect(),
    )
}

fn config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

/// Every permutation of `0..n`, by Heap's algorithm (iterative form).
fn permutations(n: usize) -> Vec<Vec<usize>> {
    let mut a: Vec<usize> = (0..n).collect();
    let mut out = vec![a.clone()];
    let mut c = vec![0usize; n];
    let mut i = 1;
    while i < n {
        if c[i] < i {
            if i % 2 == 0 {
                a.swap(0, i);
            } else {
                a.swap(c[i], i);
            }
            out.push(a.clone());
            c[i] += 1;
            i = 1;
        } else {
            c[i] = 0;
            i += 1;
        }
    }
    out
}

/// The `WHERE` bodies for every permutation and joiner, deduplicated.
fn orderings(conjuncts: &[String]) -> Vec<String> {
    if conjuncts.is_empty() {
        return vec![String::new()];
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for perm in permutations(conjuncts.len()) {
        for joiner in [", ", " AND "] {
            let body = perm
                .iter()
                .map(|&i| conjuncts[i].as_str())
                .collect::<Vec<_>>()
                .join(joiner);
            if seen.insert(body.clone()) {
                out.push(body);
            }
        }
    }
    out
}

/// Maps a VID to the `uid`/`name` of the node that holds it.
type Names = BTreeMap<u64, String>;

/// Reads every node's VID and name, so a node with no hydrated properties
/// still decodes to its name.
async fn names(db: &Uni, query: &str) -> Result<Names> {
    let result = db.session().query(query).await?;
    let mut out = Names::new();
    for row in result.rows() {
        let (Some(Value::Int(vid)), Some(Value::String(name))) =
            (row.value("vid"), row.value("name"))
        else {
            anyhow::bail!("names query returned {:?}", row.value("vid"));
        };
        out.insert(u64::try_from(*vid)?, name.clone());
    }
    Ok(out)
}

/// Decodes one cell; see the module docs for the encoding.
fn decode(v: &Value, names: &Names) -> String {
    let node_name = |props: &std::collections::HashMap<String, Value>, vid: Option<u64>| {
        for key in ["uid", "name"] {
            if let Some(Value::String(s)) = props.get(key) {
                return format!("@{s}");
            }
        }
        match vid.and_then(|v| names.get(&v)) {
            Some(n) => format!("@{n}"),
            None => format!("@vid{vid:?}"),
        }
    };
    match v {
        Value::Null => "null".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{f:?}"),
        Value::String(s) => s.clone(),
        Value::Node(n) => node_name(&n.properties, Some(n.vid.as_u64())),
        Value::Map(m) if m.contains_key("_vid") => {
            let vid = match m.get("_vid") {
                Some(Value::Int(i)) => u64::try_from(*i).ok(),
                _ => None,
            };
            node_name(m, vid)
        }
        Value::List(items) => {
            let cells: Vec<String> = items.iter().map(|i| decode(i, names)).collect();
            format!("[{}]", cells.join(","))
        }
        other => format!("{other:?}"),
    }
}

/// The outcome of one program: decoded rows (with duplicates kept), or the
/// error message.
type Outcome = std::result::Result<Vec<Row>, String>;

async fn run_query(db: &Uni, program: &str, cols: &[String], names: &Names) -> Outcome {
    let result = db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await
        .map_err(|e| format!("{e:#}"))?;
    let fact_rows = result
        .command_results
        .iter()
        .find_map(|c| match c {
            CommandResult::Query(rows) => Some(rows),
            _ => None,
        })
        .ok_or_else(|| "program produced no QUERY result".to_owned())?;
    Ok(fact_rows
        .iter()
        .map(|row| {
            cols.iter()
                .map(|c| {
                    row.get(c)
                        .map_or_else(|| "<missing>".to_owned(), |v| decode(v, names))
                })
                .collect()
        })
        .collect())
}

fn program_text(case: &Case, body: &str) -> String {
    if case.head.is_empty() {
        return format!("{}{}", case.rules, case.query);
    }
    let mut p = format!("{}{}", case.rules, case.head);
    if !body.is_empty() {
        let _ = write!(p, " WHERE {body}");
    }
    if !case.tail.is_empty() {
        let _ = write!(p, " {}", case.tail);
    }
    let _ = write!(p, " {}", case.query);
    p
}

/// Checks one outcome against the expectation; `None` when it matches.
fn judge(expect: &Expect, got: &Outcome) -> Option<String> {
    match (expect, got) {
        (Expect::Rows(want), Ok(rows)) => {
            let set: BTreeSet<Row> = rows.iter().cloned().collect();
            if set == *want && set.len() == rows.len() {
                None
            } else {
                Some(format!("expected {want:?}, got {rows:?}"))
            }
        }
        (Expect::Rows(want), Err(e)) => Some(format!("expected {want:?}, got ERR {e}")),
        (Expect::Compile(col), Err(e)) => {
            let lower = e.to_lowercase();
            if e.contains("LocyCompileError") && lower.contains("ambiguous") && e.contains(col) {
                None
            } else {
                Some(format!(
                    "expected LocyCompileError mentioning `ambiguous` and `{col}`, got ERR {e}"
                ))
            }
        }
        (Expect::Compile(col), Ok(rows)) => Some(format!(
            "expected LocyCompileError mentioning `ambiguous` and `{col}`, got rows {rows:?}"
        )),
    }
}

/// Runs every case in every order and joiner, before and after a flush, and
/// fails once listing every disagreement.
async fn check(db: &Uni, names: &Names, cases: &[Case]) -> Result<()> {
    let mut failures: Vec<String> = Vec::new();
    let mut failing_ids: BTreeSet<String> = BTreeSet::new();
    let mut runs = 0usize;
    for phase in ["pre-flush", "post-flush"] {
        if phase == "post-flush" {
            db.flush().await?;
        }
        for case in cases {
            for body in orderings(&case.conjuncts) {
                runs += 1;
                let program = program_text(case, &body);
                let got = run_query(db, &program, &case.cols, names).await;
                if let Some(why) = judge(&case.expect, &got) {
                    failing_ids.insert(case.id.clone());
                    failures.push(format!("[{phase}] {} WHERE {body}\n    {why}", case.id));
                }
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "{} of {runs} runs disagree; failing cases: {failing_ids:?}\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Fixture E: x -[OWNS 60]-> a -[OWNS 60]-> b
// ---------------------------------------------------------------------------

async fn ownership() -> Result<(Uni, Names)> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("E")
        .property("uid", DataType::String)
        .done()
        .edge_type("OWNS", &["E"], &["E"])
        .property_nullable("pct", DataType::Float64)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:E {uid: 'x'}), (:E {uid: 'a'}), (:E {uid: 'b'})")
        .await?;
    for (o, e) in [("x", "a"), ("a", "b")] {
        tx.execute_with(
            "MATCH (o:E {uid: $o}), (e:E {uid: $e}) CREATE (o)-[:OWNS {pct: 60.0}]->(e)",
        )
        .param("o", o)
        .param("e", e)
        .run()
        .await?;
    }
    tx.commit().await?;
    let names = names(&db, "MATCH (n:E) RETURN id(n) AS vid, n.uid AS name").await?;
    Ok((db, names))
}

/// `(o, e, pct)`: `(x, a, 60.0)`, `(a, b, 60.0)`.
const STAKE: &str = "CREATE RULE stake AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE r.pct IS NOT NULL \
    FOLD pct = MAX(r.pct) YIELD KEY o, KEY e, pct\n";
/// `(e, agg)`: `(x, 100.0)`.
const SEED: &str = "CREATE RULE seed AS MATCH (e:E) WHERE e.uid = 'x' YIELD KEY e, 100.0 AS agg\n";
/// `(s, agg)`: `(x, 100.0)` — no column shares a name with the reader's body.
const SEED2: &str =
    "CREATE RULE seed2 AS MATCH (s:E) WHERE s.uid = 'x' YIELD KEY s, 100.0 AS agg\n";
/// `(x, k)`: `(x, 1)`.
const ONE: &str = "CREATE RULE one AS MATCH (x:E) WHERE x.uid = 'x' YIELD KEY x, 1 AS k\n";
/// `(a, b)` where the value column `b` is a float: `(x, 60.0)`, `(a, 60.0)`.
const W: &str = "CREATE RULE w AS MATCH (a:E)-[r:OWNS]->(c:E) YIELD KEY a, r.pct AS b\n";
/// `(o, t)`: `(x, a)`, `(a, b)`.
const STAKE2: &str = "CREATE RULE stake2 AS MATCH (o:E)-[:OWNS]->(t:E) YIELD KEY o, KEY t\n";
/// `(b)`: `(a)`.
const BLOCKED1: &str = "CREATE RULE blocked1 AS MATCH (b:E) WHERE b.uid = 'a' YIELD KEY b\n";
/// `(p, c)`: `(x, a)`, `(a, b)`.
const LINK: &str = "CREATE RULE link AS MATCH (p:E)-[:OWNS]->(c:E) YIELD KEY p, KEY c\n";

/// P1 (#300): `o IS seed` plus `(o, e, pct) IS stake`. `o` must be `x`, so
/// the only stake is `(x, a, 60.0)`.
#[tokio::test]
async fn is_ref_name_resolution_p1() -> Result<()> {
    let (db, names) = ownership().await?;
    let rules = format!("{STAKE}{SEED}");
    let query = "YIELD KEY e, pct\nQUERY r RETURN e.uid AS e, pct";
    let cases = [
        case(
            "P1",
            &rules,
            "CREATE RULE r AS MATCH (o:E)",
            &["o IS seed", "(o, e, pct) IS stake"],
            "",
            query,
            &["e", "pct"],
            rows(&[&["a", "60.0"]]),
        ),
        case(
            "P1-matchE",
            &rules,
            "CREATE RULE r AS MATCH (o:E), (e:E)",
            &["o IS seed", "(o, e, pct) IS stake"],
            "",
            query,
            &["e", "pct"],
            rows(&[&["a", "60.0"]]),
        ),
    ];
    check(&db, &names, &cases).await
}

/// P1b: P1 where the first reference's KEY (`s`) shares no name with the body,
/// so a name lookup finds nothing instead of capturing.
#[tokio::test]
async fn is_ref_name_resolution_p1b() -> Result<()> {
    let (db, names) = ownership().await?;
    let cases = [case(
        "P1b",
        &format!("{STAKE}{SEED2}"),
        "CREATE RULE r AS MATCH (o:E)",
        &["o IS seed2", "(o, e, pct) IS stake"],
        "",
        "YIELD KEY e, pct\nQUERY r RETURN e.uid AS e, pct",
        &["e", "pct"],
        rows(&[&["a", "60.0"]]),
    )];
    check(&db, &names, &cases).await
}

/// P1c: P1 made recursive (the issue's form).
///
/// `blocked` has a base clause `(x, 100.0)` and a recursive clause: `e` is
/// blocked when the stakes `(o, e, pct)` held by blocked owners `o` sum
/// (`MSUM`, grouped by `e`) to at least 50. `REQUIRE` is definitional: it
/// filters each iteration's folded snapshot, which is what the self-reference
/// reads.
/// - iteration 0: `{x: 100.0}` from the base clause.
/// - iteration 1: `a`'s only owner is `x` (blocked) → MSUM = 60.0 ≥ 50 → `a`.
/// - iteration 2: `b`'s only owner is `a` (now blocked) → 60.0 → `b`.
/// - `x` has no owner, so it keeps only the base value; nothing else changes.
///
/// Fixpoint: `{(x, 100.0), (a, 60.0), (b, 60.0)}`.
#[tokio::test]
async fn is_ref_name_resolution_p1c() -> Result<()> {
    let (db, names) = ownership().await?;
    let blocked =
        "CREATE RULE blocked AS MATCH (e:E) WHERE e.uid = 'x' YIELD KEY e, 100.0 AS agg\n";
    let cases = [case(
        "P1c",
        &format!("{STAKE}{blocked}"),
        "CREATE RULE blocked AS MATCH (o:E)",
        &["o IS blocked", "(o, e, pct) IS stake"],
        "FOLD agg = MSUM(pct) REQUIRE agg >= 50.0",
        "YIELD KEY e, agg\nQUERY blocked RETURN e.uid AS e, agg",
        &["e", "agg"],
        rows(&[&["x", "100.0"], &["a", "60.0"], &["b", "60.0"]]),
    )];
    check(&db, &names, &cases).await
}

/// P2: a later reference's value column (`w.b`) is named like a MATCH
/// variable (`b`). The MATCH binds `b`; `w` must not replace it.
/// `one` holds only `x`; `x -> a`, so the answer is `(x, a)`.
#[tokio::test]
async fn is_ref_name_resolution_p2() -> Result<()> {
    let (db, names) = ownership().await?;
    let cases = [case(
        "P2",
        &format!("{ONE}{W}"),
        "CREATE RULE q AS MATCH (a:E)-[:OWNS]->(b:E)",
        &["a IS one", "a IS w"],
        "",
        "YIELD KEY a, KEY b\nQUERY q RETURN a.uid AS a, b.uid AS b",
        &["a", "b"],
        rows(&[&["x", "a"]]),
    )];
    check(&db, &names, &cases).await
}

/// P3: subjects named against the grain of the rule's KEY names. Binding is
/// positional: `(e, o) IS stake` puts the owner in `e` and the owned in `o`.
#[tokio::test]
async fn is_ref_name_resolution_p3() -> Result<()> {
    let (db, names) = ownership().await?;
    let q = "QUERY q RETURN e.uid AS e, o.uid AS o";
    let cols = ["e", "o"];
    let swapped = rows(&[&["x", "a"], &["a", "b"]]);
    let cases = [
        case(
            "P3",
            STAKE,
            "CREATE RULE q AS MATCH (n:E)",
            &["n.uid = 'x'", "(e, o) IS stake"],
            "",
            &format!("YIELD KEY e, KEY o\n{q}"),
            &cols,
            swapped,
        ),
        case(
            "P3-names-agree",
            STAKE,
            "CREATE RULE q AS MATCH (n:E)",
            &["n.uid = 'x'", "(o, e) IS stake"],
            "",
            &format!("YIELD KEY o, KEY e\n{q}"),
            &cols,
            rows(&[&["a", "x"], &["b", "a"]]),
        ),
        case(
            "P3-all-matched",
            STAKE,
            "CREATE RULE q AS MATCH (n:E), (e:E), (o:E)",
            &["n.uid = 'x'", "(e, o) IS stake"],
            "",
            &format!("YIELD KEY e, KEY o\n{q}"),
            &cols,
            rows(&[&["x", "a"], &["a", "b"]]),
        ),
        case(
            "P3-e-matched",
            STAKE,
            "CREATE RULE q AS MATCH (e:E)",
            &["(e, o) IS stake"],
            "",
            &format!("YIELD KEY e, KEY o\n{q}"),
            &cols,
            rows(&[&["x", "a"], &["a", "b"]]),
        ),
        case(
            "P3-unanchored",
            STAKE,
            "CREATE RULE q AS MATCH (n:E)",
            &["(e, o) IS stake"],
            "",
            &format!("YIELD KEY e, KEY o\n{q}"),
            &cols,
            rows(&[&["x", "a"], &["a", "b"]]),
        ),
    ];
    check(&db, &names, &cases).await
}

/// P4: a KEY-position subject (`agg`) named like an earlier reference's value
/// column (`seed.agg`). `stake2` binds `agg` to the owned node: `(x, a)`.
#[tokio::test]
async fn is_ref_name_resolution_p4() -> Result<()> {
    let (db, names) = ownership().await?;
    let cases = [case(
        "P4",
        &format!("{SEED}{STAKE2}"),
        "CREATE RULE q AS MATCH (o:E)",
        &["o IS seed", "(o, agg) IS stake2"],
        "",
        "YIELD KEY o, agg\nQUERY q RETURN o.uid AS o, agg.uid AS agg",
        &["o", "agg"],
        rows(&[&["x", "a"]]),
    )];
    check(&db, &names, &cases).await
}

/// P5: `NOT b IS blocked1` before the reference that binds `b`. Links are
/// `(x, a)` and `(a, b)`; `a` is blocked, so only `(a, b)` survives.
#[tokio::test]
async fn is_ref_name_resolution_p5() -> Result<()> {
    let (db, names) = ownership().await?;
    let cases = [case(
        "P5",
        &format!("{BLOCKED1}{LINK}"),
        "CREATE RULE q AS MATCH (a:E)",
        &["NOT b IS blocked1", "a IS link TO b"],
        "",
        "YIELD KEY a, KEY b\nQUERY q RETURN a.uid AS a, b.uid AS b",
        &["a", "b"],
        rows(&[&["a", "b"]]),
    )];
    check(&db, &names, &cases).await
}

/// P6: a WHERE filter on a reference's value column, before or after it.
/// Both stakes are 60.0.
#[tokio::test]
async fn is_ref_name_resolution_p6() -> Result<()> {
    let (db, names) = ownership().await?;
    let query = "YIELD KEY o, KEY e, pct\nQUERY q RETURN o.uid AS o, e.uid AS e, pct";
    let cases = [
        case(
            "P6 >59",
            STAKE,
            "CREATE RULE q AS MATCH (o:E)",
            &["(o, e, pct) IS stake", "pct > 59.0"],
            "",
            query,
            &["o", "e", "pct"],
            rows(&[&["x", "a", "60.0"], &["a", "b", "60.0"]]),
        ),
        case(
            "P6 >61",
            STAKE,
            "CREATE RULE q AS MATCH (o:E)",
            &["(o, e, pct) IS stake", "pct > 61.0"],
            "",
            query,
            &["o", "e", "pct"],
            rows(&[]),
        ),
    ];
    check(&db, &names, &cases).await
}

/// P7: a `TO` chain whose middle variable is bound by one reference and
/// tested by the other. `x -> a -> b` is the only two-hop chain.
#[tokio::test]
async fn is_ref_name_resolution_p7() -> Result<()> {
    let (db, names) = ownership().await?;
    let cases = [case(
        "P7",
        LINK,
        "CREATE RULE q AS MATCH (x:E)",
        &["x IS link TO mid", "mid IS link TO z"],
        "",
        "YIELD KEY x, KEY z\nQUERY q RETURN x.uid AS x, z.uid AS z",
        &["x", "z"],
        rows(&[&["x", "b"]]),
    )];
    check(&db, &names, &cases).await
}

/// P8: `EXPLAIN RULE` derives exactly the facts evaluation returns, in every
/// order. Agreement only: P1 checks the answer itself.
#[tokio::test]
async fn is_ref_name_resolution_p8() -> Result<()> {
    let (db, names) = ownership().await?;
    let conjuncts = ["o IS seed".to_owned(), "(o, e, pct) IS stake".to_owned()];
    let rules = format!("{STAKE}{SEED}");
    let mut failures = Vec::new();
    let mut non_empty_runs = 0usize;
    for phase in ["pre-flush", "post-flush"] {
        if phase == "post-flush" {
            db.flush().await?;
        }
        for body in orderings(&conjuncts) {
            let head =
                format!("{rules}CREATE RULE r AS MATCH (o:E) WHERE {body} YIELD KEY e, pct\n");
            let evaluated: BTreeSet<String> = match run_query(
                &db,
                &format!("{head}QUERY r RETURN e.uid AS e"),
                &["e".to_owned()],
                &names,
            )
            .await
            {
                Ok(rs) => rs.into_iter().flatten().collect(),
                Err(e) => {
                    failures.push(format!("[{phase}] WHERE {body}: query ERR {e}"));
                    continue;
                }
            };
            let mut explained = BTreeSet::new();
            for uid in ["x", "a", "b"] {
                let program = format!("{head}EXPLAIN RULE r WHERE e.uid = '{uid}'");
                match db
                    .session()
                    .locy_with(&program)
                    .with_config(config())
                    .run()
                    .await
                {
                    Ok(result) => {
                        let derived = result.command_results.iter().any(|c| {
                            matches!(c, CommandResult::Explain(node) if !node.children.is_empty())
                        });
                        if derived {
                            explained.insert(uid.to_owned());
                        }
                    }
                    Err(e) => {
                        failures.push(format!("[{phase}] WHERE {body} e={uid}: EXPLAIN ERR {e:#}"))
                    }
                }
            }
            if !evaluated.is_empty() {
                non_empty_runs += 1;
            }
            if explained != evaluated {
                failures.push(format!(
                    "[{phase}] WHERE {body}: EXPLAIN derives {explained:?}, evaluation returns {evaluated:?}"
                ));
            }
        }
    }
    ensure!(
        non_empty_runs > 0,
        "no order derived any fact; the agreement check is vacuous"
    );
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Fixture N/T: tags with scalar KEYs
// ---------------------------------------------------------------------------

async fn graph(setup: &str) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(setup).await?;
    tx.commit().await?;
    Ok(db)
}

/// `a` has tags `x`, `y` and is banned from `x`; `b` has tag `y` only. `b`
/// keeps the negation from being vacuous (without it, "nothing survives" is
/// the right answer to `a IS NOT banned`).
async fn tags() -> Result<(Uni, Names)> {
    let db = graph(
        "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (x:T {name: 'x'}), (y:T {name: 'y'}), \
         (a)-[:HAS]->(x), (a)-[:HAS]->(y), (a)-[:BAN]->(x), (b)-[:HAS]->(y)",
    )
    .await?;
    let names = names(&db, "MATCH (n) RETURN id(n) AS vid, n.name AS name").await?;
    Ok((db, names))
}

/// `(n, c)` with a scalar KEY `c`: `(a, 'x')`, `(a, 'y')`, `(b, 'y')`.
const TAG: &str = "CREATE RULE tag AS MATCH (n:N)-[:HAS]->(t:T) YIELD KEY n, KEY t.name AS c\n";
/// `(n, c)`: `(a, 'x')`.
const BANNED: &str =
    "CREATE RULE banned AS MATCH (n:N)-[:BAN]->(t:T) YIELD KEY n, KEY t.name AS c\n";

/// Q1: `a IS tag TO col` with a scalar KEY target, beside `a IS NOT banned TO
/// col` in either order. `(a, x)` is banned, leaving `(a, y)` and `(b, y)`.
#[tokio::test]
async fn is_ref_name_resolution_q1() -> Result<()> {
    let (db, names) = tags().await?;
    let both = format!("{TAG}{BANNED}");
    let head = "CREATE RULE pr AS MATCH (a:N)";
    let tagged = rows(&[&["a", "x"], &["a", "y"], &["b", "y"]]);
    let cases = [
        case(
            "Q1 KEY a",
            &both,
            head,
            &["a IS tag TO col", "a IS NOT banned TO col"],
            "",
            "YIELD KEY a\nQUERY pr RETURN a.name AS a",
            &["a"],
            rows(&[&["a"], &["b"]]),
        ),
        case(
            "Q1 KEY a, col",
            &both,
            head,
            &["a IS tag TO col", "a IS NOT banned TO col"],
            "",
            "YIELD KEY a, col\nQUERY pr RETURN a.name AS a, col",
            &["a", "col"],
            rows(&[&["a", "y"], &["b", "y"]]),
        ),
        case(
            "Q1 TO c, NOT banned TO c",
            &both,
            head,
            &["a IS tag TO c", "a IS NOT banned TO c"],
            "",
            "YIELD KEY a, c\nQUERY pr RETURN a.name AS a, c",
            &["a", "c"],
            rows(&[&["a", "y"], &["b", "y"]]),
        ),
        case(
            "Q1 tag rows",
            TAG,
            "",
            &[],
            "",
            "QUERY tag RETURN n.name AS n, c",
            &["n", "c"],
            tagged,
        ),
        case(
            "Q1 banned rows",
            BANNED,
            "",
            &[],
            "",
            "QUERY banned RETURN n.name AS n, c",
            &["n", "c"],
            rows(&[&["a", "x"]]),
        ),
        case(
            "Q1 tag TO col only",
            TAG,
            head,
            &["a IS tag TO col"],
            "",
            "YIELD KEY a, col\nQUERY pr RETURN a.name AS a, col",
            &["a", "col"],
            rows(&[&["a", "x"], &["a", "y"], &["b", "y"]]),
        ),
        case(
            "Q1 tag TO c only",
            TAG,
            head,
            &["a IS tag TO c"],
            "",
            "YIELD KEY a, c\nQUERY pr RETURN a.name AS a, c",
            &["a", "c"],
            rows(&[&["a", "x"], &["a", "y"], &["b", "y"]]),
        ),
        // Unmentioned KEY columns of a negated reference are existential:
        // `a` has a banned tag, `b` has none.
        case(
            "Q1 a IS NOT banned only",
            BANNED,
            head,
            &["a IS NOT banned"],
            "",
            "YIELD KEY a\nQUERY pr RETURN a.name AS a",
            &["a"],
            rows(&[&["b"]]),
        ),
    ];
    check(&db, &names, &cases).await
}

/// Q1-B: a scalar (and a node) KEY column bound by a tuple subject.
#[tokio::test]
async fn is_ref_name_resolution_q1b() -> Result<()> {
    let (db, names) = tags().await?;
    let tag_node = "CREATE RULE tagn AS MATCH (n:N)-[:HAS]->(t:T) YIELD KEY n, KEY t AS c\n";
    let head = "CREATE RULE pr AS MATCH (a:N)";
    let query_for = |rule: &str| format!("YIELD KEY a, col\nQUERY {rule} RETURN a.name AS a, col");
    let cases = [
        case(
            "Q1-B scalar KEY",
            TAG,
            head,
            &["(a, col) IS tag", "a.name <> 'z'"],
            "",
            &query_for("pr"),
            &["a", "col"],
            rows(&[&["a", "x"], &["a", "y"], &["b", "y"]]),
        ),
        case(
            "Q1-B node KEY",
            tag_node,
            head,
            &["(a, col) IS tagn", "a.name <> 'z'"],
            "",
            &query_for("pr"),
            &["a", "col"],
            rows(&[&["a", "@x"], &["a", "@y"], &["b", "@y"]]),
        ),
    ];
    check(&db, &names, &cases).await
}

/// Q1-A: a scalar column bound through a reference, where integer values
/// equal the VIDs of unrelated nodes. A position typed "KEY ⇒ node" joins the
/// integer against `_vid`: it returns that unrelated node, or — when the value
/// is read from the reference's own column — silently drops `z`, whose
/// integer equals no VID. Without `z` every integer collides, the `_vid` join
/// keeps every row, and the mis-typed position goes unseen.
#[tokio::test]
async fn is_ref_name_resolution_q1a() -> Result<()> {
    let db = graph(
        "CREATE (a:N {name: 'a'}), (x:T {name: 'x'}), (y:T {name: 'y'}), \
         (z:T {name: 'z', k: 1000000}), (d1:D {name: 'd1'}), (d2:D {name: 'd2'}), \
         (a)-[:HAS]->(x), (a)-[:HAS]->(y), (a)-[:HAS]->(z)",
    )
    .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for (t, d) in [("x", "d1"), ("y", "d2")] {
        tx.execute_with("MATCH (t:T {name: $t}), (d:D {name: $d}) SET t.k = id(d)")
            .param("t", t)
            .param("d", d)
            .run()
            .await?;
    }
    tx.commit().await?;
    let names = names(&db, "MATCH (n) RETURN id(n) AS vid, n.name AS name").await?;

    // Non-vacuity: each T.k must equal the VID of a D node, not of a T.
    let collide = db
        .session()
        .query("MATCH (t:T), (m) WHERE id(m) = t.k RETURN t.name AS t, m.name AS m, t.k AS k")
        .await?;
    let mut k_of: BTreeMap<String, String> = BTreeMap::new();
    let mut pairs = BTreeSet::new();
    for row in collide.rows() {
        let (Some(t), Some(m), Some(k)) = (row.value("t"), row.value("m"), row.value("k")) else {
            anyhow::bail!("collision query row is missing a column");
        };
        pairs.insert((decode(t, &names), decode(m, &names)));
        k_of.insert(decode(t, &names), decode(k, &names));
    }
    let want: BTreeSet<(String, String)> = [("x", "d1"), ("y", "d2")]
        .iter()
        .map(|(t, m)| ((*t).to_owned(), (*m).to_owned()))
        .collect();
    ensure!(pairs == want, "fixture lost its VID collision: {pairs:?}");
    let (kx, ky) = (k_of["x"].clone(), k_of["y"].clone());
    // Non-vacuity: `z.k` must equal no VID, so a `_vid` join drops it.
    ensure!(
        !names.contains_key(&1_000_000),
        "fixture's non-colliding integer collides with a VID"
    );
    let kz = "1000000";

    let variants = [
        ("keyStr", "YIELD KEY n, KEY t.name AS c", ["x", "y", "z"]),
        (
            "keyInt",
            "YIELD KEY n, KEY t.k AS c",
            [kx.as_str(), ky.as_str(), kz],
        ),
        ("keyNode", "YIELD KEY n, KEY t AS c", ["@x", "@y", "@z"]),
        ("valStr", "YIELD KEY n, t.name AS c", ["x", "y", "z"]),
        (
            "valInt",
            "YIELD KEY n, t.k AS c",
            [kx.as_str(), ky.as_str(), kz],
        ),
    ];
    let mut cases = Vec::new();
    for (vn, yield_clause, [vx, vy, vz]) in variants {
        let tag = format!("CREATE RULE tag AS MATCH (n:N)-[:HAS]->(t:T) {yield_clause}\n");
        for (bn, binding, v) in [
            ("TO col", "a IS tag TO col", "col"),
            ("TO c", "a IS tag TO c", "c"),
            ("tuple", "(a, col) IS tag", "col"),
        ] {
            cases.push(case(
                &format!("Q1-A {vn} {bn}"),
                &tag,
                "CREATE RULE pr AS MATCH (a:N)",
                &[binding, "a.name = 'a'"],
                "",
                &format!("YIELD KEY a, {v}\nQUERY pr RETURN a.name AS a, {v} AS v"),
                &["a", "v"],
                rows(&[&["a", vx], &["a", vy], &["a", vz]]),
            ));
        }
    }
    check(&db, &names, &cases).await
}

// ---------------------------------------------------------------------------
// Fixture N/E: n1 -[w 1]-> n2 -[w 10]-> n3, with d = 100, 200, 300
// ---------------------------------------------------------------------------

async fn weighted() -> Result<(Uni, Names)> {
    let db = graph(
        "CREATE (n1:N {name: 'n1', d: 100}), (n2:N {name: 'n2', d: 200}), \
         (n3:N {name: 'n3', d: 300}), (n1)-[:E {w: 1}]->(n2), (n2)-[:E {w: 10}]->(n3)",
    )
    .await?;
    let names = names(&db, "MATCH (n) RETURN id(n) AS vid, n.name AS name").await?;
    Ok((db, names))
}

/// Q2: ALONG `prev.cost` beside another reference (`disc`) that also yields
/// `cost`. `prev` is the recursive reference, never `disc`.
/// - base: `(n1, n2, 1)`, `(n2, n3, 10)`;
/// - recursive: `n1 -[1]-> n2`, `path(n2, n3) = 10` → `(n1, n3, 10 + 1 = 11)`;
///   `n2 -[10]-> n3` has no onward path.
#[tokio::test]
async fn is_ref_name_resolution_q2() -> Result<()> {
    let (db, names) = weighted().await?;
    let disc = "CREATE RULE disc AS MATCH (a:N) YIELD KEY a, a.d AS cost\n";
    let base =
        "CREATE RULE path AS MATCH (a:N)-[e:E]->(b:N) ALONG cost = e.w YIELD KEY a, KEY b, cost\n";
    let cases = [case(
        "Q2",
        &format!("{disc}{base}"),
        "CREATE RULE path AS MATCH (a:N)-[e:E]->(m:N)",
        &["a IS disc", "m IS path TO b"],
        "ALONG cost = prev.cost + e.w",
        "YIELD KEY a, KEY b, cost\nQUERY path RETURN a.name AS a, b.name AS b, cost",
        &["a", "b", "cost"],
        rows(&[&["n1", "n2", "1"], &["n2", "n3", "10"], &["n1", "n3", "11"]]),
    )];
    check(&db, &names, &cases).await
}

/// `(a, score)`: `score = a.d`.
const R1: &str = "CREATE RULE r1 AS MATCH (a:N) YIELD KEY a, a.d AS score\n";
/// `(a, score)`: `score = 1`.
const R2: &str = "CREATE RULE r2 AS MATCH (a:N) YIELD KEY a, 1 AS score\n";

/// Q3: two references expose `score` and the body reads it by bare name.
#[tokio::test]
async fn is_ref_name_resolution_q3() -> Result<()> {
    let (db, names) = weighted().await?;
    let cases = [case(
        "Q3",
        &format!("{R1}{R2}"),
        "CREATE RULE q AS MATCH (a:N)",
        &["a IS r1", "a IS r2", "score > 5"],
        "",
        "YIELD KEY a, score\nQUERY q RETURN a.name AS a, score",
        &["a", "score"],
        Expect::Compile("score"),
    )];
    check(&db, &names, &cases).await
}

/// Q3 controls: exposing `score` twice without reading it is legal, and
/// binding each `score` positionally resolves the read.
#[tokio::test]
async fn is_ref_name_resolution_q3_controls() -> Result<()> {
    let (db, names) = weighted().await?;
    let rules = format!("{R1}{R2}");
    let cases = [
        case(
            "Q3 exposed, not read",
            &rules,
            "CREATE RULE q AS MATCH (a:N)",
            &["a IS r1", "a IS r2"],
            "",
            "YIELD KEY a\nQUERY q RETURN a.name AS a",
            &["a"],
            rows(&[&["n1"], &["n2"], &["n3"]]),
        ),
        case(
            "Q3 positional",
            &rules,
            "CREATE RULE q AS MATCH (a:N)",
            &["(a, s1) IS r1", "(a, s2) IS r2", "s1 > 5"],
            "",
            "YIELD KEY a, s1, s2\nQUERY q RETURN a.name AS a, s1, s2",
            &["a", "s1", "s2"],
            rows(&[
                &["n1", "100", "1"],
                &["n2", "200", "1"],
                &["n3", "300", "1"],
            ]),
        ),
    ];
    check(&db, &names, &cases).await
}

// ---------------------------------------------------------------------------
// Cypher guards (Q4–Q6): candidate sites with the same shape, measured correct
// ---------------------------------------------------------------------------

/// Runs a Cypher query and decodes its rows in column order.
async fn cypher(db: &Uni, q: &str) -> Outcome {
    let result = db.session().query(q).await.map_err(|e| format!("{e:#}"))?;
    let cols = result.columns().to_vec();
    Ok(result
        .rows()
        .iter()
        .map(|row| {
            cols.iter()
                .map(|c| {
                    row.value(c)
                        .map_or_else(|| "<none>".to_owned(), |v| decode(v, &Names::new()))
                })
                .collect()
        })
        .collect())
}

fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort();
    rows
}

fn owned(rs: &[&[&str]]) -> Vec<Row> {
    rs.iter()
        .map(|r| r.iter().map(|c| (*c).to_owned()).collect())
        .collect()
}

/// Q4: a WITH scalar reused as a node pattern variable is a type conflict.
/// Q5: a list-comprehension variable inside a pattern comprehension is the
/// comprehension's element, not a same-named outer column. Q6: ORDER BY reads
/// the projected alias.
#[tokio::test]
async fn is_ref_name_resolution_cypher_guards() -> Result<()> {
    let mut failures = Vec::new();

    let q4 =
        graph("CREATE (p0:P {age: 0, name: 'p0'}), (p1:P {age: 1, name: 'p1'}), (p0)-[:K]->(p1)")
            .await?;
    match cypher(
        &q4,
        "MATCH (n:P) WITH n.age AS b MATCH (a:P)-[:K]->(b) RETURN count(*) AS c",
    )
    .await
    {
        Err(e) if e.contains("VariableTypeConflict") => {}
        other => failures.push(format!("Q4: expected VariableTypeConflict, got {other:?}")),
    }

    let q5 = graph(
        "CREATE (a:A {name: 'a'}), (b:B {name: 'b'}), (c:B {name: 'c'}), \
         (a)-[:R]->(b), (a)-[:R]->(c)",
    )
    .await?;
    for (id, q, want) in [
        (
            "Q5 x-bound",
            "MATCH (a)-->(b) WITH a, b, [b] AS bs \
             RETURN b.name AS b, [x IN bs | size([(a)-->(x) | 1])] AS r",
            owned(&[&["b", "[1]"], &["c", "[1]"]]),
        ),
        (
            "Q5 x-bound (no b)",
            "MATCH (a)-->(b) WITH a, [b] AS bs RETURN [x IN bs | size([(a)-->(x) | 1])] AS r",
            owned(&[&["[1]"], &["[1]"]]),
        ),
        (
            "Q5 fresh y",
            "MATCH (a)-->(b) WITH a, [b] AS bs RETURN [x IN bs | size([(a)-->(y) | 1])] AS r",
            owned(&[&["[2]"], &["[2]"]]),
        ),
    ] {
        match cypher(&q5, q).await {
            Ok(got) if sorted(got.clone()) == want => {}
            other => failures.push(format!("{id}: expected {want:?}, got {other:?}")),
        }
    }

    let q6 = graph(
        "CREATE (:P {name: 'z', age: 1}), (:P {name: 'a', age: 2}), (:P {name: 'm', age: 3})",
    )
    .await?;
    let ordered = owned(&[&["a"], &["m"], &["z"]]);
    for (id, q) in [
        (
            "Q6",
            "MATCH (n:P) WITH n, n.age AS k RETURN n.name AS age ORDER BY age",
        ),
        (
            "Q6 no WITH",
            "MATCH (n:P) RETURN n.name AS age ORDER BY age",
        ),
    ] {
        match cypher(&q6, q).await {
            Ok(got) if got == ordered => {}
            other => failures.push(format!(
                "{id}: expected {ordered:?} in order, got {other:?}"
            )),
        }
    }

    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
