// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Measurement probe for issue #301: does a recursive Locy rule pay for the
//! whole relation on every fixpoint iteration instead of only the delta?
//!
//! Fixture (the issue's shape): a chain `c0 -> c1 -> ... -> c8` at 60% with
//! `c0` the seed, plus `N` noise `OWNS` edges at 1.0% between noise nodes that
//! can never reach the threshold. Exactly one fact is derived per iteration, so
//! a delta-driven evaluator should touch O(1) edges per iteration while a
//! full-relation evaluator touches all `N + 8`.
//!
//! The probe prints, per iteration, `delta_facts`, `elapsed_ms` and every
//! operator's `actual_rows` from `locy_with(..).profile()`, for several rule
//! variants (MSUM / MMAX / plain recursion), storage modes (L0-only, and
//! bulk-loaded + flushed + reopened from disk) and sizes.
//!
//! It is `#[ignore]`d because it is a measurement, not a guard: it asserts only
//! that every variant answers the 9 chain uids. Run with
//! `cargo nextest run -E 'test(issue_301)' --run-ignored all --no-capture`.
//! Sizes can be overridden with `ISSUE_301_SIZES=4000,16000`.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{Uni, UniConfig, Value};

const MSUM: &str = "\
CREATE RULE blocked AS MATCH (e:E) WHERE e.seed = true YIELD KEY e, 100.0 AS agg \n\
CREATE RULE blocked AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE o IS blocked AND r.pct IS NOT NULL \
  FOLD agg = MSUM(r.pct) REQUIRE agg >= 50.0 YIELD KEY e, agg \n\
QUERY blocked RETURN e.uid AS uid";

const MMAX: &str = "\
CREATE RULE blocked AS MATCH (e:E) WHERE e.seed = true YIELD KEY e, 100.0 AS agg \n\
CREATE RULE blocked AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE o IS blocked AND r.pct IS NOT NULL \
  FOLD agg = MMAX(r.pct) REQUIRE agg >= 50.0 YIELD KEY e, agg \n\
QUERY blocked RETURN e.uid AS uid";

/// No FOLD: plain reachability from the seed. Noise nodes are disconnected
/// from the chain, so the answer is the same 9 uids.
const PLAIN: &str = "\
CREATE RULE reach AS MATCH (e:E) WHERE e.seed = true YIELD KEY e \n\
CREATE RULE reach AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE o IS reach YIELD KEY e \n\
QUERY reach RETURN e.uid AS uid";

const SCHEMA: [&str; 3] = [
    "CREATE LABEL E (uid STRING, seed BOOL)",
    "CREATE EDGE TYPE OWNS (pct FLOAT NULL) FROM E TO E",
    "CREATE INDEX idx_e_uid FOR (e:E) ON (e.uid)",
];

struct Fixture {
    /// (uid, seed)
    vertices: Vec<(String, bool)>,
    /// (src index, dst index, pct)
    edges: Vec<(usize, usize, f64)>,
}

fn fixture(n_noise: usize) -> Fixture {
    let mut vertices: Vec<(String, bool)> = (0..9).map(|i| (format!("c{i}"), i == 0)).collect();
    let len = n_noise / 2 + 2;
    vertices.extend((0..len).map(|i| (format!("n{i}"), false)));
    let mut edges: Vec<(usize, usize, f64)> = (0..8).map(|i| (i, i + 1, 60.0)).collect();
    for i in 0..n_noise {
        let (a, b) = (i % len, (i * 7 + 3) % len);
        if a != b {
            edges.push((9 + a, 9 + b, 1.0));
        }
    }
    Fixture { vertices, edges }
}

fn no_auto_flush() -> UniConfig {
    UniConfig {
        auto_flush_interval: None,
        auto_flush_threshold: usize::MAX / 2,
        // The L0 edge load is one UNWIND over N rows; a debug build needs more
        // than the 30 s default at N = 40k.
        query_timeout: Duration::from_secs(900),
        ..Default::default()
    }
}

async fn apply_schema(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    for ddl in SCHEMA {
        tx.execute(ddl).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Bulk-load, flush, close, reopen — the issue's store was persisted.
async fn build_flushed(f: &Fixture, dir: &std::path::Path) -> Result<Uni> {
    let path = dir.to_str().unwrap().to_string();
    {
        let db = Uni::open(&path).build().await?;
        apply_schema(&db).await?;
        let tx = db.session().tx().await?;
        let mut bulk = tx.bulk_writer().build()?;
        let vs: Vec<HashMap<String, Value>> = f
            .vertices
            .iter()
            .map(|(uid, seed)| {
                HashMap::from([
                    ("uid".to_string(), Value::String(uid.clone())),
                    ("seed".to_string(), Value::Bool(*seed)),
                ])
            })
            .collect();
        let vids = bulk.insert_vertices("E", vs).await?;
        let es = f
            .edges
            .iter()
            .map(|&(a, b, p)| {
                uni_bulk::EdgeData::new(
                    vids[a],
                    vids[b],
                    HashMap::from([("pct".to_string(), Value::Float(p))]),
                )
            })
            .collect();
        bulk.insert_edges("OWNS", es).await?;
        bulk.commit().await?;
        tx.commit().await?;
        db.flush().await?;
    }
    Ok(Uni::open(&path).build().await?)
}

/// Cypher writes only, auto-flush disabled: every row stays in L0.
async fn build_l0(f: &Fixture) -> Result<Uni> {
    let db = Uni::in_memory().config(no_auto_flush()).build().await?;
    apply_schema(&db).await?;
    let vs: Vec<Value> = f
        .vertices
        .iter()
        .map(|(uid, seed)| {
            Value::Map(HashMap::from([
                ("uid".to_string(), Value::String(uid.clone())),
                ("seed".to_string(), Value::Bool(*seed)),
            ]))
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.execute_with("UNWIND $rows AS r CREATE (:E {uid: r.uid, seed: r.seed})")
        .param("rows", Value::List(vs))
        .run()
        .await?;
    tx.commit().await?;
    let es: Vec<Value> = f
        .edges
        .iter()
        .map(|&(a, b, p)| {
            Value::Map(HashMap::from([
                ("a".to_string(), Value::String(f.vertices[a].0.clone())),
                ("b".to_string(), Value::String(f.vertices[b].0.clone())),
                ("p".to_string(), Value::Float(p)),
            ]))
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.execute_with(
        "UNWIND $rows AS r MATCH (a:E {uid: r.a}), (b:E {uid: r.b}) \
         CREATE (a)-[:OWNS {pct: r.p}]->(b)",
    )
    .param("rows", Value::List(es))
    .run()
    .await?;
    tx.commit().await?;
    Ok(db)
}

fn short_op(name: &str) -> String {
    name.split(['(', ':', ' '])
        .next()
        .unwrap_or(name)
        .to_string()
}

async fn run_one(db: &Uni, label: &str, program: &str, print_plan: bool) -> Result<()> {
    let config = LocyConfig {
        max_iterations: 100,
        timeout: Some(Duration::from_secs(600)),
        ..Default::default()
    };
    let started = Instant::now();
    let (result, profile) = db
        .session()
        .locy_with(program)
        .with_config(config)
        .profile()
        .await?;
    let wall = started.elapsed();

    let mut uids: Vec<String> = result
        .rows()
        .map(|rows| {
            rows.iter()
                .filter_map(|r| match r.get("uid") {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    uids.sort();
    uids.dedup();
    let expected: Vec<String> = (0..9).map(|i| format!("c{i}")).collect();

    eprintln!(
        "\n=== {label}: wall {:.3}s  locy total {:.1}ms  rows_scanned {}  iterations {}  uids {}",
        wall.as_secs_f64(),
        profile.total_time_ms,
        result.metrics().rows_scanned,
        result.stats.total_iterations,
        uids.len(),
    );
    if print_plan {
        eprintln!("--- explain plan_text ---\n{}", profile.explain.plan_text);
    }
    for s in &profile.profile.strata {
        eprintln!(
            "  stratum {} recursive={} {:.1}ms iters={} facts={}",
            s.index, s.recursive, s.elapsed_ms, s.iterations, s.facts_derived
        );
        if !s.operators.is_empty() {
            let ops: Vec<String> = s
                .operators
                .iter()
                .map(|o| {
                    format!(
                        "{}={}r/{:.0}ms",
                        short_op(&o.operator),
                        o.actual_rows,
                        o.time_ms
                    )
                })
                .collect();
            eprintln!("    stratum-ops: {}", ops.join(" "));
        }
        for r in &s.rules {
            for it in &r.iterations {
                // Aggregate by operator name so repeated nodes sum up.
                let mut agg: BTreeMap<String, (usize, f64, usize)> = BTreeMap::new();
                for o in &it.operators {
                    let e = agg.entry(short_op(&o.operator)).or_default();
                    e.0 += o.actual_rows;
                    e.1 += o.time_ms;
                    e.2 += 1;
                }
                let ops: Vec<String> = agg
                    .iter()
                    .map(|(k, (rows, ms, n))| {
                        if *n > 1 {
                            format!("{k}x{n}={rows}r/{ms:.0}ms")
                        } else {
                            format!("{k}={rows}r/{ms:.0}ms")
                        }
                    })
                    .collect();
                eprintln!(
                    "    {} it{} delta={} {:.1}ms | {}",
                    r.name,
                    it.iteration,
                    it.delta_facts,
                    it.elapsed_ms,
                    ops.join(" ")
                );
            }
        }
    }
    assert_eq!(uids, expected, "{label}: wrong answer");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement probe for #301; run with --run-ignored all --no-capture"]
async fn issue_301_fixpoint_delta_probe() -> Result<()> {
    let sizes: Vec<usize> = std::env::var("ISSUE_301_SIZES")
        .ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![4_000, 16_000, 40_000]);
    let modes: Vec<String> = std::env::var("ISSUE_301_MODES")
        .ok()
        .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
        .unwrap_or_else(|| vec!["flushed".into(), "l0".into()]);

    let mut first = true;
    for &n in &sizes {
        let f = fixture(n);
        for mode in &modes {
            let dir = tempfile::tempdir()?;
            let t = Instant::now();
            let db = match mode.as_str() {
                "flushed" => build_flushed(&f, dir.path()).await?,
                _ => build_l0(&f).await?,
            };
            eprintln!(
                "\n##### N={n} mode={mode} edges={} vertices={} load {:.1}s",
                f.edges.len(),
                f.vertices.len(),
                t.elapsed().as_secs_f64()
            );
            for (name, prog) in [("MSUM", MSUM), ("MMAX", MMAX), ("PLAIN", PLAIN)] {
                run_one(&db, &format!("N={n} {mode} {name}"), prog, first).await?;
                first = false;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Guard: a recursive clause's cost tracks the frontier, not the relation.
// ---------------------------------------------------------------------------

/// Per-iteration operator totals of one rule, keyed by short operator name.
struct IterationOps {
    delta: usize,
    /// operator -> (summed actual_rows, number of operator nodes)
    ops: BTreeMap<String, (usize, usize)>,
}

impl IterationOps {
    fn rows(&self, op: &str) -> usize {
        self.ops.get(op).map_or(0, |(rows, _)| *rows)
    }

    fn nodes(&self, op: &str) -> usize {
        self.ops.get(op).map_or(0, |(_, n)| *n)
    }
}

/// Runs `program` under `profile()` and returns the QUERY's sorted uids plus
/// the per-iteration operator totals of rule `rule`.
async fn profile_rule(
    db: &Uni,
    program: &str,
    rule: &str,
) -> Result<(Vec<String>, Vec<IterationOps>)> {
    let config = LocyConfig {
        max_iterations: 100,
        timeout: Some(Duration::from_secs(600)),
        ..Default::default()
    };
    let (result, profile) = db
        .session()
        .locy_with(program)
        .with_config(config)
        .profile()
        .await?;
    let mut uids: Vec<String> = result
        .rows()
        .map(|rows| {
            rows.iter()
                .filter_map(|r| match r.get("uid") {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    uids.sort();
    let iterations = profile
        .profile
        .strata
        .iter()
        .flat_map(|s| s.rules.iter())
        .filter(|r| r.name == rule)
        .flat_map(|r| r.iterations.iter())
        .map(|it| {
            let mut ops: BTreeMap<String, (usize, usize)> = BTreeMap::new();
            for o in &it.operators {
                let e = ops.entry(short_op(&o.operator)).or_default();
                e.0 += o.actual_rows;
                e.1 += 1;
            }
            IterationOps {
                delta: it.delta_facts,
                ops,
            }
        })
        .collect();
    Ok((uids, iterations))
}

fn chain_uids() -> Vec<String> {
    (0..9).map(|i| format!("c{i}")).collect()
}

/// The per-size facts the guard compares.
#[derive(Debug, PartialEq, Eq)]
struct Cost {
    /// GraphTraverseExec rows summed over every iteration.
    traverse_rows: usize,
    /// GraphScanExec rows summed over iterations after the first. The first
    /// iteration also runs the seed clause, whose scan legitimately reads the
    /// label; every later iteration runs the recursive clause alone.
    scan_rows_after_first: usize,
}

/// Asserts the shape every guarded variant shares and returns its cost.
fn check_variant(label: &str, uids: &[String], its: &[IterationOps]) -> Cost {
    assert_eq!(uids, chain_uids().as_slice(), "{label}: wrong answer");
    // One chain node per iteration, then one empty iteration to converge.
    let deltas: Vec<usize> = its.iter().map(|i| i.delta).collect();
    let mut expected = vec![1; 9];
    expected.push(0);
    assert_eq!(deltas, expected, "{label}: per-iteration delta pattern");

    let last = its.last().expect("at least one iteration");
    assert_eq!(
        last.rows("GraphTraverseExec"),
        0,
        "{label}: the converging iteration has an empty frontier, so it must \
         traverse nothing"
    );

    // The seed clause reads lower strata only, so it runs in the first
    // iteration and never again: every later iteration has exactly one fewer
    // vertex scan than the first.
    let first_scans = its[0].nodes("GraphScanExec");
    for (i, it) in its.iter().enumerate().skip(1) {
        assert_eq!(
            it.nodes("GraphScanExec") + 1,
            first_scans,
            "{label}: iteration {i} re-ran the seed clause's scan"
        );
    }

    Cost {
        traverse_rows: its.iter().map(|i| i.rows("GraphTraverseExec")).sum(),
        scan_rows_after_first: its.iter().skip(1).map(|i| i.rows("GraphScanExec")).sum(),
    }
}

/// Issue #301: every fixpoint iteration of a recursive rule re-traversed every
/// edge and re-scanned every vertex, and re-ran the non-recursive seed clause.
///
/// The noise edges can never reach the chain, so a delta-driven evaluation
/// costs the same at both sizes; the full-relation one costs 4x more at the
/// larger. Counted in operator rows, not time, so the guard is not timing
/// sensitive.
#[tokio::test(flavor = "multi_thread")]
async fn issue_301_recursive_cost_tracks_frontier_not_relation() -> Result<()> {
    let mut costs: BTreeMap<&str, Vec<Cost>> = BTreeMap::new();
    for n in [2_000, 8_000] {
        let dir = tempfile::tempdir()?;
        let db = build_flushed(&fixture(n), dir.path()).await?;
        for (name, program, rule) in [
            ("MSUM", MSUM, "blocked"),
            ("MMAX", MMAX, "blocked"),
            ("PLAIN", PLAIN, "reach"),
        ] {
            let label = format!("N={n} {name}");
            let (uids, its) = profile_rule(&db, program, rule).await?;
            let cost = check_variant(&label, &uids, &its);
            eprintln!("{label}: {cost:?}");
            costs.entry(name).or_default().push(cost);
        }
    }
    for (name, per_size) in &costs {
        assert_eq!(
            per_size[0], per_size[1],
            "{name}: recursive-clause cost grew with the relation (N=2000 vs N=8000)"
        );
        // Eight chain edges, each traversed once, from one frontier vertex
        // looked up per iteration.
        assert_eq!(
            per_size[0],
            Cost {
                traverse_rows: 8,
                scan_rows_after_first: 9,
            },
            "{name}: frontier-driven cost"
        );
    }
    Ok(())
}

/// Builds a small in-memory graph from `(uid, seed)` vertices and
/// `(src, dst, pct)` edges given by uid.
async fn small_graph(vertices: &[(&str, bool)], edges: &[(&str, &str, f64)]) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    apply_schema(&db).await?;
    let tx = db.session().tx().await?;
    for (uid, seed) in vertices {
        tx.execute(&format!("CREATE (:E {{uid: '{uid}', seed: {seed}}})"))
            .await?;
    }
    for (a, b, p) in edges {
        tx.execute(&format!(
            "MATCH (a:E {{uid: '{a}'}}), (b:E {{uid: '{b}'}}) CREATE (a)-[:OWNS {{pct: {p:.1}}}]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// Runs `program` and returns its QUERY rows as sorted `(uid, agg)` pairs.
async fn uid_aggs(db: &Uni, program: &str) -> Result<Vec<(String, f64)>> {
    let result = db.session().locy(program).await?;
    let mut out: Vec<(String, f64)> = result
        .rows()
        .expect("program has a QUERY")
        .iter()
        .map(|r| {
            let uid = match r.get("uid") {
                Some(Value::String(s)) => s.clone(),
                other => panic!("uid must be a string, got {other:?}"),
            };
            let agg = match r.get("agg") {
                Some(Value::Float(f)) => *f,
                Some(Value::Int(i)) => *i as f64,
                other => panic!("agg must be numeric, got {other:?}"),
            };
            (uid, agg)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// A parent whose folded value MOVES while it is already in the snapshot
/// must be re-read, so a child whose contribution depends on that value is
/// recomputed.
///
/// `x` is reached from `a` (it2, 30) and again from `c` (it3, 30 more), so its
/// MSUM moves 30 -> 60 while it already passes `REQUIRE`. `y`'s contribution is
/// `x`'s value, so `y` must end at 60, not the 30 it first derived.
#[tokio::test(flavor = "multi_thread")]
async fn issue_301_folded_value_change_is_refed() -> Result<()> {
    let db = small_graph(
        &[
            ("s", true),
            ("a", false),
            ("b", false),
            ("c", false),
            ("x", false),
            ("y", false),
        ],
        &[
            ("s", "a", 60.0),
            ("s", "b", 60.0),
            ("b", "c", 100.0),
            ("a", "x", 50.0),
            ("c", "x", 50.0),
            ("x", "y", 100.0),
        ],
    )
    .await?;
    let program = |agg: &str| {
        format!(
            "CREATE RULE v AS MATCH (e:E) WHERE e.seed = true YIELD KEY e, 100.0 AS agg \n\
             CREATE RULE v AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE o IS v \
               FOLD agg = {agg}(agg * r.pct / 100.0) REQUIRE agg >= 25.0 YIELD KEY e, agg \n\
             QUERY v RETURN e.uid AS uid, agg"
        )
    };
    let got = uid_aggs(&db, &program("MSUM")).await?;
    let want: Vec<(String, f64)> = [
        ("a", 60.0),
        ("b", 60.0),
        ("c", 60.0),
        ("s", 100.0),
        ("x", 60.0),
        ("y", 60.0),
    ]
    .iter()
    .map(|(u, v)| (u.to_string(), *v))
    .collect();
    assert_eq!(got, want, "MSUM over a value that moves mid-fixpoint");

    // MMAX: x = max(30, 30) = 30, y = 30.
    let got = uid_aggs(&db, &program("MMAX")).await?;
    let want: Vec<(String, f64)> = [
        ("a", 60.0),
        ("b", 60.0),
        ("c", 60.0),
        ("s", 100.0),
        ("x", 30.0),
        ("y", 30.0),
    ]
    .iter()
    .map(|(u, v)| (u.to_string(), *v))
    .collect();
    assert_eq!(got, want, "MMAX over the same graph");
    Ok(())
}

/// Correctness of the shapes that keep full-relation feeding: non-linear
/// recursion (two same-stratum references in one clause) and mutual
/// recursion, with and without FOLD.
#[tokio::test(flavor = "multi_thread")]
async fn issue_301_nonlinear_and_mutual_recursion_answers() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = build_flushed(&fixture(200), dir.path()).await?;

    // Transitive closure over the 60% chain edges, doubled non-linearly.
    let tc = "\
        CREATE RULE tc AS MATCH (a:E)-[r:OWNS]->(b:E) WHERE r.pct > 50.0 YIELD KEY a, KEY b \n\
        CREATE RULE tc AS MATCH (a:E) WHERE a IS tc TO m, m IS tc TO b YIELD KEY a, KEY b \n\
        QUERY tc RETURN a.uid AS src, b.uid AS dst";
    let result = db.session().locy(tc).await?;
    let mut pairs: Vec<(String, String)> = result
        .rows()
        .expect("QUERY rows")
        .iter()
        .map(|r| match (r.get("src"), r.get("dst")) {
            (Some(Value::String(a)), Some(Value::String(b))) => (a.clone(), b.clone()),
            other => panic!("unexpected row {other:?}"),
        })
        .collect();
    pairs.sort();
    let mut want: Vec<(String, String)> = Vec::new();
    for i in 0..9 {
        for j in (i + 1)..9 {
            want.push((format!("c{i}"), format!("c{j}")));
        }
    }
    want.sort();
    assert_eq!(pairs, want, "non-linear transitive closure");

    // Mutual recursion, plain.
    let even_odd = |query: &str| {
        format!(
            "CREATE RULE even AS MATCH (e:E) WHERE e.seed = true YIELD KEY e \n\
             CREATE RULE odd AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE o IS even AND r.pct > 50.0 YIELD KEY e \n\
             CREATE RULE even AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE o IS odd AND r.pct > 50.0 YIELD KEY e \n\
             QUERY {query} RETURN e.uid AS uid"
        )
    };
    let uids = |result: uni_db::locy::LocyResult| -> Vec<String> {
        let mut v: Vec<String> = result
            .rows()
            .expect("QUERY rows")
            .iter()
            .filter_map(|r| match r.get("uid") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        v.sort();
        v
    };
    let evens: Vec<String> = [0, 2, 4, 6, 8].iter().map(|i| format!("c{i}")).collect();
    let odds: Vec<String> = [1, 3, 5, 7].iter().map(|i| format!("c{i}")).collect();
    assert_eq!(uids(db.session().locy(&even_odd("even")).await?), evens);
    assert_eq!(uids(db.session().locy(&even_odd("odd")).await?), odds);

    // Mutual recursion through FOLD: each rule folds the other's value.
    let fold = |query: &str| {
        format!(
            "CREATE RULE fa AS MATCH (e:E) WHERE e.seed = true YIELD KEY e, 100.0 AS agg \n\
             CREATE RULE fb AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE o IS fa AND r.pct IS NOT NULL \
               FOLD agg = MSUM(r.pct) REQUIRE agg >= 50.0 YIELD KEY e, agg \n\
             CREATE RULE fa AS MATCH (o:E)-[r:OWNS]->(e:E) WHERE o IS fb AND r.pct IS NOT NULL \
               FOLD agg = MSUM(r.pct) REQUIRE agg >= 50.0 YIELD KEY e, agg \n\
             QUERY {query} RETURN e.uid AS uid"
        )
    };
    assert_eq!(uids(db.session().locy(&fold("fa")).await?), evens);
    assert_eq!(uids(db.session().locy(&fold("fb")).await?), odds);
    Ok(())
}

/// Once the seed clause stops re-running, an iteration in which no clause
/// derives anything must still count as converged. The monotonic-fold
/// accumulators were only re-snapshotted when *some* candidate arrived, so this
/// program — whose recursive clause never derives — ran to its iteration limit.
#[tokio::test(flavor = "multi_thread")]
async fn issue_301_iteration_with_no_candidates_converges() -> Result<()> {
    let db = small_graph(&[("s", true), ("a", false)], &[("s", "a", 60.0)]).await?;
    let program = "\
        CREATE RULE r AS MATCH (a:E)-[x:OWNS]->(b:E) ALONG step = 0.9 \
          FOLD risk = MNOR(step) YIELD KEY a, KEY b, risk \n\
        CREATE RULE r AS MATCH (a:E)-[x:OWNS]->(m:E) WHERE m IS r TO b ALONG step = 0.9 \
          FOLD risk = MNOR(step) YIELD KEY a, KEY b, risk \n\
        QUERY r RETURN a.uid AS uid";
    let config = LocyConfig {
        max_iterations: 50,
        ..Default::default()
    };
    let result = db
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await?;
    assert!(
        result.stats.total_iterations <= 3,
        "converged only after {} iterations",
        result.stats.total_iterations
    );
    Ok(())
}
