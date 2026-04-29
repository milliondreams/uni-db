//! Compare three batch-edge-creation Cypher patterns side-by-side using
//! EXPLAIN + PROFILE.
//!
//! Patterns:
//!   A. Original (cartesian MATCH with combined WHERE):
//!        UNWIND $edges AS e
//!        MATCH (a),(b) WHERE id(a) = e.src AND id(b) = e.dst
//!        CREATE (a)-[r:OBSERVED_IN]->(b)
//!        RETURN id(r) AS eid
//!
//!   B. Recommended (multi-MATCH; unambiguous point lookup per side):
//!        UNWIND $edges AS e
//!        MATCH (a) WHERE id(a) = e.src
//!        MATCH (b) WHERE id(b) = e.dst
//!        CREATE (a)-[r:OBSERVED_IN]->(b)
//!        RETURN id(r) AS eid
//!
//!   C. Recommended + idempotent (MERGE instead of CREATE):
//!        UNWIND $edges AS e
//!        MATCH (a) WHERE id(a) = e.src
//!        MATCH (b) WHERE id(b) = e.dst
//!        MERGE (a)-[r:OBSERVED_IN]->(b)
//!        RETURN id(r) AS eid
//!
//! For each pattern this test prints:
//!   - The EXPLAIN plan_text
//!   - Index-usage report and cost estimates
//!   - Per-operator runtime stats from PROFILE
//!   - Total wallclock + peak memory
//!
//! Run with:
//!   cargo nextest run -p uni-db --test issue_55_batch_edge_patterns \
//!       --release --no-capture

use std::collections::HashMap;
use std::time::Instant;

use uni_db::{Uni, Value};

const BATCH_SIZE: usize = 100;

const Q_A: &str = "
    UNWIND $edges AS e
    MATCH (a),(b) WHERE id(a) = e.src AND id(b) = e.dst
    CREATE (a)-[r:OBSERVED_IN]->(b)
    RETURN id(r) AS eid
";

const Q_B: &str = "
    UNWIND $edges AS e
    MATCH (a) WHERE id(a) = e.src
    MATCH (b) WHERE id(b) = e.dst
    CREATE (a)-[r:OBSERVED_IN]->(b)
    RETURN id(r) AS eid
";

const Q_C: &str = "
    UNWIND $edges AS e
    MATCH (a) WHERE id(a) = e.src
    MATCH (b) WHERE id(b) = e.dst
    MERGE (a)-[r:OBSERVED_IN]->(b)
    RETURN id(r) AS eid
";

async fn setup_db(num_each: usize) -> (Uni, Vec<i64>, Vec<i64>) {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Source")
        .property("name", uni_db::DataType::String)
        .done()
        .label("Target")
        .property("name", uni_db::DataType::String)
        .done()
        .edge_type("OBSERVED_IN", &["Source"], &["Target"])
        .done()
        .apply()
        .await
        .unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    let mut srcs = Vec::with_capacity(num_each);
    for i in 0..num_each {
        let r = tx
            .query_with("CREATE (n:Source {name: $name}) RETURN id(n) AS vid")
            .param("name", Value::String(format!("src-{i:04}")))
            .fetch_all()
            .await
            .unwrap();
        srcs.push(r.rows().first().unwrap().get::<i64>("vid").unwrap());
    }
    let mut dsts = Vec::with_capacity(num_each);
    for i in 0..num_each {
        let r = tx
            .query_with("CREATE (n:Target {name: $name}) RETURN id(n) AS vid")
            .param("name", Value::String(format!("dst-{i:04}")))
            .fetch_all()
            .await
            .unwrap();
        dsts.push(r.rows().first().unwrap().get::<i64>("vid").unwrap());
    }
    tx.commit().await.unwrap();
    (db, srcs, dsts)
}

fn build_edge_param(srcs: &[i64], dsts: &[i64]) -> Value {
    let edges: Vec<Value> = srcs
        .iter()
        .zip(dsts.iter())
        .map(|(s, d)| {
            let mut m: HashMap<String, Value> = HashMap::new();
            m.insert("src".into(), Value::Int(*s));
            m.insert("dst".into(), Value::Int(*d));
            Value::Map(m)
        })
        .collect();
    Value::List(edges)
}

async fn run_one(label: &str, query: &str) {
    eprintln!("\n========== Pattern {label} ==========");
    eprintln!("Query:{query}");

    // Each pattern gets a fresh DB so we don't measure interference
    // between consecutive batch creations.
    let (db, srcs, dsts) = setup_db(BATCH_SIZE).await;
    let edges_param = build_edge_param(&srcs, &dsts);

    // 1. EXPLAIN — planning-only, doesn't execute or commit.
    let explain = db
        .session()
        .query_with(query)
        .param("edges", edges_param.clone())
        .explain()
        .await
        .unwrap();

    eprintln!("\n--- EXPLAIN plan_text ---\n{}", explain.plan_text);

    eprintln!("--- EXPLAIN cost estimates ---");
    eprintln!(
        "  estimated_rows = {:.0}, estimated_cost = {:.0}",
        explain.cost_estimates.estimated_rows, explain.cost_estimates.estimated_cost
    );

    if !explain.index_usage.is_empty() {
        eprintln!("--- EXPLAIN index usage ---");
        for u in &explain.index_usage {
            eprintln!(
                "  {} on {}.{} {} -- {}",
                u.index_type,
                u.label_or_type,
                u.property,
                if u.used { "USED" } else { "UNUSED" },
                u.reason.as_deref().unwrap_or("")
            );
        }
    }

    if !explain.warnings.is_empty() {
        eprintln!("--- EXPLAIN warnings ---");
        for w in &explain.warnings {
            eprintln!("  ! {w}");
        }
    }

    if !explain.suggestions.is_empty() {
        eprintln!("--- EXPLAIN index suggestions ---");
        for s in &explain.suggestions {
            eprintln!("  -> {} ({})", s.create_statement, s.reason);
        }
    }

    // 2. PROFILE — executes the query, collecting per-operator stats.
    // Goes through db.profile_internal, which bypasses the session's
    // read-only validator and accepts CREATE/MERGE.
    let t0 = Instant::now();
    let (results, profile) = db
        .session()
        .query_with(query)
        .param("edges", edges_param)
        .profile()
        .await
        .unwrap();
    let wall_ms = t0.elapsed().as_secs_f64() * 1000.0;

    eprintln!("--- PROFILE ---");
    eprintln!("  total_time_ms (engine) : {}", profile.total_time_ms);
    eprintln!("  wallclock_ms (caller)  : {wall_ms:.2}");
    eprintln!("  peak_memory_bytes      : {}", profile.peak_memory_bytes);
    eprintln!("  rows returned          : {}", results.rows().len());

    eprintln!("--- PROFILE per-operator (post-order: leaves -> roots) ---");
    eprintln!(
        "  {:<32} {:>10} {:>10} {:>14}",
        "operator", "rows", "time(ms)", "memory(bytes)"
    );
    for op in &profile.runtime_stats {
        eprintln!(
            "  {:<32} {:>10} {:>10.3} {:>14}",
            op.operator, op.actual_rows, op.time_ms, op.memory_bytes
        );
    }

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn compare_batch_edge_patterns() {
    eprintln!(
        "\n=== issue #55 batch-edge-pattern comparison: BATCH_SIZE={BATCH_SIZE} per pattern ===\n"
    );

    run_one("A: cartesian MATCH + CREATE", Q_A).await;
    run_one("B: multi-MATCH + CREATE", Q_B).await;
    run_one("C: multi-MATCH + MERGE", Q_C).await;
}
