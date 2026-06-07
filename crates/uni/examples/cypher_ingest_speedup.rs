// Cypher ingest-path cost breakdown & speedup-lever quantification.
//
// Reproduces the bulk-vs-UNWIND gap *inside this repo* and, crucially, splits
// each Cypher write into parse / plan / exec and per-operator times so we can
// compute the ceiling of each candidate optimization WITHOUT implementing it
// first:
//
//   Lever A  prepared/cached write path  -> removes parse_time + plan_time
//   Lever B  batched CREATE executor     -> drops exec to ~bulk level
//   Lever C  CREATE+SET fusion           -> removes the MutationSetExec pass (edges)
//   Lever D  edge endpoint point-get     -> removes GraphScan+Filter+Proj+HashJoin (edges)
//
// The real ingest batch distribution is dominated by size-1 batches, so the
// sweep emphasizes small sizes. No embedder is configured: this isolates the
// parse/plan/executor deltas (the with-embed case collapses to ~1.4x and is a
// pure embedder artifact, already characterized elsewhere).
//
// Run with: cargo run --release --example cypher_ingest_speedup

use std::collections::HashMap;
use std::time::{Duration, Instant};

use mimalloc::MiMalloc;
use uni_db::{DataType, Uni, Value};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const REPS: usize = 9;
const SIZES: &[usize] = &[1, 2, 4, 8, 16, 64];
const ENDPOINT_POOL: usize = 4096;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

/// Median of `parse/plan/exec/total` (µs) for one Cypher write, rolled back per rep.
#[derive(Default, Clone, Copy)]
struct Split {
    parse: f64,
    plan: f64,
    exec: f64,
    total: f64,
    wall: f64,
}

fn node_rows(n: usize, tag: &str) -> Value {
    Value::List(
        (0..n)
            .map(|i| {
                let mut m = HashMap::new();
                m.insert("name".to_string(), Value::String(format!("{tag}{i}")));
                m.insert("idx".to_string(), Value::Int(i as i64));
                Value::Map(m)
            })
            .collect(),
    )
}

fn node_props(n: usize, tag: &str) -> Vec<HashMap<String, Value>> {
    (0..n)
        .map(|i| {
            let mut h = HashMap::new();
            h.insert("name".to_string(), Value::String(format!("{tag}{i}")));
            h.insert("idx".to_string(), Value::Int(i as i64));
            h
        })
        .collect()
}

const NODE_CYPHER: &str = "UNWIND $rows AS r CREATE (n:Person {name: r.name, idx: r.idx})";
const EDGE_CYPHER: &str = "UNWIND $edges AS e \
     MATCH (a:Ep) WHERE id(a) = e.src \
     MATCH (b:Ep) WHERE id(b) = e.dst \
     CREATE (a)-[r:LINK]->(b) SET r.role = e.role \
     RETURN id(r) AS eid";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("idx", DataType::Int64)
        .done()
        .label("Ep")
        .property("idx", DataType::Int64)
        .done()
        .edge_type("LINK", &["Ep"], &["Ep"])
        .property_nullable("role", DataType::String)
        .done()
        .apply()
        .await?;

    // Committed endpoint pool so edge MATCH ... WHERE id() = ... has real VIDs.
    let endpoint_vids: Vec<u64> = {
        let tx = db.session().tx().await?;
        let vids = tx
            .bulk_insert_vertices("Ep", node_props(ENDPOINT_POOL, "ep"))
            .await?;
        tx.commit().await?;
        vids.into_iter().map(|v| v.as_u64()).collect()
    };

    println!("# Cypher ingest cost breakdown (in-repo, no embedder)\n");
    println!("reps={REPS} (median), per-op µs, rollback per rep\n");

    // ---- NODES ----------------------------------------------------------
    println!("## Nodes — UNWIND CREATE vs bulk_insert_vertices\n");
    println!(
        "{:>5} | {:>8} {:>8} {:>8} {:>9} | {:>9} | {:>9} | {:>8}",
        "size", "parse", "plan", "exec", "cy/op", "bulk/op", "speedup", "exec%"
    );
    println!("{}", "-".repeat(78));
    let mut node_split_sz1 = Split::default();
    for &sz in SIZES {
        let split = bench_node_cypher(&db, sz).await?;
        let bulk = bench_node_bulk(&db, sz).await?;
        let cy_op = split.wall / sz as f64;
        let bulk_op = bulk / sz as f64;
        if sz == 1 {
            node_split_sz1 = split;
        }
        println!(
            "{:>5} | {:>8.1} {:>8.1} {:>8.1} {:>9.1} | {:>9.2} | {:>8.1}x | {:>7.0}%",
            sz,
            split.parse,
            split.plan,
            split.exec,
            cy_op,
            bulk_op,
            cy_op / bulk_op,
            100.0 * split.exec / split.total,
        );
    }

    // ---- EDGES ----------------------------------------------------------
    println!("\n## Edges — UNWIND MATCH MATCH CREATE SET vs bulk_insert_edges\n");
    println!(
        "{:>5} | {:>8} {:>8} {:>8} {:>9} | {:>9} | {:>9} | {:>8}",
        "size", "parse", "plan", "exec", "cy/op", "bulk/op", "speedup", "exec%"
    );
    println!("{}", "-".repeat(78));
    let mut edge_split_sz1 = Split::default();
    for &sz in SIZES {
        let split = bench_edge_cypher(&db, sz, &endpoint_vids).await?;
        let bulk = bench_edge_bulk(&db, sz, &endpoint_vids).await?;
        let cy_op = split.wall / sz as f64;
        let bulk_op = bulk / sz as f64;
        if sz == 1 {
            edge_split_sz1 = split;
        }
        println!(
            "{:>5} | {:>8.1} {:>8.1} {:>8.1} {:>9.1} | {:>9.2} | {:>8.1}x | {:>7.0}%",
            sz,
            split.parse,
            split.plan,
            split.exec,
            cy_op,
            bulk_op,
            cy_op / bulk_op,
            100.0 * split.exec / split.total,
        );
    }

    // ---- PER-OPERATOR PROFILE (size 1) ---------------------------------
    println!("\n## Per-operator profile @ size 1\n");
    println!("### node CREATE");
    let node_ops = profile_node(&db).await?;
    for (op, rows, ms) in &node_ops {
        println!("  {:<22} rows={:<4} {:.4} ms", op, rows, ms);
    }
    println!("\n### edge MATCH/MATCH/CREATE/SET");
    let edge_ops = profile_edge(&db, &endpoint_vids).await?;
    let mut endpoint_resolution_ms = 0.0;
    let mut set_ms = 0.0;
    let mut saw_vid_lookup = false;
    for (op, rows, ms) in &edge_ops {
        println!("  {:<22} rows={:<4} {:.4} ms", op, rows, ms);
        if matches!(
            op.as_str(),
            "GraphScanExec" | "FilterExec" | "ProjectionExec" | "HashJoinExec"
        ) {
            endpoint_resolution_ms += ms;
        }
        if op == "MutationSetExec" {
            set_ms += ms;
        }
        if op == "VidLookupJoinExec" {
            saw_vid_lookup = true;
        }
    }

    // ---- LEVER CEILINGS (computed from measurements) -------------------
    println!("\n## Lever ceilings (projected from size-1 measurements)\n");
    let n = node_split_sz1;
    let e = edge_split_sz1;

    println!(
        "NODES @ size 1: total={:.1}µs (parse={:.1} plan={:.1} exec={:.1})",
        n.total, n.parse, n.plan, n.exec
    );
    let a_node = n.parse + n.plan;
    println!(
        "  Lever A (skip parse+plan): -{:.1}µs -> {:.1}µs  ({:.2}x)",
        a_node,
        n.total - a_node,
        n.total / (n.total - a_node).max(0.1)
    );
    println!(
        "  Lever B (exec->bulk-level): exec {:.1}->~bulk; see node table speedups (B is the dominant node lever)",
        n.exec
    );

    println!(
        "\nEDGES @ size 1: total={:.1}µs (parse={:.1} plan={:.1} exec={:.1})",
        e.total, e.parse, e.plan, e.exec
    );
    let a_edge = e.parse + e.plan;
    println!(
        "  Lever A (skip parse+plan):     -{:.1}µs -> {:.1}µs  ({:.2}x)",
        a_edge,
        e.total - a_edge,
        e.total / (e.total - a_edge).max(0.1)
    );
    let set_us = set_ms * 1000.0;
    println!(
        "  Lever C (fuse SET into CREATE): -{:.1}µs -> {:.1}µs  ({:.2}x)   [MutationSetExec @sz1]",
        set_us,
        e.total - set_us,
        e.total / (e.total - set_us).max(0.1)
    );
    let endp_us = endpoint_resolution_ms * 1000.0;
    println!(
        "  Lever D (endpoint point-get):  -{:.1}µs -> {:.1}µs  ({:.2}x)   [Scan+Filter+Proj+Join @sz1]",
        endp_us,
        e.total - endp_us,
        e.total / (e.total - endp_us).max(0.1)
    );
    let cd_us = set_us + endp_us;
    println!(
        "  Levers C+D combined:           -{:.1}µs -> {:.1}µs  ({:.2}x)",
        cd_us,
        e.total - cd_us,
        e.total / (e.total - cd_us).max(0.1)
    );
    println!(
        "  Levers A+C+D combined:         -{:.1}µs -> {:.1}µs  ({:.2}x)",
        a_edge + cd_us,
        e.total - a_edge - cd_us,
        e.total / (e.total - a_edge - cd_us).max(0.1)
    );

    println!(
        "\nFast-path probe: VidLookupJoinExec {} in edge plan",
        if saw_vid_lookup {
            "FIRED (#55 active)"
        } else {
            "ABSENT -> fell back to GraphScan+HashJoin (lever D gap confirmed)"
        }
    );

    Ok(())
}

async fn bench_node_cypher(db: &Uni, sz: usize) -> anyhow::Result<Split> {
    let mut parse = Vec::new();
    let mut plan = Vec::new();
    let mut exec = Vec::new();
    let mut total = Vec::new();
    let mut wall = Vec::new();
    for rep in 0..REPS {
        let rows = node_rows(sz, &format!("n{rep}_"));
        let tx = db.session().tx().await?;
        let t = Instant::now();
        let res = tx
            .execute_with(NODE_CYPHER)
            .param("rows", rows)
            .run()
            .await?;
        let w = t.elapsed();
        let m = res.metrics();
        parse.push(us(m.parse_time));
        plan.push(us(m.plan_time));
        exec.push(us(m.exec_time));
        total.push(us(m.total_time));
        wall.push(us(w));
        tx.rollback();
    }
    Ok(Split {
        parse: median(parse),
        plan: median(plan),
        exec: median(exec),
        total: median(total),
        wall: median(wall),
    })
}

async fn bench_node_bulk(db: &Uni, sz: usize) -> anyhow::Result<f64> {
    let mut wall = Vec::new();
    for rep in 0..REPS {
        let props = node_props(sz, &format!("nb{rep}_"));
        let tx = db.session().tx().await?;
        let t = Instant::now();
        tx.bulk_insert_vertices("Person", props).await?;
        wall.push(us(t.elapsed()));
        tx.rollback();
    }
    Ok(median(wall))
}

fn edge_param(sz: usize, pool: &[u64], rep: usize) -> Value {
    Value::List(
        (0..sz)
            .map(|i| {
                let s = pool[(rep * 131 + i * 2) % pool.len()];
                let d = pool[(rep * 137 + i * 2 + 1) % pool.len()];
                let mut m = HashMap::new();
                m.insert("src".to_string(), Value::Int(s as i64));
                m.insert("dst".to_string(), Value::Int(d as i64));
                m.insert("role".to_string(), Value::String("member".to_string()));
                Value::Map(m)
            })
            .collect(),
    )
}

async fn bench_edge_cypher(db: &Uni, sz: usize, pool: &[u64]) -> anyhow::Result<Split> {
    let mut parse = Vec::new();
    let mut plan = Vec::new();
    let mut exec = Vec::new();
    let mut total = Vec::new();
    let mut wall = Vec::new();
    for rep in 0..REPS {
        let edges = edge_param(sz, pool, rep);
        let tx = db.session().tx().await?;
        let t = Instant::now();
        let res = tx
            .execute_with(EDGE_CYPHER)
            .param("edges", edges)
            .run()
            .await?;
        let w = t.elapsed();
        let m = res.metrics();
        parse.push(us(m.parse_time));
        plan.push(us(m.plan_time));
        exec.push(us(m.exec_time));
        total.push(us(m.total_time));
        wall.push(us(w));
        tx.rollback();
    }
    Ok(Split {
        parse: median(parse),
        plan: median(plan),
        exec: median(exec),
        total: median(total),
        wall: median(wall),
    })
}

async fn bench_edge_bulk(db: &Uni, sz: usize, pool: &[u64]) -> anyhow::Result<f64> {
    use uni_db::Vid;
    let mut wall = Vec::new();
    for rep in 0..REPS {
        let edges: Vec<(Vid, Vid, HashMap<String, Value>)> = (0..sz)
            .map(|i| {
                let s = pool[(rep * 131 + i * 2) % pool.len()];
                let d = pool[(rep * 137 + i * 2 + 1) % pool.len()];
                let mut h = HashMap::new();
                h.insert("role".to_string(), Value::String("member".to_string()));
                (Vid::new(s), Vid::new(d), h)
            })
            .collect();
        let tx = db.session().tx().await?;
        let t = Instant::now();
        tx.bulk_insert_edges("LINK", edges).await?;
        wall.push(us(t.elapsed()));
        tx.rollback();
    }
    Ok(median(wall))
}

async fn profile_node(db: &Uni) -> anyhow::Result<Vec<(String, usize, f64)>> {
    let tx = db.session().tx().await?;
    let (_r, prof) = tx
        .execute_with(NODE_CYPHER)
        .param("rows", node_rows(1, "prof_"))
        .profile()
        .await?;
    let out = prof
        .runtime_stats
        .iter()
        .map(|o| (o.operator.clone(), o.actual_rows, o.time_ms))
        .collect();
    tx.rollback();
    Ok(out)
}

async fn profile_edge(db: &Uni, pool: &[u64]) -> anyhow::Result<Vec<(String, usize, f64)>> {
    let tx = db.session().tx().await?;
    let (_r, prof) = tx
        .execute_with(EDGE_CYPHER)
        .param("edges", edge_param(1, pool, 7))
        .profile()
        .await?;
    let out = prof
        .runtime_stats
        .iter()
        .map(|o| (o.operator.clone(), o.actual_rows, o.time_ms))
        .collect();
    tx.rollback();
    Ok(out)
}
