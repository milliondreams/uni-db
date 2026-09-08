//! Where does a variable-length traversal's peak live? (#241)
//!
//! #241 observes that neither VLP stream has `slice_size` — they emit one fully
//! materialised batch per input batch — and asks for a probe that could
//! *falsify* "the peak lives in the VLP expansion accumulation" before any fix.
//! The issue names two prior cases where an LDBC peak was confidently
//! attributed to a mechanism the query did not use, which is what this exists
//! to avoid repeating.
//!
//! # The hypothesis, stated so it can fail
//!
//! `VarLengthStreamState::Reading` accumulates `Vec<VarLengthExpansion>`, one
//! entry per **path**, each carrying a full `node_path: Vec<Vid>` and
//! `edge_path: Vec<Eid>`. If that is where the memory goes, peak RSS tracks the
//! **number of paths**, not the number of rows returned and not the size of the
//! graph.
//!
//! So the query returns `count(*)` — one row, always. The graph is held at a
//! fixed node and edge count across every measurement. The only thing that
//! varies is the maximum hop count, which is what multiplies path count.
//!
//! If peak stays flat while paths grow by orders of magnitude, the hypothesis
//! is dead and the fix in #241 would be aimed at the wrong operator.
//!
//! # Controls
//!
//! * **depth 1** — a single hop takes `GraphTraverseStream`, which *does* chunk
//!   and slice. It reads the same graph and returns the same shape, so it is
//!   the "bounded sibling" baseline #241 is arguing from.
//! * **path count is printed beside the peak**, so a peak that grows with
//!   something other than paths is visible rather than assumed.
//!
//! Peak is `VmHWM` — the kernel's own high-water mark for the process — read at
//! exit. It never decreases, so each depth runs in its own process:
//!
//!   cargo build --release -p uni-db --example vlp_peak_probe
//!   for d in 1 2 3 4 5; do VLP_DEPTH=$d ./target/release/examples/vlp_peak_probe; done

use uni_db::{DataType, Uni, Value};

/// Nodes per layer. Fan-out per hop, so paths grow as `WIDTH^depth`.
///
/// Sized so the top depth produces ~1e6 paths: at ~100 bytes of `node_path` +
/// `edge_path` per path that is a signal well clear of RSS noise, while the
/// graph itself stays at 70 nodes and 600 edges — trivial, and identical at
/// every depth. A smaller mesh was tried first and its top depth landed around
/// 8k paths, which is under the noise floor and would have made a flat result
/// meaningless rather than falsifying.
const WIDTH: usize = 10;
/// Layers in the mesh. Fixed across depths so the graph never changes size.
const LAYERS: usize = 7;

fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let depth: usize = std::env::var("VLP_DEPTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("layer", DataType::Int)
        .property("idx", DataType::Int)
        .apply()
        .await?;
    db.schema().edge_type("E", &["N"], &["N"]).apply().await?;

    // A layered mesh: every node in layer i points at every node in layer i+1.
    // Node and edge counts are identical for every depth measured, so a peak
    // that tracks depth cannot be explained by the graph getting bigger.
    let tx = db.session().tx().await?;
    for layer in 0..LAYERS {
        tx.query_with("UNWIND range(0, $w - 1) AS i CREATE (:N {layer: $l, idx: i})")
            .param("w", Value::Int(WIDTH as i64))
            .param("l", Value::Int(layer as i64))
            .fetch_all()
            .await?;
    }
    for layer in 0..LAYERS - 1 {
        tx.query_with("MATCH (a:N {layer: $l}), (b:N {layer: $next}) CREATE (a)-[:E]->(b)")
            .param("l", Value::Int(layer as i64))
            .param("next", Value::Int(layer as i64 + 1))
            .fetch_all()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let baseline = peak_rss_kb();

    // Two strategies, selected by whether a path variable is bound:
    //
    //   A. endpoint-only BFS — no path variable, so the traversal never
    //      enumerates paths and dedups to reachable endpoints.
    //   B. full path enumeration — `p` is bound, which is what builds the
    //      `Vec<VarLengthExpansion>` under test.
    //
    // Both return exactly one row. Measuring both is what localises the peak:
    // if only B grows, the memory is in path enumeration and not in the
    // traversal generally. A first version of this probe measured only A and
    // reported a flat peak with path counts growing *linearly* in depth — the
    // giveaway that it had never reached the mechanism at all.
    let mode = std::env::var("VLP_MODE").unwrap_or_else(|_| "B".into());
    let q = if mode == "A" {
        format!("MATCH (a:N {{layer: 0}})-[:E*1..{depth}]->(b:N) RETURN count(*) AS c")
    } else {
        format!("MATCH p = (a:N {{layer: 0}})-[:E*1..{depth}]->(b:N) RETURN count(p) AS c")
    };
    let secs: u64 = std::env::var("VLP_TIMEOUT_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let t0 = std::time::Instant::now();
    // The pool is the operator's *own* accounting of what it holds, so a
    // ceiling it passes is a far better bound-check than RSS: RSS includes
    // allocator retention and everything downstream, the pool includes only
    // what this operator reserved and has not released.
    let session = db.session();
    let mut q_builder = session
        .query_with(&q)
        .timeout(std::time::Duration::from_secs(secs));
    if let Some(mb) = std::env::var("VLP_MAX_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        q_builder = q_builder.max_memory(mb * 1024 * 1024);
    }
    let r = q_builder.fetch_all().await?;
    let elapsed = t0.elapsed();
    let paths = match &r.rows()[0].values()[0] {
        Value::Int(i) => *i,
        other => panic!("expected a count, got {other:?}"),
    };

    let peak = peak_rss_kb();
    println!(
        "mode={mode} depth={depth:>2}  counted={paths:>9}  rows_returned=1  \
         peak_rss={:>7} MB  (delta_after_fixture={:>6} MB)  took={:?}",
        peak / 1024,
        (peak.saturating_sub(baseline)) / 1024,
        elapsed
    );

    db.shutdown().await?;
    Ok(())
}
