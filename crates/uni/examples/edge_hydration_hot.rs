//! The edge-property hydration query on its own, for a profiler (#228).
//!
//! [`edge_hydration_probe`](../edge_hydration_probe/index.html) measures *how
//! much* one edge property costs by differential. This one just runs the
//! expensive half in a loop so `perf record` has something to attribute, with
//! nothing else in the process competing for samples.
//!
//! Two mechanisms have already been measured and rejected as the dominant cost,
//! which is why this exists rather than a third guess:
//!
//! * the `serde_json` round trip in `MainEdgeDataset::parse_props_json` — real,
//!   but worth ~10% of hydration;
//! * the per-cell `serde_json::Value` in `value_codec::decode_column_value` —
//!   worth nothing measurable, though it was a genuine correctness fix.
//!
//! ```text
//! LDBC_DB=$HOME/uni-bench-tmp/ldbc-work-216 \
//!   perf record -g --call-graph dwarf -F 199 -- \
//!   ./target/release/examples/edge_hydration_hot
//! perf report --stdio --no-children | head -60
//! ```

use uni_db::Uni;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::var("LDBC_DB")?;
    let db = Uni::open(&root).build().await?;
    let session = db.session();

    // One warm pass so the samples are steady-state rather than first-touch IO.
    let q = "MATCH (a:Forum)-[r:HAS_MEMBER]->(b:Person) RETURN count(r.joinDate) AS c";
    session.query(q).await?;

    for _ in 0..3 {
        let r = session.query(q).await?;
        println!(
            "rows: {:?}",
            r.rows().first().map(|x| x.values()[0].clone())
        );
    }

    db.shutdown().await?;
    Ok(())
}
