//! What does reading one edge property cost? (#228)
//!
//! #228 observes that edge properties have no columnar hydration path: they
//! come back as one `HashMap` per edge and are transposed into Arrow a row at a
//! time, where vertex properties got a columnar path in #209. It also says,
//! correctly, that the existing measurement cannot size the fix — scattered-EID
//! hydration measures ~100 us/edge and *both* random IO and per-edge map
//! construction scale linearly with edge count, so that number does not
//! attribute between them.
//!
//! This separates them.
//!
//! # The instrument: the same differential as #237's vertex probe
//!
//! Edge-property cost cannot be read off a traversal directly, because the
//! traversal dominates it. So: two queries over the *same* anchor and the same
//! edges, differing only in whether an edge property is read.
//!
//! * `RETURN count(id(r))` — traversal, edges materialised, no property read.
//! * `RETURN count(r.<prop>)` — the same, plus edge-property hydration.
//!
//! The anchor scan, the adjacency walk and edge materialisation are common to
//! both and cancel; the difference is hydration. `count(*)` is deliberately not
//! the baseline — it binds no path variable and silently takes an endpoint-only
//! strategy, which is how an earlier probe in this repo reported a flat peak for
//! code it never reached.
//!
//! # Warm versus cold
//!
//! Each case is run twice over the same EID set. The first pass pays whatever
//! IO the read needs; the second finds the pages resident. Per-edge map
//! construction is paid in full on *both* passes, because it is CPU work over
//! the decoded blob rather than a read. So:
//!
//! * a cost that collapses on the warm pass was IO — issue #221's territory;
//! * a cost that survives it is materialisation — this issue's.
//!
//! That is the control the issue asks for, and it is what makes the number
//! attributable rather than merely large.
//!
//! # What it found
//!
//! Warm and cold agree to within a few percent in every case, and per-edge cost
//! is flat across a 200x range of edge counts — so this is per-edge CPU work,
//! not IO. Reading one `Int` column off `HAS_MEMBER` costs 16x the traversal
//! that produced the edges.
//!
//! | traversal | edges | hydrate (warm) | (cold) | per edge |
//! |---|---|---|---|---|
//! | `HAS_MEMBER`->Person `.joinDate` | 1 611 869 | 9 204 ms | 9 093 ms | 5.71 us |
//! | `KNOWS`->Person `.creationDate` | 180 623 | 967 ms | 1 024 ms | 5.35 us |
//! | `STUDY_AT`->Org `.classYear` | 7 949 | 39 ms | 44 ms | 4.94 us |
//!
//! # Where that cost is *not*
//!
//! #228 attributes it to row-oriented materialisation, and a `perf` profile of
//! the same query (`edge_hydration_hot.rs`) says otherwise:
//!
//! | frame | share |
//! |---|---|
//! | `lance_io` scheduler + `get_range` | ~15% |
//! | `lance_encoding` decode | ~6% |
//! | `merge_winning_props` (blob path) | ~6% |
//! | `build_property_column_static` | **~2%** |
//!
//! `build_property_column_static` *is* the row-by-row Arrow assembly the issue
//! names, and it is ~2% of the query. Mirroring `hydrate_vids_columnar` for
//! edges would therefore buy a few percent while owing an exact reproduction of
//! three sets of MVCC merge semantics (per-type delta tables, the L0 overlay,
//! and the main-edges fallback) against the vertex path's one.
//!
//! Two mechanisms were tried and measured rather than argued:
//!
//! * taking the decoded map directly in `parse_props_json` instead of
//!   round-tripping it through `serde_json` — **9 204 ms to 8 294 ms (-10%)**,
//!   repeated at -8.3% and -6.0% on the other two cases;
//! * dropping the per-cell `serde_json::Value` in
//!   `value_codec::decode_column_value` — **no measurable change** (8 294 to
//!   8 302 ms, inside noise), though it fixed a silent wrong answer.
//!
//! The remaining cost is reading the column out of Lance, which is a scan-shape
//! question (#218, #221), not a materialisation one.
//!
//! ```text
//! LDBC_DB=$HOME/uni-bench-tmp/ldbc-work-216 \
//!   cargo run --release -p uni-db --example edge_hydration_probe
//! ```

use std::time::Instant;

use uni_db::{Uni, Value};

/// Min-of-N once warm, so one scheduling hiccup does not become the number.
const SAMPLES: usize = 3;

struct Measured {
    cold_ms: f64,
    warm_ms: f64,
    rows: i64,
}

async fn timed(session: &uni_db::Session, q: &str) -> Measured {
    // Cold: whatever this process has not touched yet.
    let t = Instant::now();
    let r = session.query(q).await.expect("probe query failed");
    let cold_ms = t.elapsed().as_secs_f64() * 1000.0;
    let rows = match r.rows().first().map(|x| x.values()[0].clone()) {
        Some(Value::Int(i)) => i,
        _ => -1,
    };

    let mut warm = f64::MAX;
    for _ in 0..SAMPLES {
        let t = Instant::now();
        session.query(q).await.expect("probe query failed");
        warm = warm.min(t.elapsed().as_secs_f64() * 1000.0);
    }
    Measured {
        cold_ms,
        warm_ms: warm,
        rows,
    }
}

/// One traversal shape: anchor label, edge type, the edge property to read.
struct Case {
    anchor: &'static str,
    edge: &'static str,
    target: &'static str,
    prop: &'static str,
}

const CASES: &[Case] = &[
    // IC5's clause: the one #228 sizes at ~1.59M maps for a single Int column.
    Case {
        anchor: "Forum",
        edge: "HAS_MEMBER",
        target: "Person",
        prop: "joinDate",
    },
    // A larger edge set with a property, to check the cost scales with edges
    // rather than with the anchor.
    Case {
        anchor: "Person",
        edge: "KNOWS",
        target: "Person",
        prop: "creationDate",
    },
    // University/company membership: smaller, and a second property type.
    Case {
        anchor: "Person",
        edge: "STUDY_AT",
        target: "Organisation",
        prop: "classYear",
    },
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::var("LDBC_DB")?;
    let db = Uni::open(&root).build().await?;
    let session = db.session();

    println!(
        "{:>34}  {:>10}  {:>11}  {:>11}  {:>11}  {:>11}",
        "traversal", "edges", "base warm", "prop warm", "hydrate", "hydrate/edge"
    );

    for case in CASES {
        let Case {
            anchor,
            edge,
            target,
            prop,
        } = case;
        let label = format!("({anchor})-[:{edge}]->({target}).{prop}");

        let base = timed(
            &session,
            &format!("MATCH (a:{anchor})-[r:{edge}]->(b:{target}) RETURN count(id(r)) AS c"),
        )
        .await;
        let hyd = timed(
            &session,
            &format!("MATCH (a:{anchor})-[r:{edge}]->(b:{target}) RETURN count(r.{prop}) AS c"),
        )
        .await;

        if base.rows <= 0 {
            println!("{label:>34}  (no rows — edge type absent from this corpus)");
            continue;
        }

        // Clamped at zero: below the noise floor the two queries are
        // indistinguishable, and a negative number would be measurement error
        // dressed up as a saving.
        let warm_hydrate = (hyd.warm_ms - base.warm_ms).max(0.0);
        let cold_hydrate = (hyd.cold_ms - base.cold_ms).max(0.0);
        let per_edge_us = warm_hydrate * 1000.0 / base.rows as f64;

        println!(
            "{label:>34}  {:>10}  {:>9.1}ms  {:>9.1}ms  {:>9.1}ms  {:>9.2}us",
            base.rows, base.warm_ms, hyd.warm_ms, warm_hydrate, per_edge_us
        );
        println!(
            "{:>34}  cold hydrate {cold_hydrate:.1}ms vs warm {warm_hydrate:.1}ms — \
             the share that survives warming is materialisation, not IO",
            ""
        );
    }

    db.shutdown().await?;
    Ok(())
}
