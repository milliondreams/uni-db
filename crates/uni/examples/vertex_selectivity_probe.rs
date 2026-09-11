//! Where does chunked vertex hydration lose to a full scan? (#237)
//!
//! #221 gave the *edge* path a measured scan-vs-lookup choice
//! (`MainEdgeDataset::prefers_full_scan`, fitted to `EID_SCAN_CROSSOVER_RATIO`
//! and `EID_SPAN_RATIO`). Three sibling sites still choose with a fixed
//! constant, and the vertex ones say so — `scan.rs` ends its comment block
//! with, verbatim, "A selectivity-aware choice would beat a fixed constant."
//!
//! #237 is explicit that the edge constants must not be copied across: vertex
//! tables are per-label and much narrower than the unified 17.3M-row edge
//! table, and #221's rustdoc concedes its numbers are "fitted to one dataset on
//! one machine". So this measures the vertex side on its own terms.
//!
//! # Reaching the site
//!
//! `hydrate_vids_columnar` (`scan.rs`, chunked at `MAX_VIDS_PER_CHUNK`) is
//! reached from **traversal-target hydration**:
//! `traverse.rs::build_target_property_columns` returns into it whenever a
//! traversal has a typed target label and the requested properties are not
//! `_all_props`. It is *not* reached by a `WHERE id(n) IN ...` predicate — an
//! earlier version of this probe assumed it was, and measured
//! `rows_scanned = <whole table>` at every `K`: a full scan against a full
//! scan, with the wall-time spread coming from post-filtering. The `scanned`
//! columns below exist so that cannot recur silently.
//!
//! # The instrument: a differential
//!
//! Hydration cost cannot be read off a traversal directly, because the
//! traversal dominates it. So: two queries over the *same* anchor and the same
//! edges, differing only in whether a target property is read.
//!
//! * `RETURN count(id(t))`    — traversal, target vids materialised, no property.
//! * `RETURN count(t.<prop>)` — the same, plus target-property hydration.
//!
//! The anchor scan, the adjacency walk and target-vid materialisation are
//! common to both and cancel; the difference is hydration. `count(*)` is
//! deliberately *not* the baseline: it binds no path variable and silently
//! takes an endpoint-only strategy, which is how an earlier probe in this repo
//! reported a flat peak for code it never reached.
//!
//! Against that, the arm a selectivity-aware choice would switch to:
//!
//! * `MATCH (t:T) RETURN count(t.<prop>)` — one unfiltered pass over the target
//!   table. That is the *floor* for a scan-and-filter arm, not its exact cost,
//!   so a crossover reported here is optimistic for the scan side and reads as
//!   "no earlier than".
//!
//! # What it found
//!
//! Not a scan-versus-lookup crossover. The dominant cost was that hydration is
//! called once per traversal *batch* (~8 192 target vids per call, carrying
//! ~1 400-2 500 distinct ones) and re-fetched a target once per occurrence:
//!
//! | traversal | target rows | requests | distinct | before | after |
//! |---|---|---|---|---|---|
//! | `HAS_CREATOR`->Person | 9 892 | 2 052 169 | 9 343 | 4 247 ms | 1 937 ms |
//! | `KNOWS`->Person | 9 892 | 180 623 | 8 466 | 364 ms | 206 ms |
//! | `REPLY_OF`->Comment | 2 052 169 | 1 040 749 | 441 704 | 5 732 ms | 5 942 ms |
//!
//! Deduplicating unconditionally cost the third case 1.3x, and sorting rather
//! than preserving order measured the same, so the cost is the deduplication
//! itself. `scan.rs`'s `DEDUP_TABLE_RATIO` therefore gates it on the target
//! table's row count -- the selectivity-aware choice #237 asks for, keyed on
//! duplication rather than on chunk size.
//!
//! Two of #237's three sites remain: `vid_lookup_join.rs` chunks a vid set that
//! is already distinct, so it has no duplication to remove and needs its own
//! measurement; and `main_edge.rs`'s endpoint-vid arm picks between a chunked
//! lookup and a full scan by which `match` arm the caller lands in, with
//! `prefers_full_scan` never consulted -- a missing cost test rather than a
//! constant-fitted one.
//!
//! # Two axes
//!
//! `K` (target vids hydrated) is swept by restricting the anchor. Because the
//! measurement is a difference between two queries sharing that anchor, the
//! restriction's own cost cancels too.
//!
//! `N` (target table rows) varies across cases: `Person` is ~9.9k rows,
//! `Comment` ~2.05M. A rule fitted on one would not transfer to the other,
//! which is exactly why #237 refuses to copy the edge constants across.
//!
//! A run reporting the same hydration cost at every `K` is measuring nothing;
//! the `K`-sweep is its own control.
//!
//! ```text
//! LDBC_DB=$HOME/uni-bench-tmp/ldbc-work-216 \
//!   cargo run --release -p uni-db --example vertex_selectivity_probe
//! ```

use std::time::Instant;

use uni_db::{Uni, Value};

/// Min-of-N, so one scheduling hiccup does not become the reported cost.
const SAMPLES: usize = 3;

struct Measured {
    ms: f64,
    scanned: usize,
    rows: i64,
}

async fn timed(session: &uni_db::Session, q: &str) -> Measured {
    let mut best = f64::MAX;
    let mut scanned = 0usize;
    let mut rows = -1i64;
    for _ in 0..SAMPLES {
        let t = Instant::now();
        let r = session.query(q).await.expect("probe query failed");
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        if ms < best {
            best = ms;
            scanned = r.metrics().rows_scanned;
        }
        rows = match r.rows().first().map(|x| x.values()[0].clone()) {
            Some(Value::Int(i)) => i,
            _ => -1,
        };
    }
    Measured {
        ms: best,
        scanned,
        rows,
    }
}

/// One traversal shape: anchor label, edge type, target label, target property.
struct Case {
    anchor: &'static str,
    edge: &'static str,
    target: &'static str,
    prop: &'static str,
    /// Anchor restrictions, coarse to fine, used to sweep `K`.
    limits: &'static [&'static str],
}

const CASES: &[Case] = &[
    // Small target table (~9.9k rows), so even a modest `K` is a large
    // fraction of it — the regime where a full scan should start to win.
    Case {
        anchor: "Person",
        edge: "KNOWS",
        target: "Person",
        prop: "firstName",
        limits: &["p.id < 100", "p.id < 1000", "p.id < 10000", "true"],
    },
    // Large `K` into that same small table: every Comment's creator.
    Case {
        anchor: "Comment",
        edge: "HAS_CREATOR",
        target: "Person",
        prop: "firstName",
        limits: &["p.id < 100000", "p.id < 1000000", "true"],
    },
    // Large target table (~2.05M rows), where a scan is expensive and chunked
    // lookups should keep winning much longer.
    Case {
        anchor: "Comment",
        edge: "REPLY_OF",
        target: "Comment",
        prop: "length",
        limits: &["p.id < 100000", "p.id < 1000000", "true"],
    },
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::var("LDBC_DB")?;
    let db = Uni::open(&root).build().await?;
    let session = db.session();

    for case in CASES {
        let Case {
            anchor,
            edge,
            target,
            prop,
            limits,
        } = case;

        // The arm a selectivity-aware choice would switch to.
        let scan = timed(
            &session,
            &format!("MATCH (t:{target}) RETURN count(t.{prop}) AS c"),
        )
        .await;
        println!(
            "\n=== ({anchor})-[:{edge}]->({target}).{prop} — target table {} rows ===",
            scan.rows
        );
        println!(
            "full scan of {target}.{prop}: {:.1} ms  scanned={}",
            scan.ms, scan.scanned
        );
        println!(
            "{:>22}  {:>10}  {:>10}  {:>10}  {:>10}  {:>12}  {:>9}",
            "anchor restriction",
            "K",
            "no-prop ms",
            "prop ms",
            "hydrate ms",
            "prop scanned",
            "vs scan"
        );

        for lim in *limits {
            let base = timed(
                &session,
                &format!(
                    "MATCH (p:{anchor})-[:{edge}]->(t:{target}) WHERE {lim} \
                     RETURN count(id(t)) AS c"
                ),
            )
            .await;
            let hyd = timed(
                &session,
                &format!(
                    "MATCH (p:{anchor})-[:{edge}]->(t:{target}) WHERE {lim} \
                     RETURN count(t.{prop}) AS c"
                ),
            )
            .await;

            // The differential, clamped at zero: below the noise floor the two
            // queries are indistinguishable, and a negative number would be
            // measurement error dressed up as a saving.
            let hydrate_ms = (hyd.ms - base.ms).max(0.0);
            println!(
                "{lim:>22}  {:>10}  {:>10.1}  {:>10.1}  {:>10.1}  {:>12}  {:>9}",
                base.rows,
                base.ms,
                hyd.ms,
                hydrate_ms,
                hyd.scanned,
                format!("{:.2}x", hydrate_ms / scan.ms)
            );
        }
    }

    println!(
        "\nRead `vs scan` as hydration cost over the cost of reading the whole\n\
         target table. Above 1.00x a full scan would have been cheaper, so any K\n\
         at which that happens is a crossover the fixed chunk constant misses."
    );

    db.shutdown().await?;
    Ok(())
}
