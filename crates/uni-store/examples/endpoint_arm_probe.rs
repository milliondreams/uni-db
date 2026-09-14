// Rust guideline compliant
//! Does the endpoint-vid arm of `find_edges_by_type_names` ever want a full
//! scan? (#237, site 2)
//!
//! `MainEdgeDataset::find_edges_by_type_names_counted` picks its read strategy
//! by which `match` arm the caller lands in: `None` scans the whole edge type,
//! `Some((side, vids))` chunks `src_vid`/`dst_vid` `IN (...)` at a fixed
//! `VID_CHUNK = 8192`. The selectivity helper that would decide it on measured
//! cost -- `prefers_full_scan`, added by #221 -- sits in the same file and is
//! called only from the eid path.
//!
//! #237 is explicit that #221's constants must NOT be copied across, and this
//! probe exists to satisfy its acceptance: built before any fix, so it can fail
//! first, and able to report "no crossover" as a real answer.
//!
//! # Method
//!
//! Both arms run over the SAME edge type on a real SF1 store and are made to
//! return the same edges, so the only difference is read strategy:
//!
//! * **lookup** -- `Some((Src, vids))`, the chunked `IN (...)` path.
//! * **scan** -- `None`, one pass over the type, filtered to the same vids in
//!   memory afterwards. That is the floor for a scan-and-filter arm, so a
//!   crossover reported here is optimistic for the scan side and reads as "no
//!   earlier than".
//!
//! `K` (requested src vids) is swept across three orders of magnitude. A run
//! reporting the same cost at every `K` is measuring nothing, so the sweep is
//! its own control; and the equal-result assertion is what licenses reading the
//! timings as strategy rather than as two different reads.
//!
//! # What it found
//!
//! A crossover exists, and the current code is on the wrong side of it for
//! large requests. LDBC SF1, `HAS_MEMBER` (1 611 869 edges, 79 470 distinct
//! src vids), min-of-3, release:
//!
//! | K (src vids) | lookup ms | scan ms | edges returned |
//! |---|---|---|---|
//! | 100 | 21 | 840 | 2 446 |
//! | 1 000 | 43 | 840 | 34 328 |
//! | 10 000 | 389 | 840 | 268 873 |
//! | 20 000 | 717 | 840 | 501 546 |
//! | 30 000 | 1 054 | 840 | 732 193 |
//! | 40 000 | 1 429 | 840 | 951 770 |
//! | 60 000 | 2 025 | 840 | 1 346 580 |
//! | 79 000 | 2 485 | 840 | 1 607 828 |
//!
//! The lookup arm is linear in K and the scan arm is flat, as their shapes
//! predict. They cross between K = 20 000 and K = 30 000 — about 28% of the
//! type's distinct sources, or ~34% of its edges. At full breadth the chunked
//! lookup the code always takes is **3.0x slower** than the scan it never
//! considers.
//!
//! # Why this does not yet become a rule
//!
//! The decision needs a denominator, and the cheap ones are the wrong ones.
//! `count_rows("main_edges", None)` is metadata-only but counts *every* edge
//! type (~17.3M rows at SF1), while the scan arm here is bounded by the `type`
//! predicate — so the ratio that matters is K against the *type's* row count,
//! and asking for that is itself a filtered count, which scans.
//!
//! That is #260 exactly: no cached cardinality statistic exists, so a
//! selectivity rule has nowhere to read its denominator. This probe is the
//! measured consumer that makes #260 concrete rather than speculative — the
//! crossover is known, and what is missing is the statistic to compare against.
//! Fitting a constant to K alone would reproduce the mistake #221's rustdoc
//! already concedes and #237 explicitly refuses.
//!
//! Throwaway diagnostic, not shipping code.
//!
//!   LDBC_DB=$HOME/uni-bench-tmp/ldbc-work-216 \
//!     cargo run --release -p uni-store --example endpoint_arm_probe

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use uni_store::LanceDbBackend;
use uni_store::backend::StorageBackend;
use uni_store::storage::main_edge::{EndpointSide, MainEdgeDataset};

use uni_common::core::id::Vid;

/// Requested src-vid counts to sweep.
const KS: &[usize] = &[100, 1_000, 10_000, 20_000, 30_000, 40_000, 60_000, 79_000];

/// Samples per measurement; the minimum is reported.
const SAMPLES: usize = 3;

/// The edge type probed. `HAS_MEMBER` is the one #222/#228 measured, so its
/// shape is already characterised elsewhere in this repo.
const EDGE_TYPE: &str = "HAS_MEMBER";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = std::env::var("LDBC_DB").map_err(|_| anyhow::anyhow!("set LDBC_DB"))?;
    let uri = format!("{db}/storage");
    let backend: Arc<dyn StorageBackend> = Arc::new(LanceDbBackend::connect(&uri, None).await?);

    // The scan arm is independent of K; measure it once, and take the vid
    // population from its own result so both arms provably cover the same
    // edges. Deriving the vids from a separate ad-hoc scan risked disagreeing
    // with the arm under test -- the first version of this probe did, and
    // reported zero distinct vids against a 1.6M-edge type.
    let mut scan_ms = f64::MAX;
    let mut scan_rows = 0usize;
    for _ in 0..SAMPLES {
        let t = Instant::now();
        let all = MainEdgeDataset::find_edges_by_type_names(&*backend, &[EDGE_TYPE], None).await?;
        scan_ms = scan_ms.min(t.elapsed().as_secs_f64() * 1000.0);
        scan_rows = all.len();
    }
    println!("scan arm (None): {scan_ms:.0} ms, {scan_rows} edges");

    let all = MainEdgeDataset::find_edges_by_type_names(&*backend, &[EDGE_TYPE], None).await?;
    let mut seen: HashSet<u64> = HashSet::new();
    let mut srcs: Vec<Vid> = Vec::new();
    for (_, src, ..) in &all {
        if seen.insert(src.as_u64()) {
            srcs.push(*src);
        }
    }
    println!("distinct {EDGE_TYPE} src vids: {}\n", srcs.len());

    println!(
        "{:>8}  {:>12}  {:>12}  {:>8}",
        "K", "lookup ms", "scan ms", "edges"
    );
    for &k in KS {
        if k > srcs.len() {
            continue;
        }
        let vids = &srcs[..k];
        let mut lookup_ms = f64::MAX;
        let mut rows = 0usize;
        for _ in 0..SAMPLES {
            let t = Instant::now();
            let got = MainEdgeDataset::find_edges_by_type_names(
                &*backend,
                &[EDGE_TYPE],
                Some((EndpointSide::Src, vids)),
            )
            .await?;
            lookup_ms = lookup_ms.min(t.elapsed().as_secs_f64() * 1000.0);
            rows = got.len();
        }
        println!("{k:>8}  {lookup_ms:>12.0}  {scan_ms:>12.0}  {rows:>8}");
    }
    Ok(())
}
