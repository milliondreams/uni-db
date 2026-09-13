//! Which first-party operators appear in the LDBC SNB plans? (#261, step 1)
//!
//! #261 lists sixteen `ExecutionPlan` impls that register no `MemoryConsumer`,
//! and then refuses to treat the list as a work queue: "a static list is a
//! starting point for measurement, not a work queue". Step 1 is to establish
//! which of them appear in a query whose peak is actually a problem.
//!
//! # Why not `EXPLAIN`
//!
//! The first version of this probe called `Session::explain()` and reported
//! **zero** occurrences of every operator — including `GraphScanExec`, which
//! every one of these plans must contain. `ExplainOutput::plan_text` is the
//! *logical* plan; it never names a physical operator, so the instrument could
//! only ever have returned zero. It was caught by carrying the three
//! pool-accounted operators as controls, which is the only reason a
//! uniformly-empty result read as a broken instrument rather than a finding.
//!
//! # What this does instead
//!
//! `UNI_DUMP_PHYSICAL` makes the executor print the physical plan to stderr
//! **before** it pulls the first batch. So each query is started and then cut
//! off by a short timeout: the plan is captured, and the hours of SF1 execution
//! behind it are not paid.
//!
//! # What it cannot see
//!
//! Sub-plans built during execution. `GraphApplyExec` and `RecursiveCTEExec`
//! plan their bodies per row / per iteration, so those subtrees are absent from
//! the top-level dump. A zero here for an operator that only ever appears
//! inside a sub-plan is an artifact of the instrument, not evidence of absence.
//!
//!   LDBC_DB=/path/to/store cargo run --release -p uni-db \
//!     --example operator_census 2>&1 | tee census.txt
use std::time::Duration;

#[path = "../benches/ldbc/params.rs"]
mod params;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Set before the first query: the executor reads it once into a `LazyLock`.
    unsafe { std::env::set_var("UNI_DUMP_PHYSICAL", "1") };
    // A generous pool. This probe is a plan census, not a memory measurement:
    // the default 1 GiB stops parameter derivation before the corpus is even
    // reached (`GraphTraverseExec` asks for 614.7 MB of a 409.9 MB remainder),
    // and a census that never plans a query measures nothing.
    // `CENSUS_POOL_MB` and `CENSUS_TIMEOUT_S` make this double as an
    // attribution probe: at the shipped 1 GiB default with a real timeout, a
    // query that refuses tells you *which* operator could not fit, which is the
    // step the census itself cannot take.
    let pool_mb: usize = std::env::var("CENSUS_POOL_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32 * 1024);
    let timeout_s: u64 = std::env::var("CENSUS_TIMEOUT_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    // Separate from the database's pool, for the same reason the tests apply
    // their ceilings per query: parameter derivation does not fit in 1 GiB (it
    // dies in `GraphTraverseExec` asking 614.7 MB of a 409.9 MB remainder), so a
    // database-wide ceiling never reaches the corpus at all.
    let query_mb: usize = std::env::var("CENSUS_QUERY_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let only = std::env::var("CENSUS_ONLY").ok();
    eprintln!("[census] pool={pool_mb} MB timeout={timeout_s}s only={only:?}");
    let config = uni_db::UniConfig {
        max_query_memory: pool_mb * 1024 * 1024,
        ..Default::default()
    };
    let db = uni_db::Uni::open_existing(std::env::var("LDBC_DB")?)
        .config(config)
        .build()
        .await?;
    eprintln!("=== deriving parameters ===");
    let p = params::derive(&db).await?;
    let dir = std::path::Path::new("crates/uni/benches/ldbc/queries");
    let mut names: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "cypher"))
        .collect();
    names.sort();
    for path in names {
        let q = std::fs::read_to_string(&path)?;
        let stem = path.file_stem().unwrap().to_string_lossy().to_string();
        if only.as_ref().is_some_and(|o| o != &stem) {
            continue;
        }
        eprintln!("=== {stem} ===");
        let session = db.session();
        let mut qb = session
            .query_with(&q)
            .timeout(Duration::from_secs(timeout_s));
        if query_mb > 0 {
            qb = qb.max_memory(query_mb * 1024 * 1024);
        }
        for (k, v) in &p {
            qb = qb.param(k, v.clone());
        }
        // The outcome is irrelevant and a timeout is the expected one: the plan
        // has already been printed by the time the first batch is pulled.
        match qb.fetch_all().await {
            Ok(r) => eprintln!("--- {stem}: completed, {} rows", r.rows().len()),
            Err(e) => eprintln!("--- {stem}: {e}"),
        }
    }
    Ok(())
}
