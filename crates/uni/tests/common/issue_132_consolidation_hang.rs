//! Issue #132 — real-workload stress reproduction.
//!
//! A post-ingest consolidation sweep (many small `MATCH … RETURN` reads + node
//! and edge writes) over a schema carrying a dense `Vector`, a `SparseVector`,
//! AND a `List(Vector)` (multi-vector) column intermittently hung the entire
//! tokio runtime — a stalled sparse/multi-vector flush stream never submitted
//! its rotate-seq, wedging the finalizer and, via back-pressure saturation,
//! parking every later commit on `flush_lock`. Dense-only never hung.
//!
//! This exercises the exact flush path (`flush_stream_l1` →
//! `update_sparse_vector_index_incremental` + `materialize_fde_columns`) under
//! realistic churn — sparse/multivec data is inserted directly (no auto-embed
//! boilerplate needed; the flush index-build is what matters). The whole run is
//! bounded by a wall-clock timeout so a regression (a permanent wedge) FAILS
//! loudly instead of hanging CI. With the flush-stream timeout + skip-on-
//! saturation fix, a stalled flush can never permanently wedge the pipeline.

use std::time::Duration;

use anyhow::Result;
use uni_db::{DataType, IndexType, Uni, UniConfig, Value};

const DENSE_DIM: usize = 32;
const VOCAB: usize = 2000;
const MV_DIM: usize = 16;

fn dense(seed: usize) -> Value {
    Value::Vector(
        (0..DENSE_DIM)
            .map(|i| ((seed + i) % 7) as f32 * 0.1)
            .collect(),
    )
}

fn sparse(seed: usize) -> Value {
    let base = (seed * 4) % (VOCAB - 8);
    Value::SparseVector {
        indices: (0..4).map(|i| (base + i * 2) as u32).collect(),
        values: vec![1.0, 0.5, 0.25, 0.75],
    }
}

fn multivec(seed: usize) -> Value {
    Value::List(
        (0..3)
            .map(|t| {
                Value::Vector(
                    (0..MV_DIM)
                        .map(|i| ((seed + t + i) % 5) as f32 * 0.2)
                        .collect(),
                )
            })
            .collect(),
    )
}

async fn define_schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("Obs")
        .property("oid", DataType::Int64)
        .property(
            "dense",
            DataType::Vector {
                dimensions: DENSE_DIM,
            },
        )
        .property("sparse", DataType::SparseVector { dimensions: VOCAB })
        .property(
            "multi",
            DataType::List(Box::new(DataType::Vector { dimensions: MV_DIM })),
        )
        .index("sparse", IndexType::sparse(VOCAB))
        .apply()
        .await?;
    db.schema()
        .label("Fact")
        .property("fid", DataType::Int64)
        .apply()
        .await?;
    db.schema()
        .edge_type("DERIVED", &["Fact"], &["Obs"])
        .apply()
        .await?;
    Ok(())
}

/// One ingest + consolidation cycle: create Obs rows (dense+sparse+multivec),
/// then a sweep interleaving small reads with Fact-node + edge writes — the
/// shape that triggered the hang.
async fn ingest_and_consolidate(db: &Uni, rows: usize) -> Result<()> {
    for i in 0..rows {
        let t = std::time::Instant::now();
        let tx = db.session().tx().await?;
        tx.execute_with("CREATE (:Obs {oid: $o, dense: $d, sparse: $s, multi: $m})")
            .param("o", Value::Int(i as i64))
            .param("d", dense(i))
            .param("s", sparse(i))
            .param("m", multivec(i))
            .run()
            .await?;
        tx.commit().await?;
        let ms = t.elapsed().as_millis();
        if ms > 200 {
            eprintln!("[#132] slow ingest row {i}: {ms}ms");
        }
    }
    // Consolidation sweep: small reads + Fact/edge writes interleaved.
    for i in 0..rows {
        let _ = db
            .session()
            .query_with("MATCH (o:Obs {oid: $o}) RETURN o.oid AS oid")
            .param("o", Value::Int(i as i64))
            .fetch_all()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute_with("MATCH (o:Obs {oid: $o}) CREATE (f:Fact {fid: $o})-[:DERIVED]->(o)")
            .param("o", Value::Int(i as i64))
            .run()
            .await?;
        tx.commit().await?;
    }
    Ok(())
}

/// The consolidation sweep must never permanently wedge the runtime. Runs
/// several ingest+consolidate cycles under async flush with sparse+multivec
/// columns, all bounded by a generous wall clock — a #132 regression trips the
/// timeout instead of hanging forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consolidation_sweep_never_wedges() -> Result<()> {
    let cfg = UniConfig {
        async_flush_enabled: true,
        // Flush frequently so the sparse/multivec index-build runs often.
        auto_flush_threshold: 4,
        auto_flush_interval: None,
        auto_flush_min_mutations: 1,
        max_pending_flushes: 2,
        ..Default::default()
    };
    let db = Uni::temporary().config(cfg).build().await?;
    define_schema(&db).await?;

    // Bound the whole workload. Pre-fix, an intermittent stalled flush parks the
    // runtime here forever; post-fix it always completes well within the budget.
    let done = tokio::time::timeout(Duration::from_secs(90), async {
        for _cycle in 0..2 {
            ingest_and_consolidate(&db, 15).await?;
        }
        anyhow::Ok(())
    })
    .await;

    assert!(
        done.is_ok(),
        "issue #132 regression: consolidation sweep hung (runtime parked on a stalled flush)"
    );
    done.expect("bounded")?;

    // Sanity: the data is all present and queryable.
    let n = db
        .session()
        .query_with("MATCH (o:Obs) RETURN o.oid AS oid")
        .fetch_all()
        .await?
        .rows()
        .len();
    assert_eq!(
        n,
        2 * 15,
        "all ingested Obs must be queryable after the sweep"
    );
    Ok(())
}
