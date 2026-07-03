//! Regression guard (#134): `uni.vector.query ... YIELD node` must NOT
//! materialize an unread `List(Vector)` column. Pre-fix this inflated latency
//! ~3.8x vs a control without the column; the procedure-path fix prunes the
//! fetch to the emitted properties, so `YIELD node` now tracks `YIELD vid`.
//!
//! A companion angle to issue #134 (dense `similar_to` scan slows ~60x with an
//! unread `List(Vector)` column). Here the leak is exercised through the
//! `uni.vector.query` procedure: `YIELD node → RETURN node.title` loads the full
//! node (all columns, including a heavy `tokens` `List(Vector)` never referenced
//! by the query), while `YIELD vid → RETURN vid` returns only the vertex id.
//!
//! Single variable: the projection (`node` vs `vid`). A control DB without the
//! `tokens` column shows `YIELD node` is cheap when there is no heavy column to
//! drag — pinning the cost to materialization of the unread column.
//
// Rust guideline compliant: application-style example (M-APP-ERROR uses anyhow;
// all items are private to the example binary, so no public-API docs apply).

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

/// Dense embedding dimension (also each ColBERT token vector's dimension).
const DIM: usize = 128;
/// Token vectors per doc in the heavy `List(Vector)` column (~`TOKENS*DIM*4` B/doc).
const TOKENS: usize = 128;
/// Docs inserted into each database.
const N: usize = 500;
/// Docs per write transaction (bounds commit size for the heavy list writes).
const INSERT_BATCH: usize = 100;
/// Top-k requested per query.
const K: usize = 20;
/// Timed query repetitions per (config, projection).
const ITERS: usize = 200;

/// Deterministic pseudo-random f32 vector of length `dim` seeded by `seed`.
///
/// A tiny LCG keeps the example reproducible without `rand` (and without the
/// workflow-forbidden `Math::random`); values are irrelevant to the leak.
fn vecf(seed: u64, dim: usize) -> Vec<f32> {
    let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    (0..dim)
        .map(|_| {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 33) as f32 / u32::MAX as f32) - 0.5
        })
        .collect()
}

/// Encodes multi-vector tokens as `List<List<Float>>` (uni param encoding).
fn mv_value(tokens: &[Vec<f32>]) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

/// Opens a fresh (wiped) DB with a widened commit window for the list writes.
///
/// # Errors
/// Returns an error if the directory cannot be reset or the DB cannot open.
async fn fresh_db(path: &Path) -> Result<Uni> {
    if path.exists() {
        std::fs::remove_dir_all(path).ok();
    }
    std::fs::create_dir_all(path)?;
    let cfg = uni_common::UniConfig {
        commit_timeout: std::time::Duration::from_secs(300),
        ..uni_common::UniConfig::default()
    };
    Uni::open(path.to_string_lossy().to_string())
        .config(cfg)
        .build()
        .await
        .map_err(Into::into)
}

/// Builds a DB of `N` docs; `heavy` adds the unread `tokens` `List(Vector)` column.
///
/// The dense `emb` data and its Flat index are identical either way, so the only
/// variable across the two DBs is the presence of the heavy column.
///
/// # Errors
/// Returns an error if schema, writes, or index build fail.
async fn build(path: &Path, heavy: bool) -> Result<Uni> {
    let db = fresh_db(path).await?;

    let base = db
        .schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM });
    let schema = if heavy {
        base.property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
    } else {
        base
    };
    schema
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    let mut tx = db.session().tx().await?;
    for i in 0..N {
        let emb = vecf(i as u64, DIM);
        if heavy {
            let tokens: Vec<Vec<f32>> = (0..TOKENS)
                .map(|t| vecf((i * TOKENS + t) as u64 + 1, DIM))
                .collect();
            tx.execute_with("CREATE (:Doc {title: $title, emb: $emb, tokens: $tokens})")
                .param("title", Value::String(format!("d{i}")))
                .param("emb", Value::Vector(emb))
                .param("tokens", mv_value(&tokens))
                .run()
                .await?;
        } else {
            tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
                .param("title", Value::String(format!("d{i}")))
                .param("emb", Value::Vector(emb))
                .run()
                .await?;
        }
        if (i + 1) % INSERT_BATCH == 0 {
            tx.commit().await?;
            tx = db.session().tx().await?;
        }
    }
    tx.commit().await?;
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;
    Ok(db)
}

/// Mean per-query latency (ms) of `uni.vector.query` under the given projection.
///
/// `yield_node = true` materializes the full node (`YIELD node → RETURN
/// node.title`); `false` returns only the id (`YIELD vid → RETURN vid`).
///
/// # Errors
/// Returns an error if any query fails.
async fn time_query(db: &Uni, yield_node: bool) -> Result<f64> {
    let cypher = if yield_node {
        "CALL uni.vector.query('Doc', 'emb', $q, $k) YIELD node RETURN node.title AS title"
    } else {
        "CALL uni.vector.query('Doc', 'emb', $q, $k) YIELD vid RETURN vid"
    };
    let mut total = 0.0;
    for r in 0..ITERS {
        // Vary the query vector per iteration so nothing is trivially cached.
        let q = vecf(1_000_000 + r as u64, DIM);
        let t = Instant::now();
        db.session()
            .query_with(cypher)
            .param("q", Value::Vector(q))
            .param("k", Value::Int(K as i64))
            .fetch_all()
            .await?;
        total += t.elapsed().as_secs_f64() * 1000.0;
    }
    Ok(total / ITERS as f64)
}

/// Entry point: build both DBs, time both projections, report the leak ratio.
///
/// # Errors
/// Returns an error if any DB operation fails or the leak invariant regresses.
#[tokio::main]
async fn main() -> Result<()> {
    let scratch = std::env::temp_dir().join("projection_leak_repro");

    println!(
        "Building 2 DBs: {N} docs, emb=Vector({DIM}) + Flat index; \
         heavy adds tokens=List(Vector({DIM})) x{TOKENS} (~{} KB/doc)",
        TOKENS * DIM * 4 / 1024
    );
    let light = build(&scratch.join("light"), false).await?;
    let heavy = build(&scratch.join("heavy"), true).await?;

    let light_vid = time_query(&light, false).await?;
    let light_node = time_query(&light, true).await?;
    let heavy_vid = time_query(&heavy, false).await?;
    let heavy_node = time_query(&heavy, true).await?;

    println!("\n uni.vector.query top-{K}, mean of {ITERS} queries (ms)");
    println!(" ─────────────────────────────────────────────────────────");
    println!(
        " {:<28} {:>10} {:>10}",
        "columns on row", "YIELD vid", "YIELD node"
    );
    println!(
        " {:<28} {:>10.2} {:>10.2}",
        "emb only (control)", light_vid, light_node
    );
    println!(
        " {:<28} {:>10.2} {:>10.2}",
        "emb + tokens List(Vector)", heavy_vid, heavy_node
    );
    println!(" ─────────────────────────────────────────────────────────");
    println!(
        " YIELD node penalty:  control ×{:.1},  with-tokens ×{:.1}",
        light_node / light_vid.max(1e-9),
        heavy_node / heavy_vid.max(1e-9),
    );
    println!(
        " tokens-column tax on YIELD node: ×{:.1}  ({:.2}ms → {:.2}ms)",
        heavy_node / light_node.max(1e-9),
        light_node,
        heavy_node,
    );

    heavy.shutdown().await?;
    light.shutdown().await?;

    // Regression guard for the #134 procedure-path fix: with pruning in place,
    // `YIELD node` no longer materializes the unread `tokens` List(Vector), so it
    // stays close to `YIELD vid` and the tokens column does not materially inflate
    // it vs the control. Thresholds are generous (observed ~1.3x) to tolerate
    // timing variance while still catching the ~3.8x pre-fix leak.
    assert!(
        heavy_node < heavy_vid * 2.5,
        "REGRESSION (#134): YIELD node on tokens-carrying rows is >2.5x YIELD vid \
         (node={heavy_node:.2}ms vid={heavy_vid:.2}ms) — the List(Vector) projection leak is back"
    );
    assert!(
        heavy_node < light_node * 2.5,
        "REGRESSION (#134): the unread tokens column inflates YIELD node >2.5x vs the \
         control (heavy={heavy_node:.2}ms light={light_node:.2}ms) — projection leak"
    );
    println!("\nOK — YIELD node no longer pays the List(Vector) materialization tax (#134 fixed).");
    Ok(())
}
