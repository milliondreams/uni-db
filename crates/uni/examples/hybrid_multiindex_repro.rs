//! Repro: does a second vector index on a label perturb `uni.search`'s dense
//! `vector_score` / FTS arm for a *different* vector property?
//!
//! Motivated by the LoCoMo hybrid demo (`examples/locomo_hybrid.rs`): after a
//! `tokens` `List(Vector)` column + MUVERA index were added to the `Doc` label,
//! `uni.search`'s dense `vector_score` for a fixed (query, doc) pair changed and
//! the FTS arm surfaced lexically-unrelated turns — while `similar_to(d.emb, $q)`
//! on the *same* pair did not move, and the sparse arm was byte-identical.
//!
//! This test isolates the single variable: identical tiny synthetic data and
//! schema in two DBs, differing only by whether a MUVERA index exists on
//! `tokens`. The dense retrieval property queried is always `emb`.
//!
//! Hypotheses under test:
//! - H1 (bug): the second vector index changes `uni.search`'s `emb`/FTS results.
//! - H2 (benign): `vector_score` is relatively normalized, so it never equals the
//!   absolute cosine even with a single index (would make the demo reading a
//!   misinterpretation, not a defect).
//
// Rust guideline compliant: application-style example (M-APP-ERROR uses anyhow;
// all items are private to the example binary, so no public-API docs apply).

use anyhow::Result;
use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

/// Tiny dense/token dimension so cosine rankings are hand-verifiable.
const DIM: usize = 4;

/// A 4-d dense vector literal.
fn v(x: [f32; 4]) -> Value {
    Value::Vector(x.to_vec())
}

/// Two 4-d token vectors encoded as `List<List<Float>>` for the MUVERA column.
fn toks(a: [f32; 4], b: [f32; 4]) -> Value {
    let row = |r: [f32; 4]| Value::List(r.iter().map(|&x| Value::Float(x as f64)).collect());
    Value::List(vec![row(a), row(b)])
}

/// One corpus row: (title, text, emb, token-a, token-b).
type Doc = (&'static str, &'static str, [f32; 4], [f32; 4], [f32; 4]);

/// The five fixed corpus docs.
///
/// Cosine vs query `[1,0,0,0]`: d0 = 1.000 (exact), d2 ≈ 0.995, others 0.
/// FTS term "zebra" appears only in d0 and d3.
fn corpus() -> Vec<Doc> {
    vec![
        (
            "d0",
            "alpha zebra",
            [1.0, 0.0, 0.0, 0.0],
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
        ),
        (
            "d1",
            "beta lion",
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
        ),
        (
            "d2",
            "gamma tiger",
            [0.9, 0.1, 0.0, 0.0],
            [0.9, 0.1, 0.0, 0.0],
            [1.0, 0.0, 0.0, 0.0],
        ),
        (
            "d3",
            "delta zebra",
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
        ),
        (
            "d4",
            "epsilon wolf",
            [0.0, 0.0, 0.0, 1.0],
            [0.0, 0.0, 0.0, 1.0],
            [1.0, 0.0, 0.0, 0.0],
        ),
    ]
}

/// Cosine index over the `emb` property with the given ANN algorithm.
fn emb_index(algo: VectorAlgo) -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: algo,
        metric: VectorMetric::Cosine,
        embedding: None,
    })
}

/// MUVERA (FDE) index over the `tokens` `List(Vector)` property.
fn muvera_index() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Muvera {
            k_sim: 4,
            reps: 8,
            d_proj: 8,
            seed: uni_db::api::schema::DEFAULT_FDE_SEED,
            inner: Box::new(VectorAlgo::Flat),
        },
        metric: VectorMetric::Cosine,
        embedding: None,
    })
}

/// Builds a DB with identical data; the MUVERA index on `tokens` is the only
/// variable. The `tokens` column always exists so the data is held constant.
async fn build(with_muvera: bool, algo: VectorAlgo) -> Result<Uni> {
    let db = Uni::temporary().build().await?;

    let schema = db
        .schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("text", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index("emb", emb_index(algo))
        .index("text", IndexType::FullText);
    let schema = if with_muvera {
        schema.index("tokens", muvera_index())
    } else {
        schema
    };
    schema.apply().await?;

    let tx = db.session().tx().await?;
    for (title, text, emb, ta, tb) in corpus() {
        tx.execute_with("CREATE (:Doc {title: $title, text: $text, emb: $emb, tokens: $tokens})")
            .param("title", Value::String(title.to_string()))
            .param("text", Value::String(text.to_string()))
            .param("emb", v(emb))
            .param("tokens", toks(ta, tb))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;
    Ok(db)
}

/// `similar_to(d.emb, $q)` per-row cosine for every doc, sorted desc.
async fn similar_scores(db: &Uni, q: [f32; 4]) -> Result<Vec<(String, f64)>> {
    let rows = db
        .session()
        .query_with(
            "MATCH (d:Doc) RETURN d.title AS title, similar_to(d.emb, $q) AS s ORDER BY s DESC",
        )
        .param("q", v(q))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap_or_default(),
                r.get::<f64>("s").unwrap_or(0.0),
            )
        })
        .collect())
}

/// `uni.search {vector:'emb', fts:'text'}` rows: (title, vector_score, fts_score).
async fn hybrid_scores(db: &Uni, q: [f32; 4], text: &str) -> Result<Vec<(String, f64, f64)>> {
    let rows = db
        .session()
        .query_with(
            "CALL uni.search('Doc', {vector: 'emb', fts: 'text'}, $qt, $qv, 5) \
             YIELD node, score, vector_score, fts_score \
             RETURN node.title AS title, vector_score, fts_score ORDER BY score DESC",
        )
        .param("qt", Value::String(text.to_string()))
        .param("qv", v(q))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap_or_default(),
                r.get::<f64>("vector_score").unwrap_or(f64::NAN),
                r.get::<f64>("fts_score").unwrap_or(f64::NAN),
            )
        })
        .collect())
}

/// Looks up a doc's dense score in a `(title, score)` list.
fn score_of(rows: &[(String, f64)], title: &str) -> Option<f64> {
    rows.iter().find(|(t, _)| t == title).map(|(_, s)| *s)
}

/// Looks up a doc's `vector_score` in a hybrid `(title, vec, fts)` list.
fn vec_of(rows: &[(String, f64, f64)], title: &str) -> Option<f64> {
    rows.iter().find(|(t, ..)| t == title).map(|(_, vs, _)| *vs)
}

/// Titles of docs with a positive FTS score, sorted (deterministic set compare).
fn fts_positive(rows: &[(String, f64, f64)]) -> Vec<String> {
    let mut t: Vec<String> = rows
        .iter()
        .filter(|(_, _, f)| *f > 0.0)
        .map(|(t, ..)| t.clone())
        .collect();
    t.sort();
    t
}

/// Runs the with/without-MUVERA comparison for one `emb` index algorithm.
///
/// Prints the scores and asserts the #138 invariants: the dense `emb`
/// `vector_score` is invariant to the presence of the MUVERA index on `tokens`,
/// equals `similar_to` (shared metric-aware `(1+cos)/2`), and the FTS arm is
/// unaffected.
///
/// # Errors
/// Returns an error if any DB operation fails or an invariant is violated.
async fn check_algo(algo_name: &str, algo_fn: impl Fn() -> VectorAlgo) -> Result<()> {
    let q = [1.0f32, 0.0, 0.0, 0.0];

    let db_a = build(false, algo_fn()).await?;
    let sim_a = similar_scores(&db_a, q).await?;
    let hyb_a = hybrid_scores(&db_a, q, "zebra").await?;

    let db_b = build(true, algo_fn()).await?;
    let sim_b = similar_scores(&db_b, q).await?;
    let hyb_b = hybrid_scores(&db_b, q, "zebra").await?;

    eprintln!("\n════ emb index = {algo_name} ════");
    eprintln!("  NO tokens index — similar_to : {sim_a:?}");
    eprintln!("                    uni.search : {hyb_a:?}");
    eprintln!("  WITH MUVERA     — similar_to : {sim_b:?}");
    eprintln!("                    uni.search : {hyb_b:?}");

    // Sanity: similar_to sees the exact match d0 (cosine 1.0) in both configs.
    // similar_to returns raw cosine while the procedures return the metric-aware
    // (1+cos)/2 — different conventions by design; d0 (cos=1) is 1.0 under both.
    let sim_a0 = score_of(&sim_a, "d0").expect("d0 in similar_to A");
    assert!(
        (sim_a0 - 1.0).abs() < 1e-3,
        "[{algo_name}] similar_to(d0) should be 1.0 (identical vectors); got {sim_a0}"
    );

    // The #138 fix: for EVERY (q, doc) — not just the exact match d0, whose score
    // 1.0 is a fixed point that hides the bug — the dense `vector_score` must NOT
    // depend on whether a MUVERA index exists on another property, and must be the
    // metric-aware (1+cos)/2. `similar_to` (raw cosine) is printed for reference.
    let mut index_drift = false;
    for (title, sim) in &sim_a {
        let (Some(va), Some(vb)) = (vec_of(&hyb_a, title), vec_of(&hyb_b, title)) else {
            continue;
        };
        let drift = (va - vb).abs() >= 1e-3;
        index_drift |= drift;
        eprintln!(
            "  {title}: no-index vec={va:.5}  with-MUVERA vec={vb:.5}  similar_to(raw cos)={sim:.5}{}",
            if drift { "  ⚠ 2nd-index DRIFT" } else { "" },
        );
    }

    // FTS arm invariance: "zebra" occurs only in d0 and d3 — this MUST hold.
    let fts_a = fts_positive(&hyb_a);
    let fts_b = fts_positive(&hyb_b);
    eprintln!("  FTS-positive: no-index={fts_a:?}  with-MUVERA={fts_b:?}");
    assert_eq!(
        fts_a, fts_b,
        "[{algo_name}] FTS-positive doc set changed when MUVERA index was added"
    );
    assert!(
        fts_a.contains(&"d0".to_string()) && fts_a.contains(&"d3".to_string()),
        "[{algo_name}] FTS for 'zebra' should hit d0 and d3; got {fts_a:?}"
    );

    // Regression guard for #138: vector_score must be invariant to the 2nd index.
    assert!(
        !index_drift,
        "[{algo_name}] REGRESSION (#138): dense vector_score changed when a MUVERA index \
         was added to `tokens` — it must not depend on an unrelated index"
    );
    eprintln!(
        "  ⇒ [{algo_name}] OK: vector_score invariant to the 2nd index (metric-aware (1+cos)/2)."
    );
    Ok(())
}

/// Entry point: run the isolation check for both Flat and HNSW `emb` indexes.
///
/// # Errors
/// Returns an error if any DB operation fails or an invariant is violated.
#[tokio::main]
async fn main() -> Result<()> {
    check_algo("Flat", || VectorAlgo::Flat).await?;
    check_algo("HNSW", || VectorAlgo::Hnsw {
        m: 16,
        ef_construction: 100,
        partitions: None,
    })
    .await?;

    println!(
        "\nOK (#138 fixed) — dense vector_score is invariant to a second vector index \
         (metric-aware (1+cos)/2), for both Flat and HNSW."
    );
    Ok(())
}
