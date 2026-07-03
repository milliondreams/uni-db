// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase-2 tests for native multi-vector (ColBERT) INDEX retrieval (issue #96):
//! the full `VectorIndexType` menu over a `List<Vector>` column, the
//! `nprobes` / `refine_factor` query options, the inline `vector_similarity()`
//! predicate path, and the index-creation fixes (metric-from-OPTIONS, PQ
//! sub-vector validation).

use uni_db::core::schema::{DistanceMetric, IndexDefinition};
use uni_db::{DataType, Uni, Value};

const DIM: usize = 8;

/// Deterministic xorshift PRNG so corpora are reproducible across runs.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn unit(&mut self) -> Vec<f32> {
        let mut v: Vec<f32> = (0..DIM)
            .map(|_| ((self.next_u64() >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0)
            .collect();
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        for x in &mut v {
            *x /= norm;
        }
        v
    }
}

/// `i`-th standard basis vector (a clean, unit-norm token).
fn basis(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[i] = 1.0;
    v
}

/// A multi-vector as a `Value::List(Vec<Value::List<Float>>)`.
fn to_value(tokens: &[Vec<f32>]) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

/// A multi-vector as a Cypher list-of-lists literal (so reads need no params).
fn cypher_lit(tokens: &[Vec<f32>]) -> String {
    let toks: Vec<String> = tokens
        .iter()
        .map(|t| {
            let nums: Vec<String> = t.iter().map(|x| format!("{x:?}")).collect();
            format!("[{}]", nums.join(","))
        })
        .collect();
    format!("[{}]", toks.join(","))
}

/// Query tokens: basis vectors e0 and e1.
fn query_tokens() -> Vec<Vec<f32>> {
    vec![basis(0), basis(1)]
}

/// Builds a DB of `n` docs. The doc titled `"target"` has tokens == the query
/// tokens (so it is the unique MaxSim maximizer, score 2.0); the rest are random
/// (MaxSim well below 2). Returns the DB.
async fn setup(n: usize) -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .apply()
        .await?;

    let q = query_tokens();
    let mut rng = Rng(0xD1CE_5EED);
    let tx = db.session().tx().await?;
    for i in 0..n {
        let (title, tokens) = if i == n / 2 {
            ("target".to_string(), q.clone())
        } else {
            (format!("doc{i}"), (0..3).map(|_| rng.unit()).collect())
        };
        tx.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
            .param("title", Value::String(title))
            .param("toks", to_value(&tokens))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Returns the result titles of a `uni.vector.query` over the `tokens` column.
async fn query_titles(db: &Uni, k: usize, options: &str) -> anyhow::Result<Vec<String>> {
    let lit = cypher_lit(&query_tokens());
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', {lit}, {k}, null, null, {options}) \
         YIELD node, score RETURN node.title AS title"
    );
    let res = db.session().query(&cypher).await?;
    Ok(res
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

async fn create_index(db: &Uni, name: &str, opts: &str) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE VECTOR INDEX {name} FOR (d:Doc) ON (d.tokens) OPTIONS {opts}"
    ))
    .await?;
    tx.commit().await?;
    db.indexes().rebuild("Doc", false).await?;
    Ok(())
}

#[tokio::test]
async fn test_multivector_all_index_types_build() -> anyhow::Result<()> {
    // The COMPLETE menu builds over a multi-vector column. PQ variants need
    // num_sub_vectors | DIM and enough training vectors (>=256), so use a corpus
    // of ~120 docs * 3 tokens.
    let db = setup(120).await?;
    let cases = [
        ("flat", "{type: 'flat', metric: 'cosine'}"),
        (
            "ivf_flat",
            "{type: 'ivf_flat', partitions: 4, metric: 'cosine'}",
        ),
        (
            "ivf_pq",
            "{type: 'ivf_pq', partitions: 4, sub_vectors: 4, num_bits: 8, metric: 'cosine'}",
        ),
        (
            "ivf_sq",
            "{type: 'ivf_sq', partitions: 4, metric: 'cosine'}",
        ),
        (
            "ivf_rq",
            "{type: 'ivf_rq', partitions: 4, metric: 'cosine'}",
        ),
        (
            "hnsw_flat",
            "{type: 'hnsw_flat', m: 16, ef_construction: 64, metric: 'cosine'}",
        ),
        (
            "hnsw_sq",
            "{type: 'hnsw_sq', m: 16, ef_construction: 64, metric: 'cosine'}",
        ),
        (
            "hnsw_pq",
            "{type: 'hnsw_pq', m: 16, ef_construction: 64, sub_vectors: 4, metric: 'cosine'}",
        ),
    ];
    for (ty, opts) in cases {
        // Same index name → each create replaces the prior, keeping one index.
        create_index(&db, "tok_idx", opts)
            .await
            .unwrap_or_else(|e| panic!("index type {ty} failed to build: {e}"));
        // The index must serve a query and still surface the MaxSim target.
        let titles = query_titles(&db, 5, "{refine_factor: 10}").await?;
        assert!(
            titles.iter().any(|t| t == "target"),
            "index type {ty}: target missing from top-5 ({titles:?})"
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_multivector_query_no_index_cosine() -> anyhow::Result<()> {
    // No index → brute-force MaxSim, default Cosine. The exact maximizer leads.
    let db = setup(60).await?;
    let titles = query_titles(&db, 5, "{}").await?;
    assert_eq!(
        titles[0], "target",
        "brute-force MaxSim should rank target first"
    );
    Ok(())
}

#[tokio::test]
async fn test_multivector_query_ivf_pq_refine() -> anyhow::Result<()> {
    // IVF_PQ + refine_factor recovers exact MaxSim ordering at low nprobes.
    let db = setup(120).await?;
    create_index(
        &db,
        "tok_idx",
        "{type: 'ivf_pq', partitions: 4, sub_vectors: 4, num_bits: 8, metric: 'cosine'}",
    )
    .await?;
    let titles = query_titles(&db, 5, "{nprobes: 4, refine_factor: 16}").await?;
    assert!(
        titles.iter().any(|t| t == "target"),
        "IVF_PQ+refine should retrieve target: {titles:?}"
    );
    Ok(())
}

#[tokio::test]
async fn test_multivector_inline_vector_similarity() -> anyhow::Result<()> {
    // The inline predicate path: `vector_similarity(n.tokens, $q)` over a
    // multi-vector column scores by MaxSim (target = 2.0, others < ~1.0).
    let db = setup(60).await?;
    let lit = cypher_lit(&query_tokens());
    let cypher = format!(
        "MATCH (d:Doc) WHERE vector_similarity(d.tokens, {lit}) > 1.5 \
         RETURN d.title AS title"
    );
    let res = db.session().query(&cypher).await?;
    let titles: Vec<String> = res
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert_eq!(
        titles,
        vec!["target".to_string()],
        "only the MaxSim maximizer should exceed the threshold"
    );
    Ok(())
}

#[tokio::test]
async fn test_create_vector_index_metric_from_options() -> anyhow::Result<()> {
    // The metric is now parsed from OPTIONS (previously hardcoded Cosine).
    let db = setup(60).await?;
    create_index(
        &db,
        "tok_idx",
        "{type: 'ivf_flat', partitions: 4, metric: 'l2'}",
    )
    .await?;
    let metric = db
        .indexes()
        .list(Some("Doc"))
        .into_iter()
        .find_map(|idx| match idx {
            IndexDefinition::Vector(c) if c.property == "tokens" => Some(c.metric),
            _ => None,
        })
        .expect("vector index on tokens");
    assert_eq!(metric, DistanceMetric::L2, "metric must come from OPTIONS");
    Ok(())
}

#[tokio::test]
async fn test_pq_subvectors_validation_errors() -> anyhow::Result<()> {
    // PQ requires num_sub_vectors | DIM; an indivisible value fails at create
    // time with a clear error (not an opaque Lance runtime error).
    let db = setup(60).await?;
    let res = create_index(
        &db,
        "bad_idx",
        "{type: 'ivf_pq', partitions: 4, sub_vectors: 3, metric: 'cosine'}",
    )
    .await;
    assert!(
        res.is_err(),
        "PQ with sub_vectors not dividing DIM ({DIM}) should error at create"
    );
    Ok(())
}
