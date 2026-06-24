// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase-3 tests for MUVERA (Fixed-Dimensional-Encoding) multi-vector indexes
//! (issue #96). A `CREATE VECTOR INDEX ... OPTIONS {type:'muvera', ...}` over a
//! `List<Vector>` column derives a single-vector FDE column (`__fde_*`) and builds a
//! normal single-vector ANN over it; `uni.vector.query` runs the FDE ANN as a fast
//! first stage and re-ranks candidates by EXACT MaxSim.
//!
//! Token convention (DIM=8): query `[e0, e1]`. The doc titled `"target"` has tokens ==
//! the query tokens, so it is the unique MaxSim maximizer (score 2.0); other docs are
//! random (well below 2.0). Self-retrieval of an exact match is robust under MUVERA even
//! on cluster-free synthetic data, so `target` must rank first.

use uni_db::core::schema::{IndexDefinition, VectorIndexType};
use uni_db::{DataType, Uni, Value};

const DIM: usize = 8;
/// FDE params kept small so corpora train quickly; `inner: 'flat'` = exact ANN over the
/// FDE column (no PQ training needed), which makes the correctness assertions stable.
const MUVERA_OPTS: &str = "{type: 'muvera', k_sim: 4, reps: 8, d_proj: 8, inner: 'flat'}";

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

fn basis(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[i] = 1.0;
    v
}

fn to_value(tokens: &[Vec<f32>]) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

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

fn query_tokens() -> Vec<Vec<f32>> {
    vec![basis(0), basis(1)]
}

async fn define_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .apply()
        .await?;
    Ok(())
}

/// Insert `n` docs (the middle one titled `target` with tokens == query) in one tx.
async fn insert_corpus(db: &Uni, n: usize, seed: u64) -> anyhow::Result<()> {
    let q = query_tokens();
    let mut rng = Rng(seed);
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
    Ok(())
}

async fn create_muvera_index(db: &Uni, opts: &str) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE VECTOR INDEX tok_idx FOR (d:Doc) ON (d.tokens) OPTIONS {opts}"
    ))
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn query_titles(db: &Uni, k: usize) -> anyhow::Result<Vec<String>> {
    let lit = cypher_lit(&query_tokens());
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', {lit}, {k}, null, null, {{}}) \
         YIELD node, score RETURN node.title AS title"
    );
    let res = db.session().query(&cypher).await?;
    Ok(res
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

/// Backfill path: write + flush a corpus, THEN create the MUVERA index (which scans the
/// already-flushed rows, derives the FDE column, and builds the ANN). Query ranks the
/// exact MaxSim maximizer first.
#[tokio::test]
async fn muvera_backfill_then_query_ranks_target_first() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 60, 0xD1CE_5EED).await?;
    db.flush().await?;

    create_muvera_index(&db, MUVERA_OPTS).await?;

    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "MUVERA first-stage + exact re-rank should rank the maximizer first: {titles:?}"
    );
    Ok(())
}

/// Create-before-ingest: create the MUVERA index on an empty label, THEN write + flush.
/// The flush materialises the FDE column for the new rows (no backfill needed); the query
/// still finds the target (flat-scan over the FDE column when no ANN was built on empty).
#[tokio::test]
async fn muvera_create_before_ingest() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    insert_corpus(&db, 60, 0xBEEF_F00D).await?;
    db.flush().await?;

    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "flush-materialised FDE should retrieve the target: {titles:?}"
    );
    Ok(())
}

/// IVF_PQ inner index over the FDE column exercises the PQ build path (fde_dim must be
/// divisible by sub_vectors). The target still surfaces after exact re-rank.
#[tokio::test]
async fn muvera_ivf_pq_inner() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 120, 0x1234_5678).await?;
    db.flush().await?;

    // fde_dim = reps*2^k_sim*d_proj = 8*16*8 = 1024; sub_vectors=8 divides it.
    create_muvera_index(
        &db,
        "{type: 'muvera', k_sim: 4, reps: 8, d_proj: 8, inner: 'ivf_pq', partitions: 4, sub_vectors: 8}",
    )
    .await?;

    let titles = query_titles(&db, 10).await?;
    assert!(
        titles.iter().any(|t| t == "target"),
        "IVF_PQ-backed MUVERA should retrieve the target in top-10: {titles:?}"
    );
    Ok(())
}

/// L0 visibility: with a MUVERA index + flushed corpus, an UNFLUSHED insert is still
/// found — `multivector_rerank` unions live L0 candidates with the FDE first stage and
/// re-scores everything by exact MaxSim.
#[tokio::test]
async fn muvera_l0_and_flushed_mix() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 40, 0xAAAA_5555).await?;
    db.flush().await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    // A second exact-match doc inserted WITHOUT flushing (lives in L0 only).
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: $t, tokens: $toks})")
        .param("t", Value::String("l0-target".to_string()))
        .param("toks", to_value(&query_tokens()))
        .run()
        .await?;
    tx.commit().await?;

    let titles = query_titles(&db, 5).await?;
    assert!(
        titles.iter().any(|t| t == "l0-target"),
        "unflushed L0 exact match must be visible: {titles:?}"
    );
    Ok(())
}

/// Persistence: the MUVERA params (seed/k_sim/reps/d_proj) and the FDE column survive a
/// close + reopen, so query-time encoding still matches doc-time encoding.
#[tokio::test]
async fn muvera_persists_across_reopen() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    {
        let db = Uni::open(path).build().await?;
        define_schema(&db).await?;
        insert_corpus(&db, 60, 0xFEED_BEEF).await?;
        db.flush().await?;
        create_muvera_index(&db, MUVERA_OPTS).await?;
        let titles = query_titles(&db, 5).await?;
        assert_eq!(titles.first().map(String::as_str), Some("target"));
        // Data is durable after flush; drop to release the path before reopening.
        drop(db);
    }
    // Reopen the same path.
    let db = Uni::open(path).build().await?;
    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "MUVERA results must survive reopen: {titles:?}"
    );
    Ok(())
}

/// Fork fallback: a MUVERA-indexed parent is forked; the fork query inherits parent docs
/// and finds a fork-local exact match via the brute-force branch path (the FDE ANN is not
/// branched — forks always exact-rerank). The parent stays isolated from the fork write.
#[tokio::test]
async fn muvera_fork_fallback() -> anyhow::Result<()> {
    use uni_db::core::schema::DataType as DT;
    let cfg = uni_db::UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..uni_db::UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("title", DT::String)
        .property("tokens", DT::List(Box::new(DT::Vector { dimensions: DIM })))
        .apply()
        .await?;

    insert_corpus(&db, 40, 0x0F0F_0F0F).await?;
    db.flush().await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    let forked = db.session().fork("muvera_fork").await?;
    let tx = forked.tx().await?;
    tx.execute_with("CREATE (:Doc {title: $t, tokens: $toks})")
        .param("t", Value::String("fork-target".to_string()))
        .param("toks", to_value(&query_tokens()))
        .run()
        .await?;
    tx.commit().await?;

    let lit = cypher_lit(&query_tokens());
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', {lit}, 5, null, null, {{}}) \
         YIELD node, score RETURN node.title AS title"
    );
    let fork_titles: Vec<String> = forked
        .query(&cypher)
        .await?
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert!(
        fork_titles.iter().any(|t| t == "fork-target"),
        "fork must see its own exact match via brute-force fallback: {fork_titles:?}"
    );

    // Parent is isolated from the fork's write.
    let parent_titles: Vec<String> = db
        .session()
        .query(&cypher)
        .await?
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert!(
        !parent_titles.iter().any(|t| t == "fork-target"),
        "parent must not see fork-local docs: {parent_titles:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Updates / tombstones / isolation
// ---------------------------------------------------------------------------

/// Updating the source tokens recomputes the FDE on the next flush (so the new tokens
/// drive ranking); deleting a doc removes it from results.
#[tokio::test]
async fn muvera_update_and_tombstone() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 40, 0xC0FF_EE00).await?;
    db.flush().await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    // Demote the target: overwrite its tokens with orthogonal vectors (MaxSim 0).
    let orthogonal = to_value(&[basis(4), basis(5)]);
    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (d:Doc {title:'target'}) SET d.tokens = $toks")
        .param("toks", orthogonal)
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let titles = query_titles(&db, 5).await?;
    assert_ne!(
        titles.first().map(String::as_str),
        Some("target"),
        "after demotion the target must NOT rank first: {titles:?}"
    );

    // Now restore + delete it; it must vanish from results.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title:'target'}) DETACH DELETE d")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let titles = query_titles(&db, 40).await?;
    assert!(
        !titles.iter().any(|t| t == "target"),
        "deleted doc must not appear: {titles:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal-column hiding + metadata
// ---------------------------------------------------------------------------

/// The derived `__fde_*` column must not leak into user output: not in `keys(d)`, not in
/// `uni.schema.labelInfo`. The MUVERA index itself IS listed (the user created it).
#[tokio::test]
async fn muvera_internal_column_hidden() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 20, 0x5151_5151).await?;
    db.flush().await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    // keys(d) must not contain an internal column.
    let res = db
        .session()
        .query("MATCH (d:Doc {title:'target'}) RETURN keys(d) AS k")
        .await?;
    let keys: Vec<String> = res.rows()[0].get::<Vec<String>>("k").unwrap_or_default();
    assert!(
        !keys.iter().any(|k| k.starts_with("__")),
        "internal FDE column leaked into keys(d): {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k == "tokens"),
        "tokens should be present"
    );

    // labelInfo must not list the internal column.
    let info = db
        .session()
        .query("CALL uni.schema.labelInfo('Doc') YIELD property RETURN property")
        .await?;
    let props: Vec<String> = info
        .rows()
        .iter()
        .map(|r| r.get::<String>("property").unwrap())
        .collect();
    assert!(
        !props.iter().any(|p| p.starts_with("__")),
        "internal FDE column leaked into labelInfo: {props:?}"
    );

    // The MUVERA index IS persisted and listed.
    let has_muvera = db.indexes().list(Some("Doc")).into_iter().any(|idx| {
        matches!(idx, IndexDefinition::Vector(c) if c.property == "tokens" && c.name == "tok_idx")
    });
    assert!(
        has_muvera,
        "the MUVERA index should be listed for the label"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Cross-surface parity: the `uni.schema.createIndex` procedure
// ---------------------------------------------------------------------------

/// MUVERA works via the `uni.schema.createIndex` procedure exactly like the DDL path
/// (same shared option parser), including backfill over already-flushed rows.
#[tokio::test]
async fn muvera_via_procedure_createindex() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 60, 0x9999_1111).await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CALL uni.schema.createIndex('Doc', 'tokens', \
         {type: 'VECTOR', algorithm: 'muvera', k_sim: 4, reps: 8, d_proj: 8, inner: 'flat'})",
    )
    .await?;
    tx.commit().await?;

    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "procedure-path MUVERA should rank the maximizer first: {titles:?}"
    );
    // The persisted index is a MUVERA index, and the FDE column stays hidden.
    let is_muvera = db.indexes().list(Some("Doc")).into_iter().any(|idx| {
        matches!(idx, IndexDefinition::Vector(c)
            if c.property == "tokens" && matches!(c.index_type, VectorIndexType::Muvera { .. }))
    });
    assert!(is_muvera, "procedure should persist a MUVERA index");
    Ok(())
}

/// Default-ANN parity: a dense vector index created WITHOUT specifying the algorithm
/// resolves to IVF_PQ via BOTH the DDL and the procedure path (previously the procedure
/// defaulted to HNSW — the divergence this alignment fixes).
#[tokio::test]
async fn dense_default_ann_is_ivf_pq_on_both_paths() -> anyhow::Result<()> {
    async fn dense_index_type(create_via_procedure: bool) -> anyhow::Result<VectorIndexType> {
        let db = Uni::temporary().build().await?;
        db.schema()
            .label("Vec")
            .property("emb", DataType::Vector { dimensions: DIM })
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute_with("CREATE (:Vec {emb: $e})")
            .param("e", Value::List(vec![Value::Float(0.0); DIM]))
            .run()
            .await?;
        tx.commit().await?;
        db.flush().await?;

        let tx = db.session().tx().await?;
        if create_via_procedure {
            // No `algorithm` → canonical default.
            tx.execute("CALL uni.schema.createIndex('Vec', 'emb', {type: 'VECTOR'})")
                .await?;
        } else {
            // No `type` in OPTIONS → canonical default.
            tx.execute("CREATE VECTOR INDEX v_idx FOR (n:Vec) ON (n.emb) OPTIONS {}")
                .await?;
        }
        tx.commit().await?;

        Ok(db
            .indexes()
            .list(Some("Vec"))
            .into_iter()
            .find_map(|idx| match idx {
                IndexDefinition::Vector(c) if c.property == "emb" => Some(c.index_type),
                _ => None,
            })
            .expect("vector index on emb"))
    }

    assert!(
        matches!(
            dense_index_type(false).await?,
            VectorIndexType::IvfPq { .. }
        ),
        "DDL default ANN must be IVF_PQ"
    );
    assert!(
        matches!(dense_index_type(true).await?, VectorIndexType::IvfPq { .. }),
        "procedure default ANN must be IVF_PQ (aligned with DDL)"
    );
    Ok(())
}

/// Reproduces the schema-BUILDER creation path (the surface the Python `.index()` API
/// uses → `rebuild_indexes_for_label` → `create_vector_index`), which the DDL/procedure
/// e2e tests above don't exercise.
#[tokio::test]
async fn muvera_via_schema_builder_path() -> anyhow::Result<()> {
    use uni_db::{VectorAlgo, VectorIndexCfg, VectorMetric};
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 40, 0x7777_3333).await?;
    db.flush().await?;

    db.schema()
        .label("Doc")
        .index(
            "tokens",
            uni_db::IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Muvera {
                    k_sim: 4,
                    reps: 8,
                    d_proj: 8,
                    seed: uni_db::api::schema::DEFAULT_FDE_SEED,
                    inner: Box::new(VectorAlgo::Flat),
                },
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    // A plain read must not error (this is what the Python test tripped on).
    let keys_rows = db
        .session()
        .query("MATCH (d:Doc {title:'target'}) RETURN keys(d) AS k")
        .await?;
    assert_eq!(keys_rows.rows().len(), 1, "target row must be readable");

    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "builder-path MUVERA should rank the maximizer first: {titles:?}"
    );
    Ok(())
}
