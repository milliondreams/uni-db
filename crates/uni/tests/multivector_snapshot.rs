// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! MVCC snapshot-isolation parity for multi-vector (ColBERT / MaxSim) search —
//! mirrors `sparse_index.rs::sparse_snapshot_isolates_reader_from_concurrent_insert`
//! and its dense twin in `dense_index.rs`.
//!
//! A reader transaction pins its snapshot at begin; a concurrent writer commits a
//! new doc that maximizes the query. The reader, querying within its pinned
//! snapshot via `uni.vector.query` over a `List<Vector>` column, must NOT see the
//! new doc — while a fresh session (live view) must. This also exercises the
//! multi-vector L0-union read path (the new doc lives only in L0), confirming it
//! surfaces committed-unflushed data correctly (the dense path had a bug here).

use uni_db::{DataType, Uni, Value};

const DIM: usize = 8;

fn basis(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[i] = 1.0;
    v
}

/// Query tokens `[e0, e1]`; the `target` doc has tokens == these (MaxSim 2.0).
fn query_tokens() -> Vec<Vec<f32>> {
    vec![basis(0), basis(1)]
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

struct Rng(u64);
impl Rng {
    fn unit(&mut self) -> Vec<f32> {
        let mut x = self.0;
        let mut v = vec![0.0f32; DIM];
        for slot in v.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *slot = (x >> 40) as f32 / (1u64 << 24) as f32 * 2.0 - 1.0;
        }
        self.0 = x;
        let norm = v.iter().map(|c| c * c).sum::<f32>().sqrt().max(1e-9);
        for c in &mut v {
            *c /= norm;
        }
        v
    }
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

/// Query within a pinned transaction snapshot, returning visible titles.
async fn query_titles_tx(tx: &uni_db::Transaction, k: usize) -> anyhow::Result<Vec<String>> {
    let lit = cypher_lit(&query_tokens());
    let rows = tx
        .query(&format!(
            "CALL uni.vector.query('Doc', 'tokens', {lit}, {k}, null, null, {{}}) \
             YIELD node, score RETURN node.title AS title"
        ))
        .await?;
    Ok(rows
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

/// Query the live view (fresh session), returning visible titles.
async fn query_titles(db: &Uni, k: usize) -> anyhow::Result<Vec<String>> {
    let lit = cypher_lit(&query_tokens());
    let res = db
        .session()
        .query(&format!(
            "CALL uni.vector.query('Doc', 'tokens', {lit}, {k}, null, null, {{}}) \
             YIELD node, score RETURN node.title AS title"
        ))
        .await?;
    Ok(res
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multivector_snapshot_isolates_reader_from_concurrent_insert() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;

    // Seed corpus (target == query tokens) and flush.
    let mut rng = Rng(0x5A4B_3C2D_1E0F_9876);
    let tx = db.session().tx().await?;
    for i in 0..20 {
        let (title, tokens) = if i == 10 {
            ("target".to_string(), query_tokens())
        } else {
            (
                format!("doc{i}"),
                (0..3).map(|_| rng.unit()).collect::<Vec<_>>(),
            )
        };
        tx.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
            .param("title", Value::String(title))
            .param("toks", to_value(&tokens))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let s_r = db.session();
    let tx_r = s_r.tx().await?;
    let before = query_titles_tx(&tx_r, 50).await?;
    assert!(
        before.contains(&"target".to_string()),
        "snapshot sees the seed corpus"
    );

    // Concurrent writer inserts a brand-new matching doc (L0-only) and commits.
    {
        let s_w = db.session();
        let tx_w = s_w.tx().await?;
        tx_w.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
            .param("title", Value::String("late_arrival".to_string()))
            .param("toks", to_value(&query_tokens()))
            .run()
            .await?;
        tx_w.commit().await?;
    }

    // The reader's pinned snapshot must not surface the post-begin insert.
    let after = query_titles_tx(&tx_r, 50).await?;
    assert!(
        !after.contains(&"late_arrival".to_string()),
        "reader snapshot must be isolated from the concurrent insert: {after:?}"
    );

    // A fresh session (live view) DOES see it — proving the L0-union read path
    // surfaces the committed-unflushed multi-vector doc.
    let live = query_titles(&db, 50).await?;
    assert!(
        live.contains(&"late_arrival".to_string()),
        "live view should see the committed multi-vector insert: {live:?}"
    );
    Ok(())
}
