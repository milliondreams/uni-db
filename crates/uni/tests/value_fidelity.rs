// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Value type-fidelity tests for the non-scalar `Value` arms (issue #95 §0).
//!
//! Two data-fidelity properties are asserted for each rich `Value` variant —
//! dense `Vector`, multi-vector (`List<Vector>`), and `Btic`:
//!
//!   1. **RETURN-projection round-trip** — `MATCH (n) RETURN n.col` materialises
//!      the stored column back with full TYPE fidelity (a `VECTOR` column yields
//!      `Value::Vector`, not a generic `Value::List` of floats), on both the L0
//!      (unflushed) and the flushed read paths.
//!   2. **Unflushed-WAL crash recovery** — a value committed but not flushed is
//!      recovered intact after `drop(db)` + reopen, because mutation values
//!      persist through the explicit Cypher-Value codec (not lossy untagged
//!      serde_json). A flushed base batch is inserted first so the snapshot
//!      manifest the engine needs to reopen exists; the unflushed `target` row is
//!      then recovered from the WAL and read back via a plain label scan (no
//!      secondary index is involved).
//!
//! The sparse arm already has the equivalent coverage in `sparse_index.rs`
//! (`sparse_property_projection_in_return`,
//! `sparse_wal_replay_after_reopen_unflushed_delta`); these close the same gap
//! for the previously-untested arms.

use uni_db::common::TemporalValue;
use uni_db::{DataType, Uni, Value};

// ---------------------------------------------------------------------------
// Dense Vector
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dense_vector_projection_round_trips() -> anyhow::Result<()> {
    // `RETURN i.embedding` must yield `Value::Vector` (type fidelity), on both
    // the L0 (pre-flush) and the flushed read paths.
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: 4 })
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {name: 'p', embedding: [0.5, 1.5, 2.5, 3.5]})")
        .await?;
    tx.commit().await?;

    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        let rows = db
            .session()
            .query("MATCH (i:Item {name: 'p'}) RETURN i.embedding AS emb")
            .await?;
        assert_eq!(rows.rows().len(), 1);
        match rows.rows()[0].value("emb") {
            Some(Value::Vector(v)) => assert_eq!(
                v,
                &vec![0.5, 1.5, 2.5, 3.5],
                "projected vector (flush={flush})"
            ),
            other => panic!("expected projected Value::Vector (flush={flush}), got {other:?}"),
        }
    }
    Ok(())
}

#[tokio::test]
async fn dense_vector_survives_wal_recovery() -> anyhow::Result<()> {
    // A dense vector committed but not flushed must round-trip through WAL
    // recovery as a genuine `Value::Vector` (not a degraded untagged-serde form).
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();

    {
        let db = Uni::open(path).build().await?;
        db.schema()
            .label("Item")
            .property("name", DataType::String)
            .property_nullable("embedding", DataType::Vector { dimensions: 4 })
            .apply()
            .await?;
        // base batch, flushed -> manifest exists for reopen
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Item {name: 'base', embedding: [0.0, 0.0, 0.0, 0.0]})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        // target, committed only (stays in the WAL, not flushed)
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Item {name: 'target', embedding: [1.0, 2.0, 3.0, 4.0]})")
            .await?;
        tx.commit().await?;
        drop(db);
    }

    let db = Uni::open(path).build().await?;
    let rows = db
        .session()
        .query("MATCH (i:Item {name: 'target'}) RETURN i.embedding AS emb")
        .await?;
    assert_eq!(rows.rows().len(), 1, "recovered target row must be present");
    match rows.rows()[0].value("emb") {
        Some(Value::Vector(v)) => {
            assert_eq!(v, &vec![1.0, 2.0, 3.0, 4.0], "recovered vector value")
        }
        other => panic!("expected recovered Value::Vector, got {other:?}"),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-vector (List<Vector>)
// ---------------------------------------------------------------------------

/// Declare `Doc(name STRING, tokens List<Vector{3}>)`. The typed multi-vector
/// write path (storing tokens as `List<FixedSizeList<Float32>>`) is reached only
/// when the column is declared on the label being written; an undeclared column
/// falls back to schemaless CV-string encoding (`List(Utf8)`).
async fn multivec_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("name", DataType::String)
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: 3 })),
        )
        .apply()
        .await?;
    Ok(())
}

/// A multi-vector write value: `Value::List(Vec<Value::List<Float>>)` — the
/// shape every working multi-vector test uses (a Cypher nested-list literal does
/// not route through the typed builder; a parameter of this shape does).
fn multivec_value(tokens: &[Vec<f32>]) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

/// Assert a projected multi-vector is a `Value::List` whose tokens are each a
/// `Value::Vector` (not a generic nested `Value::List` of floats).
fn assert_tokens(v: Option<&Value>, expected: &[Vec<f32>], ctx: &str) {
    match v {
        Some(Value::List(tokens)) => {
            assert_eq!(tokens.len(), expected.len(), "token count ({ctx})");
            for (i, (tok, exp)) in tokens.iter().zip(expected).enumerate() {
                match tok {
                    Value::Vector(f) => assert_eq!(f, exp, "token {i} ({ctx})"),
                    other => panic!("token {i} must be Value::Vector ({ctx}), got {other:?}"),
                }
            }
        }
        other => panic!("expected Value::List of tokens ({ctx}), got {other:?}"),
    }
}

#[tokio::test]
async fn multivector_projection_round_trips() -> anyhow::Result<()> {
    // `RETURN d.tokens` yields a `Value::List` of `Value::Vector` tokens (the
    // projection-fidelity fix applies to the nested tokens too), on both the L0
    // (pre-flush) and the flushed read paths. The L0 path previously hard-errored
    // ("expected List(FixedSizeList(.. Float32)) but found List(Utf8)") because the
    // result-column builder lacked a typed multi-vector arm and fell back to
    // CV-string encoding; `build_multivector_array` now closes that.
    let db = Uni::temporary().build().await?;
    multivec_schema(&db).await?;

    let expected = vec![vec![1.0_f32, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {name: 'p', tokens: $toks})")
        .param("toks", multivec_value(&expected))
        .run()
        .await?;
    tx.commit().await?;

    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        let rows = db
            .session()
            .query("MATCH (d:Doc {name: 'p'}) RETURN d.tokens AS t")
            .await?;
        assert_eq!(rows.rows().len(), 1);
        assert_tokens(
            rows.rows()[0].value("t"),
            &expected,
            &format!("flush={flush}"),
        );
    }
    Ok(())
}

#[tokio::test]
async fn multivector_survives_wal_recovery() -> anyhow::Result<()> {
    // A multi-vector committed but not flushed must round-trip through WAL
    // recovery as a `Value::List` of `Value::Vector` tokens.
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();

    {
        let db = Uni::open(path).build().await?;
        multivec_schema(&db).await?;
        let tx = db.session().tx().await?;
        tx.execute_with("CREATE (:Doc {name: 'base', tokens: $toks})")
            .param("toks", multivec_value(&[vec![0.0, 0.0, 0.0]]))
            .run()
            .await?;
        tx.commit().await?;
        db.flush().await?;
        let tx = db.session().tx().await?;
        tx.execute_with("CREATE (:Doc {name: 'target', tokens: $toks})")
            .param(
                "toks",
                multivec_value(&[vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]]),
            )
            .run()
            .await?;
        tx.commit().await?;
        drop(db);
    }

    let db = Uni::open(path).build().await?;
    // The WAL-recovered (unflushed) row lands in L0; read it directly via the now-
    // fixed L0 multi-vector projection (no post-reopen flush). A corrupted WAL
    // value would surface here as wrong tokens.
    let rows = db
        .session()
        .query("MATCH (d:Doc {name: 'target'}) RETURN d.tokens AS t")
        .await?;
    assert_eq!(rows.rows().len(), 1, "recovered target row must be present");
    let expected = vec![vec![1.0_f32, 2.0, 3.0], vec![4.0, 5.0, 6.0]];
    assert_tokens(rows.rows()[0].value("t"), &expected, "after WAL recovery");
    Ok(())
}

// ---------------------------------------------------------------------------
// Btic (temporal)
// ---------------------------------------------------------------------------

fn btic_value() -> Value {
    Value::Temporal(TemporalValue::Btic {
        lo: 473_385_600_000,         // 1985-01-01T00:00:00Z
        hi: 504_921_600_000,         // 1986-01-01T00:00:00Z
        meta: 0x7700_0000_0000_0000, // year/year, definite/definite
    })
}

#[tokio::test]
async fn btic_survives_wal_recovery() -> anyhow::Result<()> {
    // A Btic committed but not flushed must round-trip through WAL recovery as a
    // genuine `Value::Temporal(Btic)` (the CV codec, TAG_BTIC). Btic projection
    // is already covered in `btic_cypher_test.rs`; this closes the WAL gap.
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();

    {
        let db = Uni::open(path).build().await?;
        db.schema()
            .label("Fact")
            .property("name", DataType::String)
            .property_nullable("valid_at", DataType::Btic)
            .apply()
            .await?;
        // base row (valid_at omitted -> null), flushed for the manifest.
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Fact {name: 'base'})").await?;
        tx.commit().await?;
        db.flush().await?;
        // target Btic, committed only (stays in the WAL).
        let tx = db.session().tx().await?;
        tx.execute_with("CREATE (:Fact {name: 'target', valid_at: $va})")
            .param("va", btic_value())
            .run()
            .await?;
        tx.commit().await?;
        drop(db);
    }

    let db = Uni::open(path).build().await?;
    let rows = db
        .session()
        .query("MATCH (f:Fact {name: 'target'}) RETURN f.valid_at AS va")
        .await?;
    assert_eq!(rows.rows().len(), 1, "recovered target row must be present");
    match rows.rows()[0].value("va") {
        Some(Value::Temporal(TemporalValue::Btic { lo, hi, .. })) => {
            assert_eq!(*lo, 473_385_600_000, "recovered Btic lo");
            assert_eq!(*hi, 504_921_600_000, "recovered Btic hi");
        }
        other => panic!("expected recovered Temporal(Btic), got {other:?}"),
    }
    Ok(())
}
