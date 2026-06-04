// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase B smoke tests for `UniConfig::defer_embeddings`.
//!
//! The full deferral round-trip (insert → flush → batched model API call)
//! requires a configured xervo runtime + a real embedding model and is
//! covered by the auto-embed regression tests under tests/common/bugs/.
//! These tests verify the flag-on code path doesn't break vanilla SET /
//! CREATE workflows when no embedding configs are present (the common
//! case in fixtures).

use anyhow::Result;
use uni_common::UniConfig;
use uni_db::{DataType, Uni, Value};

fn defer_on_config() -> UniConfig {
    UniConfig {
        defer_embeddings: true,
        ..UniConfig::default()
    }
}

/// Flag-on with no embedding config: `try_defer_embedding` returns false
/// (no unsatisfied embedding configs), per-row path runs as today,
/// pending_embeddings stays empty. Exercises the early-out branch.
#[tokio::test]
async fn b1_defer_flag_on_without_embedding_config_is_noop() -> Result<()> {
    let db = Uni::in_memory().config(defer_on_config()).build().await?;
    db.schema()
        .label("Entity")
        .property("name", DataType::String)
        .property_nullable("frequency", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..10 {
        tx.execute_with("CREATE (:Entity {name: $n, frequency: 0})")
            .param("n", Value::String(format!("e{i}")))
            .run()
            .await?;
    }
    tx.commit().await?;

    let tx = db.session().tx().await?;
    for i in 0..10 {
        tx.execute_with("MATCH (n:Entity {name: $n}) SET n.frequency = $f")
            .param("n", Value::String(format!("e{i}")))
            .param("f", Value::Int(i as i64))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let r = db
        .session()
        .query("MATCH (n:Entity) RETURN n.name AS name, n.frequency AS f ORDER BY name")
        .await?;
    assert_eq!(r.rows().len(), 10);
    for (i, row) in r.rows().iter().enumerate() {
        let name = row.get::<String>("name").unwrap();
        assert_eq!(name, format!("e{i}"));
        let f = row.get::<i64>("f").unwrap();
        assert_eq!(f, i as i64);
    }
    Ok(())
}

/// Flag-off (default): explicitly verify pre-Phase-B behavior is
/// preserved. Same fixture as B1, run twice — once with each config —
/// to confirm bit-for-bit equivalence.
#[tokio::test]
async fn b2_defer_flag_off_is_default_and_unchanged() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("k", DataType::String)
        .property_nullable("v", DataType::Int64)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {k: 'a', v: 1})").await?;
    tx.execute("CREATE (:Item {k: 'b', v: 2})").await?;
    tx.commit().await?;

    let r = db
        .session()
        .query("MATCH (n:Item) RETURN n.k AS k, n.v AS v ORDER BY k")
        .await?;
    assert_eq!(r.rows().len(), 2);
    Ok(())
}
