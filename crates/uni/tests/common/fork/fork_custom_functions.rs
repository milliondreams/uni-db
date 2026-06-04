// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Rust guideline compliant

//! Custom-function sharing semantics between primary and forked `Uni`s.
//!
//! `at_fork` in `crates/uni/src/api/mod.rs` shares the parent's
//! `custom_functions` `Arc<RwLock<CustomFunctionRegistry>>` and the parent's
//! cached `df_session_template` (the latter is read-only). This encodes the
//! intent: **custom functions are Uni-level**, shared across the primary and
//! all its forks. These tests pin that intent down so a future refactor that
//! either (a) accidentally isolates registries per fork, or (b) accidentally
//! leaks function lookups across unrelated `Uni` instances, gets caught.

use anyhow::Result;
use uni_db::{Uni, Value};

/// UDFs registered on the primary are visible on a fork created afterward.
#[tokio::test]
async fn primary_udf_visible_on_fork_after_registration() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.functions().register("double", |args| {
        let n = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
        Ok(Value::Int(n * 2))
    })?;

    let forked = db.session().fork("with-double").await?;
    let r = forked.query("RETURN double(7) AS val").await?;
    assert_eq!(r.rows()[0].get::<i64>("val")?, 14);

    Ok(())
}

/// UDFs registered on a fork's session are visible on the primary
/// (the registry is shared via `Arc<RwLock<...>>`).
#[tokio::test]
async fn fork_udf_visible_on_primary() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let forked = db.session().fork("with-triple").await?;
    db.functions().register("triple", |args| {
        let n = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
        Ok(Value::Int(n * 3))
    })?;
    // Sanity: the fork sees the UDF.
    let from_fork = forked.query("RETURN triple(5) AS val").await?;
    assert_eq!(from_fork.rows()[0].get::<i64>("val")?, 15);

    // The primary, sharing the same registry, must also see it.
    let from_primary = db.session().query("RETURN triple(5) AS val").await?;
    assert_eq!(from_primary.rows()[0].get::<i64>("val")?, 15);

    Ok(())
}

/// UDFs are not leaked across independent `Uni` instances — even after
/// forking. Guards against a future template-sharing regression that would
/// pool UDFs at the process level.
#[tokio::test]
async fn fork_udf_does_not_leak_to_other_uni_instance() -> Result<()> {
    let db_a = Uni::in_memory().build().await?;
    let db_b = Uni::in_memory().build().await?;

    let forked_a = db_a.session().fork("isolated").await?;
    db_a.functions().register("quad", |args| {
        let n = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
        Ok(Value::Int(n * 4))
    })?;

    // db_a and its fork see `quad`.
    let r_fork = forked_a.query("RETURN quad(3) AS val").await?;
    assert_eq!(r_fork.rows()[0].get::<i64>("val")?, 12);

    // db_b (a separate Uni) must NOT see it.
    let r_b = db_b.session().query("RETURN quad(3) AS val").await;
    assert!(r_b.is_err(), "quad() leaked from db_a to db_b: {r_b:?}");

    Ok(())
}
