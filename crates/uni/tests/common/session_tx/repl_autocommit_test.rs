// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for the CLI REPL write bug: the REPL/one-shot path routed
//! every statement through the read-only `Session::query`, so `CREATE`/`SET`/
//! `MERGE`/`DELETE`/DDL errored instead of executing. `Session::run` classifies
//! the statement and auto-commits writes; these tests pin that behavior.

use anyhow::Result;
use uni_db::Uni;

/// The pre-fix bug: routing a write through the read-only `query` path errors,
/// so a REPL using `query` for everything could never create data.
#[tokio::test]
async fn test_query_path_rejects_write_demonstrating_bug() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // This is exactly what the old REPL did for a CREATE — and it fails.
    let err = db
        .session()
        .query("CREATE (:Z)")
        .await
        .expect_err("the read-only query() path must still reject CREATE");
    assert!(
        err.to_string().contains("read-only"),
        "error should mention read-only, got: {err}"
    );
    Ok(())
}

/// `Session::run` auto-commits a write and the result is visible afterwards.
#[tokio::test]
async fn test_run_autocommits_write_and_is_visible() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.session().run("CREATE (:X {n: 1})").await?;

    // The auto-commit must be durable/visible to a subsequent read.
    let result = db.session().run("MATCH (x:X) RETURN x.n AS n").await?;
    assert_eq!(result.len(), 1, "the auto-committed node must be visible");
    let n: i64 = result.rows()[0].get("n")?;
    assert_eq!(n, 1);
    Ok(())
}

/// A write WITH a trailing `RETURN` keeps its rows — this is why the write path
/// uses `tx.query` (which returns rows) and not `tx.execute` (which discards).
#[tokio::test]
async fn test_run_write_with_return_keeps_rows() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let result = db.session().run("CREATE (:Y) RETURN 1 AS k").await?;
    assert_eq!(result.len(), 1, "CREATE … RETURN must yield its row");
    let k: i64 = result.rows()[0].get("k")?;
    assert_eq!(k, 1);
    Ok(())
}

/// A pure read through `run` is forwarded to the read path and works.
#[tokio::test]
async fn test_run_forwards_reads() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.session().run("CREATE (:R {v: 7})").await?;

    let result = db.session().run("MATCH (r:R) RETURN r.v AS v").await?;
    assert_eq!(result.len(), 1);
    let v: i64 = result.rows()[0].get("v")?;
    assert_eq!(v, 7);
    Ok(())
}

/// The read-only `query()` path is unchanged: it still rejects writes.
#[tokio::test]
async fn test_query_still_read_only_after_fix() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let err = db
        .session()
        .query("CREATE (:Z)")
        .await
        .expect_err("query() must remain read-only");
    assert!(
        err.to_string().contains("read-only"),
        "error should mention read-only, got: {err}"
    );
    Ok(())
}
