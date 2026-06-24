// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repros for the fork partial-mutation-wipe class (GitHub #102 + siblings): on a fork, a
//! partial mutation (`SET` one prop / `REMOVE` one prop / `MERGE ON MATCH SET`) of an
//! INHERITED relationship must MERGE — preserving the rel's other (untouched) properties —
//! not REPLACE its property set. The issue is relationship-specific on forks; node SET on a
//! fork already merges correctly (asserted here as the control).
//!
//! Faithful to #102: schemaless, `in_memory`, no flush (the fork inherits the parent's
//! committed-but-unflushed L0 per #97). Plus flushed variants to cover the branch path.

use anyhow::Result;
use uni_db::Uni;

/// Read the single `R` edge's `key` + `w` as nullable (so a wiped prop surfaces as `None`).
async fn edge_kw(uni: &uni_db::Session) -> Result<(Option<i64>, Option<f64>)> {
    let res = uni
        .query("MATCH ()-[r:R]->() RETURN r.key AS k, r.w AS w")
        .await?;
    let row = &res.rows()[0];
    let k: Option<i64> = serde_json::from_value(row.value("k").unwrap().clone().into())?;
    let w: Option<f64> = serde_json::from_value(row.value("w").unwrap().clone().into())?;
    Ok((k, w))
}

async fn create_inherited_edge(uni: &Uni) -> Result<uni_db::Session> {
    let s = uni.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:A)-[:R {key: 11, w: 1.0}]->(:B)")
        .await?;
    tx.commit().await?;
    Ok(s)
}

/// Probe: does a fork read the FULL inherited edge props with NO mutation at all?
#[tokio::test]
async fn a0_fork_reads_inherited_edge_unmutated() -> Result<()> {
    let uni = Uni::in_memory().build().await?;
    let s = create_inherited_edge(&uni).await?;
    let fork = s.fork("scn").await?;
    let (k, w) = edge_kw(&fork).await?;
    assert_eq!(
        (k, w),
        (Some(11), Some(1.0)),
        "fork must read full inherited edge pre-mutation"
    );
    Ok(())
}

/// Exact #102 repro: fork + SET one inherited-edge prop wipes the other (no flush, schemaless).
#[tokio::test]
async fn a1_fork_edge_set_inherited_no_flush() -> Result<()> {
    let uni = Uni::in_memory().build().await?;
    let s = create_inherited_edge(&uni).await?;

    let fork = s.fork("scn").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH ()-[r:R]->() SET r.w = 0.4").await?;
    ftx.commit().await?;

    let (k, w) = edge_kw(&fork).await?;
    assert_eq!(w, Some(0.4), "the SET must apply");
    assert_eq!(
        k,
        Some(11),
        "#102: untouched edge prop `key` must survive the SET"
    );
    Ok(())
}

/// Control: node SET on a fork must merge (the issue says this already works).
#[tokio::test]
async fn a2_fork_node_set_inherited_control() -> Result<()> {
    let uni = Uni::in_memory().build().await?;
    let s = uni.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:N {nk: 7, nx: 8})").await?;
    tx.commit().await?;

    let fork = s.fork("scn").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH (n:N) SET n.nk = 9").await?;
    ftx.commit().await?;

    let res = fork
        .query("MATCH (n:N) RETURN n.nk AS nk, n.nx AS nx")
        .await?;
    let row = &res.rows()[0];
    let nk: Option<i64> = serde_json::from_value(row.value("nk").unwrap().clone().into())?;
    let nx: Option<i64> = serde_json::from_value(row.value("nx").unwrap().clone().into())?;
    assert_eq!(nk, Some(9), "the SET must apply");
    assert_eq!(
        nx,
        Some(8),
        "node SET on a fork must preserve untouched props (control)"
    );
    Ok(())
}

/// Same as #102 but flush the fork after the SET (covers the branch flush path).
#[tokio::test]
async fn a3_fork_edge_set_inherited_with_flush() -> Result<()> {
    let uni = Uni::in_memory().build().await?;
    let s = create_inherited_edge(&uni).await?;

    let fork = s.fork("scn").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH ()-[r:R]->() SET r.w = 0.4").await?;
    ftx.commit().await?;
    fork.flush().await?;

    let (k, w) = edge_kw(&fork).await?;
    assert_eq!(w, Some(0.4), "the SET must apply after fork flush");
    assert_eq!(
        k,
        Some(11),
        "untouched edge prop `key` must survive across a fork flush"
    );
    Ok(())
}

/// REMOVE one inherited-edge prop: the removed one is gone, the others survive.
#[tokio::test]
async fn a4_fork_edge_remove_inherited() -> Result<()> {
    let uni = Uni::in_memory().build().await?;
    let s = create_inherited_edge(&uni).await?;

    let fork = s.fork("scn").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH ()-[r:R]->() REMOVE r.w").await?;
    ftx.commit().await?;

    let (k, w) = edge_kw(&fork).await?;
    assert_eq!(w, None, "REMOVE must drop the targeted prop");
    assert_eq!(k, Some(11), "REMOVE must preserve the untouched prop `key`");
    Ok(())
}

/// MERGE ... ON MATCH SET one inherited-edge prop: untouched props survive.
#[tokio::test]
async fn a5_fork_edge_merge_on_match_inherited() -> Result<()> {
    let uni = Uni::in_memory().build().await?;
    let s = create_inherited_edge(&uni).await?;

    let fork = s.fork("scn").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH (a:A),(b:B) MERGE (a)-[r:R]->(b) ON MATCH SET r.w = 0.4")
        .await?;
    ftx.commit().await?;

    let (k, w) = edge_kw(&fork).await?;
    assert_eq!(w, Some(0.4), "ON MATCH SET must apply");
    assert_eq!(
        k,
        Some(11),
        "MERGE ON MATCH SET must preserve untouched prop `key`"
    );
    Ok(())
}
