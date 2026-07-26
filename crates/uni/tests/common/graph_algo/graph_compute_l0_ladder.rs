//! Phase 0 — the upper rungs of the L0 ladder, driven through the real CALL
//! path rather than a hand-built host bridge.
//!
//! Rung 2 (an explicitly L0-detached bridge) lives next to its `g12` sibling in
//! [`super::projection_contract`]. The rungs here are the ones a *user* can
//! reach without constructing anything: a Cypher-mode `graphRef`, and a fork
//! session.
//!
//! Bisecting the ladder is the point — it names which precondition actually
//! produces an L0-blind projection instead of guessing between five candidates.
//! See `docs/proposals/graphcompute_dynamics_requirements_response_2026-07-25.md`
//! §12.3.

use uni_db::{DataType, Uni};

/// Four `:Node` rows and four `:LINKS` edges, committed and **not flushed**.
/// Returns `id(A)`, the personalization source.
async fn seed_unflushed(db: &Uni) -> anyhow::Result<i64> {
    db.schema()
        .label("Node")
        .property("name", DataType::String)
        .done()
        .edge_type("LINKS", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for name in ["A", "B", "C", "D"] {
        tx.execute(&format!("CREATE (:Node {{name: '{name}'}})"))
            .await?;
    }
    for (a, b) in [("A", "B"), ("B", "C"), ("C", "A"), ("A", "D")] {
        tx.execute(&format!(
            "MATCH (a:Node {{name: '{a}'}}), (b:Node {{name: '{b}'}}) CREATE (a)-[:LINKS]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    // Deliberately NO db.flush().
    let res = session
        .query("MATCH (a:Node {name: 'A'}) RETURN id(a) AS vid")
        .await?;
    Ok(res.rows()[0].get::<i64>("vid")?)
}

const LABEL_SCOPE: &str = "{nodeLabels: ['Node'], edgeTypes: ['LINKS']}";
const CYPHER_SCOPE: &str = "{nodeQuery: 'MATCH (n:Node) RETURN id(n) AS id', \
     edgeQuery: 'MATCH (a:Node)-[:LINKS]->(b:Node) RETURN id(a) AS source, id(b) AS target'}";

/// Runs `uni.algo.gcpagerank` under the given graphRef and returns the row count.
async fn gcpagerank_rows(
    session: &uni_db::Session,
    vid: i64,
    scope: &str,
) -> anyhow::Result<usize> {
    let q = format!(
        "CALL uni.algo.gcpagerank({vid}, 0.85, {scope}) YIELD nodeId, score \
         RETURN nodeId, score"
    );
    Ok(session.query(&q).await?.rows().len())
}

/// **PIN (was a failed hypothesis)** — a Cypher-mode `graphRef` *does* see
/// committed-but-unflushed rows.
///
/// This was written as a reproduction, on the theory that
/// `QueryProcedureHost::from_components` hardcoding `l0_context: L0Context::empty()`
/// would leave the guest `graphRef` resolver L0-blind on an ordinary
/// writer-backed session. **It passed on the first run, so the theory is wrong.**
/// The Cypher/Named path routes projection through an *injected resolver*
/// (`run_algorithm_provider`, issue #151 P3) rather than through that host's
/// inner queries, so the empty `L0Context` never reaches the selection queries.
///
/// Kept as a pin rather than deleted: it is the regression guard for a
/// visibility property that had no coverage, and it records that this candidate
/// explanation for the rare-wrong-value report is closed.
#[tokio::test]
async fn pin_cypher_graph_ref_sees_committed_unflushed_rows() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let vid = seed_unflushed(&db).await?;
    let session = db.session();

    // Control: the same data, projected by label scope, is fully visible.
    assert_eq!(
        gcpagerank_rows(&session, vid, LABEL_SCOPE).await?,
        4,
        "label-scoped projection must see the committed rows"
    );

    assert_eq!(
        gcpagerank_rows(&session, vid, CYPHER_SCOPE).await?,
        4,
        "a Cypher-mode graphRef must resolve against the same committed state as \
         a label-scoped one"
    );
    db.shutdown().await?;
    Ok(())
}

/// **PROBE-FORK (rung 3)** — outcome genuinely unknown before running.
///
/// No fork × GraphCompute test exists anywhere in the repo, so whether a forked
/// session projects correctly is unverified rather than assumed. The regime is
/// materially different from the other rungs: `create_fork_2pc` flushes the
/// parent's L0 to L1 before branching, so the fork reads a freshly-flushed
/// parent plus its own L0.
///
/// Asserts parity with the parent. If it fails, that is its own work item — not
/// something to absorb quietly into a later phase.
#[tokio::test]
async fn probe_fork_graph_compute_projects_the_same_as_its_parent() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let vid = seed_unflushed(&db).await?;
    let session = db.session();

    let parent_rows = gcpagerank_rows(&session, vid, LABEL_SCOPE).await?;
    assert_eq!(parent_rows, 4, "parent projects all four nodes");

    let fork = session.fork("gc_probe").await?;
    let fork_rows = gcpagerank_rows(&fork, vid, LABEL_SCOPE).await?;
    assert_eq!(
        fork_rows, parent_rows,
        "a forked session must project the same graph as its parent"
    );
    db.shutdown().await?;
    Ok(())
}
