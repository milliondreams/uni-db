//! WS-A — the GraphCompute *projection contract*: L0-consistent, scoped-or-loud,
//! mismatch-named, drift-detected. Each test here pins one silent-wrong-answer
//! (TRAP) gap the projection boundary used to have, exercised through the real
//! host-bridge / CALL path over committed-but-unflushed L0.

use std::sync::Arc;

use uni_db::{DataType, Uni};
use uni_plugin::traits::algorithm::GraphProjectionSpec;
use uni_plugin::{Capability, CapabilitySet};
use uni_plugin_builtin::algorithms::bridge::host_bridge_from_storage;

/// Build an `AlgorithmHostBridge` that sees the db's *committed-but-unflushed*
/// L0 — mirroring the real CALL path (`algo.rs::L0Manager::from_snapshot`),
/// which snapshots the writer's current L0 + pending-flush generations. Without
/// this the bridge is built with `l0 = None` and only sees flushed storage.
fn l0_aware_bridge(
    db: &Uni,
    caps: CapabilitySet,
) -> anyhow::Result<uni_plugin_builtin::algorithms::bridge::AlgorithmHostBridge> {
    let writer = db.writer().expect("in-memory db has a writer");
    let l0 = Arc::new(uni_db::runtime::l0_manager::L0Manager::from_snapshot(
        writer.l0_manager.get_current(),
        writer.l0_manager.get_pending_flush(),
    ));
    Ok(host_bridge_from_storage(db.storage(), Some(l0), caps))
}

fn broad_caps() -> CapabilitySet {
    CapabilitySet::from_iter_of([
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ])
}

/// Seeds a 4-node `:Node` graph with 4 `:LINKS` edges in committed-but-unflushed
/// L0. No properties — callers that need them declare their own schema.
async fn seed_node_graph(db: &Uni) -> anyhow::Result<()> {
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
    Ok(())
}

/// G9 — an unscoped projection (no `nodeLabels`/`edgeTypes`) must fail loud even
/// under the *broad* `**` HostQuery grant, instead of silently projecting every
/// declared label/edge-type (which pollutes index-keyed kernels when unrelated
/// data coexists). Naming labels, or an explicit `projectAll:true`, is required.
///
/// #151 added the fail-closed guard only under a *restricted* scope; this pins
/// the live other half of that ticket — the default grant now fails loud too.
#[tokio::test]
async fn g9_unscoped_projection_fails_loud_under_broad_grant() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_node_graph(&db).await?;
    let bridge = l0_aware_bridge(&db, broad_caps())?;

    // Unscoped, no opt-in, broad grant → rejected fail-loud (used to project all).
    let unscoped = GraphProjectionSpec {
        include_reverse: false,
        ..GraphProjectionSpec::default()
    };
    let err = bridge
        .project_for_graph_compute(&unscoped)
        .await
        .expect_err("an unscoped projection must be rejected under a broad grant (G9)");
    assert_eq!(err.code, 0x804, "unscoped projection is fail-loud (0x804)");
    let msg = err.to_string();
    assert!(
        msg.contains("projectAll") || msg.contains("unscoped"),
        "the error must explain the opt-in, got: {msg}"
    );

    // Explicit whole-graph opt-in → succeeds.
    let opted_in = GraphProjectionSpec {
        include_reverse: false,
        project_all: true,
        ..GraphProjectionSpec::default()
    };
    let proj_all = bridge.project_for_graph_compute(&opted_in).await?;
    assert_eq!(
        proj_all.vertex_count(),
        4,
        "projectAll projects the whole graph"
    );

    // Naming the label scopes it → succeeds.
    let scoped = GraphProjectionSpec {
        node_labels: vec!["Node".into()],
        edge_types: vec!["LINKS".into()],
        include_reverse: false,
        ..GraphProjectionSpec::default()
    };
    let proj_scoped = bridge.project_for_graph_compute(&scoped).await?;
    assert_eq!(
        proj_scoped.vertex_count(),
        4,
        "a scoped projection is allowed"
    );
    Ok(())
}

/// G11 — a whole-graph projection must not silently omit vertices whose label is
/// present in storage/L0 but absent from the schema (uni-db permits schemaless
/// `CREATE (:X)` without a prior `schema().label("X")`). The drift is now named
/// and rejected fail-loud; a scoped projection is exempt.
#[tokio::test]
async fn g11_undeclared_label_fails_whole_graph_projection() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    // Declare ONLY :Node / :LINKS.
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
    tx.execute("CREATE (:Node {name: 'A'})").await?;
    // A schemaless vertex under an UNDECLARED label — the G11 trap.
    tx.execute("CREATE (:Seed)").await?;
    tx.commit().await?;

    let bridge = l0_aware_bridge(&db, broad_caps())?;

    // A whole-graph (projectAll) projection would silently drop the :Seed vertex
    // — now rejected fail-loud, naming the undeclared label.
    let all = GraphProjectionSpec {
        include_reverse: false,
        project_all: true,
        ..GraphProjectionSpec::default()
    };
    let err = bridge
        .project_for_graph_compute(&all)
        .await
        .expect_err("a whole-graph projection over an undeclared label must fail loud (G11)");
    assert_eq!(
        err.code, 0x804,
        "undeclared-label drift is fail-loud (0x804)"
    );
    assert!(
        err.to_string().contains("Seed"),
        "the error must name the undeclared label, got: {err}"
    );

    // Scoping to the declared label is exempt — :Seed is intentionally (not
    // silently) excluded because the caller named exactly what it wants.
    let scoped = GraphProjectionSpec {
        node_labels: vec!["Node".into()],
        edge_types: vec!["LINKS".into()],
        include_reverse: false,
        ..GraphProjectionSpec::default()
    };
    let proj = bridge.project_for_graph_compute(&scoped).await?;
    assert_eq!(
        proj.vertex_count(),
        1,
        "only the declared :Node vertex projects"
    );
    Ok(())
}

/// G12 — stored node/edge property *values* must project correctly from
/// committed-but-unflushed L0, instead of reading `NaN` (node/edge props) or
/// silently defaulting to `1.0` (edge weight) until a flush lands them in Lance.
///
/// Before the fix, `collect_edges` / `collect_node_properties` passed `ctx =
/// None` to the batch property reads, skipping the L0 overlay that the structure
/// read already applied — so topology projected but property values were poison.
/// The property fixture shared by the L0 ladder: four `:Node` rows carrying
/// distinct `score` values and four `:LINKS` edges carrying distinct `w`
/// weights, committed but **deliberately not flushed**.
///
/// The values are non-default on purpose — a silent NaN or a defaulted `1.0`
/// weight cannot be mistaken for a real read.
async fn seed_property_graph(db: &Uni) -> anyhow::Result<([f64; 4], [f64; 4])> {
    db.schema()
        .label("Node")
        .property("score", DataType::Float)
        .done()
        .edge_type("LINKS", &["Node"], &["Node"])
        .property("w", DataType::Float)
        .done()
        .apply()
        .await?;

    let node_scores = [10.0_f64, 20.0, 30.0, 40.0];
    let edge_weights = [1.5_f64, 2.5, 3.5, 4.5];

    let session = db.session();
    let tx = session.tx().await?;
    for (name, score) in ["A", "B", "C", "D"].iter().zip(node_scores) {
        tx.execute(&format!(
            "CREATE (:Node {{name: '{name}', score: {score}}})"
        ))
        .await?;
    }
    for ((a, b), w) in [("A", "B"), ("B", "C"), ("C", "A"), ("A", "D")]
        .iter()
        .zip(edge_weights)
    {
        tx.execute(&format!(
            "MATCH (a:Node {{name: '{a}'}}), (b:Node {{name: '{b}'}}) \
             CREATE (a)-[:LINKS {{w: {w}}}]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok((node_scores, edge_weights))
}

/// The spec the ladder projects with: scoped, weighted, both property columns.
fn property_spec() -> GraphProjectionSpec {
    GraphProjectionSpec {
        node_labels: vec!["Node".into()],
        edge_types: vec!["LINKS".into()],
        include_reverse: false,
        weight_property: Some("w".into()),
        node_properties: vec!["score".into()],
        edge_properties: vec!["w".into()],
        ..GraphProjectionSpec::default()
    }
}

/// **Rung 2 of the L0 ladder** — the same fixture and the same spec as `g12`,
/// projected through a bridge built with `l0 = None`.
///
/// This is the construction the CALL path produces whenever the procedure host
/// is assembled from an `L0Context::empty()` — i.e. whenever
/// `Executor::get_context()` yields `None`. The projection then pins no L0
/// snapshot (`ProjectionBuilder::build`) and property reads see flushed storage
/// only, which for a write-back march means *an earlier step's value*: correct
/// arithmetic over stale inputs, no error anywhere.
///
/// `g12` closed this for the L0-aware construction. The degrade one layer up is
/// still silent, which is the Theme-C gap and the leading candidate for the
/// rare wrong-value report (REQ-D3 candidate C1).
#[tokio::test]
#[ignore = "REPRO (red on 254b4c26c: reads flushed storage only, silently): flips \
            when Phase 2 makes the detached construction loud. \
            Run with --run-ignored all."]
async fn repro_c1_l0_detached_projection_must_not_silently_read_stale_properties()
-> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let (node_scores, _edge_weights) = seed_property_graph(&db).await?;

    // The degrade under test: `None` where `l0_aware_bridge` passes `Some(l0)`.
    let bridge = host_bridge_from_storage(db.storage(), None, broad_caps());
    let proj = bridge.project_for_graph_compute(&property_spec()).await?;

    // Measured on baseline: the projection succeeds and reports ZERO vertices —
    // every row is still in unflushed L0. Nothing errors. A partially-flushed
    // database (the shape a write-back march produces) would instead project the
    // rows that happen to have been flushed, carrying an *earlier step's* values.
    // Both outcomes are silent; this one is simply the fully-unflushed extreme.
    assert_eq!(
        proj.vertex_count(),
        4,
        "an L0-detached projection must see committed-but-unflushed rows or fail \
         loud; it silently projected an empty graph instead"
    );

    let scores = proj
        .node_property("score")
        .expect("score column materialized")
        .to_vec();
    assert!(
        scores.iter().all(|v| v.is_finite()),
        "an L0-detached projection must not silently yield NaN scores: {scores:?}"
    );
    let mut sorted = scores.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    assert_eq!(
        sorted, node_scores,
        "an L0-detached projection must either read committed L0 or fail loud — \
         never return plausible-but-stale values"
    );
    Ok(())
}

#[tokio::test]
async fn g12_stored_property_values_project_over_unflushed_l0() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let (node_scores, edge_weights) = seed_property_graph(&db).await?;
    // Deliberately NO flush — the projection must read property values from L0.

    let bridge = l0_aware_bridge(&db, broad_caps())?;
    let proj = bridge.project_for_graph_compute(&property_spec()).await?;
    assert_eq!(proj.vertex_count(), 4, "all four nodes project");
    assert_eq!(proj.edge_count(), 4, "all four edges project");

    // Node property column carries the real stored scores (order-independent),
    // never NaN.
    let mut scores = proj
        .node_property("score")
        .expect("score column materialized")
        .to_vec();
    assert!(scores.iter().all(|v| v.is_finite()), "no NaN: {scores:?}");
    scores.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(scores, node_scores, "node property values read from L0");

    // Edge property column carries the real stored weights, never NaN.
    let mut eprops = proj
        .edge_property("w")
        .expect("edge property column materialized")
        .to_vec();
    assert!(eprops.iter().all(|v| v.is_finite()), "no NaN: {eprops:?}");
    eprops.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(eprops, edge_weights, "edge property values read from L0");

    // The weight-property column is the real weight, NOT the silent 1.0 default.
    let mut weights: Vec<f64> = Vec::new();
    for slot in 0..proj.vertex_count() as u32 {
        let start = proj.out_edge_start(slot);
        let end = proj.out_edge_start(slot + 1);
        for k in 0..end - start {
            weights.push(proj.out_weight(slot, k));
        }
    }
    weights.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        weights, edge_weights,
        "edge weights read from L0, not defaulted to 1.0"
    );
    assert!(
        weights.iter().any(|&w| (w - 1.0).abs() > 1e-9),
        "at least one weight differs from the 1.0 default"
    );
    Ok(())
}
