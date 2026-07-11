//! End-to-end tests for `uni.algo.gcpagerank` — Personalized PageRank driven
//! through the GraphCompute kernel catalog (proposal §9.4 E-1/E-5).
//!
//! Exercises the full stack: `CALL` dispatch → `AlgorithmProvider::run` →
//! `AlgorithmHostBridge::project_for_graph_compute` (gated on `GraphCompute` +
//! `HostQuery`) → an `AlgoSession` driving the coarse kernels → `emit` → the
//! `(nodeId, score)` result batch. The graph lives in unflushed L0, so the
//! kernels must observe L0 data exactly as the native providers do.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use uni_db::{DataType, Uni};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, Plugin, PluginError, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, QName, Scope, SideEffects,
};

/// Builds `A→B→C→A` (a 3-cycle) plus `A→D` in committed-but-unflushed L0.
/// Returns the vertex id of `A`.
async fn build_graph(db: &Uni) -> anyhow::Result<i64> {
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
    // Deliberately no flush: the kernels must observe L0 data.

    let res = session
        .query("MATCH (a:Node {name: 'A'}) RETURN id(a) AS vid")
        .await?;
    Ok(res.rows()[0].get::<i64>("vid")?)
}

#[tokio::test]
async fn gcpagerank_via_call_conserves_mass_over_l0() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let session = db.session();

    let query = format!(
        "CALL uni.algo.gcpagerank({vid_a}, 0.85, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
         YIELD nodeId, score RETURN nodeId, score"
    );
    let res = session.query(&query).await?;
    let rows = res.rows();
    assert_eq!(rows.len(), 4, "one score row per vertex");

    // Every score is finite and non-negative, and the mass is conserved (M-2).
    let mut total = 0.0;
    for row in rows {
        let s = row.get::<f64>("score")?;
        assert!(
            s.is_finite() && s >= 0.0,
            "score must be a valid probability"
        );
        total += s;
    }
    assert!(
        (total - 1.0).abs() < 1e-6,
        "PPR mass must sum to 1, got {total}"
    );
    Ok(())
}

#[tokio::test]
async fn gcpagerank_is_deterministic_e2e() -> anyhow::Result<()> {
    // E-5: bitwise-identical results across runs, proving §5.3 determinism holds
    // through the whole stack (projection sort + fixed-order kernel reductions).
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let session = db.session();
    let query = format!(
        "CALL uni.algo.gcpagerank({vid_a}, 0.85) YIELD nodeId, score RETURN nodeId, score \
         ORDER BY nodeId"
    );

    let collect = |res: &uni_db::QueryResult| -> anyhow::Result<Vec<(i64, u64)>> {
        res.rows()
            .iter()
            .map(|r| Ok((r.get::<i64>("nodeId")?, r.get::<f64>("score")?.to_bits())))
            .collect()
    };
    let first = collect(&session.query(&query).await?)?;
    let second = collect(&session.query(&query).await?)?;
    assert_eq!(
        first, second,
        "gcpagerank must be bitwise-reproducible via CALL"
    );
    Ok(())
}

/// A third-party plugin registering the GraphCompute PageRank provider under its
/// own namespace, with a configurable capability set to exercise the gates.
struct ExampleGcPlugin {
    manifest: OnceLock<PluginManifest>,
    caps: CapabilitySet,
}

impl ExampleGcPlugin {
    fn new(caps: CapabilitySet) -> Self {
        Self {
            manifest: OnceLock::new(),
            caps,
        }
    }
}

impl Plugin for ExampleGcPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: uni_plugin::PluginId::new("examplegc"),
            version: "0.1.0".parse().expect("static version"),
            abi: AbiRange::parse("^1").expect("static abi"),
            depends_on: vec![],
            capabilities: self.caps.clone(),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "third-party GraphCompute plugin (test)".to_owned(),
            metadata: BTreeMap::new(),
        })
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.algorithm(
            QName::new("examplegc", "pr"),
            Arc::new(
                uni_plugin_builtin::algorithms::graph_compute::provider::GraphComputePageRankProvider::new(),
            ),
        )?;
        Ok(())
    }
}

#[tokio::test]
async fn l2_kernels_denied_without_graph_compute_cap() -> anyhow::Result<()> {
    // L-2: a provider with HostQuery but WITHOUT `GraphCompute` is denied the
    // kernel surface — the orthogonal-gate rule (proposal §4.6 / error 0x86C).
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    db.add_plugin(ExampleGcPlugin::new(caps))?;

    let session = db.session();
    let query = format!("CALL examplegc.pr({vid_a}) YIELD nodeId, score RETURN nodeId");
    let err = session
        .query(&query)
        .await
        .expect_err("a provider lacking GraphCompute must be denied the kernel surface");
    let msg = err.to_string();
    assert!(
        msg.contains("graph-compute"),
        "error should name the missing `graph-compute` capability, got: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn gcpagerank_deadline_surfaces_typed_timeout_e2e() -> anyhow::Result<()> {
    // P0-7 (native path): a zero-millisecond wall-clock grant trips the very
    // first metered kernel, and the abort surfaces through CALL as a *typed*
    // GraphComputeIncomplete{Timeout} (0x867) — distinguishable from Exhausted /
    // IterationLimit — not a generic query error (proposal §5.2). Proves the
    // provider→DataFusion→query-API boundary preserves the structured reason.
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
        // A 0ms budget: the first `charge` sees the deadline already elapsed.
        Capability::WallClockMillisPerCall(0),
    ]);
    db.add_plugin(ExampleGcPlugin::new(caps))?;

    let session = db.session();
    let query = format!("CALL examplegc.pr({vid_a}) YIELD nodeId, score RETURN nodeId");
    let err = session
        .query(&query)
        .await
        .expect_err("a 0ms deadline must abort the invocation");
    match err {
        uni_common::UniError::GraphComputeIncomplete { detail } => assert_eq!(
            detail.reason,
            uni_common::GraphComputeIncompleteReason::Timeout,
            "an elapsed wall-clock deadline must be a Timeout, got {detail}"
        ),
        other => panic!("expected GraphComputeIncomplete{{Timeout}}, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn l1_runaway_budget_surfaces_typed_exhausted_e2e() -> anyhow::Result<()> {
    // L-1 / P0-7 (Exhausted): a 1-unit native-work grant is drained by the first
    // O(E) kernel, and the abort surfaces through CALL as a typed
    // GraphComputeIncomplete{Exhausted} (0x865) — distinct from Timeout /
    // IterationLimit (proposal §5.1/§5.2).
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
        // A 1-unit work budget: the first metered kernel exceeds it.
        Capability::GraphComputeWork(1),
    ]);
    db.add_plugin(ExampleGcPlugin::new(caps))?;

    let session = db.session();
    let query = format!("CALL examplegc.pr({vid_a}) YIELD nodeId, score RETURN nodeId");
    let err = session
        .query(&query)
        .await
        .expect_err("a 1-unit budget must abort the invocation");
    match err {
        uni_common::UniError::GraphComputeIncomplete { detail } => assert_eq!(
            detail.reason,
            uni_common::GraphComputeIncompleteReason::Exhausted,
            "a drained native-work budget must be Exhausted, got {detail}"
        ),
        other => panic!("expected GraphComputeIncomplete{{Exhausted}}, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn l3_project_needs_hostquery_too() -> anyhow::Result<()> {
    // L-3: a provider WITH `GraphCompute` but WITHOUT `HostQuery` can hold the
    // kernel surface but cannot `project` — the second orthogonal gate.
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let caps = CapabilitySet::from_iter_of([Capability::Algorithm, Capability::GraphCompute]);
    db.add_plugin(ExampleGcPlugin::new(caps))?;

    let session = db.session();
    let query = format!("CALL examplegc.pr({vid_a}) YIELD nodeId, score RETURN nodeId");
    let err = session
        .query(&query)
        .await
        .expect_err("project without HostQuery must be denied");
    let msg = err.to_string();
    assert!(
        msg.contains("HostQuery"),
        "error should name the missing `HostQuery` capability, got: {msg}"
    );
    Ok(())
}
