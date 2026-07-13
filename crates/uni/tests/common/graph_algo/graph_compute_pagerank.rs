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

#[tokio::test]
async fn gcwalks_via_call_emits_walk_sequences_over_l0() -> anyhow::Result<()> {
    // WS1: `emit_walks` egress reaches the user as (walk_id, step, nodeId) rows
    // through the CALL path, over unflushed L0.
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let session = db.session();

    let query = format!(
        "CALL uni.algo.gcwalks([{vid_a}], 4, 3, 1.0, 1.0, 42, \
         {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
         YIELD walk_id, step, nodeId RETURN walk_id, step, nodeId ORDER BY walk_id, step"
    );
    let res = session.query(&query).await?;
    let rows = res.rows();
    assert!(
        !rows.is_empty(),
        "walks must egress at least the start step"
    );

    // walk_id in 0..3; each walk's steps start at 0; nodeId is a real vertex id.
    let mut steps_by_walk: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for row in rows {
        let walk_id = row.get::<i64>("walk_id")?;
        let step = row.get::<i64>("step")?;
        let node_id = row.get::<i64>("nodeId")?;
        assert!((0..3).contains(&walk_id), "walk_id {walk_id} out of range");
        assert!(node_id >= 0, "nodeId must be a real vertex id");
        steps_by_walk.entry(walk_id).or_default().push(step);
    }
    for (walk_id, steps) in &steps_by_walk {
        assert_eq!(steps[0], 0, "walk {walk_id} must start at step 0");
        assert!(
            steps.windows(2).all(|w| w[1] == w[0] + 1),
            "walk {walk_id} steps must be contiguous"
        );
    }
    Ok(())
}

#[tokio::test]
async fn gcoverlap_via_call_yields_pair_rows_over_l0() -> anyhow::Result<()> {
    // WS2B: the per-edge `all_pairs_overlap` + `emit_pairs` path reaches the user
    // as (srcId, dstId, value) rows through CALL, over unflushed L0.
    let db = Uni::in_memory().build().await?;
    let _vid_a = build_graph(&db).await?;
    let session = db.session();

    let query = "CALL uni.algo.gcoverlap('count', 'adjacent', 0, \
         {nodeLabels: ['Node'], edgeTypes: ['LINKS']}) \
         YIELD srcId, dstId, value RETURN srcId, dstId, value"
        .to_owned();
    let res = session.query(&query).await?;
    // The result is well-formed: each row is a real edge with a finite support.
    for row in res.rows() {
        let src = row.get::<i64>("srcId")?;
        let dst = row.get::<i64>("dstId")?;
        let value = row.get::<f64>("value")?;
        assert!(src >= 0 && dst >= 0, "endpoints are real vertex ids");
        assert!(src < dst, "adjacent pairs are emitted with src < dst");
        assert!(
            value.is_finite() && value >= 0.0,
            "support is finite and ≥ 0"
        );
    }
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
async fn e5_hostquery_scopes_restrict_projection() -> anyhow::Result<()> {
    // E5: a plugin whose HostQuery grant is scoped to `Other` cannot project the
    // `Node` label — the projection is denied before any kernel runs.
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: vec!["Other".into()],
        },
    ]);
    db.add_plugin(ExampleGcPlugin::new(caps))?;

    let session = db.session();
    // Explicitly project the out-of-scope `Node` label.
    let query = format!(
        "CALL examplegc.pr({vid_a}, 0.85, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
         YIELD nodeId, score RETURN nodeId"
    );
    let err = session
        .query(&query)
        .await
        .expect_err("projecting a label outside the granted scopes must be denied");
    let msg = err.to_string();
    assert!(
        msg.contains("scope") || msg.contains("Node"),
        "error should name the out-of-scope label, got: {msg}"
    );
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
async fn g1_grant_above_size_budget_is_honored_e2e() -> anyhow::Result<()> {
    // G-1 (sanctioned flip, proposal §9): a `GraphComputeWork` grant *above* the
    // size-derived default is honored end-to-end — the CALL runs to completion
    // instead of the grant being clamped down. Here the grant (5e9) exceeds both
    // the tiny graph's `size_budget` and the absolute default ceiling (1e9); a
    // v1 `.min()` clamp would have silently discarded everything above
    // `size_budget`. The stronger "a raise is *required* to complete" assertion
    // lands in the AT-GRID flagship (phase 1); here we pin that a raised grant is
    // accepted and does not corrupt the result.
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
        // A grant well above the size-derived default and the 1e9 ceiling.
        Capability::GraphComputeWork(5_000_000_000),
    ]);
    db.add_plugin(ExampleGcPlugin::new(caps))?;

    let session = db.session();
    let query = format!("CALL examplegc.pr({vid_a}) YIELD nodeId, score RETURN nodeId, score");
    // A grant above size_budget must be honored, not clamped to a failure.
    let res = session.query(&query).await?;
    let rows = res.rows();
    assert_eq!(
        rows.len(),
        4,
        "one score row per vertex under the raised grant"
    );
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
        "PPR mass must sum to 1 under a raised budget, got {total}"
    );
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

// ---- DF family: registration-driven DataFusion path (proposal §6) --------

/// A provider wrapper that delegates PageRank to
/// [`GraphComputePageRankProvider`] but publishes a **chosen** `df_composable`
/// flag — so a test can register the *same* algorithm on the DataFusion path
/// and the row-based fallback and compare (DF-2 / DF-3).
struct DfFlagProvider {
    inner: uni_plugin_builtin::algorithms::graph_compute::provider::GraphComputePageRankProvider,
    sig: uni_plugin::traits::algorithm::AlgorithmSignature,
}

impl DfFlagProvider {
    fn new(df_composable: bool) -> Self {
        use uni_plugin::traits::algorithm::AlgorithmProvider as _;
        let inner =
            uni_plugin_builtin::algorithms::graph_compute::provider::GraphComputePageRankProvider::new();
        let mut sig = inner.signature().clone();
        sig.df_composable = df_composable;
        Self { inner, sig }
    }
}

impl uni_plugin::traits::algorithm::AlgorithmProvider for DfFlagProvider {
    fn signature(&self) -> &uni_plugin::traits::algorithm::AlgorithmSignature {
        &self.sig
    }
    fn run(
        &self,
        ctx: uni_plugin::traits::algorithm::AlgorithmContext<'_>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, uni_plugin::errors::FnError> {
        self.inner.run(ctx)
    }
}

/// A third-party plugin registering [`DfFlagProvider`] under an arbitrary
/// namespaced name with a chosen `df_composable` flag.
struct DfFlagPlugin {
    manifest: OnceLock<PluginManifest>,
    ns: &'static str,
    local: &'static str,
    df_composable: bool,
}

impl DfFlagPlugin {
    fn new(ns: &'static str, local: &'static str, df_composable: bool) -> Self {
        Self {
            manifest: OnceLock::new(),
            ns,
            local,
            df_composable,
        }
    }
}

impl Plugin for DfFlagPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: uni_plugin::PluginId::new(self.ns),
            version: "0.1.0".parse().expect("static version"),
            abi: AbiRange::parse("^1").expect("static abi"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([
                Capability::Algorithm,
                Capability::GraphCompute,
                Capability::HostQuery {
                    read_only: true,
                    scopes: Vec::new(),
                },
            ]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "third-party df-composable provider (test)".to_owned(),
            metadata: BTreeMap::new(),
        })
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.algorithm(
            QName::new(self.ns, self.local),
            Arc::new(DfFlagProvider::new(self.df_composable)),
        )?;
        Ok(())
    }
}

#[tokio::test]
async fn df2_df_path_and_row_path_return_identical_rows() -> anyhow::Result<()> {
    // DF-2: the same algorithm CALLed through the DataFusion path (a provider
    // declaring df_composable) and the row-based fallback (df_composable = false)
    // returns identical rows — the fallback is a correctness twin. Also DF-3: a
    // third-party `myco.algo.*` name reaches the DF path purely by *declaration*,
    // and a `*.algo.*` name that does NOT declare it stays on the row path (the
    // prefix no longer forces DataFusion).
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    db.add_plugin(DfFlagPlugin::new("mycodf", "pr", true))?; // -> DataFusion path
    db.add_plugin(DfFlagPlugin::new("mycorow", "pr", false))?; // -> row fallback

    let session = db.session();
    let run = |name: &str| {
        let q = format!(
            "CALL {name}.pr({vid_a}, 0.85, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
             YIELD nodeId, score RETURN nodeId, score ORDER BY nodeId"
        );
        let session = session.clone();
        async move {
            let res = session.query(&q).await?;
            anyhow::Ok(
                res.rows()
                    .iter()
                    .map(|r| Ok((r.get::<i64>("nodeId")?, r.get::<f64>("score")?.to_bits())))
                    .collect::<anyhow::Result<Vec<_>>>()?,
            )
        }
    };

    let df_rows = run("mycodf").await?;
    let row_rows = run("mycorow").await?;
    assert_eq!(df_rows.len(), 4, "DF path yields one score per vertex");
    assert_eq!(
        df_rows, row_rows,
        "DF path and row fallback must return identical rows (DF-2 parity)"
    );
    Ok(())
}

/// A provider that emits `k` separate single-row batches, to exercise the DF-4
/// streaming state machine's multi-batch forwarding (the `Draining` loop) — the
/// path every real single-batch provider never reaches.
struct MultiBatchProvider {
    sig: uni_plugin::traits::algorithm::AlgorithmSignature,
    k: usize,
}

impl MultiBatchProvider {
    fn new(k: usize) -> Self {
        use arrow_schema::{DataType, Field};
        let sig = uni_plugin::traits::algorithm::AlgorithmSignature {
            output_fields: vec![
                Field::new("nodeId", DataType::Int64, false),
                Field::new("score", DataType::Float64, false),
            ],
            df_composable: true,
            ..Default::default()
        };
        Self { sig, k }
    }
}

impl uni_plugin::traits::algorithm::AlgorithmProvider for MultiBatchProvider {
    fn signature(&self) -> &uni_plugin::traits::algorithm::AlgorithmSignature {
        &self.sig
    }
    fn run(
        &self,
        _ctx: uni_plugin::traits::algorithm::AlgorithmContext<'_>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, uni_plugin::errors::FnError> {
        use arrow_array::{Float64Array, Int64Array, RecordBatch};
        let schema: arrow_schema::SchemaRef =
            Arc::new(arrow_schema::Schema::new(self.sig.output_fields.clone()));
        let batches: Vec<datafusion::common::Result<RecordBatch>> = (0..self.k)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![i as i64])),
                        Arc::new(Float64Array::from(vec![1.0 / self.k as f64])),
                    ],
                )
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
            })
            .collect();
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(batches),
            ),
        ))
    }
}

struct MultiBatchPlugin {
    manifest: OnceLock<PluginManifest>,
    k: usize,
}

impl Plugin for MultiBatchPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: uni_plugin::PluginId::new("mymulti"),
            version: "0.1.0".parse().expect("static version"),
            abi: AbiRange::parse("^1").expect("static abi"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::Algorithm]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "multi-batch streaming provider (test)".to_owned(),
            metadata: BTreeMap::new(),
        })
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.algorithm(
            QName::new("mymulti", "gen"),
            Arc::new(MultiBatchProvider::new(self.k)),
        )?;
        Ok(())
    }
}

#[tokio::test]
async fn df4_multi_batch_provider_streams_every_batch() -> anyhow::Result<()> {
    // DF-4 (streaming state machine): a df_composable provider that emits K
    // separate batches is forwarded incrementally through `GraphProcedureCallExec`
    // — every batch's rows reach the caller. This exercises the `Draining` loop
    // with K>1 (the buffered `concat_batches` path it replaces, and every real
    // single-batch provider, never reach K>1). The plan-level "peak buffered
    // batches « K" metric is a physical-plan property asserted separately.
    let db = Uni::in_memory().build().await?;
    let _ = build_graph(&db).await?;
    let k = 5usize;
    db.add_plugin(MultiBatchPlugin {
        manifest: OnceLock::new(),
        k,
    })?;

    let session = db.session();
    let res = session
        .query("CALL mymulti.gen() YIELD nodeId, score RETURN nodeId, score ORDER BY nodeId")
        .await?;
    let rows = res.rows();
    assert_eq!(
        rows.len(),
        k,
        "every one of the K streamed batches must arrive"
    );
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<i64>("nodeId")?, i as i64);
        assert!((row.get::<f64>("score")? - 1.0 / k as f64).abs() < 1e-12);
    }
    Ok(())
}

#[tokio::test]
async fn q3_scratch_graph_is_never_observable_by_the_store() -> anyhow::Result<()> {
    // Q-3 (proposal §7b / §15.5, live-store): a Mode B-seq scratch graph is
    // session-local and **never observable by the store** — a concurrent reader
    // sees no trace during *or* after a B-seq run. The scratch graph holds no
    // store handle, so its (potentially large) mutable structure cannot leak into
    // the store's data or a concurrent query.
    use uni_plugin_builtin::algorithms::graph_compute::scratch::{ScratchGraph, ScratchRegistry};
    use uni_plugin_builtin::algorithms::graph_compute::{Arena, WorkBudget};

    let db = Uni::in_memory().build().await?;
    let _seed = build_graph(&db).await?; // seeds exactly 4 :Node nodes + 4 edges

    let session = db.session();
    let count_nodes = || {
        let session = session.clone();
        async move {
            let res = session.query("MATCH (n:Node) RETURN count(n) AS c").await?;
            anyhow::Ok(res.rows()[0].get::<i64>("c")?)
        }
    };
    let before = count_nodes().await?;
    assert_eq!(before, 4, "the store starts with the 4 seeded nodes");

    // Run a Mode B-seq program that builds a large mutable scratch graph via the
    // guest ABI — entirely session-local.
    let reg = ScratchRegistry::new();
    let sid = reg.open(ScratchGraph::new(
        WorkBudget::new(10_000_000),
        Arena::new(1 << 24, 1 << 20),
        0x513,
    ));
    for i in 0..100u64 {
        let r = reg.call_json(&format!(r#"{{"session":{sid},"op":"add_node","f":{i}.0}}"#));
        assert!(!r.contains("\"e\""), "scratch add_node must succeed: {r}");
    }
    for i in 0..99u32 {
        let _ = reg.call_json(&format!(
            r#"{{"session":{sid},"op":"add_edge","a":{i},"b":{}}}"#,
            i + 1
        ));
    }

    // DURING the (still-open) B-seq session, a concurrent store reader sees only
    // the original 4 nodes — the 100 scratch nodes are invisible to the store.
    let during = count_nodes().await?;
    assert_eq!(
        during, 4,
        "the store must see no trace of the scratch graph during a B-seq run"
    );

    // Close the B-seq session: the scratch graph really did hold 100 nodes.
    let scratch = reg.close(sid).expect("scratch session closes");
    assert_eq!(scratch.node_count(), 100);
    assert_eq!(scratch.edge_count(), 99);

    // AFTER the run, the store is still unchanged — no residue leaked.
    let after = count_nodes().await?;
    assert_eq!(
        after, 4,
        "the store must see no trace of the scratch graph after a B-seq run"
    );
    Ok(())
}

#[tokio::test]
async fn q3_projected_reads_are_pinned_across_concurrent_commits() -> anyhow::Result<()> {
    // Q-3 (version-stamp pinning): a read-only graph projected at time T0 is
    // pinned to that snapshot — a concurrent commit (T1) adding nodes/edges does
    // not change what the already-materialized projection sees (proposal §7b
    // "reads inside the run are pinned to the projection-time version stamp").
    use uni_plugin::traits::algorithm::GraphProjectionSpec;
    use uni_plugin_builtin::algorithms::bridge::host_bridge_from_storage;

    let db = Uni::in_memory().build().await?;
    let _ = build_graph(&db).await?; // 4 nodes, 4 edges at T0
    db.flush().await?; // land the seed in storage so the None-L0 projection sees it

    // Project a read-only graph snapshot at T0 through the host bridge.
    let storage = db.storage();
    let caps = CapabilitySet::from_iter_of([
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    let bridge = host_bridge_from_storage(storage, None, caps);
    let spec = GraphProjectionSpec {
        node_labels: vec!["Node".into()],
        edge_types: vec!["LINKS".into()],
        include_reverse: false,
        weight_property: None,
    };
    let proj0 = bridge.project_for_graph_compute(&spec).await?;
    let v0 = proj0.vertex_count();
    let e0 = proj0.edge_count();
    assert_eq!(v0, 4, "T0 snapshot has the 4 seeded nodes");

    // Concurrently commit new nodes + edges (T1).
    let session = db.session();
    let tx = session.tx().await?;
    for name in ["E", "F"] {
        tx.execute(&format!("CREATE (:Node {{name: '{name}'}})"))
            .await?;
    }
    tx.execute("MATCH (a:Node {name: 'E'}), (b:Node {name: 'F'}) CREATE (a)-[:LINKS]->(b)")
        .await?;
    tx.commit().await?;

    // The pinned T0 projection is unchanged — it never observes the T1 commit.
    assert_eq!(
        proj0.vertex_count(),
        v0,
        "projection stays pinned to T0 vertices"
    );
    assert_eq!(
        proj0.edge_count(),
        e0,
        "projection stays pinned to T0 edges"
    );
    Ok(())
}
