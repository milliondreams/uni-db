//! End-to-end tests for the GraphView plugin surface (P0).
//!
//! Exercises `AlgorithmProvider::run` wired into CALL dispatch, the
//! `AlgorithmHost::project` → `GraphView` topology API, and the
//! `HostQuery` capability gate — through real `CALL` queries:
//!
//! - the first-party `uni.algo.reachability` provider (planner / Path B),
//!   which also proves L0 (unflushed) visibility;
//! - a third-party plugin registering the same provider under its own
//!   namespace (simple-executor / Path A);
//! - the negative capability gate (a provider without `HostQuery` is
//!   denied host graph access).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use uni_db::{DataType, Uni};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, Plugin, PluginError, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, QName, Scope, SideEffects,
};

/// Build a small directed graph `A→B→C`, `A→D` in L0 (committed, not
/// flushed) and return the vertex id of `A`.
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
    tx.execute("CREATE (:Node {name: 'A'})").await?;
    tx.execute("CREATE (:Node {name: 'B'})").await?;
    tx.execute("CREATE (:Node {name: 'C'})").await?;
    tx.execute("CREATE (:Node {name: 'D'})").await?;
    tx.execute("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:LINKS]->(b)")
        .await?;
    tx.execute("MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'}) CREATE (b)-[:LINKS]->(c)")
        .await?;
    tx.execute("MATCH (a:Node {name: 'A'}), (d:Node {name: 'D'}) CREATE (a)-[:LINKS]->(d)")
        .await?;
    tx.commit().await?;
    // Deliberately no flush: the algorithm must observe L0 data.

    let res = session
        .query("MATCH (a:Node {name: 'A'}) RETURN id(a) AS vid")
        .await?;
    Ok(res.rows()[0].get::<i64>("vid")?)
}

/// Reachable-set shape assertion: `A@0`, `B@1`, `D@1`, `C@2`.
fn assert_reachable_from_a(rows: &[uni_db::Row], vid_a: i64) -> anyhow::Result<()> {
    assert_eq!(rows.len(), 4, "A reaches exactly {{A, B, C, D}}");
    let mut by_dist: HashMap<i64, i64> = HashMap::new();
    for row in rows {
        *by_dist.entry(row.get::<i64>("distance")?).or_default() += 1;
    }
    assert_eq!(by_dist.get(&0), Some(&1), "source at distance 0");
    assert_eq!(by_dist.get(&1), Some(&2), "B and D at distance 1");
    assert_eq!(by_dist.get(&2), Some(&1), "C at distance 2");
    assert!(
        rows.iter()
            .any(|r| r.get::<i64>("distance").unwrap_or(-1) == 0
                && r.get::<i64>("nodeId").unwrap_or(-1) == vid_a),
        "the distance-0 row is the source vertex"
    );
    Ok(())
}

#[tokio::test]
async fn first_party_reachability_bfs_over_l0() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let session = db.session();

    // Routes through the DF planner path (`uni.algo.*`), builds a
    // GraphView via `project`, and BFS-walks it — all against unflushed
    // L0 data.
    let query = format!(
        "CALL uni.algo.reachability({vid_a}, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
         YIELD nodeId, distance RETURN nodeId, distance"
    );
    let res = session.query(&query).await?;
    assert_reachable_from_a(res.rows(), vid_a)?;
    Ok(())
}

/// A third-party plugin registering the reachability provider under its
/// own namespace. `caps` controls whether it declares `HostQuery`.
struct ExampleAlgoPlugin {
    manifest: OnceLock<PluginManifest>,
    caps: CapabilitySet,
}

impl ExampleAlgoPlugin {
    fn new(caps: CapabilitySet) -> Self {
        Self {
            manifest: OnceLock::new(),
            caps,
        }
    }
}

impl Plugin for ExampleAlgoPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: uni_plugin::PluginId::new("example"),
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
            docs: "third-party graph-algorithm plugin (test)".to_owned(),
            metadata: BTreeMap::new(),
        })
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.algorithm(
            QName::new("example", "reach"),
            Arc::new(uni_plugin_builtin::algorithms::ReachabilityProvider::new()),
        )?;
        Ok(())
    }
}

#[tokio::test]
async fn third_party_provider_with_hostquery_runs() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;

    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    db.add_plugin(ExampleAlgoPlugin::new(caps))?;

    // `example.reach` is not `uni.algo.*` → simple-executor dispatch
    // (Path A). The provider reaches topology only through the public
    // `GraphView`.
    let session = db.session();
    let query = format!(
        "CALL example.reach({vid_a}, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
         YIELD nodeId, distance RETURN nodeId, distance"
    );
    let res = session.query(&query).await?;
    assert_reachable_from_a(res.rows(), vid_a)?;
    Ok(())
}

#[tokio::test]
async fn third_party_provider_without_hostquery_is_denied() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;

    // Declares `Algorithm` (needed to register) but NOT `HostQuery`, so
    // `AlgorithmHost::project` must refuse to hand back a GraphView.
    let caps = CapabilitySet::from_iter_of([Capability::Algorithm]);
    db.add_plugin(ExampleAlgoPlugin::new(caps))?;

    let session = db.session();
    let query = format!("CALL example.reach({vid_a}) YIELD nodeId, distance RETURN nodeId");
    let err = session
        .query(&query)
        .await
        .expect_err("a provider lacking HostQuery must be denied graph access");
    let msg = err.to_string();
    assert!(
        msg.contains("HostQuery"),
        "error should name the missing `HostQuery` capability, got: {msg}"
    );
    Ok(())
}
