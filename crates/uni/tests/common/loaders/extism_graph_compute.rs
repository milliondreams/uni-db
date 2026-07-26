//! End-to-end: an Extism guest algorithm driving GraphCompute kernels.
//!
//! Proves the guest-authorable thesis for the Extism loader (proposal Phase 4 /
//! §9.3): a Personalized PageRank compiled to `wasm32-unknown-unknown`, declared
//! via an `algorithm` registration entry, loaded, and invoked through Cypher
//! `CALL`. The guest drives every kernel through the single `uni_graph_call`
//! host fn (one JSON round-trip per op) — only handles + scalars cross. The
//! result matches the native provider.
//!
//! The fixture is built by `scripts/build-wasm-fixtures.sh`; this test panics
//! with a build hint if the artifact is missing (no silent skip).

#![cfg(feature = "extism-plugins")]

use std::sync::Arc;

use uni_db::{DataType, Uni};
use uni_plugin::{Capability, CapabilitySet};
use uni_plugin_builtin::algorithms::graph_compute::GraphComputeRegistry;

const WASM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-extism-graph/target/wasm32-unknown-unknown/release/example_extism_graph.wasm"
);

fn load_wasm_bytes() -> Vec<u8> {
    std::fs::read(WASM_PATH).unwrap_or_else(|e| {
        panic!(
            "extism graph fixture missing at {WASM_PATH}: {e}\nRun ./scripts/build-wasm-fixtures.sh"
        )
    })
}

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
    let res = session
        .query("MATCH (a:Node {name: 'A'}) RETURN id(a) AS vid")
        .await?;
    Ok(res.rows()[0].get::<i64>("vid")?)
}

#[tokio::test]
async fn extism_guest_ppr_via_call() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let bytes = load_wasm_bytes();

    // One shared registry backs both the `uni_graph_call` host fn and the
    // algorithm adapter (proposal §4.5 session-registry lifecycle).
    let registry = Arc::new(GraphComputeRegistry::new());
    let mut loader = uni_plugin_extism::ExtismLoader::new();
    uni_plugin_extism::host_svc::register_default_host_svc(&mut loader);
    let loader = loader.with_graph(Arc::clone(&registry));

    // registrar_caps gate registration (need Algorithm + the two graph gates);
    // host_grants gate host fns (need GraphCompute for uni_graph_call).
    let registrar_caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    let host_grants = CapabilitySet::from_iter_of([
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    let outcome = db.load_wasm_extism(&loader, &bytes, &host_grants, &registrar_caps)?;
    assert_eq!(outcome.plugin_id.as_str(), "ai.example.extismgc");

    let session = db.session();
    // A guest projection must be scoped explicitly (G9): an unscoped guest CALL
    // now fails loud instead of silently projecting the whole graph.
    let query = format!(
        "CALL ai.example.extismgc.ppr({vid_a}, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
         YIELD nodeId, score RETURN nodeId, score"
    );
    let res = session.query(&query).await?;
    let rows = res.rows();
    assert_eq!(rows.len(), 4, "one score row per vertex");

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
        "Extism-authored PPR mass must sum to 1, got {total}"
    );

    // Parity vs the native gcpagerank provider.
    let native = session
        .query(&format!(
            "CALL uni.algo.gcpagerank({vid_a}, 0.85) YIELD nodeId, score RETURN nodeId, score"
        ))
        .await?;
    let mut want = std::collections::HashMap::new();
    for row in native.rows() {
        want.insert(row.get::<i64>("nodeId")?, row.get::<f64>("score")?);
    }
    for row in res.rows() {
        let id = row.get::<i64>("nodeId")?;
        assert!(
            (row.get::<f64>("score")? - want[&id]).abs() < 1e-9,
            "extism PPR parity mismatch for node {id}"
        );
    }
    Ok(())
}

/// A sandboxed guest can return every value column it declared, delivered
/// through the batch wire form (`names` + `handles`).
///
/// A two-column declaration used to be unsatisfiable from every sandboxed
/// loader: the completeness check ran inside `emit`, so the first call always
/// failed. Both halves of that are covered here — the component declares two
/// yields, and emits them in one call.
#[tokio::test]
async fn extism_guest_emits_two_declared_columns() -> anyhow::Result<()> {
    let bytes = load_wasm_bytes();
    let db = Uni::in_memory().build().await?;
    let _ = build_graph(&db).await?;
    let registry = Arc::new(GraphComputeRegistry::new());
    let mut loader = uni_plugin_extism::ExtismLoader::new();
    uni_plugin_extism::host_svc::register_default_host_svc(&mut loader);
    let loader = loader.with_graph(Arc::clone(&registry));
    let registrar_caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    let host_grants = CapabilitySet::from_iter_of([
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    db.load_wasm_extism(&loader, &bytes, &host_grants, &registrar_caps)?;

    let res = db
        .session()
        .query(
            "CALL ai.example.extismgc.twocol({nodeLabels: ['Node'], edgeTypes: ['LINKS']}) \
             YIELD nodeId, a, b RETURN nodeId, a, b",
        )
        .await?;
    assert_eq!(res.rows().len(), 4, "one row per projected vertex");
    for row in res.rows() {
        let a: f64 = row.get("a")?;
        let b: f64 = row.get("b")?;
        assert!(a.is_finite() && b.is_finite(), "both columns carry values");
    }
    Ok(())
}

/// The two-layer fixture: one node set, two edge types, so a result that ignored
/// the scope would be indistinguishable from a bug.
async fn build_two_layer_graph(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Node")
        .property("name", DataType::String)
        .done()
        .edge_type("LINKS", &["Node"], &["Node"])
        .done()
        .edge_type("SECOND", &["Node"], &["Node"])
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
    for (a, b) in [("A", "C"), ("C", "D")] {
        tx.execute(&format!(
            "MATCH (a:Node {{name: '{a}'}}), (b:Node {{name: '{b}'}}) CREATE (a)-[:SECOND]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// A sandboxed guest reads a pre-declared named scope from its `graphs` map.
///
/// WASM and Extism guests do not call `graph_named` -- the handles arrive beside
/// `graph` in the invocation JSON. Nothing observed that key until this test: the
/// host emitted it and no guest read it, so the sandboxed half of named scopes
/// was write-only. The guest also crosses the two index spaces with an explicit
/// `rekey`, which is the only legal way to do so.
#[tokio::test]
async fn extism_guest_reads_a_named_scope_from_its_graphs_map() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    build_two_layer_graph(&db).await?;
    let bytes = load_wasm_bytes();
    let registry = Arc::new(GraphComputeRegistry::new());
    let mut loader = uni_plugin_extism::ExtismLoader::new();
    uni_plugin_extism::host_svc::register_default_host_svc(&mut loader);
    let loader = loader.with_graph(Arc::clone(&registry));
    let registrar_caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    let host_grants = CapabilitySet::from_iter_of([
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    db.load_wasm_extism(&loader, &bytes, &host_grants, &registrar_caps)?;

    let res = db
        .session()
        .query(
            "CALL ai.example.extismgc.layers({\
               nodeLabels: ['Node'], edgeTypes: ['LINKS'], \
               scopes: {agg: {nodeLabels: ['Node'], edgeTypes: ['SECOND']}}\
             }) YIELD nodeId, both RETURN nodeId, both",
        )
        .await?;
    assert_eq!(res.rows().len(), 4, "one row per projected vertex");
    let total: f64 = res
        .rows()
        .iter()
        .map(|r| r.get::<f64>("both").unwrap_or(f64::NAN))
        .sum();
    assert!(
        (total - 6.0).abs() < 1e-9,
        "the scope must contribute its 2 SECOND edges on top of the primary's 4 \
         LINKS edges, got {total}"
    );
    Ok(())
}
