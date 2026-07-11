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
        panic!("extism graph fixture missing at {WASM_PATH}: {e}\nRun ./scripts/build-wasm-fixtures.sh")
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
    let query = format!(
        "CALL ai.example.extismgc.ppr({vid_a}) YIELD nodeId, score RETURN nodeId, score"
    );
    let res = session.query(&query).await?;
    let rows = res.rows();
    assert_eq!(rows.len(), 4, "one score row per vertex");

    let mut total = 0.0;
    for row in rows {
        let s = row.get::<f64>("score")?;
        assert!(s.is_finite() && s >= 0.0, "score must be a valid probability");
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
