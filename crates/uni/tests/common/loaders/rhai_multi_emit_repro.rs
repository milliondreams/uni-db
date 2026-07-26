//! REPRO (Phase 0): a manifest declaring two value columns is unsatisfiable.
//!
//! The host trait is `emit(cols: &[(&str, Handle)])` — all columns in ONE call
//! (proposal §4.6). Every guest shim, however, exposes `emit(name, handle)` —
//! one column per call. The validator in `AlgoSession::emit` then demands that
//! every declared column be present *in that single call*, and it runs before
//! any capture. So for a manifest with two or more non-`nodeId` yields there is
//! no guest program that satisfies it.
//!
//! Two consequences the tests below pin:
//!
//! 1. The impossible manifest is **accepted at plugin load**. The constraint is
//!    undiscoverable until a CALL fails at runtime.
//! 2. The reported diagnosis ("the first emit lands, later ones vanish") is
//!    wrong in a way that matters: the **first** emit is what fails. That is why
//!    the error names whichever column the guest did not mention first — it
//!    looks like order-dependence, but nothing was ever captured.
//!
//! Flips when Phase 3 accumulates across calls and moves the completeness check
//! to session close. See
//! `docs/proposals/graphcompute_dynamics_requirements_response_2026-07-25.md` §12.2.

#![cfg(feature = "rhai-plugins")]

use uni_db::{DataType, Uni};
use uni_plugin::{Capability, CapabilitySet};

/// A guest declaring two value columns and emitting them one per call — the
/// only way any loader's `emit` shim allows.
const TWO_COLUMN_SCRIPT: &str = r#"
    fn uni_manifest() {
        #{
            id: "ai.example.multiemit",
            version: "0.1.0",
            determinism: "pure",
            algorithms: [
                #{ name: "twocol", args: [], yields: ["nodeId:int", "a:float", "b:float"] },
            ],
        }
    }

    fn twocol(gc) {
        let g = gc.graph();
        let deg = gc.degrees(g, "out");
        let ids = gc.vertex_ids(g);
        gc.emit("a", deg);
        gc.emit("b", ids);
    }
"#;

async fn build_graph(db: &Uni) -> anyhow::Result<()> {
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

fn gc_caps() -> CapabilitySet {
    CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ])
}

/// A two-value-column manifest loads cleanly, so nothing warns the author that
/// the algorithm they just registered can never return a row.
///
/// This is the half of REQ-D2 that would remain even under the consumer's own
/// fallback proposal ("reject a multi-field `yields` at load"): today neither
/// the accept path nor a reject path exists, so the failure lands at CALL time,
/// far from the mistake.
#[tokio::test]
async fn multi_field_yields_is_accepted_at_plugin_load() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let outcome = db
        .load_rhai_plugin(&loader, TWO_COLUMN_SCRIPT, &gc_caps())
        .expect("a two-value-column manifest is accepted at load time");
    assert_eq!(outcome.plugin_id.as_str(), "ai.example.multiemit");
    Ok(())
}

/// Runs the two-column guest and returns the CALL's outcome.
async fn call_twocol(db: &Uni) -> Result<usize, String> {
    let session = db.session();
    session
        .query(
            "CALL ai.example.multiemit.twocol({nodeLabels: ['Node'], edgeTypes: ['LINKS']}) \
             YIELD nodeId, a, b RETURN nodeId, a, b",
        )
        .await
        .map(|r| r.rows().len())
        .map_err(|e| e.to_string())
}

/// REPRO-D2: a guest must be able to return the two value columns it declared.
///
/// Asserts the desired contract, so it is red until Phase 3.
#[tokio::test]
#[ignore = "REPRO (red on 254b4c26c, 0x869): flips when Phase 3 accumulates emit \
            across calls. Run with --run-ignored all."]
async fn repro_d2_a_guest_can_emit_two_declared_value_columns() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    build_graph(&db).await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    db.load_rhai_plugin(&loader, TWO_COLUMN_SCRIPT, &gc_caps())
        .expect("load_rhai_plugin succeeds");

    let rows = call_twocol(&db).await;
    assert_eq!(
        rows,
        Ok(4),
        "a guest must be able to emit two declared value columns, one per row"
    );
    Ok(())
}

/// Phase-0 diagnosis: the failure lands at the **first** `emit`, not the second.
///
/// The consumer reported "only the first `gc.emit` takes effect; every
/// subsequent emit is silently discarded". The order-dependence they observed is
/// real but the mechanism is the opposite: `AlgoSession::emit` validates the
/// whole declared set *before* capturing anything, so `gc.emit("a", …)` itself
/// raises and nothing is ever stored.
///
/// The message text is the discriminator. Intra-call validation says "declared
/// output field `b` was not emitted"; the loader's batch assembly, which is the
/// only other place this could fail, says "guest did not emit declared column".
///
/// This test pins *current* behaviour deliberately, as Phase 0 evidence.
/// **Phase 3 deletes it** — by then the first emit legitimately succeeds and the
/// completeness check has moved to session close.
#[tokio::test]
async fn d2_diagnosis_the_first_emit_is_what_fails() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    build_graph(&db).await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    db.load_rhai_plugin(&loader, TWO_COLUMN_SCRIPT, &gc_caps())
        .expect("load_rhai_plugin succeeds");

    let err = call_twocol(&db)
        .await
        .expect_err("two-column emit cannot succeed today");

    assert!(
        err.contains("was not emitted"),
        "expected the intra-call validator's wording, got: {err}"
    );
    assert!(
        err.contains('b'),
        "the error must name the sibling column the first emit omitted: {err}"
    );
    assert!(
        !err.contains("guest did not emit declared column"),
        "batch assembly must not be where this fails — that would mean the first \
         emit succeeded: {err}"
    );
    Ok(())
}
