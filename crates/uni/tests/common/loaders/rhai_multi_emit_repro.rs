//! A guest can return every value column it declared (REQ-D2).
//!
//! The host trait is `emit(cols: &[(&str, Handle)])` — all columns in one call —
//! but every guest shim exposes `emit(name, handle)`, one column per call. The
//! validator in `AlgoSession::emit` used to demand the full declared set *in that
//! single call*, and ran before capturing anything, so a manifest with two or
//! more non-`nodeId` yields was unsatisfiable from every sandboxed loader: the
//! first emit always failed.
//!
//! The reported symptom ("only the first emit registers; later ones vanish") was
//! the inverse of the mechanism — nothing was ever captured, and the error named
//! whichever sibling the first call happened to omit, which read as
//! order-dependence.
//!
//! Phase 3 moved the completeness check to session close (`finish_emitted`) and
//! made `emit` accumulate. What remains pinned here: the two-column manifest
//! loads, and the CALL returns both columns.

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

/// A two-value-column manifest loads cleanly — and now also runs.
///
/// The consumer's fallback proposal was "reject a multi-field `yields` at load".
/// That is no longer needed: the declaration is legal, so accepting it is right.
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

/// REQ-D2: a guest must be able to return the two value columns it declared.
#[tokio::test]
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

/// The batch form: `gc.emit(#{...})` delivers every declared column in one call.
const BATCH_SCRIPT: &str = r#"
    fn uni_manifest() {
        #{
            id: "ai.example.batchemit",
            version: "0.1.0",
            determinism: "pure",
            algorithms: [
                #{ name: "twocol", args: [], yields: ["nodeId:int", "a:float", "b:float"] },
            ],
        }
    }

    fn twocol(gc) {
        let g = gc.graph();
        gc.emit(#{ "a": gc.degrees(g, "out"), "b": gc.vertex_ids(g) });
    }
"#;

/// A guest can deliver both declared columns in a single `emit` — the shape the
/// host trait always modelled, and one boundary crossing instead of two.
#[tokio::test]
async fn batch_emit_delivers_every_declared_column_in_one_call() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    build_graph(&db).await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    db.load_rhai_plugin(&loader, BATCH_SCRIPT, &gc_caps())
        .expect("load_rhai_plugin succeeds");

    let res = db
        .session()
        .query(
            "CALL ai.example.batchemit.twocol({nodeLabels: ['Node'], edgeTypes: ['LINKS']}) \
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

/// Re-emitting a column is rejected rather than silently dropped.
///
/// Batch assembly resolves each declared field by the first matching entry, so
/// an accumulating `emit` that appended a duplicate would quietly discard the
/// second value — the silent-wrong-answer shape this contract exists to avoid.
#[tokio::test]
async fn re_emitting_a_column_is_rejected() -> anyhow::Result<()> {
    const DUP_SCRIPT: &str = r#"
        fn uni_manifest() {
            #{
                id: "ai.example.dupemit",
                version: "0.1.0",
                determinism: "pure",
                algorithms: [
                    #{ name: "dup", args: [], yields: ["nodeId:int", "a:float"] },
                ],
            }
        }

        fn dup(gc) {
            let g = gc.graph();
            gc.emit("a", gc.degrees(g, "out"));
            gc.emit("a", gc.vertex_ids(g));
        }
    "#;

    let db = Uni::in_memory().build().await?;
    build_graph(&db).await?;
    let loader = uni_plugin_rhai::RhaiLoader::new();
    db.load_rhai_plugin(&loader, DUP_SCRIPT, &gc_caps())
        .expect("load_rhai_plugin succeeds");

    let err = db
        .session()
        .query(
            "CALL ai.example.dupemit.dup({nodeLabels: ['Node'], edgeTypes: ['LINKS']}) \
             YIELD nodeId, a RETURN nodeId, a",
        )
        .await
        .expect_err("re-emitting a column must fail")
        .to_string();
    assert!(
        err.contains("already emitted"),
        "the error must say the column was already emitted: {err}"
    );
    Ok(())
}
