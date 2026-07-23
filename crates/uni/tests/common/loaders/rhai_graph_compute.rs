//! End-to-end: a Rhai-authored graph algorithm driving GraphCompute kernels.
//!
//! Proves the flagship "guest-authorable graph algorithm" thesis (proposal §1,
//! Phase 2 / §9.3): a Personalized PageRank written in ~20 lines of Rhai,
//! declared via an `algorithms:` manifest entry, loaded as a plugin, and invoked
//! through Cypher `CALL`. The guest holds only opaque integer handles; every
//! O(V+E) operation is a native kernel. The result matches the native
//! `uni.algo.gcpagerank` provider (same kernels, same determinism).

#![cfg(feature = "rhai-plugins")]

use uni_db::{DataType, Uni};
use uni_plugin::{Capability, CapabilitySet};

/// The guest algorithm: Personalized PageRank in Rhai, driving the kernels.
const PPR_SCRIPT: &str = r#"
    fn uni_manifest() {
        #{
            id: "ai.example.gc",
            version: "0.1.0",
            determinism: "pure",
            algorithms: [
                #{ name: "ppr", args: ["int"], yields: ["nodeId:int", "score:float"] },
            ],
        }
    }

    fn ppr(gc, source) {
        let alpha = 0.85;
        let g = gc.graph();
        let seed_set = gc.frontier(g, [source]);
        let seed_map = gc.set_to_map(seed_set, 1.0);
        let teleport = gc.normalize(seed_map, "l1");
        gc.free(seed_map);
        gc.free(seed_set);

        let deg = gc.degrees(g, "out");
        let inv_deg = gc.recip(deg);
        let dangling = gc.map_to_set(deg, "is_zero", 0.0);
        gc.free(deg);

        let rank = gc.scale(teleport, 1.0);
        for i in 0..100 {
            let contrib = gc.ewise(rank, inv_deg, "mul", 0.0);
            let spread = gc.spmv(g, contrib, "linear_algebra", "out");
            gc.free(contrib);
            let dm = gc.reduce_sum_masked(rank, dangling);
            let scaled = gc.scale(spread, alpha);
            gc.free(spread);
            let blend = 1.0 - alpha + alpha * dm;
            let next = gc.ewise(scaled, teleport, "axpy", blend);
            gc.free(scaled);
            let diff = gc.l1_diff(rank, next);
            gc.free(rank);
            rank = next;
            if diff < 0.000000001 { break; }
        }

        gc.free(teleport);
        gc.free(inv_deg);
        gc.free(dangling);
        gc.emit("score", rank);
    }
"#;

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
async fn rhai_guest_ppr_via_call() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;

    // Grant the three orthogonal GraphCompute capabilities to the guest.
    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    let outcome = db
        .load_rhai_plugin(&loader, PPR_SCRIPT, &caps)
        .expect("load_rhai_plugin succeeds");
    assert_eq!(outcome.plugin_id.as_str(), "ai.example.gc");

    // Invoke the guest algorithm through Cypher CALL and check the result.
    let session = db.session();
    // A guest projection must be scoped explicitly (G9): an unscoped guest CALL
    // now fails loud instead of silently projecting the whole graph.
    let query = format!(
        "CALL ai.example.gc.ppr({vid_a}, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
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
        "guest-authored PPR mass must sum to 1, got {total}"
    );

    // Parity: the guest result must match the native gcpagerank provider (same
    // kernels, same determinism) row-for-row.
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
        let got = row.get::<f64>("score")?;
        let expected = want[&id];
        assert!(
            (got - expected).abs() < 1e-9,
            "guest PPR for node {id}: got {got}, native {expected}"
        );
    }
    Ok(())
}

/// A guest algorithm declaring a single `value` argument, driven by a
/// variable-length seed set. Proves the G4 capability: a Cypher list reaches the
/// guest as one argument — no per-arity plugin codegen, no `-1` sentinel padding.
const SET_ARG_SCRIPT: &str = r#"
    fn uni_manifest() {
        #{
            id: "ai.example.gcset",
            version: "0.1.0",
            determinism: "pure",
            algorithms: [
                #{ name: "reach", args: ["value"], yields: ["nodeId:int", "seen:float"] },
            ],
        }
    }

    fn reach(gc, seeds) {
        let g = gc.graph();
        let front = gc.frontier(g, seeds);   // `seeds` is the whole list, one arg
        let seen = gc.set_to_map(front, 1.0);
        gc.free(front);
        gc.emit("seen", seen);
    }
"#;

/// G4 — a `value`-typed algorithm argument accepts a variable-length set, and
/// declared args are now validated (they were silently-ignored dead metadata).
#[tokio::test]
async fn rhai_guest_value_arg_takes_variable_length_set() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;
    let vid_b = db
        .session()
        .query("MATCH (b:Node {name: 'B'}) RETURN id(b) AS vid")
        .await?
        .rows()[0]
        .get::<i64>("vid")?;

    let loader = uni_plugin_rhai::RhaiLoader::new();
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    db.load_rhai_plugin(&loader, SET_ARG_SCRIPT, &caps)
        .expect("load_rhai_plugin succeeds");

    let session = db.session();
    // Pass a variable-length seed set as ONE `value` argument (a Cypher list).
    let res = session
        .query(&format!(
            "CALL ai.example.gcset.reach([{vid_a}, {vid_b}], \
             {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
             YIELD nodeId, seen RETURN nodeId, seen"
        ))
        .await?;
    let seen_count = res
        .rows()
        .iter()
        .filter(|r| r.get::<f64>("seen").map(|v| v > 0.5).unwrap_or(false))
        .count();
    assert_eq!(
        seen_count, 2,
        "both seeds in the variable-length set are marked seen"
    );

    // Validation now activates (declared args were silently-ignored dead
    // metadata before): a CALL with too many positional args — three seed lists
    // plus the trailing config — exceeds the declared arity and is rejected
    // before the guest runs.
    let err = session
        .query(&format!(
            "CALL ai.example.gcset.reach([{vid_a}], [{vid_b}], [{vid_a}], \
             {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) \
             YIELD nodeId, seen RETURN nodeId"
        ))
        .await
        .expect_err("too many positional args must be rejected (args are validated now)");
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("too many") || msg.contains("argument") || msg.contains("arg"),
        "the arity mismatch must surface as an argument error, got: {err}"
    );
    Ok(())
}
