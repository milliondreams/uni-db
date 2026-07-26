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

/// A guest that reads two pre-declared named scopes over the same vertices.
///
/// Counts each vertex's out-degree in the primary layer and in the `agg` layer,
/// and emits both. The point is that `deg_agg` is computed from a *different*
/// projection and combined with the primary's — which is only legal because both
/// scopes cover the same node set, and is only *safe* because slot correspondence
/// holds for Native projections.
const SCOPES_SCRIPT: &str = r#"
    fn uni_manifest() {
        #{
            id: "ai.example.gcscopes",
            version: "0.1.0",
            determinism: "pure",
            algorithms: [
                #{ name: "layers", args: [], yields: ["nodeId:int", "both:float"] },
            ],
        }
    }

    fn layers(gc) {
        let g   = gc.graph();
        let agg = gc.graph_named("agg");
        let a = gc.degrees(g, "out");
        let b = gc.degrees(agg, "out");
        // `b` is keyed to `agg`. Combining it with `a` requires an explicit,
        // verified re-key: `rekey` walks both slot->Vid maps and fails if the
        // projections do not describe the same vertices.
        let b_here = gc.rekey(b, g);
        let sum = gc.ewise(a, b_here, "add", 0.0);
        gc.free(a);
        gc.free(b);
        gc.free(b_here);
        gc.emit("both", sum);
    }
"#;

/// The same guest, but mixing a scope's value into a kernel bound to the primary.
const SCOPES_CROSS_SCRIPT: &str = r#"
    fn uni_manifest() {
        #{
            id: "ai.example.gccross",
            version: "0.1.0",
            determinism: "pure",
            algorithms: [
                #{ name: "cross", args: [], yields: ["nodeId:int", "v:float"] },
            ],
        }
    }

    fn cross(gc) {
        let g   = gc.graph();
        let agg = gc.graph_named("agg");
        let from_agg = gc.degrees(agg, "out");
        // Illegal: `from_agg` is keyed to `agg`'s vertices, not `g`'s.
        let spread = gc.spmv(g, from_agg, "linear_algebra", "out");
        gc.emit("v", spread);
    }
"#;

async fn build_two_layer_graph(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Cell")
        .property("name", DataType::String)
        .done()
        .edge_type("ADJACENT", &["Cell"], &["Cell"])
        .done()
        .edge_type("AGGREGATES", &["Cell"], &["Cell"])
        .done()
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for name in ["A", "B", "C", "D"] {
        tx.execute(&format!("CREATE (:Cell {{name: '{name}'}})"))
            .await?;
    }
    // Two edge layers over the SAME four vertices, with different topology, so a
    // result that ignored one layer would be indistinguishable from a bug.
    for (a, b) in [("A", "B"), ("B", "C")] {
        tx.execute(&format!(
            "MATCH (a:Cell {{name: '{a}'}}), (b:Cell {{name: '{b}'}}) CREATE (a)-[:ADJACENT]->(b)"
        ))
        .await?;
    }
    for (a, b) in [("A", "C"), ("A", "D"), ("C", "D")] {
        tx.execute(&format!(
            "MATCH (a:Cell {{name: '{a}'}}), (b:Cell {{name: '{b}'}}) CREATE (a)-[:AGGREGATES]->(b)"
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

/// A guest reads a second projection by name and combines it with the primary.
///
/// This is the fixture the multi-projection work needed and nothing had: two
/// projections with identical vertex counts **and** identical vid sets, so slot
/// `i` names the same vertex in both. Every earlier two-graph test relied on
/// differing sizes, which is exactly the case a length check already catches.
#[tokio::test]
async fn rhai_guest_reads_a_named_scope() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    build_two_layer_graph(&db).await?;

    let loader = uni_plugin_rhai::RhaiLoader::new();
    db.load_rhai_plugin(&loader, SCOPES_SCRIPT, &gc_caps())
        .expect("load_rhai_plugin succeeds");

    let session = db.session();
    let res = session
        .query(
            "CALL ai.example.gcscopes.layers({\
               nodeLabels: ['Cell'], edgeTypes: ['ADJACENT'], \
               scopes: {agg: {nodeLabels: ['Cell'], edgeTypes: ['AGGREGATES']}}\
             }) YIELD nodeId, both RETURN nodeId, both",
        )
        .await?;
    let rows = res.rows();
    assert_eq!(
        rows.len(),
        4,
        "one row per vertex of the primary projection"
    );

    // ADJACENT out-degrees: A=1, B=1, C=0, D=0.  AGGREGATES: A=2, B=0, C=1, D=0.
    // Sum per vertex: A=3, B=1, C=1, D=0 -> total 5 = 2 + 3 edges.
    let total: f64 = rows
        .iter()
        .map(|r| r.get::<f64>("both").unwrap_or(f64::NAN))
        .sum();
    assert!(
        (total - 5.0).abs() < 1e-9,
        "both layers must contribute (2 ADJACENT + 3 AGGREGATES edges), got {total}"
    );

    // The per-vertex split proves the scope is a genuinely different topology
    // rather than the primary projected twice.
    let mut by_id: Vec<f64> = rows
        .iter()
        .map(|r| r.get::<f64>("both").unwrap_or(f64::NAN))
        .collect();
    by_id.sort_by(f64::total_cmp);
    assert_eq!(
        by_id
            .iter()
            .map(|v| format!("{v:.0}"))
            .collect::<Vec<_>>()
            .join(","),
        "0,1,1,3",
        "degree sums must be the two layers combined, not one layer doubled"
    );
    Ok(())
}

/// Mixing a scope's value into a kernel bound to another graph is rejected.
///
/// Both projections have four vertices, so no length or capacity check can
/// separate them. Only the index space can — and without it the guest would get
/// a plausible number computed against the wrong adjacency.
#[tokio::test]
async fn rhai_guest_cannot_mix_values_across_scopes() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    build_two_layer_graph(&db).await?;

    let loader = uni_plugin_rhai::RhaiLoader::new();
    db.load_rhai_plugin(&loader, SCOPES_CROSS_SCRIPT, &gc_caps())
        .expect("load_rhai_plugin succeeds");

    let err = db
        .session()
        .query(
            "CALL ai.example.gccross.cross({\
               nodeLabels: ['Cell'], edgeTypes: ['ADJACENT'], \
               scopes: {agg: {nodeLabels: ['Cell'], edgeTypes: ['AGGREGATES']}}\
             }) YIELD nodeId, v RETURN nodeId, v",
        )
        .await
        .expect_err("a cross-projection spmv must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("different index spaces") || msg.contains("spmv"),
        "the error must name the index-space fault, got: {msg}"
    );
    Ok(())
}

/// An unknown scope name is an error that lists what was declared.
#[tokio::test]
async fn rhai_guest_gets_a_helpful_error_for_an_unknown_scope() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    build_two_layer_graph(&db).await?;

    let script = SCOPES_SCRIPT.replace("gc.graph_named(\"agg\")", "gc.graph_named(\"typo\")");
    let loader = uni_plugin_rhai::RhaiLoader::new();
    db.load_rhai_plugin(&loader, &script, &gc_caps())
        .expect("load_rhai_plugin succeeds");

    let err = db
        .session()
        .query(
            "CALL ai.example.gcscopes.layers({\
               nodeLabels: ['Cell'], edgeTypes: ['ADJACENT'], \
               scopes: {agg: {nodeLabels: ['Cell'], edgeTypes: ['AGGREGATES']}}\
             }) YIELD nodeId, both RETURN nodeId, both",
        )
        .await
        .expect_err("an undeclared scope name must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("typo") && msg.contains("agg"),
        "the error must name the miss and list the declared scopes, got: {msg}"
    );
    Ok(())
}

/// A **Cypher** named scope alongside a Native primary.
///
/// This is the path that forced `graph_ref` off the bridge and into a per-call
/// parameter: each scope picks its own mode, so a Native primary with a Cypher
/// scope needs the resolver installed *without* a primary ref. Before that, the
/// scope fell through to a Native storage scan of an empty spec.
///
/// The scope's edge query selects the AGGREGATES layer, so a result matching the
/// ADJACENT layer would prove the Cypher scope had been ignored.
#[tokio::test]
async fn rhai_guest_reads_a_cypher_named_scope() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    build_two_layer_graph(&db).await?;

    let loader = uni_plugin_rhai::RhaiLoader::new();
    db.load_rhai_plugin(&loader, SCOPES_SCRIPT, &gc_caps())
        .expect("load_rhai_plugin succeeds");

    let res = db
        .session()
        .query(
            "CALL ai.example.gcscopes.layers({\
               nodeLabels: ['Cell'], edgeTypes: ['ADJACENT'], \
               scopes: {agg: {\
                 nodeQuery: 'MATCH (n:Cell) RETURN id(n) AS id', \
                 edgeQuery: 'MATCH (a:Cell)-[:AGGREGATES]->(b:Cell) \
                             RETURN id(a) AS source, id(b) AS target'}}\
             }) YIELD nodeId, both RETURN nodeId, both",
        )
        .await?;
    let rows = res.rows();
    assert_eq!(
        rows.len(),
        4,
        "one row per vertex of the primary projection"
    );

    let total: f64 = rows
        .iter()
        .map(|r| r.get::<f64>("both").unwrap_or(f64::NAN))
        .sum();
    assert!(
        (total - 5.0).abs() < 1e-9,
        "the Cypher scope must contribute its 3 AGGREGATES edges on top of the \
         primary's 2 ADJACENT edges, got {total}"
    );
    Ok(())
}

/// A Cypher scope whose rows do not correspond is caught by `rekey`, not trusted.
///
/// `GraphProjection::from_rows` interns in row order and deliberately does not
/// sort, so a Cypher scope has no slot correspondence with a Native projection in
/// general. The guarantee is not "Cypher scopes are unusable" — it is that the
/// claim gets *checked* where a guest relies on it. Here the scope covers only
/// three of the four vertices, so `rekey` refuses rather than producing a
/// right-looking wrong answer.
#[tokio::test]
async fn a_non_corresponding_cypher_scope_is_refused_by_rekey() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    build_two_layer_graph(&db).await?;

    let loader = uni_plugin_rhai::RhaiLoader::new();
    db.load_rhai_plugin(&loader, SCOPES_SCRIPT, &gc_caps())
        .expect("load_rhai_plugin succeeds");

    let err = db
        .session()
        .query(
            "CALL ai.example.gcscopes.layers({\
               nodeLabels: ['Cell'], edgeTypes: ['ADJACENT'], \
               scopes: {agg: {\
                 nodeQuery: 'MATCH (n:Cell) WHERE n.name <> \"D\" RETURN id(n) AS id', \
                 edgeQuery: 'MATCH (a:Cell)-[:AGGREGATES]->(b:Cell) \
                             RETURN id(a) AS source, id(b) AS target'}}\
             }) YIELD nodeId, both RETURN nodeId, both",
        )
        .await
        .expect_err("a scope over a different vertex set must not be re-keyed");
    let msg = err.to_string();
    assert!(
        msg.contains("rekey"),
        "the failure must come from the correspondence check, got: {msg}"
    );
    Ok(())
}
