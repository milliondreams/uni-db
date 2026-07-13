//! End-to-end: a Python-authored graph algorithm driving GraphCompute kernels.
//!
//! Proves the guest-authorable thesis for the PyO3 loader (proposal Phase 5 /
//! §9.3): a Personalized PageRank written in Python, declared via `@db.algorithm`,
//! loaded, and invoked through Cypher `CALL`. The guest holds only opaque integer
//! handles; every O(V+E) op is a native kernel. Also closes the loader's two
//! prerequisite gaps — the host projection is injected as `gc`, and each kernel
//! call is deadline-checked. The result matches the native provider.

#![cfg(feature = "pyo3-plugins")]

use pyo3::Python;
use uni_db::{DataType, Uni};
use uni_plugin::{Capability, CapabilitySet};

/// The guest algorithm: Personalized PageRank in Python driving the kernels.
const PPR_MODULE: &str = r#"
db.set_plugin_id("ai.example.pygc")
db.set_version("0.1.0")

@db.algorithm("ppr", args=["int"], yields=["nodeId:int", "score:float"])
def ppr(gc, source):
    alpha = 0.85
    g = gc.graph()
    seed_set = gc.frontier(g, [source])
    seed_map = gc.set_to_map(seed_set, 1.0)
    teleport = gc.normalize(seed_map, "l1")
    gc.free(seed_map)
    gc.free(seed_set)

    deg = gc.degrees(g, "out")
    inv_deg = gc.recip(deg)
    dangling = gc.map_to_set(deg, "is_zero", 0.0)
    gc.free(deg)

    rank = gc.scale(teleport, 1.0)
    for _ in range(100):
        contrib = gc.ewise(rank, inv_deg, "mul")
        spread = gc.spmv(g, contrib, "linear_algebra", "out")
        gc.free(contrib)
        dm = gc.reduce_sum_masked(rank, dangling)
        scaled = gc.scale(spread, alpha)
        gc.free(spread)
        blend = 1.0 - alpha + alpha * dm
        nxt = gc.ewise(scaled, teleport, "axpy", blend)
        gc.free(scaled)
        diff = gc.l1_diff(rank, nxt)
        gc.free(rank)
        rank = nxt
        if diff < 1e-9:
            break

    gc.free(teleport)
    gc.free(inv_deg)
    gc.free(dangling)
    gc.emit("score", rank)
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
async fn python_guest_ppr_via_call() -> anyhow::Result<()> {
    Python::initialize();
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;

    let loader = uni_plugin_pyo3::PythonPluginLoader::with_default_plugin_id("ai.example.pygc");
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
    ]);
    let outcome = Python::attach(|py| {
        db.load_python_plugin(py, &loader, PPR_MODULE, "ai.example.pygc", &caps)
            .expect("load_python_plugin succeeds")
    });
    assert_eq!(outcome.plugin_id.as_str(), "ai.example.pygc");

    let session = db.session();
    let query =
        format!("CALL ai.example.pygc.ppr({vid_a}) YIELD nodeId, score RETURN nodeId, score");
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
        "Python-authored PPR mass must sum to 1, got {total}"
    );

    // Parity vs the native gcpagerank provider (same kernels, same determinism).
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
        assert!(
            (got - want[&id]).abs() < 1e-9,
            "python PPR for node {id}: got {got}, native {}",
            want[&id]
        );
    }
    Ok(())
}

/// A plugin with a runaway `spin` algorithm (never calls a kernel) + a working
/// `noop` (emits zeros), used to prove the watchdog forcibly interrupts a
/// pure-Python spin loop and the worker survives for the next CALL.
const SPIN_MODULE: &str = r#"
db.set_plugin_id("ai.example.pyspin")
db.set_version("0.1.0")

@db.algorithm("spin", args=["int"], yields=["nodeId:int", "score:float"])
def spin(gc, source):
    while True:
        pass

@db.algorithm("noop", args=["int"], yields=["nodeId:int", "score:float"])
def noop(gc, source):
    g = gc.graph()
    d = gc.degrees(g, "out")
    z = gc.scale(d, 0.0)
    gc.emit("score", z)
"#;

#[tokio::test]
async fn pyo3_deadline_honored() -> anyhow::Result<()> {
    // A1 gate: a guest doing `while True: pass` (zero kernel calls) is forcibly
    // interrupted by the watchdog within the WallClockMillisPerCall deadline, the
    // worker survives, and a subsequent CALL still succeeds.
    Python::initialize();
    let db = Uni::in_memory().build().await?;
    let vid_a = build_graph(&db).await?;

    let loader = uni_plugin_pyo3::PythonPluginLoader::with_default_plugin_id("ai.example.pyspin");
    let caps = CapabilitySet::from_iter_of([
        Capability::Algorithm,
        Capability::GraphCompute,
        Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        },
        // 500ms deadline so the spin loop is interrupted fast, not after 30s.
        Capability::WallClockMillisPerCall(500),
    ]);
    Python::attach(|py| {
        db.load_python_plugin(py, &loader, SPIN_MODULE, "ai.example.pyspin", &caps)
            .expect("load spin plugin");
    });

    let session = db.session();
    let start = std::time::Instant::now();
    let err = session
        .query(&format!(
            "CALL ai.example.pyspin.spin({vid_a}) YIELD nodeId, score RETURN nodeId"
        ))
        .await
        .expect_err("a runaway spin guest must be interrupted, not hang");
    let elapsed = start.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(15),
        "spin must be interrupted near the 500ms deadline, took {elapsed:?}"
    );
    // The interrupt surfaces as a typed incomplete outcome, not a generic query
    // error: the reason is Timeout (0x867), distinguishable from Exhausted /
    // IterationLimit so a caller knows to raise the deadline (proposal §5.2).
    match err {
        uni_common::UniError::GraphComputeIncomplete { detail } => assert_eq!(
            detail.reason,
            uni_common::GraphComputeIncompleteReason::Timeout,
            "a runaway guest past its wall-clock deadline must be a Timeout, got {detail}"
        ),
        other => panic!("expected GraphComputeIncomplete{{Timeout}}, got {other:?}"),
    }

    // Worker survived: a normal CALL on the same plugin still works.
    let ok = session
        .query(&format!(
            "CALL ai.example.pyspin.noop({vid_a}) YIELD nodeId, score RETURN nodeId, score"
        ))
        .await?;
    assert_eq!(ok.rows().len(), 4, "worker must survive the interrupt");
    Ok(())
}
