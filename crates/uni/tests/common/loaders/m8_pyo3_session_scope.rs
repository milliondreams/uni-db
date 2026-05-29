#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M8.6 — session-scoped PyO3 plugin registry tests.
//!
//! Proves the proposal §5.4.2 contract: Python plugins added through
//! [`uni_db::Session::add_python_plugin`] are visible only to the
//! session that registered them, disappear when that session is
//! dropped, and shadow instance-level plugins of the same qname in
//! the registering session.

#![cfg(feature = "pyo3-plugins")]

use pyo3::Python;
use uni_plugin::{Capability, CapabilitySet, QName};
use uni_plugin_pyo3::PythonPluginLoader;

/// Helper: read the value of `py.score(1.0, 2.0)` from a session's
/// query path. Returns `Ok(Some(value))` if the function is visible
/// and produces a value; `Ok(None)` if the function resolves but is
/// somehow non-callable; `Err` for "function not registered"-class
/// failures.
async fn try_score(session: &uni_db::Session) -> anyhow::Result<f64> {
    let result = session
        .query_with("RETURN score(1.0, 2.0) AS s")
        .fetch_all()
        .await?;
    let row = result.into_rows().into_iter().next().expect("one row");
    let v: f64 = row.get("s").map_err(|e| anyhow::anyhow!("get s: {e}"))?;
    Ok(v)
}

const SCORE_MODULE: &str = r#"
db.set_plugin_id("ai.example.pyscore")
db.set_version("0.1.0")

@db.scalar_fn("score", args=["float","float"], returns="float", determinism="pure")
def score(x, y):
    return x * 0.7 + y * 0.3
"#;

const SHADOW_MODULE: &str = r#"
db.set_plugin_id("ai.example.pyscore")
db.set_version("0.2.0")

@db.scalar_fn("score", args=["float","float"], returns="float", determinism="pure")
def score(x, y):
    # Session-scope shadow returns a recognizably-different value
    # so the test can assert which implementation served the call.
    return 999.0
"#;

#[tokio::test]
async fn session_local_visibility() -> anyhow::Result<()> {
    Python::initialize();
    let uni = uni_db::Uni::in_memory().build().await?;
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let loader = PythonPluginLoader::with_default_plugin_id("ai.example.pyscore");

    let session_a = uni.session();
    let session_b = uni.session();

    // Register through Session A only.
    let outcome = Python::attach(|py| {
        session_a.add_python_plugin(py, &loader, SCORE_MODULE, "ai.example.pyscore", &caps)
    })?;
    assert_eq!(outcome.scalars_registered.len(), 1);

    // Session A sees the registration via Cypher.
    let got_a = try_score(&session_a).await?;
    assert!(
        (got_a - (1.0 * 0.7 + 2.0 * 0.3)).abs() < 1e-12,
        "session A got {got_a}"
    );

    // Session B does NOT see it — query must error.
    let err = try_score(&session_b).await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("score")
            || msg.to_lowercase().contains("function")
            || msg.to_lowercase().contains("unknown"),
        "expected `function not found`-class error, got: {msg}"
    );

    Ok(())
}

#[tokio::test]
async fn session_drop_unregisters() -> anyhow::Result<()> {
    Python::initialize();
    let uni = uni_db::Uni::in_memory().build().await?;
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let loader = PythonPluginLoader::with_default_plugin_id("ai.example.pyscore");

    let direct_invoke = {
        let session = uni.session();
        Python::attach(|py| {
            session
                .add_python_plugin(py, &loader, SCORE_MODULE, "ai.example.pyscore", &caps)
                .expect("register")
        });
        // Plugin lives on the *session-local* registry; confirm via
        // direct lookup before drop.
        let qn = QName::new("ai.example.pyscore", "score");
        session.plugin_registry().scalar_fn(&qn).is_some()
    };
    assert!(
        direct_invoke,
        "scalar should be in session registry pre-drop"
    );

    // Session dropped at end of the block. A fresh session must not
    // see the plugin in either its session-local registry or the
    // instance registry.
    let fresh = uni.session();
    let qn = QName::new("ai.example.pyscore", "score");
    assert!(
        fresh.plugin_registry().scalar_fn(&qn).is_none(),
        "fresh session must not see dropped session's plugin"
    );
    assert!(
        fresh.instance_plugin_registry().scalar_fn(&qn).is_none(),
        "instance registry must not carry session-scoped plugin"
    );

    // Cypher resolution path also fails.
    let err = try_score(&fresh).await.unwrap_err();
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("py.score") || msg.contains("function") || msg.contains("unknown"),
        "expected `function not found`-class, got: {msg}"
    );

    Ok(())
}

#[tokio::test]
async fn session_shadows_instance() -> anyhow::Result<()> {
    Python::initialize();
    let uni = uni_db::Uni::in_memory().build().await?;
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let loader = PythonPluginLoader::with_default_plugin_id("ai.example.pyscore");

    // 1. Instance-scope: `score(x, y)` returns x*0.7 + y*0.3.
    Python::attach(|py| {
        uni.load_python_plugin(py, &loader, SCORE_MODULE, "ai.example.pyscore", &caps)
            .expect("instance load")
    });
    let qn = QName::new("ai.example.pyscore", "score");
    assert!(
        uni.plugin_registry().scalar_fn(&qn).is_some(),
        "instance registry must carry the scalar after Uni::load_python_plugin"
    );

    // 2. Session-scope shadow: register a different implementation in Session A only.
    let session_a = uni.session();
    let session_b = uni.session();
    Python::attach(|py| {
        session_a
            .add_python_plugin(py, &loader, SHADOW_MODULE, "ai.example.pyscore", &caps)
            .expect("session shadow")
    });

    // Session A: Cypher query MUST see the session shadow (returns 999.0).
    let got_a = try_score(&session_a).await?;
    assert!(
        (got_a - 999.0).abs() < 1e-9,
        "session A got {got_a}; expected shadow"
    );

    // Session B: Cypher query MUST see the instance implementation (returns 1.7).
    let got_b = try_score(&session_b).await?;
    assert!(
        (got_b - 1.3).abs() < 1e-9,
        "session B got {got_b}; expected instance impl (1.0*0.7 + 2.0*0.3 = 1.3)"
    );

    Ok(())
}
