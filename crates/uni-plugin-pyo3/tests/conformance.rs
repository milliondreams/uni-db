//! M8.8 — conformance suite hookup for PyO3 plugins.
//!
//! Loads a minimal Python plugin via the PyO3 loader, wraps the
//! `LoadOutcome` in a [`PyPluginHandle`], and runs the
//! [`uni_plugin_conformance`] 6-probe suite against it. Asserts all 6
//! probes pass (proposal §19 acceptance criterion #8).

#![cfg(feature = "pyo3")]

use pyo3::Python;
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry};
use uni_plugin_conformance::run_against_plugin;
use uni_plugin_pyo3::{PyPluginHandle, PythonPluginLoader};

const PYTHON_SRC: &str = r#"
db.set_plugin_id("ai.example.conformance")
db.set_version("1.0.0")

@db.scalar_fn("noop", args=["float"], returns="float", determinism="pure")
def noop(x):
    return x

@db.scalar_fn("twice", args=["float"], returns="float", determinism="pure")
def twice(x):
    return x * 2.0
"#;

#[test]
fn conformance_suite_passes_on_python_plugin() {
    Python::initialize();

    let loader = PythonPluginLoader::with_default_plugin_id("ai.example.conformance");
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    let outcome = Python::attach(|py| {
        let registry = PluginRegistry::new();
        let mut registrar =
            PluginRegistrar::new(PluginId::new("ai.example.conformance"), &caps, &registry);
        let outcome = loader
            .load(
                py,
                PYTHON_SRC,
                "ai.example.conformance",
                &mut registrar,
                &caps,
            )
            .expect("load python plugin");
        registrar.commit_to_registry().expect("commit");
        outcome
    });

    let handle = PyPluginHandle::new(outcome);
    let report = run_against_plugin(&handle);

    // All 6 probes from proposal §16.4 must pass:
    //  1. manifest.parse, 2. manifest.id_format, 3. abi.in_range,
    //  4. capabilities.declared, 5. registration.commit,
    //  6. registration.idempotent.
    assert_eq!(report.checks.len(), 6, "expected 6 probes");
    for c in &report.checks {
        assert!(c.passed, "probe `{}` failed: {}", c.id, c.detail);
    }
}

#[test]
fn conformance_probes_have_stable_ids() {
    // Ensures the probe set hasn't drifted between conformance lib
    // versions in a way that would silently break PyO3 plugin authors.
    Python::initialize();
    let loader = PythonPluginLoader::with_default_plugin_id("ai.example.idstable");
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let outcome = Python::attach(|py| {
        let registry = PluginRegistry::new();
        let mut registrar =
            PluginRegistrar::new(PluginId::new("ai.example.idstable"), &caps, &registry);
        let src = r#"
db.set_plugin_id("ai.example.idstable")
db.set_version("0.1.0")

@db.scalar_fn("x", args=["float"], returns="float", determinism="pure")
def x(v):
    return v
"#;
        loader
            .load(py, src, "ai.example.idstable", &mut registrar, &caps)
            .expect("load")
    });
    let handle = PyPluginHandle::new(outcome);
    let report = run_against_plugin(&handle);
    let ids: Vec<&str> = report.checks.iter().map(|c| c.id.as_str()).collect();
    assert!(ids.contains(&"manifest.parse"));
    assert!(ids.contains(&"manifest.id_format"));
    assert!(ids.contains(&"abi.in_range"));
    assert!(ids.contains(&"capabilities.declared"));
    assert!(ids.contains(&"registration.commit"));
    assert!(ids.contains(&"registration.idempotent"));
}
