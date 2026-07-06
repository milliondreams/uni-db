//! Repro for crates/uni-plugin-pyo3/src/loader.rs:614
//!
//! `db.set_determinism("nondeterministic")` writes
//! `PyManifest.determinism`, which NO registration code ever reads.
//! `register_scalars` (loader.rs:289) computes volatility purely from the
//! PER-ENTRY `entry.determinism`, whose decorator default is `"pure"`
//! (loader.rs:544), and `"pure"` maps to `Volatility::Immutable`
//! (adapter_scalar_helpers.rs:40).
//!
//! Therefore a plugin that sets the manifest-wide determinism to
//! `"nondeterministic"` and declares a scalar WITHOUT a per-entry
//! `determinism` arg registers that scalar as `Immutable` — the
//! `set_determinism` call has zero observable effect. DataFusion is then
//! free to constant-fold / dedupe a function the author explicitly
//! marked non-deterministic.

#![cfg(feature = "pyo3")]

use datafusion::logical_expr::Volatility;
use pyo3::Python;
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_pyo3::PythonPluginLoader;

/// A module that sets a manifest-wide `nondeterministic` default and
/// declares a scalar with NO per-entry determinism arg. If
/// `set_determinism` had any effect, the registered scalar would be
/// `Volatility::Volatile`; instead it stays `Immutable`.
const SRC: &str = r#"
db.set_plugin_id("ai.example.rng")
db.set_version("0.1.0")
db.set_determinism("nondeterministic")

@db.scalar_fn("rnd", args=["float"], returns="float")
def rnd(x):
    import random
    return random.random()
"#;

#[test]
fn set_determinism_is_a_silent_noop() {
    Python::initialize();
    let loader = PythonPluginLoader::with_default_plugin_id("ai.example.rng");
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    let registry = PluginRegistry::new();
    Python::attach(|py| {
        let mut r = PluginRegistrar::new(PluginId::new("ai.example.rng"), &caps, &registry);
        let outcome = loader
            .load(py, SRC, "ai.example.rng", &mut r, &caps)
            .expect("load python plugin");
        assert_eq!(outcome.scalars_registered.len(), 1);
        r.commit_to_registry().expect("commit");
    });

    let q = QName::new("ai.example.rng", "rnd");
    let entry = registry.scalar_fn(&q).expect("rnd registered");
    let vol = entry.signature.volatility;

    // FIXED (loader.rs): the scalar declares no per-entry determinism (decorator
    // default "inherit"), so it takes the manifest-wide "nondeterministic" set via
    // db.set_determinism -> Volatile, letting the author's intent stop DataFusion
    // from constant-folding / deduping the function.
    assert_eq!(
        vol,
        Volatility::Volatile,
        "set_determinism(\"nondeterministic\") must make the inheriting entry Volatile, got {vol:?}"
    );
}
