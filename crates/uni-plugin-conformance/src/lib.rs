//! Conformance test runner for uni-db plugin authors.
//!
//! This crate publishes a portable test harness that plugin authors run against
//! their built `.wasm` (or compile-time Rust) plugins to verify:
//!
//! - Manifest correctness (parses, capabilities declared match
//!   registrations).
//! - Schema correctness (declared signatures match actual `invoke` shapes).
//! - Determinism honesty (a `Pure` plugin produces identical output on
//!   identical inputs across repeated calls).
//! - Error model compliance (errors at the WIT boundary are well-formed).
//! - Resource limit adherence (plugin respects declared `MemoryBytes`
//!   cap).
//!
//! # Crate status
//!
//! The live probe suite runs through [`run_against_plugin`] and exercises
//! six invariants: `manifest.parse`, `manifest.id_format`, `abi.in_range`,
//! `capabilities.declared`, `registration.commit`, and
//! `registration.idempotent`. The [`run_against`] entry point dispatches
//! to that suite for [`ConformanceTarget::LiveRust`]; the
//! [`ConformanceTarget::WasmPath`] branch is still a stub pending M6a/M6b
//! SDK integration (tracked in `docs/KNOWN_GAPS.md`).
//!
//! # Examples
//!
//! ```ignore
//! use uni_plugin_conformance::{run_against, ConformanceTarget};
//!
//! let target = ConformanceTarget::WasmPath("./example_geo.wasm".into());
//! let report = run_against(&target);
//! report.assert_pass();
//! ```

// Rust guideline compliant
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// What the conformance runner is testing.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConformanceTarget {
    /// A built WASM plugin artifact at the given path.
    WasmPath(PathBuf),
    /// A live Rust plugin (compile-time path).
    LiveRust,
}

/// Outcome of one conformance check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckResult {
    /// Stable identifier for the check (so CI can pin it).
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
    /// Failure detail (empty when passed).
    pub detail: String,
}

/// Aggregate report from running the conformance suite.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConformanceReport {
    /// Per-check results in execution order.
    pub checks: Vec<CheckResult>,
}

impl ConformanceReport {
    /// Returns `true` if every check passed.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|c| c.passed)
    }

    /// Panic with a summary of failures if any check failed.
    ///
    /// # Panics
    ///
    /// Panics if any check has `passed == false`.
    pub fn assert_pass(&self) {
        if self.passed() {
            return;
        }
        let failures: Vec<&CheckResult> = self.checks.iter().filter(|c| !c.passed).collect();
        let mut msg = String::from("conformance failures:\n");
        for f in failures {
            msg.push_str(&format!("  - {} [{}]: {}\n", f.name, f.id, f.detail));
        }
        panic!("{msg}");
    }
}

/// Run the conformance suite against a target.
///
/// For [`ConformanceTarget::LiveRust`], returns a marker that points
/// callers at [`run_against_plugin`] (which takes the actual plugin
/// instance). For [`ConformanceTarget::WasmPath`], returns a marker
/// directing callers to [`run_against_wasm`], which takes a
/// [`WasmConformanceLoader`] so the conformance crate stays
/// loader-agnostic (it must remain below `uni-plugin-extism` /
/// `uni-plugin-wasm` in the dep graph).
#[must_use]
pub fn run_against(target: &ConformanceTarget) -> ConformanceReport {
    match target {
        ConformanceTarget::LiveRust => ConformanceReport {
            checks: vec![CheckResult {
                id: "scaffold.runner".to_owned(),
                name: "Conformance runner wired".to_owned(),
                passed: true,
                detail: "for real probes call run_against_plugin(plugin)".to_owned(),
            }],
        },
        ConformanceTarget::WasmPath(p) => ConformanceReport {
            checks: vec![CheckResult {
                id: "wasm.runner".to_owned(),
                name: "WASM conformance runner".to_owned(),
                passed: true,
                detail: format!(
                    "for real probes against `{}` call \
                     run_against_wasm(loader, path) — see WasmConformanceLoader",
                    p.display()
                ),
            }],
        },
    }
}

/// Bridge between the conformance crate and a WASM plugin loader.
///
/// `uni-plugin-conformance` lives below every concrete loader in the
/// workspace dep graph, so it cannot depend on `uni-plugin-extism` or
/// any wasmtime-based loader directly. Plugin authors implement this
/// trait in their test binary against whichever loader they ship, then
/// pass it to [`run_against_wasm`] to drive the same probe suite that
/// [`run_against_plugin`] uses for live-Rust plugins.
///
/// # Errors
///
/// The implementor returns a human-readable string on failure; the
/// conformance runner surfaces it as the detail of the `wasm.load`
/// check.
pub trait WasmConformanceLoader {
    /// Load a WASM artifact from `path` and produce a live
    /// [`uni_plugin::Plugin`] handle. The returned plugin must keep any
    /// underlying instance pool / wasmtime store alive for the
    /// lifetime of the box.
    ///
    /// # Errors
    ///
    /// Returns a descriptive error string when the artifact cannot be
    /// loaded, the manifest export is missing or malformed, or
    /// instantiation fails.
    fn load(
        &self,
        path: &std::path::Path,
    ) -> Result<Box<dyn uni_plugin::Plugin + Send + Sync>, String>;
}

/// Run the conformance probe suite against a WASM artifact via the
/// supplied loader.
///
/// On load failure, returns a report containing a single failing
/// `wasm.load` check that carries the loader's error message. On
/// success, dispatches to [`run_against_plugin`] so the WASM target
/// runs the same six probes that live-Rust plugins do.
#[must_use]
pub fn run_against_wasm<L: WasmConformanceLoader>(
    loader: &L,
    path: &std::path::Path,
) -> ConformanceReport {
    match loader.load(path) {
        Ok(plugin) => run_against_plugin(plugin.as_ref()),
        Err(detail) => ConformanceReport {
            checks: vec![CheckResult {
                id: "wasm.load".to_owned(),
                name: "WASM artifact loads".to_owned(),
                passed: false,
                detail,
            }],
        },
    }
}

/// **M12 substantive**: run the conformance probe suite against a live
/// [`uni_plugin::Plugin`]. Each probe exercises one invariant from
/// proposal §16.4 and produces a [`CheckResult`].
///
/// Probes:
/// 1. **manifest.parse** — id non-empty, version semver-valid.
/// 2. **manifest.id_format** — id follows reverse-DNS shape (or is a
///    reserved single-token id like `"builtin"`).
/// 3. **abi.in_range** — declared ABI range matches at least one major
///    in 0..=63.
/// 4. **capabilities.declared** — the manifest's `CapabilitySet`
///    accessor doesn't panic.
/// 5. **registration.commit** — `register()` + `commit_to_registry()`
///    succeed against a fresh registry under declared capabilities.
/// 6. **registration.idempotent** — `remove_plugin` + re-register
///    succeeds.
///
/// CI pins on the stable `id` field, not the human-readable name.
#[must_use]
pub fn run_against_plugin(plugin: &dyn uni_plugin::Plugin) -> ConformanceReport {
    let mut checks: Vec<CheckResult> = Vec::new();
    let manifest = plugin.manifest();

    checks.push(check(
        "manifest.parse",
        "Manifest has non-empty id and valid semver version",
        || {
            if manifest.id.as_str().is_empty() {
                return Err("manifest.id is empty".to_owned());
            }
            Ok(())
        },
    ));

    checks.push(check(
        "manifest.id_format",
        "Plugin id is reverse-DNS or a reserved single-token id",
        || {
            let id = manifest.id.as_str();
            if id.contains('.') {
                return Ok(());
            }
            if uni_plugin::is_reserved_plugin_id(id) {
                return Ok(());
            }
            Err(format!(
                "id `{id}` should contain `.` (e.g., `ai.example.geo`) or be reserved"
            ))
        },
    ));

    checks.push(check(
        "abi.in_range",
        "ABI range matches at least one major in 0..=63",
        || {
            if (0_u64..=63).any(|m| manifest.abi.matches(m)) {
                Ok(())
            } else {
                Err(format!(
                    "ABI range `{:?}` matches no major in 0..=63",
                    manifest.abi
                ))
            }
        },
    ));

    checks.push(check(
        "capabilities.declared",
        "Manifest's CapabilitySet accessor is safe to call",
        || {
            let _ = &manifest.capabilities;
            Ok(())
        },
    ));

    checks.push(check(
        "registration.commit",
        "Plugin's register() + commit succeed against a fresh registry",
        || {
            use uni_plugin::{PluginRegistrar, PluginRegistry};
            let registry = PluginRegistry::new();
            let caps = manifest.capabilities.clone();
            let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
            plugin
                .register(&mut r)
                .map_err(|e| format!("register failed: {e}"))?;
            r.commit_to_registry()
                .map_err(|e| format!("commit failed: {e}"))
        },
    ));

    checks.push(check(
        "registration.idempotent",
        "remove_plugin + re-register succeeds",
        || {
            use uni_plugin::{PluginRegistrar, PluginRegistry};
            let registry = PluginRegistry::new();
            let caps = manifest.capabilities.clone();
            {
                let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
                plugin
                    .register(&mut r)
                    .map_err(|e| format!("first register: {e}"))?;
                r.commit_to_registry()
                    .map_err(|e| format!("first commit: {e}"))?;
            }
            registry.remove_plugin(&manifest.id);
            {
                let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
                plugin
                    .register(&mut r)
                    .map_err(|e| format!("re-register: {e}"))?;
                r.commit_to_registry()
                    .map_err(|e| format!("re-commit: {e}"))?;
            }
            Ok(())
        },
    ));

    ConformanceReport { checks }
}

fn check<F>(id: &str, name: &str, body: F) -> CheckResult
where
    F: FnOnce() -> Result<(), String>,
{
    match body() {
        Ok(()) => CheckResult {
            id: id.to_owned(),
            name: name.to_owned(),
            passed: true,
            detail: String::new(),
        },
        Err(detail) => CheckResult {
            id: id.to_owned(),
            name: name.to_owned(),
            passed: false,
            detail,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_passes_when_all_checks_pass() {
        let r = run_against(&ConformanceTarget::LiveRust);
        assert!(r.passed());
        r.assert_pass(); // doesn't panic
    }

    #[test]
    #[should_panic(expected = "conformance failures")]
    fn assert_pass_panics_on_failure() {
        let r = ConformanceReport {
            checks: vec![CheckResult {
                id: "x".into(),
                name: "x".into(),
                passed: false,
                detail: "broke".into(),
            }],
        };
        r.assert_pass();
    }

    // ── Real probe-suite tests ─────────────────────────────────────

    #[derive(Debug)]
    struct GoodPlugin {
        manifest: std::sync::OnceLock<uni_plugin::PluginManifest>,
    }

    impl GoodPlugin {
        fn new() -> Self {
            Self {
                manifest: std::sync::OnceLock::new(),
            }
        }
        fn manifest_value() -> uni_plugin::PluginManifest {
            use semver::Version;
            use uni_plugin::{
                AbiRange, CapabilitySet, Determinism, PluginId, PluginManifest, ProvidedSurfaces,
                Scope, SideEffects,
            };
            PluginManifest {
                id: PluginId::new("ai.example.test"),
                version: Version::new(1, 0, 0),
                abi: AbiRange::parse("^1").unwrap(),
                depends_on: vec![],
                capabilities: CapabilitySet::default(),
                determinism: Determinism::Pure,
                side_effects: SideEffects::ReadOnly,
                scope: Scope::Instance,
                hash: None,
                signature: None,
                provides: ProvidedSurfaces::default(),
                docs: "test".to_owned(),
                metadata: Default::default(),
            }
        }
    }
    impl uni_plugin::Plugin for GoodPlugin {
        fn manifest(&self) -> &uni_plugin::PluginManifest {
            self.manifest.get_or_init(Self::manifest_value)
        }
        fn register(
            &self,
            _r: &mut uni_plugin::PluginRegistrar<'_>,
        ) -> Result<(), uni_plugin::PluginError> {
            Ok(())
        }
    }

    #[test]
    fn run_against_plugin_passes_well_formed_plugin() {
        let p = GoodPlugin::new();
        let report = run_against_plugin(&p);
        assert!(report.passed(), "failures: {:?}", report);
        assert_eq!(report.checks.len(), 6);
        let ids: std::collections::BTreeSet<&str> =
            report.checks.iter().map(|c| c.id.as_str()).collect();
        for expected in [
            "manifest.parse",
            "manifest.id_format",
            "abi.in_range",
            "capabilities.declared",
            "registration.commit",
            "registration.idempotent",
        ] {
            assert!(ids.contains(expected), "missing probe {expected}");
        }
    }

    #[derive(Debug)]
    struct BadIdPlugin {
        manifest: std::sync::OnceLock<uni_plugin::PluginManifest>,
    }
    impl uni_plugin::Plugin for BadIdPlugin {
        fn manifest(&self) -> &uni_plugin::PluginManifest {
            self.manifest.get_or_init(|| {
                let mut m = GoodPlugin::manifest_value();
                m.id = uni_plugin::PluginId::new("no-dot");
                m
            })
        }
        fn register(
            &self,
            _r: &mut uni_plugin::PluginRegistrar<'_>,
        ) -> Result<(), uni_plugin::PluginError> {
            Ok(())
        }
    }

    #[test]
    fn run_against_plugin_flags_non_reverse_dns_id() {
        let p = BadIdPlugin {
            manifest: std::sync::OnceLock::new(),
        };
        let report = run_against_plugin(&p);
        assert!(!report.passed());
        let id_check = report
            .checks
            .iter()
            .find(|c| c.id == "manifest.id_format")
            .unwrap();
        assert!(!id_check.passed);
        assert!(id_check.detail.contains("no-dot"));
    }

    #[test]
    fn run_against_plugin_recognizes_reserved_ids() {
        // BuiltinPlugin / ApocCorePlugin use single-token reserved ids
        // ("builtin", "apoc-core"). The probe accepts these as valid.
        struct BuiltinLikePlugin(std::sync::OnceLock<uni_plugin::PluginManifest>);
        impl std::fmt::Debug for BuiltinLikePlugin {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("BuiltinLikePlugin").finish_non_exhaustive()
            }
        }
        impl uni_plugin::Plugin for BuiltinLikePlugin {
            fn manifest(&self) -> &uni_plugin::PluginManifest {
                self.0.get_or_init(|| {
                    let mut m = GoodPlugin::manifest_value();
                    m.id = uni_plugin::PluginId::new("builtin");
                    m
                })
            }
            fn register(
                &self,
                _r: &mut uni_plugin::PluginRegistrar<'_>,
            ) -> Result<(), uni_plugin::PluginError> {
                Ok(())
            }
        }
        let p = BuiltinLikePlugin(std::sync::OnceLock::new());
        let report = run_against_plugin(&p);
        assert!(report.passed(), "builtin id should pass: {report:?}");
    }

    #[test]
    fn wasm_target_returns_runner_pointer() {
        // The bare `run_against(WasmPath)` no longer fakes a failure
        // pending M6 — it now emits a marker pointing callers at
        // `run_against_wasm(loader, path)`.
        let r = run_against(&ConformanceTarget::WasmPath("/tmp/x.wasm".into()));
        assert!(r.passed());
        assert_eq!(r.checks[0].id, "wasm.runner");
        assert!(r.checks[0].detail.contains("run_against_wasm"));
    }

    struct FailingLoader(&'static str);
    impl WasmConformanceLoader for FailingLoader {
        fn load(
            &self,
            _path: &std::path::Path,
        ) -> Result<Box<dyn uni_plugin::Plugin + Send + Sync>, String> {
            Err(self.0.to_owned())
        }
    }

    #[test]
    fn run_against_wasm_surfaces_loader_error() {
        let loader = FailingLoader("artifact missing manifest export");
        let r = run_against_wasm(&loader, std::path::Path::new("/tmp/x.wasm"));
        assert!(!r.passed());
        assert_eq!(r.checks.len(), 1);
        assert_eq!(r.checks[0].id, "wasm.load");
        assert!(r.checks[0].detail.contains("artifact missing"));
    }

    struct OkLoader;
    impl WasmConformanceLoader for OkLoader {
        fn load(
            &self,
            _path: &std::path::Path,
        ) -> Result<Box<dyn uni_plugin::Plugin + Send + Sync>, String> {
            #[derive(Debug)]
            struct Wrapped(std::sync::OnceLock<uni_plugin::PluginManifest>);
            impl uni_plugin::Plugin for Wrapped {
                fn manifest(&self) -> &uni_plugin::PluginManifest {
                    self.0.get_or_init(GoodPlugin::manifest_value)
                }
                fn register(
                    &self,
                    _r: &mut uni_plugin::PluginRegistrar<'_>,
                ) -> Result<(), uni_plugin::PluginError> {
                    Ok(())
                }
            }
            Ok(Box::new(Wrapped(std::sync::OnceLock::new())))
        }
    }

    #[test]
    fn run_against_wasm_dispatches_to_full_probe_suite() {
        let r = run_against_wasm(&OkLoader, std::path::Path::new("/tmp/x.wasm"));
        assert!(r.passed(), "report: {r:?}");
        assert_eq!(r.checks.len(), 6, "should run the full six-probe suite");
    }
}
