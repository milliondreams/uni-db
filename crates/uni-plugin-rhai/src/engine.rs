//! Per-plugin `rhai::Engine` factory.
//!
//! Builds a Rhai engine configured for the framework's sandbox model:
//!
//! - **Eval disabled** at the symbol level so scripts cannot smuggle in
//!   dynamic code generation.
//! - **Module resolver replaced with a deny-all stub** so `import` always
//!   fails. Modules can only be made available through host-registered
//!   Rhai packages (none are exposed in v1).
//! - **Resource limits** wired from the effective `CapabilitySet`:
//!   `Capability::FuelPerCall(N)` → `Engine::set_max_operations(N)`;
//!   `Capability::MemoryBytes(N)` → conservative caps on string / array /
//!   map sizes (full memory accounting is M10's broader work).
//! - **Capability-gated host fns** registered conditionally — fns whose
//!   `required_capability` is not in the effective set are simply not
//!   registered, and the script fails at parse-resolution with
//!   `ErrorFunctionNotFound`. This is the in-host analogue of CM's
//!   linker-absence guarantee (proposal §10.2).

#[cfg(feature = "rhai-runtime")]
use rhai::Engine;

use uni_plugin::{Capability, CapabilitySet};

use crate::host_fns::RhaiHostFnRegistry;

/// Default maximum recursion depth for Rhai scripts. Overridable by
/// scripts via the loader's per-plugin engine configuration; future:
/// expose a `Capability::MaxCallLevels(N)` so plugins can request more.
pub const DEFAULT_MAX_CALL_LEVELS: usize = 64;

/// Default Rhai operation-limit floor applied to every engine.
///
/// Rhai's built-in default of `max_operations = 0` means *unlimited*, so
/// without an explicit cap a malicious or buggy plugin running `while true
/// {}` would wedge the synchronous DataFusion worker thread forever. This
/// floor bounds every engine's per-call work even when no
/// `Capability::FuelPerCall` grant is present. A `FuelPerCall` grant may
/// only *raise* this floor, never lower or disable it.
pub const DEFAULT_MAX_OPERATIONS: u64 = 10_000_000;

/// Build a Rhai engine pre-configured for a single plugin's effective
/// capability set.
///
/// The returned Engine has:
/// - Full Rhai stdlib (math, array, map, string, time).
/// - `eval` disabled.
/// - A deny-all module resolver (no `import` statements work).
/// - Resource limits applied from `effective_caps`.
/// - Each `host_fns` spec whose required capability is satisfied has its
///   `register` closure invoked against the engine.
#[cfg(feature = "rhai-runtime")]
#[must_use]
pub fn build_engine(effective_caps: &CapabilitySet, host_fns: &RhaiHostFnRegistry) -> Engine {
    let mut engine = Engine::new();

    // Disable eval — no dynamic code generation inside scripts.
    engine.disable_symbol("eval");

    // Deny-all module resolver. Rhai's `DummyModuleResolver` always
    // returns "ModuleNotFound" for any path, blocking `import` statements.
    engine.set_module_resolver(rhai::module_resolvers::DummyModuleResolver::new());

    // Always cap call depth (stack-overflow protection).
    engine.set_max_call_levels(DEFAULT_MAX_CALL_LEVELS);

    // Always apply an operation-limit floor. Rhai defaults to
    // `max_operations = 0` (unlimited); without this every engine lacking a
    // `FuelPerCall` grant could be wedged forever by an unbounded loop. A
    // grant raises this floor in `apply_resource_limits`; it never lowers it.
    engine.set_max_operations(DEFAULT_MAX_OPERATIONS);

    // Apply resource limits from capabilities.
    apply_resource_limits(&mut engine, effective_caps);

    // Always-available column userdata for vectorized mode.
    crate::columns::register_column_types(&mut engine);

    // Always-available GraphCompute kernel surface. A guest that is never
    // handed a `GcSession` cannot call any kernel; the capability gate is
    // enforced host-side at projection time (proposal §4.6).
    crate::graph_compute::register_graph_compute(&mut engine);

    // Register capability-gated host fns.
    for spec in host_fns.iter() {
        let granted = match &spec.required_capability {
            None => true,
            Some(required) => caps_grant(effective_caps, required),
        };
        if granted {
            // Pass the effective set so the registered fn can enforce
            // call-time attenuation (URL allow-list, key-id match, …).
            (spec.register)(&mut engine, effective_caps);
        }
    }

    engine
}

/// Stub variant used when the `rhai-runtime` feature is disabled — keeps
/// the crate compiling for embedders that only want the trait surface.
#[cfg(not(feature = "rhai-runtime"))]
pub fn build_engine(_effective_caps: &CapabilitySet, _host_fns: &RhaiHostFnRegistry) {}

#[cfg(feature = "rhai-runtime")]
fn apply_resource_limits(engine: &mut Engine, caps: &CapabilitySet) {
    for cap in caps.iter() {
        match cap {
            Capability::FuelPerCall(n) => {
                // A grant may only raise the always-applied floor, never
                // lower it (and never re-enable Rhai's unlimited default of 0).
                engine.set_max_operations((*n).max(DEFAULT_MAX_OPERATIONS));
            }
            Capability::MemoryBytes(n) => {
                // Rhai doesn't have a direct total-memory cap. Apply
                // conservative per-collection caps derived from the
                // total budget. Full memory accounting is M10 work.
                let per_collection = (*n / 4).max(1024) as usize;
                engine.set_max_string_size(per_collection);
                engine.set_max_array_size(per_collection);
                engine.set_max_map_size(per_collection);
            }
            _ => {}
        }
    }
}

/// Does the effective set grant a capability variant that satisfies
/// `required`?
///
/// For now this is variant-equality. Pattern-narrowed grants
/// (`Filesystem { read: ["/data/**"] }`) are validated at host-fn-body
/// call time (Phase 5), not at engine-construction time.
fn caps_grant(effective: &CapabilitySet, required: &Capability) -> bool {
    effective.contains_variant(required)
}

#[cfg(all(test, feature = "rhai-runtime"))]
mod tests {
    use super::*;
    use crate::host_fns::RhaiHostFnSpec;
    use std::sync::Arc;

    fn empty_caps() -> CapabilitySet {
        CapabilitySet::new()
    }

    #[test]
    fn eval_is_disabled() {
        let engine = build_engine(&empty_caps(), &RhaiHostFnRegistry::new());
        // `eval` is a Rhai keyword; disable_symbol turns it into a parse
        // error.
        let result = engine.eval::<rhai::Dynamic>(r#"eval("1 + 1")"#);
        assert!(result.is_err(), "eval should be disabled");
    }

    #[test]
    fn import_is_denied() {
        let engine = build_engine(&empty_caps(), &RhaiHostFnRegistry::new());
        let script = r#"import "math" as m; m.pi"#;
        let result = engine.eval::<rhai::Dynamic>(script);
        assert!(
            result.is_err(),
            "import should be denied by module resolver"
        );
    }

    #[test]
    fn ungranted_host_fn_not_registered() {
        let mut host_fns = RhaiHostFnRegistry::new();
        host_fns.register(RhaiHostFnSpec {
            name: "uni.fs.read".to_owned(),
            required_capability: Some(Capability::Filesystem {
                read: vec!["/data/**".into()],
                write: vec![],
            }),
            docs: String::new(),
            register: Arc::new(|engine: &mut Engine, _caps: &CapabilitySet| {
                engine.register_fn("uni_fs_read", |_path: &str| "ok".to_string());
            }),
        });
        let engine = build_engine(&empty_caps(), &host_fns);
        let result = engine.eval::<String>(r#"uni_fs_read("/data/x")"#);
        assert!(result.is_err(), "ungranted host fn must not resolve");
    }

    #[test]
    fn granted_host_fn_callable() {
        let mut host_fns = RhaiHostFnRegistry::new();
        let cap = Capability::Filesystem {
            read: vec!["/data/**".into()],
            write: vec![],
        };
        host_fns.register(RhaiHostFnSpec {
            name: "uni.fs.read".to_owned(),
            required_capability: Some(cap.clone()),
            docs: String::new(),
            register: Arc::new(|engine: &mut Engine, _caps: &CapabilitySet| {
                engine.register_fn("uni_fs_read", |_path: &str| "ok".to_string());
            }),
        });
        let caps = CapabilitySet::from_iter_of([cap]);
        let engine = build_engine(&caps, &host_fns);
        let result: String = engine
            .eval(r#"uni_fs_read("/data/x")"#)
            .expect("should call");
        assert_eq!(result, "ok");
    }

    #[test]
    fn fuel_limit_trips() {
        // Grant a budget above the always-applied DEFAULT_MAX_OPERATIONS
        // floor so this exercises the granted limit, not the floor.
        let grant = DEFAULT_MAX_OPERATIONS + 2_000_000;
        let caps = CapabilitySet::from_iter_of([Capability::FuelPerCall(grant)]);
        let engine = build_engine(&caps, &RhaiHostFnRegistry::new());
        // 100M loop iterations cost far more than `grant` operations.
        let script = r#"
            let i = 0;
            while i < 100000000 {
                i += 1;
            }
            i
        "#;
        let result = engine.eval::<i64>(script);
        assert!(
            result.is_err(),
            "a FuelPerCall grant should trip on a long-enough loop"
        );
    }

    #[test]
    fn default_op_limit_floor_applies_without_fuel_grant() {
        // No FuelPerCall grant: the DEFAULT_MAX_OPERATIONS floor must still
        // trip an unbounded loop instead of Rhai's unlimited default.
        let engine = build_engine(&empty_caps(), &RhaiHostFnRegistry::new());
        let script = r#"
            let i = 0;
            while true {
                i += 1;
            }
            i
        "#;
        let result = engine.eval::<i64>(script);
        assert!(
            result.is_err(),
            "the default op-limit floor must trip an unbounded loop without any fuel grant"
        );
    }

    #[test]
    fn fuel_grant_below_floor_is_raised_to_floor() {
        // A grant below the floor must not lower it: a loop that exceeds the
        // tiny grant but stays under the floor must succeed.
        let caps = CapabilitySet::from_iter_of([Capability::FuelPerCall(1_000)]);
        let engine = build_engine(&caps, &RhaiHostFnRegistry::new());
        let script = r#"
            let i = 0;
            while i < 50000 {
                i += 1;
            }
            i
        "#;
        let result: i64 = engine
            .eval(script)
            .expect("a sub-floor grant must be raised to the floor, not lowered");
        assert_eq!(result, 50000);
    }

    #[test]
    fn always_available_fn_registered_with_empty_caps() {
        let mut host_fns = RhaiHostFnRegistry::new();
        host_fns.register(RhaiHostFnSpec {
            name: "uni.always".to_owned(),
            required_capability: None,
            docs: String::new(),
            register: Arc::new(|engine: &mut Engine, _caps: &CapabilitySet| {
                engine.register_fn("uni_always", || 42_i64);
            }),
        });
        let engine = build_engine(&empty_caps(), &host_fns);
        let result: i64 = engine.eval("uni_always()").expect("should call");
        assert_eq!(result, 42);
    }
}
