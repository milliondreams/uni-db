//! Integration test verifying `BuiltinPlugin` registers its built-ins
//! successfully through the framework.

use uni_plugin::{Plugin, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_builtin::BuiltinPlugin;

#[test]
fn builtin_plugin_registers_into_registry() {
    let registry = PluginRegistry::new();
    let plugin = BuiltinPlugin::new();
    let manifest = plugin.manifest();
    let caps = manifest.capabilities.clone();

    let mut registrar = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
    plugin.register(&mut registrar).expect("register");
    registrar.commit_to_registry().expect("commit");

    // Smoke check: the placeholder built-in is present.
    assert!(registry.scalar_fn(&QName::builtin("identity")).is_some());
}

#[test]
fn builtin_locy_aggregates_resolve_by_name() {
    let registry = PluginRegistry::new();
    let plugin = BuiltinPlugin::new();
    let manifest = plugin.manifest();
    let caps = manifest.capabilities.clone();
    let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
    plugin.register(&mut r).unwrap();
    r.commit_to_registry().unwrap();

    for name in [
        "MIN", "MAX", "SUM", "MSUM", "COUNT", "AVG", "COLLECT", "MNOR", "MPROD",
    ] {
        let q = QName::builtin(name);
        assert!(
            registry.locy_aggregate(&q).is_some(),
            "expected {name} to be registered as a Locy aggregate"
        );
    }

    // SUM and MSUM share runtime but differ in monotonicity contract.
    let sum_sl = registry
        .locy_aggregate(&QName::builtin("SUM"))
        .unwrap()
        .aggregate
        .semilattice();
    assert!(!sum_sl.monotone_join, "SUM must be non-monotone");

    let msum_sl = registry
        .locy_aggregate(&QName::builtin("MSUM"))
        .unwrap()
        .aggregate
        .semilattice();
    assert!(
        msum_sl.monotone_join,
        "MSUM must be monotone (caller asserts non-negative inputs)"
    );
    assert!(
        !msum_sl.has_top,
        "MSUM is unbounded — has_top must be false"
    );
}

#[test]
fn builtin_system_procedure_is_registered() {
    let registry = PluginRegistry::new();
    let plugin = BuiltinPlugin::new();
    let manifest = plugin.manifest();
    let caps = manifest.capabilities.clone();
    let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
    plugin.register(&mut r).unwrap();
    r.commit_to_registry().unwrap();

    // `uni.system.echo` is registered under the qname `builtin.system.echo`
    // (the framework's namespace prefix). Real procedures will use the
    // `uni.` prefix once M4 ports them through the framework's alias
    // resolution layer.
    let q = QName::new("builtin", "system.echo");
    assert!(registry.procedure(&q).is_some());
}

#[test]
fn builtin_register_is_idempotent_after_remove() {
    let registry = PluginRegistry::new();

    // First load.
    {
        let plugin = BuiltinPlugin::new();
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
        plugin.register(&mut r).unwrap();
        r.commit_to_registry().unwrap();
    }

    assert!(registry.scalar_fn(&QName::builtin("identity")).is_some());

    // Remove and re-load — should succeed without DuplicateRegistration.
    registry.remove_plugin(&uni_plugin::PluginId::new(BuiltinPlugin::ID));
    assert!(registry.scalar_fn(&QName::builtin("identity")).is_none());

    {
        let plugin = BuiltinPlugin::new();
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
        plugin.register(&mut r).unwrap();
        r.commit_to_registry().unwrap();
    }

    assert!(registry.scalar_fn(&QName::builtin("identity")).is_some());
}
