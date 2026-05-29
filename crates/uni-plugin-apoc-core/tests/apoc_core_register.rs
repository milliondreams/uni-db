//! Integration test verifying `ApocCorePlugin` registers its procedures.

use uni_plugin::{Plugin, PluginRegistrar, PluginRegistry, QName};
use uni_plugin_apoc_core::ApocCorePlugin;

fn install_plugin() -> PluginRegistry {
    let registry = PluginRegistry::new();
    let plugin = ApocCorePlugin::new();
    let manifest = plugin.manifest();
    let caps = manifest.capabilities.clone();
    let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &registry);
    plugin.register(&mut r).expect("register");
    r.commit_to_registry().expect("commit");
    registry
}

#[test]
fn bitwise_procedures_are_registered_under_apoc_core_namespace() {
    let registry = install_plugin();
    for local in [
        "bitwise.and",
        "bitwise.or",
        "bitwise.xor",
        "bitwise.not",
        "bitwise.shiftLeft",
        "bitwise.shiftRight",
    ] {
        let q = QName::new("apoc-core", local);
        assert!(
            registry.procedure(&q).is_some(),
            "expected {local} registered"
        );
    }
}

#[test]
fn manifest_id_is_apoc_core() {
    let p = ApocCorePlugin::new();
    assert_eq!(p.manifest().id.as_str(), ApocCorePlugin::ID);
}
