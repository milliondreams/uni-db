#![allow(dead_code, unused_imports, clippy::all)]
//! M5b — verify that phased `SessionHook`s registered through the
//! plugin registry fire in addition to legacy per-session hooks.
//
// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use uni_db::api::Uni;
use uni_plugin::errors::HookOutcome;
use uni_plugin::traits::hook::{
    ExecuteContext, ParseContext, QueryMetrics, SessionHook as PluginSessionHook,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

#[derive(Default, Debug)]
struct CountingPhasedHook {
    parse_count: AtomicU32,
    execute_end_count: AtomicU32,
}

impl PluginSessionHook for CountingPhasedHook {
    fn on_parse(&self, _ctx: &ParseContext<'_>) -> HookOutcome {
        self.parse_count.fetch_add(1, Ordering::SeqCst);
        HookOutcome::Continue
    }
    fn on_execute_end(&self, _ctx: &ExecuteContext<'_>, _m: &QueryMetrics) {
        self.execute_end_count.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn registry_hooks_fire_via_session() {
    let uni = Uni::in_memory().build().await.expect("Uni::in_memory");

    // Register a phased hook directly into the live plugin registry
    // through a `PluginRegistrar`. The hook is `Arc`-cloned so the
    // test retains a handle to its counters.
    let counter: Arc<CountingPhasedHook> = Arc::new(CountingPhasedHook::default());
    let registry = uni.plugin_registry().clone();
    let plugin_id = PluginId::new("test_phased_hook");
    let caps = CapabilitySet::from_iter_of([Capability::Hook]);
    let mut r = PluginRegistrar::new(plugin_id, &caps, &registry);
    r.hook(counter.clone() as Arc<dyn PluginSessionHook>)
        .unwrap();
    r.commit_to_registry().unwrap();

    // Run a trivial query through a session.
    let session = uni.session();
    let _ = session.query("RETURN 1 AS n").await;

    assert!(
        counter.parse_count.load(Ordering::SeqCst) >= 1,
        "on_parse must fire at least once"
    );
    assert!(
        counter.execute_end_count.load(Ordering::SeqCst) >= 1,
        "on_execute_end must fire at least once"
    );
}
