// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-plugin-host/src/triggers.rs:1555
//
// Persisted deferral rows are re-bound to trigger plugins by
// `subscription_name` (the FIRST LINE of the subscription docs) using
// `Iterator::find`, which returns the FIRST match. Two triggers that share
// the same first-line-of-docs (or both empty → "<unnamed trigger>") map to
// the same derived name, so a persisted item deferred by the *second*
// trigger is silently re-bound to the *first* on reload and fires against
// the wrong plugin.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use uni_common::{Value, Vid};
use uni_store::runtime::l0::L0Buffer;

use uni_plugin::errors::FnError;
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerDeferral, TriggerEventMask, TriggerOutcome,
    TriggerPhase, TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry};

use uni_plugin_host::triggers::{DeferralQueue, MutationEvents, TriggerRouter};

struct TestTrigger {
    sub: TriggerSubscription,
    defers: bool,
    /// Incremented whenever THIS trigger's deferred callback runs.
    deferred_fires: Arc<AtomicU64>,
}

impl TriggerPlugin for TestTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        if self.defers {
            Ok(TriggerOutcome::Defer {
                until: TriggerDeferral::from_payload("p"),
            })
        } else {
            Ok(TriggerOutcome::Continue)
        }
    }

    fn on_deferred(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
        _payload: &str,
    ) -> Result<TriggerOutcome, FnError> {
        self.deferred_fires.fetch_add(1, Ordering::SeqCst);
        Ok(TriggerOutcome::Continue)
    }
}

/// Both triggers share the SAME first-line-of-docs ("shared"), so
/// `subscription_name` derives the identical identifier for both.
fn sub() -> TriggerSubscription {
    TriggerSubscription {
        phase: TriggerPhase::BeforeCommit,
        events: TriggerEventMask::NODE_UPDATE,
        labels: None,
        edge_types: None,
        properties: None,
        predicate_source: None,
        fire_mode: FireMode::Synchronous,
        docs: "shared".to_owned(),
    }
}

#[test]
fn persisted_deferral_misroutes_to_first_colliding_trigger() {
    let tmp = tempfile::TempDir::new().unwrap();
    let data_path = tmp.path().to_path_buf();

    let a_fires = Arc::new(AtomicU64::new(0));
    let b_fires = Arc::new(AtomicU64::new(0));

    // A (first-registered) does NOT defer; B (second) DOES defer.
    let trigger_a = Arc::new(TestTrigger {
        sub: sub(),
        defers: false,
        deferred_fires: Arc::clone(&a_fires),
    });
    let trigger_b = Arc::new(TestTrigger {
        sub: sub(),
        defers: true,
        deferred_fires: Arc::clone(&b_fires),
    });

    let registry = Arc::new(PluginRegistry::new());
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(PluginId::new("test"), &caps, &registry);
    r.trigger(trigger_a as Arc<dyn TriggerPlugin>).unwrap();
    r.trigger(trigger_b as Arc<dyn TriggerPlugin>).unwrap();
    r.commit_to_registry().unwrap();

    // Phase 1: a persistence-backed queue receives B's deferral and
    // writes a sidecar row with name = subscription_name(B) = "shared".
    {
        let queue = DeferralQueue::with_persistence(data_path.clone());
        let router =
            TriggerRouter::from_registry_with_queue(&registry, Some(Arc::clone(&queue))).unwrap();

        let mut l0 = L0Buffer::new(0, None);
        let mut props = HashMap::new();
        props.insert("p".to_owned(), Value::Int(1));
        l0.insert_vertex(Vid::new(7), props);
        let events = MutationEvents::from_l0(&l0);

        let ctx = TriggerContext::new("session-1", 99);
        router.dispatch_before(ctx, &events).unwrap();
        assert_eq!(queue.pending(), 1, "B deferred exactly one item");
        // Sidecar row is written synchronously on push.
        assert!(queue.sidecar_path().unwrap().exists(), "sidecar persisted");
    }

    // Phase 2: simulate restart — fresh queue over the same data_path,
    // reload the persisted row and re-bind it to a trigger by name.
    let queue2 = DeferralQueue::with_persistence(data_path.clone());
    let restored = queue2.load_from_sidecar(&registry);
    assert_eq!(restored, 1, "one persisted item reloaded");

    queue2.tick();

    // FIXED: the persisted row carries B's (name, ordinal) = ("shared", 1), so
    // reload re-binds it to the SECOND same-named trigger (B) — its true origin.
    assert_eq!(
        a_fires.load(Ordering::SeqCst),
        0,
        "trigger A must not receive B's reloaded deferral"
    );
    assert_eq!(
        b_fires.load(Ordering::SeqCst),
        1,
        "trigger B — the original deferrer — must receive its own reloaded item"
    );
}
