// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-plugin-host/src/triggers.rs:559
//
// `dispatch_before` enqueues `TriggerOutcome::Defer` into the DeferralQueue
// *before* the transaction commits. If the commit is then aborted — here by
// a later Synchronous trigger returning `Reject`, which makes
// `dispatch_before` return Err and the tx roll back — the deferred item is
// already queued and the background tick fires it with the mutation events
// of the never-committed transaction. This contradicts the module's own
// invariant that pre-commit work must not observe a tx that later aborts.

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

/// Configurable test trigger. `fire` returns the configured outcome;
/// `on_deferred` records that the deferred item actually fired (proving
/// the aborted tx was observed) and returns Continue so it does not
/// re-defer.
struct TestTrigger {
    sub: TriggerSubscription,
    fire_outcome: FireKind,
    deferred_fires: Arc<AtomicU64>,
}

#[derive(Clone, Copy)]
enum FireKind {
    Defer,
    Reject,
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
        match self.fire_outcome {
            FireKind::Defer => Ok(TriggerOutcome::Defer {
                until: TriggerDeferral::from_payload("p"),
            }),
            FireKind::Reject => Ok(TriggerOutcome::Reject {
                reason: "abort the tx".to_owned(),
            }),
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

fn sub(docs: &str) -> TriggerSubscription {
    TriggerSubscription {
        phase: TriggerPhase::BeforeCommit,
        events: TriggerEventMask::NODE_UPDATE,
        labels: None,
        edge_types: None,
        properties: None,
        predicate_source: None,
        fire_mode: FireMode::Synchronous,
        docs: docs.to_owned(),
    }
}

#[test]
fn deferred_trigger_fires_for_aborted_transaction() {
    // Trigger A defers; trigger B (registered after A) rejects.
    let a_deferred = Arc::new(AtomicU64::new(0));
    let trigger_a = Arc::new(TestTrigger {
        sub: sub("A"),
        fire_outcome: FireKind::Defer,
        deferred_fires: Arc::clone(&a_deferred),
    });
    let trigger_b = Arc::new(TestTrigger {
        sub: sub("B"),
        fire_outcome: FireKind::Reject,
        deferred_fires: Arc::new(AtomicU64::new(0)),
    });

    let registry = Arc::new(PluginRegistry::new());
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(PluginId::new("test"), &caps, &registry);
    r.trigger(trigger_a as Arc<dyn TriggerPlugin>).unwrap();
    r.trigger(trigger_b as Arc<dyn TriggerPlugin>).unwrap();
    r.commit_to_registry().unwrap();

    let queue = DeferralQueue::new();
    let router =
        TriggerRouter::from_registry_with_queue(&registry, Some(Arc::clone(&queue))).unwrap();

    // Build a mutation-event log with one vertex write (→ NODE_UPDATE).
    let mut l0 = L0Buffer::new(0, None);
    let mut props = HashMap::new();
    props.insert("p".to_owned(), Value::Int(1));
    l0.insert_vertex(Vid::new(7), props);
    let events = MutationEvents::from_l0(&l0);

    // Drive the before-commit dispatch: A defers (enqueued), then B
    // rejects → dispatch_before returns Err → the tx is aborted and
    // NOTHING is committed.
    let ctx = TriggerContext::new("session-1", 99);
    let result = router.dispatch_before(ctx, &events);
    assert!(
        result.is_err(),
        "the transaction must abort (trigger B rejected it)"
    );

    // FIXED: trigger A's deferral was buffered, not enqueued, and dropped when B
    // rejected — so nothing is queued for the aborted transaction.
    assert_eq!(
        queue.pending(),
        0,
        "an aborted transaction's deferral must not be enqueued"
    );

    // A background tick has nothing to fire.
    queue.tick();
    assert_eq!(
        a_deferred.load(Ordering::SeqCst),
        0,
        "no deferred trigger may fire for an aborted transaction"
    );
}
