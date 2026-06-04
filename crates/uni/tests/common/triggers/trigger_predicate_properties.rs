#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5f.1 acceptance — `predicate_source` may reference node/edge
//! properties via `n.<prop>` (post-mutation value) and `old.<prop>`
//! (pre-image). The trigger compiler rewrites these into accesses on
//! the per-row `properties_new` / `properties_old` event-row columns,
//! and `MutationEvents::from_l0_with_probe` materializes exactly the
//! referenced keys into those bags (predicate-gated cost).
//!
//! Coverage:
//! 1. `predicate_filters_on_new_property` — `n.balance > 100` fires
//!    only on rows whose new property value exceeds 100.
//! 2. `predicate_filters_on_value_change` — `old.balance <> n.balance`
//!    fires only on real value changes (no-op rewrite is suppressed).
//! 3. `predicate_on_edge_property` — predicate against an edge
//!    property fires on edge UPDATE events with the matching value.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use smol_str::SmolStr;
use uni_db::{DataType, Uni};
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, PluginId, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, Scope, SideEffects as PluginSideEffects,
};

/// Trigger that counts fired rows; subscription shape is configured
/// per test (label/edge filter, predicate, event mask).
struct CountingPropertyTrigger {
    sub: TriggerSubscription,
    counter: Arc<AtomicU64>,
}

impl CountingPropertyTrigger {
    fn node(events: TriggerEventMask, label: &str, predicate: &str) -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        let sub = TriggerSubscription {
            phase: TriggerPhase::AfterCommit,
            events,
            labels: Some(vec![SmolStr::new(label)]),
            edge_types: None,
            properties: None,
            predicate_source: Some(predicate.to_owned()),
            fire_mode: FireMode::Synchronous,
            docs: format!("predicate-prop trigger for label {label}"),
        };
        (
            Self {
                sub,
                counter: counter.clone(),
            },
            counter,
        )
    }

    fn edge(events: TriggerEventMask, edge_type: &str, predicate: &str) -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        let sub = TriggerSubscription {
            phase: TriggerPhase::AfterCommit,
            events,
            labels: None,
            edge_types: Some(vec![SmolStr::new(edge_type)]),
            properties: None,
            predicate_source: Some(predicate.to_owned()),
            fire_mode: FireMode::Synchronous,
            docs: format!("predicate-prop trigger for edge type {edge_type}"),
        };
        (
            Self {
                sub,
                counter: counter.clone(),
            },
            counter,
        )
    }
}

impl TriggerPlugin for CountingPropertyTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        let n = events.events.num_rows() as u64;
        self.counter.fetch_add(n, Ordering::SeqCst);
        Ok(TriggerOutcome::Continue)
    }
}

fn register_trigger(
    uni: &Uni,
    plugin_id: &str,
    trigger: Arc<dyn TriggerPlugin>,
) -> std::result::Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let _manifest = PluginManifest {
        id: PluginId::new(plugin_id),
        version: "1.0.0".parse().expect("static version parses"),
        abi: AbiRange::parse("^1").expect("ABI range valid"),
        depends_on: vec![],
        capabilities: caps.clone(),
        determinism: Determinism::Pure,
        side_effects: PluginSideEffects::ReadOnly,
        scope: Scope::Instance,
        hash: None,
        signature: None,
        provides: ProvidedSurfaces::default(),
        docs: format!("Test property-predicate trigger '{plugin_id}'."),
        metadata: std::collections::BTreeMap::new(),
    };
    let mut r = PluginRegistrar::new(PluginId::new(plugin_id), &caps, registry);
    r.trigger(trigger)?;
    r.commit_to_registry()?;
    Ok(())
}

async fn db_with_account_schema() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Account")
        .property("balance", DataType::Int64)
        .apply()
        .await?;
    Ok(db)
}

#[tokio::test]
async fn predicate_filters_on_new_property() -> Result<()> {
    let db = db_with_account_schema().await?;
    let (trigger, counter) = CountingPropertyTrigger::node(
        TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
        "Account",
        "n.balance > 100",
    );
    register_trigger(&db, "test-new-prop-gt", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Account {balance: 50})").await?;
    tx.execute("CREATE (:Account {balance: 150})").await?;
    tx.execute("CREATE (:Account {balance: 250})").await?;
    tx.commit().await?;

    // Only the two rows with balance > 100 should fire.
    let fired = counter.load(Ordering::SeqCst);
    assert_eq!(
        fired, 2,
        "predicate `n.balance > 100` must fire on exactly the 2 high-balance rows (got {fired})"
    );
    Ok(())
}

#[tokio::test]
async fn predicate_filters_on_value_change() -> Result<()> {
    let db = db_with_account_schema().await?;
    let (trigger, counter) = CountingPropertyTrigger::node(
        TriggerEventMask::NODE_UPDATE,
        "Account",
        "old.balance <> n.balance",
    );
    register_trigger(&db, "test-value-change", Arc::new(trigger))?;

    // tx 1: seed two accounts. NODE_CREATE — predicate scope excludes
    // creates so this should not fire (and old.balance is absent
    // anyway, so the `<>` comparison would short-circuit to NULL/false).
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Account {balance: 100})").await?;
    tx.execute("CREATE (:Account {balance: 200})").await?;
    tx.commit().await?;
    let after_create = counter.load(Ordering::SeqCst);
    assert_eq!(
        after_create, 0,
        "NODE_CREATE must not satisfy the UPDATE-only predicate (got {after_create})"
    );

    // tx 2: real change on one account, no-op rewrite on the other.
    // The trigger should fire exactly once (only on the real change).
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Account {balance: 100}) SET a.balance = 175")
        .await?;
    tx.execute("MATCH (a:Account {balance: 200}) SET a.balance = 200")
        .await?;
    tx.commit().await?;

    let fired = counter.load(Ordering::SeqCst);
    assert_eq!(
        fired, 1,
        "predicate `old.balance <> n.balance` must fire only on the real change (got {fired})"
    );
    Ok(())
}

#[tokio::test]
async fn predicate_on_edge_property() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Account")
        .property("balance", DataType::Int64)
        .edge_type("TRANSFER", &["Account"], &["Account"])
        .property("amount", DataType::Int64)
        .apply()
        .await?;

    let (trigger, counter) = CountingPropertyTrigger::edge(
        TriggerEventMask::EDGE_CREATE.union(TriggerEventMask::EDGE_UPDATE),
        "TRANSFER",
        "n.amount > 500",
    );
    register_trigger(&db, "test-edge-prop", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Account {balance: 1000})").await?;
    tx.execute("CREATE (:Account {balance: 2000})").await?;
    tx.commit().await?;

    // Two edges — one below threshold, one above.
    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (a:Account {balance: 1000}), (b:Account {balance: 2000}) \
         CREATE (a)-[:TRANSFER {amount: 100}]->(b)",
    )
    .await?;
    tx.execute(
        "MATCH (a:Account {balance: 1000}), (b:Account {balance: 2000}) \
         CREATE (a)-[:TRANSFER {amount: 750}]->(b)",
    )
    .await?;
    tx.commit().await?;

    let fired = counter.load(Ordering::SeqCst);
    assert_eq!(
        fired, 1,
        "edge predicate `n.amount > 500` must fire only on the 750-amount edge (got {fired})"
    );
    Ok(())
}
