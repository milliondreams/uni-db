#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Workstream E acceptance — `TriggerSubscription.predicate_source`
//! is compiled at router build and evaluated per-row in `filter_for`.
//!
//! Tests:
//! 1. **`predicate_filters_to_subset`** — `event_kind = 1` keeps only
//!    CREATEs, drops UPDATEs from the same commit.
//! 2. **`predicate_rejects_invalid_at_commit`** — malformed Cypher
//!    surfaces as a `TriggerRejected` error from `commit()`. (v1 ships
//!    the dep edge through commit, not registration — documented
//!    trade-off in `triggers.rs:21-46`.)
//! 3. **`predicate_unknown_column_rejected`** — references to columns
//!    outside the event-row schema (e.g. property refs) error
//!    cleanly. Confirms node-property projection is genuinely deferred
//!    to v1.1.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use smol_str::SmolStr;
use uni_db::{DataType, Uni, UniError};
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, PluginId, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, Scope, SideEffects as PluginSideEffects,
};

/// Counting trigger with an optional `predicate_source`.
struct PredicateTrigger {
    sub: TriggerSubscription,
    counter: Arc<AtomicU64>,
}

impl PredicateTrigger {
    fn new(predicate_source: Option<&str>) -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        let sub = TriggerSubscription {
            phase: TriggerPhase::AfterCommit,
            events: TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
            labels: Some(vec![SmolStr::new("_AuditMe")]),
            edge_types: None,
            properties: None,
            predicate_source: predicate_source.map(str::to_owned),
            fire_mode: FireMode::Synchronous,
            docs: "predicate-test-trigger".to_owned(),
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

impl TriggerPlugin for PredicateTrigger {
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
        docs: format!("Test predicate trigger '{plugin_id}'."),
        metadata: std::collections::BTreeMap::new(),
    };
    let mut r = PluginRegistrar::new(PluginId::new(plugin_id), &caps, registry);
    r.trigger(trigger)?;
    r.commit_to_registry()?;
    Ok(())
}

async fn db_with_audit_schema() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("_AuditMe")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    Ok(db)
}

#[tokio::test]
async fn predicate_filters_to_subset() -> Result<()> {
    let db = db_with_audit_schema().await?;
    // event_kind == 1 is NODE_CREATE per mask_to_discriminant.
    let (trigger, counter) = PredicateTrigger::new(Some("event_kind = 1"));
    register_trigger(&db, "test-predicate-subset", Arc::new(trigger))?;

    // Two CREATEs in tx 1.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.execute("CREATE (:_AuditMe {x: 2})").await?;
    tx.commit().await?;

    let after_creates = counter.load(Ordering::SeqCst);
    assert!(
        after_creates >= 2,
        "predicate matching CREATE should fire on the two new vertices (got {after_creates})"
    );

    // tx 2: update existing vertex — predicate should reject NODE_UPDATE.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:_AuditMe {x: 1}) SET n.x = 99").await?;
    tx.commit().await?;

    let after_updates = counter.load(Ordering::SeqCst);
    assert_eq!(
        after_updates, after_creates,
        "UPDATE must be filtered out by `event_kind = 1` predicate"
    );
    Ok(())
}

#[tokio::test]
async fn predicate_rejects_invalid_at_commit() -> Result<()> {
    let db = db_with_audit_schema().await?;
    // Garbage Cypher.
    let (trigger, _counter) = PredicateTrigger::new(Some("@@@ not a predicate @@@"));
    register_trigger(&db, "test-predicate-bad", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    let err = tx
        .commit()
        .await
        .expect_err("commit must reject an unparseable predicate");

    match err {
        UniError::TriggerRejected { reason, .. } => {
            assert!(
                reason.contains("predicate_source compile failed"),
                "error should be predicate-source compile failure: {reason}"
            );
        }
        other => panic!("expected TriggerRejected, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn predicate_unknown_column_rejected() -> Result<()> {
    let db = db_with_audit_schema().await?;
    // Property reference is out of scope in v1 — the event-row schema
    // has no `x` column. Compile must reject with a clear message.
    let (trigger, _counter) = PredicateTrigger::new(Some("x > 5"));
    register_trigger(&db, "test-predicate-prop", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    let err = tx
        .commit()
        .await
        .expect_err("commit must reject a predicate referencing unknown columns");

    match err {
        UniError::TriggerRejected { reason, .. } => {
            assert!(
                reason.contains("predicate_source compile failed"),
                "error should be a compile failure: {reason}"
            );
        }
        other => panic!("expected TriggerRejected, got {other:?}"),
    }
    Ok(())
}
