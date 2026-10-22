// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The database's [`BulkEventSink`]: announces a visible bulk load on the
//! commit broadcaster, fires its create events at after-phase triggers, and
//! streams them to CDC. See `uni_bulk::events` for the load side.

use std::pin::Pin;
use std::sync::{Arc, Weak};

use uni_bulk::{BulkEventSink, BulkWriteKind, VisibleLoad};
use uni_plugin::traits::trigger::{TriggerContext, TriggerEventMask};

use crate::api::UniInner;
use crate::api::notifications::CommitNotification;
use crate::api::triggers::{TriggerRouter, tx_id_to_u64};

/// The session id a bulk load's announcement and trigger fires carry: a load
/// does not run in a session of its own.
const BULK_SESSION_ID: &str = "bulk-load";

/// [`BulkEventSink`] over a database inner. Holds it weakly: a bulk writer, or
/// the background re-drive of one, must not keep the database alive.
pub(crate) struct DbBulkEventSink {
    inner: Weak<UniInner>,
}

impl DbBulkEventSink {
    pub(crate) fn new(inner: &Arc<UniInner>) -> Self {
        Self {
            inner: Arc::downgrade(inner),
        }
    }
}

fn router(inner: &UniInner) -> Result<TriggerRouter, uni_common::UniError> {
    Ok(TriggerRouter::from_registry_with_queue(
        &inner.plugin_registry,
        Some(Arc::clone(&inner.defer_queue)),
        Arc::clone(&inner.ec_queue),
    )?
    .with_task_tracker(crate::shutdown::ShutdownHandle::clone(
        &inner.shutdown_handle,
    )))
}

impl BulkEventSink for DbBulkEventSink {
    fn blocking_trigger(&self, kind: BulkWriteKind, label_or_type: &str) -> Option<String> {
        let inner = self.inner.upgrade()?;
        let mask = match kind {
            BulkWriteKind::Vertex => TriggerEventMask::NODE_CREATE,
            BulkWriteKind::Edge => TriggerEventMask::EDGE_CREATE,
        };
        match router(&inner) {
            Ok(router) => router
                .synchronous_before_trigger_for(mask, label_or_type)
                .map(str::to_owned),
            // A trigger whose predicate does not compile fails every commit;
            // name it so the load is refused rather than loaded past it.
            Err(e) => Some(format!("<trigger registry: {e}>")),
        }
    }

    fn publish(
        &self,
        load: VisibleLoad,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let Some(inner) = self.inner.upgrade() else {
                return;
            };
            publish(&inner, load).await;
        })
    }
}

/// Announce `load` in commit order, then stream its events to after-phase
/// triggers and CDC, one bounded chunk at a time.
async fn publish(inner: &Arc<UniInner>, mut load: VisibleLoad) {
    let Some(writer) = inner.writer.as_ref() else {
        return;
    };
    let router = match router(inner) {
        Ok(r) => Some(r),
        Err(e) => {
            tracing::error!(
                load_id = %load.load_id,
                error = %e,
                "bulk load events: the trigger registry could not be read; no trigger fires for \
                 this load",
            );
            None
        }
    };
    let wants_triggers = router
        .as_ref()
        .is_some_and(TriggerRouter::has_after_triggers);
    let cdc_stream = if inner.plugin_registry.cdc_outputs_is_empty() {
        None
    } else {
        inner
            .cdc_runtime
            .as_ref()
            .and_then(|cdc| cdc.open_mutation_stream())
    };

    // The load's place in the commit order, and the version it is announced
    // at, are taken together under the writer's commit lock.
    let (ticket, version) = writer.sequence_announcement().await;
    let notif = CommitNotification {
        version,
        mutation_count: load.rows,
        labels_affected: load.labels.clone(),
        edge_types_affected: load.edge_types.clone(),
        rules_promoted: 0,
        timestamp: chrono::Utc::now(),
        tx_id: load.load_id.clone(),
        session_id: BULK_SESSION_ID.to_owned(),
        causal_version: load.write_version,
        mutations: None,
        mutations_failed: false,
        mutation_stream: cdc_stream.as_ref().map(|s| s.id()),
        dropped_before: 0,
    };
    let commit_tx = inner.commit_tx.clone();
    let notif = Arc::new(notif);
    ticket.publish(move || {
        let _ = commit_tx.send(notif);
    });

    if !wants_triggers && cdc_stream.is_none() {
        return;
    }
    let props_referenced = router
        .as_ref()
        .map(TriggerRouter::properties_referenced)
        .unwrap_or_default();
    let host = wants_triggers.then(|| trigger_host(inner, writer));
    let tx_id = tx_id_to_u64(&load.load_id);
    let runtime = tokio::runtime::Handle::current();
    let mut cdc_stream = cdc_stream;
    loop {
        let events = match load.reader.next_chunk(&props_referenced).await {
            Ok(Some(events)) => events,
            Ok(None) => break,
            Err(e) => {
                // Dropping the CDC stream unfinished halts CDC rather than
                // checkpointing past the rows it never got.
                tracing::error!(
                    load_id = %load.load_id,
                    error = %e,
                    "bulk load events: reading the load's rows back failed; triggers missed the \
                     rest of the load and CDC streams halt",
                );
                return;
            }
        };
        if let (Some(router), Some(host)) = (router.as_ref(), host.as_ref()) {
            let ctx = TriggerContext::new(BULK_SESSION_ID, tx_id).with_host(Arc::clone(host));
            router.dispatch_after(ctx, &events, &runtime);
        }
        if let Some(stream) = cdc_stream.as_ref() {
            match events.materialize_all() {
                Ok(Some(batch)) => {
                    if !stream.send(batch).await {
                        // CDC stopped reading (shutdown, or it fell behind and
                        // halted); nothing more to send it.
                        cdc_stream = None;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(
                        load_id = %load.load_id,
                        error = %e,
                        "bulk load events: a chunk's CDC batch could not be built; CDC streams \
                         halt rather than checkpoint past it",
                    );
                    cdc_stream = None;
                }
            }
        }
        if !wants_triggers && cdc_stream.is_none() {
            return;
        }
    }
    if let Some(stream) = cdc_stream {
        stream.finish().await;
    }
}

/// The write-enabled host a declared trigger's after-commit action runs
/// against, as a transaction commit builds it (WS-A): the load is in storage,
/// and the action's own writes land in main L0.
fn trigger_host(
    inner: &UniInner,
    writer: &Arc<uni_store::Writer>,
) -> Arc<dyn uni_plugin::traits::procedure::ProcedureHost> {
    let l0_ctx = uni_query::query::df_graph::L0Context {
        current_l0: Some(writer.l0_manager.get_current()),
        transaction_l0: None,
        pending_flush_l0s: writer.l0_manager.get_pending_flush(),
    };
    Arc::new(
        uni_query::query::executor::procedure_host::QueryProcedureHost::from_commit_parts(
            Arc::clone(&inner.storage),
            Arc::clone(&inner.properties),
            Some(Arc::clone(&inner.procedure_registry)),
            inner.xervo_runtime.clone(),
            l0_ctx,
            0,
        )
        .with_writer(Arc::clone(writer)),
    )
}
