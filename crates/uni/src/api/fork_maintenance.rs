// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni-db host implementation for the `uni_fork::maintenance` background tasks.
//!
//! The reusable scheduling/shutdown skeleton lives in
//! `uni_fork::maintenance`; this module supplies the per-tick bodies that
//! touch [`UniInner`] (the TTL sweep and the fork-local index build). The host
//! holds a `Weak<UniInner>` so the tasks do not extend the database's
//! lifetime — each tick attempts to upgrade and no-ops if the database has
//! been dropped.

use std::sync::Weak;

use tracing::{debug, warn};
use uni_common::core::schema::IndexDefinition;
use uni_store::fork::ForkLocalIndexKind;

use super::{Uni, UniInner};

/// `Weak<UniInner>`-backed host for the fork-maintenance tasks.
pub(crate) struct ForkMaintenanceHostImpl {
    weak: Weak<UniInner>,
}

impl ForkMaintenanceHostImpl {
    pub(crate) fn new(weak: Weak<UniInner>) -> Self {
        Self { weak }
    }
}

#[async_trait::async_trait]
impl uni_fork::ForkMaintenanceHost for ForkMaintenanceHostImpl {
    async fn sweep_expired_forks(&self) {
        if let Err(e) = sweep_tick(&self.weak).await {
            warn!("fork sweeper tick failed: {e}");
        }
    }

    async fn build_fork_local_indexes(&self, threshold: u64) {
        if let Err(e) = index_builder_tick(&self.weak, threshold).await {
            warn!("fork index builder tick failed: {e}");
        }
    }
}

/// One TTL-sweeper tick: drop every fork whose `ttl_expires_at` is past now.
async fn sweep_tick(weak: &Weak<UniInner>) -> anyhow::Result<()> {
    let Some(inner) = weak.upgrade() else {
        // Database dropped; the next tick observes the same and shuts down
        // via the broadcast channel instead of looping.
        return Ok(());
    };
    let now = chrono::Utc::now();
    let expired = inner.fork_registry.list_expired(now).await;
    if expired.is_empty() {
        return Ok(());
    }
    // Re-wrap into a Uni handle so we can call the public cascade API.
    // `Uni { inner }` is a thin newtype; this does not duplicate state.
    let db = Uni { inner };
    for fork in expired {
        match db.drop_fork_cascade(&fork.name).await {
            Ok(()) => {
                debug!(fork = %fork.name, "swept expired fork");
            }
            Err(e) => {
                warn!(
                    fork = %fork.name,
                    "sweeper failed to drop expired fork (will retry next tick): {e}"
                );
            }
        }
    }
    Ok(())
}

/// One index-builder tick: for every active fork whose per-table fragment
/// count crosses `threshold`, build a fork-local `ScalarBtree` for any column
/// primary has a single-column scalar index on.
async fn index_builder_tick(weak: &Weak<UniInner>, threshold: u64) -> anyhow::Result<()> {
    let Some(inner) = weak.upgrade() else {
        return Ok(());
    };
    let active = inner.fork_registry.list_active().await;
    if active.is_empty() {
        return Ok(());
    }

    // Snapshot which (label, column) pairs primary has scalar indexes on. The
    // schema is shared across forks, so we read it once per tick.
    let scalar_index_columns: Vec<(String, String)> = inner
        .schema
        .schema()
        .indexes
        .iter()
        .filter_map(|idx| match idx {
            IndexDefinition::Scalar(cfg) if cfg.properties.len() == 1 => {
                Some((cfg.label.clone(), cfg.properties[0].clone()))
            }
            _ => None,
        })
        .collect();
    if scalar_index_columns.is_empty() {
        return Ok(());
    }

    for fork_info in active {
        // Only fire builds against forks that have a live UniInner — the build
        // path runs through the fork's storage manager.
        let Some(fork_inner_weak) = inner
            .fork_inners
            .get(&fork_info.id)
            .map(|e| e.value().clone())
        else {
            continue;
        };
        let Some(fork_inner) = fork_inner_weak.upgrade() else {
            continue;
        };
        let Some(scope) = fork_inner.storage.fork_scope().cloned() else {
            warn!(
                fork = %fork_info.name,
                "fork inner cache hit but storage has no fork scope; skipping",
            );
            continue;
        };
        // Optional flush so the build sees the latest committed rows. Errors
        // here are non-fatal — the build uses whatever's already on the branch
        // and the next tick can retry.
        if let Some(writer_lock) = &fork_inner.writer {
            let writer: &uni_store::Writer = writer_lock.as_ref();
            if let Err(e) = writer.flush_to_l1(None).await {
                warn!(
                    fork = %fork_info.name,
                    "pre-build flush failed; continuing with current branch tip: {e}"
                );
            }
        }
        let base_uri = fork_inner.storage.base_uri().to_string();
        for (label, column) in &scalar_index_columns {
            let dataset_name = format!("vertices_{label}");
            let count = scope.fragment_count(&dataset_name);
            if count < threshold {
                continue;
            }
            if scope.fork_local_index(label, column).is_some() {
                continue;
            }
            match uni_store::fork::index_builder::build_fork_local_index(
                &scope,
                &base_uri,
                label,
                column,
                ForkLocalIndexKind::ScalarBtree,
            )
            .await
            {
                Ok(()) => {
                    debug!(
                        fork = %fork_info.name,
                        label = %label,
                        column = %column,
                        "auto-built fork-local scalar index"
                    );
                }
                Err(e) => {
                    warn!(
                        fork = %fork_info.name,
                        label = %label,
                        column = %column,
                        "fork-local scalar index build failed (will retry next tick): {e}"
                    );
                }
            }
        }
    }
    Ok(())
}
