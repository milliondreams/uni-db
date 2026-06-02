// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M11 FU-4 — change-data-capture (CDC) runtime.
//!
//! Drives every registered [`uni_plugin::traits::cdc::CdcOutputProvider`]
//! by subscribing to the commit broadcaster and converting each
//! `crate::notifications::CommitNotification` into a
//! [`uni_plugin::traits::cdc::CdcBatch`] delivered to every active
//! stream.
//!
//! ## Lifecycle
//!
//! - At `Uni::build` time, [`CdcRuntime::spawn`] takes a snapshot of
//!   the registered CDC providers, loads each provider's last
//!   committed LSN from the JSON-sidecar
//!   `<data_path>/_system/cdc_checkpoints.json`, and calls
//!   `provider.start(CdcStartContext { from_lsn })` to obtain a live
//!   [`uni_plugin::traits::cdc::CdcStream`]. The runtime spawns a
//!   tokio task that subscribes to the commit broadcaster and
//!   forwards each commit as a `CdcBatch` to every stream.
//! - Per-commit, after every stream has accepted the batch, the
//!   runtime calls `checkpoint()` on each stream and persists the
//!   returned LSN to the sidecar. On restart, providers resume from
//!   that LSN.
//! - On shutdown the runtime calls `shutdown()` on each stream and
//!   exits.
//!
//! ## v1 limitations
//!
//! `CdcBatch::mutations` ships as an empty single-row `RecordBatch`
//! today — the LSN advancement, ordering, and checkpoint round-trip
//! are the parts under test. Filling the batch with the actual
//! mutation rows uses the same machinery as
//! `crate::triggers::MutationEvents` and is tracked as a follow-up.

// Rust guideline compliant

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uni_plugin::PluginRegistry;
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcStartContext, CdcStream};

use crate::notifications::CommitNotification;
use crate::shutdown::ShutdownHandle;
use uni_sidecar::VecSidecar;

/// Per-provider checkpoint row written to the JSON sidecar.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistedCheckpoint {
    /// Provider name (`CdcOutputProvider::name()`).
    pub name: String,
    /// Last successfully-acknowledged LSN.
    pub last_lsn: u64,
}

/// JSON-sidecar checkpoint store at
/// `<data_path>/_system/cdc_checkpoints.json`.
#[derive(Clone, Debug)]
pub struct CdcCheckpointSidecar {
    sidecar: VecSidecar<PersistedCheckpoint>,
}

impl CdcCheckpointSidecar {
    /// Construct rooted at `<data_path>/_system/cdc_checkpoints.json`.
    #[must_use]
    pub fn new(data_path: PathBuf) -> Self {
        Self {
            sidecar: VecSidecar::new(data_path, "cdc_checkpoints.json"),
        }
    }

    /// Borrow the sidecar path (for diagnostics).
    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        self.sidecar.path()
    }

    /// Load all persisted checkpoints. Returns an empty vec if the
    /// sidecar doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on I/O or parse failure.
    pub fn load_all(&self) -> Result<Vec<PersistedCheckpoint>, String> {
        self.sidecar.load().map_err(|e| e.to_string())
    }

    /// Write the full checkpoint set atomically.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on I/O failure.
    pub fn write_all(&self, rows: &[PersistedCheckpoint]) -> Result<(), String> {
        self.sidecar.store(rows).map_err(|e| e.to_string())
    }

    /// Look up the persisted LSN for a single provider.
    #[must_use]
    pub fn lookup(&self, name: &str) -> Option<CdcLsn> {
        self.load_all()
            .ok()
            .and_then(|rows| rows.into_iter().find(|r| r.name == name))
            .map(|r| CdcLsn(r.last_lsn))
    }

    /// Replace a single provider's LSN, leaving other providers
    /// unchanged. Reads-modify-writes the full sidecar atomically.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on I/O or parse failure.
    pub fn write_one(&self, name: &str, lsn: CdcLsn) -> Result<(), String> {
        let mut rows = self.load_all()?;
        if let Some(row) = rows.iter_mut().find(|r| r.name == name) {
            row.last_lsn = lsn.0;
        } else {
            rows.push(PersistedCheckpoint {
                name: name.to_owned(),
                last_lsn: lsn.0,
            });
        }
        self.write_all(&rows)
    }
}

/// Wraps a live CDC stream with the provider's name and most-recent
/// committed LSN.
struct ActiveStream {
    name: String,
    stream: Box<dyn CdcStream>,
}

/// Resume `provider` from its persisted LSN and start its stream.
///
/// Returns the [`ActiveStream`] on success, or `None` (logged) on failure so
/// the caller skips it. Shared by [`CdcRuntime::spawn`] (`late = false`) and
/// [`CdcRuntime::discover_new_providers`] (`late = true`); the only difference
/// is the log wording.
fn start_stream(
    checkpoint: Option<&CdcCheckpointSidecar>,
    name: &str,
    provider: &Arc<dyn uni_plugin::traits::cdc::CdcOutputProvider>,
    late: bool,
) -> Option<ActiveStream> {
    let from_lsn = checkpoint.and_then(|c| c.lookup(name));
    match provider.start(CdcStartContext::new(from_lsn)) {
        Ok(stream) => {
            if late {
                tracing::info!(provider = %name, from_lsn = ?from_lsn, "CdcRuntime: late-registered provider started");
            } else {
                tracing::info!(provider = %name, from_lsn = ?from_lsn, "CdcRuntime: provider started");
            }
            Some(ActiveStream {
                name: name.to_owned(),
                stream,
            })
        }
        Err(e) => {
            if late {
                tracing::warn!(provider = %name, error = %e, "CdcRuntime: late-registered provider start failed");
            } else {
                tracing::warn!(provider = %name, error = %e, "CdcRuntime: provider start failed; skipping");
            }
            None
        }
    }
}

/// Host-side CDC runtime that drives every registered provider on
/// the commit broadcaster.
///
/// One per `Uni` instance. Constructed by [`Self::spawn`] in
/// `Uni::build`; the running background task exits when
/// `ShutdownHandle` signals shutdown.
pub struct CdcRuntime {
    /// Active streams keyed by provider name.
    streams: Arc<Mutex<Vec<ActiveStream>>>,
    /// Checkpoint sidecar (`None` when no local data path).
    checkpoint: Option<CdcCheckpointSidecar>,
    /// Shared plugin registry — consulted on every commit to discover
    /// providers registered *after* `Uni::build` returned (e.g., via
    /// `Uni::add_plugin`).
    registry: Arc<PluginRegistry>,
}

impl std::fmt::Debug for CdcRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.streams.lock().len();
        f.debug_struct("CdcRuntime")
            .field("active_streams", &count)
            .field(
                "checkpoint_path",
                &self.checkpoint.as_ref().map(|c| c.path().to_path_buf()),
            )
            .finish()
    }
}

impl CdcRuntime {
    /// Construct and spawn the CDC runtime.
    ///
    /// Snapshots every registered [`uni_plugin::traits::cdc::CdcOutputProvider`],
    /// resumes each from its last persisted LSN (via the sidecar at
    /// `<data_path>/_system/cdc_checkpoints.json`), and starts a tokio
    /// task that delivers each commit notification to every active
    /// stream.
    ///
    /// When no providers are registered, this is a no-op fast path —
    /// the background task is still spawned so dynamic
    /// `Uni::add_plugin` registrations land as a future improvement,
    /// but it currently subscribes once at startup.
    #[must_use]
    pub fn spawn(
        registry: &Arc<PluginRegistry>,
        commit_rx: broadcast::Receiver<Arc<CommitNotification>>,
        data_path: Option<PathBuf>,
        shutdown: &ShutdownHandle,
    ) -> Arc<Self> {
        let checkpoint = data_path.map(CdcCheckpointSidecar::new);

        let mut active: Vec<ActiveStream> = Vec::new();
        for (name, provider) in registry.cdc_outputs_snapshot() {
            if let Some(stream) = start_stream(checkpoint.as_ref(), name.as_str(), &provider, false)
            {
                active.push(stream);
            }
        }

        let runtime = Arc::new(Self {
            streams: Arc::new(Mutex::new(active)),
            checkpoint,
            registry: Arc::clone(registry),
        });

        // Spawn the driver task. When the broadcast channel sends an
        // Err (lagged or closed) we re-loop; on `recv` of an
        // `Arc<CommitNotification>` we forward.
        let runtime_clone = Arc::clone(&runtime);
        let mut commit_rx = commit_rx;
        let mut shutdown_rx = shutdown.subscribe();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.recv() => {
                        runtime_clone.shutdown_streams();
                        break;
                    }
                    next = commit_rx.recv() => match next {
                        Ok(notif) => runtime_clone.deliver_commit(&notif),
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                lagged = n,
                                "CdcRuntime: commit broadcaster lagged",
                            );
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        });
        shutdown.track_task(handle);

        runtime
    }

    /// Number of currently-active CDC streams (for diagnostics + tests).
    #[must_use]
    pub fn active_stream_count(&self) -> usize {
        self.streams.lock().len()
    }

    /// Borrow the checkpoint sidecar, if local-disk persistence is
    /// enabled. Used by tests to assert on persisted LSN.
    #[must_use]
    pub fn checkpoint_sidecar(&self) -> Option<&CdcCheckpointSidecar> {
        self.checkpoint.as_ref()
    }

    /// Discover any providers registered after `Uni::build` (e.g.,
    /// via `Uni::add_plugin`) and start a stream for each one. Called
    /// at the start of every `deliver_commit` so dynamic
    /// registrations don't miss any commits past the first.
    fn discover_new_providers(&self) {
        let snapshot = self.registry.cdc_outputs_snapshot();
        let mut streams = self.streams.lock();
        for (name, provider) in snapshot {
            if streams.iter().any(|s| s.name == name.as_str()) {
                continue;
            }
            if let Some(stream) =
                start_stream(self.checkpoint.as_ref(), name.as_str(), &provider, true)
            {
                streams.push(stream);
            }
        }
    }

    /// Convert a single [`CommitNotification`] into a [`CdcBatch`] and
    /// deliver it to every active stream, then checkpoint each
    /// stream and persist the new LSN to the sidecar.
    fn deliver_commit(&self, notif: &CommitNotification) {
        self.discover_new_providers();
        // FU-4: the broadcaster pre-materializes the mutation RecordBatch
        // when at least one `CdcOutputProvider` is registered (see
        // `Transaction::commit`). `None` here means either there were
        // zero rows or the broadcaster ran without CDC subscribers
        // (race: provider registered between the snapshot and now —
        // discover_new_providers above picks them up for the *next*
        // commit). Fall back to an empty batch matching the canonical
        // event-row schema so downstream filters see consistent
        // column types.
        let mutations = notif.mutations.clone().unwrap_or_else(|| {
            std::sync::Arc::new(arrow_array::RecordBatch::new_empty(
                crate::triggers::event_row_schema(),
            ))
        });
        let batch = CdcBatch {
            lsn_start: CdcLsn(notif.causal_version),
            lsn_end: CdcLsn(notif.version),
            mutations,
            commit_timestamp: SystemTime::now(),
        };

        let mut streams = self.streams.lock();
        for active in streams.iter_mut() {
            if let Err(e) = active.stream.deliver(&batch) {
                tracing::warn!(
                    provider = %active.name,
                    error = %e,
                    "CdcRuntime: deliver failed",
                );
                continue;
            }
            match active.stream.checkpoint() {
                Ok(lsn) => {
                    if let Some(sidecar) = &self.checkpoint
                        && let Err(e) = sidecar.write_one(&active.name, lsn)
                    {
                        tracing::debug!(
                            provider = %active.name,
                            error = %e,
                            "CdcRuntime: checkpoint write failed",
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    provider = %active.name,
                    error = %e,
                    "CdcRuntime: checkpoint failed",
                ),
            }
        }
    }

    /// Call `shutdown()` on every active stream and drop them.
    fn shutdown_streams(&self) {
        let mut streams = self.streams.lock();
        for active in streams.iter_mut() {
            if let Err(e) = active.stream.shutdown() {
                tracing::warn!(
                    provider = %active.name,
                    error = %e,
                    "CdcRuntime: shutdown failed",
                );
            }
        }
        streams.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn checkpoint_sidecar_round_trip() {
        let tmp = TempDir::new().unwrap();
        let s = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
        assert!(s.load_all().unwrap().is_empty());
        s.write_one("kafka", CdcLsn(42)).unwrap();
        s.write_one("pulsar", CdcLsn(7)).unwrap();
        let rows = s.load_all().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(s.lookup("kafka"), Some(CdcLsn(42)));
        assert_eq!(s.lookup("pulsar"), Some(CdcLsn(7)));
    }

    #[test]
    fn checkpoint_sidecar_survives_close_reopen() {
        let tmp = TempDir::new().unwrap();
        {
            let s = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
            s.write_one("kafka", CdcLsn(99)).unwrap();
        }
        let s2 = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
        assert_eq!(s2.lookup("kafka"), Some(CdcLsn(99)));
    }

    #[test]
    fn checkpoint_sidecar_overwrites_existing_provider() {
        let tmp = TempDir::new().unwrap();
        let s = CdcCheckpointSidecar::new(tmp.path().to_path_buf());
        s.write_one("kafka", CdcLsn(1)).unwrap();
        s.write_one("kafka", CdcLsn(2)).unwrap();
        s.write_one("kafka", CdcLsn(3)).unwrap();
        assert_eq!(s.lookup("kafka"), Some(CdcLsn(3)));
        assert_eq!(s.load_all().unwrap().len(), 1);
    }
}
