// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Durable [`SchedulerPersistence`] backend for `uni-db`.
//!
//! Mirrors the
//! [`crate::persistence::SystemLabelPersistence`] pattern but scoped
//! to scheduler job state. Writes are dual-routed:
//!
//! 1. **JSON sidecar** at `<data_path>/_system/background_jobs.json`
//!    — atomic write-then-rename, source-of-truth at startup
//!    (`load_all`). The sidecar lives next to
//!    `declared_plugins.json` under the same `_system/` reservation.
//! 2. **`_BackgroundJob` graph label** (best-effort) — issued via the
//!    shared [`crate::persistence::LazyCypherSink`] once
//!    `Uni::build` finishes wiring it. Gives operators
//!    `MATCH (j:_BackgroundJob) RETURN j` visibility without needing
//!    a separate introspection procedure.

// Rust guideline compliant

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use uni_plugin::qname::QName;
use uni_plugin::scheduler::{
    SchedulerJobRecord, SchedulerJobStatus, SchedulerPersistence, SchedulerPersistenceError,
};
use uni_plugin::traits::background::{CancellationToken, Schedule};

use crate::persistence::LazyCypherSink;
use uni_sidecar::VecSidecar;

/// JSON-encoded shape of a scheduler job row in the sidecar / system
/// label. Stable across restarts so the on-disk + on-graph forms are
/// the same.
///
/// `schedule` and `next_fire_at` are `#[serde(default)]` so sidecars
/// written by pre-M11-closure builds (which only carried
/// `qname`/`status`/`consecutive_failures`) keep deserializing. The
/// default for `schedule` is [`Schedule::Manual`], matching prior
/// behavior.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedJob {
    qname: String,
    status: String,
    consecutive_failures: u32,
    #[serde(default = "default_schedule")]
    schedule: Schedule,
    #[serde(default)]
    next_fire_at: Option<SystemTime>,
}

fn default_schedule() -> Schedule {
    Schedule::Manual
}

/// Durable [`SchedulerPersistence`] backed by a JSON sidecar.
///
/// The Cypher mirror is best-effort; failures are logged at debug.
#[derive(Debug)]
pub struct SystemLabelSchedulerPersistence {
    sidecar: VecSidecar<PersistedJob>,
    write_guard: parking_lot::Mutex<()>,
    cypher_sink: Arc<LazyCypherSink>,
}

impl SystemLabelSchedulerPersistence {
    /// Construct rooted at `data_path/_system/background_jobs.json`.
    #[must_use]
    pub fn new(data_path: impl Into<PathBuf>) -> Self {
        Self {
            sidecar: VecSidecar::new(data_path.into(), "background_jobs.json"),
            write_guard: parking_lot::Mutex::new(()),
            cypher_sink: Arc::new(LazyCypherSink::new()),
        }
    }

    /// Borrow the lazy Cypher sink so the host can wire it after
    /// `Uni::build` completes.
    #[must_use]
    pub fn cypher_sink(&self) -> &Arc<LazyCypherSink> {
        &self.cypher_sink
    }

    fn read_all(&self) -> Result<Vec<PersistedJob>, SchedulerPersistenceError> {
        self.sidecar
            .load()
            .map_err(|e| SchedulerPersistenceError::Backend(e.to_string()))
    }

    fn write_all(&self, rows: &[PersistedJob]) -> Result<(), SchedulerPersistenceError> {
        self.sidecar
            .store(rows)
            .map_err(|e| SchedulerPersistenceError::Backend(e.to_string()))
    }

    /// Serialize a read-modify-write against the sidecar: take the write
    /// guard, load the full row set, apply `f`, and persist the result
    /// atomically. Every mutating entry point routes through this so the
    /// lock → read → mutate → write skeleton lives in one place.
    fn mutate_rows<F>(&self, f: F) -> Result<(), SchedulerPersistenceError>
    where
        F: FnOnce(&mut Vec<PersistedJob>),
    {
        let _guard = self.write_guard.lock();
        let mut rows = self.read_all()?;
        f(&mut rows);
        self.write_all(&rows)
    }

    /// Best-effort Cypher mirror: issue `cypher` against the lazy sink,
    /// logging (but not propagating) a skip at debug. `context` labels the
    /// log line (e.g. `"cypher mirror skipped"`).
    fn mirror_cypher(&self, qname: &str, cypher: &str, context: &str) {
        if let Err(e) = self.cypher_sink.try_write_cypher(cypher) {
            tracing::debug!(
                qname = %qname,
                error = %e,
                "SystemLabelSchedulerPersistence: {context}",
            );
        }
    }

    fn upsert(
        &self,
        id: &QName,
        status: SchedulerJobStatus,
    ) -> Result<(), SchedulerPersistenceError> {
        let qname_str = id.to_string();
        let status_str = format!("{status:?}");
        self.mutate_rows(
            |rows| match rows.iter_mut().find(|r| r.qname == qname_str) {
                Some(existing) => existing.status = status_str.clone(),
                None => rows.push(PersistedJob {
                    qname: qname_str.clone(),
                    status: status_str.clone(),
                    consecutive_failures: 0,
                    schedule: Schedule::Manual,
                    next_fire_at: None,
                }),
            },
        )?;
        let cypher = format!(
            "MERGE (j:_BackgroundJob {{qname: '{q}'}}) SET j.status = '{s}'",
            q = qname_str.replace('\'', "''"),
            s = status_str.replace('\'', "''"),
        );
        self.mirror_cypher(&qname_str, &cypher, "cypher mirror skipped");
        Ok(())
    }
}

impl SchedulerPersistence for SystemLabelSchedulerPersistence {
    fn record_scheduled(
        &self,
        id: &QName,
        schedule: &Schedule,
    ) -> Result<(), SchedulerPersistenceError> {
        let qname_str = id.to_string();
        let next_fire_at = schedule.next_after(std::time::SystemTime::now());
        self.mutate_rows(
            |rows| match rows.iter_mut().find(|r| r.qname == qname_str) {
                Some(existing) => {
                    existing.schedule = schedule.clone();
                    existing.next_fire_at = next_fire_at;
                }
                None => rows.push(PersistedJob {
                    qname: qname_str.clone(),
                    status: format!("{:?}", SchedulerJobStatus::Pending),
                    consecutive_failures: 0,
                    schedule: schedule.clone(),
                    next_fire_at,
                }),
            },
        )
    }

    fn record_started(
        &self,
        id: &QName,
        _started_at: std::time::SystemTime,
    ) -> Result<(), SchedulerPersistenceError> {
        self.upsert(id, SchedulerJobStatus::Running)
    }

    fn record_finished(
        &self,
        id: &QName,
        _finished_at: std::time::SystemTime,
        success: bool,
    ) -> Result<(), SchedulerPersistenceError> {
        let status = if success {
            SchedulerJobStatus::Idle
        } else {
            SchedulerJobStatus::FailedRetrying
        };
        self.upsert(id, status)
    }

    fn cancel(&self, id: &QName) -> Result<(), SchedulerPersistenceError> {
        let qname_str = id.to_string();
        self.mutate_rows(|rows| rows.retain(|r| r.qname != qname_str))?;
        let cypher = format!(
            "MATCH (j:_BackgroundJob {{qname: '{q}'}}) DETACH DELETE j",
            q = qname_str.replace('\'', "''"),
        );
        self.mirror_cypher(&qname_str, &cypher, "cypher cancel mirror skipped");
        Ok(())
    }

    fn load_all(&self) -> Result<Vec<SchedulerJobRecord>, SchedulerPersistenceError> {
        let rows = self.read_all()?;
        let records: Vec<SchedulerJobRecord> = rows
            .into_iter()
            .filter_map(|r| {
                // The persisted `qname` is the dotted `namespace.local` form
                // (`QName::to_string`). Split on the *last* dot so a multi-dot
                // local part (e.g. `system.ttl_sweep`) stays intact as the
                // local segment. A qname with no dot can't be reconstructed into
                // a `(namespace, local)` pair, so the row is dropped — such rows
                // are never written by this backend (every `QName` it persists
                // is namespaced) and would only appear from hand-edited or
                // foreign-written sidecars.
                let (ns, local) = r.qname.rsplit_once('.')?;
                let qname = QName::new(ns, local);
                Some(SchedulerJobRecord {
                    id: qname,
                    // Restored jobs always re-enter as `Pending`: on restart the
                    // scheduler re-evaluates each schedule from scratch, so the
                    // persisted run-state (`Running`/`Idle`/...) is intentionally
                    // not resurrected — it would be stale across the restart.
                    status: SchedulerJobStatus::Pending,
                    next_fire_at: r.next_fire_at,
                    last_started_at: None,
                    last_finished_at: None,
                    consecutive_failures: r.consecutive_failures,
                    schedule: r.schedule,
                    cancel: CancellationToken::new(),
                })
            })
            .collect();
        Ok(records)
    }
}

/// Choose the appropriate [`SchedulerPersistence`] for a `Uni`
/// instance. Returns [`SystemLabelSchedulerPersistence`] for local-disk
/// paths and `None` (caller falls back to `MemoryPersistence`) for
/// remote / in-memory URIs.
#[must_use]
pub fn scheduler_persistence_for_data_path(
    data_path: Option<&std::path::Path>,
) -> (Arc<dyn SchedulerPersistence>, Option<Arc<LazyCypherSink>>) {
    match data_path {
        Some(path) => {
            let p = Arc::new(SystemLabelSchedulerPersistence::new(path.to_owned()));
            let sink = Arc::clone(p.cypher_sink());
            (p as Arc<dyn SchedulerPersistence>, Some(sink))
        }
        None => (Arc::new(uni_plugin::scheduler::MemoryPersistence), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn record_started_and_load_all_round_trip() {
        let tmp = TempDir::new().unwrap();
        let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
        let id = QName::new("uni", "system.ttl_sweep");
        p.record_started(&id, std::time::SystemTime::now())
            .expect("record_started");
        let loaded = p.load_all().expect("load_all");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id.to_string(), "uni.system.ttl_sweep");
    }

    #[test]
    fn cancel_removes_the_record() {
        let tmp = TempDir::new().unwrap();
        let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
        let id = QName::new("uni", "system.ttl_sweep");
        p.record_started(&id, std::time::SystemTime::now())
            .expect("record_started");
        p.cancel(&id).expect("cancel");
        assert!(p.load_all().expect("load_all").is_empty());
    }

    #[test]
    fn close_reopen_survives() {
        let tmp = TempDir::new().unwrap();
        let id = QName::new("uni", "system.ttl_sweep");
        {
            let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
            p.record_started(&id, std::time::SystemTime::now())
                .expect("record_started");
        }
        let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
        let loaded = p.load_all().expect("load_all");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id.to_string(), "uni.system.ttl_sweep");
    }

    #[test]
    fn scheduler_persistence_for_in_memory_returns_no_sink() {
        let (p, sink) = scheduler_persistence_for_data_path(None);
        assert!(sink.is_none());
        assert!(p.load_all().expect("load_all").is_empty());
    }

    #[test]
    fn scheduler_persistence_for_local_path_returns_sink() {
        let tmp = TempDir::new().unwrap();
        let (_p, sink) = scheduler_persistence_for_data_path(Some(tmp.path()));
        assert!(sink.is_some());
    }

    #[test]
    fn periodic_schedule_survives_restart() {
        let tmp = TempDir::new().unwrap();
        let id = QName::new("myorg", "nightly");
        let schedule = Schedule::Periodic(std::time::Duration::from_secs(60));
        {
            let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
            p.record_scheduled(&id, &schedule)
                .expect("record_scheduled");
        }
        let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
        let loaded = p.load_all().expect("load_all");
        assert_eq!(loaded.len(), 1);
        match &loaded[0].schedule {
            Schedule::Periodic(d) => assert_eq!(*d, std::time::Duration::from_secs(60)),
            other => panic!("expected Periodic, got {other:?}"),
        }
        assert!(
            loaded[0].next_fire_at.is_some(),
            "next_fire_at should round-trip for Periodic"
        );
    }

    #[test]
    fn cron_schedule_survives_restart() {
        let tmp = TempDir::new().unwrap();
        let id = QName::new("myorg", "hourly");
        let schedule = Schedule::Cron(smol_str::SmolStr::new("0 0 * * * *"));
        {
            let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
            p.record_scheduled(&id, &schedule)
                .expect("record_scheduled");
        }
        let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
        let loaded = p.load_all().expect("load_all");
        assert_eq!(loaded.len(), 1);
        match &loaded[0].schedule {
            Schedule::Cron(expr) => assert_eq!(expr.as_str(), "0 0 * * * *"),
            other => panic!("expected Cron, got {other:?}"),
        }
    }

    #[test]
    fn once_schedule_survives_restart() {
        let tmp = TempDir::new().unwrap();
        let id = QName::new("myorg", "oneoff");
        let fire_at = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
        let schedule = Schedule::Once(fire_at);
        {
            let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
            p.record_scheduled(&id, &schedule)
                .expect("record_scheduled");
        }
        let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
        let loaded = p.load_all().expect("load_all");
        assert_eq!(loaded.len(), 1);
        match &loaded[0].schedule {
            Schedule::Once(at) => assert_eq!(*at, fire_at),
            other => panic!("expected Once, got {other:?}"),
        }
    }

    #[test]
    fn legacy_sidecar_without_schedule_falls_back_to_manual() {
        // Simulate a sidecar written by a pre-closure build that only
        // carried `qname` / `status` / `consecutive_failures`. The
        // `#[serde(default)]` annotations on the new fields should
        // make this deserialize cleanly with `Schedule::Manual`.
        let tmp = TempDir::new().unwrap();
        let sidecar_dir = tmp.path().join("_system");
        std::fs::create_dir_all(&sidecar_dir).unwrap();
        std::fs::write(
            sidecar_dir.join("background_jobs.json"),
            r#"[{"qname":"uni.system.ttl_sweep","status":"Pending","consecutive_failures":0}]"#,
        )
        .unwrap();
        let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
        let loaded = p.load_all().expect("legacy sidecar loads");
        assert_eq!(loaded.len(), 1);
        assert!(matches!(loaded[0].schedule, Schedule::Manual));
        assert!(loaded[0].next_fire_at.is_none());
    }

    #[test]
    fn record_scheduled_updates_existing_row() {
        let tmp = TempDir::new().unwrap();
        let p = SystemLabelSchedulerPersistence::new(tmp.path().to_path_buf());
        let id = QName::new("myorg", "nightly");
        p.record_scheduled(&id, &Schedule::Periodic(std::time::Duration::from_secs(60)))
            .unwrap();
        // Re-register with a different schedule — should overwrite,
        // not duplicate.
        p.record_scheduled(
            &id,
            &Schedule::Periodic(std::time::Duration::from_secs(120)),
        )
        .unwrap();
        let loaded = p.load_all().expect("load_all");
        assert_eq!(loaded.len(), 1);
        match &loaded[0].schedule {
            Schedule::Periodic(d) => assert_eq!(*d, std::time::Duration::from_secs(120)),
            other => panic!("expected Periodic(120s), got {other:?}"),
        }
    }
}
