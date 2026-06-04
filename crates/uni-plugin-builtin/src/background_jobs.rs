// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Built-in [`BackgroundJobProvider`] registrations.
//!
//! Per proposal §4.19 the host ships three "always-on" maintenance
//! jobs that are part of the `uni-db` operational layer:
//!
//! - **`uni.system.ttl_sweep`** — periodically deletes nodes whose
//!   `__ttl` property has expired.
//! - **`uni.system.statistics_refresh`** — refreshes planner
//!   cardinality estimates so the optimizer's row counts track recent
//!   writes.
//! - **`uni.system.compaction`** — triggers Lance-level background
//!   compaction so old fragments are coalesced.
//!
//! Each is implemented as a thin `BackgroundJobProvider` whose
//! `execute()` body is currently a tracing-only stub. The real work
//! lands progressively as the supporting host services come online
//! (the M9 cutover unlocks write-mode Cypher for `ttl_sweep`; the
//! planner statistics refresh is gated on the M5 statistics surface;
//! Lance compaction is triggered through the storage manager). The
//! registrations exist today so the scheduler driver wires them up
//! end-to-end and so operators can `CALL uni.periodic.list()` and see
//! the built-ins enumerated.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use uni_plugin::QName;
use uni_plugin::errors::FnError;
use uni_plugin::registrar::PluginRegistrar;
use uni_plugin::traits::background::{
    BackgroundJobProvider, ConcurrencyLimit, JobContext, JobDefinition, JobOutcome, RetryPolicy,
    Schedule,
};

/// The three built-in maintenance jobs, dispatched through a single
/// [`BackgroundJobProvider`] impl.
///
/// Each variant carries no data; per-variant `JobDefinition`s are
/// constructed lazily and cached in process-global `OnceLock`s so the
/// trait's `definition()` accessor can hand out a stable reference.
#[derive(Clone, Copy, Debug)]
pub enum BuiltinJob {
    /// `uni.system.ttl_sweep` — delete nodes whose `__ttl` expired.
    TtlSweep,
    /// `uni.system.statistics_refresh` — refresh planner cardinalities.
    StatsRefresh,
    /// `uni.system.compaction` — trigger Lance background compaction.
    Compaction,
}

impl BuiltinJob {
    /// Every built-in job, in registration order.
    pub const ALL: &'static [Self] = &[Self::TtlSweep, Self::StatsRefresh, Self::Compaction];

    /// Qualified job id.
    #[must_use]
    pub fn qname(&self) -> QName {
        match self {
            Self::TtlSweep => QName::new("uni", "system.ttl_sweep"),
            Self::StatsRefresh => QName::new("uni", "system.statistics_refresh"),
            Self::Compaction => QName::new("uni", "system.compaction"),
        }
    }

    fn build_definition(&self) -> JobDefinition {
        match self {
            Self::TtlSweep => JobDefinition {
                id: self.qname(),
                schedule: Schedule::Periodic(Duration::from_secs(60)),
                concurrency: ConcurrencyLimit::Exclusive,
                timeout: Duration::from_secs(120),
                retry: RetryPolicy::Never,
                docs: "Delete nodes whose `__ttl` property is in the past.".to_owned(),
            },
            Self::StatsRefresh => JobDefinition {
                id: self.qname(),
                schedule: Schedule::Periodic(Duration::from_secs(300)),
                concurrency: ConcurrencyLimit::Exclusive,
                timeout: Duration::from_secs(60),
                retry: RetryPolicy::Never,
                docs: "Refresh planner cardinality estimates after recent writes.".to_owned(),
            },
            Self::Compaction => JobDefinition {
                id: self.qname(),
                schedule: Schedule::Periodic(Duration::from_secs(900)),
                concurrency: ConcurrencyLimit::Exclusive,
                timeout: Duration::from_secs(600),
                retry: RetryPolicy::Never,
                docs: "Trigger Lance background compaction for older fragments.".to_owned(),
            },
        }
    }

    /// Return the cached `JobDefinition` for this variant.
    fn definition_cached(&self) -> &'static JobDefinition {
        // One `OnceLock` per variant so each can hand back a `'static`
        // reference without re-allocating on every call.
        static TTL_SWEEP: OnceLock<JobDefinition> = OnceLock::new();
        static STATS_REFRESH: OnceLock<JobDefinition> = OnceLock::new();
        static COMPACTION: OnceLock<JobDefinition> = OnceLock::new();
        match self {
            Self::TtlSweep => TTL_SWEEP.get_or_init(|| self.build_definition()),
            Self::StatsRefresh => STATS_REFRESH.get_or_init(|| self.build_definition()),
            Self::Compaction => COMPACTION.get_or_init(|| self.build_definition()),
        }
    }
}

impl BackgroundJobProvider for BuiltinJob {
    fn definition(&self) -> &JobDefinition {
        self.definition_cached()
    }

    fn execute(&self, ctx: JobContext<'_>) -> Result<JobOutcome, FnError> {
        match self {
            Self::TtlSweep => ttl_sweep_execute(ctx),
            Self::StatsRefresh => stats_refresh_execute(ctx),
            Self::Compaction => compaction_execute(ctx),
        }
    }
}

fn ttl_sweep_execute(ctx: JobContext<'_>) -> Result<JobOutcome, FnError> {
    tracing::debug!(job = "uni.system.ttl_sweep", "fire");
    // M11 A.4: dispatch through the host's `execute_write_cypher`
    // hook. Without a JobHost (test fixtures, pre-`Uni::build`
    // registration), this is a tracing-debug no-op so the job
    // remains schedulable. The Cypher body deletes any node whose
    // `__ttl` numeric property is strictly less than the current
    // wall-clock millisecond timestamp.
    let Some(host) = ctx.host else {
        tracing::debug!(
            job = "uni.system.ttl_sweep",
            "no JobHost attached; skipping"
        );
        return Ok(JobOutcome::Done);
    };
    let cypher = "MATCH (n) WHERE n.__ttl < timestamp() DETACH DELETE n";
    if let Err(e) = host.execute_write_cypher(cypher) {
        tracing::warn!(
            job = "uni.system.ttl_sweep",
            error = %e,
            "execute_write_cypher failed",
        );
        return Ok(JobOutcome::Failed {
            reason: format!("ttl_sweep: {e}"),
            retry: true,
        });
    }
    Ok(JobOutcome::Done)
}

fn stats_refresh_execute(_ctx: JobContext<'_>) -> Result<JobOutcome, FnError> {
    tracing::debug!(job = "uni.system.statistics_refresh", "fire");
    Ok(JobOutcome::Done)
}

fn compaction_execute(ctx: JobContext<'_>) -> Result<JobOutcome, FnError> {
    tracing::debug!(job = "uni.system.compaction", "fire");
    // M11 A.3: dispatch through the host's `compact_storage` hook
    // when a `JobHost` is wired. Without a host (test fixtures,
    // pre-`Uni::build` registration), this is a no-op so the job
    // remains schedulable.
    let Some(host) = ctx.host else {
        tracing::debug!(
            job = "uni.system.compaction",
            "no JobHost attached; skipping"
        );
        return Ok(JobOutcome::Done);
    };
    if let Err(e) = host.compact_storage() {
        tracing::warn!(
            job = "uni.system.compaction",
            error = %e,
            "compact_storage failed",
        );
        return Ok(JobOutcome::Failed {
            reason: format!("compact_storage: {e}"),
            retry: true,
        });
    }
    Ok(JobOutcome::Done)
}

/// Register the three built-in maintenance jobs into the registrar.
///
/// Called from the "uni" plugin's registration block in
/// `crates/uni/src/api/mod.rs::register_builtin_plugins`. All three
/// jobs land in the `uni` namespace (qnames like
/// `uni.system.ttl_sweep`), so they cannot be registered from
/// `BuiltinPlugin` (whose plugin id is `builtin`) without bypassing
/// the qname / namespace check.
///
/// # Errors
///
/// Propagates any registrar error from
/// [`PluginRegistrar::background_job`].
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), uni_plugin::PluginError> {
    for job in BuiltinJob::ALL {
        r.background_job(Arc::new(*job))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_sweep_definition_qname() {
        let j = BuiltinJob::TtlSweep;
        assert_eq!(j.definition().id.namespace(), "uni");
        assert_eq!(j.definition().id.local(), "system.ttl_sweep");
        assert!(matches!(j.definition().schedule, Schedule::Periodic(_)));
    }

    #[test]
    fn statistics_refresh_definition_qname() {
        let j = BuiltinJob::StatsRefresh;
        assert_eq!(j.definition().id.namespace(), "uni");
        assert_eq!(j.definition().id.local(), "system.statistics_refresh");
    }

    #[test]
    fn compaction_definition_qname() {
        let j = BuiltinJob::Compaction;
        assert_eq!(j.definition().id.namespace(), "uni");
        assert_eq!(j.definition().id.local(), "system.compaction");
    }

    #[test]
    fn execute_returns_done_outcome() {
        let j = BuiltinJob::TtlSweep;
        let cancel = uni_plugin::traits::background::CancellationToken::new();
        let ctx = JobContext::new(cancel, None);
        let out = j.execute(ctx).expect("execute returns Ok");
        assert!(matches!(out, JobOutcome::Done));
    }

    /// Test fixture: a `JobHost` that records each `compact_storage` + `execute_write_cypher` call so we can verify the dispatch paths.
    #[derive(Debug, Default)]
    struct RecordingJobHost {
        compact_calls: std::sync::atomic::AtomicUsize,
        cyphers: parking_lot::Mutex<Vec<String>>,
    }

    impl uni_plugin::traits::background::JobHost for RecordingJobHost {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn compact_storage(&self) -> Result<(), FnError> {
            self.compact_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }

        fn execute_write_cypher(&self, cypher: &str) -> Result<(), FnError> {
            self.cyphers.lock().push(cypher.to_owned());
            Ok(())
        }
    }

    /// FU-6: when a `JobHost` is attached, the TTL-sweep variant
    /// dispatches a delete-where-expired Cypher body. Validates the
    /// integration shape (host wired, single statement issued) without
    /// requiring a live `Uni` instance.
    #[test]
    fn ttl_sweep_job_issues_delete_cypher_to_host() {
        let host = RecordingJobHost::default();
        let j = BuiltinJob::TtlSweep;
        let cancel = uni_plugin::traits::background::CancellationToken::new();
        let ctx = JobContext::new(cancel, None).with_host(&host);
        let out = j.execute(ctx).expect("execute returns Ok");
        assert!(matches!(out, JobOutcome::Done));
        let cyphers = host.cyphers.lock();
        assert_eq!(cyphers.len(), 1, "TtlSweep must issue exactly one cypher");
        assert!(
            cyphers[0].contains("DETACH DELETE"),
            "ttl_sweep cypher must DETACH DELETE expired nodes; got {:?}",
            cyphers[0]
        );
        assert!(
            cyphers[0].contains("__ttl"),
            "ttl_sweep cypher must filter on __ttl property; got {:?}",
            cyphers[0]
        );
    }

    /// FU-6: when the JobHost's `execute_write_cypher` surfaces an
    /// error, the TTL-sweep variant returns `JobOutcome::Failed` with
    /// `retry=true` so the scheduler re-fires on the next tick rather
    /// than burning a circuit-breaker failure on a transient error.
    #[test]
    fn ttl_sweep_job_returns_failed_retry_on_host_error() {
        #[derive(Debug, Default)]
        struct AlwaysFailHost;
        impl uni_plugin::traits::background::JobHost for AlwaysFailHost {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn execute_write_cypher(&self, _cypher: &str) -> Result<(), FnError> {
                Err(FnError::new(0xBAD, "synthetic host failure"))
            }
        }
        let host = AlwaysFailHost;
        let j = BuiltinJob::TtlSweep;
        let cancel = uni_plugin::traits::background::CancellationToken::new();
        let ctx = JobContext::new(cancel, None).with_host(&host);
        let out = j.execute(ctx).expect("execute returns Ok wrapping Failed");
        match out {
            JobOutcome::Failed { retry, reason } => {
                assert!(retry, "host-error must be retryable");
                assert!(
                    reason.contains("ttl_sweep"),
                    "reason must mention job; got {reason:?}"
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn compaction_job_calls_compact_storage_on_host() {
        let host = RecordingJobHost::default();
        let j = BuiltinJob::Compaction;
        let cancel = uni_plugin::traits::background::CancellationToken::new();
        let ctx = JobContext::new(cancel, None).with_host(&host);
        let out = j.execute(ctx).expect("execute returns Ok");
        assert!(matches!(out, JobOutcome::Done));
        assert_eq!(
            host.compact_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "Compaction must call JobHost::compact_storage"
        );
    }

    #[test]
    fn compaction_job_no_op_without_host() {
        let j = BuiltinJob::Compaction;
        let cancel = uni_plugin::traits::background::CancellationToken::new();
        let ctx = JobContext::new(cancel, None);
        let out = j.execute(ctx).expect("execute returns Ok");
        assert!(matches!(out, JobOutcome::Done));
    }
}
