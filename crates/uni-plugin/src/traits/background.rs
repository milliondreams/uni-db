//! Background-job provider plugins.
//!
//! Scheduled / periodic / fire-and-forget execution analogous to APOC's
//! `apoc.periodic.*` family. Jobs run on a host-owned scheduler; this
//! trait describes the job interface, not the scheduler itself. The
//! host-side scheduler (`uni/src/scheduler.rs`) is delivered as part of
//! M11.

// Rust guideline compliant

use std::time::Duration;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::errors::FnError;
use crate::qname::QName;

/// A background-job provider.
pub trait BackgroundJobProvider: Send + Sync {
    /// Static definition (schedule, concurrency, timeout, docs).
    fn definition(&self) -> &JobDefinition;

    /// Execute one run of the job.
    ///
    /// # Threading policy
    ///
    /// - **Driven from Tokio via `tokio::task::spawn_blocking`.** The
    ///   host scheduler runs this synchronous method on a blocking
    ///   worker thread so it never stalls the async runtime.
    /// - **Must not block the runtime directly.** If the job needs to
    ///   perform I/O, it must do so on the current (blocking) thread —
    ///   never call `block_on` against the host runtime from inside
    ///   `execute`.
    /// - **Must observe [`JobContext::cancel`] cooperatively.** Poll
    ///   [`CancellationToken::is_cancelled`] at every safe point
    ///   (between batches, before long compute, before issuing each
    ///   query). The scheduler trips the token on shutdown / reload /
    ///   explicit cancel; an unresponsive job stays alive until the
    ///   process exits.
    /// - **Errors propagate as [`FnError`].** Panics are caught at the
    ///   scheduler boundary and recorded as a failed run; they do not
    ///   crash the host.
    ///
    /// See `docs/PLUGIN_THREADING.md` for the long-form rationale.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on execution failure. The host's scheduler
    /// honors the [`JobDefinition::retry`] policy.
    fn execute(&self, ctx: JobContext<'_>) -> Result<JobOutcome, FnError>;
}

/// Static definition for a [`BackgroundJobProvider`].
#[derive(Clone, Debug)]
pub struct JobDefinition {
    /// Qualified job id.
    pub id: QName,
    /// When this job runs.
    pub schedule: Schedule,
    /// Concurrency cap for *this job* (independent of the plugin's overall
    /// concurrency limit, which is enforced by the scheduler).
    pub concurrency: ConcurrencyLimit,
    /// Per-run wall-clock cap.
    pub timeout: Duration,
    /// Retry policy on transient failure.
    pub retry: RetryPolicy,
    /// Markdown docs.
    pub docs: String,
}

/// When a background job runs.
///
/// Implements `Serialize`/`Deserialize` so durable persistence backends
/// (e.g. `SystemLabelSchedulerPersistence`) can round-trip the schedule
/// across restart. `SystemTime`, `Duration`, and `SmolStr` are all
/// serde-compatible out of the box.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Schedule {
    /// Fire once at the absolute instant given.
    Once(std::time::SystemTime),
    /// Repeat every `period` (uniform spacing).
    Periodic(Duration),
    /// Cron-style schedule (`"0 */15 * * * *"`).
    Cron(SmolStr),
    /// Only via explicit `uni.plugin.runJob('id')`.
    Manual,
}

impl Schedule {
    /// Compute the next instant at or after `from` that this schedule
    /// fires, or `None` if the schedule is exhausted (a `Once` whose
    /// instant has already passed) or the cron expression cannot be
    /// parsed.
    ///
    /// Used by the host scheduler driver
    /// ([`crate::scheduler::Scheduler::tick_at`]) to time-gate
    /// dispatch.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::{Duration, SystemTime};
    /// use uni_plugin::traits::background::Schedule;
    ///
    /// let now = SystemTime::now();
    /// let s = Schedule::Periodic(Duration::from_secs(10));
    /// let next = s.next_after(now).unwrap();
    /// assert!(next >= now + Duration::from_secs(10));
    ///
    /// // A Once whose instant has passed is exhausted.
    /// let past = now - Duration::from_secs(60);
    /// assert!(Schedule::Once(past).next_after(now).is_none());
    /// ```
    #[must_use]
    pub fn next_after(&self, from: std::time::SystemTime) -> Option<std::time::SystemTime> {
        use std::str::FromStr;
        match self {
            Schedule::Manual => Some(from),
            Schedule::Once(at) => (*at >= from).then_some(*at),
            Schedule::Periodic(every) => Some(from + *every),
            Schedule::Cron(expr) => {
                // Registration-time validation rejects malformed cron
                // expressions, but a persisted job's expression could
                // round-trip through storage and fail to re-parse here.
                // Log loudly so the operator notices, then return `None`
                // so the job is treated as "not currently due" rather
                // than silently lost. (`next_after`'s signature is
                // infallible because tons of call sites depend on it;
                // changing it is a separate, larger refactor.)
                let sched = match cron::Schedule::from_str(expr.as_str()) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(
                            target: "uni_plugin::scheduler",
                            cron_expr = %expr,
                            error = %e,
                            "Cron schedule failed to parse; job will not fire until \
                             the expression is fixed or the job is re-registered."
                        );
                        return None;
                    }
                };
                let from_chrono: chrono::DateTime<chrono::Utc> = from.into();
                sched
                    .after(&from_chrono)
                    .next()
                    .map(|t: chrono::DateTime<chrono::Utc>| t.into())
            }
        }
    }
}

/// Concurrency limit for one job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConcurrencyLimit {
    /// Never overlaps with itself.
    Exclusive,
    /// At most `N` concurrent runs.
    Bounded(u32),
    /// No limit.
    Unbounded,
}

/// Retry policy on transient failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetryPolicy {
    /// No retry; failure surfaces immediately.
    Never,
    /// Up to `max` attempts with `delay` between.
    FixedDelay {
        /// Maximum attempts (including the first).
        max: u32,
        /// Delay between attempts.
        delay: Duration,
    },
}

/// Outcome of one job execution.
#[derive(Debug)]
#[non_exhaustive]
pub enum JobOutcome {
    /// Job completed; no further work needed.
    Done,
    /// Job completed; reschedule to fire again after `delay`.
    DoneAndReschedule(Duration),
    /// Job failed; `retry` indicates whether retry-policy applies.
    Failed {
        /// Failure reason for telemetry.
        reason: String,
        /// `true` if retry-policy should be honored.
        retry: bool,
    },
}

/// Marker trait for the host's background-job execution facilities.
///
/// Concrete hosts (e.g., `uni-db`'s `SchedulerJobHost`) implement this
/// and expose typed accessors on the concrete type. Job providers
/// downcast via [`JobHost::as_any`] when they need host services like
/// the storage manager, plugin registry, or write-mode inner-query
/// execution.
///
/// Mirrors [`crate::traits::procedure::ProcedureHost`] — same
/// downcasting pattern, just per-job-context flavor.
pub trait JobHost: Send + Sync + std::any::Any + std::fmt::Debug {
    /// Returns the host as a downcastable `&dyn Any`.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Trigger a best-effort storage compaction.
    ///
    /// The built-in `uni.system.compaction` job calls this from its
    /// `execute()` body. The default impl is a no-op so test hosts
    /// don't have to implement storage access.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the host's storage manager surfaces a
    /// compaction failure.
    fn compact_storage(&self) -> Result<(), FnError> {
        Ok(())
    }

    /// Execute a write-mode Cypher statement against the host.
    ///
    /// The built-in `uni.system.ttl_sweep` job calls this with a
    /// `MATCH (n) WHERE n.__ttl < timestamp() DETACH DELETE n` body.
    /// The default impl returns an error so test hosts that don't
    /// wire write-mode Cypher can still load.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the host has not wired write-mode
    /// Cypher (default) or if the statement fails.
    fn execute_write_cypher(&self, _cypher: &str) -> Result<(), FnError> {
        Err(FnError::new(
            0xD10,
            "JobHost: write-mode Cypher not supported by this host",
        ))
    }
}

/// Per-run context.
#[derive(Debug)]
#[non_exhaustive]
pub struct JobContext<'a> {
    /// Information about the previous run, if any.
    pub last_run: Option<JobRunRecord>,
    /// Cooperative-cancel token — implementations check between work
    /// units to honor reload / shutdown.
    pub cancel: CancellationToken,
    /// Optional host services pointer. `None` in pure unit tests; the
    /// scheduler driver populates it with a concrete `dyn JobHost`
    /// (typically `uni-db::scheduler::SchedulerJobHost`).
    pub host: Option<&'a dyn JobHost>,
    /// Lifetime marker for session / config refs added later.
    pub _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> JobContext<'a> {
    /// Construct a fresh per-run context from a cancellation token
    /// and the previous run's record (if any).
    ///
    /// Out-of-crate callers (e.g., the host scheduler driver in
    /// `uni-db::scheduler`) use this constructor because
    /// [`JobContext`] is `#[non_exhaustive]` and cannot be built with
    /// a struct literal from outside this crate. Host services
    /// (storage, inner-query, etc.) are attached via
    /// [`Self::with_host`].
    #[must_use]
    pub fn new(cancel: CancellationToken, last_run: Option<JobRunRecord>) -> Self {
        Self {
            last_run,
            cancel,
            host: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Attach a host pointer for the run.
    #[must_use]
    pub fn with_host(mut self, host: &'a dyn JobHost) -> Self {
        self.host = Some(host);
        self
    }
}

/// Bookkeeping record of a prior run; persisted in `uni_system.background_jobs`.
#[derive(Clone, Debug)]
pub struct JobRunRecord {
    /// Run started at.
    pub started_at: std::time::SystemTime,
    /// Run finished at (or last activity, if still running).
    pub finished_at: std::time::SystemTime,
    /// Outcome — recorded as the variant name as a string for portability.
    pub outcome: String,
}

/// Cooperative cancellation token.
///
/// The scheduler creates one per run and trips it on shutdown / reload /
/// explicit cancel. Job implementations are responsible for checking the
/// token at safe points (sync polling via [`CancellationToken::is_cancelled`])
/// or, for async-aware bodies, awaiting [`CancellationToken::cancelled`].
///
/// §1.2 / Phase 6 consolidation: re-exported from
/// [`tokio_util::sync::CancellationToken`]. The previous hand-rolled
/// `Arc<AtomicBool>` token shipped only the sync `is_cancelled()` flag,
/// forcing the scheduler driver to poll. The upstream type adds an async
/// `cancelled().await` future, which lets the driver wrap dispatch in a
/// `tokio::select!` against the cancel signal and react immediately —
/// the sync API (`new`, `cancel`, `is_cancelled`, `Clone`, `Debug`,
/// `Default`) is preserved verbatim, so existing call sites compile
/// unchanged.
pub use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_token_round_trip() {
        let t = CancellationToken::new();
        assert!(!t.is_cancelled());
        t.cancel();
        assert!(t.is_cancelled());
    }

    #[test]
    fn cancel_token_clone_shares_state() {
        let t = CancellationToken::new();
        let u = t.clone();
        t.cancel();
        assert!(u.is_cancelled());
    }
}
