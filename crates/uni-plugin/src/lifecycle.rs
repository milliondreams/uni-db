//! Plugin lifecycle state machine.
//!
//! Per `docs/proposals/plugin_framework.md` §11.1 and
//! `docs/plans/plugin_framework_implementation.md` §4 M10, each plugin
//! moves through a defined lifecycle:
//!
//! ```text
//!   Loaded → Linked → Initialized → Active → Draining → Removed
//! ```
//!
//! The state machine is encapsulated here so hot reload (M10 cutover)
//! can drain in-flight references through `Arc::clone` semantics before
//! swapping in the new instance.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::plugin::PluginId;

/// Discrete lifecycle state.
///
/// The state machine is monotonic-forward: a plugin progresses from
/// `Loaded` through `Removed` and never moves backward. Hot reload
/// removes the old generation and starts a new one at `Loaded`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LifecycleState {
    /// Bytes/source ingested, manifest parsed.
    Loaded = 0,
    /// Capabilities negotiated, WIT linker configured, `register()` called.
    Linked = 1,
    /// `init()` ran successfully in dependency order.
    Initialized = 2,
    /// In the registry; visible to query planning and execution.
    Active = 3,
    /// Removed from new operations; in-flight tx may still hold Arcs.
    Draining = 4,
    /// All in-flight references released; resources freed.
    Removed = 5,
}

impl LifecycleState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Loaded,
            1 => Self::Linked,
            2 => Self::Initialized,
            3 => Self::Active,
            4 => Self::Draining,
            _ => Self::Removed,
        }
    }
}

/// Per-plugin lifecycle handle.
///
/// Constructed at `Uni::add_plugin` time; advanced through the states
/// by the loader. The state is held in an `AtomicU8` so wait-free
/// reads work from the query path (e.g., to short-circuit dispatch
/// against a draining plugin).
#[derive(Debug)]
pub struct PluginLifecycle {
    plugin: PluginId,
    state: AtomicU8,
}

impl PluginLifecycle {
    /// Construct in [`LifecycleState::Loaded`].
    #[must_use]
    pub fn new(plugin: PluginId) -> Self {
        Self {
            plugin,
            state: AtomicU8::new(LifecycleState::Loaded as u8),
        }
    }

    /// Current state.
    #[must_use]
    pub fn state(&self) -> LifecycleState {
        LifecycleState::from_u8(self.state.load(Ordering::SeqCst))
    }

    /// Plugin id this handle tracks.
    #[must_use]
    pub fn plugin(&self) -> &PluginId {
        &self.plugin
    }

    /// Advance to the next state. Returns the new state.
    ///
    /// Forward-only: invocation when already at [`LifecycleState::Removed`]
    /// stays at `Removed`.
    pub fn advance(&self) -> LifecycleState {
        let cur = self.state.load(Ordering::SeqCst);
        let next = match LifecycleState::from_u8(cur) {
            LifecycleState::Loaded => LifecycleState::Linked,
            LifecycleState::Linked => LifecycleState::Initialized,
            LifecycleState::Initialized => LifecycleState::Active,
            LifecycleState::Active => LifecycleState::Draining,
            LifecycleState::Draining => LifecycleState::Removed,
            LifecycleState::Removed => LifecycleState::Removed,
        };
        self.state.store(next as u8, Ordering::SeqCst);
        next
    }

    /// Force-set the state. Useful for tests and for unwinding a failed
    /// `register()` back to `Loaded`.
    pub fn set(&self, s: LifecycleState) {
        self.state.store(s as u8, Ordering::SeqCst);
    }

    /// Returns `true` if the plugin is in the `Active` state.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.state() == LifecycleState::Active
    }

    /// Returns `true` if the plugin is in `Draining` or `Removed`.
    #[must_use]
    pub fn is_winding_down(&self) -> bool {
        matches!(
            self.state(),
            LifecycleState::Draining | LifecycleState::Removed
        )
    }
}

/// Shared lifecycle handle suitable for capturing in plan / query closures.
pub type SharedLifecycle = Arc<PluginLifecycle>;

// =========================================================================
// M10: Epoch-fenced reload driver
// =========================================================================

/// Orchestrator for an atomic plugin reload — implements the
/// proposal's §11.2 epoch-fenced cutover.
///
/// The reload protocol:
///
/// 1. The host calls [`EpochFencedReload::begin_drain`] on the **old**
///    plugin's lifecycle. The state advances `Active → Draining`. New
///    operations no longer capture the old plugin; in-flight ops that
///    already captured an `Arc<dyn ScalarPluginFn>` continue against
///    the old instance.
/// 2. The host invokes the load path for the **new** plugin and walks
///    its lifecycle `Loaded → Linked → Initialized → Active`.
/// 3. The host calls [`EpochFencedReload::wait_for_drain`], polling
///    `Arc::strong_count` on the old plugin's lifecycle. When the
///    only outstanding reference is the framework's bookkeeping `Arc`,
///    every in-flight operation against the old plugin has completed.
/// 4. The host advances the old plugin's lifecycle to `Removed` via
///    [`EpochFencedReload::finalize`].
///
/// The driver is decoupled from per-kind reload discipline (per
/// proposal §11.2.1) — that lives in the host's storage / index /
/// background-job specific code. This driver provides the general
/// state-machine orchestration.
///
/// `EpochFencedReload` is intentionally synchronous (no `async fn`)
/// so it can be called from any context. For long-drain scenarios
/// (e.g., 10s wall-clock queries against a storage backend), the
/// host wraps the polling loop in its own async runtime.
#[derive(Debug)]
pub struct EpochFencedReload {
    /// Lifecycle of the plugin being drained.
    old: Arc<PluginLifecycle>,
}

impl EpochFencedReload {
    /// Construct a driver for an old-plugin lifecycle.
    #[must_use]
    pub fn new(old: Arc<PluginLifecycle>) -> Self {
        Self { old }
    }

    /// Begin draining the old plugin. Advances state `Active → Draining`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the old plugin is not in `Active` state — the
    /// caller can choose to retry or bail out depending on policy.
    pub fn begin_drain(&self) -> Result<(), DrainError> {
        if !self.old.is_active() {
            return Err(DrainError::NotActive {
                current: self.old.state(),
            });
        }
        // Single atomic advance Active → Draining.
        let new = self.old.advance();
        if new != LifecycleState::Draining {
            return Err(DrainError::UnexpectedTransition { reached: new });
        }
        Ok(())
    }

    /// Wait until the old plugin has been fully drained.
    ///
    /// Polls `Arc::strong_count(&self.old)` with the supplied poll
    /// interval. Returns when the count drops to `threshold` (the
    /// number of bookkeeping `Arc`s the framework holds), or
    /// `Err(DrainError::Timeout)` if `max_wait` elapses first.
    ///
    /// `threshold` is typically 1 (just the framework's own
    /// `EpochFencedReload::old` Arc); pass 2 if the host also keeps
    /// a side reference for diagnostics.
    ///
    /// # Errors
    ///
    /// Returns [`DrainError::Timeout`] if the strong-count doesn't
    /// drop to `threshold` within `max_wait`.
    pub fn wait_for_drain(
        &self,
        threshold: usize,
        poll_interval: std::time::Duration,
        max_wait: std::time::Duration,
    ) -> Result<(), DrainError> {
        let start = std::time::Instant::now();
        loop {
            let count = Arc::strong_count(&self.old);
            if count <= threshold {
                return Ok(());
            }
            if start.elapsed() >= max_wait {
                return Err(DrainError::Timeout {
                    waited: start.elapsed(),
                    strong_count: count,
                });
            }
            std::thread::sleep(poll_interval);
        }
    }

    /// Finalize the drain — advance to `Removed`.
    ///
    /// Typically called only after [`Self::wait_for_drain`] succeeds.
    /// Safe to call multiple times (idempotent at `Removed`).
    pub fn finalize(&self) {
        // Drive forward to Removed regardless of where we are.
        loop {
            match self.old.state() {
                LifecycleState::Removed => return,
                _ => {
                    self.old.advance();
                }
            }
        }
    }

    /// Shared access to the old plugin's lifecycle (e.g., for
    /// observability hooks reading the current state).
    #[must_use]
    pub fn old_lifecycle(&self) -> &Arc<PluginLifecycle> {
        &self.old
    }
}

/// Drain operation errors.
#[derive(Debug, thiserror::Error)]
pub enum DrainError {
    /// `begin_drain` called on a plugin not in `Active`.
    #[error("cannot drain: plugin is in {current:?}, not Active")]
    NotActive {
        /// The current lifecycle state.
        current: LifecycleState,
    },

    /// `advance()` from `Active` returned an unexpected state. Should
    /// never happen with the current state machine.
    #[error("unexpected lifecycle transition: reached {reached:?}")]
    UnexpectedTransition {
        /// State we landed in unexpectedly.
        reached: LifecycleState,
    },

    /// `wait_for_drain` timed out without the strong-count dropping.
    #[error(
        "drain timed out after {waited:?}; strong_count remained {strong_count} (threshold not reached)"
    )]
    Timeout {
        /// Elapsed wait time.
        waited: std::time::Duration,
        /// Final observed Arc strong count.
        strong_count: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_starts_at_loaded() {
        let l = PluginLifecycle::new(PluginId::new("x"));
        assert_eq!(l.state(), LifecycleState::Loaded);
    }

    #[test]
    fn advance_progresses_through_states() {
        let l = PluginLifecycle::new(PluginId::new("x"));
        assert_eq!(l.advance(), LifecycleState::Linked);
        assert_eq!(l.advance(), LifecycleState::Initialized);
        assert_eq!(l.advance(), LifecycleState::Active);
        assert!(l.is_active());
        assert_eq!(l.advance(), LifecycleState::Draining);
        assert!(l.is_winding_down());
        assert_eq!(l.advance(), LifecycleState::Removed);
        assert!(l.is_winding_down());
        // Already at Removed — stays put.
        assert_eq!(l.advance(), LifecycleState::Removed);
    }

    #[test]
    fn set_is_explicit_state_override() {
        let l = PluginLifecycle::new(PluginId::new("x"));
        l.set(LifecycleState::Active);
        assert!(l.is_active());
    }

    // ── EpochFencedReload tests ────────────────────────────────────

    #[test]
    fn epoch_drain_advances_state_from_active() {
        let l = Arc::new(PluginLifecycle::new(PluginId::new("x")));
        l.set(LifecycleState::Active);
        let driver = EpochFencedReload::new(Arc::clone(&l));
        driver.begin_drain().unwrap();
        assert_eq!(l.state(), LifecycleState::Draining);
    }

    #[test]
    fn epoch_drain_rejects_non_active_state() {
        let l = Arc::new(PluginLifecycle::new(PluginId::new("x")));
        // Default state is Loaded.
        let driver = EpochFencedReload::new(l);
        let err = driver.begin_drain().unwrap_err();
        match err {
            DrainError::NotActive { current } => {
                assert_eq!(current, LifecycleState::Loaded);
            }
            other => panic!("expected NotActive, got {other:?}"),
        }
    }

    #[test]
    fn wait_for_drain_returns_immediately_when_below_threshold() {
        let l = Arc::new(PluginLifecycle::new(PluginId::new("x")));
        l.set(LifecycleState::Active);
        let driver = EpochFencedReload::new(Arc::clone(&l));
        driver.begin_drain().unwrap();
        // The driver holds one Arc; `l` here holds another. With
        // threshold=2, count == threshold immediately.
        driver
            .wait_for_drain(
                2,
                std::time::Duration::from_millis(1),
                std::time::Duration::from_millis(100),
            )
            .expect("should drain immediately");
    }

    #[test]
    fn wait_for_drain_times_out_when_references_persist() {
        let l = Arc::new(PluginLifecycle::new(PluginId::new("x")));
        l.set(LifecycleState::Active);
        let extra = Arc::clone(&l);
        let driver = EpochFencedReload::new(Arc::clone(&l));
        driver.begin_drain().unwrap();
        // Now strong_count >= 3 (driver + l + extra). With threshold 1,
        // wait_for_drain can't succeed.
        let err = driver
            .wait_for_drain(
                1,
                std::time::Duration::from_millis(1),
                std::time::Duration::from_millis(20),
            )
            .unwrap_err();
        match err {
            DrainError::Timeout {
                waited: _,
                strong_count,
            } => {
                assert!(strong_count >= 3);
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
        drop(extra); // release the captured ref
    }

    #[test]
    fn finalize_advances_to_removed() {
        let l = Arc::new(PluginLifecycle::new(PluginId::new("x")));
        l.set(LifecycleState::Active);
        let driver = EpochFencedReload::new(Arc::clone(&l));
        driver.begin_drain().unwrap();
        driver.finalize();
        assert_eq!(l.state(), LifecycleState::Removed);
    }

    #[test]
    fn finalize_is_idempotent_at_removed() {
        let l = Arc::new(PluginLifecycle::new(PluginId::new("x")));
        l.set(LifecycleState::Removed);
        let driver = EpochFencedReload::new(Arc::clone(&l));
        driver.finalize();
        driver.finalize();
        assert_eq!(l.state(), LifecycleState::Removed);
    }

    #[test]
    fn epoch_fenced_reload_end_to_end() {
        // Realistic flow: an old plugin is Active; the host begins
        // draining, releases its non-bookkeeping references (simulated),
        // wait_for_drain succeeds, finalize moves to Removed.
        let l = Arc::new(PluginLifecycle::new(PluginId::new("plugin.geo")));
        l.set(LifecycleState::Active);

        // Host has its bookkeeping Arc + driver's Arc.
        let host_arc = Arc::clone(&l);
        let driver = EpochFencedReload::new(Arc::clone(&l));
        driver.begin_drain().expect("drain begin");

        // No extra references: strong_count is 3 (l, host_arc, driver.old).
        // Drop host_arc to simulate the host releasing.
        drop(host_arc);

        // Now strong_count == 2 (l, driver.old). With threshold=2 the
        // drain completes immediately.
        driver
            .wait_for_drain(
                2,
                std::time::Duration::from_millis(1),
                std::time::Duration::from_secs(1),
            )
            .expect("wait_for_drain");
        driver.finalize();
        assert_eq!(l.state(), LifecycleState::Removed);
    }
}
