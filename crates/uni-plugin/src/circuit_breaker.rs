//! Per-`(plugin_id, qname)` circuit breaker.
//!
//! Plugins that fail repeatedly in a hot inner loop should fail fast for
//! a cooldown
//! window rather than produce a deluge of identical errors. This
//! breaker opens after `N` consecutive failures and stays open for
//! `cooldown` before testing again.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::plugin::PluginId;
use crate::qname::QName;

/// Configuration for the circuit breaker.
#[derive(Clone, Copy, Debug)]
pub struct BreakerConfig {
    /// Consecutive failures before the breaker opens.
    pub failure_threshold: u32,
    /// How long the breaker stays open before re-testing.
    pub cooldown: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 10,
            cooldown: Duration::from_secs(30),
        }
    }
}

/// State of a breaker for one `(plugin, qname)` pair.
#[derive(Debug)]
struct BreakerState {
    consecutive_failures: AtomicU64,
    opened_at: RwLock<Option<Instant>>,
}

impl Default for BreakerState {
    fn default() -> Self {
        Self {
            consecutive_failures: AtomicU64::new(0),
            opened_at: RwLock::new(None),
        }
    }
}

/// Per-(plugin, qname) circuit breaker.
#[derive(Debug)]
pub struct CircuitBreaker {
    cfg: BreakerConfig,
    states: DashMap<(PluginId, QName), Arc<BreakerState>>,
}

impl CircuitBreaker {
    /// Construct a breaker with the given config.
    #[must_use]
    pub fn new(cfg: BreakerConfig) -> Self {
        Self {
            cfg,
            states: DashMap::new(),
        }
    }

    /// Check whether the breaker permits the call. Call before invoking
    /// the plugin; if `false`, fail fast without invocation.
    #[must_use]
    pub fn allow(&self, plugin: &PluginId, qname: &QName) -> bool {
        let key = (plugin.clone(), qname.clone());
        let Some(state) = self.states.get(&key) else {
            return true;
        };
        let opened_at = *state.opened_at.read();
        match opened_at {
            None => true,
            Some(t) => {
                if t.elapsed() >= self.cfg.cooldown {
                    // Cooldown elapsed — half-open: clear the trip and
                    // allow this call. The breaker re-opens if it fails.
                    *state.opened_at.write() = None;
                    state.consecutive_failures.store(0, Ordering::SeqCst);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Record a successful invocation; resets the failure counter.
    pub fn record_success(&self, plugin: &PluginId, qname: &QName) {
        let key = (plugin.clone(), qname.clone());
        if let Some(state) = self.states.get(&key) {
            state.consecutive_failures.store(0, Ordering::SeqCst);
            *state.opened_at.write() = None;
        }
    }

    /// Record a failed invocation; trips the breaker if the threshold is met.
    pub fn record_failure(&self, plugin: &PluginId, qname: &QName) {
        let key = (plugin.clone(), qname.clone());
        let state = self
            .states
            .entry(key)
            .or_insert_with(|| Arc::new(BreakerState::default()))
            .clone();
        let n = state.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        if n >= u64::from(self.cfg.failure_threshold) {
            let mut opened = state.opened_at.write();
            if opened.is_none() {
                *opened = Some(Instant::now());
            }
        }
    }

    /// Returns the current consecutive-failure count for diagnostics.
    #[must_use]
    pub fn failure_count(&self, plugin: &PluginId, qname: &QName) -> u64 {
        let key = (plugin.clone(), qname.clone());
        self.states
            .get(&key)
            .map_or(0, |s| s.consecutive_failures.load(Ordering::SeqCst))
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(BreakerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (CircuitBreaker, PluginId, QName) {
        (
            CircuitBreaker::new(BreakerConfig {
                failure_threshold: 3,
                cooldown: Duration::from_millis(50),
            }),
            PluginId::new("test"),
            QName::builtin("doomed"),
        )
    }

    #[test]
    fn fresh_breaker_allows_calls() {
        let (b, p, q) = fixture();
        assert!(b.allow(&p, &q));
    }

    #[test]
    fn breaker_opens_after_threshold_failures() {
        let (b, p, q) = fixture();
        for _ in 0..3 {
            b.record_failure(&p, &q);
        }
        assert!(!b.allow(&p, &q));
    }

    #[test]
    fn success_resets_failure_count() {
        let (b, p, q) = fixture();
        b.record_failure(&p, &q);
        b.record_failure(&p, &q);
        b.record_success(&p, &q);
        assert_eq!(b.failure_count(&p, &q), 0);
    }

    #[test]
    fn breaker_half_opens_after_cooldown() {
        let (b, p, q) = fixture();
        for _ in 0..3 {
            b.record_failure(&p, &q);
        }
        assert!(!b.allow(&p, &q));
        std::thread::sleep(Duration::from_millis(60));
        // After cooldown, allow returns true (half-open).
        assert!(b.allow(&p, &q));
        // Counter was reset.
        assert_eq!(b.failure_count(&p, &q), 0);
    }
}
