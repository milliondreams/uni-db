//! Per-plugin instance cache with a concurrency cap.
//!
//! One [`InstancePool`] per loaded plugin. **It does not reuse live
//! instances.** Every [`InstancePool::acquire`] constructs a *fresh*
//! instance via the loader-supplied factory; the factory is expected to
//! be cheap because the heavy artifacts (a compiled wasmtime `Component`
//! plus its `InstancePre`, or extism's prepared `Manifest`) are cached
//! by the loader and the factory only spins up a fresh `Store`+instance.
//!
//! Freshness per acquire is a *security* property, not just hygiene:
//!
//! - A reused `Store<HostState>` would leak guest linear memory,
//!   globals, and WASI context across unrelated invocations — a `Pure`
//!   function could carry state between two unrelated queries (bug #2).
//! - A trapped store recycled back into a warm pool would re-trap or
//!   read poisoned memory on its next use (bug #3).
//!
//! Re-instantiating per acquire closes both: fresh state every call, and
//! a trapped instance is simply dropped (its `Drop` decrements the live
//! counter) and never handed out again.
//!
//! What remains of the old pool is the **concurrency cap**:
//! `PoolConfig::max_instances` bounds how many instances may be live at
//! once (so a flood of concurrent UDF calls can't exhaust wasmtime
//! memory), enforced via the same CAS-guarded `live` counter the old
//! capacity check used. [`PoolMetrics`] keeps a sane meaning —
//! `misses` counts fresh constructions (every acquire), `hits` is now
//! always zero (no warm reuse), `exhausted` counts cap rejections,
//! `live` is the current in-flight count.
//!
//! Generic over both:
//!
//! - **`T`** — the per-invoke instance type (`extism::Plugin`, a
//!   wasmtime component instance wrapper, or a dummy in tests).
//! - **`E`** — the loader-specific error type. The factory returns
//!   `Result<T, E>`; `acquire` constructs `E` from a
//!   resource-exhaustion message via [`PoolResourceLimit`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// Per-pool configuration.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Maximum concurrent live instances.
    ///
    /// Bounds the wasmtime memory footprint. Default `4` matches the
    /// `Capability::ConcurrentInstances` default in the proposal. Acts
    /// as a concurrency semaphore: at most this many instances may be
    /// in flight at once.
    pub max_instances: usize,
    /// Retained for API compatibility; no longer pre-warms anything.
    ///
    /// Instances are now built fresh per [`InstancePool::acquire`] (so a
    /// reused store can't leak guest state across calls), so there is no
    /// warm pool to populate. The field stays so existing
    /// `PoolConfig { max_instances, warm_count }` construction sites keep
    /// compiling and downstream config surfaces keep their shape.
    pub warm_count: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_instances: 4,
            warm_count: 1,
        }
    }
}

/// Pool metrics surface — read by `host.metric_counter` host imports.
#[derive(Debug, Default)]
pub struct PoolMetrics {
    /// Warm-reuse hits. Always `0` since instances are never reused.
    pub hits: AtomicU64,
    /// Fresh constructions — one per successful acquire.
    pub misses: AtomicU64,
    /// Acquires that failed because `max_instances` was reached.
    pub exhausted: AtomicU64,
    /// Currently-live (in-flight) instances.
    pub live: AtomicU64,
}

/// Loader-error trait used by [`InstancePool::acquire`] to construct
/// the "pool at capacity" error.
///
/// Each loader implements this with one line:
///
/// ```ignore
/// impl uni_plugin_wasm_rt::PoolResourceLimit for ExtismError {
///     fn resource_limit(msg: String) -> Self { Self::ResourceLimit(msg) }
/// }
/// ```
pub trait PoolResourceLimit {
    /// Construct a "resource limit exceeded" instance from a diagnostic
    /// message. Called when the pool's `max_instances` is reached.
    #[must_use]
    fn resource_limit(msg: String) -> Self;
}

/// A per-plugin instance cache with a concurrency cap.
///
/// Generic over the per-invoke instance type `T` and the loader's error
/// type `E`. Production use: `InstancePool<extism::Plugin, ExtismError>`
/// or `InstancePool<ScalarPluginInstance, WasmError>`.
///
/// **Does not reuse instances** — every [`Self::acquire`] builds a fresh
/// one and every release drops it. See the module docs for why.
pub struct InstancePool<T, E>
where
    T: Send + 'static,
    E: PoolResourceLimit + Send + Sync + 'static,
{
    cfg: PoolConfig,
    factory: Mutex<Box<dyn Fn() -> Result<T, E> + Send + Sync>>,
    metrics: Arc<PoolMetrics>,
}

impl<T, E> std::fmt::Debug for InstancePool<T, E>
where
    T: Send + 'static,
    E: PoolResourceLimit + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstancePool")
            .field("cfg", &self.cfg)
            .field(
                "metrics.misses",
                &self.metrics.misses.load(Ordering::Relaxed),
            )
            .field("metrics.live", &self.metrics.live.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<T, E> InstancePool<T, E>
where
    T: Send + 'static,
    E: PoolResourceLimit + Send + Sync + 'static,
{
    /// Construct a pool that builds fresh instances via `factory`.
    ///
    /// `cfg.warm_count` is accepted for API compatibility but ignored:
    /// nothing is pre-warmed, because instances are never reused.
    ///
    /// # Errors
    ///
    /// This constructor is infallible in practice; the `E` in the return
    /// type is retained so the signature is stable across the refactor.
    pub fn new(
        cfg: PoolConfig,
        factory: impl Fn() -> Result<T, E> + Send + Sync + 'static,
    ) -> Result<Self, E> {
        let factory = Mutex::new(Box::new(factory) as Box<dyn Fn() -> Result<T, E> + Send + Sync>);
        Ok(Self {
            cfg,
            factory,
            metrics: Arc::new(PoolMetrics::default()),
        })
    }

    /// Acquire a *fresh* instance, honoring the concurrency cap.
    ///
    /// Reserves a live slot (CAS against `max_instances`), then builds a
    /// brand-new instance via the factory. No warm reuse — the returned
    /// instance has clean state. Releasing it (via [`PooledInstance`]'s
    /// drop) frees the slot.
    ///
    /// # Errors
    ///
    /// - `E::resource_limit(...)` when `max_instances` is reached.
    /// - Whatever the factory returns on construction failure.
    pub fn acquire(&self) -> Result<T, E> {
        // Reserve a live slot atomically. CAS-loop guarantees the
        // invariant `live <= max` even under concurrent acquirers.
        let max = self.cfg.max_instances as u64;
        loop {
            let live = self.metrics.live.load(Ordering::SeqCst);
            if live >= max {
                self.metrics.exhausted.fetch_add(1, Ordering::SeqCst);
                return Err(E::resource_limit(format!(
                    "instance pool at capacity ({} live)",
                    self.cfg.max_instances
                )));
            }
            if self
                .metrics
                .live
                .compare_exchange(live, live + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        // The slot is reserved; construct a fresh instance. If
        // construction fails, give the slot back.
        let inst = match (self.factory.lock())() {
            Ok(v) => v,
            Err(err) => {
                self.metrics.live.fetch_sub(1, Ordering::SeqCst);
                return Err(err);
            }
        };
        self.metrics.misses.fetch_add(1, Ordering::SeqCst);
        Ok(inst)
    }

    /// Release an instance, freeing its concurrency slot.
    ///
    /// The instance is dropped here (never recycled), so its `Drop` impl
    /// runs any cleanup. A trapped instance is therefore discarded, not
    /// handed back out.
    pub fn release(&self, inst: T) {
        drop(inst);
        self.metrics.live.fetch_sub(1, Ordering::SeqCst);
    }

    /// Snapshot the current metrics.
    #[must_use]
    pub fn metrics(&self) -> Arc<PoolMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Pool configuration, for diagnostics.
    #[must_use]
    pub fn config(&self) -> &PoolConfig {
        &self.cfg
    }
}

/// RAII handle to an instance acquired from an [`InstancePool`].
///
/// Holds the fresh instance and frees its concurrency slot on drop
/// (dropping the instance — never recycling it). Adapters use this to
/// make "acquire-call-drop" exception-safe: if the plugin call panics or
/// traps, the slot still frees and the (possibly poisoned) instance is
/// discarded.
pub struct PooledInstance<T, E>
where
    T: Send + 'static,
    E: PoolResourceLimit + Send + Sync + 'static,
{
    pool: Arc<InstancePool<T, E>>,
    inst: Option<T>,
}

impl<T, E> std::fmt::Debug for PooledInstance<T, E>
where
    T: Send + 'static,
    E: PoolResourceLimit + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledInstance")
            .field("has_inst", &self.inst.is_some())
            .finish_non_exhaustive()
    }
}

impl<T, E> PooledInstance<T, E>
where
    T: Send + 'static,
    E: PoolResourceLimit + Send + Sync + 'static,
{
    /// Acquire a fresh `PooledInstance` from the pool.
    ///
    /// # Errors
    ///
    /// Propagates [`InstancePool::acquire`].
    pub fn acquire(pool: Arc<InstancePool<T, E>>) -> Result<Self, E> {
        let inst = pool.acquire()?;
        Ok(Self {
            pool,
            inst: Some(inst),
        })
    }

    /// Mutable access to the instance.
    ///
    /// # Panics
    ///
    /// If called after [`Self::take`].
    pub fn get_mut(&mut self) -> &mut T {
        self.inst
            .as_mut()
            .expect("PooledInstance accessed after take/drop")
    }

    /// Consume the wrapper, returning the inner instance without freeing
    /// its concurrency slot via the pool.
    ///
    /// Retained for API compatibility. With per-invoke instances there is
    /// no "corrupted vs clean" distinction at the pool level (a dropped
    /// instance is always discarded), but `take` still moves the instance
    /// out and decrements the live counter so callers that need ownership
    /// keep working.
    pub fn take(mut self) -> T {
        let inst = self.inst.take().expect("PooledInstance already taken");
        self.pool.metrics.live.fetch_sub(1, Ordering::SeqCst);
        inst
    }
}

impl<T, E> Drop for PooledInstance<T, E>
where
    T: Send + 'static,
    E: PoolResourceLimit + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(inst) = self.inst.take() {
            // Always discards the instance and frees the slot — never
            // recycles, so a trapped store can't be handed out again.
            self.pool.release(inst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    enum TestErr {
        #[error("resource limit: {0}")]
        ResourceLimit(String),
    }

    impl PoolResourceLimit for TestErr {
        fn resource_limit(msg: String) -> Self {
            Self::ResourceLimit(msg)
        }
    }

    #[derive(Debug)]
    #[allow(dead_code)]
    struct Dummy(u32);

    type TestPool = InstancePool<Dummy, TestErr>;

    #[test]
    fn acquire_constructs_fresh_each_time() {
        let n = Arc::new(AtomicU64::new(0));
        let nc = Arc::clone(&n);
        let pool = TestPool::new(
            PoolConfig {
                max_instances: 4,
                warm_count: 1,
            },
            move || Ok(Dummy(nc.fetch_add(1, Ordering::SeqCst) as u32)),
        )
        .unwrap();

        // Nothing pre-warmed: live starts at zero.
        assert_eq!(pool.metrics.live.load(Ordering::SeqCst), 0);

        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        // Distinct fresh instances, both counted as misses (no warm reuse).
        assert_ne!(a.0, b.0);
        assert_eq!(pool.metrics.misses.load(Ordering::SeqCst), 2);
        assert_eq!(pool.metrics.hits.load(Ordering::SeqCst), 0);
        assert_eq!(pool.metrics.live.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn release_frees_the_slot() {
        let pool = Arc::new(
            TestPool::new(
                PoolConfig {
                    max_instances: 1,
                    warm_count: 0,
                },
                || Ok(Dummy(0)),
            )
            .unwrap(),
        );
        {
            let _h = PooledInstance::acquire(Arc::clone(&pool)).unwrap();
            assert_eq!(pool.metrics.live.load(Ordering::SeqCst), 1);
            // At capacity while held.
            assert!(PooledInstance::acquire(Arc::clone(&pool)).is_err());
        }
        // Slot freed on drop — acquirable again.
        assert_eq!(pool.metrics.live.load(Ordering::SeqCst), 0);
        let _h = PooledInstance::acquire(Arc::clone(&pool)).unwrap();
    }

    #[test]
    fn exhaustion_returns_resource_limit() {
        let pool = TestPool::new(
            PoolConfig {
                max_instances: 1,
                warm_count: 0,
            },
            || Ok(Dummy(0)),
        )
        .unwrap();
        let _held = pool.acquire().unwrap();
        let err = pool.acquire().unwrap_err();
        assert!(matches!(err, TestErr::ResourceLimit(_)));
        assert_eq!(pool.metrics.exhausted.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pooled_instance_take_does_not_double_free() {
        let pool = Arc::new(
            TestPool::new(
                PoolConfig {
                    max_instances: 2,
                    warm_count: 0,
                },
                || Ok(Dummy(7)),
            )
            .unwrap(),
        );
        let h = PooledInstance::acquire(Arc::clone(&pool)).unwrap();
        assert_eq!(pool.metrics.live.load(Ordering::SeqCst), 1);
        let taken = h.take();
        assert_eq!(taken.0, 7);
        // `take` decremented live; drop of `taken` does nothing extra.
        assert_eq!(pool.metrics.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn config_default_matches_proposal() {
        let c = PoolConfig::default();
        assert_eq!(c.max_instances, 4);
        assert_eq!(c.warm_count, 1);
    }

    /// The concurrency cap holds even under contention: at most
    /// `max_instances` acquires succeed concurrently; the rest get
    /// `resource_limit`. (The CAS-guarded `live` counter is the same one
    /// the old capacity check used.)
    #[test]
    fn concurrent_acquire_never_exceeds_max() {
        use std::sync::Barrier;
        use std::thread;

        const MAX: usize = 4;
        const THREADS: usize = 32;

        let pool = Arc::new(
            TestPool::new(
                PoolConfig {
                    max_instances: MAX,
                    warm_count: 0,
                },
                || Ok(Dummy(0)),
            )
            .unwrap(),
        );

        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let p = Arc::clone(&pool);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                p.acquire().ok()
            }));
        }

        let mut held = Vec::with_capacity(THREADS);
        for h in handles {
            if let Some(inst) = h.join().unwrap() {
                held.push(inst);
            }
        }

        assert_eq!(held.len(), MAX, "exactly max_instances must be live");
        assert_eq!(pool.metrics.live.load(Ordering::SeqCst), MAX as u64);
        assert_eq!(
            pool.metrics.exhausted.load(Ordering::SeqCst),
            (THREADS - MAX) as u64
        );
    }
}
