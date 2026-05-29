//! Generic pre-warmed instance pool for wasm-backed plugins.
//!
//! One pool per loaded plugin. Holds a fixed-size queue of warm
//! instances; `acquire` is a wait-free `pop` in steady state. Cold
//! first-call latency (10–100 ms wasmtime instantiation) amortizes to
//! pool-size `O(1)` cost once the pool is primed.
//!
//! Per the M6.shared lift, the pool is generic over both:
//!
//! - **`T`** — the pooled instance type (`extism::Plugin`,
//!   `wasmtime::component::Instance`, or a dummy in tests).
//! - **`E`** — the loader-specific error type. The factory returns
//!   `Result<T, E>`; `acquire` constructs `E` from a
//!   resource-exhaustion message via [`PoolResourceLimit`].
//!
//! Each loader supplies a one-line `impl PoolResourceLimit for ItsError`
//! so the pool can raise its capacity errors without knowing the loader's
//! error shape.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_queue::ArrayQueue;
use parking_lot::Mutex;

/// Per-pool configuration.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Maximum concurrent live instances.
    ///
    /// Bounds the wasmtime memory footprint. Default `4` matches the
    /// `Capability::ConcurrentInstances` default in the proposal.
    pub max_instances: usize,
    /// Number of instances eagerly instantiated at pool construction.
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
    /// Successful pool acquires (hit on warm instance).
    pub hits: AtomicU64,
    /// Cold-path acquires (constructed fresh).
    pub misses: AtomicU64,
    /// Acquires that failed because `max_instances` was reached.
    pub exhausted: AtomicU64,
    /// Currently-live instances (warm + checked-out).
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

/// A pool of pre-warmed instances for one plugin.
///
/// Generic over the pooled instance type `T` and the loader's error
/// type `E`. Production use: `InstancePool<extism::Plugin, ExtismError>`
/// or `InstancePool<wasmtime::component::Instance, WasmError>`.
pub struct InstancePool<T, E>
where
    T: Send + 'static,
    E: PoolResourceLimit + Send + Sync + 'static,
{
    cfg: PoolConfig,
    idle: ArrayQueue<T>,
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
            .field("idle.len", &self.idle.len())
            .field("metrics.hits", &self.metrics.hits.load(Ordering::Relaxed))
            .field(
                "metrics.misses",
                &self.metrics.misses.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl<T, E> InstancePool<T, E>
where
    T: Send + 'static,
    E: PoolResourceLimit + Send + Sync + 'static,
{
    /// Construct a pool that builds new instances via `factory`.
    ///
    /// Eagerly constructs `cfg.warm_count.min(cfg.max_instances)`
    /// instances at construction time so first-call latency is the
    /// pool's steady-state hit cost, not a fresh wasmtime compile.
    ///
    /// # Errors
    ///
    /// Propagates factory errors from initial warm-up.
    pub fn new(
        cfg: PoolConfig,
        factory: impl Fn() -> Result<T, E> + Send + Sync + 'static,
    ) -> Result<Self, E> {
        let idle = ArrayQueue::new(cfg.max_instances.max(1));
        let factory = Mutex::new(Box::new(factory) as Box<dyn Fn() -> Result<T, E> + Send + Sync>);
        let metrics = Arc::new(PoolMetrics::default());

        let pool = Self {
            cfg: cfg.clone(),
            idle,
            factory,
            metrics: Arc::clone(&metrics),
        };

        for _ in 0..cfg.warm_count.min(cfg.max_instances) {
            let inst = (pool.factory.lock())()?;
            let _ = pool.idle.push(inst);
            metrics.live.fetch_add(1, Ordering::SeqCst);
        }
        Ok(pool)
    }

    /// Acquire an instance from the pool.
    ///
    /// Pops a warm instance if available; otherwise constructs a new
    /// one if `live < max_instances`. Returns a loader-specific
    /// resource-limit error if the pool is at capacity.
    ///
    /// # Errors
    ///
    /// - `E::resource_limit(...)` when `max_instances` is reached.
    /// - Whatever the factory returns on cold-construction failure.
    pub fn acquire(&self) -> Result<T, E> {
        if let Some(inst) = self.idle.pop() {
            self.metrics.hits.fetch_add(1, Ordering::SeqCst);
            return Ok(inst);
        }
        // Reserve a live slot atomically. The previous form had a
        // check-then-act race between `load()` and `fetch_add()` that
        // let two concurrent acquirers both pass the capacity check and
        // briefly push `live` above `max_instances`.
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
        // The slot is reserved; construct the instance. If construction
        // fails, give the slot back so the next acquirer can try.
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

    /// Release an instance back to the pool.
    ///
    /// On overflow (race with reaper), the instance is dropped — its
    /// `Drop` impl is responsible for any cleanup.
    pub fn release(&self, inst: T) {
        if self.idle.push(inst).is_err() {
            self.metrics.live.fetch_sub(1, Ordering::SeqCst);
        }
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

    #[doc(hidden)]
    pub fn idle_len(&self) -> usize {
        self.idle.len()
    }
}

/// RAII wrapper acquired from an [`InstancePool`]: holds the instance
/// and returns it to the pool on drop.
///
/// Adapters use this to make "acquire-call-release" exception-safe — if
/// the plugin call panics, the instance still returns home (drop runs).
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

    /// Consume the wrapper, returning the inner instance and **not**
    /// releasing it to the pool. Use this if the instance is known to
    /// be corrupted (e.g., trapped on epoch interrupt).
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
    fn warmup_populates_idle_queue() {
        let n = Arc::new(AtomicU64::new(0));
        let nc = Arc::clone(&n);
        let pool = TestPool::new(
            PoolConfig {
                max_instances: 4,
                warm_count: 2,
            },
            move || Ok(Dummy(nc.fetch_add(1, Ordering::SeqCst) as u32)),
        )
        .unwrap();
        assert_eq!(pool.metrics.live.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn acquire_release_round_trip_counts_hits_and_misses() {
        let n = Arc::new(AtomicU64::new(0));
        let nc = Arc::clone(&n);
        let pool = TestPool::new(
            PoolConfig {
                max_instances: 2,
                warm_count: 1,
            },
            move || Ok(Dummy(nc.fetch_add(1, Ordering::SeqCst) as u32)),
        )
        .unwrap();

        let a = pool.acquire().unwrap();
        assert_eq!(pool.metrics.hits.load(Ordering::SeqCst), 1);

        let b = pool.acquire().unwrap();
        assert_eq!(pool.metrics.misses.load(Ordering::SeqCst), 1);

        pool.release(a);
        pool.release(b);

        let _ = pool.acquire().unwrap();
        assert_eq!(pool.metrics.hits.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn exhaustion_returns_resource_limit() {
        let pool = TestPool::new(
            PoolConfig {
                max_instances: 1,
                warm_count: 1,
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
    fn pooled_instance_releases_on_drop() {
        let n = Arc::new(AtomicU64::new(0));
        let nc = Arc::clone(&n);
        let pool = Arc::new(
            TestPool::new(
                PoolConfig {
                    max_instances: 2,
                    warm_count: 1,
                },
                move || Ok(Dummy(nc.fetch_add(1, Ordering::SeqCst) as u32)),
            )
            .unwrap(),
        );
        assert_eq!(pool.idle_len(), 1);
        {
            let _h = PooledInstance::acquire(Arc::clone(&pool)).unwrap();
            assert_eq!(pool.idle_len(), 0);
        }
        assert_eq!(pool.idle_len(), 1);
    }

    #[test]
    fn pooled_instance_take_does_not_release() {
        let n = Arc::new(AtomicU64::new(0));
        let nc = Arc::clone(&n);
        let pool = Arc::new(
            TestPool::new(
                PoolConfig {
                    max_instances: 2,
                    warm_count: 1,
                },
                move || Ok(Dummy(nc.fetch_add(1, Ordering::SeqCst) as u32)),
            )
            .unwrap(),
        );
        let h = PooledInstance::acquire(Arc::clone(&pool)).unwrap();
        let _taken = h.take();
        assert_eq!(pool.idle_len(), 0);
        assert_eq!(pool.metrics.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn config_default_matches_proposal() {
        let c = PoolConfig::default();
        assert_eq!(c.max_instances, 4);
        assert_eq!(c.warm_count, 1);
    }

    /// Regression: previously `acquire` was a check-then-increment
    /// (`load`, then conditional `fetch_add`) under SeqCst — two
    /// concurrent acquirers could both pass the capacity check and
    /// briefly push `live` above `max_instances`. The CAS-loop form
    /// guarantees the invariant `live <= max` even under contention.
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

        // Exactly MAX acquires must have succeeded; the rest must have
        // failed with `resource_limit`. The peak `live` count seen at
        // any point must never have exceeded MAX.
        assert_eq!(held.len(), MAX, "exactly max_instances must be live");
        assert_eq!(pool.metrics.live.load(Ordering::SeqCst), MAX as u64);
        assert_eq!(
            pool.metrics.exhausted.load(Ordering::SeqCst),
            (THREADS - MAX) as u64
        );
    }
}
