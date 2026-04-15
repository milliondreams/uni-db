// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use std::fmt::{Debug, Display};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;
use uni_common::config::ObjectStoreConfig;

#[derive(Debug)]
struct CircuitBreaker {
    failures: AtomicU32,
    last_failure: AtomicI64, // timestamp as millis
    threshold: u32,
    reset_timeout: Duration,
}

impl CircuitBreaker {
    fn new(threshold: u32, reset_timeout: Duration) -> Self {
        Self {
            failures: AtomicU32::new(0),
            last_failure: AtomicI64::new(0),
            threshold,
            reset_timeout,
        }
    }

    fn allow_request(&self) -> bool {
        let failures = self.failures.load(Ordering::Relaxed);
        if failures < self.threshold {
            return true;
        }

        let last = self.last_failure.load(Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        if (now - last) > self.reset_timeout.as_millis() as i64 {
            // Half-open: allow one request (or probabilistic)
            // Ideally we transition state. For simple Atomic impl, we allow retry after timeout.
            return true;
        }
        false
    }

    fn report_success(&self) {
        // Only reset if we were in a failure state to avoid contention?
        // Relaxed store is cheap.
        self.failures.store(0, Ordering::Relaxed);
    }

    fn report_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.last_failure.store(now, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct ResilientObjectStore {
    inner: Arc<dyn ObjectStore>,
    config: ObjectStoreConfig,
    cb: CircuitBreaker,
}

impl ResilientObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, config: ObjectStoreConfig) -> Self {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30)); // Hardcoded defaults for CB for now or use config?
        // We can use config.max_retries * 2 as threshold?
        // Let's use 5 failures and 30s reset.
        Self { inner, config, cb }
    }

    async fn retry<F, Fut, T>(&self, mut f: F, op_name: &str) -> StoreResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = StoreResult<T>>,
    {
        if !self.cb.allow_request() {
            return Err(object_store::Error::Generic {
                store: "ResilientObjectStore",
                source: Box::new(std::io::Error::other("Circuit breaker open")),
            });
        }

        let mut attempt = 0;
        let mut backoff = self.config.retry_backoff_base;

        loop {
            match f().await {
                Ok(val) => {
                    self.cb.report_success();
                    return Ok(val);
                }
                Err(e) => {
                    attempt += 1;
                    if attempt > self.config.max_retries {
                        self.cb.report_failure();
                        return Err(e);
                    }

                    // Check for non-retryable errors
                    let msg = e.to_string().to_lowercase();
                    if msg.contains("not found") || msg.contains("already exists") {
                        // Don't count 404 as failure for CB?
                        // Usually 404 is application level logic, not system failure.
                        // So we report success to CB? Or just ignore?
                        // Let's ignore it (don't report failure).
                        return Err(e);
                    }

                    warn!(
                        error = %e,
                        attempt,
                        operation = op_name,
                        "ObjectStore operation failed, retrying"
                    );

                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, self.config.retry_backoff_max);
                }
            }
        }
    }

    async fn timeout<F, Fut, T>(&self, f: F, duration: std::time::Duration) -> StoreResult<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = StoreResult<T>>,
    {
        tokio::time::timeout(duration, f())
            .await
            .map_err(|_| object_store::Error::Generic {
                store: "ResilientObjectStore",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "operation timed out",
                )),
            })?
    }
}

impl Display for ResilientObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResilientObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ResilientObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> StoreResult<PutResult> {
        let timeout = self.config.write_timeout;
        // Non-retryable
        // We still check CB logic?
        if !self.cb.allow_request() {
            return Err(object_store::Error::Generic {
                store: "ResilientObjectStore",
                source: Box::new(std::io::Error::other("Circuit breaker open")),
            });
        }

        let res = self
            .timeout(|| self.inner.put(location, payload), timeout)
            .await;
        match res {
            Ok(_) => self.cb.report_success(),
            Err(_) => self.cb.report_failure(), // Count timeout/error as failure
        }
        res
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let timeout = self.config.write_timeout;
        if !self.cb.allow_request() {
            return Err(object_store::Error::Generic {
                store: "ResilientObjectStore",
                source: Box::new(std::io::Error::other("Circuit breaker open")),
            });
        }
        let res = self
            .timeout(|| self.inner.put_opts(location, payload, opts), timeout)
            .await;
        match res {
            Ok(_) => self.cb.report_success(),
            Err(_) => self.cb.report_failure(),
        }
        res
    }

    async fn put_multipart(&self, location: &Path) -> StoreResult<Box<dyn MultipartUpload>> {
        self.put_multipart_opts(location, PutMultipartOptions::default())
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        let timeout = self.config.write_timeout;
        self.retry(
            || async {
                self.timeout(
                    || self.inner.put_multipart_opts(location, opts.clone()),
                    timeout,
                )
                .await
            },
            "put_multipart_opts",
        )
        .await
    }

    async fn get(&self, location: &Path) -> StoreResult<GetResult> {
        self.get_opts(location, GetOptions::default()).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let timeout = self.config.read_timeout;
        self.retry(
            || async {
                self.timeout(|| self.inner.get_opts(location, options.clone()), timeout)
                    .await
            },
            "get_opts",
        )
        .await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> StoreResult<Bytes> {
        let timeout = self.config.read_timeout;
        self.retry(
            || async {
                self.timeout(|| self.inner.get_range(location, range.clone()), timeout)
                    .await
            },
            "get_range",
        )
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        let timeout = self.config.read_timeout;
        self.retry(
            || async {
                self.timeout(|| self.inner.get_ranges(location, ranges), timeout)
                    .await
            },
            "get_ranges",
        )
        .await
    }

    async fn head(&self, location: &Path) -> StoreResult<ObjectMeta> {
        let timeout = self.config.read_timeout;
        self.retry(
            || async { self.timeout(|| self.inner.head(location), timeout).await },
            "head",
        )
        .await
    }

    async fn delete(&self, location: &Path) -> StoreResult<()> {
        let timeout = self.config.write_timeout;
        self.retry(
            || async { self.timeout(|| self.inner.delete(location), timeout).await },
            "delete",
        )
        .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        // Check CB? List returns stream.
        // We can check CB on initial call.
        if !self.cb.allow_request() {
            // How to return error stream?
            // We can return a stream that yields an error immediately.
            // But BoxStream signature expects Stream of Results.
            return futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "ResilientObjectStore",
                    source: Box::new(std::io::Error::other("Circuit breaker open")),
                })
            })
            .boxed();
        }

        // We wrap stream to report failure/success?
        // Too complex for P2. Just pass through.
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        if !self.cb.allow_request() {
            return futures::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "ResilientObjectStore",
                    source: Box::new(std::io::Error::other("Circuit breaker open")),
                })
            })
            .boxed();
        }
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        let timeout = self.config.read_timeout;
        self.retry(
            || async {
                self.timeout(|| self.inner.list_with_delimiter(prefix), timeout)
                    .await
            },
            "list_with_delimiter",
        )
        .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> StoreResult<()> {
        let timeout = self.config.write_timeout;
        self.retry(
            || async { self.timeout(|| self.inner.copy(from, to), timeout).await },
            "copy",
        )
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> StoreResult<()> {
        let timeout = self.config.write_timeout;
        self.retry(
            || async { self.timeout(|| self.inner.rename(from, to), timeout).await },
            "rename",
        )
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> StoreResult<()> {
        let timeout = self.config.write_timeout;
        self.retry(
            || async {
                self.timeout(|| self.inner.copy_if_not_exists(from, to), timeout)
                    .await
            },
            "copy_if_not_exists",
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    // ── CircuitBreaker unit tests ────────────────────────────────────

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        assert!(cb.allow_request());
        assert_eq!(cb.failures.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_cb_closed_to_open_after_threshold() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        for _ in 0..5 {
            cb.report_failure();
        }
        assert!(!cb.allow_request(), "CB should be open after 5 failures");
    }

    #[test]
    fn test_cb_stays_closed_below_threshold() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        for _ in 0..4 {
            cb.report_failure();
        }
        assert!(cb.allow_request(), "CB should stay closed with 4 failures");
    }

    #[test]
    fn test_cb_open_to_half_open_after_timeout() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(1));
        cb.report_failure();
        cb.report_failure();
        assert!(!cb.allow_request(), "CB should be open");

        std::thread::sleep(Duration::from_millis(5));
        assert!(
            cb.allow_request(),
            "CB should allow probe after reset timeout"
        );
    }

    #[test]
    fn test_cb_half_open_to_closed_on_success() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(1));
        cb.report_failure();
        cb.report_failure();
        std::thread::sleep(Duration::from_millis(5));

        // In half-open state, report success
        cb.report_success();
        assert_eq!(cb.failures.load(Ordering::Relaxed), 0);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_cb_half_open_to_open_on_failure() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(1));
        cb.report_failure();
        cb.report_failure();
        std::thread::sleep(Duration::from_millis(5));

        // In half-open state, report another failure
        cb.report_failure();
        assert!(
            !cb.allow_request(),
            "CB should be open again after failure in half-open"
        );
    }

    #[test]
    fn test_cb_success_resets_failures() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(30));
        cb.report_failure();
        cb.report_failure();
        cb.report_failure();
        assert_eq!(cb.failures.load(Ordering::Relaxed), 3);

        cb.report_success();
        assert_eq!(cb.failures.load(Ordering::Relaxed), 0);
    }

    // ── ResilientObjectStore integration tests ───────────────────────

    #[tokio::test]
    async fn test_resilient_store_retry_succeeds() {
        // InMemory store succeeds on first try — verify success path
        let inner = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let config = ObjectStoreConfig {
            connect_timeout: Duration::from_secs(5),
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(10),
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
        };
        let store = ResilientObjectStore::new(inner, config);

        // Write and read back
        let path = Path::from("test/key");
        store
            .put(&path, PutPayload::from_static(b"hello"))
            .await
            .unwrap();
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello");

        // CB should be healthy
        assert_eq!(store.cb.failures.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_resilient_store_circuit_open_rejects() {
        let inner = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let config = ObjectStoreConfig {
            connect_timeout: Duration::from_secs(5),
            max_retries: 1,
            retry_backoff_base: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(10),
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
        };
        let store = ResilientObjectStore::new(inner, config);

        // Manually trip the circuit breaker
        for _ in 0..5 {
            store.cb.report_failure();
        }

        // All operations should fail with circuit breaker error
        let err = store.get(&Path::from("any")).await.unwrap_err();
        assert!(
            err.to_string().contains("Circuit breaker open"),
            "Expected circuit breaker error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_resilient_store_not_found_skips_retry() {
        let inner = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let config = ObjectStoreConfig {
            connect_timeout: Duration::from_secs(5),
            max_retries: 3,
            retry_backoff_base: Duration::from_millis(1),
            retry_backoff_max: Duration::from_millis(10),
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
        };
        let store = ResilientObjectStore::new(inner, config);

        // Get a non-existent key — should return error but NOT increment CB
        let err = store.get(&Path::from("nonexistent")).await.unwrap_err();
        assert!(err.to_string().to_lowercase().contains("not found"));
        assert_eq!(
            store.cb.failures.load(Ordering::Relaxed),
            0,
            "Not-found errors should not count as CB failures"
        );
    }
}
