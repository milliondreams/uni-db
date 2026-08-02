// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shared fault-injecting `ObjectStore` wrapper used by correctness repros.
//!
//! Wraps an inner store and delegates every operation, except it can be armed
//! to fail `get`/`put` with a non-`NotFound` `Generic` error — modeling a
//! transient object-store I/O blip (permission/timeout/list failure) rather
//! than a genuinely-absent object. Several load/save paths collapse *any*
//! error into "empty"/"absent", which this store lets us observe.

#![allow(dead_code)]

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Which `object_store::Error` variant an armed fault produces.
///
/// The distinction matters because retry layers classify by variant: `Generic`
/// is retryable no matter what its message says, while `NotFound` /
/// `AlreadyExists` are terminal no matter what their message says. A test that
/// can only inject one of the two cannot tell a correct classifier from a
/// substring-matching one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FaultKind {
    /// `Generic` — a transient blip. Retryable. Message overridable via
    /// [`FaultStore::set_transient_message`].
    #[default]
    Transient,
    /// Typed `NotFound`. Terminal.
    NotFound,
    /// Typed `AlreadyExists` — what a conditional PUT reports on an OCC
    /// conflict. Terminal.
    AlreadyExists,
}

pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    fail_get: AtomicBool,
    fail_get_remaining: AtomicUsize,
    fail_put_remaining: AtomicUsize,
    /// Every `get_opts` / `put_opts` that reaches this store, armed or not.
    ///
    /// This is what distinguishes "the classifier returned early" from "the
    /// retry budget was exhausted" — both surface the same error to the caller,
    /// and only the attempt count tells them apart.
    get_attempts: AtomicUsize,
    put_attempts: AtomicUsize,
    /// Which variant an armed fault produces. See [`FaultKind`].
    fault_kind: std::sync::Mutex<FaultKind>,
    /// Per-attempt fault script for GET, consumed front-to-back.
    ///
    /// Needed to place a *specific* variant on a *specific* attempt — e.g. a
    /// terminal error arriving on the last attempt of a retry budget, which is
    /// the only position that distinguishes "classify before spending an
    /// attempt" from "classify after".
    get_script: std::sync::Mutex<std::collections::VecDeque<FaultKind>>,
    /// When set, a [`FaultKind::Transient`] fault uses this text instead of the
    /// default. Lets a test inject a *transient* error whose message happens to
    /// read like a missing object — a proxy 404 body, or a wrapped S3
    /// "bucket not found" — which is what defeats substring-based NotFound
    /// detection.
    transient_message: std::sync::Mutex<Option<String>>,
}

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            fail_get: AtomicBool::new(false),
            fail_get_remaining: AtomicUsize::new(0),
            fail_put_remaining: AtomicUsize::new(0),
            get_attempts: AtomicUsize::new(0),
            put_attempts: AtomicUsize::new(0),
            fault_kind: std::sync::Mutex::new(FaultKind::default()),
            get_script: std::sync::Mutex::new(std::collections::VecDeque::new()),
            transient_message: std::sync::Mutex::new(None),
        }
    }

    /// Arm/disarm persistent GET failure (survives retry loops).
    pub fn set_fail_get(&self, on: bool) {
        self.fail_get.store(on, Ordering::SeqCst);
    }

    /// Fail the next `n` GET operations, then heal.
    ///
    /// Unlike [`FaultStore::set_fail_get`] this models a *recoverable* blip, so
    /// a retry layer that correctly classifies the error is expected to succeed.
    pub fn fail_next_gets(&self, n: usize) {
        self.fail_get_remaining.store(n, Ordering::SeqCst);
    }

    /// Fail the next `n` PUT operations, then heal.
    pub fn fail_next_puts(&self, n: usize) {
        self.fail_put_remaining.store(n, Ordering::SeqCst);
    }

    /// Choose which error variant armed faults produce.
    pub fn set_fault_kind(&self, kind: FaultKind) {
        *self.fault_kind.lock().unwrap() = kind;
    }

    /// Script the next GETs one fault per attempt, then heal.
    ///
    /// Takes precedence over [`FaultStore::set_fail_get`] /
    /// [`FaultStore::fail_next_gets`] while the script is non-empty.
    pub fn script_gets(&self, kinds: &[FaultKind]) {
        *self.get_script.lock().unwrap() = kinds.iter().copied().collect();
    }

    /// Override the injected transient error's message.
    ///
    /// The error stays an `object_store::Error::Generic` — only its rendered
    /// text changes. This is the distinction a typed `NotFound` check makes and
    /// a `to_string().contains(..)` check cannot.
    pub fn set_transient_message(&self, msg: &str) {
        *self.transient_message.lock().unwrap() = Some(msg.to_owned());
    }

    /// GET operations that reached this store since construction.
    pub fn get_attempts(&self) -> usize {
        self.get_attempts.load(Ordering::SeqCst)
    }

    /// PUT operations that reached this store since construction.
    pub fn put_attempts(&self) -> usize {
        self.put_attempts.load(Ordering::SeqCst)
    }

    /// Zero both attempt counters, leaving armed faults untouched.
    pub fn reset_counters(&self) {
        self.get_attempts.store(0, Ordering::SeqCst);
        self.put_attempts.store(0, Ordering::SeqCst);
    }

    fn injected_error(&self, location: &Path) -> object_store::Error {
        let kind = *self.fault_kind.lock().unwrap();
        self.error_of(kind, location)
    }

    fn error_of(&self, kind: FaultKind, location: &Path) -> object_store::Error {
        match kind {
            FaultKind::Transient => self.transient(),
            FaultKind::NotFound => object_store::Error::NotFound {
                path: location.to_string(),
                source: Box::new(std::io::Error::other("injected NotFound")),
            },
            FaultKind::AlreadyExists => object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: Box::new(std::io::Error::other("injected AlreadyExists")),
            },
        }
    }

    fn transient(&self) -> object_store::Error {
        let msg = self
            .transient_message
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "injected transient store failure".to_owned());
        object_store::Error::Generic {
            store: "FaultStore",
            source: Box::new(std::io::Error::other(msg)),
        }
    }

    /// Decrement a fault budget, returning `true` when this call should fail.
    fn consume(counter: &AtomicUsize) -> bool {
        let mut cur = counter.load(Ordering::SeqCst);
        while cur > 0 {
            match counter.compare_exchange(cur, cur - 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(observed) => cur = observed,
            }
        }
        false
    }
}

impl Debug for FaultStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultStore")
    }
}

impl Display for FaultStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultStore")
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        self.put_attempts.fetch_add(1, Ordering::SeqCst);
        // Fetch-and-decrement: fail while a budget remains.
        if Self::consume(&self.fail_put_remaining) {
            return Err(self.injected_error(location));
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        self.get_attempts.fetch_add(1, Ordering::SeqCst);
        let scripted = self.get_script.lock().unwrap().pop_front();
        if let Some(kind) = scripted {
            return Err(self.error_of(kind, location));
        }
        if self.fail_get.load(Ordering::SeqCst) || Self::consume(&self.fail_get_remaining) {
            return Err(self.injected_error(location));
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
