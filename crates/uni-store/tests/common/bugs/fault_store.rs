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

pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    fail_get: AtomicBool,
    fail_put_remaining: AtomicUsize,
}

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            fail_get: AtomicBool::new(false),
            fail_put_remaining: AtomicUsize::new(0),
        }
    }

    /// Arm/disarm persistent GET failure (survives retry loops).
    pub fn set_fail_get(&self, on: bool) {
        self.fail_get.store(on, Ordering::SeqCst);
    }

    /// Fail the next `n` PUT operations, then heal.
    pub fn fail_next_puts(&self, n: usize) {
        self.fail_put_remaining.store(n, Ordering::SeqCst);
    }

    fn transient() -> object_store::Error {
        object_store::Error::Generic {
            store: "FaultStore",
            source: Box::new(std::io::Error::other("injected transient store failure")),
        }
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
        // Fetch-and-decrement: fail while a budget remains.
        let mut cur = self.fail_put_remaining.load(Ordering::SeqCst);
        while cur > 0 {
            match self.fail_put_remaining.compare_exchange(
                cur,
                cur - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Err(Self::transient()),
                Err(observed) => cur = observed,
            }
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
        if self.fail_get.load(Ordering::SeqCst) {
            return Err(Self::transient());
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
