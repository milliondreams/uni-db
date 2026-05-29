#![allow(dead_code, unused_imports, clippy::all)]
//! M10 reload discipline for `IndexKindProvider` + `IndexHandoff`.
//!
//! Per §11.2.1 an index reload calls `persist()` on the old in-flight
//! `IndexHandle` and `open()` on the new provider with those bytes,
//! preserving built indexes across the swap. This test wires the
//! `ReloadDispatcher::with_handlers([IndexHandoff])` path against a
//! pair of test providers and asserts the round-trip preserves the
//! original payload.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use uni_plugin::reload::{IndexHandoff, ReloadDispatcher, ReloadKindHandlers};
use uni_plugin::traits::index::{IndexBuild, IndexHandle, IndexKind, IndexKindProvider};
use uni_plugin::{FnError, PluginRecordSnapshot, PluginRegistry};

/// Index handle that records how many times `persist()` was called.
struct CountingHandle {
    payload: Vec<u8>,
    persist_calls: Arc<AtomicUsize>,
}

impl IndexHandle for CountingHandle {
    fn probe(&self, _query: &RecordBatch, _k: usize) -> Result<RecordBatch, FnError> {
        Err(FnError::new(0, "unused"))
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        self.persist_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.payload.clone())
    }
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

/// Provider that records `open()` invocations and surfaces the bytes.
struct RecordingProvider {
    open_calls: Arc<AtomicUsize>,
    last_bytes: Arc<parking_lot::Mutex<Vec<u8>>>,
}

impl IndexKindProvider for RecordingProvider {
    fn kind(&self) -> IndexKind {
        IndexKind::new("recording")
    }
    fn build(&self, _source: &RecordBatch, _options: &str) -> Result<Box<dyn IndexBuild>, FnError> {
        Err(FnError::new(0, "unused"))
    }
    fn open(&self, persisted: &[u8]) -> Result<Box<dyn IndexHandle>, FnError> {
        self.open_calls.fetch_add(1, Ordering::SeqCst);
        *self.last_bytes.lock() = persisted.to_vec();
        Ok(Box::new(CountingHandle {
            payload: persisted.to_vec(),
            persist_calls: Arc::new(AtomicUsize::new(0)),
        }))
    }
}

#[test]
fn index_reload_calls_persist_then_open_with_same_bytes() {
    let registry = PluginRegistry::new();
    let snap = PluginRecordSnapshot::default();

    let persist_calls = Arc::new(AtomicUsize::new(0));
    let open_calls = Arc::new(AtomicUsize::new(0));
    let last_bytes = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let mut handlers = ReloadKindHandlers::default();
    handlers.index_handles.push(IndexHandoff {
        name: "ix-recording".to_owned(),
        old: Box::new(CountingHandle {
            payload: vec![7, 14, 21, 28],
            persist_calls: Arc::clone(&persist_calls),
        }),
        new: Arc::new(RecordingProvider {
            open_calls: Arc::clone(&open_calls),
            last_bytes: Arc::clone(&last_bytes),
        }),
    });

    let outcome = ReloadDispatcher::new(&snap, &registry)
        .with_handlers(handlers)
        .dispatch()
        .expect("dispatch");

    assert_eq!(
        persist_calls.load(Ordering::SeqCst),
        1,
        "persist called once"
    );
    assert_eq!(open_calls.load(Ordering::SeqCst), 1, "open called once");
    assert_eq!(*last_bytes.lock(), vec![7, 14, 21, 28], "bytes round-trip");
    assert_eq!(outcome.index_handles.len(), 1);
    assert_eq!(outcome.index_handles[0].0, "ix-recording");
}
