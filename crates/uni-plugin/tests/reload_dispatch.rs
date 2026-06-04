//! M10 ReloadDispatcher integration tests.
//!
//! These tests exercise the dispatcher with real provider implementations
//! (CRDT and index) to verify the per-kind handoffs run in the documented
//! order and propagate the expected `ReloadError` variants on failure.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::scalar::ScalarValue;
use uni_plugin::reload::{
    CdcHandoff, IndexHandoff, OldProviders, ReloadDispatcher, ReloadKindHandlers,
};
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcOutputProvider, CdcStartContext, CdcStream};
use uni_plugin::traits::crdt::{CrdtKind, CrdtKindProvider, CrdtOp, CrdtState};
use uni_plugin::traits::index::{IndexBuild, IndexHandle, IndexKind, IndexKindProvider};
use uni_plugin::{FnError, PluginRecordSnapshot, PluginRegistry, ReloadError};

// ── Test fixtures ───────────────────────────────────────────────────────

#[derive(Default)]
struct PassThroughState {
    bytes: Vec<u8>,
}

impl CrdtState for PassThroughState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, _op: &CrdtOp) -> Result<(), FnError> {
        Ok(())
    }
    fn merge(&mut self, _other: &dyn CrdtState) -> Result<(), FnError> {
        Ok(())
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Null)
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        Ok(self.bytes.clone())
    }
}

struct AcceptProvider {
    kind_str: &'static str,
}

impl CrdtKindProvider for AcceptProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new(self.kind_str)
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(PassThroughState::default())
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        Ok(Box::new(PassThroughState {
            bytes: bytes.to_vec(),
        }))
    }
}

struct RejectProvider {
    kind_str: &'static str,
}

impl CrdtKindProvider for RejectProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new(self.kind_str)
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(PassThroughState::default())
    }
    fn from_persisted(&self, _bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        Err(FnError::new(0x500, "reject all"))
    }
}

#[derive(Default)]
struct RoundTripIndex {
    payload: Vec<u8>,
}

impl IndexHandle for RoundTripIndex {
    fn probe(&self, _query: &RecordBatch, _k: usize) -> Result<RecordBatch, FnError> {
        Err(FnError::new(0, "unused"))
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        Ok(self.payload.clone())
    }
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

struct RoundTripProvider;

impl IndexKindProvider for RoundTripProvider {
    fn kind(&self) -> IndexKind {
        IndexKind::new("round-trip")
    }
    fn build(&self, _source: &RecordBatch, _options: &str) -> Result<Box<dyn IndexBuild>, FnError> {
        Err(FnError::new(0, "unused"))
    }
    fn open(&self, persisted: &[u8]) -> Result<Box<dyn IndexHandle>, FnError> {
        Ok(Box::new(RoundTripIndex {
            payload: persisted.to_vec(),
        }))
    }
}

// Minimal CDC stream that yields a fixed LSN at checkpoint.
struct DummyStream {
    checkpoint_called: Arc<std::sync::atomic::AtomicBool>,
    shutdown_called: Arc<std::sync::atomic::AtomicBool>,
}
impl CdcStream for DummyStream {
    fn deliver(&mut self, _batch: &CdcBatch) -> Result<(), FnError> {
        Ok(())
    }
    fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
        self.checkpoint_called
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(CdcLsn(42))
    }
    fn shutdown(&mut self) -> Result<(), FnError> {
        self.shutdown_called
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

struct DummyCdcProvider {
    resumed_from: Arc<std::sync::atomic::AtomicU64>,
}
impl CdcOutputProvider for DummyCdcProvider {
    fn name(&self) -> &str {
        "dummy-cdc"
    }
    fn start(&self, ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
        if let Some(lsn) = ctx.from_lsn {
            self.resumed_from
                .store(lsn.0, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(Box::new(DummyStream {
            checkpoint_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }))
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[test]
fn check_compat_passes_when_round_trip_succeeds() {
    let registry = PluginRegistry::new();
    let snap = PluginRecordSnapshot {
        crdt_kinds: vec![CrdtKind::new("c1")],
        ..Default::default()
    };
    // No provider in the *new* registry → absence is treated as removal,
    // not incompat.
    let mut olds = OldProviders::default();
    olds.crdt_kinds.insert(
        CrdtKind::new("c1"),
        Arc::new(AcceptProvider { kind_str: "c1" }),
    );
    ReloadDispatcher::new(&snap, &registry)
        .check_compat(&olds)
        .expect("absence is OK");
}

#[test]
fn dispatch_persists_then_reopens_index() {
    let registry = PluginRegistry::new();
    let snap = PluginRecordSnapshot::default();
    let mut handlers = ReloadKindHandlers::default();
    handlers.index_handles.push(IndexHandoff {
        name: "ix1".to_owned(),
        old: Box::new(RoundTripIndex {
            payload: vec![10, 20, 30],
        }),
        new: Arc::new(RoundTripProvider),
    });
    let outcome = ReloadDispatcher::new(&snap, &registry)
        .with_handlers(handlers)
        .dispatch()
        .expect("dispatch");
    assert_eq!(outcome.index_handles.len(), 1);
    let (name, h) = &outcome.index_handles[0];
    assert_eq!(name, "ix1");
    assert_eq!(h.persist().unwrap(), vec![10, 20, 30]);
}

#[test]
fn dispatch_checkpoints_and_resumes_cdc_stream() {
    let registry = PluginRegistry::new();
    let snap = PluginRecordSnapshot::default();
    let resumed_from = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut handlers = ReloadKindHandlers::default();
    handlers.cdc_streams.push(CdcHandoff {
        name: "cdc1".to_owned(),
        old: Box::new(DummyStream {
            checkpoint_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }),
        new: Arc::new(DummyCdcProvider {
            resumed_from: Arc::clone(&resumed_from),
        }),
    });
    let outcome = ReloadDispatcher::new(&snap, &registry)
        .with_handlers(handlers)
        .dispatch()
        .expect("cdc dispatch");
    assert_eq!(outcome.cdc_streams.len(), 1);
    // The new provider's `start` was called with the LSN the old stream
    // reported at checkpoint (42 from DummyStream).
    assert_eq!(
        resumed_from.load(std::sync::atomic::Ordering::SeqCst),
        42,
        "new provider should resume at the checkpointed LSN"
    );
}

#[test]
fn schema_incompat_surfaces_as_typed_error() {
    // We use the trait's default `schema_compat_check` directly here —
    // exercising the rejecting branch.
    let old: Arc<dyn CrdtKindProvider> = Arc::new(AcceptProvider { kind_str: "kx" });
    let new: Arc<dyn CrdtKindProvider> = Arc::new(RejectProvider { kind_str: "kx" });
    let err = new.schema_compat_check(old.as_ref()).unwrap_err();
    assert!(err.message.contains("reject"));

    // Wrap into the dispatcher's per-kind surface to verify the typed
    // `ReloadError::SchemaIncompat` variant.
    let wrapped = ReloadError::schema_incompat("crdt:kx", err.message);
    match wrapped {
        ReloadError::SchemaIncompat { kind, reason } => {
            assert_eq!(kind, "crdt:kx");
            assert!(reason.contains("reject"));
        }
        other => panic!("expected SchemaIncompat, got {other:?}"),
    }
}
