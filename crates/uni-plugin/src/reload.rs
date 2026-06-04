//! Per-kind hot-reload discipline orchestration.
//!
//! [`ReloadDispatcher`] is invoked between the *drain* and *commit*
//! phases of `Uni::reload`. By the time it runs, the old plugin has
//! already been removed from the registry (so new captures cannot see
//! its surfaces), but in-flight queries that captured `Arc<dyn Foo>`
//! before the swap still operate against the old instances.
//!
//! The dispatcher's job is to run the per-kind handoff each surface
//! needs **before** committing the new plugin's registrations. The
//! handoffs are spelled out below:
//!
//! | Surface                 | Discipline                                         |
//! |-------------------------|----------------------------------------------------|
//! | Scalar / aggregate / …  | Clean — no protocol step needed.                   |
//! | `StorageBackend`        | Clean — old `Storage` continues until drained.     |
//! | `IndexKindProvider`     | `persist()` on old handle → `open()` on new.       |
//! | `BackgroundJobProvider` | Clean — next tick picks up the new provider.       |
//! | `CdcOutputProvider`     | `checkpoint()` on old → `start(lsn)` on new.       |
//! | `CrdtKindProvider`      | Schema-compat round-trip — hard error on mismatch. |
//! | `LogicalTypeProvider`   | Arrow extension contract unchanged — hard error.   |
//!
//! Per-kind handoffs that the trait surface already exposes (e.g.,
//! `CdcStream::checkpoint`) are invoked directly. Surfaces that need
//! a richer contract (CRDT / logical-type schema-compat) get a
//! default-method on the trait (`schema_compat_check`, `compat_check`)
//! that providers can override.
//!
//! Stateful surfaces with **in-flight private resources** (live
//! `IndexHandle`s, open `CdcStream`s) are reload-managed by the host
//! that owns those resources — the dispatcher receives them through
//! the [`ReloadKindHandlers`] builder rather than by registry walk,
//! because the registry only tracks *providers*, not the per-instance
//! resources those providers spawn.

use std::sync::Arc;

use crate::errors::{FnError, ReloadError};
use crate::registry::{PluginRecordSnapshot, PluginRegistry};
use crate::traits::cdc::{CdcLsn, CdcOutputProvider, CdcStartContext, CdcStream};
use crate::traits::crdt::CrdtKindProvider;
use crate::traits::index::{IndexHandle, IndexKindProvider};
use crate::traits::types::LogicalTypeProvider;

/// Host-supplied handlers wiring per-kind in-flight resources into the
/// reload pipeline.
///
/// The registry tracks providers, not the per-instance resources those
/// providers spawn (open index handles, live CDC streams). The host
/// owns those resources and supplies them through this builder so the
/// dispatcher can persist / checkpoint them at the right moment.
#[derive(Default)]
pub struct ReloadKindHandlers {
    /// Live index handles to persist and reopen against the new provider.
    pub index_handles: Vec<IndexHandoff>,
    /// Live CDC streams to checkpoint and restart against the new provider.
    pub cdc_streams: Vec<CdcHandoff>,
}

impl std::fmt::Debug for ReloadKindHandlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadKindHandlers")
            .field("index_handles", &self.index_handles.len())
            .field("cdc_streams", &self.cdc_streams.len())
            .finish()
    }
}

/// One live index handle and the new provider that will reopen it.
pub struct IndexHandoff {
    /// Diagnostic name for the index (typically the registry key).
    pub name: String,
    /// The live, in-flight index handle owned by the old plugin.
    pub old: Box<dyn IndexHandle>,
    /// The new plugin's provider that will reopen the persisted bytes.
    pub new: Arc<dyn IndexKindProvider>,
}

impl std::fmt::Debug for IndexHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexHandoff")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// One live CDC stream and the new provider that will resume it.
pub struct CdcHandoff {
    /// Diagnostic name for the stream (typically the registry key).
    pub name: String,
    /// The live CDC stream owned by the old plugin.
    pub old: Box<dyn CdcStream>,
    /// The new plugin's provider that will start a fresh stream at the
    /// checkpointed LSN.
    pub new: Arc<dyn CdcOutputProvider>,
}

impl std::fmt::Debug for CdcHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcHandoff")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// The outcome of a successful reload — opaque container for any
/// new in-flight resources the dispatcher constructed (reopened index
/// handles, restarted CDC streams) so the host can re-attach them.
#[derive(Default)]
pub struct ReloadOutcome {
    /// Reopened index handles, paired with their registry name.
    pub index_handles: Vec<(String, Box<dyn IndexHandle>)>,
    /// Restarted CDC streams, paired with their registry name.
    pub cdc_streams: Vec<(String, Box<dyn CdcStream>)>,
}

impl std::fmt::Debug for ReloadOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadOutcome")
            .field("index_handles", &self.index_handles.len())
            .field("cdc_streams", &self.cdc_streams.len())
            .finish()
    }
}

/// Orchestrates per-kind reload discipline between drain and commit.
///
/// Construct with [`ReloadDispatcher::new`], optionally populate live
/// resources via [`ReloadKindHandlers`], then call [`Self::dispatch`].
/// Failures abort the reload before the new plugin's registrations
/// commit.
#[derive(Debug)]
pub struct ReloadDispatcher<'a> {
    /// Snapshot of the old plugin's registry footprint.
    old: &'a PluginRecordSnapshot,
    /// The *new* registry view — already updated to point at the new
    /// plugin's providers for any surfaces both registered.
    new_registry: &'a PluginRegistry,
    /// Optional live-resource handoffs.
    handlers: ReloadKindHandlers,
}

impl<'a> ReloadDispatcher<'a> {
    /// Construct a dispatcher over the old plugin's snapshot and the
    /// new plugin's already-committed surface registry.
    #[must_use]
    pub fn new(old: &'a PluginRecordSnapshot, new_registry: &'a PluginRegistry) -> Self {
        Self {
            old,
            new_registry,
            handlers: ReloadKindHandlers::default(),
        }
    }

    /// Attach per-kind live-resource handoffs.
    #[must_use]
    pub fn with_handlers(mut self, handlers: ReloadKindHandlers) -> Self {
        self.handlers = handlers;
        self
    }

    /// Pre-flight check: run schema-compat checks for CRDT kinds and
    /// logical types that both old and new plugin register.
    ///
    /// `old_providers` supplies the *pre-swap* views of the providers
    /// the old plugin owned. The dispatcher cannot recover those from
    /// the registry once the swap has happened, so the host snapshots
    /// them immediately before evicting the old plugin.
    ///
    /// # Errors
    ///
    /// Returns [`ReloadError::SchemaIncompat`] when any pair fails its
    /// compat check.
    pub fn check_compat(&self, old_providers: &OldProviders) -> Result<(), ReloadError> {
        for kind in &self.old.crdt_kinds {
            let Some(old) = old_providers.crdt_kinds.get(kind) else {
                continue;
            };
            let Some(new) = self.new_registry.crdt_kind(kind) else {
                // New plugin did not re-register this CRDT kind — that
                // is a plain removal, not an incompat reload.
                continue;
            };
            new.schema_compat_check(old.as_ref())
                .map_err(|e: FnError| {
                    ReloadError::schema_incompat(format!("crdt:{}", kind.0), e.message)
                })?;
        }
        for name in &old_providers.logical_type_names {
            let Some(old) = old_providers.logical_types.get(name) else {
                continue;
            };
            let Some(new) = self.new_registry.logical_type(name) else {
                continue;
            };
            new.compat_check(old.as_ref()).map_err(|e: FnError| {
                ReloadError::schema_incompat(format!("logical-type:{name}"), e.message)
            })?;
        }
        Ok(())
    }

    /// Drive the per-instance handoffs: persist & reopen index handles;
    /// checkpoint & restart CDC streams.
    ///
    /// # Errors
    ///
    /// Returns [`ReloadError::Persist`] if a handoff fails. The old
    /// resources are dropped on failure — the host must be prepared to
    /// surface the failure and continue serving against the new
    /// providers' freshly-initialized resources.
    pub fn dispatch(mut self) -> Result<ReloadOutcome, ReloadError> {
        let mut outcome = ReloadOutcome::default();
        for handoff in self.handlers.index_handles.drain(..) {
            let bytes = handoff.old.persist().map_err(ReloadError::Persist)?;
            // Drop the old handle now — the new one is about to take its
            // place. RAII closes any underlying resources (mmaps, file
            // handles) the old handle owned.
            drop(handoff.old);
            let reopened = handoff.new.open(&bytes).map_err(ReloadError::Persist)?;
            outcome.index_handles.push((handoff.name, reopened));
        }
        for mut handoff in self.handlers.cdc_streams.drain(..) {
            let lsn: CdcLsn = handoff.old.checkpoint().map_err(ReloadError::Persist)?;
            // Best-effort shutdown of the old stream; surface failure as
            // Persist (the spec treats shutdown failure as fatal).
            handoff.old.shutdown().map_err(ReloadError::Persist)?;
            drop(handoff.old);
            let resumed = handoff
                .new
                .start(CdcStartContext::new(Some(lsn)))
                .map_err(ReloadError::Persist)?;
            outcome.cdc_streams.push((handoff.name, resumed));
        }
        Ok(outcome)
    }
}

/// Pre-swap snapshot of the old plugin's stateful providers.
///
/// The host populates this immediately before evicting the old plugin
/// from the registry so the dispatcher's schema-compat checks have
/// the old providers to compare against. The vectors are keyed by
/// the same names the registry uses (`CrdtKind` for CRDTs, extension
/// `name()` for logical types).
#[derive(Default)]
pub struct OldProviders {
    /// CRDT kind providers the old plugin owned, keyed by kind.
    pub crdt_kinds:
        std::collections::HashMap<crate::traits::crdt::CrdtKind, Arc<dyn CrdtKindProvider>>,
    /// Names of logical types the old plugin owned (preserves order).
    pub logical_type_names: Vec<smol_str::SmolStr>,
    /// Logical type providers keyed by extension name.
    pub logical_types: std::collections::HashMap<smol_str::SmolStr, Arc<dyn LogicalTypeProvider>>,
}

impl std::fmt::Debug for OldProviders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OldProviders")
            .field("crdt_kinds", &self.crdt_kinds.len())
            .field("logical_types", &self.logical_types.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::crdt::{CrdtKind, CrdtOp, CrdtState};
    use datafusion::scalar::ScalarValue;

    // ── Test fixtures ───────────────────────────────────────────────

    #[derive(Default)]
    struct CountState {
        v: i64,
    }

    impl CrdtState for CountState {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn apply(&mut self, op: &CrdtOp) -> Result<(), FnError> {
            self.v += op.bytes.len() as i64;
            Ok(())
        }
        fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
            let other = other
                .as_any()
                .downcast_ref::<CountState>()
                .ok_or_else(|| FnError::new(0x100, "merge: wrong state type"))?;
            if other.v > self.v {
                self.v = other.v;
            }
            Ok(())
        }
        fn value(&self) -> Result<ScalarValue, FnError> {
            Ok(ScalarValue::Int64(Some(self.v)))
        }
        fn persist(&self) -> Result<Vec<u8>, FnError> {
            Ok(self.v.to_le_bytes().to_vec())
        }
    }

    struct CountProvider {
        kind_str: &'static str,
    }

    impl CrdtKindProvider for CountProvider {
        fn kind(&self) -> CrdtKind {
            CrdtKind::new(self.kind_str)
        }
        fn empty(&self) -> Box<dyn CrdtState> {
            Box::new(CountState::default())
        }
        fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
            if bytes.len() != 8 {
                return Err(FnError::new(
                    0x101,
                    format!("expected 8 bytes, got {}", bytes.len()),
                ));
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(bytes);
            Ok(Box::new(CountState {
                v: i64::from_le_bytes(arr),
            }))
        }
    }

    struct RejectingProvider;

    impl CrdtKindProvider for RejectingProvider {
        fn kind(&self) -> CrdtKind {
            CrdtKind::new("count")
        }
        fn empty(&self) -> Box<dyn CrdtState> {
            Box::new(CountState::default())
        }
        fn from_persisted(&self, _bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
            Err(FnError::new(0x102, "rejecting all persisted bytes"))
        }
    }

    // ── Tests ───────────────────────────────────────────────────────

    #[test]
    fn schema_compat_accepts_round_trip() {
        let old = CountProvider { kind_str: "count" };
        let new = CountProvider { kind_str: "count" };
        new.schema_compat_check(&old).expect("compatible");
    }

    #[test]
    fn schema_compat_rejects_incompatible_round_trip() {
        let old = CountProvider { kind_str: "count" };
        let new = RejectingProvider;
        let err = new.schema_compat_check(&old).unwrap_err();
        assert!(err.message.contains("rejecting"));
    }

    #[test]
    fn dispatcher_check_compat_passes_when_all_round_trip() {
        let registry = PluginRegistry::new();
        // Manually drop a provider into the *new* registry's crdt_kinds.
        // We use a Helper to bypass the registrar; this is test-only.
        let snap = PluginRecordSnapshot {
            crdt_kinds: vec![CrdtKind::new("count")],
            ..Default::default()
        };
        // Insert the new provider into the new registry directly.
        // Since DashMap is private, we use a tiny test-helper plugin
        // registered via the public API in the integration test layer.
        // Here we just check the dispatcher logic in isolation:
        let mut olds = OldProviders::default();
        olds.crdt_kinds.insert(
            CrdtKind::new("count"),
            Arc::new(CountProvider { kind_str: "count" }),
        );
        // With no provider in `new_registry`, the dispatcher should treat
        // the absence as a clean removal — `Ok(())`.
        let d = ReloadDispatcher::new(&snap, &registry);
        d.check_compat(&olds).expect("absence is OK");
    }

    #[test]
    fn dispatcher_dispatch_handles_index_handoff() {
        struct DummyHandle {
            bytes: Vec<u8>,
        }
        impl IndexHandle for DummyHandle {
            fn probe(
                &self,
                _query: &datafusion::arrow::record_batch::RecordBatch,
                _k: usize,
            ) -> Result<datafusion::arrow::record_batch::RecordBatch, FnError> {
                Err(FnError::new(0, "unused"))
            }
            fn persist(&self) -> Result<Vec<u8>, FnError> {
                Ok(self.bytes.clone())
            }
            fn schema(&self) -> arrow_schema::SchemaRef {
                std::sync::Arc::new(arrow_schema::Schema::empty())
            }
        }
        struct DummyProvider;
        impl IndexKindProvider for DummyProvider {
            fn kind(&self) -> crate::traits::index::IndexKind {
                crate::traits::index::IndexKind::new("dummy")
            }
            fn build(
                &self,
                _source: &datafusion::arrow::record_batch::RecordBatch,
                _options: &str,
            ) -> Result<Box<dyn crate::traits::index::IndexBuild>, FnError> {
                Err(FnError::new(0, "unused"))
            }
            fn open(&self, persisted: &[u8]) -> Result<Box<dyn IndexHandle>, FnError> {
                Ok(Box::new(DummyHandle {
                    bytes: persisted.to_vec(),
                }))
            }
        }
        let snap = PluginRecordSnapshot::default();
        let registry = PluginRegistry::new();
        let mut handlers = ReloadKindHandlers::default();
        handlers.index_handles.push(IndexHandoff {
            name: "i1".to_owned(),
            old: Box::new(DummyHandle {
                bytes: vec![1, 2, 3, 4],
            }),
            new: Arc::new(DummyProvider),
        });
        let outcome = ReloadDispatcher::new(&snap, &registry)
            .with_handlers(handlers)
            .dispatch()
            .expect("handoff");
        assert_eq!(outcome.index_handles.len(), 1);
        assert_eq!(outcome.index_handles[0].0, "i1");
        assert_eq!(
            outcome.index_handles[0].1.persist().unwrap(),
            vec![1, 2, 3, 4]
        );
    }
}
