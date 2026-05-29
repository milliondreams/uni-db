//! Built-in reference impls for the remaining M5 surface traits:
//! `Connector`, `CdcOutputProvider`, `CatalogProvider`,
//! `ReplacementScanProvider`. Each is a minimal-but-real
//! implementation suitable for tests, the conformance suite, and as
//! the authoring template user plugins follow.

use std::sync::Arc;

use parking_lot::Mutex;
use uni_plugin::traits::catalog::{
    CatalogEdgeType, CatalogLabel, CatalogProvider, CatalogTable, Replacement, ReplacementRequest,
    ReplacementScanProvider,
};
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcOutputProvider, CdcStartContext, CdcStream};
use uni_plugin::traits::connector::{Connector, ConnectorConfig, ConnectorHandle};
use uni_plugin::{FnError, PluginError, PluginRegistrar};

/// Register the reference impls for `connector`, `cdc-output`,
/// `catalog`, `replacement-scan` surfaces.
///
/// # Errors
///
/// Returns [`PluginError`] on registration failure.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.connector(Arc::new(NoopConnector::new()))?;
    r.cdc_output(Arc::new(MemoryCdcOutputProvider))?;
    r.catalog(Arc::new(EmptyCatalog::new("builtin")))?;
    r.replacement_scan(Arc::new(NeverReplacementScan))?;
    Ok(())
}

// =========================================================================
// Connector — noop reference impl
// =========================================================================

/// `Connector` reference impl — no-op connector with a `noop` protocol.
///
/// Useful as a registration test fixture and as the authoring template
/// for real connectors (Bolt, GraphQL, etc.). Real connectors spawn a
/// server task in `start()` and signal it to drain in `stop()`.
#[derive(Debug)]
pub struct NoopConnector {
    next_id: Mutex<u64>,
}

impl Default for NoopConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl NoopConnector {
    /// Construct.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_id: Mutex::new(1),
        }
    }
}

impl Connector for NoopConnector {
    fn protocol(&self) -> &str {
        "noop"
    }
    fn start(&self, _cfg: ConnectorConfig) -> Result<ConnectorHandle, FnError> {
        let mut id = self.next_id.lock();
        let h = *id;
        *id = id.saturating_add(1);
        Ok(ConnectorHandle(h))
    }
    fn stop(&self, _handle: ConnectorHandle) -> Result<(), FnError> {
        Ok(())
    }
}

// =========================================================================
// CdcOutputProvider — in-memory reference impl
// =========================================================================

/// `CdcOutputProvider` that captures batches in memory. Useful for
/// tests and authoring-time validation of mutation event shapes.
#[derive(Debug, Default)]
pub struct MemoryCdcOutputProvider;

impl CdcOutputProvider for MemoryCdcOutputProvider {
    fn name(&self) -> &str {
        "memory"
    }
    fn start(&self, _ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
        Ok(Box::new(MemoryCdcStream {
            delivered: Vec::new(),
            checkpoint: CdcLsn(0),
        }))
    }
}

/// In-memory CDC stream. Captures every `deliver()` call into a Vec
/// for inspection. `checkpoint()` advances the high-water mark to the
/// max LSN seen; `shutdown()` clears the buffer.
#[derive(Debug)]
pub struct MemoryCdcStream {
    /// Every batch this stream has received, in delivery order.
    pub delivered: Vec<CdcBatch>,
    /// Most-recent acknowledged LSN.
    pub checkpoint: CdcLsn,
}

impl CdcStream for MemoryCdcStream {
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError> {
        self.delivered.push(batch.clone());
        Ok(())
    }
    fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
        // Advance to the highest lsn_end we've delivered.
        if let Some(b) = self.delivered.last() {
            self.checkpoint = b.lsn_end;
        }
        Ok(self.checkpoint)
    }
    fn shutdown(&mut self) -> Result<(), FnError> {
        self.delivered.clear();
        Ok(())
    }
}

// =========================================================================
// CatalogProvider — empty reference impl
// =========================================================================

/// `CatalogProvider` reference impl that exposes an empty catalog. Real
/// catalogs (e.g., federated Postgres tables, HuggingFace datasets)
/// follow the same shape but populate `list_labels` / `resolve_label`
/// from external sources.
#[derive(Debug)]
pub struct EmptyCatalog {
    name: String,
}

impl EmptyCatalog {
    /// Construct an empty catalog with the given name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl CatalogProvider for EmptyCatalog {
    fn name(&self) -> &str {
        &self.name
    }
    fn list_labels(&self) -> Result<Vec<CatalogLabel>, FnError> {
        Ok(vec![])
    }
    fn list_edge_types(&self) -> Result<Vec<CatalogEdgeType>, FnError> {
        Ok(vec![])
    }
    fn resolve_label(&self, _label: &str) -> Option<Arc<dyn CatalogTable>> {
        None
    }
    fn resolve_edge_type(&self, _edge: &str) -> Option<Arc<dyn CatalogTable>> {
        None
    }
}

// =========================================================================
// ReplacementScanProvider — never-replace reference impl
// =========================================================================

/// `ReplacementScanProvider` that never replaces — every unknown
/// identifier passes through to the standard "unknown" error. Useful
/// as the conservative default; real replacement scans (DuckDB-style)
/// route unknown labels to a catalog or external table function.
#[derive(Debug)]
pub struct NeverReplacementScan;

impl ReplacementScanProvider for NeverReplacementScan {
    fn replace(&self, _request: &ReplacementRequest<'_>) -> Option<Replacement> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::SystemTime;

    #[test]
    fn noop_connector_protocol_is_noop() {
        let c = NoopConnector::new();
        assert_eq!(c.protocol(), "noop");
    }

    #[test]
    fn noop_connector_start_returns_monotonic_handles() {
        let c = NoopConnector::new();
        let cfg = ConnectorConfig::default();
        let h1 = c.start(cfg.clone()).unwrap();
        let h2 = c.start(cfg.clone()).unwrap();
        assert!(h2.0 > h1.0);
        c.stop(h1).unwrap();
        c.stop(h2).unwrap();
    }

    fn make_cdc_batch(start: u64, end: u64) -> CdcBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_kind",
            DataType::Int64,
            false,
        )]));
        let arr: Arc<dyn arrow_array::Array> =
            Arc::new(Int64Array::from(vec![1_i64; (end - start) as usize]));
        let batch = arrow_array::RecordBatch::try_new(schema, vec![arr]).unwrap();
        CdcBatch {
            lsn_start: CdcLsn(start),
            lsn_end: CdcLsn(end),
            mutations: Arc::new(batch),
            commit_timestamp: SystemTime::now(),
        }
    }

    #[test]
    fn memory_cdc_captures_delivered_batches() {
        let provider = MemoryCdcOutputProvider;
        let ctx = CdcStartContext::new(None);
        let mut stream = provider.start(ctx).unwrap();
        stream.deliver(&make_cdc_batch(0, 3)).unwrap();
        stream.deliver(&make_cdc_batch(3, 7)).unwrap();
        let ack = stream.checkpoint().unwrap();
        assert_eq!(ack, CdcLsn(7));
        // shutdown clears.
        stream.shutdown().unwrap();
        // Re-checkpoint after shutdown returns the last ack value (no
        // new batches to advance).
        let ack2 = stream.checkpoint().unwrap();
        assert_eq!(ack2, CdcLsn(7));
    }

    #[test]
    fn empty_catalog_returns_empty_lists() {
        let c = EmptyCatalog::new("test-catalog");
        assert_eq!(c.name(), "test-catalog");
        assert!(c.list_labels().unwrap().is_empty());
        assert!(c.list_edge_types().unwrap().is_empty());
        assert!(c.resolve_label("anything").is_none());
        assert!(c.resolve_edge_type("anything").is_none());
    }

    #[test]
    fn never_replacement_scan_returns_none() {
        let r = NeverReplacementScan;
        let qname = uni_plugin::QName::builtin("anything");
        assert!(r.replace(&ReplacementRequest::Label("Foo")).is_none());
        assert!(r.replace(&ReplacementRequest::Procedure(&qname)).is_none());
        assert!(r.replace(&ReplacementRequest::Function(&qname)).is_none());
    }
}
