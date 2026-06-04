//! Catalog / virtual-schema plugins + replacement scans.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::Statistics;
use smol_str::SmolStr;

use crate::errors::FnError;
use crate::qname::QName;

/// A catalog provider exposing labels / edge-types not backed by `uni-store`.
pub trait CatalogProvider: Send + Sync {
    /// Catalog name (used as a prefix in qualified label / edge references).
    fn name(&self) -> &str;

    /// Enumerate labels in this catalog.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the listing fails.
    fn list_labels(&self) -> Result<Vec<CatalogLabel>, FnError>;

    /// Enumerate edge types in this catalog.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the listing fails.
    fn list_edge_types(&self) -> Result<Vec<CatalogEdgeType>, FnError>;

    /// Resolve a label name to a queryable table reference.
    fn resolve_label(&self, label: &str) -> Option<Arc<dyn CatalogTable>>;

    /// Resolve an edge type name.
    fn resolve_edge_type(&self, edge: &str) -> Option<Arc<dyn CatalogTable>>;
}

/// Catalog-declared label descriptor.
#[derive(Clone, Debug)]
pub struct CatalogLabel {
    /// Label name.
    pub name: SmolStr,
    /// Optional human description.
    pub doc: String,
}

/// Catalog-declared edge-type descriptor.
#[derive(Clone, Debug)]
pub struct CatalogEdgeType {
    /// Edge type name.
    pub name: SmolStr,
    /// Optional human description.
    pub doc: String,
}

/// A queryable catalog table — like a DataFusion `TableProvider` but in the
/// plugin namespace.
pub trait CatalogTable: Send + Sync {
    /// Schema of rows this table produces.
    fn schema(&self) -> SchemaRef;

    /// Stream rows matching the optional projection, filters, and limit.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the scan cannot start.
    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, FnError>;

    /// Cardinality / size statistics, if known.
    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

/// Request to a [`ReplacementScanProvider`].
#[derive(Debug)]
#[non_exhaustive]
pub enum ReplacementRequest<'a> {
    /// Unknown label encountered in `MATCH (n:Foo)`.
    Label(&'a str),
    /// Unknown procedure encountered in `CALL`.
    Procedure(&'a QName),
    /// Unknown scalar function encountered in an expression.
    Function(&'a QName),
}

/// Replacement to use in place of an unknown identifier.
#[non_exhaustive]
pub enum Replacement {
    /// Serve via a catalog table.
    CatalogTable(Arc<dyn CatalogTable>),
    /// Rewrite the call to a different procedure.
    Procedure(QName),
    /// Rewrite the call to a different scalar function.
    Function(QName),
}

impl std::fmt::Debug for Replacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CatalogTable(_) => f
                .debug_tuple("CatalogTable")
                .field(&"<dyn CatalogTable>")
                .finish(),
            Self::Procedure(q) => f.debug_tuple("Procedure").field(q).finish(),
            Self::Function(q) => f.debug_tuple("Function").field(q).finish(),
        }
    }
}

/// Replacement-scan provider — DuckDB-style auto-routing of unknown
/// identifiers.
pub trait ReplacementScanProvider: Send + Sync {
    /// Attempt to provide a [`Replacement`] for the given request.
    fn replace(&self, request: &ReplacementRequest<'_>) -> Option<Replacement>;
}
