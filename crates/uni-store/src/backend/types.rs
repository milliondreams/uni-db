// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Backend-agnostic types for storage operations.

/// Filter expression for backend queries.
#[derive(Debug, Clone)]
pub enum FilterExpr {
    /// SQL-like filter string (e.g., `"_deleted = false AND _version <= 5"`).
    Sql(String),
    /// No filter — return all rows.
    None,
}

impl FilterExpr {
    /// Create a filter from an optional SQL string.
    pub fn from_optional(filter: Option<&str>) -> Self {
        match filter {
            Some(s) => FilterExpr::Sql(s.to_string()),
            None => FilterExpr::None,
        }
    }

    /// Returns the SQL string if present.
    pub fn as_sql(&self) -> Option<&str> {
        match self {
            FilterExpr::Sql(s) => Some(s),
            FilterExpr::None => None,
        }
    }
}

/// Column projection for backend queries.
#[derive(Debug, Clone)]
pub enum ColumnProjection {
    /// Select specific columns by name.
    Columns(Vec<String>),
    /// Select all columns.
    All,
}

/// Scan request for table reads.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    /// Table name to scan.
    pub table_name: String,
    /// Columns to project.
    pub columns: ColumnProjection,
    /// Filter expression.
    pub filter: FilterExpr,
    /// Maximum number of rows to return.
    pub limit: Option<usize>,
}

impl ScanRequest {
    /// Create a scan request for all columns with no filter.
    pub fn all(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: ColumnProjection::All,
            filter: FilterExpr::None,
            limit: None,
        }
    }

    /// Builder: set columns.
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = ColumnProjection::Columns(columns);
        self
    }

    /// Builder: set filter.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = FilterExpr::Sql(filter.into());
        self
    }

    /// Builder: set optional filter.
    pub fn with_optional_filter(mut self, filter: Option<&str>) -> Self {
        self.filter = FilterExpr::from_optional(filter);
        self
    }

    /// Builder: set limit.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Write mode for backend writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Append rows to existing data.
    Append,
    /// Replace all existing data (atomic overwrite).
    Overwrite,
}

/// Distance metric for vector search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance.
    L2,
    /// Cosine distance.
    Cosine,
    /// Dot product distance.
    Dot,
}

/// Vector index type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIndexType {
    /// Inverted File Index with Product Quantization.
    IvfPq,
    /// Inverted File Index with HNSW and Scalar Quantization.
    IvfHnswSq,
}

/// Vector index configuration.
#[derive(Debug, Clone)]
pub struct VectorIndexConfig {
    /// Distance metric for the index.
    pub metric: DistanceMetric,
    /// Type of vector index to build.
    pub index_type: VectorIndexType,
    /// Number of IVF partitions.
    pub num_partitions: Option<usize>,
    /// Number of PQ sub-vectors.
    pub num_sub_vectors: Option<usize>,
}

/// Scalar index type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarIndexType {
    /// B-Tree index for range queries.
    BTree,
    /// Bitmap index for low-cardinality columns.
    Bitmap,
    /// Label list index for array columns.
    LabelList,
}

/// Index metadata.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Columns covered by the index.
    pub columns: Vec<String>,
    /// Index type description (backend-specific, e.g., "IVF_PQ", "BTree").
    pub index_type: String,
}
