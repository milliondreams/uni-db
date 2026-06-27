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

/// Tunable knobs for a vector / multi-vector ANN query.
///
/// `Default` (`{None, None}`) lets Lance pick its built-in defaults, i.e. the
/// behavior before these knobs existed. `nprobes` controls how many IVF
/// partitions are probed (higher = better recall, slower); `refine_factor` re-ranks
/// `refine_factor * k` index candidates with exact distances (recovers PQ error).
#[derive(Debug, Clone, Copy, Default)]
pub struct VectorQueryOpts {
    /// Number of IVF partitions to probe. `None` = Lance default.
    pub nprobes: Option<usize>,
    /// Exact-distance re-rank factor over the candidate set. `None` = no refine.
    pub refine_factor: Option<u32>,
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
    /// Optional Lance branch to read from. `None` = primary (main) branch.
    ///
    /// Set by the storage manager when a session has fork scope active;
    /// see `crate::backend::lance_branch` for the underlying primitives.
    pub branch: Option<String>,
}

impl ScanRequest {
    /// Create a scan request for all columns with no filter.
    pub fn all(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: ColumnProjection::All,
            filter: FilterExpr::None,
            limit: None,
            branch: None,
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

    /// Builder: set the Lance branch to read from.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Builder: set the Lance branch from an `Option`.
    pub fn with_optional_branch(mut self, branch: Option<String>) -> Self {
        self.branch = branch;
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

/// The buildable physical vector-index shapes and their tuning parameters.
///
/// A backend-agnostic mirror of the ANN index families a storage backend can
/// construct. The logical MUVERA type is resolved to its `inner` shape before
/// reaching the backend, so it never appears here. `num_partitions` is already
/// resolved to a concrete value (the logical `Option` default of "auto" is
/// mapped to a single partition by the caller, matching the prior behavior).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIndexKind {
    /// Brute-force flat index (a single IVF partition).
    Flat,
    /// IVF with uncompressed vectors.
    IvfFlat { num_partitions: u32 },
    /// IVF with Product Quantization.
    IvfPq {
        num_partitions: u32,
        num_sub_vectors: u32,
        num_bits: u8,
    },
    /// IVF with Scalar Quantization.
    IvfSq { num_partitions: u32 },
    /// IVF with RabitQ Quantization. `num_bits` `None` keeps the backend default.
    IvfRq {
        num_partitions: u32,
        num_bits: Option<u8>,
    },
    /// IVF-HNSW without quantization (highest recall).
    HnswFlat {
        m: u32,
        ef_construction: u32,
        num_partitions: u32,
    },
    /// IVF-HNSW with Scalar Quantization.
    HnswSq {
        m: u32,
        ef_construction: u32,
        num_partitions: u32,
    },
    /// IVF-HNSW with Product Quantization.
    HnswPq {
        m: u32,
        ef_construction: u32,
        num_sub_vectors: u32,
        num_partitions: u32,
    },
}

/// Parameters for building a physical vector (ANN) index.
///
/// Pairs the distance metric with the index shape ([`VectorIndexKind`]). This is
/// the backend-agnostic input to [`StorageBackend::create_vector_index`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorIndexParams {
    /// Distance metric used to compare vectors.
    pub metric: DistanceMetric,
    /// The index shape and its tuning parameters.
    pub kind: VectorIndexKind,
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
