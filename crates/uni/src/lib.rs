// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! # Uni - Embedded Graph Database
//!
//! Uni is an embedded, object-store-backed graph database with OpenCypher queries,
//! columnar analytics, and vector search.

pub mod api;
mod shutdown;

pub use api::appender::{AppenderBuilder, StreamingAppender};
pub use api::builder::PropertiesBuilder;
pub use api::hooks::{CommitHookContext, HookContext, QueryType, SessionHook};
pub use api::multi_agent::{LeaseGuard, WriteLease, WriteLeaseProvider};
pub use api::notifications::{CommitNotification, CommitStream, WatchBuilder};
pub use api::prepared::{PreparedLocy, PreparedLocyBinder, PreparedQuery, PreparedQueryBinder};
pub use api::schema::{
    ConstraintInfo, EdgeTypeBuilder, IndexInfo, IndexType, LabelBuilder, LabelInfo, PropertyInfo,
    ScalarType, SchemaBuilder, VectorAlgo, VectorIndexCfg, VectorMetric,
};
pub use api::session::{
    AutoCommitBuilder, AutoCommitResult, ProfileBuilder, Session, SessionCapabilities,
    SessionMetrics, TransactionBuilder, WriteLeaseSummary,
};
pub use api::sync::{
    ApplyBuilderSync, AutoCommitBuilderSync, ExecuteBuilderSync, LocyBuilderSync,
    ProfileBuilderSync, QueryBuilderSync, SessionSync, TransactionBuilderSync, TransactionSync,
    TxLocyBuilderSync, TxQueryBuilderSync, UniSync,
};
pub use api::template::{SessionTemplate, SessionTemplateBuilder};
pub use api::transaction::{
    ApplyBuilder, ApplyResult, CommitResult, ExecuteBuilder, IsolationLevel, Transaction,
};
pub use api::xervo::UniXervo;
pub use api::{DatabaseMetrics, ThrottlePressure, Uni, UniBuilder};

// Re-exports from internal crates
pub use uni_common::{
    CrdtType, DataType, Eid, Result, Schema, UniConfig, UniError, UniId, Vid, unival,
};
pub use uni_query::{
    Edge, ExecuteResult, ExplainOutput, FromValue, Node, Path, ProfileOutput, QueryMetrics,
    QueryResult, QueryWarning, Row, Value,
};

#[cfg(feature = "storage-internals")]
pub use uni_store::storage::StorageManager;

#[cfg(feature = "snapshot-internals")]
pub use uni_common::core::snapshot::SnapshotManifest;
#[cfg(feature = "snapshot-internals")]
pub use uni_store::snapshot::manager::SnapshotManager;

// Re-export crates
pub use uni_algo as algo_crate;
pub use uni_common as common;
pub use uni_query as query_crate;
pub use uni_store as store;

// Module aliases for internal crate access
pub mod core {
    pub use crate::common::core::*;
}

pub mod storage {
    pub use crate::store::storage::*;
    // Fix for tests expecting IndexManager in storage root or similar?
    // tests use uni_db::storage::manager::StorageManager.
    // crate::store::storage has manager.
}

pub mod runtime {
    pub use crate::store::runtime::*;
}

pub mod query {
    pub use crate::query_crate::query::*;
}

pub mod algo {
    // Tests use uni_db::algo::* (from src/algo).
    // uni-algo has `algo` module.
    pub use crate::algo_crate::algo::*;
}

pub mod xervo {
    pub use crate::api::xervo::*;
}

pub mod locy {
    pub use crate::api::impl_locy::LocyEngine;
    pub use crate::api::locy_result::{LocyExplainOutput, LocyResult};
    pub use uni_cypher::locy_ast::LocyProgram;
    pub use uni_cypher::{ParseError, parse_locy};
    pub use uni_locy::LocyResult as RawLocyResult;
    pub use uni_locy::{
        CommandResult, CompiledProgram, DerivedEdge, DerivedFactSet, LocyCompileError, LocyConfig,
        LocyError,
    };
}
