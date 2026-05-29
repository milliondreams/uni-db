// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

pub mod algo;
pub mod projection_input;

pub use algo::AlgorithmRegistry;
pub use algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
pub use algo::projection::{GraphProjection, ProjectionBuilder, ProjectionConfig};
pub use projection_input::{GraphRefParseError, ProjectionInput, parse_graph_ref};
