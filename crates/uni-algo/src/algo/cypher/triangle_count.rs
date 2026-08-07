// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.triangleCount procedure implementation.

use crate::algo::algorithms::{Algorithm, TriangleCount, TriangleCountConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, vid_pair_rows};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::Value;

pub struct TriangleCountAdapter;

impl GraphAlgoAdapter for TriangleCountAdapter {
    const NAME: &'static str = "uni.algo.triangleCount";
    type Algo = TriangleCount;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![
            ("nodeId", ValueType::Int),
            ("triangleCount", ValueType::Int),
        ]
    }

    fn to_config(_args: Vec<Value>) -> Result<TriangleCountConfig> {
        Ok(TriangleCountConfig)
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(vid_pair_rows(result.node_counts))
    }
}

pub type TriangleCountProcedure = GenericAlgoProcedure<TriangleCountAdapter>;
