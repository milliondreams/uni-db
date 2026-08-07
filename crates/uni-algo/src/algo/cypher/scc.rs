// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.scc procedure implementation.

use crate::algo::algorithms::{Algorithm, Scc, SccConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, vid_pair_rows};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::Value;

pub struct SccAdapter;

impl GraphAlgoAdapter for SccAdapter {
    const NAME: &'static str = "uni.algo.scc";
    type Algo = Scc;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("componentId", ValueType::Int)]
    }

    fn to_config(_args: Vec<Value>) -> Result<SccConfig> {
        Ok(SccConfig::default())
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(vid_pair_rows(result.components))
    }
}

pub type SccProcedure = GenericAlgoProcedure<SccAdapter>;
