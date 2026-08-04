// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.harmonicCentrality procedure implementation.

use crate::algo::algorithms::{Algorithm, HarmonicCentrality, HarmonicCentralityConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, vid_pair_rows};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::Value;

pub struct HarmonicCentralityAdapter;

impl GraphAlgoAdapter for HarmonicCentralityAdapter {
    const NAME: &'static str = "uni.algo.harmonicCentrality";
    type Algo = HarmonicCentrality;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("centrality", ValueType::Float)]
    }

    fn to_config(_args: Vec<Value>) -> Result<HarmonicCentralityConfig> {
        Ok(HarmonicCentralityConfig {})
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(vid_pair_rows(result.scores))
    }
}

pub type HarmonicCentralityProcedure = GenericAlgoProcedure<HarmonicCentralityAdapter>;
