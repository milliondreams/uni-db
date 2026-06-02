// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.louvain procedure implementation.

use crate::algo::algorithms::{Algorithm, Louvain, LouvainConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, arg_f64, arg_u64};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct LouvainAdapter;

impl GraphAlgoAdapter for LouvainAdapter {
    const NAME: &'static str = "uni.algo.louvain";
    type Algo = Louvain;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("resolution", ValueType::Float, Some(json!(1.0))),
            ("maxIterations", ValueType::Int, Some(json!(10))),
            ("minModularityGain", ValueType::Float, Some(json!(1e-4))),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("communityId", ValueType::Int)]
    }

    fn to_config(args: Vec<Value>) -> Result<LouvainConfig> {
        Ok(LouvainConfig {
            resolution: arg_f64(&args, 0, "resolution")?,
            max_iterations: arg_u64(&args, 1, "maxIterations")? as usize,
            min_modularity_gain: arg_f64(&args, 2, "minModularityGain")?,
        })
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .communities
            .into_iter()
            .map(|(vid, cid)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(cid)],
            })
            .collect())
    }
}

pub type LouvainProcedure = GenericAlgoProcedure<LouvainAdapter>;
