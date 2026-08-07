// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.betweenness procedure implementation.

use crate::algo::algorithms::{Algorithm, Betweenness, BetweennessConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, vid_pair_rows};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct BetweennessAdapter;

impl GraphAlgoAdapter for BetweennessAdapter {
    const NAME: &'static str = "uni.algo.betweenness";
    type Algo = Betweenness;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("normalize", ValueType::Bool, Some(json!(true))),
            ("samplingSize", ValueType::Int, Some(Value::Null)),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("score", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> Result<BetweennessConfig> {
        Ok(BetweennessConfig {
            normalize: args[0].as_bool().unwrap_or(true),
            sampling_size: args[1].as_u64().map(|v| v as usize),
        })
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(vid_pair_rows(result.scores))
    }
}

pub type BetweennessProcedure = GenericAlgoProcedure<BetweennessAdapter>;
