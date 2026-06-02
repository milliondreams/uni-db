// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.bellmanFord procedure implementation.

use crate::algo::algorithms::{Algorithm, BellmanFord, BellmanFordConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, parse_vid_arg};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};

pub struct BellmanFordAdapter;

impl GraphAlgoAdapter for BellmanFordAdapter {
    const NAME: &'static str = "uni.algo.bellmanFord";
    type Algo = BellmanFord;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("sourceNode", ValueType::Node, None),
            ("weightProperty", ValueType::String, Some(Value::Null)),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("distance", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> Result<BellmanFordConfig> {
        Ok(BellmanFordConfig {
            source: parse_vid_arg(&args[0], "sourceNode")?,
        })
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        if result.has_negative_cycle {
            return Err(anyhow!("Negative cycle detected"));
        }

        Ok(result
            .distances
            .into_iter()
            .map(|(vid, dist)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(dist)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        false
    }

    fn weight_arg_index() -> Option<usize> {
        Some(1)
    }
}

pub type BellmanFordProcedure = GenericAlgoProcedure<BellmanFordAdapter>;
