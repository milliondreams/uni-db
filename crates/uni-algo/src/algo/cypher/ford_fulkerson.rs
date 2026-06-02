// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.fordFulkerson procedure implementation.

use crate::algo::algorithms::{Algorithm, FordFulkerson, FordFulkersonConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, parse_vid_arg};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct FordFulkersonAdapter;

impl GraphAlgoAdapter for FordFulkersonAdapter {
    const NAME: &'static str = "uni.algo.fordFulkerson";
    type Algo = FordFulkerson;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("sourceNode", ValueType::Node, None),
            ("sinkNode", ValueType::Node, None),
            ("capacityProperty", ValueType::String, None),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("maxFlow", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> Result<FordFulkersonConfig> {
        Ok(FordFulkersonConfig {
            source: parse_vid_arg(&args[0], "sourceNode")?,
            sink: parse_vid_arg(&args[1], "sinkNode")?,
        })
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(vec![AlgoResultRow {
            values: vec![json!(result.max_flow)],
        }])
    }

    fn include_reverse() -> bool {
        false
    }

    fn weight_arg_index() -> Option<usize> {
        Some(2)
    }
}

pub type FordFulkersonProcedure = GenericAlgoProcedure<FordFulkersonAdapter>;
