// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.eigenvectorCentrality procedure implementation.

use crate::algo::algorithms::{Algorithm, EigenvectorCentrality, EigenvectorCentralityConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct EigenvectorCentralityAdapter;

impl GraphAlgoAdapter for EigenvectorCentralityAdapter {
    const NAME: &'static str = "uni.algo.eigenvectorCentrality";
    type Algo = EigenvectorCentrality;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("maxIterations", ValueType::Int, Some(json!(100))),
            ("tolerance", ValueType::Float, Some(json!(1e-6))),
            ("weightProperty", ValueType::String, Some(Value::Null)),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("score", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> Result<EigenvectorCentralityConfig> {
        Ok(EigenvectorCentralityConfig {
            max_iterations: args[0].as_u64().unwrap_or(100) as usize,
            tolerance: args[1].as_f64().unwrap_or(1e-6),
        })
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(result
            .scores
            .into_iter()
            .map(|(vid, score)| AlgoResultRow {
                values: vec![json!(vid.as_u64()), json!(score)],
            })
            .collect())
    }

    fn include_reverse() -> bool {
        false
    }

    fn weight_arg_index() -> Option<usize> {
        Some(2)
    }
}

pub type EigenvectorCentralityProcedure = GenericAlgoProcedure<EigenvectorCentralityAdapter>;
