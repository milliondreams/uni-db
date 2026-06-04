// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.diameter procedure implementation.

use crate::algo::algorithms::{Algorithm, GraphMetrics, GraphMetricsConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct DiameterAdapter;

impl GraphAlgoAdapter for DiameterAdapter {
    const NAME: &'static str = "uni.algo.diameter";
    type Algo = GraphMetrics;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![("weightProperty", ValueType::String, Some(Value::Null))]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("diameter", ValueType::Float), ("path", ValueType::Path)]
    }

    fn to_config(_args: Vec<Value>) -> Result<GraphMetricsConfig> {
        Ok(GraphMetricsConfig {})
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(vec![AlgoResultRow {
            values: vec![json!(result.diameter), Value::Null],
        }])
    }

    fn include_reverse() -> bool {
        false
    }

    fn weight_arg_index() -> Option<usize> {
        Some(0)
    }
}

pub type DiameterProcedure = GenericAlgoProcedure<DiameterAdapter>;
