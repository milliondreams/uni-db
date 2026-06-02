// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.labelPropagation procedure implementation.

use crate::algo::algorithms::{Algorithm, LabelPropagation, LabelPropagationConfig};
use crate::algo::procedure_template::{
    GenericAlgoProcedure, GraphAlgoAdapter, arg_bool, arg_str, arg_u64,
};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct LabelPropagationAdapter;

impl GraphAlgoAdapter for LabelPropagationAdapter {
    const NAME: &'static str = "uni.algo.labelPropagation";
    type Algo = LabelPropagation;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("maxIterations", ValueType::Int, Some(json!(10))),
            ("write", ValueType::Bool, Some(json!(false))),
            ("writeProperty", ValueType::String, Some(json!("community"))),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("communityId", ValueType::Int)]
    }

    fn to_config(args: Vec<Value>) -> Result<LabelPropagationConfig> {
        Ok(LabelPropagationConfig {
            max_iterations: arg_u64(&args, 0, "maxIterations")? as usize,
            write: arg_bool(&args, 1, "write")?,
            write_property: arg_str(&args, 2, "writeProperty")?.to_string(),
            seed_property: None,
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

pub type LabelPropagationProcedure = GenericAlgoProcedure<LabelPropagationAdapter>;
