// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.pageRank procedure implementation.

use crate::algo::algorithms::{Algorithm, PageRank, PageRankConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, arg_f64, arg_u64};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct PageRankAdapter;

impl GraphAlgoAdapter for PageRankAdapter {
    const NAME: &'static str = "uni.algo.pageRank";
    type Algo = PageRank;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("dampingFactor", ValueType::Float, Some(json!(0.85))),
            ("maxIterations", ValueType::Int, Some(json!(20))),
            ("tolerance", ValueType::Float, Some(json!(1e-6))),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("nodeId", ValueType::Int), ("score", ValueType::Float)]
    }

    fn to_config(args: Vec<Value>) -> Result<PageRankConfig> {
        Ok(PageRankConfig {
            damping_factor: arg_f64(&args, 0, "dampingFactor")?,
            max_iterations: arg_u64(&args, 1, "maxIterations")? as usize,
            tolerance: arg_f64(&args, 2, "tolerance")?,
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
}

pub type PageRankProcedure = GenericAlgoProcedure<PageRankAdapter>;
