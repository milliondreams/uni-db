// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! uni.algo.maxFlow procedure implementation (using Dinic).

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::{Algorithm, Dinic, DinicConfig};
use crate::algo::procedure_template::{GenericAlgoProcedure, GraphAlgoAdapter, parse_vid_arg};
use crate::algo::procedures::{AlgoResultRow, ValueType};
use anyhow::Result;
use serde_json::{Value, json};

pub struct DinicAdapter;

impl GraphAlgoAdapter for DinicAdapter {
    const NAME: &'static str = "uni.algo.maxFlow";
    type Algo = Dinic;

    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)> {
        vec![
            ("sourceNode", ValueType::Node, None),
            ("sinkNode", ValueType::Node, None),
            ("capacityProperty", ValueType::String, None),
        ]
    }

    fn yields() -> Vec<(&'static str, ValueType)> {
        vec![("maxFlow", ValueType::Float), ("flowEdges", ValueType::Int)]
    }

    fn to_config(args: Vec<Value>) -> Result<DinicConfig> {
        Ok(DinicConfig {
            source: parse_vid_arg(&args[0], "sourceNode")?,
            sink: parse_vid_arg(&args[1], "sinkNode")?,
        })
    }

    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>> {
        Ok(vec![AlgoResultRow {
            values: vec![json!(result.max_flow), json!(result.flow_edges)],
        }])
    }

    fn customize_projection(mut builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
        if let Some(prop) = args[2].as_str() {
            builder = builder.weight_property(prop);
        }
        builder
    }
}

pub type DinicProcedure = GenericAlgoProcedure<DinicAdapter>;
