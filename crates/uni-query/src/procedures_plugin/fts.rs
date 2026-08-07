// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.fts.query` — full-text-search over an FTS index.

use std::sync::Arc;
use std::sync::OnceLock;

use arrow_schema::DataType;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use uni_plugin::traits::procedure::{
    ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use crate::procedures_plugin::host_args::arg;
use crate::procedures_plugin::vector::{fts_query_yields, run_search_procedure};
use crate::query::df_graph::search_procedures::run_fts_query;

// Rust guideline compliant

fn signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: vec![
            arg(
                "label",
                ArgType::Primitive(DataType::Utf8),
                "Vertex label to search.",
            ),
            arg(
                "property",
                ArgType::Primitive(DataType::Utf8),
                "FTS property name on the label.",
            ),
            arg(
                "search_term",
                ArgType::Primitive(DataType::Utf8),
                "Free-text search term.",
            ),
            arg(
                "k",
                ArgType::Primitive(DataType::Int64),
                "Number of top hits to return.",
            ),
            arg(
                "filter",
                ArgType::Primitive(DataType::Utf8),
                "Optional pushdown filter expression.",
            ),
            arg(
                "threshold",
                ArgType::Primitive(DataType::Float64),
                "Optional minimum score threshold (post-filter).",
            ),
            arg(
                "options",
                ArgType::CypherValue,
                "Optional reranker / extra options map.",
            ),
        ],
        yields: fts_query_yields(),
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "BM25 full-text search over an FTS index with optional cross-encoder rerank."
            .to_owned(),
    })
}

#[derive(Debug)]
struct FtsQueryProc;

impl ProcedurePlugin for FtsQueryProc {
    fn signature(&self) -> &ProcedureSignature {
        signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        run_search_procedure(
            "uni.fts.query",
            &ctx,
            args,
            signature(),
            |host, uni_args, yield_items, output_schema| async move {
                let target_properties = host.target_properties().clone();
                run_fts_query(
                    &host,
                    &uni_args,
                    &yield_items,
                    &target_properties,
                    &output_schema,
                )
                .await
            },
        )
    }
}

/// Register `uni.fts.query` into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.procedure(
        QName::new("uni", "fts.query"),
        signature().clone(),
        Arc::new(FtsQueryProc),
    )?;
    Ok(())
}
