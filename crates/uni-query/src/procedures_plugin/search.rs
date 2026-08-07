// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.search` — hybrid (vector + FTS) search with RRF fusion.

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
use crate::procedures_plugin::vector::{hybrid_search_yields, run_search_procedure};
use crate::query::df_graph::search_procedures::run_hybrid_search;

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
                "properties",
                ArgType::CypherValue,
                "Either a property name (used for both vector and fts) or a map `{vector: '...', fts: '...'}`.",
            ),
            arg(
                "query_text",
                ArgType::Primitive(DataType::Utf8),
                "Free-text query (used for FTS and, optionally, auto-embedding).",
            ),
            arg(
                "query_vector",
                ArgType::CypherValue,
                "Optional pre-computed query vector (List<Float>); omit to auto-embed.",
            ),
            arg(
                "k",
                ArgType::Primitive(DataType::Int64),
                "Number of fused results to return.",
            ),
            arg(
                "filter",
                ArgType::Primitive(DataType::Utf8),
                "Optional pushdown filter expression.",
            ),
            arg(
                "options",
                ArgType::CypherValue,
                "Optional options map (fusion method, alpha, rrf_k, reranker, …).",
            ),
        ],
        yields: hybrid_search_yields(),
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "Hybrid vector + FTS search with RRF (or weighted) fusion and optional rerank."
            .to_owned(),
    })
}

#[derive(Debug)]
struct HybridSearchProc;

impl ProcedurePlugin for HybridSearchProc {
    fn signature(&self) -> &ProcedureSignature {
        signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        run_search_procedure(
            "uni.search",
            &ctx,
            args,
            signature(),
            |host, uni_args, yield_items, output_schema| async move {
                let target_properties = host.target_properties().clone();
                run_hybrid_search(
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

/// Register `uni.search` into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.procedure(
        QName::new("uni", "search"),
        signature().clone(),
        Arc::new(HybridSearchProc),
    )?;
    Ok(())
}
