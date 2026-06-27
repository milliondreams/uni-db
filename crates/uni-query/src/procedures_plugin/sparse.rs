// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.sparse.query` — scored sparse-vector (SPLADE / learned-sparse) search.

use std::sync::Arc;
use std::sync::OnceLock;

use arrow_schema::DataType;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use crate::procedures_plugin::vector::{fts_query_yields, run_search_procedure};
use crate::query::df_graph::search_procedures::run_sparse_query;

// Rust guideline compliant

fn signature() -> &'static ProcedureSignature {
    static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
    SIG.get_or_init(|| ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("label"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Vertex label to search.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("property"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Sparse-vector property name on the label.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("query"),
                ty: ArgType::CypherValue,
                default: None,
                doc: "Query sparse vector ({indices, values}).".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("k"),
                ty: ArgType::Primitive(DataType::Int64),
                default: None,
                doc: "Number of top hits to return.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("filter"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Optional pushdown filter expression.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("threshold"),
                ty: ArgType::Primitive(DataType::Float64),
                default: None,
                doc: "Optional minimum dot-score threshold (post-filter).".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("options"),
                ty: ArgType::CypherValue,
                default: None,
                doc: "Optional extra options map (e.g. over_fetch).".to_owned(),
            },
        ],
        // Sparse scoring is a dot product (similarity), so like FTS there is no
        // `distance` column — only `score`/`rerank_score`.
        yields: fts_query_yields(),
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "Scored sparse-vector (SPLADE / learned-sparse) retrieval by dot product, \
               MVCC/L0-aware via exact re-scoring."
            .to_owned(),
    })
}

#[derive(Debug)]
struct SparseQueryProc;

impl ProcedurePlugin for SparseQueryProc {
    fn signature(&self) -> &ProcedureSignature {
        signature()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        run_search_procedure(
            "uni.sparse.query",
            &ctx,
            args,
            signature(),
            |host, uni_args, yield_items, output_schema| async move {
                let target_properties = host.target_properties().clone();
                run_sparse_query(
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

/// Register `uni.sparse.query` into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.procedure(
        QName::new("uni", "sparse.query"),
        signature().clone(),
        Arc::new(SparseQueryProc),
    )?;
    Ok(())
}
