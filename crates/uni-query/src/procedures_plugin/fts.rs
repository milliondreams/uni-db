// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.fts.query` — full-text-search over an FTS index.

use std::sync::Arc;
use std::sync::OnceLock;

use arrow_schema::{DataType, Schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use crate::procedures_plugin::host_args::{columnar_args_to_values, require_host};
use crate::procedures_plugin::vector::{fts_query_yields, resolve_yields_and_schema};
use crate::query::df_graph::search_procedures::run_fts_query;

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
                doc: "FTS property name on the label.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("search_term"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Free-text search term.".to_owned(),
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
                doc: "Optional minimum score threshold (post-filter).".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("options"),
                ty: ArgType::CypherValue,
                default: None,
                doc: "Optional reranker / extra options map.".to_owned(),
            },
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
        let host = require_host(&ctx, "uni.fts.query")?.clone();
        let uni_args = columnar_args_to_values(args);
        let sig = signature();
        let fallback_schema = Arc::new(Schema::new(sig.yields.clone()));
        let (yield_items, output_schema) = resolve_yields_and_schema(&host, sig, &fallback_schema);
        let target_properties = host.target_properties().clone();

        let stream_schema = output_schema.clone();
        let stream = stream::once(async move {
            let batch = run_fts_query(
                &host,
                &uni_args,
                &yield_items,
                &target_properties,
                &output_schema,
            )
            .await?
            .unwrap_or_else(|| arrow_array::RecordBatch::new_empty(output_schema.clone()));
            Ok::<_, datafusion::error::DataFusionError>(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
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
