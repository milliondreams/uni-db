// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Template for graph algorithm procedures to reduce boilerplate.

use crate::algo::ProjectionBuilder;
use crate::algo::algorithms::Algorithm;
use crate::algo::procedures::{
    AlgoContext, AlgoProcedure, AlgoResultRow, ProcedureSignature, ValueType,
};
use anyhow::{Result, anyhow};
use futures::stream::{self, BoxStream, StreamExt};
use serde_json::Value;
use std::marker::PhantomData;
use uni_common::core::id::Vid;

/// Parse a procedure argument as a `Vid`.
///
/// Accepts JSON numbers (preferred) or string-encoded unsigned integers
/// (the form `ProjectionBuilder` emits when terminal IDs round-trip
/// through Cypher parameter binding).
///
/// # Errors
///
/// Returns an error if `value` is not a `u64` or a string that parses as
/// one. The previous `unwrap_or(0)` form silently routed invalid input to
/// vertex `0`, masking caller mistakes as "no path" results.
pub fn parse_vid_arg(value: &Value, name: &str) -> Result<Vid> {
    let raw = match value {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| anyhow!("`{name}` must be a non-negative integer"))?,
        Value::String(s) => s
            .parse::<u64>()
            .map_err(|_| anyhow!("`{name}` string must parse as a u64, got {s:?}"))?,
        other => {
            return Err(anyhow!(
                "`{name}` must be an integer (or integer-string); got {other:?}"
            ));
        }
    };
    Ok(Vid::from(raw))
}

/// Adapter trait for specific graph algorithms.
pub trait GraphAlgoAdapter: Send + Sync + 'static {
    /// Name of the procedure (e.g., "algo.pageRank").
    const NAME: &'static str;

    /// The underlying algorithm.
    type Algo: Algorithm;

    /// Define algorithm-specific arguments (after nodeLabels and relationshipTypes).
    /// Returns: (name, type, default_value_if_optional)
    /// If default_value is None, it's required.
    fn specific_args() -> Vec<(&'static str, ValueType, Option<Value>)>;

    /// Define output columns.
    fn yields() -> Vec<(&'static str, ValueType)>;

    /// Convert parsed specific arguments to Algorithm Config.
    /// `args` contains only the algorithm-specific arguments.
    ///
    /// # Errors
    ///
    /// Returns an error when a required argument is missing, of the wrong
    /// type, or otherwise invalid (e.g. a vertex ID that fails to parse).
    /// Previously this method was infallible and adapters silently
    /// substituted `Vid::from(0)` for bad input — see `parse_vid_arg`.
    fn to_config(args: Vec<Value>) -> Result<<Self::Algo as Algorithm>::Config>;

    /// Convert algorithm result to output rows.
    fn map_result(result: <Self::Algo as Algorithm>::Result) -> Result<Vec<AlgoResultRow>>;

    /// Optional: Customize projection if needed (e.g., weights, directions).
    fn customize_projection(builder: ProjectionBuilder, _args: &[Value]) -> ProjectionBuilder {
        builder.include_reverse(Self::include_reverse())
    }

    /// Deprecated: use customize_projection instead.
    fn include_reverse() -> bool {
        true
    }
}

/// Generic implementation of `AlgoProcedure` for any `GraphAlgoAdapter`.
pub struct GenericAlgoProcedure<A: GraphAlgoAdapter> {
    _marker: PhantomData<A>,
}

impl<A: GraphAlgoAdapter> GenericAlgoProcedure<A> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<A: GraphAlgoAdapter> Default for GenericAlgoProcedure<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A: GraphAlgoAdapter> AlgoProcedure for GenericAlgoProcedure<A>
where
    <A::Algo as Algorithm>::Result: Send + 'static,
{
    fn name(&self) -> &str {
        A::NAME
    }

    fn signature(&self) -> ProcedureSignature {
        let mut args = vec![
            ("nodeLabels", ValueType::List),
            ("relationshipTypes", ValueType::List),
        ];
        let mut optional_args = Vec::new();

        for (name, ty, default) in A::specific_args() {
            if let Some(def) = default {
                optional_args.push((name, ty, def));
            } else {
                args.push((name, ty));
            }
        }

        ProcedureSignature {
            args,
            optional_args,
            yields: A::yields(),
        }
    }

    fn execute_with_projection(
        &self,
        _ctx: AlgoContext,
        args: Vec<Value>,
        projection: crate::algo::GraphProjection,
    ) -> BoxStream<'static, Result<AlgoResultRow>> {
        // V2 entry point — `args[0]` and `args[1]` are placeholder
        // empty arrays; the projection is supplied directly. Specific
        // algorithm args start at position 2.
        let signature = self.signature();
        let args = match signature.validate_args(args) {
            Ok(a) => a,
            Err(e) => return stream::once(async { Err(e) }).boxed(),
        };
        let specific_args = args[2..].to_vec();
        let stream = async_stream::try_stream! {
            let config = A::to_config(specific_args)?;
            let result = tokio::task::spawn_blocking(move || {
                A::Algo::run(&projection, config)
            }).await?;
            let rows = A::map_result(result)?;
            for row in rows {
                yield row;
            }
        };
        Box::pin(stream)
    }

    fn customize_projection(
        &self,
        builder: ProjectionBuilder,
        args: &[Value],
    ) -> ProjectionBuilder {
        // Delegate to the adapter's hook so per-algorithm projection
        // tweaks (edge weights, reverse-edge toggle) still apply when
        // the V2 dispatcher builds the projection from
        // `(nodeLabels, edgeTypes, …)`-shaped Direct args.
        A::customize_projection(builder, args)
    }
}

/// Build a [`crate::algo::GraphProjection`] from the legacy
/// `(nodeLabels, edgeTypes, …)` argument shape. Used by both the
/// V2 dispatcher in `uni-query` and the `AlgorithmProvider` bridge in
/// `uni-plugin-builtin` when an [`AlgoProcedure`] does not opt into
/// [`AlgoProcedure::wants_native_terminals`].
///
/// `args[0]` must be an array of label names and `args[1]` an array
/// of edge-type names. Specific algorithm args at position 2.. are
/// passed to [`AlgoProcedure::customize_projection`] so per-algorithm
/// projection knobs (weights, include-reverse, …) apply.
///
/// # Errors
///
/// Returns an `anyhow::Error` if `args[0]` / `args[1]` are not arrays,
/// reference labels / edge types not in the schema, or projection
/// construction fails.
pub async fn build_projection_from_direct_args(
    proc: &dyn AlgoProcedure,
    ctx: &AlgoContext,
    args: &[Value],
) -> Result<crate::algo::GraphProjection> {
    let node_labels: Vec<String> = args
        .first()
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("args[0] must be an array of node-label names"))?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| anyhow!("node-label must be a string"))
                .map(str::to_owned)
        })
        .collect::<Result<Vec<_>>>()?;
    let edge_types: Vec<String> = args
        .get(1)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("args[1] must be an array of edge-type names"))?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| anyhow!("edge-type must be a string"))
                .map(str::to_owned)
        })
        .collect::<Result<Vec<_>>>()?;

    let schema = ctx.storage.schema_manager().schema();
    for label in &node_labels {
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{label}' not found"));
        }
    }
    for etype in &edge_types {
        if !schema.edge_types.contains_key(etype) {
            return Err(anyhow!("Edge type '{etype}' not found"));
        }
    }

    let builder = ProjectionBuilder::new(ctx.storage.clone())
        .l0_manager(ctx.l0_manager.clone())
        .node_labels(&node_labels.iter().map(String::as_str).collect::<Vec<_>>())
        .edge_types(&edge_types.iter().map(String::as_str).collect::<Vec<_>>());

    let specific_args = &args[2..];
    let builder = proc.customize_projection(builder, specific_args);
    builder.build().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_vid_arg_accepts_number() {
        let vid = parse_vid_arg(&json!(42_u64), "node").expect("number must parse");
        assert_eq!(vid.as_u64(), 42);
    }

    #[test]
    fn parse_vid_arg_accepts_numeric_string() {
        // Some callers round-trip terminals through Cypher parameter binding,
        // which can stringify u64 values; that path must still work.
        let vid = parse_vid_arg(&json!("17"), "node").expect("numeric string must parse");
        assert_eq!(vid.as_u64(), 17);
    }

    #[test]
    fn parse_vid_arg_rejects_non_numeric_string() {
        // Regression: previously `unwrap_or(0)` would route a typo like
        // "noad" to vertex 0; we now surface a clear error to the caller.
        let err = parse_vid_arg(&json!("abc"), "source").unwrap_err();
        assert!(
            err.to_string().contains("`source`"),
            "error should name the arg: {err}"
        );
    }

    #[test]
    fn parse_vid_arg_rejects_negative_number() {
        // Negative ints cannot be a Vid (u64). Previously `as_u64()` would
        // return None and the `unwrap_or(0)` fallback silently swapped in
        // vertex 0; we now error.
        let err = parse_vid_arg(&json!(-1_i64), "source").unwrap_err();
        assert!(err.to_string().contains("non-negative"), "error: {err}");
    }

    #[test]
    fn parse_vid_arg_rejects_wrong_type() {
        let err = parse_vid_arg(&json!(true), "source").unwrap_err();
        assert!(err.to_string().contains("`source`"), "error: {err}");
    }
}
