//! First-party `uni.algo.reachability` — BFS reachability as a provider.
//!
//! This algorithm is deliberately authored the same way a third-party
//! plugin must: it obtains graph topology **only** through the stable
//! [`GraphView`] returned by
//! [`AlgorithmHost::project`](uni_plugin::traits::algorithm::AlgorithmHost::project), never by
//! downcasting host internals. It is registered purely as an
//! [`AlgorithmProvider`] (not through the static `uni_algo` registry), so
//! a `CALL uni.algo.reachability(...)` exercises the provider-dispatch
//! path end to end — proving the flagship "write BFS as a plugin" use
//! case works through the front door.
//!
//! # CALL shape
//!
//! `CALL uni.algo.reachability(<sourceVid>[, {nodeLabels, edgeTypes, reverse}])`
//!
//! - `sourceVid` (required integer) — the BFS start vertex id.
//! - optional config object: `nodeLabels` / `edgeTypes` (string arrays,
//!   empty = all), `reverse` (bool, traverse inbound edges instead).
//!
//! Yields `(nodeId INT, distance INT)` — every vertex reachable from the
//! source and its hop distance, source included at distance 0.
//
// Rust guideline compliant

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use uni_common::core::id::Vid;
use uni_plugin::FnError;
use uni_plugin::traits::algorithm::{
    AlgorithmContext, AlgorithmProvider, AlgorithmSignature, GraphProjectionSpec, GraphView,
};

/// BFS reachability provider (`uni.algo.reachability`).
///
/// See the [module docs](self) for the CALL shape and semantics.
pub struct ReachabilityProvider {
    signature: AlgorithmSignature,
}

impl std::fmt::Debug for ReachabilityProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReachabilityProvider")
            .finish_non_exhaustive()
    }
}

impl Default for ReachabilityProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ReachabilityProvider {
    /// Construct the provider with its fixed output signature.
    #[must_use]
    pub fn new() -> Self {
        let output_fields = vec![
            Field::new("nodeId", DataType::Int64, false),
            Field::new("distance", DataType::Int64, false),
        ];
        Self {
            signature: AlgorithmSignature {
                output_fields,
                docs: "uni.algo.reachability(sourceVid[, config]) — BFS reachable set".to_owned(),
            },
        }
    }
}

/// Parse the positional `config_json` array into `(source_vid, spec)`.
fn parse_config(config_json: &str) -> Result<(u64, GraphProjectionSpec), FnError> {
    let args: Vec<serde_json::Value> = if config_json.is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(config_json)
            .map_err(|e| FnError::new(0x811, format!("reachability: config_json parse: {e}")))?
    };

    let source = args
        .first()
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            FnError::new(
                0x812,
                "reachability: first argument must be an integer source vid",
            )
        })?;

    // Optional second-arg config object selects the projection subgraph.
    let mut spec = GraphProjectionSpec::default();
    if let Some(cfg) = args.get(1).and_then(serde_json::Value::as_object) {
        if let Some(labels) = cfg.get("nodeLabels").and_then(serde_json::Value::as_array) {
            spec.node_labels = labels
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect();
        }
        if let Some(types) = cfg.get("edgeTypes").and_then(serde_json::Value::as_array) {
            spec.edge_types = types
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect();
        }
        // Inbound traversal needs reverse adjacency materialized.
        if cfg
            .get("reverse")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            spec.include_reverse = true;
        }
    }

    #[allow(
        clippy::cast_sign_loss,
        reason = "vids are non-negative; a negative arg simply fails to resolve to a slot"
    )]
    Ok((source as u64, spec))
}

/// Run BFS over `view` from `source`, returning `(nodeId, distance)` rows.
fn bfs(
    view: &dyn GraphView,
    source: u64,
    reverse: bool,
) -> Result<(Vec<i64>, Vec<i64>), DataFusionError> {
    let mut node_ids: Vec<i64> = Vec::new();
    let mut distances: Vec<i64> = Vec::new();

    let Some(source_slot) = view.to_slot(Vid::new(source)) else {
        // Source absent from the projected subgraph → empty reachable set.
        return Ok((node_ids, distances));
    };

    if reverse && !view.has_reverse() {
        return Err(DataFusionError::Execution(
            "reachability: reverse traversal requested but projection has no inbound adjacency"
                .to_owned(),
        ));
    }

    let mut visited = vec![false; view.vertex_count()];
    visited[source_slot as usize] = true;
    let mut queue: VecDeque<(u32, i64)> = VecDeque::new();
    queue.push_back((source_slot, 0));

    while let Some((slot, dist)) = queue.pop_front() {
        #[allow(
            clippy::cast_possible_wrap,
            reason = "vids fit i64 in practice; Cypher integers are i64"
        )]
        node_ids.push(view.to_vid(slot).as_u64() as i64);
        distances.push(dist);

        let neighbors = if reverse {
            view.in_neighbors(slot)
        } else {
            view.out_neighbors(slot)
        };
        for &n in neighbors {
            if !visited[n as usize] {
                visited[n as usize] = true;
                queue.push_back((n, dist + 1));
            }
        }
    }

    Ok((node_ids, distances))
}

impl AlgorithmProvider for ReachabilityProvider {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x810, "reachability: host unbound"))?;

        let (source, spec) = parse_config(ctx.config_json)?;
        let reverse = spec.include_reverse;

        // Obtain the `'static` projection future NOW (before the stream),
        // so the borrow of `ctx.host` does not escape this synchronous
        // `run` into the returned stream.
        let projection = host.project(&spec);

        let fields = self.signature.output_fields.clone();
        let out_schema = Arc::new(Schema::new(fields));

        let schema_for_batch = Arc::clone(&out_schema);
        let stream = futures::stream::once(async move {
            let view = projection
                .await
                .map_err(|e| DataFusionError::Execution(format!("reachability: {e}")))?;
            let (node_ids, distances) = bfs(view.as_ref(), source, reverse)?;
            RecordBatch::try_new(
                schema_for_batch,
                vec![
                    Arc::new(Int64Array::from(node_ids)),
                    Arc::new(Int64Array::from(distances)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}
