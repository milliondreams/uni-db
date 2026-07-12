//! First-party `uni.path.expand` — APOC-style config-driven path expansion.
//!
//! A pragmatic subset of Neo4j APOC `apoc.path.expandConfig`: bounded BFS from a
//! source vertex over a projected subgraph, filtered by node label / edge type /
//! direction and depth, with `NODE_GLOBAL` uniqueness. Authored purely against
//! the public [`AlgorithmProvider`] + [`GraphView`] surface (like
//! [`super::reachability`]) — the label/edge-type filters are applied at
//! projection-build time via [`GraphProjectionSpec`], so no typed-multigraph
//! `GraphView` extension is needed.
//!
//! # CALL shape
//!
//! `CALL uni.path.expand(<sourceVid>[, {nodeLabels, edgeTypes, direction,
//! minLevel, maxLevel}])`
//!
//! - `sourceVid` (required integer) — the expansion start vertex id.
//! - optional config: `nodeLabels` / `edgeTypes` (string arrays, empty = all);
//!   `direction` (`"out"` \[default\] / `"in"` / `"both"`); `minLevel` /
//!   `maxLevel` (integers; defaults `0` / unbounded).
//!
//! Yields `(nodeId INT, level INT)` — each vertex reachable within the depth
//! band and its BFS hop level.
//!
//! Not yet covered (documented follow-ups): APOC's per-type directional
//! `relationshipFilter` DSL (`LIKES>|<KNOWS`), `labelFilter` terminate/end
//! modes, and uniqueness modes other than `NODE_GLOBAL`.
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

/// Traversal direction for [`ExpandProvider`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Out,
    In,
    Both,
}

/// Parsed `uni.path.expand` configuration.
struct ExpandConfig {
    source: u64,
    spec: GraphProjectionSpec,
    direction: Direction,
    min_level: i64,
    max_level: i64,
}

/// Bounded-BFS path-expansion provider (`uni.path.expand`).
///
/// See the [module docs](self) for the CALL shape and semantics.
pub struct ExpandProvider {
    signature: AlgorithmSignature,
}

impl std::fmt::Debug for ExpandProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExpandProvider").finish_non_exhaustive()
    }
}

impl Default for ExpandProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpandProvider {
    /// Construct the provider with its fixed output signature.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: AlgorithmSignature {
                output_fields: vec![
                    Field::new("nodeId", DataType::Int64, false),
                    Field::new("level", DataType::Int64, false),
                ],
                docs: "uni.path.expand(sourceVid[, config]) — bounded BFS path expansion"
                    .to_owned(),
                ..Default::default()
            },
        }
    }
}

/// Parse the positional `config_json` array into an [`ExpandConfig`].
fn parse_config(config_json: &str) -> Result<ExpandConfig, FnError> {
    let args: Vec<serde_json::Value> = if config_json.is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(config_json)
            .map_err(|e| FnError::new(0x831, format!("expand: config_json parse: {e}")))?
    };

    let source = args
        .first()
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| {
            FnError::new(
                0x832,
                "expand: first argument must be an integer source vid",
            )
        })?;

    let mut spec = GraphProjectionSpec::default();
    let mut direction = Direction::Out;
    let mut min_level = 0_i64;
    let mut max_level = i64::MAX;

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
        direction = match cfg.get("direction").and_then(serde_json::Value::as_str) {
            Some("in") => Direction::In,
            Some("both") => Direction::Both,
            _ => Direction::Out,
        };
        if let Some(m) = cfg.get("minLevel").and_then(serde_json::Value::as_i64) {
            min_level = m;
        }
        if let Some(m) = cfg.get("maxLevel").and_then(serde_json::Value::as_i64) {
            max_level = m;
        }
    }

    // Inbound / both traversal needs reverse adjacency materialized.
    if matches!(direction, Direction::In | Direction::Both) {
        spec.include_reverse = true;
    }

    #[allow(
        clippy::cast_sign_loss,
        reason = "vids are non-negative; a negative arg fails to resolve to a slot"
    )]
    Ok(ExpandConfig {
        source: source as u64,
        spec,
        direction,
        min_level,
        max_level,
    })
}

/// Run the bounded BFS over `view`, returning `(nodeId, level)` rows within the
/// `[min_level, max_level]` band.
fn expand(
    view: &dyn GraphView,
    cfg: &ExpandConfig,
) -> Result<(Vec<i64>, Vec<i64>), DataFusionError> {
    let mut node_ids: Vec<i64> = Vec::new();
    let mut levels: Vec<i64> = Vec::new();

    let Some(source_slot) = view.to_slot(Vid::new(cfg.source)) else {
        return Ok((node_ids, levels));
    };

    let mut visited = vec![false; view.vertex_count()];
    visited[source_slot as usize] = true;
    let mut queue: VecDeque<(u32, i64)> = VecDeque::new();
    queue.push_back((source_slot, 0));

    while let Some((slot, level)) = queue.pop_front() {
        if level >= cfg.min_level && level <= cfg.max_level {
            #[allow(
                clippy::cast_possible_wrap,
                reason = "vids fit i64 in practice; Cypher integers are i64"
            )]
            {
                node_ids.push(view.to_vid(slot).as_u64() as i64);
            }
            levels.push(level);
        }
        // Do not expand past the depth band.
        if level >= cfg.max_level {
            continue;
        }

        let push = |n: u32, q: &mut VecDeque<(u32, i64)>, visited: &mut [bool]| {
            if !visited[n as usize] {
                visited[n as usize] = true;
                q.push_back((n, level + 1));
            }
        };
        match cfg.direction {
            Direction::Out => {
                for &n in view.out_neighbors(slot) {
                    push(n, &mut queue, &mut visited);
                }
            }
            Direction::In => {
                for &n in view.in_neighbors(slot) {
                    push(n, &mut queue, &mut visited);
                }
            }
            Direction::Both => {
                for &n in view.out_neighbors(slot) {
                    push(n, &mut queue, &mut visited);
                }
                for &n in view.in_neighbors(slot) {
                    push(n, &mut queue, &mut visited);
                }
            }
        }
    }

    Ok((node_ids, levels))
}

impl AlgorithmProvider for ExpandProvider {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x830, "expand: host unbound"))?;

        let cfg = parse_config(ctx.config_json)?;
        let projection = host.project(&cfg.spec);

        let fields = self.signature.output_fields.clone();
        let out_schema = Arc::new(Schema::new(fields));
        let schema_for_batch = Arc::clone(&out_schema);

        let stream = futures::stream::once(async move {
            let view = projection
                .await
                .map_err(|e| DataFusionError::Execution(format!("expand: {e}")))?;
            let (node_ids, levels) = expand(view.as_ref(), &cfg)?;
            RecordBatch::try_new(
                schema_for_batch,
                vec![
                    Arc::new(Int64Array::from(node_ids)),
                    Arc::new(Int64Array::from(levels)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}
