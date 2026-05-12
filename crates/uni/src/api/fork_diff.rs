// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 6 — Fork diff & promote types.
//!
//! `ForkDiff` describes the structural delta between two fork views
//! (or a fork and primary). The convention is *forward*: `diff(a, b)`
//! is the delta that, if applied to `a`, would produce `b`. So
//! `added` rows exist in `b` only, `deleted` exist in `a` only, and
//! `changed` is a per-row before/after on rows with matching identity.
//!
//! **Identity** is `UniId` for vertices and `(src_uid, dst_uid, type)`
//! for edges. Both are content-addressed (vertex UID = SHA3-256 of
//! `(label, ext_id, properties)`; edge UID is the tuple of endpoint
//! UIDs plus the edge type), so the diff is correct across two
//! unrelated forks that happen to have rolled the same VIDs. The
//! per-side VID is preserved on `DiffVertex` as informational; pairing
//! never depends on it.
//!
//! Phase 6a (the initial MVP) keyed diffs by VID. Phase 6b lifted
//! identity to UID so siblings-off-a-shared-parent and totally
//! unrelated forks compare correctly.
//!
//! `PromotePattern` is the spec for what to scan on a fork during
//! `Uni::promote_from_fork`. Phase 6 supports the most common shape
//! (label + optional Cypher WHERE clause); future phases may grow
//! relationship-aware patterns.

use std::fmt;

use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::{UniId, Vid};

/// The full delta from one fork view to another.
#[derive(Debug, Clone, Default)]
pub struct ForkDiff {
    /// Per-label vertex deltas.
    pub vertices: VertexDiff,
    /// Per-edge-type edge deltas.
    pub edges: EdgeDiff,
}

impl ForkDiff {
    /// Returns `true` when there are no vertex or edge differences.
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty() && self.edges.is_empty()
    }

    /// Total rows in this diff across vertices and edges.
    pub fn total_rows(&self) -> usize {
        self.vertices.total_rows() + self.edges.total_rows()
    }

    /// Return the inverse: swap added/deleted and swap before/after in
    /// every property change. By construction
    /// `diff(a,b).invert() == diff(b,a)`.
    pub fn invert(mut self) -> Self {
        self.vertices = self.vertices.invert();
        self.edges = self.edges.invert();
        self
    }
}

/// Vertex-side of [`ForkDiff`].
#[derive(Debug, Clone, Default)]
pub struct VertexDiff {
    /// Rows present in `b` but not `a`.
    pub added: Vec<DiffVertex>,
    /// Rows present in `a` but not `b`.
    pub deleted: Vec<DiffVertex>,
    /// Rows with matching identity in both sides but differing properties.
    pub changed: Vec<VertexPropertyChange>,
}

impl VertexDiff {
    /// Returns `true` when added, deleted, and changed are all empty.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.deleted.is_empty() && self.changed.is_empty()
    }

    /// Sum of added + deleted + changed counts.
    pub fn total_rows(&self) -> usize {
        self.added.len() + self.deleted.len() + self.changed.len()
    }

    fn invert(self) -> Self {
        Self {
            added: self.deleted,
            deleted: self.added,
            changed: self
                .changed
                .into_iter()
                .map(VertexPropertyChange::invert)
                .collect(),
        }
    }
}

/// Edge-side of [`ForkDiff`].
#[derive(Debug, Clone, Default)]
pub struct EdgeDiff {
    /// Edges present in `b` but not `a`.
    pub added: Vec<DiffEdge>,
    /// Edges present in `a` but not `b`.
    pub deleted: Vec<DiffEdge>,
    /// Edges with matching `(src_uid, dst_uid, type)` but differing properties.
    pub changed: Vec<EdgePropertyChange>,
}

impl EdgeDiff {
    /// Returns `true` when added, deleted, and changed are all empty.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.deleted.is_empty() && self.changed.is_empty()
    }

    /// Sum of added + deleted + changed counts.
    pub fn total_rows(&self) -> usize {
        self.added.len() + self.deleted.len() + self.changed.len()
    }

    fn invert(self) -> Self {
        Self {
            added: self.deleted,
            deleted: self.added,
            changed: self
                .changed
                .into_iter()
                .map(EdgePropertyChange::invert)
                .collect(),
        }
    }
}

/// A vertex row from one side of a diff.
#[derive(Debug, Clone)]
pub struct DiffVertex {
    /// The vertex's label.
    pub label: String,
    /// Content-addressed identity (`compute_vertex_uid(label, None,
    /// properties)`). This is the bucketing key during diff.
    pub uid: UniId,
    /// Informational: which VID this row carried on the side it was
    /// scanned from. `None` if the per-side scan returned a node
    /// without a VID, which should not happen in practice.
    pub vid: Option<Vid>,
    /// Property bag for the vertex (user properties only).
    pub properties: Properties,
}

/// A change to one vertex's properties.
#[derive(Debug, Clone)]
pub struct VertexPropertyChange {
    /// The vertex's label.
    pub label: String,
    /// UID of the vertex — the pairing key across sides.
    pub uid: UniId,
    /// One entry per property whose value differs between sides.
    pub changes: Vec<PropertyChange>,
}

impl VertexPropertyChange {
    fn invert(self) -> Self {
        Self {
            label: self.label,
            uid: self.uid,
            changes: self.changes.into_iter().map(PropertyChange::invert).collect(),
        }
    }
}

/// An edge row from one side of a diff.
#[derive(Debug, Clone)]
pub struct DiffEdge {
    /// The edge type.
    pub edge_type: String,
    /// Content-addressed edge UID (computed via
    /// `MainEdgeDataset::compute_edge_uid` over
    /// `(src_uid, dst_uid, edge_type, sorted_properties)`). Two
    /// parallel edges between the same endpoints with different
    /// property bags have different `edge_uid`s — that's how the
    /// diff distinguishes them.
    pub edge_uid: UniId,
    /// Source vertex UID (content-addressed).
    pub src_uid: UniId,
    /// Destination vertex UID (content-addressed).
    pub dst_uid: UniId,
    /// Property bag for the edge.
    pub properties: Properties,
}

/// A change to one edge's properties.
#[derive(Debug, Clone)]
pub struct EdgePropertyChange {
    /// The edge type.
    pub edge_type: String,
    /// Source vertex UID.
    pub src_uid: UniId,
    /// Destination vertex UID.
    pub dst_uid: UniId,
    /// One entry per property whose value differs between sides.
    pub changes: Vec<PropertyChange>,
}

impl EdgePropertyChange {
    fn invert(self) -> Self {
        Self {
            edge_type: self.edge_type,
            src_uid: self.src_uid,
            dst_uid: self.dst_uid,
            changes: self.changes.into_iter().map(PropertyChange::invert).collect(),
        }
    }
}

/// A single property's before/after pair.
#[derive(Debug, Clone)]
pub struct PropertyChange {
    /// Property key.
    pub key: String,
    /// Value on the `a` side, or `None` if absent.
    pub before: Option<Value>,
    /// Value on the `b` side, or `None` if absent.
    pub after: Option<Value>,
}

impl PropertyChange {
    fn invert(self) -> Self {
        Self {
            key: self.key,
            before: self.after,
            after: self.before,
        }
    }
}

/// Selector for [`crate::api::Uni::promote_from_fork`].
///
/// Two shapes:
/// - [`PromotePattern::label`] — match every vertex with this label;
///   bulk-inserted on primary, deduplicated by content-derived UID.
/// - [`PromotePattern::edge_type`] — match every edge of this type
///   whose endpoints already exist on primary; the edge is inserted
///   between the resolved primary endpoints, deduplicated by
///   `(src_uid, dst_uid, edge_type)`.
///
/// Both variants accept an optional Cypher `WHERE` clause, interpolated
/// verbatim into the fork-side scan. Callers are responsible for
/// quoting and parameter safety.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PromotePattern {
    /// Promote vertices.
    Vertex {
        /// Vertex label.
        label: String,
        /// Optional `WHERE` predicate on the fork-side scan.
        where_clause: Option<String>,
    },
    /// Promote edges. Endpoints must already exist on primary (by UID);
    /// fork-only endpoints are skipped and counted in
    /// [`PromoteReport::edges_skipped_no_endpoint`].
    Edge {
        /// Edge type.
        edge_type: String,
        /// Optional `WHERE` predicate on the fork-side scan. The bound
        /// names are `a` (source), `r` (edge), `b` (destination).
        where_clause: Option<String>,
    },
}

impl PromotePattern {
    /// Match every vertex with this label.
    pub fn label(label: impl Into<String>) -> Self {
        Self::Vertex {
            label: label.into(),
            where_clause: None,
        }
    }

    /// Match every edge with this type. Endpoints must already exist
    /// on primary (resolved by UID); fork-only endpoints are counted
    /// and skipped — they need to be promoted first via a vertex
    /// pattern.
    pub fn edge_type(edge_type: impl Into<String>) -> Self {
        Self::Edge {
            edge_type: edge_type.into(),
            where_clause: None,
        }
    }

    /// Restrict the scan to rows matching this Cypher predicate.
    /// Verbatim interpolation — caller owns quoting.
    pub fn where_clause(mut self, expr: impl Into<String>) -> Self {
        let expr = expr.into();
        match &mut self {
            Self::Vertex { where_clause, .. } => *where_clause = Some(expr),
            Self::Edge { where_clause, .. } => *where_clause = Some(expr),
        }
        self
    }

    /// Vertex label for vertex patterns. Empty string for edge patterns.
    pub fn label_name(&self) -> &str {
        match self {
            Self::Vertex { label, .. } => label,
            Self::Edge { .. } => "",
        }
    }

    /// Edge type for edge patterns. Empty string for vertex patterns.
    pub fn edge_type_name(&self) -> &str {
        match self {
            Self::Edge { edge_type, .. } => edge_type,
            Self::Vertex { .. } => "",
        }
    }

    /// The optional `WHERE` predicate.
    pub fn where_expr(&self) -> Option<&str> {
        match self {
            Self::Vertex { where_clause, .. } | Self::Edge { where_clause, .. } => {
                where_clause.as_deref()
            }
        }
    }

    /// `true` if this pattern targets edges.
    pub fn is_edge(&self) -> bool {
        matches!(self, Self::Edge { .. })
    }
}

impl fmt::Display for PromotePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Vertex {
                label,
                where_clause: Some(w),
            } => write!(f, "(:{} WHERE {})", label, w),
            Self::Vertex {
                label,
                where_clause: None,
            } => write!(f, "(:{})", label),
            Self::Edge {
                edge_type,
                where_clause: Some(w),
            } => write!(f, "[:{} WHERE {}]", edge_type, w),
            Self::Edge {
                edge_type,
                where_clause: None,
            } => write!(f, "[:{}]", edge_type),
        }
    }
}

/// Outcome of [`crate::api::Uni::promote_from_fork`].
#[derive(Debug, Clone, Default)]
pub struct PromoteReport {
    /// Number of vertices inserted into primary.
    pub vertices_inserted: usize,
    /// Number of fork rows skipped because primary already has the same UID.
    pub vertices_skipped_uid_conflict: usize,
    /// Reserved for future use — currently always 0.
    pub vertices_skipped_no_uid: usize,
    /// Number of edges inserted into primary.
    pub edges_inserted: usize,
    /// Number of fork edges skipped because primary already has an
    /// edge of the same type between the resolved endpoints.
    pub edges_skipped_duplicate: usize,
    /// Number of fork edges skipped because at least one endpoint had
    /// no UID match on primary. To insert these edges, promote the
    /// missing vertices first via a vertex pattern, then re-run.
    pub edges_skipped_no_endpoint: usize,
    /// Number of edges that touched a promoted vertex but were not
    /// themselves promoted (no edge pattern in the call). Phase 6
    /// MVP's behaviour: silently skip + warn. Phase 6b adds explicit
    /// edge patterns; when no edge pattern is given, this counter
    /// still surfaces incidental edges for visibility.
    pub edges_skipped: usize,
    /// Per-pattern row counts so callers can see which pattern matched
    /// what. Indexed by pattern position in the input slice.
    pub per_pattern_inserted: Vec<usize>,
}

// ============================================================================
// Diff engine
// ============================================================================

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;
use uni_common::Result;

use super::session::Session;
use super::transaction::Transaction;

/// Compute the structural delta between two views.
///
/// Both `a` and `b` may be primary or forked sessions. The convention is
/// *forward*: returned `ForkDiff.vertices.added` is rows present in `b`
/// but not `a`; `deleted` is rows in `a` but not `b`.
///
/// Identity is content-addressed UID for vertices and `(src_uid,
/// dst_uid)` for edges, scoped by edge type — so two unrelated forks
/// with overlapping VIDs but distinct content pair correctly.
pub(crate) async fn compute_diff(a: &Session, b: &Session) -> Result<ForkDiff> {
    let mut diff = ForkDiff::default();

    let labels_a: HashSet<String> = a.db().schema.schema().labels.keys().cloned().collect();
    let labels_b: HashSet<String> = b.db().schema.schema().labels.keys().cloned().collect();
    let labels_union: Vec<&String> = labels_a.union(&labels_b).collect();

    for label in labels_union {
        let rows_a = scan_label_nodes(a, label).await?;
        let rows_b = scan_label_nodes(b, label).await?;
        diff_label(label, rows_a, rows_b, &mut diff.vertices);
    }

    let edges_a: HashSet<String> = a
        .db()
        .schema
        .schema()
        .edge_types
        .keys()
        .cloned()
        .collect();
    let edges_b: HashSet<String> = b
        .db()
        .schema
        .schema()
        .edge_types
        .keys()
        .cloned()
        .collect();
    let edges_union: Vec<&String> = edges_a.union(&edges_b).collect();

    for edge_type in edges_union {
        let rows_a = scan_edge_type(a, edge_type).await?;
        let rows_b = scan_edge_type(b, edge_type).await?;
        diff_edge_type(edge_type, rows_a, rows_b, &mut diff.edges);
    }

    Ok(diff)
}

/// One bucketed vertex row keyed by content UID.
type VertexBucket = HashMap<UniId, VertexRow>;
/// One bucketed edge row keyed by content-addressed edge UID
/// (`compute_edge_uid(src_uid, dst_uid, type, properties)`). Two
/// parallel edges between the same endpoints with different property
/// bags hash to different keys and therefore appear as distinct
/// entries — that's the Phase 7d multi-edge semantics.
type EdgeBucket = HashMap<UniId, EdgeRow>;

#[derive(Debug, Clone)]
struct VertexRow {
    label: String,
    vid: Vid,
    properties: Properties,
}

#[derive(Debug, Clone)]
struct EdgeRow {
    src_uid: UniId,
    dst_uid: UniId,
    properties: Properties,
}

async fn scan_label_nodes(s: &Session, label: &str) -> Result<VertexBucket> {
    use uni_store::storage::vertex::VertexDataset;
    let cypher = format!("MATCH (n:`{}`) RETURN n", escape_backticks(label));
    let result = s.query(&cypher).await?;
    let mut bucket = VertexBucket::new();
    for row in result.rows() {
        let Some(Value::Node(node)) = row.value("n") else {
            continue;
        };
        let row_label = node
            .labels
            .iter()
            .find(|l| l.as_str() == label)
            .cloned()
            .unwrap_or_else(|| label.to_string());
        let uid = VertexDataset::compute_vertex_uid(&row_label, None, &node.properties);
        bucket.insert(
            uid,
            VertexRow {
                label: row_label,
                vid: node.vid,
                properties: node.properties.clone(),
            },
        );
    }
    Ok(bucket)
}

async fn scan_edge_type(s: &Session, edge_type: &str) -> Result<EdgeBucket> {
    use uni_store::storage::main_edge::MainEdgeDataset;
    use uni_store::storage::vertex::VertexDataset;
    let cypher = format!(
        "MATCH (a)-[r:`{}`]->(b) RETURN a, r, b",
        escape_backticks(edge_type)
    );
    let result = s.query(&cypher).await?;
    let mut bucket = EdgeBucket::new();
    for row in result.rows() {
        let (Some(Value::Edge(edge)), Some(Value::Node(a)), Some(Value::Node(b))) =
            (row.value("r"), row.value("a"), row.value("b"))
        else {
            continue;
        };
        let a_label = a.labels.first().cloned().unwrap_or_default();
        let b_label = b.labels.first().cloned().unwrap_or_default();
        let src_uid = VertexDataset::compute_vertex_uid(&a_label, None, &a.properties);
        let dst_uid = VertexDataset::compute_vertex_uid(&b_label, None, &b.properties);
        let edge_uid = MainEdgeDataset::compute_edge_uid(
            &src_uid,
            &dst_uid,
            edge_type,
            &edge.properties,
        );
        bucket.insert(
            edge_uid,
            EdgeRow {
                src_uid,
                dst_uid,
                properties: edge.properties.clone(),
            },
        );
    }
    Ok(bucket)
}

fn diff_label(label: &str, a: VertexBucket, b: VertexBucket, out: &mut VertexDiff) {
    let keys_a: HashSet<UniId> = a.keys().copied().collect();
    let keys_b: HashSet<UniId> = b.keys().copied().collect();

    for uid in keys_b.difference(&keys_a) {
        let row = b[uid].clone();
        out.added.push(DiffVertex {
            label: row.label,
            uid: *uid,
            vid: Some(row.vid),
            properties: row.properties,
        });
    }
    for uid in keys_a.difference(&keys_b) {
        let row = a[uid].clone();
        out.deleted.push(DiffVertex {
            label: row.label,
            uid: *uid,
            vid: Some(row.vid),
            properties: row.properties,
        });
    }
    for uid in keys_a.intersection(&keys_b) {
        let row_a = &a[uid];
        let row_b = &b[uid];
        let changes = property_changes(&row_a.properties, &row_b.properties);
        if !changes.is_empty() {
            out.changed.push(VertexPropertyChange {
                label: label.to_string(),
                uid: *uid,
                changes,
            });
        }
    }
}

fn diff_edge_type(edge_type: &str, a: EdgeBucket, b: EdgeBucket, out: &mut EdgeDiff) {
    let keys_a: HashSet<UniId> = a.keys().copied().collect();
    let keys_b: HashSet<UniId> = b.keys().copied().collect();

    for edge_uid in keys_b.difference(&keys_a) {
        let row = b[edge_uid].clone();
        out.added.push(DiffEdge {
            edge_type: edge_type.to_string(),
            edge_uid: *edge_uid,
            src_uid: row.src_uid,
            dst_uid: row.dst_uid,
            properties: row.properties,
        });
    }
    for edge_uid in keys_a.difference(&keys_b) {
        let row = a[edge_uid].clone();
        out.deleted.push(DiffEdge {
            edge_type: edge_type.to_string(),
            edge_uid: *edge_uid,
            src_uid: row.src_uid,
            dst_uid: row.dst_uid,
            properties: row.properties,
        });
    }
    // Note: under content-addressed identity, two edges with the same
    // edge_uid have, by construction, identical (src, dst, type,
    // properties) — so the intersection cannot contain a property
    // difference. The `changed` branch is intentionally unreachable
    // under multi-edge semantics; property mutations surface as
    // added+deleted of distinct edge UIDs. `EdgePropertyChange`
    // remains in the public API for forward compatibility with a
    // future identity model that anchors on a stable edge id.
    let _ = (keys_a.intersection(&keys_b), out as &mut EdgeDiff);
}

fn property_changes(a: &Properties, b: &Properties) -> Vec<PropertyChange> {
    let mut changes = Vec::new();
    let keys: HashSet<&String> = a.keys().chain(b.keys()).collect();
    let mut sorted: Vec<&String> = keys.into_iter().collect();
    sorted.sort();
    for k in sorted {
        let va = a.get(k);
        let vb = b.get(k);
        if va != vb {
            changes.push(PropertyChange {
                key: k.clone(),
                before: va.cloned(),
                after: vb.cloned(),
            });
        }
    }
    changes
}

fn escape_backticks(s: &str) -> String {
    s.replace('`', "``")
}

/// Resolve a content-derived UID to the VID it has on `primary` (if
/// any). The shared `UidIndex` isn't fork-isolated — it returns a
/// candidate VID for any UID that has *ever* been registered, fork or
/// otherwise. This helper verifies the candidate by a primary Cypher
/// MATCH-by-VID; if the vertex isn't in primary's view, returns None.
async fn resolve_primary_vid(
    primary: &Session,
    primary_storage: &Arc<uni_store::storage::manager::StorageManager>,
    label: &str,
    uid: &uni_common::core::id::UniId,
) -> Option<Vid> {
    let candidate = primary_storage
        .uid_index(label)
        .ok()?
        .get_vid(uid)
        .await
        .ok()
        .flatten()?;
    // Cross-check: primary actually has a vertex with this VID.
    let cypher = format!(
        "MATCH (n:`{}`) WHERE id(n) = {} RETURN id(n) LIMIT 1",
        escape_backticks(label),
        candidate.as_u64()
    );
    let rs = primary.query(&cypher).await.ok()?;
    if rs.rows().is_empty() {
        None
    } else {
        Some(candidate)
    }
}

// ============================================================================
// Promote engine
// ============================================================================

/// Scan a fork session for matches per pattern, then bulk-insert the
/// matched vertices on primary (deduplicated by content-derived UID)
/// and edges (deduplicated by `(src_uid, dst_uid, edge_type)`).
///
/// Edges whose endpoints don't exist on primary by UID are skipped and
/// counted in `edges_skipped_no_endpoint` — promote the missing
/// vertices first via a vertex pattern, then re-run.
///
/// If the call contains no edge patterns, incidental edges on the fork
/// are counted in `edges_skipped` and a tracing warning is emitted.
pub(crate) async fn run_promote(
    fork: &Session,
    primary: &Session,
    primary_tx: &Transaction,
    patterns: &[PromotePattern],
) -> Result<PromoteReport> {
    use uni_store::storage::vertex::VertexDataset;

    let mut report = PromoteReport {
        per_pattern_inserted: vec![0usize; patterns.len()],
        ..Default::default()
    };

    let primary_storage = primary_tx.db.storage.clone();
    let mut any_edge_pattern = false;
    // Cache of vertices just promoted inside this call. Edge patterns
    // check this before falling back to primary's UidIndex + Cypher
    // verify — pending tx_l0 writes aren't visible to a primary
    // Cypher round-trip until commit, so without this cache an edge
    // pattern in the same call wouldn't see endpoints we just added.
    let mut just_inserted: HashMap<(String, UniId), Vid> = HashMap::new();

    for (idx, pattern) in patterns.iter().enumerate() {
        match pattern {
            PromotePattern::Vertex { label, where_clause } => {
                let cypher = match where_clause {
                    Some(w) => format!(
                        "MATCH (n:`{}`) WHERE {} RETURN n",
                        escape_backticks(label),
                        w
                    ),
                    None => format!("MATCH (n:`{}`) RETURN n", escape_backticks(label)),
                };

                let result = fork.query(&cypher).await?;
                if result.rows().is_empty() {
                    continue;
                }

                let mut to_insert: Vec<Properties> = Vec::with_capacity(result.rows().len());
                let mut insert_uids: Vec<UniId> = Vec::with_capacity(result.rows().len());
                for row in result.rows() {
                    let Some(Value::Node(node)) = row.value("n") else {
                        continue;
                    };
                    let uid = VertexDataset::compute_vertex_uid(label, None, &node.properties);
                    // Skip if already promoted earlier in this call.
                    if just_inserted.contains_key(&(label.clone(), uid)) {
                        report.vertices_skipped_uid_conflict += 1;
                        continue;
                    }
                    let conflict =
                        resolve_primary_vid(primary, &primary_storage, label, &uid)
                            .await
                            .is_some();
                    if conflict {
                        report.vertices_skipped_uid_conflict += 1;
                    } else {
                        to_insert.push(node.properties.clone());
                        insert_uids.push(uid);
                    }
                }

                if !to_insert.is_empty() {
                    let n = to_insert.len();
                    let vids = primary_tx.bulk_insert_vertices(label, to_insert).await?;
                    for (uid, vid) in insert_uids.into_iter().zip(vids) {
                        just_inserted.insert((label.clone(), uid), vid);
                    }
                    report.vertices_inserted += n;
                    report.per_pattern_inserted[idx] = n;
                }
            }
            PromotePattern::Edge { edge_type, where_clause } => {
                any_edge_pattern = true;
                let cypher = match where_clause {
                    Some(w) => format!(
                        "MATCH (a)-[r:`{}`]->(b) WHERE {} RETURN a, r, b",
                        escape_backticks(edge_type),
                        w
                    ),
                    None => format!(
                        "MATCH (a)-[r:`{}`]->(b) RETURN a, r, b",
                        escape_backticks(edge_type)
                    ),
                };

                let result = fork.query(&cypher).await?;
                if result.rows().is_empty() {
                    continue;
                }

                let mut pattern_inserted = 0usize;
                for row in result.rows() {
                    let (
                        Some(Value::Edge(edge)),
                        Some(Value::Node(a)),
                        Some(Value::Node(b)),
                    ) = (row.value("r"), row.value("a"), row.value("b")) else {
                        continue;
                    };
                    let a_label = match a.labels.first() {
                        Some(l) => l.clone(),
                        None => continue,
                    };
                    let b_label = match b.labels.first() {
                        Some(l) => l.clone(),
                        None => continue,
                    };
                    let src_uid =
                        VertexDataset::compute_vertex_uid(&a_label, None, &a.properties);
                    let dst_uid =
                        VertexDataset::compute_vertex_uid(&b_label, None, &b.properties);

                    // Check the just-inserted cache first (vertex
                    // pattern in this same call). Otherwise fall back
                    // to UidIndex + Cypher verify on primary; the
                    // verify step keeps fork-only registered UIDs
                    // from leaking through the shared UidIndex.
                    let src_vid = match just_inserted.get(&(a_label.clone(), src_uid)) {
                        Some(v) => Some(*v),
                        None => resolve_primary_vid(
                            primary,
                            &primary_storage,
                            &a_label,
                            &src_uid,
                        )
                        .await,
                    };
                    let dst_vid = match just_inserted.get(&(b_label.clone(), dst_uid)) {
                        Some(v) => Some(*v),
                        None => resolve_primary_vid(
                            primary,
                            &primary_storage,
                            &b_label,
                            &dst_uid,
                        )
                        .await,
                    };
                    let (src_vid, dst_vid) = match (src_vid, dst_vid) {
                        (Some(s), Some(d)) => (s, d),
                        _ => {
                            report.edges_skipped_no_endpoint += 1;
                            continue;
                        }
                    };

                    // Multi-edge-aware dedup (Phase 7d): the fork-side
                    // bucket is keyed by content edge UID, so within
                    // this call we never insert the same content
                    // twice. Against primary we have to enumerate all
                    // parallel edges between the resolved endpoints
                    // and compare each one's content UID — primary
                    // doesn't store edge UIDs (Phase 7d kept the
                    // edges-table schema unchanged), so we recompute.
                    use uni_store::storage::main_edge::MainEdgeDataset;
                    let fork_edge_uid = MainEdgeDataset::compute_edge_uid(
                        &src_uid,
                        &dst_uid,
                        edge_type,
                        &edge.properties,
                    );
                    let dedup_cypher = format!(
                        "MATCH (a)-[r:`{}`]->(b) WHERE id(a) = {} AND id(b) = {} RETURN r",
                        escape_backticks(edge_type),
                        src_vid.as_u64(),
                        dst_vid.as_u64()
                    );
                    let exists = match primary.query(&dedup_cypher).await {
                        Ok(rs) => rs.rows().iter().any(|row| {
                            let Some(Value::Edge(existing)) = row.value("r") else {
                                return false;
                            };
                            let existing_uid = MainEdgeDataset::compute_edge_uid(
                                &src_uid,
                                &dst_uid,
                                edge_type,
                                &existing.properties,
                            );
                            existing_uid == fork_edge_uid
                        }),
                        Err(_) => false,
                    };
                    if exists {
                        report.edges_skipped_duplicate += 1;
                        continue;
                    }

                    primary_tx
                        .bulk_insert_edges(
                            edge_type,
                            vec![(src_vid, dst_vid, edge.properties.clone())],
                        )
                        .await?;
                    report.edges_inserted += 1;
                    pattern_inserted += 1;
                }
                report.per_pattern_inserted[idx] = pattern_inserted;
            }
        }
    }

    // When the call contains no edge patterns, surface incidental edges
    // on the fork so callers see they exist (and weren't promoted).
    if !any_edge_pattern {
        let mut edge_seen = 0usize;
        for et in fork.db().schema.schema().edge_types.keys() {
            let cypher = format!(
                "MATCH ()-[r:`{}`]->() RETURN count(r) AS c",
                escape_backticks(et)
            );
            if let Ok(rs) = fork.query(&cypher).await
                && let Some(row) = rs.rows().first()
                && let Ok(c) = row.get::<i64>("c")
            {
                edge_seen += c as usize;
            }
        }
        if edge_seen > 0 {
            report.edges_skipped = edge_seen;
            warn!(
                target: "uni::promote",
                edges_skipped = edge_seen,
                "promote_from_fork: fork contains {} edges; pass \
                 PromotePattern::edge_type(...) to promote them",
                edge_seen
            );
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_change_inverts_before_after() {
        let pc = PropertyChange {
            key: "age".into(),
            before: Some(Value::Int(30)),
            after: Some(Value::Int(31)),
        };
        let inv = pc.clone().invert();
        assert_eq!(inv.before, pc.after);
        assert_eq!(inv.after, pc.before);
    }

    #[test]
    fn vertex_diff_invert_swaps_added_deleted() {
        let v_a = DiffVertex {
            label: "Person".into(),
            uid: UniId::from_bytes([1; 32]),
            vid: Some(Vid::new(1)),
            properties: Default::default(),
        };
        let v_b = DiffVertex {
            label: "Person".into(),
            uid: UniId::from_bytes([2; 32]),
            vid: Some(Vid::new(2)),
            properties: Default::default(),
        };
        let d = VertexDiff {
            added: vec![v_a.clone()],
            deleted: vec![v_b.clone()],
            changed: vec![],
        };
        let inv = d.invert();
        assert_eq!(inv.added.len(), 1);
        assert_eq!(inv.deleted.len(), 1);
    }

    #[test]
    fn fork_diff_default_is_empty() {
        let d = ForkDiff::default();
        assert!(d.is_empty());
        assert_eq!(d.total_rows(), 0);
    }

    #[test]
    fn promote_pattern_display() {
        let p = PromotePattern::label("Person");
        assert_eq!(format!("{}", p), "(:Person)");
        let p2 = PromotePattern::label("Person").where_clause("n.age > 30");
        assert_eq!(format!("{}", p2), "(:Person WHERE n.age > 30)");
    }
}
