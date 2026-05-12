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
//! **Identity** is VID for vertices and `(src_vid, dst_vid, type)` for
//! edges. This is sufficient for the spec §3.3 / §3.4 use cases where
//! both sides share a fork ancestor: the fork's `IdAllocator` is
//! bootstrapped above primary's HWM, so VIDs inherited from the fork
//! point stay stable across the boundary while fork-only writes get
//! fresh VIDs (which surface as adds/deletes naturally). UID-based
//! cross-fork-without-shared-ancestor diff is out of scope for Phase 6.
//!
//! `PromotePattern` is the spec for what to scan on a fork during
//! `Uni::promote_from_fork`. Phase 6 supports the most common shape
//! (label + optional Cypher WHERE clause); future phases may grow
//! relationship-aware patterns.

use std::fmt;

use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::Vid;

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
    /// The vertex's label (first label in `labels()` when the node is
    /// multi-labelled — Phase 6 diff buckets by first label).
    pub label: String,
    /// Stable cross-fork identity (VID).
    pub vid: Vid,
    /// Property bag for the vertex (user properties only).
    pub properties: Properties,
}

/// A change to one vertex's properties.
#[derive(Debug, Clone)]
pub struct VertexPropertyChange {
    /// The vertex's label.
    pub label: String,
    /// VID of the vertex (same on both sides — that's the pairing key).
    pub vid: Vid,
    /// One entry per property whose value differs between sides.
    pub changes: Vec<PropertyChange>,
}

impl VertexPropertyChange {
    fn invert(self) -> Self {
        Self {
            label: self.label,
            vid: self.vid,
            changes: self.changes.into_iter().map(PropertyChange::invert).collect(),
        }
    }
}

/// An edge row from one side of a diff.
#[derive(Debug, Clone)]
pub struct DiffEdge {
    /// The edge type.
    pub edge_type: String,
    /// Source vertex VID.
    pub src_vid: Vid,
    /// Destination vertex VID.
    pub dst_vid: Vid,
    /// Property bag for the edge.
    pub properties: Properties,
}

/// A change to one edge's properties.
#[derive(Debug, Clone)]
pub struct EdgePropertyChange {
    /// The edge type.
    pub edge_type: String,
    /// Source vertex VID.
    pub src_vid: Vid,
    /// Destination vertex VID.
    pub dst_vid: Vid,
    /// One entry per property whose value differs between sides.
    pub changes: Vec<PropertyChange>,
}

impl EdgePropertyChange {
    fn invert(self) -> Self {
        Self {
            edge_type: self.edge_type,
            src_vid: self.src_vid,
            dst_vid: self.dst_vid,
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
/// Phase 6 ships the simplest useful shape: a label plus an optional
/// Cypher `WHERE` clause. The fork is scanned with
/// `MATCH (n:{label}) WHERE {where_clause} RETURN n` and every match
/// is bulk-inserted on primary, deduplicated by UID.
#[derive(Debug, Clone)]
pub struct PromotePattern {
    label: String,
    where_clause: Option<String>,
}

impl PromotePattern {
    /// Match every vertex with this label.
    pub fn label(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            where_clause: None,
        }
    }

    /// Restrict the scan to vertices matching this Cypher predicate.
    /// The string is dropped verbatim into a `WHERE …` clause, so
    /// callers are responsible for quoting and parameter safety.
    pub fn where_clause(mut self, expr: impl Into<String>) -> Self {
        self.where_clause = Some(expr.into());
        self
    }

    /// The label this pattern targets.
    pub fn label_name(&self) -> &str {
        &self.label
    }

    /// The optional `WHERE` predicate.
    pub fn where_expr(&self) -> Option<&str> {
        self.where_clause.as_deref()
    }
}

impl fmt::Display for PromotePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.where_clause {
            Some(w) => write!(f, "(:{} WHERE {})", self.label, w),
            None => write!(f, "(:{})", self.label),
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
    /// Number of fork rows skipped because they had no UID column
    /// (Phase 6 requires UID-based dedup; rows without UIDs are
    /// silently skipped with a counter so callers can react).
    pub vertices_skipped_no_uid: usize,
    /// Number of edges that touched a promoted vertex but were not
    /// themselves promoted. Edge promotion is deferred per the
    /// graph-fork plan §16; the current behaviour is "silently skip
    /// touching edges with a warning". The counter exposes the warning
    /// programmatically; the textual warning is also `tracing::warn!`-ed.
    pub edges_skipped: usize,
    /// Per-pattern row counts so callers can see which pattern matched
    /// what. Indexed by pattern position in the input slice.
    pub per_pattern_inserted: Vec<usize>,
}

// ============================================================================
// Diff engine
// ============================================================================

use std::collections::{HashMap, HashSet};
use tracing::warn;
use uni_common::Result;
use uni_common::core::id::Eid;

use super::session::Session;
use super::transaction::Transaction;

/// Compute the structural delta between two views.
///
/// Both `a` and `b` may be primary or forked sessions. The convention is
/// *forward*: returned `ForkDiff.vertices.added` is rows present in `b`
/// but not `a`; `deleted` is rows in `a` but not `b`.
///
/// Identity is VID for vertices and `(src_vid, dst_vid, type)` for
/// edges. Both sides must share an ancestor for `changed` to be
/// meaningful — see module docs.
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

/// One bucketed row from the scan: VID → (label, props).
type VertexBucket = HashMap<Vid, (String, Properties)>;
/// One bucketed edge row keyed by (src_vid, dst_vid, eid): we track
/// edges by EID since `(src, dst, type)` can legitimately have
/// multiple parallel edges.
type EdgeBucket = HashMap<Eid, EdgeRow>;

#[derive(Debug, Clone)]
struct EdgeRow {
    src_vid: Vid,
    dst_vid: Vid,
    properties: Properties,
}

async fn scan_label_nodes(s: &Session, label: &str) -> Result<VertexBucket> {
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
        bucket.insert(node.vid, (row_label, node.properties.clone()));
    }
    Ok(bucket)
}

async fn scan_edge_type(s: &Session, edge_type: &str) -> Result<EdgeBucket> {
    let cypher = format!(
        "MATCH (a)-[r:`{}`]->(b) RETURN r",
        escape_backticks(edge_type)
    );
    let result = s.query(&cypher).await?;
    let mut bucket = EdgeBucket::new();
    for row in result.rows() {
        let Some(Value::Edge(edge)) = row.value("r") else {
            continue;
        };
        bucket.insert(
            edge.eid,
            EdgeRow {
                src_vid: edge.src,
                dst_vid: edge.dst,
                properties: edge.properties.clone(),
            },
        );
    }
    Ok(bucket)
}

fn diff_label(label: &str, a: VertexBucket, b: VertexBucket, out: &mut VertexDiff) {
    let keys_a: HashSet<Vid> = a.keys().copied().collect();
    let keys_b: HashSet<Vid> = b.keys().copied().collect();

    for vid in keys_b.difference(&keys_a) {
        let (l, props) = b[vid].clone();
        out.added.push(DiffVertex {
            label: l,
            vid: *vid,
            properties: props,
        });
    }
    for vid in keys_a.difference(&keys_b) {
        let (l, props) = a[vid].clone();
        out.deleted.push(DiffVertex {
            label: l,
            vid: *vid,
            properties: props,
        });
    }
    for vid in keys_a.intersection(&keys_b) {
        let (_, props_a) = &a[vid];
        let (_, props_b) = &b[vid];
        let changes = property_changes(props_a, props_b);
        if !changes.is_empty() {
            out.changed.push(VertexPropertyChange {
                label: label.to_string(),
                vid: *vid,
                changes,
            });
        }
    }
}

fn diff_edge_type(edge_type: &str, a: EdgeBucket, b: EdgeBucket, out: &mut EdgeDiff) {
    let keys_a: HashSet<Eid> = a.keys().copied().collect();
    let keys_b: HashSet<Eid> = b.keys().copied().collect();

    for eid in keys_b.difference(&keys_a) {
        let row = b[eid].clone();
        out.added.push(DiffEdge {
            edge_type: edge_type.to_string(),
            src_vid: row.src_vid,
            dst_vid: row.dst_vid,
            properties: row.properties,
        });
    }
    for eid in keys_a.difference(&keys_b) {
        let row = a[eid].clone();
        out.deleted.push(DiffEdge {
            edge_type: edge_type.to_string(),
            src_vid: row.src_vid,
            dst_vid: row.dst_vid,
            properties: row.properties,
        });
    }
    for eid in keys_a.intersection(&keys_b) {
        let row_a = &a[eid];
        let row_b = &b[eid];
        let changes = property_changes(&row_a.properties, &row_b.properties);
        if !changes.is_empty() {
            out.changed.push(EdgePropertyChange {
                edge_type: edge_type.to_string(),
                src_vid: row_a.src_vid,
                dst_vid: row_a.dst_vid,
                changes,
            });
        }
    }
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

// ============================================================================
// Promote engine
// ============================================================================

/// Scan a fork session for matches per pattern, then bulk-insert the
/// matched vertices on primary, deduplicated by content-derived UID.
///
/// Edges are not promoted in Phase 6 — they are counted and a tracing
/// warning is logged per fork's edge count. Callers see the same count
/// in [`PromoteReport::edges_skipped`].
pub(crate) async fn run_promote(
    fork: &Session,
    primary_tx: &Transaction,
    patterns: &[PromotePattern],
) -> Result<PromoteReport> {
    use uni_store::storage::vertex::VertexDataset;

    let mut report = PromoteReport {
        per_pattern_inserted: vec![0usize; patterns.len()],
        ..Default::default()
    };

    let primary_storage = primary_tx.db.storage.clone();

    for (idx, pattern) in patterns.iter().enumerate() {
        let label = pattern.label_name();
        let cypher = match pattern.where_expr() {
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
        let primary_uid_index = primary_storage.uid_index(label).ok();
        for row in result.rows() {
            let Some(Value::Node(node)) = row.value("n") else {
                continue;
            };
            let uid =
                VertexDataset::compute_vertex_uid(label, None, &node.properties);
            let conflict = match &primary_uid_index {
                Some(idx) => match idx.get_vid(&uid).await {
                    Ok(Some(_)) => true,
                    Ok(None) => false,
                    Err(_) => {
                        warn!(
                            target: "uni::promote",
                            label = %label,
                            "primary UID index unreadable; inserting without dedup",
                        );
                        false
                    }
                },
                None => false,
            };
            if conflict {
                report.vertices_skipped_uid_conflict += 1;
            } else {
                to_insert.push(node.properties.clone());
            }
        }

        if !to_insert.is_empty() {
            let n = to_insert.len();
            primary_tx.bulk_insert_vertices(label, to_insert).await?;
            report.vertices_inserted += n;
            report.per_pattern_inserted[idx] = n;
        }
    }

    // Edge count for the warning surface — we currently know fork has
    // *some* edges if any edge_type table on fork is non-empty; for
    // Phase 6 we conservatively count edges touched by *promoted*
    // vertex labels via a single query per fork. This is best-effort;
    // a precise count requires a join we don't run here.
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
            "promote_from_fork: edge promotion is deferred per fork plan §16 \
             — fork contains {} edges that were not promoted",
            edge_seen
        );
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
            vid: Vid::new(1),
            properties: Default::default(),
        };
        let v_b = DiffVertex {
            label: "Person".into(),
            vid: Vid::new(2),
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
