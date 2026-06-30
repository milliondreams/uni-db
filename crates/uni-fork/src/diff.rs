// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork diff & promote engine (Phase 6+).
//!
//! `compute_diff` computes the structural delta between two views; `run_promote`
//! scans a fork for matched rows and bulk-inserts them onto primary. Both are
//! generic over the [`ForkQueryHost`] / [`ForkPromoteSink`] host traits that
//! uni-db implements for its `Session`/`Transaction` types.

// ============================================================================
// Diff engine
// ============================================================================

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;
use uni_common::Properties;
use uni_common::Result;
use uni_common::Value;
use uni_common::core::id::{UniId, Vid};

use crate::host::{ForkPromoteSink, ForkQueryHost};
use crate::types::{
    ConflictPolicy, DiffEdge, DiffVertex, EdgeDiff, ForkDiff, PromoteBaseline, PromoteOptions,
    PromotePattern, PromoteReport, PropertyChange, VertexDiff, VertexPropertyChange,
};

/// Compute the structural delta between two views.
///
/// Both `a` and `b` may be primary or forked sessions. The convention is
/// *forward*: returned `ForkDiff.vertices.added` is rows present in `b`
/// but not `a`; `deleted` is rows in `a` but not `b`.
///
/// Identity is content-addressed UID for vertices and `(src_uid,
/// dst_uid)` for edges, scoped by edge type — so two unrelated forks
/// with overlapping VIDs but distinct content pair correctly.
pub async fn compute_diff<Q: ForkQueryHost + ?Sized>(a: &Q, b: &Q) -> Result<ForkDiff> {
    let mut diff = ForkDiff::default();

    // vid → ext_id per side: ext_id is folded into the content UID but stripped
    // from query results, so look it up from storage (review H4).
    let ext_a = a.storage().get_vertex_ext_ids().await.unwrap_or_default();
    let ext_b = b.storage().get_vertex_ext_ids().await.unwrap_or_default();

    let labels_a: HashSet<String> = a.schema().schema().labels.keys().cloned().collect();
    let labels_b: HashSet<String> = b.schema().schema().labels.keys().cloned().collect();
    let labels_union: Vec<&String> = labels_a.union(&labels_b).collect();

    for label in labels_union {
        let rows_a = scan_label_nodes(a, label, &ext_a).await?;
        let rows_b = scan_label_nodes(b, label, &ext_b).await?;
        diff_label(label, rows_a, rows_b, &mut diff.vertices);
    }

    let edges_a: HashSet<String> = a.schema().schema().edge_types.keys().cloned().collect();
    let edges_b: HashSet<String> = b.schema().schema().edge_types.keys().cloned().collect();
    let edges_union: Vec<&String> = edges_a.union(&edges_b).collect();

    for edge_type in edges_union {
        let rows_a = scan_edge_type(a, edge_type, &ext_a).await?;
        let rows_b = scan_edge_type(b, edge_type, &ext_b).await?;
        diff_edge_type(edge_type, rows_a, rows_b, &mut diff.edges);
    }

    Ok(diff)
}

/// A vertex's `ext_id` for content-UID computation, from a `vid → ext_id` map
/// sourced from storage (`StorageManager::get_vertex_ext_ids`).
///
/// `ext_id` is folded into the storage `_uid` but is stripped from query
/// results, so the diff can't recover it by re-hashing query rows — two
/// vertices differing only by `ext_id` would collapse to one identity (review
/// H4). We fold it back into the *recomputed* UID (not the storage `_uid`,
/// which diverges from a recompute and breaks L0/flushed consistency). Vertices
/// without an `ext_id` are absent from the map → `None`, i.e. unchanged
/// behavior. Limitation: covers flushed rows; a vertex created fork-local and
/// not yet flushed is absent from the map (its `ext_id` collapse only matters
/// for promote, and flushing the fork closes it).
fn ext_id_for(map: &HashMap<Vid, String>, vid: Vid) -> Option<&str> {
    map.get(&vid).map(String::as_str)
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

async fn scan_label_nodes<Q: ForkQueryHost + ?Sized>(
    s: &Q,
    label: &str,
    ext_ids: &HashMap<Vid, String>,
) -> Result<VertexBucket> {
    use uni_store::storage::vertex::VertexDataset;
    let cypher = format!("MATCH (n:`{}`) RETURN n", escape_backticks(label));
    let result = s.query(&cypher).await?;
    let mut bucket = VertexBucket::new();
    for row in result.rows() {
        let Some(Value::Node(node)) = row.value("n") else {
            continue;
        };
        // The MATCH already filters to nodes carrying `label`, so the bucketed
        // row's label is always `label`. Fold the stored `ext_id` into the UID
        // so ext_id-distinct vertices don't collapse (review H4).
        let uid = VertexDataset::compute_vertex_uid(
            label,
            ext_id_for(ext_ids, node.vid),
            &node.properties,
        );
        if bucket
            .insert(
                uid,
                VertexRow {
                    label: label.to_string(),
                    vid: node.vid,
                    properties: node.properties.clone(),
                },
            )
            .is_some()
        {
            // Two distinct vertices hashed to the same content UID — one will be
            // dropped from the diff. Observable signal for residual identity
            // collisions (review H4).
            warn!(
                label,
                vid = node.vid.as_u64(),
                "fork diff: vertex content-UID collision; a row is being shadowed"
            );
        }
    }
    Ok(bucket)
}

async fn scan_edge_type<Q: ForkQueryHost + ?Sized>(
    s: &Q,
    edge_type: &str,
    ext_ids: &HashMap<Vid, String>,
) -> Result<EdgeBucket> {
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
        let src_uid =
            VertexDataset::compute_vertex_uid(&a_label, ext_id_for(ext_ids, a.vid), &a.properties);
        let dst_uid =
            VertexDataset::compute_vertex_uid(&b_label, ext_id_for(ext_ids, b.vid), &b.properties);
        let edge_uid =
            MainEdgeDataset::compute_edge_uid(&src_uid, &dst_uid, edge_type, &edge.properties);
        if bucket
            .insert(
                edge_uid,
                EdgeRow {
                    src_uid,
                    dst_uid,
                    properties: edge.properties.clone(),
                },
            )
            .is_some()
        {
            warn!(
                edge_type,
                "fork diff: edge content-UID collision; a row is being shadowed"
            );
        }
    }
    Ok(bucket)
}

/// Split two content-keyed buckets into *added* (present in `b`, not `a`)
/// and *deleted* (present in `a`, not `b`) rows, moving each row out of its
/// owning map via the supplied builders. Returns the rows shared by both
/// buckets (`(uid, row_a, row_b)`) so the caller can diff their properties.
fn partition_added_deleted<R, A, D>(
    mut a: HashMap<UniId, R>,
    mut b: HashMap<UniId, R>,
    mut mk_added: A,
    mut mk_deleted: D,
) -> Vec<(UniId, R, R)>
where
    A: FnMut(UniId, R),
    D: FnMut(UniId, R),
{
    let keys_a: HashSet<UniId> = a.keys().copied().collect();
    let keys_b: HashSet<UniId> = b.keys().copied().collect();

    let mut common = Vec::new();
    for uid in &keys_b {
        if !keys_a.contains(uid) {
            mk_added(*uid, b.remove(uid).expect("key from keys_b"));
        }
    }
    for uid in &keys_a {
        if keys_b.contains(uid) {
            let row_a = a.remove(uid).expect("key from keys_a");
            let row_b = b.remove(uid).expect("shared key in b");
            common.push((*uid, row_a, row_b));
        } else {
            mk_deleted(*uid, a.remove(uid).expect("key from keys_a"));
        }
    }
    common
}

fn diff_label(label: &str, a: VertexBucket, b: VertexBucket, out: &mut VertexDiff) {
    let common = partition_added_deleted(
        a,
        b,
        |uid, row| {
            out.added.push(DiffVertex {
                label: row.label,
                uid,
                vid: Some(row.vid),
                properties: row.properties,
            });
        },
        |uid, row| {
            out.deleted.push(DiffVertex {
                label: row.label,
                uid,
                vid: Some(row.vid),
                properties: row.properties,
            });
        },
    );
    for (uid, row_a, row_b) in common {
        let changes = property_changes(&row_a.properties, &row_b.properties);
        if !changes.is_empty() {
            out.changed.push(VertexPropertyChange {
                label: label.to_string(),
                uid,
                changes,
            });
        }
    }
}

fn diff_edge_type(edge_type: &str, a: EdgeBucket, b: EdgeBucket, out: &mut EdgeDiff) {
    // Note: under content-addressed identity, two edges with the same
    // edge_uid have, by construction, identical (src, dst, type,
    // properties) — so the shared (intersection) rows cannot contain a
    // property difference. The `changed` branch is intentionally
    // unreachable under multi-edge semantics; property mutations surface
    // as added+deleted of distinct edge UIDs. `EdgePropertyChange` remains
    // in the public API for forward compatibility with a future identity
    // model that anchors on a stable edge id. We therefore discard the
    // common rows.
    partition_added_deleted(
        a,
        b,
        |edge_uid, row| {
            out.added.push(DiffEdge {
                edge_type: edge_type.to_string(),
                edge_uid,
                src_uid: row.src_uid,
                dst_uid: row.dst_uid,
                properties: row.properties,
            });
        },
        |edge_uid, row| {
            out.deleted.push(DiffEdge {
                edge_type: edge_type.to_string(),
                edge_uid,
                src_uid: row.src_uid,
                dst_uid: row.dst_uid,
                properties: row.properties,
            });
        },
    );
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

/// Render an iterator of VID-bearing values as a comma-separated list of
/// their `u64` ids for a Cypher `id(n) IN [...]` clause.
fn vid_in_list(vids: impl IntoIterator<Item = u64>) -> String {
    vids.into_iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Resolve a set of UIDs to their primary VIDs in two queries
/// regardless of the input size.
///
/// Returns a `HashMap<UniId, Vid>` containing only those UIDs that
/// successfully resolve to a *primary* VID (i.e., a candidate VID
/// from the shared `UidIndex` is actually present in primary's view
/// of the label's vertex table). UIDs absent from the result map
/// either had no candidate registered or all candidates pointed at
/// fork-only rows.
///
/// Two queries per call regardless of `uids.len()`: one IN-filter
/// scan of `UidIndex`'s dataset (collecting **all** registered VIDs
/// per UID — `UidIndex::resolve_uids` collapses to one VID per UID
/// which loses fork/primary disambiguation), and one primary Cypher
/// MATCH with an `id(n) IN [...]` predicate to confirm which
/// candidates live on primary.
async fn batch_resolve_primary_vids<Q: ForkQueryHost + ?Sized>(
    primary: &Q,
    primary_storage: &Arc<uni_store::storage::manager::StorageManager>,
    label: &str,
    uids: &[UniId],
) -> (HashMap<UniId, Vid>, bool) {
    // NOTE: every error path below degrades to whatever has been
    // resolved so far (an empty or partial map) rather than
    // propagating. This is deliberate: `run_promote` treats an
    // unresolved UID as "not present on primary" and inserts it, so a
    // transient resolve failure must not abort the promote. The returned
    // `degraded` flag (M5) tells the caller that "absent" was inferred
    // from a failed resolve, so the resulting inserts are unverified and
    // may be duplicates — surfaced as `vertices_inserted_unverified`.
    let mut out: HashMap<UniId, Vid> = HashMap::new();
    if uids.is_empty() {
        return (out, false);
    }
    // Collect *all* candidate VIDs per UID by scanning the shared
    // UidIndex with an IN filter. The shared index is not
    // branch-isolated, so a single UID may have a fork-only VID and
    // a primary VID both registered — we keep both and let the
    // primary Cypher MATCH below decide which is real.
    // A missing index or a failed scan both degrade to "not present" (the
    // `degraded` flag tells the caller the resulting inserts are unverified).
    let Ok(uix) = primary_storage.uid_index(label) else {
        return (out, true);
    };
    let Ok(candidates_per_uid) = resolve_all_candidate_vids(&uix, uids).await else {
        return (out, true);
    };
    if candidates_per_uid.is_empty() {
        return (out, false);
    }
    // Single Cypher with IN clause over every candidate VID across
    // every UID. Primary's branched backend filters out fork-only
    // VIDs naturally — they have no row in the primary view.
    let vid_set: HashSet<u64> = candidates_per_uid
        .values()
        .flat_map(|vs| vs.iter().map(|v| v.as_u64()))
        .collect();
    let cypher = format!(
        "MATCH (n:`{}`) WHERE id(n) IN [{}] RETURN id(n) AS vid",
        escape_backticks(label),
        vid_in_list(vid_set)
    );
    let Ok(rs) = primary.query(&cypher).await else {
        return (out, true);
    };
    let primary_vids: HashSet<u64> = rs
        .rows()
        .iter()
        .filter_map(|row| row.get::<i64>("vid").ok())
        .map(|v| v as u64)
        .collect();
    for (uid, vids) in candidates_per_uid {
        // If *any* candidate VID for this UID lives on primary, the
        // UID exists on primary. Pick the first such VID.
        if let Some(vid) = vids
            .into_iter()
            .find(|v| primary_vids.contains(&v.as_u64()))
        {
            out.insert(uid, vid);
        }
    }
    (out, false)
}

/// Resolve fork candidate vertices to existing primary VIDs by their
/// stable `(label, ext_id)` identity, returning each match's current
/// primary properties for the upsert equality check.
///
/// Unlike [`batch_resolve_primary_vids`] (which keys by mutable
/// content-UID and so cannot recognize an *edited* vertex), this keys by
/// the immutable `ext_id`, so a fork edit resolves to the same primary
/// vertex instead of looking like a brand-new row. Fork rows whose
/// `ext_id` is absent are not returned here and fall back to the
/// content-UID path.
///
/// A failed primary round-trip degrades to an empty map (treated as "not
/// present" → insert), matching the deliberate non-aborting contract.
async fn batch_resolve_primary_by_ext_id<Q: ForkQueryHost + ?Sized>(
    primary: &Q,
    primary_ext_ids: &HashMap<Vid, String>,
    label: &str,
    ext_ids: &HashSet<String>,
) -> HashMap<String, (Vid, Properties)> {
    let mut out: HashMap<String, (Vid, Properties)> = HashMap::new();
    if ext_ids.is_empty() {
        return out;
    }
    // Invert primary's vid→ext_id map for just the candidate ext_ids.
    // `get_vertex_ext_ids` is not label-scoped, so the Cypher below
    // confirms the label (and fetches current props).
    let mut ext_to_vid: HashMap<String, Vid> = HashMap::new();
    for (vid, eid) in primary_ext_ids {
        if ext_ids.contains(eid) {
            ext_to_vid.insert(eid.clone(), *vid);
        }
    }
    if ext_to_vid.is_empty() {
        return out;
    }
    let cypher = format!(
        "MATCH (n:`{}`) WHERE id(n) IN [{}] RETURN id(n) AS vid, n AS node",
        escape_backticks(label),
        vid_in_list(ext_to_vid.values().map(|v| v.as_u64()))
    );
    let Ok(rs) = primary.query(&cypher).await else {
        return out;
    };
    let mut vid_to_props: HashMap<u64, Properties> = HashMap::new();
    for row in rs.rows() {
        if let Ok(vid) = row.get::<i64>("vid")
            && let Some(Value::Node(node)) = row.value("node")
        {
            vid_to_props.insert(vid as u64, node.properties.clone());
        }
    }
    for (eid, vid) in ext_to_vid {
        if let Some(props) = vid_to_props.get(&vid.as_u64()) {
            out.insert(eid, (vid, props.clone()));
        }
    }
    out
}

/// Scan `UidIndex`'s underlying dataset with an `_uid_hex IN (...)`
/// filter and collect **every** VID registered for each UID — unlike
/// `UidIndex::resolve_uids`, which collapses to one VID per UID via
/// HashMap overwrite (losing fork-vs-primary disambiguation).
async fn resolve_all_candidate_vids(
    uix: &uni_store::storage::index::UidIndex,
    uids: &[UniId],
) -> uni_common::Result<HashMap<UniId, Vec<Vid>>> {
    use arrow_array::Array;
    use futures::TryStreamExt;

    // Lance/DataFusion errors all wrap uniformly as `Internal`; the
    // generic bound lets one helper cover the scan-builder and stream
    // error types alike.
    fn internal<E>(e: E) -> uni_common::UniError
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        uni_common::UniError::Internal(anyhow::anyhow!(e))
    }

    let ds = uix.open().await.map_err(uni_common::UniError::Internal)?;
    let hex_values: Vec<String> = uids.iter().map(uid_to_hex).collect();
    let filter = format!(
        "_uid_hex IN ({})",
        hex_values
            .iter()
            .map(|h| format!("'{}'", h))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut stream = ds
        .scan()
        .filter(&filter)
        .map_err(internal)?
        .project(&["_uid_hex", "_vid"])
        .map_err(internal)?
        .try_into_stream()
        .await
        .map_err(internal)?;

    let hex_to_uid: HashMap<String, UniId> =
        uids.iter().map(|uid| (uid_to_hex(uid), *uid)).collect();
    let mut out: HashMap<UniId, Vec<Vid>> = HashMap::new();
    while let Some(batch) = stream.try_next().await.map_err(internal)? {
        let uid_hex_col = batch
            .column_by_name("_uid_hex")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>())
            .ok_or_else(|| {
                uni_common::UniError::Internal(anyhow::anyhow!("Missing _uid_hex column"))
            })?;
        let vid_col = batch
            .column_by_name("_vid")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>())
            .ok_or_else(|| {
                uni_common::UniError::Internal(anyhow::anyhow!("Missing _vid column"))
            })?;
        for i in 0..batch.num_rows() {
            if uid_hex_col.is_null(i) {
                continue;
            }
            let hex = uid_hex_col.value(i);
            if let Some(&uid) = hex_to_uid.get(hex) {
                out.entry(uid)
                    .or_default()
                    .push(Vid::from(vid_col.value(i)));
            }
        }
    }
    Ok(out)
}

fn uid_to_hex(uid: &UniId) -> String {
    use std::fmt::Write as _;

    let bytes = uid.as_bytes();
    let mut hex = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Infallible: writing to a `String` never errors.
        let _ = write!(hex, "{b:02x}");
    }
    hex
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
pub async fn run_promote<Q, S>(
    fork: &Q,
    primary: &Q,
    primary_tx: &S,
    patterns: &[PromotePattern],
    options: &PromoteOptions,
    baseline: Option<&PromoteBaseline>,
) -> Result<PromoteReport>
where
    Q: ForkQueryHost + ?Sized,
    S: ForkPromoteSink + ?Sized,
{
    use uni_store::storage::vertex::VertexDataset;

    let mut report = PromoteReport {
        per_pattern_inserted: vec![0usize; patterns.len()],
        ..Default::default()
    };

    let primary_storage = primary.storage();
    // vid → ext_id maps so promote keys candidates by the same ext_id-aware
    // content UID, distinguishing ext_id-distinct rows (review H4).
    let fork_ext_ids = fork
        .storage()
        .get_vertex_ext_ids()
        .await
        .unwrap_or_default();
    let primary_ext_ids = primary_storage
        .get_vertex_ext_ids()
        .await
        .unwrap_or_default();
    let mut any_edge_pattern = false;
    // Cache of vertices just promoted inside this call. Edge patterns
    // check this before falling back to primary's UidIndex + Cypher
    // verify — pending tx_l0 writes aren't visible to a primary
    // Cypher round-trip until commit, so without this cache an edge
    // pattern in the same call wouldn't see endpoints we just added.
    let mut just_inserted: HashMap<(String, UniId), Vid> = HashMap::new();

    for (idx, pattern) in patterns.iter().enumerate() {
        match pattern {
            PromotePattern::Vertex {
                label,
                where_clause,
            } => {
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

                // First pass: extract (uid, props, ext_id) for every fork
                // row, skipping rows already in the within-call cache.
                let mut candidates: Vec<(UniId, Properties, Option<String>)> =
                    Vec::with_capacity(result.rows().len());
                for row in result.rows() {
                    let Some(Value::Node(node)) = row.value("n") else {
                        continue;
                    };
                    let ext_id = ext_id_for(&fork_ext_ids, node.vid).map(str::to_string);
                    let uid = VertexDataset::compute_vertex_uid(
                        label,
                        ext_id.as_deref(),
                        &node.properties,
                    );
                    if just_inserted.contains_key(&(label.clone(), uid)) {
                        report.vertices_skipped_uid_conflict += 1;
                        continue;
                    }
                    candidates.push((uid, node.properties.clone(), ext_id));
                }

                // M4 upsert: resolve ext_id-bearing candidates against
                // primary by their stable `(label, ext_id)` identity so a
                // fork EDIT updates the existing vertex instead of inserting
                // a twin. Only consulted when `options.upsert`.
                let ext_resolved: HashMap<String, (Vid, Properties)> = if options.upsert {
                    let ext_ids: HashSet<String> = candidates
                        .iter()
                        .filter_map(|(_, _, e)| e.clone())
                        .collect();
                    batch_resolve_primary_by_ext_id(primary, &primary_ext_ids, label, &ext_ids)
                        .await
                } else {
                    HashMap::new()
                };

                // Per-label fork-point baseline (merge mode only).
                let label_baseline = baseline.and_then(|b| b.ext.get(label));

                // Partition: ext_id matches become in-place upserts; every
                // other candidate flows through the content-UID
                // insert-or-skip path (unchanged legacy behavior).
                let mut uid_candidates: Vec<(UniId, Properties)> =
                    Vec::with_capacity(candidates.len());
                for (uid, props, ext_id) in candidates {
                    let resolved = ext_id
                        .as_ref()
                        .and_then(|e| ext_resolved.get(e).map(|r| (e.clone(), r)));
                    let Some((eid, (pvid, pprops))) = resolved else {
                        uid_candidates.push((uid, props));
                        continue;
                    };
                    match label_baseline.and_then(|m| m.get(&eid)) {
                        // Baseline-aware merge (with_merge): reconcile the
                        // fork value `props` against primary-now `pprops` and
                        // the fork-point baseline `b`.
                        Some(b) => {
                            if props == *pprops {
                                // Already converged — keeps re-promote
                                // idempotent. Must be checked first.
                                report.vertices_skipped_no_op += 1;
                            } else if props == *b {
                                // Fork left this vertex untouched since the
                                // fork point — never revert primary's edit.
                                report.vertices_skipped_no_op += 1;
                            } else if *pprops != *b {
                                // Both sides moved off baseline → conflict.
                                report.vertices_conflicting += 1;
                                if options.on_conflict == ConflictPolicy::Overwrite {
                                    primary_tx
                                        .update_vertex_properties(label, *pvid, props)
                                        .await?;
                                    report.vertices_updated += 1;
                                }
                            } else {
                                // Only the fork changed → clean fast-forward.
                                primary_tx
                                    .update_vertex_properties(label, *pvid, props)
                                    .await?;
                                report.vertices_updated += 1;
                            }
                        }
                        // No baseline for this ext_id: fork-wins upsert.
                        None => {
                            if props == *pprops {
                                report.vertices_skipped_no_op += 1;
                            } else {
                                primary_tx
                                    .update_vertex_properties(label, *pvid, props)
                                    .await?;
                                report.vertices_updated += 1;
                            }
                        }
                    }
                }

                // Batch-resolve the remaining candidates by content-UID.
                // Two queries total per pattern (UidIndex.resolve_uids +
                // Cypher IN-clause verify) instead of 2N. `degraded` (M5)
                // signals the resolve could not confirm presence.
                let uids_to_check: Vec<UniId> = uid_candidates.iter().map(|(u, _)| *u).collect();
                let (on_primary, degraded) =
                    batch_resolve_primary_vids(primary, &primary_storage, label, &uids_to_check)
                        .await;

                let mut to_insert: Vec<Properties> = Vec::with_capacity(uid_candidates.len());
                let mut insert_uids: Vec<UniId> = Vec::with_capacity(uid_candidates.len());
                for (uid, props) in uid_candidates {
                    if on_primary.contains_key(&uid) {
                        report.vertices_skipped_uid_conflict += 1;
                    } else {
                        to_insert.push(props);
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
                    // M5: presence could not be confirmed for this batch, so
                    // some of these inserts may be duplicates of existing
                    // primary rows. Surface it instead of silently dup'ing.
                    if degraded {
                        report.vertices_inserted_unverified += n;
                        warn!(
                            label = %label,
                            count = n,
                            "promote inserted vertices whose primary presence could not be \
                             confirmed (resolve degraded); they may be duplicates"
                        );
                    }
                }
            }
            PromotePattern::Edge {
                edge_type,
                where_clause,
            } => {
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

                use uni_store::storage::main_edge::MainEdgeDataset;

                // First pass: extract every fork edge into a typed
                // record so we can batch-resolve endpoints and
                // pre-fetch primary parallel edges in one shot each.
                struct ForkEdgeRow {
                    a_label: String,
                    b_label: String,
                    src_uid: UniId,
                    dst_uid: UniId,
                    edge_uid: UniId,
                    edge_props: Properties,
                }
                let mut fork_edges: Vec<ForkEdgeRow> = Vec::with_capacity(result.rows().len());
                for row in result.rows() {
                    let (Some(Value::Edge(edge)), Some(Value::Node(a)), Some(Value::Node(b))) =
                        (row.value("r"), row.value("a"), row.value("b"))
                    else {
                        continue;
                    };
                    let (Some(a_label), Some(b_label)) = (a.labels.first(), b.labels.first())
                    else {
                        continue;
                    };
                    let (a_label, b_label) = (a_label.clone(), b_label.clone());
                    let src_uid = VertexDataset::compute_vertex_uid(
                        &a_label,
                        ext_id_for(&fork_ext_ids, a.vid),
                        &a.properties,
                    );
                    let dst_uid = VertexDataset::compute_vertex_uid(
                        &b_label,
                        ext_id_for(&fork_ext_ids, b.vid),
                        &b.properties,
                    );
                    let edge_uid = MainEdgeDataset::compute_edge_uid(
                        &src_uid,
                        &dst_uid,
                        edge_type,
                        &edge.properties,
                    );
                    fork_edges.push(ForkEdgeRow {
                        a_label,
                        b_label,
                        src_uid,
                        dst_uid,
                        edge_uid,
                        edge_props: edge.properties.clone(),
                    });
                }

                // Group endpoints by label so we can batch-resolve
                // each label's UIDs in a single round-trip.
                let mut to_resolve: HashMap<String, HashSet<UniId>> = HashMap::new();
                for fe in &fork_edges {
                    if !just_inserted.contains_key(&(fe.a_label.clone(), fe.src_uid)) {
                        to_resolve
                            .entry(fe.a_label.clone())
                            .or_default()
                            .insert(fe.src_uid);
                    }
                    if !just_inserted.contains_key(&(fe.b_label.clone(), fe.dst_uid)) {
                        to_resolve
                            .entry(fe.b_label.clone())
                            .or_default()
                            .insert(fe.dst_uid);
                    }
                }
                let mut endpoint_resolved: HashMap<(String, UniId), Vid> = HashMap::new();
                for (lbl, uid_set) in to_resolve {
                    let uid_vec: Vec<UniId> = uid_set.into_iter().collect();
                    let (resolved, _degraded) =
                        batch_resolve_primary_vids(primary, &primary_storage, &lbl, &uid_vec).await;
                    for (uid, vid) in resolved {
                        endpoint_resolved.insert((lbl.clone(), uid), vid);
                    }
                }
                // Seed with just_inserted cache hits.
                for ((lbl, uid), vid) in just_inserted.iter() {
                    endpoint_resolved.insert((lbl.clone(), *uid), *vid);
                }

                // Pre-fetch primary's parallel edges for dedup: one
                // query covering every (src_vid, dst_vid) pair across
                // all resolved fork edges. Hash by computed edge UID.
                let mut resolved_pairs: HashSet<(Vid, Vid)> = HashSet::new();
                for fe in &fork_edges {
                    let s = endpoint_resolved.get(&(fe.a_label.clone(), fe.src_uid));
                    let d = endpoint_resolved.get(&(fe.b_label.clone(), fe.dst_uid));
                    if let (Some(s), Some(d)) = (s, d) {
                        resolved_pairs.insert((*s, *d));
                    }
                }
                let mut primary_edge_uids: HashSet<UniId> = HashSet::new();
                if !resolved_pairs.is_empty() {
                    let src_vids: HashSet<u64> =
                        resolved_pairs.iter().map(|(s, _)| s.as_u64()).collect();
                    let dst_vids: HashSet<u64> =
                        resolved_pairs.iter().map(|(_, d)| d.as_u64()).collect();
                    let dedup_cypher = format!(
                        "MATCH (a)-[r:`{}`]->(b) \
                         WHERE id(a) IN [{}] AND id(b) IN [{}] \
                         RETURN a, r, b",
                        escape_backticks(edge_type),
                        vid_in_list(src_vids),
                        vid_in_list(dst_vids),
                    );
                    if let Ok(rs) = primary.query(&dedup_cypher).await {
                        for row in rs.rows() {
                            let (
                                Some(Value::Edge(existing)),
                                Some(Value::Node(ea)),
                                Some(Value::Node(eb)),
                            ) = (row.value("r"), row.value("a"), row.value("b"))
                            else {
                                continue;
                            };
                            let ea_label = ea.labels.first().cloned().unwrap_or_default();
                            let eb_label = eb.labels.first().cloned().unwrap_or_default();
                            let esrc = VertexDataset::compute_vertex_uid(
                                &ea_label,
                                ext_id_for(&primary_ext_ids, ea.vid),
                                &ea.properties,
                            );
                            let edst = VertexDataset::compute_vertex_uid(
                                &eb_label,
                                ext_id_for(&primary_ext_ids, eb.vid),
                                &eb.properties,
                            );
                            let euid = MainEdgeDataset::compute_edge_uid(
                                &esrc,
                                &edst,
                                edge_type,
                                &existing.properties,
                            );
                            primary_edge_uids.insert(euid);
                        }
                    }
                }

                // Second pass: classify each fork edge against the
                // resolved endpoints and primary edge-UID set. Edges
                // are accumulated and bulk-inserted in one call.
                let mut edges_to_insert: Vec<(Vid, Vid, Properties)> =
                    Vec::with_capacity(fork_edges.len());
                let mut pattern_inserted = 0usize;
                for fe in fork_edges {
                    let src_vid = endpoint_resolved
                        .get(&(fe.a_label.clone(), fe.src_uid))
                        .copied();
                    let dst_vid = endpoint_resolved
                        .get(&(fe.b_label.clone(), fe.dst_uid))
                        .copied();
                    let (src_vid, dst_vid) = match (src_vid, dst_vid) {
                        (Some(s), Some(d)) => (s, d),
                        _ => {
                            report.edges_skipped_no_endpoint += 1;
                            continue;
                        }
                    };
                    if primary_edge_uids.contains(&fe.edge_uid) {
                        report.edges_skipped_duplicate += 1;
                        continue;
                    }
                    edges_to_insert.push((src_vid, dst_vid, fe.edge_props));
                    pattern_inserted += 1;
                }
                if !edges_to_insert.is_empty() {
                    let n = edges_to_insert.len();
                    primary_tx
                        .bulk_insert_edges(edge_type, edges_to_insert)
                        .await?;
                    report.edges_inserted += n;
                }
                report.per_pattern_inserted[idx] = pattern_inserted;
            }
        }
    }

    // Delete-promotion (M4): a vertex present at the fork point but removed
    // on the fork is deleted on primary. Opt-in and ext_id-keyed. We scan
    // the FULL fork label (ignoring per-pattern where-clauses, which select
    // which present rows to *promote*, not which to keep), so a filtered-out
    // but still-present fork row is never read as a deletion. A row primary
    // added after the fork point is absent from the baseline and so is never
    // a delete candidate — the anti-spurious-delete guarantee. Runs after
    // the pattern loop so vertex deletes are issued last in tx order.
    if options.delete_promotion
        && let Some(baseline) = baseline
    {
        let mut del_labels: Vec<&str> = patterns
            .iter()
            .filter(|p| !p.is_edge())
            .map(|p| p.label_name())
            .collect();
        del_labels.sort_unstable();
        del_labels.dedup();

        for label in del_labels {
            let cypher = format!("MATCH (n:`{}`) RETURN n", escape_backticks(label));
            let result = fork.query(&cypher).await?;
            let mut fork_now_ext: HashSet<String> = HashSet::new();
            let mut fork_now_noext: HashSet<UniId> = HashSet::new();
            for row in result.rows() {
                if let Some(Value::Node(node)) = row.value("n") {
                    match ext_id_for(&fork_ext_ids, node.vid) {
                        Some(eid) if !eid.is_empty() => {
                            fork_now_ext.insert(eid.to_string());
                        }
                        _ => {
                            fork_now_noext.insert(VertexDataset::compute_vertex_uid(
                                label,
                                None,
                                &node.properties,
                            ));
                        }
                    }
                }
            }

            // ext_id rows present at the fork point, absent on the fork now.
            if let Some(base_ext) = baseline.ext.get(label) {
                let deleted_ext: HashSet<String> = base_ext
                    .keys()
                    .filter(|eid| !fork_now_ext.contains(*eid))
                    .cloned()
                    .collect();
                if !deleted_ext.is_empty() {
                    // Resolve against primary NOW; delete only those still
                    // present (idempotent if primary already removed them).
                    let resolved = batch_resolve_primary_by_ext_id(
                        primary,
                        &primary_ext_ids,
                        label,
                        &deleted_ext,
                    )
                    .await;
                    for (_eid, (pvid, _props)) in resolved {
                        primary_tx.delete_vertex(label, pvid).await?;
                        report.vertices_deleted += 1;
                    }
                }
            }

            // Non-ext_id fork-point rows that vanished can't be safely
            // delete-promoted (no stable identity); surface the count.
            if let Some(base_noext) = baseline.no_ext.get(label) {
                let gone = base_noext
                    .iter()
                    .filter(|u| !fork_now_noext.contains(*u))
                    .count();
                report.vertices_skipped_no_ext_id_for_delete += gone;
            }
        }
    }

    // When the call contains no edge patterns, surface incidental edges
    // on the fork so callers see they exist (and weren't promoted).
    if !any_edge_pattern {
        let mut edge_seen = 0usize;
        for et in fork.schema().schema().edge_types.keys() {
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
