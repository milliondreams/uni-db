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
    DiffEdge, DiffVertex, EdgeDiff, ForkDiff, PromotePattern, PromoteReport, PropertyChange,
    VertexDiff, VertexPropertyChange,
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

    let labels_a: HashSet<String> = a.schema().schema().labels.keys().cloned().collect();
    let labels_b: HashSet<String> = b.schema().schema().labels.keys().cloned().collect();
    let labels_union: Vec<&String> = labels_a.union(&labels_b).collect();

    for label in labels_union {
        let rows_a = scan_label_nodes(a, label).await?;
        let rows_b = scan_label_nodes(b, label).await?;
        diff_label(label, rows_a, rows_b, &mut diff.vertices);
    }

    let edges_a: HashSet<String> = a.schema().schema().edge_types.keys().cloned().collect();
    let edges_b: HashSet<String> = b.schema().schema().edge_types.keys().cloned().collect();
    let edges_union: Vec<&String> = edges_a.union(&edges_b).collect();

    for edge_type in edges_union {
        let rows_a = scan_edge_type(a, edge_type).await?;
        let rows_b = scan_edge_type(b, edge_type).await?;
        diff_edge_type(edge_type, rows_a, rows_b, &mut diff.edges);
    }

    Ok(diff)
}

/// Extract a vertex's `ext_id` for content-UID computation.
///
/// NOTE (review H4): `ext_id` is a distinct field in the storage `_uid` but is
/// stripped from query results and not projectable through the query layer the
/// diff uses, so this is currently always `None` in practice — two vertices
/// differing only by `ext_id` (settable only via the OGM / bulk layer, not
/// Cypher `CREATE`) still collapse to one diff identity. The `bucket.insert`
/// collision warnings make that observable. A full fix requires exposing
/// `ext_id` through projection so it can be folded in consistently across L0
/// and flushed rows (keying off the storage `_uid` is NOT viable: it diverges
/// from the query-recomputed UID, breaking L0/flushed consistency).
fn ext_id_of(props: &Properties) -> Option<&str> {
    props.get("ext_id").and_then(|v| v.as_str())
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

async fn scan_label_nodes<Q: ForkQueryHost + ?Sized>(s: &Q, label: &str) -> Result<VertexBucket> {
    use uni_store::storage::vertex::VertexDataset;
    let cypher = format!("MATCH (n:`{}`) RETURN n", escape_backticks(label));
    let result = s.query(&cypher).await?;
    let mut bucket = VertexBucket::new();
    for row in result.rows() {
        let Some(Value::Node(node)) = row.value("n") else {
            continue;
        };
        // The MATCH already filters to nodes carrying `label`, so the
        // bucketed row's label is always `label`. (`ext_id` is not available
        // here — see `ext_id_of`; the collision warning below surfaces any
        // resulting content-UID collapse.)
        let uid =
            VertexDataset::compute_vertex_uid(label, ext_id_of(&node.properties), &node.properties);
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

async fn scan_edge_type<Q: ForkQueryHost + ?Sized>(s: &Q, edge_type: &str) -> Result<EdgeBucket> {
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
            VertexDataset::compute_vertex_uid(&a_label, ext_id_of(&a.properties), &a.properties);
        let dst_uid =
            VertexDataset::compute_vertex_uid(&b_label, ext_id_of(&b.properties), &b.properties);
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
        match keys_b.contains(uid) {
            true => {
                let row_a = a.remove(uid).expect("key from keys_a");
                let row_b = b.remove(uid).expect("shared key in b");
                common.push((*uid, row_a, row_b));
            }
            false => mk_deleted(*uid, a.remove(uid).expect("key from keys_a")),
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
) -> HashMap<UniId, Vid> {
    // NOTE: every error path below degrades to whatever has been
    // resolved so far (an empty or partial map) rather than
    // propagating. This is deliberate: `run_promote` treats an
    // unresolved UID as "not present on primary" and inserts it, so a
    // transient resolve failure must not abort the promote. Changing
    // this to propagate would alter promote semantics.
    let mut out: HashMap<UniId, Vid> = HashMap::new();
    if uids.is_empty() {
        return out;
    }
    // Collect *all* candidate VIDs per UID by scanning the shared
    // UidIndex with an IN filter. The shared index is not
    // branch-isolated, so a single UID may have a fork-only VID and
    // a primary VID both registered — we keep both and let the
    // primary Cypher MATCH below decide which is real.
    let candidates_per_uid: HashMap<UniId, Vec<Vid>> = match primary_storage.uid_index(label).ok() {
        Some(uix) => match resolve_all_candidate_vids(&uix, uids).await {
            Ok(m) => m,
            Err(_) => return out,
        },
        None => return out,
    };
    if candidates_per_uid.is_empty() {
        return out;
    }
    // Single Cypher with IN clause over every candidate VID across
    // every UID. Primary's branched backend filters out fork-only
    // VIDs naturally — they have no row in the primary view.
    let vid_set: HashSet<u64> = candidates_per_uid
        .values()
        .flat_map(|vs| vs.iter().map(|v| v.as_u64()))
        .collect();
    let vid_list: Vec<String> = vid_set.iter().map(|v| v.to_string()).collect();
    let cypher = format!(
        "MATCH (n:`{}`) WHERE id(n) IN [{}] RETURN id(n) AS vid",
        escape_backticks(label),
        vid_list.join(", ")
    );
    let rs = match primary.query(&cypher).await {
        Ok(rs) => rs,
        Err(_) => return out,
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
    uid.as_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
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

                // First pass: extract (uid, props) for every fork row,
                // skipping rows already in the within-call cache.
                let mut candidates: Vec<(UniId, Properties)> =
                    Vec::with_capacity(result.rows().len());
                for row in result.rows() {
                    let Some(Value::Node(node)) = row.value("n") else {
                        continue;
                    };
                    let uid = VertexDataset::compute_vertex_uid(
                        label,
                        ext_id_of(&node.properties),
                        &node.properties,
                    );
                    if just_inserted.contains_key(&(label.clone(), uid)) {
                        report.vertices_skipped_uid_conflict += 1;
                        continue;
                    }
                    candidates.push((uid, node.properties.clone()));
                }

                // Batch-resolve every candidate UID against primary.
                // Two queries total per pattern (UidIndex.resolve_uids
                // + Cypher IN-clause verify) instead of 2N.
                let uids_to_check: Vec<UniId> = candidates.iter().map(|(u, _)| *u).collect();
                let on_primary =
                    batch_resolve_primary_vids(primary, &primary_storage, label, &uids_to_check)
                        .await;

                let mut to_insert: Vec<Properties> = Vec::with_capacity(candidates.len());
                let mut insert_uids: Vec<UniId> = Vec::with_capacity(candidates.len());
                for (uid, props) in candidates {
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
                    let a_label = match a.labels.first() {
                        Some(l) => l.clone(),
                        None => continue,
                    };
                    let b_label = match b.labels.first() {
                        Some(l) => l.clone(),
                        None => continue,
                    };
                    let src_uid = VertexDataset::compute_vertex_uid(
                        &a_label,
                        ext_id_of(&a.properties),
                        &a.properties,
                    );
                    let dst_uid = VertexDataset::compute_vertex_uid(
                        &b_label,
                        ext_id_of(&b.properties),
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
                    let resolved =
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
                    let src_list: Vec<String> = src_vids.iter().map(|v| v.to_string()).collect();
                    let dst_list: Vec<String> = dst_vids.iter().map(|v| v.to_string()).collect();
                    let dedup_cypher = format!(
                        "MATCH (a)-[r:`{}`]->(b) \
                         WHERE id(a) IN [{}] AND id(b) IN [{}] \
                         RETURN a, r, b",
                        escape_backticks(edge_type),
                        src_list.join(", "),
                        dst_list.join(", "),
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
                                ext_id_of(&ea.properties),
                                &ea.properties,
                            );
                            let edst = VertexDataset::compute_vertex_uid(
                                &eb_label,
                                ext_id_of(&eb.properties),
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
