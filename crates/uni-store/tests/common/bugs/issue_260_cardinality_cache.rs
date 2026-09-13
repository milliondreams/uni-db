// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #260 — a cached, invalidated row count readable without `await`.
//!
//! The counting was never the hard part. These cover the four that are:
//! reading it synchronously, invalidating it, including L0, and declining for
//! readers whose view is not the live primary tip.

// Rust guideline compliant
#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::core::id::Vid;
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::cardinality::CardinalityKey;
use uni_store::storage::manager::StorageManager;

async fn fixture() -> Result<(TempDir, Arc<StorageManager>, Arc<SchemaManager>, Writer)> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let sm = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    sm.add_label("N")?;
    sm.save().await?;
    let storage = Arc::new(StorageManager::new(path, sm.clone()).await?);
    let writer = Writer::new(storage.clone(), sm.clone(), 1).await?;
    Ok((dir, storage, sm, writer))
}

async fn insert(writer: &Writer, n: usize) -> Result<Vec<Vid>> {
    let mut vids = Vec::new();
    for _ in 0..n {
        let vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(vid, HashMap::new(), &["N".to_string()], None)
            .await?;
        vids.push(vid);
    }
    Ok(vids)
}

fn key() -> CardinalityKey {
    CardinalityKey::Vertex("N".to_string())
}

/// A miss is `None`, and a refresh makes it readable without `await`.
#[tokio::test]
async fn a_refresh_makes_the_count_readable_synchronously() -> Result<()> {
    let (_d, storage, _sm, writer) = fixture().await?;
    assert_eq!(
        storage.cached_row_count(&key(), None),
        None,
        "an unrefreshed table must miss, never read as zero"
    );

    insert(&writer, 5).await?;
    writer.flush_to_l1(None).await?;
    storage.refresh_row_count(&key()).await?;

    assert_eq!(
        storage.cached_row_count(&key(), None),
        Some(5),
        "the refreshed count must be readable with no await"
    );
    Ok(())
}

/// **The L0 case.** A label whose rows are all unflushed must not read as zero.
///
/// This is the one where a wrong answer is silent rather than slow.
/// `crates/uni/src/api/schema.rs` records exactly this defect for the `count`
/// surface — "a silent wrong answer, and the reason a Python assertion on this
/// value was once weakened rather than fixed" — so the test asserts the
/// mechanism that prevents it: the flushed half is cached, the L0 half is read
/// live on every call.
#[tokio::test]
async fn an_unflushed_label_does_not_read_as_zero() -> Result<()> {
    let (_d, storage, _sm, writer) = fixture().await?;

    // Count an empty flushed table, so the cache holds a legitimate zero.
    storage.refresh_row_count(&key()).await?;
    assert_eq!(storage.cached_row_count(&key(), None), Some(0));

    // Five rows that never reach storage.
    insert(&writer, 5).await?;
    let l0 = writer.l0_manager.get_current();
    let guard = l0.read();

    assert_eq!(
        storage.cached_row_count(&key(), None),
        Some(0),
        "asked for the flushed count alone, zero is the correct answer"
    );
    assert_eq!(
        storage.cached_row_count(&key(), Some(&guard)),
        Some(5),
        "a label with five L0-resident rows and no flush read as {:?}. Reading \
         zero here is the silent wrong answer this issue names",
        storage.cached_row_count(&key(), Some(&guard))
    );
    Ok(())
}

/// A commit needs no invalidation, because the L0 half is never cached.
#[tokio::test]
async fn a_commit_needs_no_invalidation() -> Result<()> {
    let (_d, storage, _sm, writer) = fixture().await?;
    insert(&writer, 2).await?;
    writer.flush_to_l1(None).await?;
    storage.refresh_row_count(&key()).await?;

    // More rows, committed but not flushed. Nothing invalidates, and nothing
    // needs to: the cached half did not change and the live half is re-read.
    insert(&writer, 3).await?;
    let l0 = writer.l0_manager.get_current();
    let guard = l0.read();
    assert_eq!(
        storage.cached_row_count(&key(), Some(&guard)),
        Some(5),
        "2 flushed + 3 committed-but-unflushed must total 5"
    );
    Ok(())
}

/// A flush invalidates, so the next read misses rather than serving a stale
/// count — through the sync path and the async one, which share a finalize.
#[tokio::test]
async fn a_flush_invalidates_the_cached_count() -> Result<()> {
    let (_d, storage, _sm, writer) = fixture().await?;
    insert(&writer, 2).await?;
    writer.flush_to_l1(None).await?;
    storage.refresh_row_count(&key()).await?;
    assert_eq!(storage.cached_row_count(&key(), None), Some(2));

    insert(&writer, 3).await?;
    writer.flush_to_l1(None).await?;

    assert_eq!(
        storage.cached_row_count(&key(), None),
        None,
        "the flush moved rows into storage, so the cached 2 must be forgotten \
         rather than served"
    );
    storage.refresh_row_count(&key()).await?;
    assert_eq!(
        storage.cached_row_count(&key(), None),
        Some(5),
        "and a refresh after the flush must see all five"
    );
    Ok(())
}

/// Compaction invalidates every table's count.
#[tokio::test]
async fn compaction_invalidates_every_count() -> Result<()> {
    let (_d, storage, _sm, writer) = fixture().await?;
    insert(&writer, 4).await?;
    writer.flush_to_l1(None).await?;
    storage.refresh_row_count(&key()).await?;
    assert_eq!(storage.cached_row_count(&key(), None), Some(4));

    let compactor = uni_store::storage::compaction::Compactor::new(storage.clone());
    compactor.compact_all().await?;

    assert_eq!(
        storage.cached_row_count(&key(), None),
        None,
        "compaction rewrites tables and drops superseded rows, so every cached \
         count must be forgotten"
    );
    Ok(())
}

/// A pinned reader declines rather than answering from the live tip.
#[tokio::test]
async fn a_pinned_reader_declines() -> Result<()> {
    let (_d, storage, _sm, writer) = fixture().await?;
    insert(&writer, 3).await?;
    writer.flush_to_l1(None).await?;
    storage.refresh_row_count(&key()).await?;
    assert_eq!(storage.cached_row_count(&key(), None), Some(3));

    // The cache tracks the live tip; a version-pinned view reads an older one,
    // so the cached number is not its answer.
    let pinned = storage.pinned_at_version(1);
    assert_eq!(
        pinned.cached_row_count(&key(), None),
        None,
        "a pinned view must decline, not serve the live tip's count — the two \
         views disagree by construction"
    );
    assert_eq!(
        pinned.refresh_row_count(&key()).await?,
        None,
        "and a refresh from a pinned view must not write the tip's count into \
         the shared cache"
    );
    Ok(())
}

/// A broad endpoint request takes one pass over the edge type instead of
/// chunked `IN (...)` lookups, and returns the same edges either way.
///
/// This is #260's statistic being consumed: the arm is chosen by comparing the
/// request against the type's cached row count (#237), where before it was
/// chosen by which `match` arm the caller happened to land in. Measured
/// crossover and the constant behind it are in
/// `StorageManager::endpoint_scan_beats_lookup`.
///
/// The contrast is against the dataset-level call, which always chunks — so the
/// test fails if the decision stops firing, and the equal-results assertion is
/// what licenses reading the scan counts as strategy rather than as two
/// different reads.
#[tokio::test]
async fn a_broad_endpoint_request_switches_to_one_pass() -> Result<()> {
    use object_store::ObjectStore;
    use uni_common::Properties;
    use uni_common::config::UniConfig;
    use uni_store::backend::lance::LanceDbBackend;
    use uni_store::storage::main_edge::{EndpointSide, MainEdgeDataset};

    use super::fault_backend::FaultBackend;

    const EDGES: usize = 100;
    /// Past `VID_CHUNK` (8 192), so the chunked arm needs more than one scan and
    /// the two strategies are distinguishable by count.
    const REQUESTED: u64 = 20_000;

    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let lance = LanceDbBackend::connect(&uri, None).await?;
    let fault = Arc::new(FaultBackend::new(Arc::new(lance)));
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let sm = Arc::new(
        SchemaManager::load_from_store(store.clone(), &ObjectStorePath::from("schema.json"))
            .await?,
    );
    sm.add_label("N")?;
    let etype = sm.add_edge_type("E", vec!["N".to_string()], vec!["N".to_string()])?;
    sm.save().await?;
    let storage = Arc::new(
        StorageManager::new_with_backend(
            &uri,
            store,
            fault.clone(),
            sm.clone(),
            UniConfig::default(),
        )
        .await?,
    );
    let writer = Writer::new(storage.clone(), sm.clone(), 1).await?;

    let mut srcs = Vec::new();
    for _ in 0..EDGES {
        let src = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(src, HashMap::new(), &["N".to_string()], None)
            .await?;
        let dst = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(dst, HashMap::new(), &["N".to_string()], None)
            .await?;
        let eid = writer.next_eid(etype).await?;
        writer
            .insert_edge(src, dst, etype, eid, Properties::new(), None, None)
            .await?;
        srcs.push(src);
    }
    writer.flush_to_l1(None).await?;

    // A request far broader than the type is large: every real src, plus enough
    // absent vids to clear the minimum and to exceed one chunk.
    let mut wanted: Vec<Vid> = srcs.clone();
    let mut next = 1_000_000u64;
    while (wanted.len() as u64) < REQUESTED {
        wanted.push(Vid::from(next));
        next += 1;
    }

    fault.reset_scans();
    let via_manager = storage
        .find_edges_by_type_names(&["E"], Some((EndpointSide::Src, &wanted)))
        .await?;
    let manager_scans = fault.scans();

    fault.reset_scans();
    let via_dataset = MainEdgeDataset::find_edges_by_type_names(
        storage.backend(),
        &["E"],
        Some((EndpointSide::Src, &wanted)),
    )
    .await?;
    let dataset_scans = fault.scans();

    let mut a: Vec<_> = via_manager.iter().map(|(eid, ..)| eid.as_u64()).collect();
    let mut b: Vec<_> = via_dataset.iter().map(|(eid, ..)| eid.as_u64()).collect();
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(
        a, b,
        "the two strategies returned different edges; the scan counts below \
         would then be comparing different reads, not read strategies"
    );
    assert_eq!(
        a.len(),
        EDGES,
        "expected every one of the {EDGES} edges back, got {}",
        a.len()
    );

    assert!(
        dataset_scans > 0,
        "the chunked arm reported zero scans, so the decorator is not observing \
         this path and the comparison below cannot mean anything"
    );
    assert!(
        manager_scans < dataset_scans,
        "a request covering {REQUESTED} endpoint vids against a {EDGES}-edge \
         type issued {manager_scans} scans through the manager and \
         {dataset_scans} through the always-chunking dataset call. The manager \
         is supposed to consult the cached row count and take one pass; equal \
         means the decision is not firing (#237/#260)"
    );
    Ok(())
}
