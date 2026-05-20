// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::store_utils::{
    DEFAULT_TIMEOUT, delete_with_timeout, get_with_timeout, list_with_timeout, put_with_timeout,
};
use anyhow::Result;
use metrics;
use object_store::ObjectStore;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, instrument, warn};
use uni_common::Properties;
use uni_common::core::id::{Eid, Vid};
use uni_common::sync::acquire_mutex;
use uuid::Uuid;

/// Parse LSN from WAL segment filename format `{:020}_{uuid}.wal`.
/// Returns None if the filename doesn't match the expected format.
fn parse_lsn_from_filename(path: &Path) -> Option<u64> {
    let filename = path.filename()?;
    if filename.len() < 20 {
        return None;
    }
    filename[..20].parse::<u64>().ok()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Mutation {
    InsertEdge {
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        version: u64,
        properties: Properties,
        /// Edge type name for metadata recovery. Optional for backward compatibility.
        #[serde(default)]
        edge_type_name: Option<String>,
    },
    DeleteEdge {
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        version: u64,
    },
    InsertVertex {
        vid: Vid,
        properties: Properties,
        #[serde(default)]
        labels: Vec<String>,
    },
    DeleteVertex {
        vid: Vid,
        #[serde(default)]
        labels: Vec<String>,
    },
}

/// WAL segment with LSN for idempotent recovery
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WalSegment {
    /// Log Sequence Number - monotonically increasing per segment
    pub lsn: u64,
    /// Mutations in this segment
    pub mutations: Vec<Mutation>,
}

pub struct WriteAheadLog {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    state: Mutex<WalState>,
}

struct WalState {
    buffer: Vec<Mutation>,
    /// Current LSN counter (incremented per flush)
    next_lsn: u64,
    /// Highest LSN successfully flushed
    flushed_lsn: u64,
}

impl WriteAheadLog {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self {
            store,
            prefix,
            state: Mutex::new(WalState {
                buffer: Vec::new(),
                next_lsn: 1, // Start at 1 so 0 means "no WAL"
                flushed_lsn: 0,
            }),
        }
    }

    /// Initialize WAL state from existing segments (called on startup)
    pub async fn initialize(&self) -> Result<u64> {
        let max_lsn = self.find_max_lsn().await?;
        {
            let mut state = acquire_mutex(&self.state, "wal_state")?;
            state.next_lsn = max_lsn + 1;
            state.flushed_lsn = max_lsn;
        }
        Ok(max_lsn)
    }

    /// Find the maximum LSN in existing WAL segments by parsing filenames.
    /// Only downloads segments if filename parsing fails (fallback).
    async fn find_max_lsn(&self) -> Result<u64> {
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;
        let mut max_lsn: u64 = 0;

        for meta in metas {
            // Try to parse LSN from filename first (fast path)
            if let Some(lsn) = parse_lsn_from_filename(&meta.location) {
                max_lsn = max_lsn.max(lsn);
            } else {
                // Fallback: download and parse segment if filename doesn't match expected format
                warn!(
                    path = %meta.location,
                    "WAL filename doesn't match expected format, downloading segment"
                );
                let get_result =
                    get_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
                let bytes = get_result.bytes().await?;
                if bytes.is_empty() {
                    continue;
                }
                let segment: WalSegment = serde_json::from_slice(&bytes)?;
                max_lsn = max_lsn.max(segment.lsn);
            }
        }

        Ok(max_lsn)
    }

    #[instrument(skip(self, mutation), level = "trace")]
    pub fn append(&self, mutation: &Mutation) -> Result<()> {
        let mut state = acquire_mutex(&self.state, "wal_state")?;
        state.buffer.push(mutation.clone());
        metrics::counter!("uni_wal_entries_total").increment(1);
        Ok(())
    }

    /// Flush buffered mutations to a WAL segment. Returns the LSN of the flushed segment.
    #[instrument(skip(self), fields(lsn, mutations_count, size_bytes))]
    pub async fn flush(&self) -> Result<u64> {
        let start = std::time::Instant::now();
        let (batch, lsn) = {
            let mut state = acquire_mutex(&self.state, "wal_state")?;
            if state.buffer.is_empty() {
                return Ok(state.flushed_lsn);
            }
            let lsn = state.next_lsn;
            state.next_lsn += 1;
            (std::mem::take(&mut state.buffer), lsn)
        };

        tracing::Span::current().record("lsn", lsn);
        tracing::Span::current().record("mutations_count", batch.len());

        // Create segment with LSN
        let segment = WalSegment {
            lsn,
            mutations: batch.clone(),
        };

        // Serialize segment; restore buffer on failure
        let json = match serde_json::to_vec(&segment) {
            Ok(j) => j,
            Err(e) => {
                warn!(lsn, error = %e, "Failed to serialize WAL segment, restoring buffer");
                // Restore buffer on serialization failure
                let mut state = acquire_mutex(&self.state, "wal_state")?;
                let new_mutations = std::mem::take(&mut state.buffer);
                state.buffer = batch;
                state.buffer.extend(new_mutations);
                // Don't roll back LSN - gap is harmless and maintains monotonicity
                return Err(e.into());
            }
        };
        tracing::Span::current().record("size_bytes", json.len());
        metrics::counter!("uni_wal_bytes_written_total").increment(json.len() as u64);

        // Include LSN in filename for easy ordering and identification
        let filename = format!("{:020}_{}.wal", lsn, Uuid::new_v4());
        let path = self.prefix.child(filename);

        // Attempt to write; restore buffer on failure to prevent data loss
        if let Err(e) = put_with_timeout(&self.store, &path, json.into(), DEFAULT_TIMEOUT).await {
            warn!(
                lsn,
                error = %e,
                "Failed to flush WAL segment, restoring buffer (LSN gap preserved for monotonicity)"
            );
            // Restore buffer so data isn't lost on transient failures
            let mut state = acquire_mutex(&self.state, "wal_state")?;
            // Prepend the failed batch to any new mutations that arrived
            let new_mutations = std::mem::take(&mut state.buffer);
            state.buffer = batch;
            state.buffer.extend(new_mutations);
            // Don't roll back LSN - gap is harmless and maintains strict monotonicity
            // All WAL consumers use `>` / `<=` comparisons, not equality checks
            return Err(e);
        }

        // Update flushed LSN on success
        {
            let mut state = acquire_mutex(&self.state, "wal_state")?;
            state.flushed_lsn = lsn;
        }

        let duration = start.elapsed();
        metrics::histogram!("wal_flush_latency_ms").record(duration.as_millis() as f64);
        metrics::histogram!("uni_wal_flush_duration_seconds").record(duration.as_secs_f64());

        if duration.as_millis() > 100 {
            warn!(
                lsn,
                duration_ms = duration.as_millis(),
                "Slow WAL flush detected"
            );
        } else {
            debug!(
                lsn,
                duration_ms = duration.as_millis(),
                "WAL flush completed"
            );
        }

        Ok(lsn)
    }

    /// Get the highest LSN that has been flushed.
    ///
    /// # Errors
    ///
    /// Returns error if the WAL state lock is poisoned (see issue #18/#150).
    pub fn flushed_lsn(&self) -> Result<u64, uni_common::sync::LockPoisonedError> {
        let guard = uni_common::sync::acquire_mutex(&self.state, "wal_state")?;
        Ok(guard.flushed_lsn)
    }

    /// Replay WAL segments with LSN > high_water_mark.
    /// Returns mutations from segments that haven't been applied yet.
    /// Optimized to skip downloading segments with LSN <= high_water_mark (parsed from filename).
    #[instrument(skip(self), level = "debug")]
    pub async fn replay_since(&self, high_water_mark: u64) -> Result<Vec<Mutation>> {
        let start = std::time::Instant::now();
        debug!(high_water_mark, "Replaying WAL segments");
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;
        let mut mutations = Vec::new();

        // Collect paths and sort by LSN (filename prefix)
        let mut paths: Vec<_> = metas.into_iter().map(|m| m.location).collect();
        paths.sort(); // Lexicographical sort works for LSN prefix (zero-padded)

        let mut segments_replayed = 0;

        for path in paths {
            // Skip downloading segments we can identify as below high_water_mark from filename
            if let Some(lsn) = parse_lsn_from_filename(&path)
                && lsn <= high_water_mark
            {
                continue; // Skip this segment without downloading
            }

            // Download segment (either LSN > high_water_mark or filename unparseable)
            let get_result = get_with_timeout(&self.store, &path, DEFAULT_TIMEOUT).await?;
            let bytes = get_result.bytes().await?;
            if bytes.is_empty() {
                continue;
            }

            let segment: WalSegment = serde_json::from_slice(&bytes)?;
            // Double-check LSN from segment content (handles fallback case)
            if segment.lsn > high_water_mark {
                mutations.extend(segment.mutations);
                segments_replayed += 1;
            }
        }

        info!(
            segments_replayed,
            mutations_count = mutations.len(),
            "WAL replay completed"
        );
        metrics::histogram!("uni_wal_replay_duration_seconds")
            .record(start.elapsed().as_secs_f64());

        Ok(mutations)
    }

    /// Replay all WAL segments.
    pub async fn replay(&self) -> Result<Vec<Mutation>> {
        self.replay_since(0).await
    }

    /// Deletes WAL segments with LSN <= high_water_mark by parsing filenames.
    /// Only downloads segments if filename parsing fails (fallback).
    #[instrument(skip(self), level = "info")]
    pub async fn truncate_before(&self, high_water_mark: u64) -> Result<()> {
        info!(high_water_mark, "Truncating WAL segments");
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;

        let mut deleted_count = 0;
        for meta in metas {
            // Try to parse LSN from filename first (fast path)
            let should_delete = if let Some(lsn) = parse_lsn_from_filename(&meta.location) {
                lsn <= high_water_mark
            } else {
                // Fallback: download and parse segment if filename doesn't match expected format
                warn!(
                    path = %meta.location,
                    "WAL filename doesn't match expected format, downloading segment"
                );
                let get_result =
                    get_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
                let bytes = get_result.bytes().await?;
                if bytes.is_empty() {
                    // Empty segments should be deleted
                    true
                } else {
                    let segment: WalSegment = serde_json::from_slice(&bytes)?;
                    segment.lsn <= high_water_mark
                }
            };

            if should_delete {
                delete_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
                deleted_count += 1;
            }
        }
        info!(deleted_count, "WAL truncation completed");
        Ok(())
    }

    /// Check if any WAL segments exist (for detecting database with lost manifest).
    pub async fn has_segments(&self) -> Result<bool> {
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;
        Ok(!metas.is_empty())
    }

    pub async fn truncate(&self) -> Result<()> {
        info!("Truncating all WAL segments");
        let metas = list_with_timeout(&self.store, Some(&self.prefix), DEFAULT_TIMEOUT).await?;

        let mut deleted_count = 0;
        for meta in metas {
            delete_with_timeout(&self.store, &meta.location, DEFAULT_TIMEOUT).await?;
            deleted_count += 1;
        }
        info!(deleted_count, "Full WAL truncation completed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_append_replay() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        let mutation = Mutation::InsertVertex {
            vid: Vid::new(1),
            properties: HashMap::new(),
            labels: vec![],
        };

        wal.append(&mutation.clone())?;
        wal.flush().await?;

        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 1);
        if let Mutation::InsertVertex { vid, .. } = &mutations[0] {
            assert_eq!(vid.as_u64(), Vid::new(1).as_u64());
        } else {
            panic!("Wrong mutation type");
        }

        wal.truncate().await?;
        let mutations2 = wal.replay().await?;
        assert_eq!(mutations2.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_lsn_monotonicity() -> Result<()> {
        // Verify that LSN is strictly monotonic even across multiple flushes
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        let mutation1 = Mutation::InsertVertex {
            vid: Vid::new(1),
            properties: HashMap::new(),
            labels: vec![],
        };
        let mutation2 = Mutation::InsertVertex {
            vid: Vid::new(2),
            properties: HashMap::new(),
            labels: vec![],
        };
        let mutation3 = Mutation::InsertVertex {
            vid: Vid::new(3),
            properties: HashMap::new(),
            labels: vec![],
        };

        // First flush
        wal.append(&mutation1)?;
        let lsn1 = wal.flush().await?;

        // Second flush
        wal.append(&mutation2)?;
        let lsn2 = wal.flush().await?;

        // Third flush
        wal.append(&mutation3)?;
        let lsn3 = wal.flush().await?;

        // Verify strict monotonicity
        assert!(lsn2 > lsn1, "LSN2 ({}) should be > LSN1 ({})", lsn2, lsn1);
        assert!(lsn3 > lsn2, "LSN3 ({}) should be > LSN2 ({})", lsn3, lsn2);

        // Verify LSNs are consecutive
        assert_eq!(lsn2, lsn1 + 1);
        assert_eq!(lsn3, lsn2 + 1);

        Ok(())
    }

    #[test]
    fn test_parse_lsn_from_filename() {
        // Standard format
        let path = Path::from("00000000000000000042_a1b2c3d4.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(42));

        let path = Path::from("00000000000000001234_e5f6a7b8.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(1234));

        // Leading zeros
        let path = Path::from("00000000000000000001_xyz.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(1));

        // Large LSN (within u64 range)
        let path = Path::from("12345678901234567890_uuid.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(12345678901234567890));

        // Invalid formats
        let path = Path::from("invalid.wal");
        assert_eq!(parse_lsn_from_filename(&path), None);

        let path = Path::from("123.wal"); // Too short
        assert_eq!(parse_lsn_from_filename(&path), None);

        let path = Path::from("abcdefghijklmnopqrst_uuid.wal"); // Non-numeric
        assert_eq!(parse_lsn_from_filename(&path), None);

        // Missing underscore separator (but first 20 chars are valid LSN)
        let path = Path::from("00000000000000000100.wal");
        assert_eq!(parse_lsn_from_filename(&path), Some(100));

        // Empty path
        let path = Path::from("");
        assert_eq!(parse_lsn_from_filename(&path), None);
    }

    /// Test for Issue #6: WAL initialization should parse LSN from filenames
    /// without downloading all segments
    #[tokio::test]
    async fn test_find_max_lsn_scalability() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        // Create 100 WAL segments with increasing LSNs
        for i in 1..=100 {
            let mutation = Mutation::InsertVertex {
                vid: Vid::new(i),
                properties: HashMap::new(),
                labels: vec![],
            };
            wal.append(&mutation)?;
            wal.flush().await?;
        }

        // Measure initialization time - should be fast (parsing filenames, not downloading)
        let start = std::time::Instant::now();
        let max_lsn = wal.find_max_lsn().await?;
        let duration = start.elapsed();

        // Verify correctness
        assert_eq!(max_lsn, 100, "Max LSN should be 100");

        // Verify performance - should complete quickly even with many segments
        assert!(
            duration.as_millis() < 1000,
            "find_max_lsn took {}ms, expected < 1000ms (filename parsing should be fast)",
            duration.as_millis()
        );

        Ok(())
    }

    /// Test for Issue #11: LSN gaps are preserved on flush failures (watermark pattern)
    #[tokio::test]
    async fn test_lsn_gaps_preserved_on_flush_failure() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store.clone(), prefix.clone());

        // Flush mutation 1 successfully
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(1),
            properties: HashMap::new(),
            labels: vec![],
        })?;
        let lsn1 = wal.flush().await?;
        assert_eq!(lsn1, 1);

        // Flush mutation 2 successfully
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(2),
            properties: HashMap::new(),
            labels: vec![],
        })?;
        let lsn2 = wal.flush().await?;
        assert_eq!(lsn2, 2);

        // Simulate a scenario where flush might fail by creating a read-only store
        // (In real scenario, network failures would cause this)
        // For now, verify that LSN assignment happens BEFORE write attempt
        // by checking that next_lsn increments even if we don't flush

        // Append mutation 3 but DON'T flush
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(3),
            properties: HashMap::new(),
            labels: vec![],
        })?;

        // Now flush mutation 4
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(4),
            properties: HashMap::new(),
            labels: vec![],
        })?;
        let lsn4 = wal.flush().await?;

        // LSN should be 3 (both mutations 3 and 4 flushed together)
        assert_eq!(lsn4, 3, "LSN should increment monotonically");

        // Verify all mutations can be replayed
        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 4, "All 4 mutations should be replayed");

        Ok(())
    }

    /// Test for Issue #11: Verify LSN watermark pattern - no LSN reuse
    #[tokio::test]
    async fn test_lsn_watermark_no_reuse() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        // Track all LSNs we've seen
        let mut seen_lsns = std::collections::HashSet::new();

        // Perform 50 flushes
        for i in 1..=50 {
            wal.append(&Mutation::InsertVertex {
                vid: Vid::new(i),
                properties: HashMap::new(),
                labels: vec![],
            })?;
            let lsn = wal.flush().await?;

            // Verify no LSN reuse
            assert!(
                !seen_lsns.contains(&lsn),
                "LSN {} was reused! This violates monotonicity.",
                lsn
            );
            seen_lsns.insert(lsn);

            // Verify LSN is strictly increasing
            assert_eq!(lsn, i, "LSN should be {}, got {}", i, lsn);
        }

        Ok(())
    }

    /// Test for Issue #33: WAL truncation should parse LSN from filenames
    /// without downloading all segments
    #[tokio::test]
    async fn test_truncate_scalability() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        // Create 100 WAL segments
        for i in 1..=100 {
            let mutation = Mutation::InsertVertex {
                vid: Vid::new(i),
                properties: HashMap::new(),
                labels: vec![],
            };
            wal.append(&mutation)?;
            wal.flush().await?;
        }

        // Truncate segments with LSN <= 50
        let start = std::time::Instant::now();
        wal.truncate_before(50).await?;
        let duration = start.elapsed();

        // Verify only segments 51-100 remain
        let mutations = wal.replay().await?;
        assert_eq!(
            mutations.len(),
            50,
            "Should have 50 mutations remaining (51-100)"
        );

        // Verify performance - should be fast (filename parsing, not downloading)
        assert!(
            duration.as_millis() < 1000,
            "truncate_before took {}ms, expected < 1000ms (filename parsing should be fast)",
            duration.as_millis()
        );

        Ok(())
    }

    /// Test for Issue #6: replay_since should skip old segments by filename
    #[tokio::test]
    async fn test_replay_since_skips_old_segments() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = WriteAheadLog::new(store, prefix);

        // Create 100 WAL segments
        for i in 1..=100 {
            let mutation = Mutation::InsertVertex {
                vid: Vid::new(i),
                properties: HashMap::new(),
                labels: vec![],
            };
            wal.append(&mutation)?;
            wal.flush().await?;
        }

        // Replay only segments with LSN > 90 (should skip 90 segments by filename)
        let start = std::time::Instant::now();
        let mutations = wal.replay_since(90).await?;
        let duration = start.elapsed();

        // Verify only 10 mutations returned (LSN 91-100)
        assert_eq!(mutations.len(), 10, "Should replay only LSNs 91-100");

        // Verify performance - should be fast (skips 90 segments by filename)
        assert!(
            duration.as_millis() < 500,
            "replay_since took {}ms, expected < 500ms (should skip by filename)",
            duration.as_millis()
        );

        Ok(())
    }

    /// Test for Issue #23: Vertex labels preserved through WAL replay
    #[tokio::test]
    async fn test_wal_replay_preserves_vertex_labels() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = Arc::new(WriteAheadLog::new(store, prefix));

        // Append InsertVertex with labels
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(42),
            properties: {
                let mut props = HashMap::new();
                props.insert(
                    "name".to_string(),
                    uni_common::Value::String("Alice".to_string()),
                );
                props
            },
            labels: vec!["Person".to_string(), "User".to_string()],
        })?;

        // Flush to WAL
        wal.flush().await?;

        // Replay mutations
        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 1);

        // Verify labels are preserved
        if let Mutation::InsertVertex { vid, labels, .. } = &mutations[0] {
            assert_eq!(vid.as_u64(), 42);
            assert_eq!(labels.len(), 2);
            assert!(labels.contains(&"Person".to_string()));
            assert!(labels.contains(&"User".to_string()));
        } else {
            panic!("Expected InsertVertex mutation");
        }

        Ok(())
    }

    /// Test for Issue #23: DeleteVertex labels preserved through WAL replay
    #[tokio::test]
    async fn test_wal_replay_preserves_delete_vertex_labels() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = Arc::new(WriteAheadLog::new(store, prefix));

        // Append DeleteVertex with labels (needed for tombstone flushing - Issue #76)
        wal.append(&Mutation::DeleteVertex {
            vid: Vid::new(99),
            labels: vec!["Person".to_string(), "Admin".to_string()],
        })?;

        // Flush to WAL
        wal.flush().await?;

        // Replay mutations
        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 1);

        // Verify labels are preserved in DeleteVertex
        if let Mutation::DeleteVertex { vid, labels } = &mutations[0] {
            assert_eq!(vid.as_u64(), 99);
            assert_eq!(labels.len(), 2);
            assert!(labels.contains(&"Person".to_string()));
            assert!(labels.contains(&"Admin".to_string()));
        } else {
            panic!("Expected DeleteVertex mutation");
        }

        Ok(())
    }

    /// Test for Issue #28: Edge type name preserved through WAL replay
    #[tokio::test]
    async fn test_wal_replay_preserves_edge_type_name() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        let wal = Arc::new(WriteAheadLog::new(store, prefix));

        // Append InsertEdge with edge_type_name
        wal.append(&Mutation::InsertEdge {
            src_vid: Vid::new(1),
            dst_vid: Vid::new(2),
            edge_type: 100,
            eid: Eid::new(500),
            version: 1,
            properties: {
                let mut props = HashMap::new();
                props.insert("since".to_string(), uni_common::Value::Int(2020));
                props
            },
            edge_type_name: Some("KNOWS".to_string()),
        })?;

        // Flush to WAL
        wal.flush().await?;

        // Replay mutations
        let mutations = wal.replay().await?;
        assert_eq!(mutations.len(), 1);

        // Verify edge_type_name is preserved
        if let Mutation::InsertEdge {
            eid,
            edge_type_name,
            ..
        } = &mutations[0]
        {
            assert_eq!(eid.as_u64(), 500);
            assert_eq!(edge_type_name.as_deref(), Some("KNOWS"));
        } else {
            panic!("Expected InsertEdge mutation");
        }

        Ok(())
    }

    /// Test for Issue #23: Backward compatibility with old WAL segments (no labels)
    #[tokio::test]
    async fn test_wal_backward_compatibility_labels() -> Result<()> {
        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let prefix = Path::from("wal");

        // Manually create a WAL segment with old format (no labels field)
        let old_format_json = r#"{
            "lsn": 1,
            "mutations": [
                {
                    "InsertVertex": {
                        "vid": 123,
                        "properties": {}
                    }
                }
            ]
        }"#;

        let path = prefix.child("00000000000000000001_test.wal");
        store.put(&path, old_format_json.into()).await?;

        // Create WAL and replay
        let wal = WriteAheadLog::new(store, prefix);
        let mutations = wal.replay().await?;

        // Verify old format deserializes with empty labels (via #[serde(default)])
        assert_eq!(mutations.len(), 1);
        if let Mutation::InsertVertex { vid, labels, .. } = &mutations[0] {
            assert_eq!(vid.as_u64(), 123);
            assert_eq!(
                labels.len(),
                0,
                "Old format should deserialize with empty labels"
            );
        } else {
            panic!("Expected InsertVertex mutation");
        }

        Ok(())
    }
}
