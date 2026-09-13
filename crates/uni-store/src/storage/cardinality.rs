// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A cached, invalidated row-count statistic readable without `await` (#260).
//!
//! Plan-time code needs to know how big a table is, and the primitive that can
//! answer — `StorageBackend::count_rows` — is `async`. Plan-time paths are not,
//! so every decision that wanted a size was fitted to a constant instead. This
//! caches the `async` half so the sync half can read it.
//!
//! # What is cached, and what is not
//!
//! Only the **flushed** count is cached, because only that half is expensive to
//! obtain. The L0 half is a walk over an in-memory index and is read live on
//! every call, so a write that has not been flushed is never invisible here —
//! which is the failure this is partly written to fix. `crates/uni/src/api/schema.rs`
//! records a `count: 0` returned for a label whose rows were all L0-resident:
//! "a silent wrong answer, and the reason a Python assertion on this value was
//! once weakened rather than fixed".
//!
//! # The contract is an upper bound, deliberately
//!
//! [`CardinalityCache`] cannot be exact without `await`. The flushed count and
//! the set of L0-resident vids for a label are each exact, but their *overlap*
//! is not knowable synchronously: a vertex updated in place appears in both, and
//! an L0 tombstone for a flushed vertex is not subtracted. So the total is an
//! **upper bound** on live rows, and callers are told so.
//!
//! That direction is chosen rather than accepted. Every consumer so far uses the
//! count as a denominator for "is my request big relative to this table"; an
//! over-estimate makes a request look relatively *smaller*, which biases toward
//! the indexed-lookup arm. Lookup costs the request, scan costs the table, so
//! erring toward lookup risks paying more per requested key and never risks
//! reading an entire table by mistake.
//!
//! Exactness is available where it is affordable: `StorageManager::vertex_row_count`
//! remains the `async`, flushed-only, exact answer.

// Rust guideline compliant

use std::collections::HashMap;

use parking_lot::RwLock;

/// Which table a cached count refers to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CardinalityKey {
    /// Rows in a label's vertex table.
    Vertex(String),
    /// Rows of one edge type in the shared main-edges table.
    ///
    /// This one is why the cache earns its keep beyond the sync boundary:
    /// `count_rows(table, None)` is metadata-only, but a *per-type* count needs
    /// a predicate, and a filtered count reads every row. Measured at ~6.6x the
    /// unfiltered form and growing with the table
    /// (`examples/count_rows_probe.rs`), so it is worth not repeating.
    EdgeType(String),
}

/// Flushed row counts, keyed by table, with explicit invalidation.
///
/// Shared by `Arc` across every `StorageManager` derived from one primary. Fork
/// and pinned readers do not consult it — see
/// `StorageManager::cached_row_count`, which declines for them rather than
/// answering from a view that is not theirs.
#[derive(Debug, Default)]
pub struct CardinalityCache {
    flushed: RwLock<HashMap<CardinalityKey, u64>>,
}

impl CardinalityCache {
    /// An empty cache. Every lookup misses until something refreshes it.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The cached flushed count, or `None` if this table has not been counted
    /// since it was last invalidated.
    ///
    /// Synchronous by construction — this is the whole point of the type.
    #[must_use]
    pub fn get(&self, key: &CardinalityKey) -> Option<u64> {
        self.flushed.read().get(key).copied()
    }

    /// Record a freshly measured flushed count.
    pub fn put(&self, key: CardinalityKey, rows: u64) {
        self.flushed.write().insert(key, rows);
    }

    /// Forget one table's count.
    pub fn invalidate(&self, key: &CardinalityKey) {
        self.flushed.write().remove(key);
    }

    /// Forget every count.
    ///
    /// The blunt form, and the right one for compaction and vacuum: both rewrite
    /// files across tables, and an invalidation that tried to enumerate exactly
    /// which tables moved would be a second place to get that wrong. A missing
    /// entry costs one `count_rows` on the next refresh; a stale one is a wrong
    /// answer.
    pub fn invalidate_all(&self) {
        self.flushed.write().clear();
    }

    /// How many tables currently have a cached count. Test observability.
    #[must_use]
    pub fn len(&self) -> usize {
        self.flushed.read().len()
    }

    /// Whether nothing is cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.flushed.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_put_is_readable_and_an_invalidate_forgets_it() {
        let cache = CardinalityCache::new();
        let key = CardinalityKey::Vertex("Person".to_string());
        assert_eq!(cache.get(&key), None, "a fresh cache must miss");

        cache.put(key.clone(), 42);
        assert_eq!(cache.get(&key), Some(42));

        cache.invalidate(&key);
        assert_eq!(
            cache.get(&key),
            None,
            "an invalidated entry must miss rather than return a stale count"
        );
    }

    #[test]
    fn vertex_and_edge_keys_of_the_same_name_do_not_collide() {
        let cache = CardinalityCache::new();
        cache.put(CardinalityKey::Vertex("Knows".to_string()), 1);
        cache.put(CardinalityKey::EdgeType("Knows".to_string()), 2);
        assert_eq!(
            cache.get(&CardinalityKey::Vertex("Knows".to_string())),
            Some(1)
        );
        assert_eq!(
            cache.get(&CardinalityKey::EdgeType("Knows".to_string())),
            Some(2),
            "a label and an edge type may share a name; the key must separate them"
        );
    }

    #[test]
    fn invalidate_all_clears_every_table() {
        let cache = CardinalityCache::new();
        cache.put(CardinalityKey::Vertex("A".to_string()), 1);
        cache.put(CardinalityKey::EdgeType("B".to_string()), 2);
        assert_eq!(cache.len(), 2);
        cache.invalidate_all();
        assert!(cache.is_empty(), "compaction and vacuum clear everything");
    }
}
