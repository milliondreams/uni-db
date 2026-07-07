// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::{CrdtMerge, LWWRegister};
use fxhash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::hash::Hash;

/// A Last-Writer-Wins (LWW) Map.
///
/// Each key in the map is managed by an independent LWWRegister.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LWWMap<K: Hash + Eq + Clone, V: Clone> {
    map: FxHashMap<K, LWWRegister<Option<V>>>,
}

impl<K: Hash + Eq + Clone, V: Clone> Default for LWWMap<K, V> {
    fn default() -> Self {
        Self {
            map: FxHashMap::default(),
        }
    }
}

impl<K: Hash + Eq + Clone, V: Clone> LWWMap<K, V> {
    /// Create a new, empty LWWMap.
    pub fn new() -> Self {
        Self::default()
    }

    /// Put a key-value pair into the map with a timestamp.
    ///
    /// A first write to a key that is not yet present always wins: instead of
    /// seeding a sentinel-timestamped register (which would reserve part of the
    /// `i64` range and silently drop writes with sufficiently negative
    /// timestamps), the register is created directly from this write.
    pub fn put(&mut self, key: K, value: V, timestamp: i64) {
        match self.map.get_mut(&key) {
            Some(register) => register.set(Some(value), timestamp),
            None => {
                self.map.insert(key, LWWRegister::new(Some(value), timestamp));
            }
        }
    }

    /// Remove a key from the map with a timestamp (using a tombstone).
    ///
    /// As with [`put`](Self::put), a first observation of a key is recorded
    /// directly from this write, so no sentinel timestamp is reserved and every
    /// `i64` timestamp behaves correctly.
    pub fn remove(&mut self, key: &K, timestamp: i64) {
        match self.map.get_mut(key) {
            Some(register) => register.set(None, timestamp),
            None => {
                self.map
                    .insert(key.clone(), LWWRegister::new(None, timestamp));
            }
        }
    }

    /// Get the value associated with a key.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key).and_then(|reg| reg.get().as_ref())
    }

    /// Returns an iterator over all keys that have a value (not tombstoned).
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.map
            .iter()
            .filter(|(_, reg)| reg.get().is_some())
            .map(|(k, _)| k)
    }

    /// Returns the number of non-tombstoned entries.
    pub fn len(&self) -> usize {
        self.map.values().filter(|reg| reg.get().is_some()).count()
    }

    /// Returns true if the map has no non-tombstoned entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K: Hash + Eq + Clone, V: Clone + Serialize> CrdtMerge for LWWMap<K, V> {
    fn merge(&mut self, other: &Self) {
        for (key, other_register) in &other.map {
            match self.map.get_mut(key) {
                Some(register) => register.merge(other_register),
                None => {
                    self.map.insert(key.clone(), other_register.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_get() {
        let mut map = LWWMap::new();
        map.put("a".to_string(), 1, 100);
        map.put("b".to_string(), 2, 110);
        assert_eq!(map.get(&"a".to_string()), Some(&1));
        assert_eq!(map.get(&"b".to_string()), Some(&2));

        map.put("a".to_string(), 3, 105);
        assert_eq!(map.get(&"a".to_string()), Some(&3));
    }

    #[test]
    fn test_remove() {
        let mut map = LWWMap::new();
        map.put("a".to_string(), 1, 100);
        map.remove(&"a".to_string(), 110);
        assert_eq!(map.get(&"a".to_string()), None);

        map.put("a".to_string(), 2, 105);
        assert_eq!(map.get(&"a".to_string()), None); // 105 < 110
    }

    #[test]
    fn test_merge() {
        let mut a = LWWMap::new();
        a.put("a".to_string(), 1, 100);

        let mut b = LWWMap::new();
        b.put("a".to_string(), 2, 110);
        b.put("b".to_string(), 3, 100);

        a.merge(&b);
        assert_eq!(a.get(&"a".to_string()), Some(&2));
        assert_eq!(a.get(&"b".to_string()), Some(&3));
    }
}
