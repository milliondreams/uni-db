use rustc_hash::{FxHashMap, FxHashSet};
use std::ops::ControlFlow;
use uni_common::core::id::{Eid, Vid};

use super::nfa::{NfaStateId, PathMode, PathSelector};

/// A single predecessor record in the DAG pool.
///
/// Records that to reach `(dst_vid, dst_state)` at a given depth,
/// one can come from `(src_vid, src_state)` via edge `eid`.
/// The `next` field implements a singly-linked list within the pool.
#[derive(Debug, Clone)]
pub struct PredRec {
    pub src_vid: Vid,
    pub src_state: NfaStateId,
    pub eid: Eid,
    /// Index of next PredRec in the chain, or -1 for end of chain.
    pub next: i32,
}

/// Predecessor DAG for path enumeration.
///
/// During BFS frontier expansion, predecessors are recorded into an append-only pool.
/// After BFS completes, backward DFS through the DAG enumerates all valid paths
/// while applying Trail/Acyclic/Simple filtering.
pub struct PredecessorDag {
    /// Append-only pool of predecessor records.
    pred_pool: Vec<PredRec>,

    /// Head of predecessor chain for each `(dst_vid, dst_state, depth)`.
    /// Value is an index into `pred_pool`, or absent if no predecessors.
    pred_head: FxHashMap<(Vid, NfaStateId, u32), i32>,

    /// First-visit depth for each `(vid, state)`.
    first_depth: FxHashMap<(Vid, NfaStateId), u32>,

    /// Path selector determines DAG construction mode.
    selector: PathSelector,
}

impl PredecessorDag {
    /// Create a new empty DAG with the given path selector.
    pub fn new(selector: PathSelector) -> Self {
        Self {
            pred_pool: Vec::new(),
            pred_head: FxHashMap::default(),
            first_depth: FxHashMap::default(),
            selector,
        }
    }

    /// Returns true for layered DAG modes (All/Any), false for shortest-only.
    pub fn is_layered(&self) -> bool {
        matches!(self.selector, PathSelector::All | PathSelector::Any)
    }

    /// Add a predecessor record to the DAG.
    ///
    /// Records that to reach `(dst, dst_state)` at `depth`, one can come from
    /// `(src, src_state)` via edge `eid`. In shortest-only mode, predecessors
    /// at depths greater than the first-visit depth are skipped.
    pub fn add_predecessor(
        &mut self,
        dst: Vid,
        dst_state: NfaStateId,
        src: Vid,
        src_state: NfaStateId,
        eid: Eid,
        depth: u32,
    ) {
        // Update first_depth to track minimum discovery depth.
        let first = self.first_depth.entry((dst, dst_state)).or_insert(depth);
        if depth < *first {
            *first = depth;
        }

        // For shortest-only mode, skip predecessors at depths > first visit.
        if !self.is_layered() && depth > *self.first_depth.get(&(dst, dst_state)).unwrap() {
            return;
        }

        // Get current head for this (dst, dst_state, depth) chain.
        let key = (dst, dst_state, depth);
        let current_head = self.pred_head.get(&key).copied().unwrap_or(-1);

        // Append new PredRec to pool, linking to current head.
        let new_idx = self.pred_pool.len() as i32;
        self.pred_pool.push(PredRec {
            src_vid: src,
            src_state,
            eid,
            next: current_head,
        });

        // Update head to point to new record.
        self.pred_head.insert(key, new_idx);
    }

    /// Enumerate all valid paths from `source` to `target` through `accepting_state`.
    ///
    /// Iterates over depths in `[min_depth, max_depth]`, performing backward DFS
    /// through predecessor chains. Applies mode-specific filtering (Trail/Acyclic/Simple).
    /// The callback can return `ControlFlow::Break(())` to stop enumeration early.
    ///
    /// This is the run-to-completion form. It is a thin wrapper over
    /// [`PathEnumerator`], which is the same walk with its stack made explicit
    /// so a caller that needs to stop and continue later can. Keeping this
    /// wrapper is deliberate: it means every existing caller and test exercises
    /// the resumable walk, so the two cannot drift apart.
    #[expect(
        clippy::too_many_arguments,
        reason = "path enumeration requires full traversal context"
    )]
    pub fn enumerate_paths<F>(
        &self,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
        mode: &PathMode,
        yield_path: &mut F,
    ) where
        F: FnMut(&[Vid], &[Eid]) -> ControlFlow<()>,
    {
        let mut cursor = PathEnumerator::new(
            self,
            source,
            target,
            accepting_state,
            min_depth,
            max_depth,
            mode.clone(),
        );
        let _ = cursor.resume(self, yield_path);
    }

    /// Check if at least one Trail-valid path exists from source to target.
    ///
    /// Returns true on the first valid path found (early-stop).
    pub fn has_trail_valid_path(
        &self,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
    ) -> bool {
        let mut found = false;
        self.enumerate_paths(
            source,
            target,
            accepting_state,
            min_depth,
            max_depth,
            &PathMode::Trail,
            &mut |_nodes, _edges| {
                found = true;
                ControlFlow::Break(())
            },
        );
        found
    }

    /// Get the number of records in the predecessor pool.
    pub fn pool_len(&self) -> usize {
        self.pred_pool.len()
    }

    /// Get the first-visit depth for a (vid, state) pair, if any.
    pub fn first_depth_of(&self, vid: Vid, state: NfaStateId) -> Option<u32> {
        self.first_depth.get(&(vid, state)).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vid(n: u64) -> Vid {
        Vid::new(n)
    }
    fn eid(n: u64) -> Eid {
        Eid::new(n)
    }

    /// Collect all enumerated paths as (Vec<Vid>, Vec<Eid>) pairs.
    fn collect_paths(
        dag: &PredecessorDag,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
        mode: &PathMode,
    ) -> Vec<(Vec<Vid>, Vec<Eid>)> {
        let mut paths = Vec::new();
        dag.enumerate_paths(
            source,
            target,
            accepting_state,
            min_depth,
            max_depth,
            mode,
            &mut |nodes, edges| {
                paths.push((nodes.to_vec(), edges.to_vec()));
                ControlFlow::Continue(())
            },
        );
        paths
    }

    /// An independent enumerator, written as the obvious recursion directly
    /// over the DAG, used as an oracle for [`PathEnumerator`].
    ///
    /// Deliberately not a refactor of the real one: the explicit-stack walk
    /// replaced a recursion, and a self-comparison cannot see a bug that the
    /// stack machine and its own pause path share. Its job is to be slow,
    /// obvious and wrong in different ways.
    fn brute_force_paths(
        dag: &PredecessorDag,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
        mode: &PathMode,
    ) -> Vec<(Vec<Vid>, Vec<Eid>)> {
        #[expect(
            clippy::too_many_arguments,
            reason = "oracle mirrors the walk's context"
        )]
        fn walk(
            dag: &PredecessorDag,
            source: Vid,
            at: Vid,
            state: NfaStateId,
            remaining: u32,
            nodes: &mut Vec<Vid>,
            edges: &mut Vec<Eid>,
            mode: &PathMode,
            out: &mut Vec<(Vec<Vid>, Vec<Eid>)>,
        ) {
            if remaining == 0 {
                if at == source {
                    out.push((
                        nodes.iter().rev().copied().collect(),
                        edges.iter().rev().copied().collect(),
                    ));
                }
                return;
            }
            let mut idx = dag
                .pred_head
                .get(&(at, state, remaining))
                .copied()
                .unwrap_or(-1);
            while idx >= 0 {
                let pred = dag.pred_pool[idx as usize].clone();
                // Membership is recomputed from the path itself every time,
                // so there is no incremental set to forget to undo -- which is
                // exactly the class of bug this oracle exists to catch.
                let skip = match mode {
                    PathMode::Walk => false,
                    PathMode::Trail => edges.contains(&pred.eid),
                    PathMode::Acyclic => nodes.contains(&pred.src_vid),
                    PathMode::Simple => {
                        nodes.contains(&pred.src_vid) && !(remaining == 1 && pred.src_vid == source)
                    }
                };
                if !skip {
                    nodes.push(pred.src_vid);
                    edges.push(pred.eid);
                    walk(
                        dag,
                        source,
                        pred.src_vid,
                        pred.src_state,
                        remaining - 1,
                        nodes,
                        edges,
                        mode,
                        out,
                    );
                    nodes.pop();
                    edges.pop();
                }
                idx = pred.next;
            }
        }

        let mut out = Vec::new();
        for depth in min_depth..=max_depth {
            if depth == 0 {
                if source == target {
                    out.push((vec![source], vec![]));
                }
                continue;
            }
            let mut nodes = vec![target];
            let mut edges = Vec::new();
            walk(
                dag,
                source,
                target,
                accepting_state,
                depth,
                &mut nodes,
                &mut edges,
                mode,
                &mut out,
            );
        }
        out
    }

    /// The rewritten walk must agree with the oracle on every mode and depth.
    ///
    /// This is the absolute check; [`assert_pause_equivalent`] is the relative
    /// one. Both are needed: the oracle cannot see a pause bug, and the
    /// equivalence cannot see a bug the walk has whether it pauses or not.
    #[test]
    fn explicit_stack_walk_matches_the_brute_force_oracle() {
        let dag = mesh_dag(4);
        for mode in [
            PathMode::Walk,
            PathMode::Trail,
            PathMode::Acyclic,
            PathMode::Simple,
        ] {
            for max_depth in 1..=4 {
                for target in [vid(0), vid(1), vid(3)] {
                    let expected = brute_force_paths(&dag, vid(0), target, 0, 0, max_depth, &mode);
                    let actual = collect_paths(&dag, vid(0), target, 0, 0, max_depth, &mode);
                    assert_eq!(
                        actual, expected,
                        "walk diverged from the oracle ({mode:?}, depth<={max_depth}, target {target:?})"
                    );
                }
            }
        }
    }

    /// Drive a [`PathEnumerator`] one path per `resume` call.
    fn collect_paths_paused(
        dag: &PredecessorDag,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
        mode: &PathMode,
    ) -> Vec<(Vec<Vid>, Vec<Eid>)> {
        let mut cursor = PathEnumerator::new(
            dag,
            source,
            target,
            accepting_state,
            min_depth,
            max_depth,
            mode.clone(),
        );
        let mut paths = Vec::new();
        // One path per call, so every path in the set is produced across a
        // pause boundary rather than within a single uninterrupted walk.
        while !cursor.is_finished() {
            let mut got = None;
            let _ = cursor.resume(dag, &mut |nodes, edges| {
                got = Some((nodes.to_vec(), edges.to_vec()));
                ControlFlow::Break(())
            });
            match got {
                Some(path) => paths.push(path),
                // `resume` ran out without yielding: the range is exhausted.
                None => break,
            }
            // A thousand paths is far above any fixture here; a cursor that
            // fails to advance would otherwise hang the suite instead of
            // failing it.
            assert!(paths.len() < 1000, "cursor is not advancing");
        }
        paths
    }

    /// A walk paused after every single path must produce exactly the sequence
    /// the uninterrupted walk produces -- same paths, same order, no repeats
    /// and none dropped at a pause boundary.
    ///
    /// This is the property the run-to-completion tests cannot see: they only
    /// ever call `resume` once, so a frame restored wrongly after a `Break`
    /// would leave every one of them green.
    fn assert_pause_equivalent(
        dag: &PredecessorDag,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
        mode: &PathMode,
    ) {
        let whole = collect_paths(
            dag,
            source,
            target,
            accepting_state,
            min_depth,
            max_depth,
            mode,
        );
        let paused = collect_paths_paused(
            dag,
            source,
            target,
            accepting_state,
            min_depth,
            max_depth,
            mode,
        );
        assert_eq!(
            whole, paused,
            "paused enumeration diverged from the uninterrupted walk ({mode:?})"
        );
    }

    /// Fully-connected triangle with both directions on every pair, which is
    /// the shape that makes an unbounded pattern explode: every mode's
    /// bookkeeping (`edge_set` for Trail, `node_counts` for Acyclic/Simple) has to
    /// survive a pause, and a cycle means a mis-restored set shows up as a
    /// wrong path rather than a missing one.
    fn mesh_dag(max_depth: u32) -> PredecessorDag {
        let mut dag = PredecessorDag::new(PathSelector::All);
        let n = 4u64;
        for depth in 1..=max_depth {
            for src in 0..n {
                for dst in 0..n {
                    if src == dst {
                        continue;
                    }
                    dag.add_predecessor(vid(dst), 0, vid(src), 0, eid(src * n + dst), depth);
                }
            }
        }
        dag
    }

    #[test]
    fn pause_after_every_path_matches_uninterrupted_walk() {
        let dag = mesh_dag(4);
        for mode in [
            PathMode::Walk,
            PathMode::Trail,
            PathMode::Acyclic,
            PathMode::Simple,
        ] {
            for max_depth in 1..=4 {
                assert_pause_equivalent(&dag, vid(0), vid(3), 0, 1, max_depth, &mode);
            }
        }
    }

    #[test]
    fn pause_equivalence_holds_across_a_depth_range() {
        // `min_depth < max_depth` makes `resume` cross a depth boundary, where
        // the stack is re-seeded. A pause landing on that boundary is the case
        // a single-depth fixture never reaches.
        let dag = mesh_dag(3);
        for mode in [PathMode::Walk, PathMode::Trail, PathMode::Acyclic] {
            assert_pause_equivalent(&dag, vid(0), vid(1), 0, 1, 3, &mode);
        }
    }

    #[test]
    fn pause_equivalence_covers_the_zero_length_path() {
        // depth 0 with source == target yields the single empty path, and it is
        // produced by a different branch of `resume` than every other path.
        let dag = mesh_dag(2);
        let paused = collect_paths_paused(&dag, vid(0), vid(0), 0, 0, 0, &PathMode::Trail);
        assert_eq!(paused, vec![(vec![vid(0)], vec![])]);
        assert_pause_equivalent(&dag, vid(0), vid(0), 0, 0, 2, &PathMode::Trail);
    }

    #[test]
    fn a_cursor_stopped_early_leaves_the_rest_unproduced() {
        // The point of the whole cursor: taking 3 paths must not walk the set.
        let dag = mesh_dag(4);
        let all = collect_paths(&dag, vid(0), vid(3), 0, 1, 4, &PathMode::Trail);
        assert!(all.len() > 3, "fixture too small to show early stop");

        let mut cursor = PathEnumerator::new(&dag, vid(0), vid(3), 0, 1, 4, PathMode::Trail);
        let mut taken = Vec::new();
        let _ = cursor.resume(&dag, &mut |nodes, edges| {
            taken.push((nodes.to_vec(), edges.to_vec()));
            if taken.len() == 3 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        assert_eq!(taken.len(), 3);
        assert!(!cursor.is_finished());
        assert_eq!(taken, all[..3].to_vec());
    }

    // ── Pool Storage Tests (23-26) ─────────────────────────────────────

    #[test]
    fn test_pred_dag_add_single() {
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(10), 1);
        assert_eq!(dag.pool_len(), 1);
        assert!(dag.pred_head.contains_key(&(vid(2), 1, 1)));
    }

    #[test]
    fn test_pred_dag_add_chain() {
        // A(0)→B(1)→C(2) chain
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(11), 2);
        assert_eq!(dag.pool_len(), 2);
        assert!(dag.pred_head.contains_key(&(vid(1), 1, 1)));
        assert!(dag.pred_head.contains_key(&(vid(2), 2, 2)));
    }

    #[test]
    fn test_pred_dag_multiple_preds() {
        // Both A(0) and B(1) reach C(2) at depth 1
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 1);
        assert_eq!(dag.pool_len(), 2);
        // Both are in the same chain for (vid(2), state=1, depth=1)
        let head = dag.pred_head[&(vid(2), 1, 1)];
        assert!(head >= 0);
        let first = &dag.pred_pool[head as usize];
        assert!(first.next >= 0); // Chain has a second element
    }

    #[test]
    fn test_pred_dag_first_depth() {
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 3);
        assert_eq!(dag.first_depth_of(vid(2), 1), Some(3));

        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 2);
        assert_eq!(dag.first_depth_of(vid(2), 1), Some(2));

        // Adding at higher depth doesn't change first_depth
        dag.add_predecessor(vid(2), 1, vid(3), 0, eid(12), 5);
        assert_eq!(dag.first_depth_of(vid(2), 1), Some(2));
    }

    // ── PathSelector / DAG Mode Tests (27-29) ─────────────────────────

    #[test]
    fn test_pred_dag_layered_stores_all() {
        let mut dag = PredecessorDag::new(PathSelector::All);
        assert!(dag.is_layered());

        // Add at depth 2 and depth 3 — both should be stored
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 2);
        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 3);
        assert_eq!(dag.pool_len(), 2);
        assert!(dag.pred_head.contains_key(&(vid(2), 1, 2)));
        assert!(dag.pred_head.contains_key(&(vid(2), 1, 3)));
    }

    #[test]
    fn test_pred_dag_shortest_skips() {
        let mut dag = PredecessorDag::new(PathSelector::AnyShortest);
        assert!(!dag.is_layered());

        // First visit at depth 2
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 2);
        assert_eq!(dag.pool_len(), 1);

        // Second visit at depth 3 — should be skipped
        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 3);
        assert_eq!(dag.pool_len(), 1); // Still 1 — depth 3 was skipped

        // Another visit at depth 2 — should be stored (same depth as first)
        dag.add_predecessor(vid(2), 1, vid(3), 0, eid(12), 2);
        assert_eq!(dag.pool_len(), 2);
    }

    #[test]
    fn test_pred_dag_selector_switch() {
        // Same graph, different selectors produce different pool sizes
        let build = |selector: PathSelector| -> usize {
            let mut dag = PredecessorDag::new(selector);
            dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 2);
            dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 3);
            dag.add_predecessor(vid(2), 1, vid(3), 0, eid(12), 4);
            dag.pool_len()
        };

        assert_eq!(build(PathSelector::All), 3); // Layered: stores all
        assert_eq!(build(PathSelector::AnyShortest), 1); // Only depth 2
    }

    // ── Walk Enumeration Tests (30-33) ─────────────────────────────────

    #[test]
    fn test_pred_dag_linear_walk() {
        // A(0) → B(1) → C(2) linear chain
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(11), 2);

        let paths = collect_paths(&dag, vid(0), vid(2), 2, 2, 2, &PathMode::Walk);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, vec![vid(0), vid(1), vid(2)]);
        assert_eq!(paths[0].1, vec![eid(10), eid(11)]);
    }

    #[test]
    fn test_pred_dag_diamond_walk() {
        // A(0) → {B(1), C(2)} → D(3) diamond
        let mut dag = PredecessorDag::new(PathSelector::All);
        // A→B and A→C at depth 1
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(11), 1);
        // B→D and C→D at depth 2
        dag.add_predecessor(vid(3), 2, vid(1), 1, eid(12), 2);
        dag.add_predecessor(vid(3), 2, vid(2), 1, eid(13), 2);

        let paths = collect_paths(&dag, vid(0), vid(3), 2, 2, 2, &PathMode::Walk);
        assert_eq!(paths.len(), 2);

        let mut sorted: Vec<_> = paths.iter().map(|(n, _)| n.clone()).collect();
        sorted.sort();
        assert!(sorted.contains(&vec![vid(0), vid(1), vid(3)]));
        assert!(sorted.contains(&vec![vid(0), vid(2), vid(3)]));
    }

    #[test]
    fn test_pred_dag_multiple_depths() {
        // Target reachable at depth 1 (state q1) and depth 2 (state q2).
        // In a linear NFA, different depths use different NFA states.
        let mut dag = PredecessorDag::new(PathSelector::All);
        // Direct: A→C at depth 1, arriving at state 1
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 1);
        // Via B: A→B at depth 1 (state 1), B→C at depth 2 (state 2)
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(11), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(12), 2);

        // Depth 1 via accepting state 1
        let paths1 = collect_paths(&dag, vid(0), vid(2), 1, 1, 1, &PathMode::Walk);
        assert_eq!(paths1.len(), 1);
        assert_eq!(paths1[0].0, vec![vid(0), vid(2)]); // [A, C]

        // Depth 2 via accepting state 2
        let paths2 = collect_paths(&dag, vid(0), vid(2), 2, 2, 2, &PathMode::Walk);
        assert_eq!(paths2.len(), 1);
        assert_eq!(paths2[0].0, vec![vid(0), vid(1), vid(2)]); // [A, B, C]

        // Total: 2 paths across both accepting states
        assert_eq!(paths1.len() + paths2.len(), 2);
    }

    #[test]
    fn test_pred_dag_fan_out() {
        // A(0) → {B1(1), B2(2), B3(3)} → C(4)
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(11), 1);
        dag.add_predecessor(vid(3), 1, vid(0), 0, eid(12), 1);
        dag.add_predecessor(vid(4), 2, vid(1), 1, eid(13), 2);
        dag.add_predecessor(vid(4), 2, vid(2), 1, eid(14), 2);
        dag.add_predecessor(vid(4), 2, vid(3), 1, eid(15), 2);

        let paths = collect_paths(&dag, vid(0), vid(4), 2, 2, 2, &PathMode::Walk);
        assert_eq!(paths.len(), 3);
    }

    // ── Trail Mode Tests (34-37) ──────────────────────────────────────

    #[test]
    fn test_pred_dag_trail_no_repeat() {
        // A(0) → B(1) → A(0) → C(2) via edges e1, e2, e1
        // The path A→B→A→B uses edge e1 twice — rejected by Trail
        let mut dag = PredecessorDag::new(PathSelector::All);
        // depth 1: B reached from A via e1
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        // depth 2: A reached from B via e2
        dag.add_predecessor(vid(0), 2, vid(1), 1, eid(2), 2);
        // depth 3: B reached from A via e1 again (same edge!)
        dag.add_predecessor(vid(1), 3, vid(0), 2, eid(1), 3);

        // Walk mode: path exists (edge repeat OK)
        let walk_paths = collect_paths(&dag, vid(0), vid(1), 3, 3, 3, &PathMode::Walk);
        assert_eq!(walk_paths.len(), 1);
        assert_eq!(walk_paths[0].1, vec![eid(1), eid(2), eid(1)]);

        // Trail mode: path rejected (e1 appears twice)
        let trail_paths = collect_paths(&dag, vid(0), vid(1), 3, 3, 3, &PathMode::Trail);
        assert_eq!(trail_paths.len(), 0);
    }

    #[test]
    fn test_pred_dag_trail_allows_node_repeat() {
        // A(0) → B(1) → C(2) → B(1) with distinct edges e1, e2, e3
        // Trail allows this (node B repeated but all edges distinct)
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(2), 2);
        dag.add_predecessor(vid(1), 3, vid(2), 2, eid(3), 3);

        let paths = collect_paths(&dag, vid(0), vid(1), 3, 3, 3, &PathMode::Trail);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, vec![vid(0), vid(1), vid(2), vid(1)]);
        assert_eq!(paths[0].1, vec![eid(1), eid(2), eid(3)]);
    }

    #[test]
    fn test_pred_dag_trail_diamond() {
        // Diamond A→{B,C}→D with distinct edges on each path
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(11), 1);
        dag.add_predecessor(vid(3), 2, vid(1), 1, eid(12), 2);
        dag.add_predecessor(vid(3), 2, vid(2), 1, eid(13), 2);

        // Trail: both paths kept (distinct edges on each)
        let paths = collect_paths(&dag, vid(0), vid(3), 2, 2, 2, &PathMode::Trail);
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn test_pred_dag_trail_cycle_2_hop() {
        // A→B→A on *2 pattern
        // depth 1: B from A via e1
        // depth 2: A from B via e2
        // This uses distinct edges so Trail allows it
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(0), 2, vid(1), 1, eid(2), 2);

        let paths = collect_paths(&dag, vid(0), vid(0), 2, 2, 2, &PathMode::Trail);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, vec![vid(0), vid(1), vid(0)]);
        assert_eq!(paths[0].1, vec![eid(1), eid(2)]);
    }

    // ── Acyclic Mode Tests (38-39) ────────────────────────────────────

    #[test]
    fn test_pred_dag_acyclic_filter() {
        // A(0) → B(1) → C(2) → A(0) — node A repeated
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(2), 2);
        dag.add_predecessor(vid(0), 3, vid(2), 2, eid(3), 3);

        // Walk: path exists
        let walk_paths = collect_paths(&dag, vid(0), vid(0), 3, 3, 3, &PathMode::Walk);
        assert_eq!(walk_paths.len(), 1);

        // Acyclic: path rejected (node A repeated)
        let acyclic_paths = collect_paths(&dag, vid(0), vid(0), 3, 3, 3, &PathMode::Acyclic);
        assert_eq!(acyclic_paths.len(), 0);
    }

    #[test]
    fn test_pred_dag_acyclic_diamond() {
        // Diamond A→{B,C}→D — no node repeats
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(11), 1);
        dag.add_predecessor(vid(3), 2, vid(1), 1, eid(12), 2);
        dag.add_predecessor(vid(3), 2, vid(2), 1, eid(13), 2);

        let paths = collect_paths(&dag, vid(0), vid(3), 2, 2, 2, &PathMode::Acyclic);
        assert_eq!(paths.len(), 2);
    }

    // ── Trail Existence Tests (40-42) ─────────────────────────────────

    #[test]
    fn test_has_trail_valid_true() {
        // Simple A→B→C chain — Trail-valid
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(11), 2);

        assert!(dag.has_trail_valid_path(vid(0), vid(2), 2, 2, 2));
    }

    #[test]
    fn test_has_trail_valid_false() {
        // Only path to target reuses an edge
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(0), 2, vid(1), 1, eid(2), 2);
        dag.add_predecessor(vid(1), 3, vid(0), 2, eid(1), 3); // e1 reused

        assert!(!dag.has_trail_valid_path(vid(0), vid(1), 3, 3, 3));
    }

    #[test]
    fn test_has_trail_valid_one_of_many() {
        // Two paths to target: one valid, one not
        let mut dag = PredecessorDag::new(PathSelector::All);
        // Path 1: A→B→C (valid, distinct edges)
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(2), 2);
        // Path 2: A→D→C (also valid)
        dag.add_predecessor(vid(3), 1, vid(0), 0, eid(3), 1);
        dag.add_predecessor(vid(2), 2, vid(3), 1, eid(4), 2);

        // At least one valid → true
        assert!(dag.has_trail_valid_path(vid(0), vid(2), 2, 2, 2));
    }

    // ── Streaming & Early Termination Tests (43-45) ───────────────────

    #[test]
    fn test_pred_dag_early_stop() {
        // Build a DAG with many paths, verify callback can break early.
        // Fan-out: A → {B1..B10} → C (10 paths of length 2)
        let mut dag = PredecessorDag::new(PathSelector::All);
        for i in 1..=10u64 {
            dag.add_predecessor(Vid::new(i), 1, vid(0), 0, Eid::new(i), 1);
            dag.add_predecessor(vid(99), 2, Vid::new(i), 1, Eid::new(100 + i), 2);
        }

        let mut count = 0;
        dag.enumerate_paths(
            vid(0),
            vid(99),
            2,
            2,
            2,
            &PathMode::Walk,
            &mut |_nodes, _edges| {
                count += 1;
                if count >= 3 {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            },
        );
        assert_eq!(count, 3); // Stopped after 3, not 10
    }

    #[test]
    fn test_pred_dag_empty_enumerate() {
        // Empty DAG — no paths
        let dag = PredecessorDag::new(PathSelector::All);
        let paths = collect_paths(&dag, vid(0), vid(1), 0, 1, 5, &PathMode::Walk);
        assert!(paths.is_empty());
    }

    #[test]
    fn test_pred_dag_zero_length() {
        // Zero-length path: source == target with min_depth=0
        let dag = PredecessorDag::new(PathSelector::All);
        let paths = collect_paths(&dag, vid(5), vid(5), 0, 0, 0, &PathMode::Walk);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, vec![vid(5)]);
        assert!(paths[0].1.is_empty());
    }

    // ── Correctness Tests (46-47) ─────────────────────────────────────

    #[test]
    fn test_pred_dag_path_order() {
        // Verify nodes and edges are in correct forward order (source → target)
        // Build: A(0)→B(1)→C(2)→D(3)
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(20), 2);
        dag.add_predecessor(vid(3), 3, vid(2), 2, eid(30), 3);

        let paths = collect_paths(&dag, vid(0), vid(3), 3, 3, 3, &PathMode::Walk);
        assert_eq!(paths.len(), 1);
        // Forward order: source first, target last
        assert_eq!(paths[0].0, vec![vid(0), vid(1), vid(2), vid(3)]);
        assert_eq!(paths[0].1, vec![eid(10), eid(20), eid(30)]);
    }

    #[test]
    fn test_pred_dag_eid_in_path() {
        // Verify edges match traversal order exactly
        // A(0) →e1→ B(1) →e2→ C(2) →e3→ D(3)
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(100), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(200), 2);
        dag.add_predecessor(vid(3), 3, vid(2), 2, eid(300), 3);

        let paths = collect_paths(&dag, vid(0), vid(3), 3, 3, 3, &PathMode::Walk);
        assert_eq!(paths.len(), 1);

        // Edge e1 connects nodes[0]→nodes[1], e2 connects nodes[1]→nodes[2], etc.
        let (nodes, edges) = &paths[0];
        assert_eq!(nodes.len(), edges.len() + 1);
        assert_eq!(edges[0], eid(100)); // A→B
        assert_eq!(edges[1], eid(200)); // B→C
        assert_eq!(edges[2], eid(300)); // C→D
    }
}

/// One level of the backward DFS, made explicit.
///
/// `idx` is the position in the predecessor chain of this level's
/// `(vid, state, remaining)` key that it is currently considering; `descended` records that the
/// child for `idx` is on the stack, so the undo happens exactly once when
/// control comes back.
#[derive(Debug, Clone)]
struct DfsFrame {
    vid: Vid,
    remaining: u32,
    idx: i32,
    descended: bool,
}

/// A resumable backward DFS over a [`PredecessorDag`].
///
/// The recursive enumerator it replaces could only run to completion: a
/// `ControlFlow::Break` from the callback unwound the Rust stack and threw the
/// position away, so a consumer that wanted the first `n` paths of a large set
/// had to enumerate all of them and discard the rest. That is why `LIMIT 5`
/// over an unbounded pattern on a cyclic graph cost exactly what no limit cost
/// (#285): the operator above could stop asking, but this could not stop
/// producing.
///
/// Holding the DFS stack in a struct makes `Break` a pause instead of an abort.
/// `resume` returns `ControlFlow::Break(())` with every frame intact, and the
/// next call picks up at the path after the one that broke.
///
/// The DAG is passed to each call rather than borrowed by the struct so the
/// cursor can be parked in an operator's state alongside the DAG it walks,
/// which a self-referential borrow would not allow.
pub struct PathEnumerator {
    source: Vid,
    target: Vid,
    accepting_state: NfaStateId,
    /// Depth currently being walked, and the inclusive bound it stops at.
    depth: u32,
    max_depth: u32,
    mode: PathMode,
    stack: Vec<DfsFrame>,
    /// Path under construction, in backward order (target first).
    nodes: Vec<Vid>,
    edges: Vec<Eid>,
    /// How many times each vid appears on the path under construction.
    ///
    /// A count, not a set. `Simple` admits one repeat -- the source may equal
    /// the target -- so the same vid can be pushed twice, and with a set the
    /// first undo erases the seeded target entry, after which every later
    /// branch wrongly re-admits it. The recursion this replaced had the same
    /// defect; it is unreachable from Cypher (the planner only ever emits
    /// `Trail`) but wrong all the same, and the brute-force oracle in the
    /// tests is what surfaced it.
    node_counts: FxHashMap<Vid, u32>,
    edge_set: FxHashSet<Eid>,
    /// A zero-length path is owed for the current depth (`depth == 0` and
    /// `source == target`), and has not been handed over yet.
    zero_pending: bool,
    /// The current depth's stack has been seeded.
    seeded: bool,
    finished: bool,
}

impl PathEnumerator {
    /// Start a cursor over paths of every depth in `[min_depth, max_depth]`.
    pub fn new(
        dag: &PredecessorDag,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
        mode: PathMode,
    ) -> Self {
        let mut this = Self {
            source,
            target,
            accepting_state,
            depth: min_depth,
            max_depth,
            mode,
            stack: Vec::new(),
            nodes: Vec::new(),
            edges: Vec::new(),
            node_counts: FxHashMap::default(),
            edge_set: FxHashSet::default(),
            zero_pending: false,
            seeded: false,
            finished: min_depth > max_depth,
        };
        if !this.finished {
            this.seed(dag);
        }
        this
    }

    /// True once every path in the depth range has been handed over.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Prepare the walk for `self.depth`, leaving `seeded` true.
    fn seed(&mut self, dag: &PredecessorDag) {
        self.stack.clear();
        self.nodes.clear();
        self.edges.clear();
        self.node_counts.clear();
        self.edge_set.clear();
        self.zero_pending = false;
        self.seeded = true;

        if self.depth == 0 {
            self.zero_pending = self.source == self.target;
            return;
        }

        let key = (self.target, self.accepting_state, self.depth);
        let Some(&head) = dag.pred_head.get(&key) else {
            return;
        };

        self.nodes.push(self.target);
        if matches!(self.mode, PathMode::Acyclic | PathMode::Simple) {
            *self.node_counts.entry(self.target).or_insert(0) += 1;
        }
        self.stack.push(DfsFrame {
            vid: self.target,
            remaining: self.depth,
            idx: head,
            descended: false,
        });
    }

    /// Advance to the next depth, or finish.
    fn next_depth(&mut self, dag: &PredecessorDag) {
        if self.depth >= self.max_depth {
            self.finished = true;
            return;
        }
        self.depth += 1;
        self.seed(dag);
    }

    /// Hand paths to `yield_path` until the range is exhausted, or until the
    /// callback breaks.
    ///
    /// Returns `Continue` when there is nothing left (and [`Self::is_finished`]
    /// is then true), or `Break` with the walk paused exactly after the path
    /// the callback rejected.
    pub fn resume<F>(&mut self, dag: &PredecessorDag, yield_path: &mut F) -> ControlFlow<()>
    where
        F: FnMut(&[Vid], &[Eid]) -> ControlFlow<()>,
    {
        loop {
            if self.finished {
                return ControlFlow::Continue(());
            }
            if !self.seeded {
                self.seed(dag);
            }

            if self.zero_pending {
                self.zero_pending = false;
                let src = self.source;
                if yield_path(&[src], &[]).is_break() {
                    return ControlFlow::Break(());
                }
            }

            let Some(li) = self.stack.len().checked_sub(1) else {
                self.next_depth(dag);
                continue;
            };

            if self.stack[li].descended {
                // The child for the current `idx` has finished; undo what was
                // pushed for it, then step past it in the chain.
                self.stack[li].descended = false;
                let node = self.nodes.pop().expect("node pushed with the child frame");
                let edge = self.edges.pop().expect("edge pushed with the child frame");
                match self.mode {
                    PathMode::Trail => {
                        self.edge_set.remove(&edge);
                    }
                    PathMode::Acyclic | PathMode::Simple => {
                        if let Some(count) = self.node_counts.get_mut(&node) {
                            *count -= 1;
                            if *count == 0 {
                                self.node_counts.remove(&node);
                            }
                        }
                    }
                    PathMode::Walk => {}
                }
                let idx = self.stack[li].idx;
                self.stack[li].idx = dag.pred_pool[idx as usize].next;
            }

            let remaining = self.stack[li].remaining;
            if remaining == 0 {
                let vid = self.stack[li].vid;
                self.stack.pop();
                if vid == self.source {
                    let fwd_nodes: Vec<Vid> = self.nodes.iter().rev().copied().collect();
                    let fwd_edges: Vec<Eid> = self.edges.iter().rev().copied().collect();
                    if yield_path(&fwd_nodes, &fwd_edges).is_break() {
                        return ControlFlow::Break(());
                    }
                }
                continue;
            }

            // Walk the chain forward past predecessors the mode rejects.
            loop {
                let idx = self.stack[li].idx;
                if idx < 0 {
                    break;
                }
                let pred = &dag.pred_pool[idx as usize];
                let skip = match self.mode {
                    PathMode::Walk => false,
                    PathMode::Trail => self.edge_set.contains(&pred.eid),
                    PathMode::Acyclic => self.node_counts.contains_key(&pred.src_vid),
                    PathMode::Simple => {
                        // No repeated nodes except source may equal target.
                        self.node_counts.contains_key(&pred.src_vid)
                            && !(remaining == 1 && pred.src_vid == self.source)
                    }
                };
                if skip {
                    self.stack[li].idx = pred.next;
                } else {
                    break;
                }
            }

            let idx = self.stack[li].idx;
            if idx < 0 {
                self.stack.pop();
                continue;
            }

            let pred = dag.pred_pool[idx as usize].clone();
            self.nodes.push(pred.src_vid);
            self.edges.push(pred.eid);
            match self.mode {
                PathMode::Trail => {
                    self.edge_set.insert(pred.eid);
                }
                PathMode::Acyclic | PathMode::Simple => {
                    *self.node_counts.entry(pred.src_vid).or_insert(0) += 1;
                }
                PathMode::Walk => {}
            }
            self.stack[li].descended = true;

            let child_head = dag
                .pred_head
                .get(&(pred.src_vid, pred.src_state, remaining - 1))
                .copied()
                .unwrap_or(-1);
            self.stack.push(DfsFrame {
                vid: pred.src_vid,
                remaining: remaining - 1,
                idx: child_head,
                descended: false,
            });
        }
    }
}
