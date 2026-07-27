// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The kernel catalog — one source of truth for every guest-callable kernel.
//!
//! Before this module the op names existed only as string literals inside
//! `dispatch.rs`'s `match`, which ended in a wildcard. Nothing connected them to
//! the Rhai `register_fn` list or the PyO3 `#[pymethods]` block, so the three
//! surfaces could — and did — drift apart silently: `edge_count` was
//! dispatchable over JSON while being invisible to Rhai and Python guests. That
//! is the same defect issue #152 reported, and issue #151 before it.
//!
//! The fix mirrors [`ProjectionKnob`] (#151) and `every_variant_classified_exactly_once`
//! (capability model), with one strengthening: the `kernels!` macro generates
//! the enum, [`KernelId::ALL`], [`KernelId::op_name`], [`KernelId::reach`] and
//! [`KernelId::from_op_name`] from a *single* declaration, so those cannot drift
//! from each other by construction. In the #151 pattern `ALL` is hand-maintained
//! and only a runtime test catches an un-appended variant; here it is generated.
//!
//! Adding a kernel is one line. Forgetting to dispatch it is a compile error
//! (`dispatch.rs` matches on `KernelId` with no wildcard); forgetting to expose
//! it to a loader is a test failure (see the per-loader contract tests in
//! `uni-plugin-rhai` and `uni-plugin-pyo3`).
//!
//! [`ProjectionKnob`]: uni_plugin::traits::algorithm::ProjectionKnob
//
// Rust guideline compliant

/// Which guest surfaces a kernel is expected to be reachable from.
///
/// The taxonomy deliberately carries two declared-exception buckets. Without
/// them the contract would be bypassed by *omission* — a kernel quietly left off
/// a loader — rather than by an explicit, reviewable decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelReach {
    /// Reachable from every loader: dispatchable over JSON (which serves WASM
    /// and Extism through the shared `graph-call` entrypoint) *and* individually
    /// registered on the in-process Rhai and PyO3 surfaces.
    AllLoaders,
    /// Deliberately absent from the JSON catalog because the host supplies the
    /// value another way — the graph handle reaches sandboxed guests through the
    /// `invoke-algorithm` arguments, so it needs no kernel op.
    HostSuppliedOtherwise,
    /// Present on one in-process loader only, by design.
    ///
    /// **Reserved, currently unused** — the same shape as `KnobReach::HostOnly`
    /// in the #151 contract. It exists so that a future loader-specific kernel
    /// is *declared* as such rather than quietly failing the `AllLoaders`
    /// assertion and tempting someone to weaken the test.
    LoaderLocal,
}

/// Declares the catalog once and derives every projection of it.
macro_rules! kernels {
    ($( $variant:ident => $name:literal, $reach:expr ; )*) => {
        /// A guest-callable kernel, identified independently of any one surface.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum KernelId {
            $( #[doc = concat!("The `", $name, "` kernel.")] $variant, )*
        }

        impl KernelId {
            /// Every kernel in the catalog. Generated, so it cannot omit a variant.
            pub const ALL: &'static [KernelId] = &[ $( KernelId::$variant, )* ];

            /// The wire/registration name, identical across every surface.
            #[must_use]
            pub fn op_name(self) -> &'static str {
                match self { $( Self::$variant => $name, )* }
            }

            /// How far this kernel is expected to reach.
            #[must_use]
            pub fn reach(self) -> KernelReach {
                match self { $( Self::$variant => $reach, )* }
            }

            /// Resolves a wire name to its kernel, or `None` if unknown.
            #[must_use]
            pub fn from_op_name(name: &str) -> Option<Self> {
                match name { $( $name => Some(Self::$variant), )* _ => None }
            }
        }
    };
}

// `LoaderLocal` is intentionally not imported: it is a reserved variant with no
// current member, so importing it would warn. Add the import with its first use.
use KernelReach::{AllLoaders, HostSuppliedOtherwise};

kernels! {
    // Graph shape and stored properties.
    Graph => "graph", HostSuppliedOtherwise;
    GraphNamed => "graph_named", HostSuppliedOtherwise;
    Rekey => "rekey", AllLoaders;
    Interp => "interp", AllLoaders;
    VertexCount => "vertex_count", AllLoaders;
    EdgeCount => "edge_count", AllLoaders;
    Degrees => "degrees", AllLoaders;
    VertexIds => "vertex_ids", AllLoaders;
    NodeProperty => "node_property", AllLoaders;
    EdgeProperty => "edge_property", AllLoaders;
    EdgeWeights => "edge_weights", AllLoaders;
    EdgesAll => "edges_all", AllLoaders;

    // Vertex sets.
    Frontier => "frontier", AllLoaders;
    SetToMap => "set_to_map", AllLoaders;
    MapToSet => "map_to_set", AllLoaders;
    SetUnion => "set_union", AllLoaders;
    SetDiff => "set_diff", AllLoaders;
    SetIntersect => "set_intersect", AllLoaders;
    SetLen => "set_len", AllLoaders;
    IsEmpty => "is_empty", AllLoaders;

    // Tensors and maps.
    Ewise => "ewise", AllLoaders;
    Compare => "compare", AllLoaders;
    WorkBudget => "work_budget", AllLoaders;
    WorkSpent => "work_spent", AllLoaders;
    WorkRemaining => "work_remaining", AllLoaders;
    ZeroMap => "zero_map", AllLoaders;
    Scatter => "scatter", AllLoaders;
    MapApply => "map_apply", AllLoaders;
    Recip => "recip", AllLoaders;
    Scale => "scale", AllLoaders;
    Normalize => "normalize", AllLoaders;
    ReduceSum => "reduce_sum", AllLoaders;
    ReduceSumMasked => "reduce_sum_masked", AllLoaders;
    ArgExtreme => "arg_extreme", AllLoaders;
    Topk => "topk", AllLoaders;
    L1Diff => "l1_diff", AllLoaders;

    // Traversal.
    Expand => "expand", AllLoaders;
    ReachFixpoint => "reach_fixpoint", AllLoaders;
    ExpandMasked => "expand_masked", AllLoaders;
    ExpandSampled => "expand_sampled", AllLoaders;
    Spmv => "spmv", AllLoaders;
    SpmvMasked => "spmv_masked", AllLoaders;
    NextBucket => "next_bucket", AllLoaders;

    // Edge sets.
    EdgeSetLen => "edge_set_len", AllLoaders;
    EdgeMaskWindow => "edge_mask_window", AllLoaders;
    EdgeIntersect => "edge_intersect", AllLoaders;
    EdgeUnion => "edge_union", AllLoaders;
    SampleEdges => "sample_edges", AllLoaders;
    SampleEdgesUndirected => "sample_edges_undirected", AllLoaders;
    SegmentedReduce => "segmented_reduce", AllLoaders;

    // Walks.
    RandomWalks => "random_walks", AllLoaders;
    WalkVisitCounts => "walk_visit_counts", AllLoaders;
    EmitWalks => "emit_walks", AllLoaders;
    Sample => "sample", AllLoaders;

    // Pairs.
    NeighborhoodOverlap => "neighborhood_overlap", AllLoaders;
    AllPairsOverlap => "all_pairs_overlap", AllLoaders;
    EmitPairs => "emit_pairs", AllLoaders;

    // Emit and lifecycle.
    Emit => "emit", AllLoaders;
    Free => "free", AllLoaders;

    // Mutable arena (`graph-arena@1`, proposal §5.1 / §13.6).
    ArenaNew => "arena_new", AllLoaders;
    ArenaAlloc => "arena_alloc", AllLoaders;
    ArenaLink => "arena_link", AllLoaders;
    ArenaColumn => "arena_column", AllLoaders;
    ArenaCandidates => "arena_candidates", AllLoaders;
    ArenaGather => "arena_gather", AllLoaders;
    ArenaScatter => "arena_scatter", AllLoaders;
    ArenaBackup => "arena_backup", AllLoaders;
    ArenaDescend => "arena_descend", AllLoaders;
    ArenaExpand => "arena_expand", AllLoaders;
    ArenaFreeze => "arena_freeze", AllLoaders;
    ArenaSeed => "arena_seed", AllLoaders;
}

impl KernelId {
    /// The kernels every loader must expose — what the contract tests assert.
    pub fn all_loaders() -> impl Iterator<Item = KernelId> {
        Self::ALL
            .iter()
            .copied()
            .filter(|k| k.reach() == KernelReach::AllLoaders)
    }

    /// The kernels an **in-process** loader (Rhai, PyO3) must expose.
    ///
    /// [`KernelReach::HostSuppliedOtherwise`] means "a *sandboxed* guest receives
    /// this in its invocation arguments instead of calling for it" — it is a
    /// statement about WASM and Extism, not a blanket exemption. Rhai and PyO3
    /// guests hold a session object and call `graph()` / `graph_named(..)` on it,
    /// so for them these are ordinary methods that must exist.
    ///
    /// Filtering the in-process reachability tests on [`Self::all_loaders`] alone
    /// let `graph_named` ship with no assertion that either loader registered it;
    /// `graph` had been in the same position since the bucket was introduced.
    pub fn in_process() -> impl Iterator<Item = KernelId> {
        Self::ALL.iter().copied().filter(|k| {
            matches!(
                k.reach(),
                KernelReach::AllLoaders | KernelReach::HostSuppliedOtherwise
            )
        })
    }
}

#[cfg(test)]
mod kernel_catalog_contract {
    use super::*;
    use std::collections::HashSet;

    /// Names are the join key across all three surfaces, so duplicates would
    /// make the per-loader contracts silently weaker (two kernels, one probe).
    #[test]
    fn every_kernel_has_a_unique_name_that_round_trips() {
        let mut seen = HashSet::new();
        for k in KernelId::ALL {
            assert!(
                seen.insert(k.op_name()),
                "duplicate kernel name `{}`",
                k.op_name()
            );
            assert_eq!(
                KernelId::from_op_name(k.op_name()),
                Some(*k),
                "`{}` does not round-trip through from_op_name",
                k.op_name()
            );
            let _ = k.reach();
        }
    }

    /// `HostSuppliedOtherwise` must stay populated, because it is the bucket
    /// that keeps `AllLoaders` honest: without it, `graph` — which has no wire
    /// form — would either fail the reachability assertion or tempt someone to
    /// weaken it. `LoaderLocal` is deliberately empty and reserved.
    #[test]
    fn the_host_supplied_bucket_is_actually_used() {
        let host_supplied: Vec<&str> = KernelId::ALL
            .iter()
            .filter(|k| k.reach() == KernelReach::HostSuppliedOtherwise)
            .map(|k| k.op_name())
            .collect();
        assert_eq!(
            host_supplied,
            vec!["graph", "graph_named"],
            "if this bucket changes, the reachability contract's carve-outs changed too"
        );
    }

    #[test]
    fn unknown_names_do_not_resolve() {
        assert_eq!(KernelId::from_op_name("vertex_kount"), None);
        assert_eq!(KernelId::from_op_name(""), None);
    }
}
