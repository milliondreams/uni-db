// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The physical-operator registry the gate enforces.
//!
//! One row per **observable runtime name**, not per impl. `MutationExec`
//! returns one of five per-instance `display_name`s (`mutation_common.rs`), and
//! proving that SET fires says nothing about DETACH DELETE — so those get
//! separate rows keyed on the same `ty`.
//!
//! Adding an operator without a row fails [`super::gate`]. That is the point:
//! the failure mode this exists to stop is an operator quietly ceasing to be
//! emitted while every result-comparing test stays green.

/// How we know whether this operator can run.
///
/// Three states, and the third one needs justifying. The first draft of this
/// registry had only `Proven` and `Unreachable`, on the reasoning that a
/// "pending" variant becomes the permanent home of every hard case. That was
/// right about the risk and wrong about the facts: 27 of these operators are
/// plainly *reachable* — a MATCH emits `GraphScanExec`, UNWIND emits
/// `GraphUnwindExec` — they simply have no test asserting it. Filing them under
/// `Unreachable` would have written a falsehood into the data, and would have
/// broken [`super::gate::unreachable_operators_stay_unreachable`] the moment
/// anyone proved one.
///
/// So `Unproven` exists, and the dumping-ground risk is handled by a **ratchet**
/// instead: `MAX_UNPROVEN` may never increase. New operators must arrive
/// `Proven` or `Unreachable`, and every retrofit lowers the bound permanently.
#[derive(Debug, Clone, Copy)]
pub enum Status {
    /// A named, non-ignored test asserts this operator appears in an executed
    /// plan. The gate verifies the test exists *and* that some test really
    /// calls an assertion helper with this operator's `runtime_name`.
    Proven {
        by: &'static str,
        in_file: &'static str,
    },
    /// No planner path can emit it. The reason is the artifact: it is what the
    /// next reader needs to decide wire-it-up versus delete-it.
    Unreachable { reason: &'static str },
    /// Reachable, but nothing asserts it. This is a *gap*, counted and
    /// ratcheted — never a resting place.
    Unproven,
}

/// One physically-observable execution operator.
#[derive(Debug, Clone, Copy)]
pub struct Operator {
    /// Type named in `impl ExecutionPlan for <ty>`; joins this table to the
    /// source scan. Not unique — see the module docs on `MutationExec`.
    pub ty: &'static str,
    /// What `ExecutionPlan::name()` returns, i.e. what shows up in
    /// `ProfileOutput::runtime_stats[].operator`. This is what assertions match.
    pub runtime_name: &'static str,
    pub status: Status,
}

/// The ratchet. **This number may only ever go down.**
///
/// Every operator here is one that could stop being emitted with nothing
/// turning red — the condition that let `vid_lookup_join.rs` reach 0/441
/// executed lines under a dedicated 15-test suite. Retrofitting a proof lowers
/// the bound; the gate fails if the count exceeds it, so a new operator cannot
/// arrive unproven.
pub const MAX_UNPROVEN: usize = 10;

pub const OPERATORS: &[Operator] = &[
    // ── Measured 2026-08-14 ────────────────────────────────────────────────
    Operator {
        ty: "VidLookupJoinExec",
        runtime_name: "VidLookupJoinExec",
        status: Status::Proven {
            by: "documented_query_uses_the_vid_lookup_join",
            in_file: "crates/uni/tests/common/bugs/vid_lookup_join_reachability.rs",
        },
    },
    // 1,044 lines across these two, constructed ONLY from `iteration_driver.rs`'s
    // own `#[cfg(test)] mod tests`. Their self-tests give them nonzero coverage,
    // which is why a coverage map does not flag them — the opposite blind spot
    // from vid_lookup_join, and the reason this registry exists alongside it.
    Operator {
        ty: "PowerStepExec",
        runtime_name: "PowerStepExec",
        status: Status::Unreachable {
            reason: "No planner path constructs it. df_planner.rs never mentions \
                     iteration_driver; the only call sites are inside that file's own \
                     `#[cfg(test)] mod tests`. Resolve by wiring the iteration driver \
                     into the Locy fixpoint planner (then flip to Proven) or deleting \
                     it — but do not leave it unclassified.",
        },
    },
    Operator {
        ty: "GraphGatherStepExec",
        runtime_name: "GraphGatherStepExec",
        status: Status::Unreachable {
            reason: "Same as PowerStepExec — constructed only from iteration_driver.rs's \
                     own `#[cfg(test)] mod tests`, with no df_planner.rs construction \
                     site. Wire it up or delete it; its unit tests make coverage look \
                     healthy either way.",
        },
    },
    // ── Everything else: recorded, not yet proven ──────────────────────────
    Operator {
        ty: "GraphScanExec",
        runtime_name: "GraphScanExec",
        status: Status::Proven {
            by: "a_labelled_match_runs_the_graph_scan",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphTraverseExec",
        runtime_name: "GraphTraverseExec",
        status: Status::Proven {
            by: "a_declared_single_hop_runs_the_graph_traverse",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphTraverseMainExec",
        runtime_name: "GraphTraverseMainExec",
        status: Status::Proven {
            by: "an_undeclared_single_hop_runs_the_main_traverse_instead",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphVariableLengthTraverseExec",
        runtime_name: "GraphVariableLengthTraverseExec",
        status: Status::Proven {
            by: "a_declared_variable_length_hop_runs_the_vlp_traverse",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphVariableLengthTraverseMainExec",
        runtime_name: "GraphVariableLengthTraverseMainExec",
        status: Status::Proven {
            by: "an_undeclared_variable_length_hop_runs_the_main_vlp_traverse",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationCreateExec",
        status: Status::Proven {
            by: "a_create_runs_the_create_mutation",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationSetExec",
        status: Status::Proven {
            by: "a_set_runs_the_set_mutation_and_not_its_siblings",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationDeleteExec",
        status: Status::Proven {
            by: "a_delete_runs_the_delete_mutation",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationRemoveExec",
        status: Status::Proven {
            by: "a_remove_runs_the_remove_mutation",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "MutationExec",
        runtime_name: "MutationMergeExec",
        status: Status::Proven {
            by: "a_merge_runs_the_merge_mutation",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "ForeachExec",
        runtime_name: "ForeachExec",
        // Was unprovable rather than unproven: #176 found the clause had no
        // grammar rule and no AST variant, so no query could construct the plan
        // node and the 154 executable lines beneath it were unreachable. The
        // front end now exists, and running the operator for the first time
        // immediately turned up two defects in it — a lossy row/batch round
        // trip on the pass-through, and a per-item scope rebuilt from the
        // original row. "Never ran" was not "not worth running", and it was not
        // "works" either.
        status: Status::Proven {
            by: "the_foreach_clause_plans_the_foreach_operator",
            in_file: "crates/uni/tests/common/cypher_write/foreach_test.rs",
        },
    },
    Operator {
        ty: "GraphUnwindExec",
        runtime_name: "GraphUnwindExec",
        status: Status::Proven {
            by: "an_unwind_runs_the_graph_unwind",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "OptionalFilterExec",
        runtime_name: "OptionalFilterExec",
        status: Status::Proven {
            by: "an_optional_match_predicate_runs_the_optional_filter",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphApplyExec",
        runtime_name: "GraphApplyExec",
        status: Status::Proven {
            by: "a_call_subquery_runs_the_graph_apply",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphProcedureCallExec",
        runtime_name: "GraphProcedureCallExec",
        status: Status::Proven {
            by: "a_procedure_call_runs_the_procedure_call_operator",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // Reachable: a plugin `CatalogProvider` registers a virtual label and
    // `MATCH (n:External)` dispatches to it. The fixture exists
    // (`plugin/plugin_virtual_label_dispatch.rs`); this row is a plain gap.
    Operator {
        ty: "CatalogVertexScanExec",
        runtime_name: "CatalogVertexScanExec",
        status: Status::Unproven,
    },
    // Reachable only in principle. `plan_traverse_virtual_edge` requires the
    // traversal's edge-type ids to be *all* virtual, and the one test that
    // exercises it (`plugin_virtual_label_dispatch.rs`) discards its result with
    // a note that a bare `MATCH ()-[r:VirtualRel]->()` "requires a native
    // source/target label resolution that the MVP doesn't cover". Prove it by
    // finishing that resolution, not by writing a test against today's planner.
    Operator {
        ty: "CatalogEdgeScanExec",
        runtime_name: "CatalogEdgeScanExec",
        status: Status::Unproven,
    },
    Operator {
        ty: "GraphVectorKnnExec",
        runtime_name: "GraphVectorKnnExec",
        status: Status::Proven {
            by: "a_vector_similarity_predicate_runs_the_vector_knn",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphShortestPathExec",
        runtime_name: "GraphShortestPathExec",
        status: Status::Proven {
            by: "a_shortest_path_runs_the_shortest_path_operator",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "GraphExtIdLookupExec",
        runtime_name: "GraphExtIdLookupExec",
        status: Status::Proven {
            by: "an_unlabelled_ext_id_match_runs_the_ext_id_lookup",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "RecursiveCTEExec",
        runtime_name: "RecursiveCTEExec",
        status: Status::Proven {
            by: "a_with_recursive_runs_the_recursive_cte",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "BindFixedPathExec",
        runtime_name: "BindFixedPathExec",
        status: Status::Proven {
            by: "a_named_fixed_path_runs_the_bind_fixed_path",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "BindZeroLengthPathExec",
        runtime_name: "BindZeroLengthPathExec",
        status: Status::Proven {
            by: "a_single_node_path_runs_the_bind_zero_length_path",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    Operator {
        ty: "EndpointHydrateExec",
        runtime_name: "EndpointHydrateExec",
        status: Status::Proven {
            by: "the_post_with_endpoint_query_runs_the_hydration_operator",
            in_file: "crates/uni/tests/common/cypher_read/start_end_node_test.rs",
        },
    },
    Operator {
        ty: "ReadSetRecordingExec",
        runtime_name: "ReadSetRecordingExec",
        status: Status::Proven {
            by: "a_transactional_read_runs_the_read_set_recording",
            in_file: "crates/uni/tests/common/plan_shape/proofs.rs",
        },
    },
    // UNOBSERVABLE, not merely unproven. It is the root of a plan that
    // `impl_locy.rs` executes directly, and that execution produces no
    // `ProfileOutput` -- while `LocyProfileOutput` carries only clause-body
    // sub-plans. So it appears in neither profile surface. Provable today only
    // by a `uni-query` test that plans a `LocyProgram` and asserts
    // `plan.name()`; a query-level proof needs a collector change first.
    Operator {
        ty: "LocyProgramExec",
        runtime_name: "LocyProgramExec",
        status: Status::Unproven,
    },
    // UNOBSERVABLE for a structural reason, and the sharpest case here.
    // `LocyProgramExec::execute` constructs it imperatively
    // (`locy_program.rs:934`) and drives it with `collect_all_partitions`; it is
    // never a *child* of any plan, so it cannot reach `runtime_stats` however
    // often it runs. The collector it is handed records the clause bodies
    // inside it, never itself. Unlike PowerStepExec above it genuinely runs --
    // which is why this is not `Unreachable`.
    Operator {
        ty: "FixpointExec",
        runtime_name: "FixpointExec",
        status: Status::Unproven,
    },
    // Reachable and observable, via `session.locy_with(..).profile()` op names
    // (a positive IS-reference cross-joins one in per occurrence). A plain gap:
    // it needs the Locy profile accessor, not a planner change.
    Operator {
        ty: "DerivedScanExec",
        runtime_name: "DerivedScanExec",
        status: Status::Unproven,
    },
    // UNOBSERVABLE. `LocyFold` is never lowered into a clause body -- the
    // planner says wrapping the body would double-apply the aggregate -- so the
    // `FoldExec` that runs is built ad hoc in the post-fixpoint chain, outside
    // every metric-collected plan. `locy_issue_162_fold_scaling.rs` already
    // records this. Fix by adding it to the post-fixpoint op list, then prove.
    Operator {
        ty: "FoldExec",
        runtime_name: "FoldExec",
        status: Status::Unproven,
    },
    // Reachable and observable, but the guard is a trap worth stating: the body
    // wrapper is emitted only when `best_by.is_some() && fold.is_empty()`. With
    // FOLD it is deferred to the post-fixpoint chain and becomes unobservable
    // like `FoldExec`. Every existing BEST BY test in the tree pairs it with
    // FOLD, so none of them can serve as the proof.
    Operator {
        ty: "BestByExec",
        runtime_name: "BestByExec",
        status: Status::Unproven,
    },
    // UNOBSERVABLE, same shape as `FoldExec`: `clause.priority` stays a scalar
    // field and is never lowered into the body plan (the planner's own
    // `test_clause_with_priority` asserts no wrapper). It runs only in the
    // post-fixpoint chain, outside metric collection.
    Operator {
        ty: "PriorityExec",
        runtime_name: "PriorityExec",
        status: Status::Unproven,
    },
    // Reachable and observable: a clause calling a `CREATE MODEL` name emits it,
    // and `LocyConfig::classifier_registry` takes a `MockClassifier` so no
    // external service is needed. A plain gap.
    Operator {
        ty: "LocyModelInvokeExec",
        runtime_name: "LocyModelInvokeExec",
        status: Status::Unproven,
    },
    // Not reachable from Cypher at all: it comes from
    // `StorageTableProvider::scan`, and nothing in `uni`/`uni-query` registers
    // that provider into a session context. The virtual-label MATCH path goes
    // to `CatalogVertexScanExec` instead — a different leg to the same trait.
    // Provable from `crates/uni-query/tests` against a raw DataFusion
    // `SessionContext` (that root is already scanned), not from a query here.
    Operator {
        ty: "StorageScanExec",
        runtime_name: "StorageScanExec",
        status: Status::Unproven,
    },
];
