// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `LocyProgramExec` emission proof (#177).
//!
//! # Why this lives here and not beside the Locy suites
//!
//! `LocyProgramExec` reaches neither profile surface. `impl_locy.rs` executes it
//! directly rather than through the Cypher pipeline, so no `ProfileOutput` is
//! produced; and `LocyProfileOutput` carries only the per-rule *clause-body*
//! plans from inside it, never the operator wrapping them. No query-level
//! assertion can see it without a collector change.
//!
//! What can be asserted today is that the physical planner emits it for the
//! logical node that represents a Locy program. That is one step weaker than
//! the gate's usual "appears in an executed plan" — it proves emission, not
//! execution — and the registry row says so rather than claiming more.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use parking_lot::RwLock;
use tempfile::tempdir;
use uni_common::core::schema::SchemaManager;
use uni_query::query::df_planner::HybridPhysicalPlanner;
use uni_query::query::planner::LogicalPlan;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

/// A physical planner over an empty store — enough to lower a logical node.
async fn planner(path: &std::path::Path) -> (HybridPhysicalPlanner, u16) {
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();
    let label_id = schema_manager.add_label("N").unwrap();
    schema_manager.save().await.unwrap();
    let schema = schema_manager.schema();
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await
        .unwrap(),
    );
    let l0 = Arc::new(RwLock::new(L0Buffer::new(0, None)));
    let property_manager = Arc::new(PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        0,
    ));
    (
        HybridPhysicalPlanner::new(
            Arc::new(RwLock::new(SessionContext::new())),
            storage,
            l0,
            property_manager,
            schema,
            HashMap::new(),
        ),
        label_id,
    )
}

/// A `LocyProgram` logical node lowers to `LocyProgramExec`.
///
/// Strata and commands are empty on purpose: what is under test is the
/// planner's dispatch on the node *variant*, and an empty program exercises it
/// with nothing else in the way.
#[tokio::test]
async fn a_locy_program_node_plans_the_locy_program_operator() {
    let dir = tempdir().unwrap();
    let (planner, _label_id) = planner(dir.path()).await;

    let logical = LogicalPlan::LocyProgram {
        strata: Vec::new(),
        commands: Vec::new(),
        derived_scan_registry: Arc::new(Default::default()),
        max_iterations: 10,
        timeout: std::time::Duration::from_secs(5),
        max_derived_bytes: 1 << 20,
        deterministic_best_by: true,
        strict_probability_domain: false,
        probability_epsilon: 1e-9,
        exact_probability: false,
        max_bdd_variables: 32,
        top_k_proofs: 0,
        semiring_kind: uni_locy::SemiringKind::default(),
        classifier_registry: Arc::new(Default::default()),
        classifier_cache: None,
        classifier_provenance_store: None,
    };

    let physical = planner.plan(&logical).expect("physical plan");
    let names = vec![physical.name().to_string()];
    uni_query::plan_shape::assert_uses(&names, "LocyProgramExec", "LogicalPlan::LocyProgram");
}

/// The negative twin: an ordinary scan node does not lower to it.
#[tokio::test]
async fn a_scan_node_avoids_the_locy_program_operator() {
    let dir = tempdir().unwrap();
    let (planner, label_id) = planner(dir.path()).await;

    let logical = LogicalPlan::Scan {
        label_id,
        labels: vec!["N".to_string()],
        variable: "n".to_string(),
        filter: None,
        optional: false,
    };
    let physical = planner.plan(&logical).expect("physical plan");
    let names = vec![physical.name().to_string()];
    uni_query::plan_shape::assert_avoids(&names, "LocyProgramExec", "LogicalPlan::Scan");
}
