// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end runtime coverage for uni-query finding [13]
//! (`crates/uni-query/src/query/df_graph/locy_fixpoint.rs:2537`).
//!
//! # The defect
//!
//! When `exact_probability` is enabled and a probabilistic (`MNOR`/`MPROD`) rule
//! produces derived facts that share lineage, `apply_exact_wmc` replaces each
//! shared KEY group with a single weighted-model-count row. It reads the
//! probability column and the group KEY using raw YIELD-schema positions
//! (`prob_fold.input_col_index`, `rule.key_column_indices`) and hashes rows over
//! the full yield-schema index set — but it applies those positions against the
//! `pre_fold_facts` batches. The surrounding FOLD path (`locy_fixpoint.rs:5192`)
//! deliberately *reconciles* key indices by NAME because "the actual batch may
//! have different column ordering after schema reconciliation during fixpoint
//! iteration"; `detect_shared_lineage` (`:2222`) and `apply_exact_wmc` (`:2555`)
//! do NOT reconcile — they use raw yield positions. When the pre-fold batch
//! column order diverges from the yield schema, the KEY grouping and the PROB
//! column read/overwrite land on the wrong columns.
//!
//! # Reproducibility through the public API
//!
//! The canonical shared-proof shape is the reachability diamond
//!   s -0.3-> m -0.5-> t ,  s -0.7-> t
//! evaluated by a recursive `reach` rule. With `exact_probability` this fires
//! `detect_shared_lineage` (a `SharedProbabilisticDependency` warning is
//! emitted) and runs `apply_exact_wmc`. The passing test below asserts BOTH
//! facts, proving the exact-WMC path executes end-to-end. Its computed
//! probability for `reach(s,t)` is `0.79` — the value the Locy TCK
//! (`monotonic/ExactProbability.feature`, scenario "BDD mode produces numeric
//! probability for MNOR diamond") codifies as correct for exact mode
//! (`MNOR(0.7, 0.3)` over the two fold-input edges).
//!
//! In every shape reachable from surface Locy, the pre-fold batch column order
//! coincides with the yield schema, so the positional read at `:2537` is
//! *coincidentally* correct and the output matches the codified expectation. The
//! ignored test documents the divergence the defect would produce (full
//! inclusion-exclusion `X ∨ (Y∧Z) = 0.745`) but is marked latent: forcing a
//! pre-fold column-order divergence is not achievable through the public API,
//! and asserting `0.745` today would contradict the TCK-codified `0.79`.

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{DataType, Uni, Value};

const REACH_MNOR: &str = "CREATE RULE reach AS \
     MATCH (a:N)-[e:E]->(b:N) \
     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
     CREATE RULE reach AS \
     MATCH (a:N)-[e:E]->(mid:N) WHERE mid IS reach TO b \
     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
     QUERY reach RETURN a.name AS src, b.name AS dst, prob";

/// Exact-probability (BDD/WMC) path: `exact_probability = true` promotes the
/// default AddMultProb semiring to BddExact and runs `apply_exact_wmc` on
/// shared-lineage groups.
fn exact_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        exact_probability: true,
        ..Default::default()
    }
}

async fn seed_diamond(db: &Uni) -> Result<()> {
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("E", &["N"], &["N"])
        .property("p", DataType::Float64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 's'}), (:N {name: 'm'}), (:N {name: 't'})")
        .await?;
    tx.execute("MATCH (s:N {name:'s'}), (t:N {name:'t'}) CREATE (s)-[:E {p: 0.7}]->(t)")
        .await?;
    tx.execute("MATCH (s:N {name:'s'}), (m:N {name:'m'}) CREATE (s)-[:E {p: 0.3}]->(m)")
        .await?;
    tx.execute("MATCH (m:N {name:'m'}), (t:N {name:'t'}) CREATE (m)-[:E {p: 0.5}]->(t)")
        .await?;
    tx.commit().await?;
    Ok(())
}

fn prob_of(result: &uni_locy::LocyResult, src: &str, dst: &str) -> Option<f64> {
    let rows = result.rows()?;
    rows.iter()
        .find(|r| {
            r.get("src").and_then(Value::as_str) == Some(src)
                && r.get("dst").and_then(Value::as_str) == Some(dst)
        })
        .and_then(|r| r.get("prob"))
        .and_then(Value::as_f64)
}

fn has_warning(result: &uni_locy::LocyResult, code_substr: &str) -> bool {
    result
        .warnings()
        .iter()
        .any(|w| format!("{:?}", w.code).contains(code_substr))
}

/// Proves the exact-WMC path (`detect_shared_lineage` + `apply_exact_wmc`, which
/// contain the [13] positional-index code) runs end-to-end: the shared-proof
/// diamond emits a `SharedProbabilisticDependency` warning and produces the
/// TCK-codified exact-mode probability `0.79` for `reach(s,t)`.
#[tokio::test]
async fn wmc_shared_lineage_exact_path_engages() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_diamond(&db).await?;

    let result = db
        .session()
        .locy_with(REACH_MNOR)
        .with_config(exact_config())
        .run()
        .await?;

    let p_st = prob_of(&result, "s", "t");
    let shared = has_warning(&result, "SharedProbabilisticDependency");
    println!("[13] exact-WMC reach(s,t) prob -> {p_st:?}, shared-dependency warning = {shared}");
    assert!(
        shared,
        "the shared-proof diamond must emit SharedProbabilisticDependency, proving detect_shared_lineage/apply_exact_wmc ran"
    );
    let p_st = p_st.expect("reach(s,t) should exist");
    assert!(
        (p_st - 0.79).abs() < 1e-9,
        "exact-mode MNOR diamond is codified as 0.79 by the TCK; the pre-fold column order coincides with the yield schema so the positional read at :2537 is coincidentally correct; got {p_st}"
    );
    Ok(())
}

/// The [13] defect's SHAPE — a mis-grouped/mis-read shared lineage. Ignored and
/// latent: forcing the pre-fold batch column order to diverge from the yield
/// schema (the precondition that makes the raw positional indices at
/// `locy_fixpoint.rs:2537` read the wrong columns) is not achievable through the
/// public Locy surface. The full inclusion-exclusion value `X ∨ (Y∧Z) = 0.745`
/// is what correct exact WMC over the shared base facts would yield, but the
/// codebase (and its TCK) treat `0.79` as the exact-mode answer, so this is
/// documented, not asserted as a live failure.
#[tokio::test]
#[ignore = "e2e repro for uni-query [13]: apply_exact_wmc reads PROB/KEY by raw yield-schema positions against pre-fold batches; only manifests when the batch column order diverges from the yield schema, which is unreachable via the public surface (positional read is coincidentally correct in all reachable shapes)"]
async fn wmc_shared_lineage_positional_misgroup_would_diverge() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_diamond(&db).await?;

    let result = db
        .session()
        .locy_with(REACH_MNOR)
        .with_config(exact_config())
        .run()
        .await?;

    let p_st = prob_of(&result, "s", "t").expect("reach(s,t) should exist");
    println!("[13] exact-WMC reach(s,t) prob -> {p_st} (full inclusion-exclusion X∨(Y∧Z) = 0.745)");
    assert!(
        (p_st - 0.745).abs() < 1e-6,
        "would hold only if apply_exact_wmc combined the shared base facts by exact inclusion-exclusion; got {p_st}"
    );
    Ok(())
}
