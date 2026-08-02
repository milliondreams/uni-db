// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for the monotonicity oracle never reaching the plugin registry
//! (Tier 1.5, phase 1).
//!
//! `check_non_monotonic_in_recursion` rejects any FOLD aggregate whose oracle
//! answer is not `Some(true)`. Every host compile path passed
//! `default_monotonicity_oracle`, which recognises exactly six hardcoded names
//! (`MMAX`, `MMIN`, `MCOUNT`, `MNOR`, `MPROD`, `MSUM`) and answers `None` for
//! everything else — including aggregates the plugin registry knows perfectly
//! well are monotone.
//!
//! The registry-backed oracle and its injection point both existed; neither was
//! on any host path. So an aggregate declared `monotone_join: true` was rejected
//! at compile time with a message asserting it is non-monotonic, while the
//! planner's own guard — which *does* consult the registry — would have passed
//! it. Compile and plan disagreed about the same program.
//!
//! `MAX` is the cheapest witness: the builtin registry gives it
//! `Semilattice::BOUNDED_MIN_MAX` (`monotone_join: true`), and the default
//! oracle answers `None` for it. No plugin registration is needed to
//! distinguish the two oracles.

use uni_db::Uni;

/// The recursive rule below folds with `MAX`, which the registry reports as a
/// monotone join and the six-name default oracle does not know at all.
const RECURSIVE_MAX: &str = "\
CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b, a.cost AS peak \
CREATE RULE reach AS MATCH (a:N)-[:E]->(mid:N) WHERE mid IS reach TO b \
FOLD peak = MAX(a.cost) YIELD KEY a, KEY b, peak";

async fn db() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema().label("N").apply().await.unwrap();
    db.schema()
        .edge_type("E", &["N"], &["N"])
        .apply()
        .await
        .unwrap();
    db
}

/// Compiling must consult the registry, not the six-name default.
#[tokio::test]
async fn recursive_fold_over_a_registry_monotone_aggregate_compiles() {
    let db = db().await;
    let result = db.session().locy_with(RECURSIVE_MAX).explain();

    assert!(
        result.is_ok(),
        "a recursive FOLD over a registry-monotone aggregate must compile; \
         before the oracle reached the registry this failed with \
         NonMonotonicInRecursion: {:?}",
        result.err()
    );
}

/// Inverse guard: a genuinely non-monotone aggregate is still refused.
///
/// Without this, an oracle that simply answered `Some(true)` for everything
/// would satisfy the test above.
#[tokio::test]
async fn recursive_fold_over_a_non_monotone_aggregate_is_still_rejected() {
    let db = db().await;
    let program = RECURSIVE_MAX.replace("MAX(a.cost)", "AVG(a.cost)");

    let err = db
        .session()
        .locy_with(&program)
        .explain()
        .expect_err("AVG is monotone_join: false and must stay rejected in recursion");
    let msg = err.to_string();
    assert!(
        msg.contains("non-monotonic") || msg.contains("NonMonotonicInRecursion"),
        "expected a monotonicity rejection, got: {msg}"
    );
}

/// The built-in `M*` contract must survive the swap.
///
/// The composed oracle falls back to `default_monotonicity_oracle` on a
/// registry miss, so `MSUM` keeps working even where the registry has no entry.
#[tokio::test]
async fn builtin_m_prefixed_folds_still_compile_in_recursion() {
    let db = db().await;
    let program = RECURSIVE_MAX.replace("MAX(a.cost)", "MSUM(a.cost)");

    assert!(
        db.session().locy_with(&program).explain().is_ok(),
        "MSUM must remain legal in a recursive stratum"
    );
}

// ── Phase 2: registration and persisted reload ──────────────────────────

/// `rules().register()` compiled against a builtin-only registry, so a program
/// legal through `session.locy()` was refused at registration.
///
/// The blocker was ordering, not plumbing: `Uni::build` loaded and recompiled
/// the persisted rules before it constructed the plugin registry, so there was
/// nothing to compile against. The registry construction is now hoisted above
/// that load.
#[tokio::test]
async fn a_registry_monotone_fold_can_be_registered() {
    let db = db().await;

    db.rules()
        .register(RECURSIVE_MAX)
        .await
        .expect("registering a recursive FOLD over a registry-monotone aggregate must succeed");

    let names = db.rules().list();
    assert!(
        names.iter().any(|n| n == "reach"),
        "the rule should be registered; got {names:?}"
    );
}

/// The reload half: a registered rule must still compile when the database is
/// reopened, which is the path `build_locy_registry_from_persisted` drives.
#[tokio::test]
async fn a_registered_rule_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path).build().await.unwrap();
        db.schema().label("N").apply().await.unwrap();
        db.schema()
            .edge_type("E", &["N"], &["N"])
            .apply()
            .await
            .unwrap();
        db.rules().register(RECURSIVE_MAX).await.unwrap();
        db.shutdown().await.unwrap();
    }

    // Reopening recompiles every persisted source. Before the registry was
    // hoisted this ran against a builtin-only registry and the open failed.
    let db = Uni::open(&path)
        .build()
        .await
        .expect("reopening must recompile the persisted rule against the plugin registry");
    let names = db.rules().list();
    assert!(
        names.iter().any(|n| n == "reach"),
        "the persisted rule should reload; got {names:?}"
    );
}
