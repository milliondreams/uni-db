#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/rule_registry.rs:129 (finding [7]).
//!
//! `RuleRegistry::remove` and its sibling `register_rules_on_registry`
//! (impl_locy.rs) both perform a non-atomic read-modify-write: snapshot
//! `sources` under a READ lock, drop the guard, rebuild a fresh
//! `LocyRuleRegistry` OUTSIDE any lock, then `*self.registry.write() = rebuilt`
//! — clobbering the WHOLE value. Any registration committed by another task
//! between the read-guard drop and the write is silently lost (a classic
//! lost-update / TOCTOU on the shared `Arc<RwLock<..>>`).
//!
//! Demonstration: fire N concurrent `register()` calls of DISTINCT single-rule
//! programs at the same db-level registry. With no lost update the final rule
//! count is N; the read-snapshot-rebuild-clobber loses some, so the count
//! comes out < N.
//!
//! Timing-dependent: `#[ignore]`d so CI stays green. Run explicitly with
//! `--run-ignored=all` to observe.

use std::sync::Arc;
use std::time::Duration;

use uni_db::{DataType, Uni};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_register_loses_rules() -> anyhow::Result<()> {
    const CONCURRENCY: usize = 16;
    const ROUNDS: usize = 30;

    let reproduced_at: Option<(usize, usize)> = None;

    for round in 0..ROUNDS {
        let db = Arc::new(Uni::in_memory().build().await?);
        db.schema()
            .label("Node")
            .property("name", DataType::String)
            .edge_type("EDGE", &["Node"], &["Node"])
            .done()
            .apply()
            .await?;

        let mut handles = Vec::with_capacity(CONCURRENCY);
        for i in 0..CONCURRENCY {
            let db = Arc::clone(&db);
            handles.push(tokio::spawn(async move {
                let program = format!(
                    "CREATE RULE r{i} AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b"
                );
                db.rules().register(&program).await
            }));
        }
        for h in handles {
            h.await.expect("task join").expect("register ok");
        }

        // FIXED (impl_locy.rs / rule_registry.rs): register/remove hold the write
        // lock across read→rebuild→assign, so concurrent registrations serialize
        // and every round ends with all CONCURRENCY rules — no lost update.
        let count = db.rules().count();
        assert_eq!(
            count, CONCURRENCY,
            "lost update at round {round}: final rule count {count} < {CONCURRENCY}"
        );
    }
    let _ = reproduced_at;

    Ok(())
}
