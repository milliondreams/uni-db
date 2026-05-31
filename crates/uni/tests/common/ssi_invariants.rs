// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Invariant-oracle stress: randomized concurrent workloads whose
//! serializability has a closed-form witness.
//!
//! Rather than a general (and easy-to-get-wrong) serializability checker, each
//! workload here has an invariant that holds under *every* serial order and is
//! violated by any non-serializable execution:
//!
//! - **Counter RMW**: blind increments retried on conflict → final value equals
//!   the number of committed increments (a lost update would lose one).
//! - **Bank transfer**: read-check-write transfers with an overdraft guard →
//!   the total is conserved and no balance goes negative (a write skew would
//!   break one or both).
//!
//! proptest generates the workload shape; a fresh tokio runtime drives the
//! genuine concurrency per case. See `ssi_support::oracle` for the witnesses.

use std::sync::Arc;

use anyhow::Result;
use proptest::prelude::*;
use uni_db::{DataType, RetryOptions, Uni, Value};

use crate::ssi_support::oracle;

// ── helpers ──────────────────────────────────────────────────────────────────

async fn counter_db(keys: &[&str]) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Counter")
        .property("id", DataType::String)
        .property("n", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    for k in keys {
        tx.execute(&format!("CREATE (:Counter {{id: '{k}', n: 0}})"))
            .await?;
    }
    tx.commit().await?;
    Ok(db)
}

async fn counter_of(db: &Uni, key: &str) -> Result<i64> {
    let r = db
        .session()
        .query(&format!(
            "MATCH (c:Counter {{id: '{key}'}}) RETURN c.n AS n"
        ))
        .await?;
    match r.rows()[0].value("n") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

/// `writers` tasks each apply `incs` retried increments to keys chosen round-robin
/// from `keys`. Returns the number of increments targeted at each key.
async fn run_counter_workload(
    db: Arc<Uni>,
    keys: Vec<String>,
    writers: usize,
    incs: usize,
) -> Vec<i64> {
    let mut expected = vec![0i64; keys.len()];
    let mut handles = Vec::new();
    for w in 0..writers {
        let db = db.clone();
        let keys = keys.clone();
        // Deterministically assign this writer's key so the oracle knows the
        // expected per-key total without races (no RNG in the hot path).
        let key_idx = w % keys.len();
        expected[key_idx] += incs as i64;
        let key = keys[key_idx].clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..incs {
                db.session()
                    .transact_with_retry(
                        RetryOptions {
                            max_attempts: 100,
                            ..Default::default()
                        },
                        |tx| {
                            let key = key.clone();
                            Box::pin(async move {
                                tx.execute(&format!(
                                    "MATCH (c:Counter {{id: '{key}'}}) SET c.n = c.n + 1"
                                ))
                                .await?;
                                Ok(())
                            })
                        },
                    )
                    .await
                    .expect("retry should eventually commit");
            }
        }));
    }
    for h in handles {
        h.await.expect("writer task panicked");
    }
    expected
}

// ── Counter convergence (lost-update oracle) ─────────────────────────────────

/// A fixed high-contention case: 8 writers hammering one key, 5 increments each.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn counter_convergence_single_hot_key() -> Result<()> {
    let db = Arc::new(counter_db(&["x"]).await?);
    let expected = run_counter_workload(db.clone(), vec!["x".into()], 8, 5).await;
    oracle::assert_counter(counter_of(&db, "x").await?, expected[0]);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Randomized counter workloads: every key converges to exactly the number
    /// of increments that targeted it, across many writer/inc/key shapes.
    #[test]
    fn prop_counter_convergence(
        writers in 2usize..8,
        incs in 1usize..4,
        n_keys in 1usize..4,
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let keys: Vec<String> = (0..n_keys).map(|i| format!("k{i}")).collect();
            let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            let db = Arc::new(counter_db(&key_refs).await.unwrap());
            let expected = run_counter_workload(db.clone(), keys.clone(), writers, incs).await;
            for (i, key) in keys.iter().enumerate() {
                oracle::assert_counter(counter_of(&db, key).await.unwrap(), expected[i]);
            }
        });
    }
}

// ── Bank transfer (write-skew / conservation oracle) ─────────────────────────

async fn bank_db(accounts: usize, seed: i64) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Acct")
        .property("id", DataType::String)
        .property("bal", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    for i in 0..accounts {
        tx.execute(&format!("CREATE (:Acct {{id: 'a{i}', bal: {seed}}})"))
            .await?;
    }
    tx.commit().await?;
    Ok(db)
}

async fn balances(db: &Uni, accounts: usize) -> Result<Vec<i64>> {
    let mut out = Vec::with_capacity(accounts);
    for i in 0..accounts {
        let r = db
            .session()
            .query(&format!("MATCH (a:Acct {{id: 'a{i}'}}) RETURN a.bal AS b"))
            .await?;
        match r.rows()[0].value("b") {
            Some(Value::Int(b)) => out.push(*b),
            other => panic!("expected Int, got {other:?}"),
        }
    }
    Ok(out)
}

/// `n` concurrent transfers, each moving `amount` from a random src to dst with a
/// read-check-write overdraft guard and bounded retry. Conservation + the
/// non-negative guard are the serializability witnesses.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn bank_transfers_conserve_and_stay_non_negative() -> Result<()> {
    const ACCOUNTS: usize = 4;
    const SEED: i64 = 100;
    const TRANSFERS: usize = 24;
    const AMOUNT: i64 = 30;

    let db = Arc::new(bank_db(ACCOUNTS, SEED).await?);
    let mut handles = Vec::new();
    for t in 0..TRANSFERS {
        let db = db.clone();
        // Fixed src/dst pattern (no RNG): pairs overlap enough to force conflicts.
        let src = format!("a{}", t % ACCOUNTS);
        let dst = format!("a{}", (t + 1) % ACCOUNTS);
        handles.push(tokio::spawn(async move {
            db.session()
                .transact_with_retry(
                    RetryOptions {
                        max_attempts: 100,
                        ..Default::default()
                    },
                    move |tx| {
                        let (src, dst) = (src.clone(), dst.clone());
                        Box::pin(async move {
                            // Read both balances (populates the read-set).
                            let r = tx
                                .query(&format!("MATCH (a:Acct {{id: '{src}'}}) RETURN a.bal AS b"))
                                .await?;
                            let bal = match r.rows()[0].value("b") {
                                Some(Value::Int(b)) => *b,
                                _ => 0,
                            };
                            // Overdraft guard: only move funds that exist.
                            if bal >= AMOUNT {
                                tx.execute(&format!(
                                    "MATCH (a:Acct {{id: '{src}'}}) SET a.bal = a.bal - {AMOUNT}"
                                ))
                                .await?;
                                tx.execute(&format!(
                                    "MATCH (b:Acct {{id: '{dst}'}}) SET b.bal = b.bal + {AMOUNT}"
                                ))
                                .await?;
                            }
                            Ok(())
                        })
                    },
                )
                .await
                .expect("transfer should eventually commit or no-op");
        }));
    }
    for h in handles {
        h.await.expect("transfer task panicked");
    }

    let bals = balances(&db, ACCOUNTS).await?;
    oracle::assert_conserved(&bals, SEED * ACCOUNTS as i64);
    oracle::assert_non_negative(&bals);
    Ok(())
}
