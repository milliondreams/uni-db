// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Nested forks walkthrough — spec §3.5.
//!
//! Demonstrates the Phase 3 nested-fork API: forking a fork, snapshot
//! isolation at each level, and `drop_fork_cascade` for tearing down
//! the whole subtree.
//!
//! Run with: `cargo run --example fork_nested`

use uni_db::{DataType, Uni};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    let primary = db.session();

    // === primary seed ==================================================
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'Primary-Alice'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    println!("=== primary view ===");
    print_names(&primary, "primary").await?;

    // === level 1: fork off primary =====================================
    let a = primary.fork("scenario_a").await?;
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {name: 'A-Bob'})").await?;
    tx.commit().await?;

    println!("\n=== scenario_a view (child of primary) ===");
    print_names(&a, "a").await?;

    // === level 2: fork off a ===========================================
    // The parent is inferred from the receiver session — calling
    // `a.fork(...)` makes scenario_a the parent of scenario_b.
    // create_fork_2pc auto-flushes A's L0 first so B sees A's writes
    // through Lance.
    let b = a.fork("scenario_b").await?;
    let tx = b.tx().await?;
    tx.execute("CREATE (:Person {name: 'B-Carol'})").await?;
    tx.commit().await?;

    println!("\n=== scenario_b view (child of scenario_a) ===");
    print_names(&b, "b").await?;

    // === snapshot isolation demonstration ==============================
    // Writes on A *after* B is created stay invisible to B.
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {name: 'A-After-B'})").await?;
    tx.commit().await?;

    println!("\n=== after a post-B write on a ===");
    print_names(&a, "a").await?;
    print_names(&b, "b (must NOT see A-After-B)").await?;

    // === cascade drop ==================================================
    // `drop_fork("scenario_a")` would error with ForkHasChildren because
    // scenario_b is a descendant. The cascade variant drops the whole
    // subtree, pre-validating every node for live sessions / in-flight
    // transactions before tombstoning anything.
    drop(a);
    drop(b);

    db.drop_fork_cascade("scenario_a").await?;

    let remaining: Vec<String> = db.list_forks().await.into_iter().map(|i| i.name).collect();
    println!("\n=== after cascade drop ===");
    println!("forks remaining: {remaining:?}");

    db.shutdown().await?;
    Ok(())
}

async fn print_names(
    session: &uni_db::Session,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = session.query("MATCH (p:Person) RETURN p.name").await?;
    let names: Vec<String> = result
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("p.name").ok())
        .collect();
    println!("  {label}: {} row(s) — {names:?}", names.len());
    Ok(())
}
