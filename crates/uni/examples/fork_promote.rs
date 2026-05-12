// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Write-audit-publish walkthrough — spec §3.3.
//!
//! Models a typical "stage changes on a fork, audit them via diff,
//! then publish to primary" workflow:
//! 1. Seed primary with a stable baseline.
//! 2. Open a fork and stage new vertices on it.
//! 3. Audit: call `Uni::diff_fork_primary` and print what would land.
//! 4. Publish: `Uni::promote_from_fork` to copy the audited rows
//!    onto primary inside a single transaction.
//! 5. Drop the fork — primary has the new state, fork is gone.
//!
//! Run with: `cargo run --example fork_promote`

use uni_db::api::fork_diff::PromotePattern;
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

    // === 1. seed primary ===============================================
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;
    db.flush().await?;

    println!("=== primary baseline ===");
    print_people(&primary).await?;

    // === 2. stage on a fork ============================================
    {
        let staging = primary.fork("publish_2026Q2").await?;
        let tx = staging.tx().await?;
        tx.execute("CREATE (:Person {name: 'Carol'})").await?;
        tx.execute("CREATE (:Person {name: 'Dave'})").await?;
        tx.commit().await?;

        println!("\n=== staging fork view ===");
        print_people(&staging).await?;
    }

    // === 3. audit via diff =============================================
    let diff = db.diff_fork_primary("publish_2026Q2").await?;
    println!(
        "\n=== audit ===\nstaged adds: {}\nstaged deletes: {}\nstaged property changes: {}",
        diff.vertices.added.len(),
        diff.vertices.deleted.len(),
        diff.vertices.changed.len()
    );
    for v in &diff.vertices.added {
        println!("  + ({}:{}) {:?}", v.label, v.vid, v.properties);
    }

    // === 4. publish ====================================================
    let report = db
        .promote_from_fork(
            "publish_2026Q2",
            &[PromotePattern::label("Person")],
        )
        .await?;
    println!(
        "\n=== publish ===\ninserted: {}\nskipped (UID conflict): {}\nedges skipped (Phase 6): {}",
        report.vertices_inserted,
        report.vertices_skipped_uid_conflict,
        report.edges_skipped,
    );

    // === 5. drop staging fork ==========================================
    db.drop_fork("publish_2026Q2").await?;
    println!("\n=== primary post-publish ===");
    print_people(&primary).await?;

    db.shutdown().await?;
    Ok(())
}

async fn print_people(session: &uni_db::Session) -> Result<(), Box<dyn std::error::Error>> {
    let rows = session
        .query("MATCH (p:Person) RETURN p.name AS name")
        .await?;
    for row in rows.rows() {
        if let Ok(name) = row.get::<String>("name") {
            println!("  {}", name);
        }
    }
    Ok(())
}
