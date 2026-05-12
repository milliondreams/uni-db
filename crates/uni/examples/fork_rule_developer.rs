// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Rule-developer iterative loop — spec §3.1.
//!
//! Models a typical workflow:
//!   1. Seed primary with a small social graph (Person + KNOWS).
//!   2. Fork "hypothesis_a" and run a candidate rule on the fork:
//!      "derive a FRIEND_OF_FRIEND edge whenever two people share
//!      a common acquaintance". The rule is expressed as a Cypher
//!      mutation; in production it could equally be a Locy rule.
//!   3. Audit the derivation via `db.diff_fork_primary(...)` —
//!      print exactly what the rule produced.
//!   4. Decide: promote the new edges to primary, or drop the fork
//!      and try a different rule.
//!   5. Demonstrate the *drop* path with a second hypothesis,
//!      then the *promote* path with the first.
//!
//! Run with: `cargo run --example fork_rule_developer`

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
    db.schema()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    db.schema()
        .edge_type("FRIEND_OF_FRIEND", &["Person"], &["Person"])
        .apply()
        .await?;

    let primary = db.session();

    // === 1. Seed primary ===============================================
    // alice knows bob; bob knows carol; carol knows dave.
    let tx = primary.tx().await?;
    tx.execute(
        "CREATE (alice:Person {name: 'alice'}),\
                (bob:Person {name: 'bob'}),\
                (carol:Person {name: 'carol'}),\
                (dave:Person {name: 'dave'}),\
                (alice)-[:KNOWS]->(bob),\
                (bob)-[:KNOWS]->(carol),\
                (carol)-[:KNOWS]->(dave)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    println!("=== primary seed ===");
    print_graph(&primary, "primary").await?;

    // === 2. Hypothesis A — derive 2-hop FRIEND_OF_FRIEND ===============
    println!("\n=== hypothesis A: friend-of-friend = 2-hop KNOWS ===");
    {
        let fork = primary.fork("hypothesis_a").await?;
        let tx = fork.tx().await?;
        // "Candidate rule": for every (a)-[:KNOWS]->(b)-[:KNOWS]->(c)
        // where a != c, create an (a)-[:FRIEND_OF_FRIEND]->(c) edge.
        tx.execute(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE id(a) <> id(c) \
             CREATE (a)-[:FRIEND_OF_FRIEND]->(c)",
        )
        .await?;
        tx.commit().await?;
        fork.flush().await?;
    }

    // === 3. Audit ======================================================
    let diff_a = db.diff_fork_primary("hypothesis_a").await?;
    println!(
        "  diff: +{} vertices, +{} edges, ~{} changed",
        diff_a.vertices.added.len(),
        diff_a.edges.added.len(),
        diff_a.vertices.changed.len()
    );
    for e in &diff_a.edges.added {
        println!(
            "  + ({})-[:{}]->({})",
            short_uid(&e.src_uid),
            e.edge_type,
            short_uid(&e.dst_uid)
        );
    }

    // === 4. Hypothesis B — also derive a "lonely" label on hubs =======
    // This second hypothesis would conflict with our intent (it adds a
    // label we don't actually want), so we'll *drop* it after auditing.
    println!("\n=== hypothesis B: tag Person hubs (no thanks) ===");
    {
        let fork = primary.fork("hypothesis_b").await?;
        let tx = fork.tx().await?;
        tx.execute(
            "MATCH (n:Person {name: 'bob'}) \
             SET n.is_hub = true",
        )
        .await?;
        tx.commit().await?;
        fork.flush().await?;
    }

    let diff_b = db.diff_fork_primary("hypothesis_b").await?;
    println!(
        "  diff: +{} vertices, +{} edges, ~{} changed",
        diff_b.vertices.added.len(),
        diff_b.edges.added.len(),
        diff_b.vertices.changed.len()
    );
    // Drop hypothesis B — we don't like the property addition.
    db.drop_fork("hypothesis_b").await?;
    println!("  decision: drop (kept primary clean)");

    // === 5. Promote hypothesis A =======================================
    println!("\n=== promote hypothesis A ===");
    let report = db
        .promote_from_fork(
            "hypothesis_a",
            &[PromotePattern::edge_type("FRIEND_OF_FRIEND")],
        )
        .await?;
    println!(
        "  inserted: {} vertices, {} edges; skipped (UID conflict): {}, (no endpoint): {}",
        report.vertices_inserted,
        report.edges_inserted,
        report.vertices_skipped_uid_conflict,
        report.edges_skipped_no_endpoint
    );
    db.drop_fork("hypothesis_a").await?;

    // === 6. Final state on primary =====================================
    println!("\n=== primary after publish ===");
    print_graph(&primary, "primary").await?;

    db.shutdown().await?;
    Ok(())
}

async fn print_graph(
    session: &uni_db::Session,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let edges = session
        .query(
            "MATCH (a:Person)-[r]->(b:Person) \
             RETURN a.name AS src, type(r) AS rel, b.name AS dst",
        )
        .await?;
    let mut rows: Vec<String> = edges
        .rows()
        .iter()
        .filter_map(|r| {
            let s = r.get::<String>("src").ok()?;
            let t = r.get::<String>("rel").ok()?;
            let d = r.get::<String>("dst").ok()?;
            Some(format!("  ({s})-[:{t}]->({d})"))
        })
        .collect();
    rows.sort();
    println!("[{label}]");
    for line in rows {
        println!("{line}");
    }
    Ok(())
}

fn short_uid(uid: &uni_db::UniId) -> String {
    let full = uid.to_string();
    // base32-multibase representations are long; show just the suffix
    // so the diff readout is visually scannable.
    let tail: String = full.chars().rev().take(8).collect::<String>();
    let tail: String = tail.chars().rev().collect();
    format!("uid…{tail}")
}
