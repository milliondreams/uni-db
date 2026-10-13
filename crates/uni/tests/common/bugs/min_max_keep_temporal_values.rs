// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `min` / `max` over a declared DateTime or Time property answer the earliest
//! or latest instant, as the value it is: offset and zone included.
//!
//! The planner reduced such a struct to its nanoseconds, and the Cypher
//! `min` / `max` aggregate promised its input's type (`Timestamp(ns)`) while
//! producing a Cypher value, so every such query failed with "column types
//! must match schema types, expected Timestamp(ns) but found LargeBinary".
//! Reduced to nanoseconds, the answer would also have lost the offset.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(min_max_keep_temporal_values)'

// Rust guideline compliant

use anyhow::{Result, bail};
use uni_db::{DataType, Uni, Value};

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("g", DataType::String)
        .property_nullable("t", DataType::DateTime)
        .property_nullable("h", DataType::Time)
        .done()
        .edge_type("E", &["N"], &["N"])
        .property_nullable("t", DataType::DateTime)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    // Instants: Jan 1 00:00Z < Jan 15 05:00Z < Jan 31 22:00Z.
    tx.execute(
        "CREATE (a:N {g: 'x', t: datetime('2024-01-15T00:00:00-05:00'), h: time('07:30:00-01:00')}), \
                (b:N {g: 'x', t: datetime('2024-02-01T00:00:00+02:00'), h: time('09:00:00Z')}), \
                (c:N {g: 'x', t: datetime('2024-01-01T00:00:00Z'), h: time('10:00:00+02:00')}), \
                (a)-[:E {t: datetime('2024-02-01T00:00:00+02:00')}]->(b), \
                (a)-[:E {t: datetime('2024-01-01T00:00:00Z')}]->(c)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn column(db: &Uni, query: &str) -> Result<Vec<Value>> {
    let result = db.session().query(query).await?;
    Ok(result
        .rows()
        .iter()
        .map(|r| r.values()[0].clone())
        .collect())
}

#[tokio::test]
async fn min_max_keep_temporal_values() -> Result<()> {
    let db = open().await?;
    let earliest = column(&db, "RETURN datetime('2024-01-01T00:00:00Z')").await?;
    let latest = column(&db, "RETURN datetime('2024-02-01T00:00:00+02:00')").await?;
    // 10:00+02:00 is 08:00Z, before 07:30-01:00 (08:30Z) and 09:00Z.
    let first_time = column(&db, "RETURN time('10:00:00+02:00')").await?;
    let last_time = column(&db, "RETURN time('09:00:00Z')").await?;
    let cases = [
        ("MATCH (n:N) RETURN min(n.t)", &earliest),
        ("MATCH (n:N) RETURN max(n.t)", &latest),
        ("MATCH (n:N) RETURN n.g, min(n.t)", &earliest),
        ("MATCH (n:N) WITH n.g AS g, max(n.t) AS m RETURN m", &latest),
        ("MATCH (:N)-[e:E]->(:N) RETURN min(e.t)", &earliest),
        ("MATCH (:N)-[e:E]->(:N) RETURN max(e.t)", &latest),
        ("MATCH (n:N) RETURN min(n.h)", &first_time),
        ("MATCH (n:N) RETURN max(n.h)", &last_time),
    ];
    let mut failures = Vec::new();
    for (query, want) in cases {
        let got = db.session().query(query).await.map(|r| {
            r.rows()
                .iter()
                .map(|row| row.values().last().cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        });
        match got {
            Ok(got) if &got == want => {}
            Ok(got) => failures.push(format!("{query}: expected {want:?}, got {got:?}")),
            Err(e) => failures.push(format!("{query}: expected {want:?}, got error {e}")),
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}
