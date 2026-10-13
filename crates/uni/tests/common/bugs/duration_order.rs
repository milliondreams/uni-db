// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Durations sort by length, `min` / `max` follow `ORDER BY`, and `<` between
//! two durations is null.
//!
//! Neo4j orders durations in `ORDER BY` (and so `min` / `max`) by total
//! length, a month being 30.436875 days and a day 24 hours, and leaves `<`
//! between two undefined (null). uni-db sorted months, days and nanoseconds in
//! turn (`PT48H` before `P1D`); `min` / `max` ranked every pair of temporal
//! values equal, so they answered whichever value came first, for durations
//! and for dates alike; and two `duration(...)` literals compared field by
//! field.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(duration_order)'

// Rust guideline compliant

use anyhow::{Result, bail};
use uni_db::{DataType, Uni, Value};

/// Ascending by length: 1 hour, 1 day, 36 hours, 1 month, 40 days.
const BY_LENGTH: [&str; 5] = ["PT1H", "P1D", "PT36H", "P1M", "P40D"];

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("T")
        .property("d", DataType::Duration)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    for d in BY_LENGTH.iter().rev() {
        tx.execute(&format!(
            "CREATE (:T {{d: duration('{d}')}}), (:U {{d: duration('{d}')}})"
        ))
        .await?;
    }
    tx.execute(
        "CREATE (:W {t: date('2024-03-01')}), (:W {t: date('2024-01-01')}), (:W {t: date('2024-02-01')})",
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

/// `duration('<iso>')` as the engine returns it.
async fn durations(db: &Uni, isos: &[&str]) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    for iso in isos {
        out.extend(column(db, &format!("RETURN duration('{iso}')")).await?);
    }
    Ok(out)
}

#[tokio::test]
async fn duration_order_is_by_length() -> Result<()> {
    let db = open().await?;
    let ascending = durations(&db, &BY_LENGTH).await?;
    let shortest = vec![ascending[0].clone()];
    let longest = vec![ascending[4].clone()];
    let list = |order: &[&str]| {
        order
            .iter()
            .map(|d| format!("duration('{d}')"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let forward = list(&BY_LENGTH);
    let mut reversed = BY_LENGTH;
    reversed.reverse();
    let backward = list(&reversed);
    let cases: Vec<(String, Vec<Value>)> = vec![
        (
            format!("UNWIND [{backward}] AS d RETURN d ORDER BY d"),
            ascending.clone(),
        ),
        (
            "MATCH (t:T) RETURN t.d ORDER BY t.d".into(),
            ascending.clone(),
        ),
        ("MATCH (u:U) RETURN u.d ORDER BY u.d".into(), ascending),
        (
            format!("UNWIND [{forward}] AS d RETURN min(d)"),
            shortest.clone(),
        ),
        (
            format!("UNWIND [{backward}] AS d RETURN min(d)"),
            shortest.clone(),
        ),
        (
            format!("UNWIND [{forward}] AS d RETURN max(d)"),
            longest.clone(),
        ),
        (
            format!("UNWIND [{backward}] AS d RETURN max(d)"),
            longest.clone(),
        ),
        ("MATCH (t:T) RETURN min(t.d)".into(), shortest.clone()),
        ("MATCH (u:U) RETURN min(u.d)".into(), shortest),
        ("MATCH (t:T) RETURN max(t.d)".into(), longest.clone()),
        ("MATCH (u:U) RETURN max(u.d)".into(), longest),
        (
            "RETURN duration('P1D') < duration('P2D')".into(),
            vec![Value::Null],
        ),
        (
            "RETURN duration('P2D') >= duration('P1D')".into(),
            vec![Value::Null],
        ),
        (
            "MATCH (t:T) WHERE t.d = duration('P1D') RETURN t.d < duration('P2D')".into(),
            vec![Value::Null],
        ),
        (
            "RETURN duration('P1D') = duration('P1D')".into(),
            vec![Value::Bool(true)],
        ),
    ];
    let mut failures = Vec::new();
    for (query, want) in cases {
        match column(&db, &query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}: expected {want:?}, got {got:?}")),
            Err(e) => failures.push(format!("{query}: expected {want:?}, got error {e}")),
        }
    }
    // Dates were tied too: `min` / `max` answered the first one stored.
    let first = column(&db, "MATCH (w:W) RETURN w.t ORDER BY w.t").await?;
    for (query, want) in [
        ("MATCH (w:W) RETURN min(w.t)", first.first()),
        ("MATCH (w:W) RETURN max(w.t)", first.last()),
    ] {
        let want: Vec<Value> = want.cloned().into_iter().collect();
        match column(&db, query).await {
            Ok(got) if got == want => {}
            other => failures.push(format!("{query}: expected {want:?}, got {other:?}")),
        }
    }
    if !failures.is_empty() {
        bail!("{}", failures.join("\n"));
    }
    Ok(())
}
