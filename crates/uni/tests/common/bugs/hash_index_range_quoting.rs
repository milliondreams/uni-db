// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: a two-sided inclusive range on a Hash-indexed column returned
//! **zero rows**.
//!
//! `LanceFilterGenerator` used to fuse `p >= L AND p <= U` into the single
//! string `"p" >= L AND "p" <= U`. Lance's filter dialect parses a
//! double-quoted name as a **string literal**, not a quoted identifier, so that
//! clause was really `'p' >= L AND 'p' <= U` — a constant with no reference to
//! the data at all. For this operator pair it evaluates false for every row, so
//! the scan emitted nothing and the residual `FilterExec` above it had no rows
//! left to keep.
//!
//! Reaching the generator needs all three of:
//!   1. a **Hash** scalar index on the column,
//!   2. an `=` (or `IN`) predicate on it — that is what puts the column into
//!      `PushdownStrategy::hash_index_columns`, and
//!   3. a two-sided *inclusive* range on the same column, which is the only
//!      shape the fusion fired on.
//!
//! Drop any one and the predicate never reaches `LanceFilterGenerator`, which
//! is why the plain `WHERE p >= 2 AND p <= 4` form always looked correct.
//!
//! The fix deleted the fusion entirely: structured output makes it a no-op, as
//! the generic path emits the same two `Compare` nodes with bare column names.

use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

async fn setup() -> anyhow::Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Ev")
        .property("createdAt", DataType::Int)
        .index("createdAt", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for i in 1..=6i64 {
        tx.execute(&format!("CREATE (:Ev {{createdAt: {i}}})"))
            .await?;
    }
    tx.commit().await?;
    // The predicate is only pushed to storage, so the rows must be flushed out
    // of L0 for this to exercise anything.
    session.flush().await?;
    Ok(db)
}

async fn created_ats(session: &uni_db::Session, query: &str) -> anyhow::Result<Vec<i64>> {
    let result = session.query(query).await?;
    let mut out: Vec<i64> = result
        .rows()
        .iter()
        .filter_map(|row| match row.value("v") {
            Some(Value::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    Ok(out)
}

/// The exact shape that returned `[]`.
#[tokio::test]
async fn eq_plus_two_sided_range_on_hash_indexed_column() -> anyhow::Result<()> {
    let db = setup().await?;
    let session = db.session();

    let got = created_ats(
        &session,
        "MATCH (n:Ev) WHERE n.createdAt = 3 AND n.createdAt >= 2 AND n.createdAt <= 4 \
         RETURN n.createdAt AS v",
    )
    .await?;
    assert_eq!(got, vec![3], "the range must not annihilate the equality");
    Ok(())
}

/// A range whose bounds exclude the equality must still be empty — proving the
/// assertion above is not just "the filter got dropped".
#[tokio::test]
async fn range_still_constrains_when_it_excludes_the_equality() -> anyhow::Result<()> {
    let db = setup().await?;
    let session = db.session();

    let got = created_ats(
        &session,
        "MATCH (n:Ev) WHERE n.createdAt = 3 AND n.createdAt >= 4 AND n.createdAt <= 6 \
         RETURN n.createdAt AS v",
    )
    .await?;
    assert!(got.is_empty(), "3 is outside [4, 6], got {got:?}");
    Ok(())
}

/// The `IN` form takes the same route into `hash_index_columns`.
#[tokio::test]
async fn in_list_plus_two_sided_range_on_hash_indexed_column() -> anyhow::Result<()> {
    let db = setup().await?;
    let session = db.session();

    let got = created_ats(
        &session,
        "MATCH (n:Ev) WHERE n.createdAt IN [2, 3, 5] AND n.createdAt >= 2 AND n.createdAt <= 4 \
         RETURN n.createdAt AS v",
    )
    .await?;
    assert_eq!(
        got,
        vec![2, 3],
        "5 is outside the range, 2 and 3 are inside"
    );
    Ok(())
}
