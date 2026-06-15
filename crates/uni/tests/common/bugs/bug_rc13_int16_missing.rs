// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for RC13 (uniko `UNI_DB_WORKAROUNDS.md`): there is no 16-bit integer
//! `DataType`. `uni-common/src/core/schema.rs` `enum DataType` offers only
//! `Int32` / `Int64` among signed integers, so inherently-16-bit columns (e.g.
//! uniko's audio `channels` in `store/schema/artifacts.rs`) must be over-declared
//! as `Int32`.
//!
//! POLARITY EXCEPTION — unlike the other RC repros this is a **green
//! documentation guard**, not a red `#[ignore]` test. A true red repro would have
//! to compile-reference `DataType::Int16`, which does not exist, so it could not
//! be written. And `DataType` is `#[non_exhaustive]`, so it also cannot be
//! exhaustively matched from outside `uni-common` to auto-detect a new variant.
//! The guard therefore pins current behaviour and carries a `FIXME(RC13)` so the
//! day a 16-bit type lands, whoever adds it updates this test and narrows uniko's
//! `channels`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bug_rc13_int16_missing

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// An i16-range column (`channels`) must be declared `Int32` today, and round-trips.
///
/// FIXME(RC13): when `DataType::Int16` is added, narrow this column to it and
/// assert the narrower declared type here.
#[tokio::test]
async fn int16_range_column_must_use_int32() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // `channels` only ever needs i16 range, but Int32 is the narrowest signed
    // integer DataType available — RC13.
    db.schema()
        .label("Audio")
        .property("channels", DataType::Int32)
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // Store a value at the i16 boundary to show the column genuinely holds
    // 16-bit-range data while being declared 32-bit.
    tx.execute_with("CREATE (:Audio {channels: $c})")
        .param("c", Value::Int(i16::MAX as i64))
        .run()
        .await?;
    tx.commit().await?;

    let r = session
        .query("MATCH (a:Audio) RETURN a.channels AS c")
        .await?;
    assert_eq!(
        r.rows()[0].get::<i64>("c")?,
        i16::MAX as i64,
        "i16-range value round-trips through the Int32 column"
    );
    Ok(())
}
