// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro/regression for issue #68.
//!
//! Writing a value whose runtime type does not match a column's declared
//! `DataType` (the motivating case: a `Value::String` into a non-nullable
//! `DataType::DateTime` column) used to be silently accepted at CREATE/commit,
//! then nulled at L0→snapshot flush — and because the column is non-nullable,
//! the whole row was silently dropped from the per-label persisted table after
//! reopen (still reachable by `id()`/edges). `commit()` returned `Ok` and the
//! row was visible in-session, so the application had no signal.
//!
//! The fix validates property values against the declared schema type at write
//! time (issue #68, "reject + coerce known-safe"):
//!   - a `Value::String` into a temporal column is coerced into the proper
//!     `Temporal` value, exactly as the Cypher `datetime()`/`date()`/`time()`/
//!     `duration()` constructors would — so the row persists with a real value;
//!   - an unparseable string, or any other genuine type mismatch, is rejected
//!     with a `TypeError` at the call site rather than nulled at flush;
//!   - intentional lossless widenings (`Int`→`Float`, `Int`→`Int32`,
//!     `Temporal`→`Timestamp`) and schemaless properties are unaffected.
//!
//! Run with:
//!   cargo nextest run -p uni --test integration issue_68

use tempfile::tempdir;
use uni_db::{DataType, Uni};

/// A string into a non-nullable DateTime column is coerced and survives reopen.
///
/// This is the exact original repro: pre-fix the per-label count was 0 after
/// reopen; post-fix the row persists with a real timestamp.
#[tokio::test]
async fn issue_68_string_into_datetime_coerced_and_persisted() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("Event")
            .property("ts", DataType::DateTime) // non-nullable
            .done()
            .apply()
            .await?;

        let session = db.session();
        let tx = session.tx().await?;
        // A bare string literal (Value::String), NOT datetime('...').
        tx.execute("CREATE (:Event {ts: '2026-01-01T00:00:00Z'})")
            .await?;
        tx.commit().await?;

        let pre = session
            .query_with("MATCH (e:Event) RETURN count(e) AS c")
            .fetch_all()
            .await?
            .rows()[0]
            .get::<i64>("c")?;
        assert_eq!(pre, 1, "row should be visible in-session before shutdown");
        db.flush().await?;
        db.shutdown().await?;
    }

    // Reopen: the label-anchored scan must still find the row (the bug dropped it).
    let db = Uni::open(&path).build().await?;
    let post = db
        .session()
        .query_with("MATCH (e:Event) RETURN count(e) AS c")
        .fetch_all()
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(
        post, 1,
        "issue #68: row dropped from label-scan after reopen (silent type-mismatch null)"
    );
    Ok(())
}

/// An unparseable string into a temporal column is rejected at CREATE time.
#[tokio::test]
async fn issue_68_unparseable_string_into_datetime_rejected() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Event")
        .property("ts", DataType::DateTime)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query_with("CREATE (:Event {ts: 'not-a-real-datetime'})")
        .fetch_all()
        .await;
    let err = res.expect_err("garbage string into a DateTime column must be rejected");
    assert!(
        err.to_string().contains("TypeError"),
        "expected a TypeError, got: {err}"
    );
    Ok(())
}

/// SET coerces a valid string and rejects an unparseable one, symmetrically with CREATE.
#[tokio::test]
async fn issue_68_set_string_into_datetime_coerced_and_rejected() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Event")
        .property("ts", DataType::DateTime)
        .done()
        .apply()
        .await?;

    // Seed a correctly-typed row.
    {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Event {ts: datetime('2020-01-01T00:00:00Z')})")
            .await?;
        tx.commit().await?;
    }

    // SET with a valid string coerces and commits.
    {
        let tx = db.session().tx().await?;
        tx.execute("MATCH (e:Event) SET e.ts = '2030-06-15T12:00:00Z'")
            .await?;
        tx.commit().await?;
    }

    // SET with garbage is rejected.
    {
        let tx = db.session().tx().await?;
        let res = tx
            .query_with("MATCH (e:Event) SET e.ts = 'definitely-not-a-date'")
            .fetch_all()
            .await;
        assert!(
            res.is_err(),
            "SET of an unparseable string into a DateTime column must be rejected"
        );
    }
    Ok(())
}

/// Genuine scalar type mismatches are rejected (no silent null, no stringification).
#[tokio::test]
async fn issue_68_genuine_scalar_mismatches_rejected() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("flag", DataType::Bool)
        .property("count", DataType::Int64)
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;

    // (cypher, why) — each must be rejected.
    let cases = [
        ("CREATE (:N {flag: 'true'})", "String into Bool"),
        ("CREATE (:N {count: true})", "Bool into Int"),
        ("CREATE (:N {flag: 1.5})", "Float into Bool"),
        (
            "CREATE (:N {name: 10})",
            "Int into String (no stringification)",
        ),
    ];
    for (cypher, why) in cases {
        let tx = db.session().tx().await?;
        let res = tx.query_with(cypher).fetch_all().await;
        assert!(
            res.is_err(),
            "{why} must be rejected, but it succeeded: {cypher}"
        );
    }
    Ok(())
}

/// Intentional lossless coercions and schemaless properties must still be accepted.
#[tokio::test]
async fn issue_68_anti_over_rejection() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("f", DataType::Float64)
        .property("i", DataType::Int32)
        .property("ts", DataType::DateTime)
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // Int widens to Float; Int fits Int32; a datetime() literal is a Temporal.
    tx.execute("CREATE (:N {f: 10, i: 50, ts: datetime('2020-01-01T00:00:00Z')})")
        .await?;
    // Schemaless label: no declared types → no type guard, only structural checks.
    tx.execute("CREATE (:Misc {price: 10, kind: 'widget'})")
        .await?;
    tx.commit().await?;

    let n = session
        .query_with("MATCH (n:N) RETURN count(n) AS c")
        .fetch_all()
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(n, 1, "all lossless coercions should have been accepted");
    Ok(())
}
