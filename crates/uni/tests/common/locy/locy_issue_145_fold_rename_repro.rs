// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #145 + blast-radius matrix: FOLD value-aggregates are silently zeroed
//! when the YIELD clause renames the FOLD variable to a different alias.
//!
//! Root cause: the body-clause projection keys the FOLD-input map by the FOLD
//! variable name but looks it up by the YIELD alias, so under rename the
//! aggregate's input column is never projected and `FoldExec` falls back to an
//! all-zeros array. COUNT is immune (it counts rows, not values); every
//! value-carrying aggregate (SUM/AVG/MAX/MIN/COLLECT/...) is affected.

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Build a db with 3 Events (action='deploy'):
///   importance = 0.2, 0.6, 1.0   → SUM=1.8, AVG=0.6, MAX=1.0, MIN=0.2, COUNT=3
///   weight     = 10,  20,  30    → SUM=60,  AVG=20,  MAX=30,  MIN=10
/// Two distinct numeric columns so cross-column contamination is detectable.
async fn events_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Event")
        .property("action", DataType::String)
        .property("importance", DataType::Float64)
        .property("weight", DataType::Float64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for (imp, wgt) in [(0.2_f64, 10.0_f64), (0.6, 20.0), (1.0, 30.0)] {
        tx.execute(&format!(
            "CREATE (:Event {{action: 'deploy', importance: {imp}, weight: {wgt}}})"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

async fn first_row(db: &Uni, program: &str) -> Vec<(String, Option<f64>)> {
    let session = db.session();
    let result = session.locy_with(program).run().await.expect("locy run");
    let empty = vec![];
    let row = match result.rows().unwrap_or(&empty).first() {
        Some(r) => r.clone(),
        None => return vec![],
    };
    let mut cols: Vec<String> = row.keys().cloned().collect();
    cols.sort();
    cols.into_iter()
        .map(|k| (k.clone(), row.get(&k).and_then(Value::as_f64)))
        .collect()
}

fn get(row: &[(String, Option<f64>)], col: &str) -> Option<f64> {
    row.iter().find(|(k, _)| k == col).and_then(|(_, v)| *v)
}

/// Probabilistic-input db: 2 signals in group 'x' with p = 0.3, 0.5.
///   MNOR(p) = 1 − (1−0.3)(1−0.5) = 0.65 ;  MPROD(p) = 0.3·0.5 = 0.15
async fn prob_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Sig")
        .property("grp", DataType::String)
        .property("p", DataType::Float64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for p in [0.3_f64, 0.5] {
        tx.execute(&format!("CREATE (:Sig {{grp: 'x', p: {p}}})"))
            .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// Read the raw `Value` of a column from the first row (for non-scalar
/// aggregates like COLLECT whose result is a list).
async fn first_row_value(db: &Uni, program: &str, col: &str) -> Option<Value> {
    let session = db.session();
    let result = session.locy_with(program).run().await.expect("locy run");
    let empty = vec![];
    let row = result.rows().unwrap_or(&empty).first().cloned()?;
    row.get(col).cloned()
}

/// The exact #145 repro: SUM/AVG renamed in YIELD.
#[tokio::test]
async fn issue_145_sum_avg_renamed() -> Result<()> {
    let db = events_db().await?;
    let bug = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Event) \
         FOLD total = SUM(e.importance), avg_imp = AVG(e.importance) \
         YIELD KEY e.action AS action, total AS sum_out, avg_imp AS avg_out \
         QUERY r RETURN action, sum_out, avg_out",
    )
    .await;
    println!("sum/avg renamed -> {bug:?}");
    assert_eq!(get(&bug, "sum_out"), Some(1.8), "SUM zeroed under rename");
    assert_eq!(get(&bug, "avg_out"), Some(0.6), "AVG zeroed under rename");
    Ok(())
}

/// Related: MAX / MIN renamed. Same projection path — expected broken too,
/// which shows the #145 report understates the scope (it only cites SUM/AVG).
#[tokio::test]
async fn issue_145_max_min_renamed() -> Result<()> {
    let db = events_db().await?;
    let bug = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Event) \
         FOLD mx = MAX(e.importance), mn = MIN(e.importance) \
         YIELD KEY e.action AS action, mx AS max_out, mn AS min_out \
         QUERY r RETURN action, max_out, min_out",
    )
    .await;
    println!("max/min renamed -> {bug:?}");
    assert_eq!(
        get(&bug, "max_out"),
        Some(1.0),
        "MAX corrupted under rename"
    );
    assert_eq!(
        get(&bug, "min_out"),
        Some(0.2),
        "MIN corrupted under rename"
    );
    Ok(())
}

/// Control: the SAME aggregates with same-name YIELD alias must stay correct.
#[tokio::test]
async fn issue_145_control_same_name_ok() -> Result<()> {
    let db = events_db().await?;
    let ok = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Event) \
         FOLD s = SUM(e.importance), a = AVG(e.importance), mx = MAX(e.importance) \
         YIELD KEY e.action AS action, s AS s, a AS a, mx AS mx \
         QUERY r RETURN action, s, a, mx",
    )
    .await;
    println!("same-name -> {ok:?}");
    assert_eq!(get(&ok, "s"), Some(1.8));
    assert_eq!(get(&ok, "a"), Some(0.6));
    assert_eq!(get(&ok, "mx"), Some(1.0));
    Ok(())
}

/// Cross-column contamination: two value-aggregates over DIFFERENT columns,
/// the first renamed. The runtime name-lookup misses and falls back to a
/// positional index that can land on the *sibling* column — so the renamed
/// aggregate returns another column's value (a plausible WRONG number), not
/// even 0.0. This is the most insidious variant: silent, data-dependent.
#[tokio::test]
async fn issue_145_cross_column_contamination() -> Result<()> {
    let db = events_db().await?;
    let row = first_row(
        &db,
        // SUM(importance)=1.8, SUM(weight)=60. First renamed, second same-name.
        "CREATE RULE r AS MATCH (e:Event) \
         FOLD imp = SUM(e.importance), wgt = SUM(e.weight) \
         YIELD KEY e.action AS action, imp AS renamed_imp, wgt AS wgt \
         QUERY r RETURN action, renamed_imp, wgt",
    )
    .await;
    println!("cross-column -> {row:?}");
    assert_eq!(
        get(&row, "wgt"),
        Some(60.0),
        "same-name SUM(weight) should be fine"
    );
    assert_eq!(
        get(&row, "renamed_imp"),
        Some(1.8),
        "renamed SUM(importance) returned wrong/zero value (cross-column or zeros fallback)"
    );
    Ok(())
}

/// COUNT renamed — expected to REMAIN correct (immune), documenting the
/// asymmetry that fingerprints the root cause.
#[tokio::test]
async fn issue_145_count_renamed_is_immune() -> Result<()> {
    let db = events_db().await?;
    let row = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Event) \
         FOLD n = COUNT(*) \
         YIELD KEY e.action AS action, n AS support \
         QUERY r RETURN action, support",
    )
    .await;
    println!("count renamed -> {row:?}");
    assert_eq!(
        get(&row, "support"),
        Some(3.0),
        "COUNT should survive rename"
    );
    Ok(())
}

/// MSUM (monotonic sum) renamed — same projection path as SUM.
#[tokio::test]
async fn issue_145_msum_renamed() -> Result<()> {
    let db = events_db().await?;
    let row = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Event) \
         FOLD total = MSUM(e.importance) \
         YIELD KEY e.action AS action, total AS msum_out \
         QUERY r RETURN action, msum_out",
    )
    .await;
    println!("msum renamed -> {row:?}");
    assert_eq!(get(&row, "msum_out"), Some(1.8), "MSUM zeroed under rename");
    Ok(())
}

/// MNOR (noisy-OR) renamed — MOST SEVERE manifestation: the PROB path consumes
/// the out-of-range `input_col_index` fallback and CRASHES the engine with
/// `index out of bounds` in RecordBatch::column (not just silent corruption).
/// Correct answer would be 0.65; today the query panics.
#[tokio::test]
async fn issue_145_mnor_renamed() -> Result<()> {
    let db = prob_db().await?;
    let row = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Sig) \
         FOLD risk = MNOR(e.p) \
         YIELD KEY e.grp AS grp, risk AS risk_out \
         QUERY r RETURN grp, risk_out",
    )
    .await;
    println!("mnor renamed -> {row:?}");
    assert_eq!(
        get(&row, "risk_out"),
        Some(0.65),
        "MNOR probability collapsed under rename"
    );
    Ok(())
}

/// MPROD (product) renamed — same PROB path as MNOR; crashes the engine.
/// Correct answer would be 0.15; today the query panics.
#[tokio::test]
async fn issue_145_mprod_renamed() -> Result<()> {
    let db = prob_db().await?;
    let row = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Sig) \
         FOLD score = MPROD(e.p) \
         YIELD KEY e.grp AS grp, score AS score_out \
         QUERY r RETURN grp, score_out",
    )
    .await;
    println!("mprod renamed -> {row:?}");
    assert_eq!(
        get(&row, "score_out"),
        Some(0.15),
        "MPROD probability collapsed under rename"
    );
    Ok(())
}

/// COLLECT renamed — result should be the collected values, but under rename
/// it becomes a list of fabricated zeros.
#[tokio::test]
async fn issue_145_collect_renamed() -> Result<()> {
    let db = prob_db().await?;
    let val = first_row_value(
        &db,
        "CREATE RULE r AS MATCH (e:Sig) \
         FOLD ps = COLLECT(e.p) \
         YIELD KEY e.grp AS grp, ps AS ps_out \
         QUERY r RETURN grp, ps_out",
        "ps_out",
    )
    .await;
    println!("collect renamed -> {val:?}");
    let list = match val {
        Some(Value::List(v)) => v,
        other => panic!("expected COLLECT list, got {other:?}"),
    };
    let mut nums: Vec<f64> = list.iter().filter_map(Value::as_f64).collect();
    nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        nums,
        vec![0.3, 0.5],
        "COLLECT collected fabricated zeros under rename"
    );
    Ok(())
}

/// Type corruption: MAX/MIN preserve the input dtype (Int64 here), but the
/// runtime zeros-fallback is hard-coded Float64. Under rename the value is not
/// just wrong (0) — the column type is also silently changed.
#[tokio::test]
async fn issue_145_int_max_min_type_corruption() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Ev")
        .property("kind", DataType::String)
        .property("score", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for s in [10_i64, 20, 30] {
        tx.execute(&format!("CREATE (:Ev {{kind: 'a', score: {s}}})"))
            .await?;
    }
    tx.commit().await?;
    drop(session);

    let row = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Ev) \
         FOLD mx = MAX(e.score), mn = MIN(e.score) \
         YIELD KEY e.kind AS kind, mx AS max_out, mn AS min_out \
         QUERY r RETURN kind, max_out, min_out",
    )
    .await;
    println!("int max/min renamed -> {row:?}");
    assert_eq!(
        get(&row, "max_out"),
        Some(30.0),
        "Int64 MAX corrupted under rename"
    );
    assert_eq!(
        get(&row, "min_out"),
        Some(10.0),
        "Int64 MIN corrupted under rename"
    );
    Ok(())
}

/// Related defect (scenario 2): an expression computed OVER a FOLD output.
/// `total * 2.0 AS score` — the fold var is nested in a BinaryOp, so it is
/// pushed into the body projection where `total` does not yet exist (FOLD runs
/// later). Fixing this needs a dedicated post-fold YIELD projection stage, which
/// is deferred to a follow-up (see the #145 fix plan, Layer 5). SUM=1.8 → 3.6.
#[ignore = "expr-over-FOLD-output needs a post-fold YIELD projection stage — deferred follow-up (see #145 fix plan Layer 5)"]
#[tokio::test]
async fn issue_145_expr_over_fold_output() -> Result<()> {
    let db = events_db().await?;
    let row = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Event) \
         FOLD total = SUM(e.importance) \
         YIELD KEY e.action AS action, total * 2.0 AS score \
         QUERY r RETURN action, score",
    )
    .await;
    println!("expr over fold output -> {row:?}");
    assert_eq!(get(&row, "score"), Some(3.6), "expr over FOLD output wrong");
    Ok(())
}

/// Downstream corruption: HAVING references a renamed value aggregate.
/// If the value is zeroed, the HAVING predicate is evaluated against 0.0 and
/// wrongly filters the group out entirely.
#[tokio::test]
async fn issue_145_having_on_renamed_value() -> Result<()> {
    let db = events_db().await?;
    // SUM = 1.8, so `total > 1.0` should PASS and keep the group.
    let row = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Event) \
         FOLD total = SUM(e.importance) \
         WHERE total > 1.0 \
         YIELD KEY e.action AS action, total AS sum_out \
         QUERY r RETURN action, sum_out",
    )
    .await;
    println!("having on renamed -> {row:?}");
    assert_eq!(
        get(&row, "sum_out"),
        Some(1.8),
        "group wrongly dropped by HAVING because renamed SUM was zeroed"
    );
    Ok(())
}

/// Downstream corruption: BEST BY orders on a renamed value aggregate.
#[tokio::test]
async fn issue_145_best_by_on_renamed_value() -> Result<()> {
    let db = events_db().await?;
    let row = first_row(
        &db,
        "CREATE RULE r AS MATCH (e:Event) \
         FOLD total = SUM(e.importance) \
         BEST BY total DESC \
         YIELD KEY e.action AS action, total AS sum_out \
         QUERY r RETURN action, sum_out",
    )
    .await;
    println!("best by on renamed -> {row:?}");
    assert_eq!(
        get(&row, "sum_out"),
        Some(1.8),
        "BEST BY renamed value zeroed"
    );
    Ok(())
}
