//! Regression test for <https://github.com/rustic-ai/uni-db/issues/23>
//!
//! An over-budget Locy evaluation is a hard error by default
//! (`UniError::LocyIncomplete`), naming the rules that were left incomplete or
//! skipped so a zero-row count is never mistaken for an empty result. Opting
//! into `allow_partial` recovers the partial result with the same diagnostics.

use std::time::Duration;

use anyhow::Result;
use uni_db::{DataType, LocyIncompleteReason, Uni, UniError};

/// Seeds 100 `Item` nodes across five categories — enough that the cross join
/// below is non-trivial work to evaluate.
async fn seed_items() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property("category", DataType::String)
        .property("value", DataType::Float64)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    for i in 0..100 {
        tx.execute(&format!(
            "CREATE (:Item {{name: 'item_{i}', category: 'cat_{cat}', value: {val}}})",
            i = i,
            cat = i % 5,
            val = i as f64 * 0.1,
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

const PROGRAM: &str = r#"
    CREATE RULE cross_join AS
        MATCH (a:Item), (b:Item)
        WHERE a.category = b.category
          AND a.name <> b.name
        YIELD KEY a, KEY b

    QUERY cross_join RETURN *
"#;

#[tokio::test]
async fn locy_timeout_is_hard_error_by_default() -> Result<()> {
    let db = seed_items().await?;

    // A 1 ns budget guarantees the evaluation is cut short.
    let result = db
        .session()
        .locy_with(PROGRAM)
        .timeout(Duration::from_nanos(1))
        .run()
        .await;

    let err = result.expect_err("a 1ns timeout should error by default, not return partial");
    match err {
        UniError::LocyIncomplete { detail } => {
            assert_eq!(detail.reason, LocyIncompleteReason::Timeout);
            // cross_join did not finish, so it is reported — not silently empty.
            assert!(
                detail.skipped_rules.iter().any(|r| r == "cross_join")
                    || detail.incomplete_rules.iter().any(|r| r == "cross_join"),
                "cross_join should be reported as skipped/incomplete, got {detail:?}"
            );
        }
        other => panic!("expected UniError::LocyIncomplete, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn locy_timeout_allow_partial_returns_result() -> Result<()> {
    let db = seed_items().await?;

    // Opt into best-effort semantics: the partial result comes back instead.
    let result = db
        .session()
        .locy_with(PROGRAM)
        .timeout(Duration::from_nanos(1))
        .allow_partial(true)
        .run()
        .await?;

    assert!(
        result.timed_out(),
        "allow_partial result should flag timed_out"
    );
    let detail = result
        .incomplete
        .as_ref()
        .expect("allow_partial result should carry incomplete diagnostics");
    assert_eq!(detail.reason, LocyIncompleteReason::Timeout);
    Ok(())
}

#[tokio::test]
async fn locy_timeout_flags_complement_rule_as_unsound() -> Result<()> {
    let db = seed_items().await?;

    // `healthy` is the complement of `positive`. A timeout that skips it must
    // flag it: stratified negation over an unfinished relation is unsound, so a
    // zero-row `healthy` is meaningless rather than "no healthy items".
    let program = "\
        CREATE RULE positive AS MATCH (e:Item) WHERE e.value > 0.0 YIELD KEY e \n\
        CREATE RULE healthy AS MATCH (e:Item) WHERE e IS NOT positive YIELD KEY e";

    let err = db
        .session()
        .locy_with(program)
        .timeout(Duration::from_nanos(1))
        .run()
        .await
        .expect_err("a 1ns timeout should error by default");
    match err {
        UniError::LocyIncomplete { detail } => {
            assert_eq!(detail.reason, LocyIncompleteReason::Timeout);
            assert!(
                detail
                    .complement_rules_affected
                    .iter()
                    .any(|r| r == "healthy"),
                "the IS NOT rule 'healthy' should be flagged as unsound, got {detail:?}"
            );
        }
        other => panic!("expected UniError::LocyIncomplete, got {other:?}"),
    }
    Ok(())
}
