//! Regression test for <https://github.com/rustic-ai/uni-db/issues/23>
//!
//! Locy timeout should return partial results (Ok with `timed_out = true`)
//! instead of a hard error.

use std::time::Duration;

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn locy_timeout_returns_partial_results() -> Result<()> {
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

    let program = r#"
        CREATE RULE cross_join AS
            MATCH (a:Item), (b:Item)
            WHERE a.category = b.category
              AND a.name <> b.name
            YIELD KEY a, KEY b

        QUERY cross_join RETURN *
    "#;

    // Use an extremely short timeout to force timeout.
    let result = session
        .locy_with(program)
        .timeout(Duration::from_nanos(1))
        .run()
        .await;

    // Should be Ok, not Err — partial results instead of hard error.
    assert!(
        result.is_ok(),
        "expected Ok with timed_out=true, got Err: {}",
        result.unwrap_err()
    );

    let result = result.unwrap();
    assert!(
        result.timed_out,
        "expected timed_out=true for a 1ns timeout"
    );

    Ok(())
}
