//! Regression test for <https://github.com/rustic-ai/uni-db/issues/22>
//!
//! FOLD variables (e.g. `n` from `FOLD n = COUNT(*)`) must be available in the
//! YIELD output schema when property-level KEY expressions are used.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn fold_var_projected_with_property_key() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Event")
        .property("action", DataType::String)
        .property("outcome", DataType::String)
        .property("importance", DataType::Float64)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    for _ in 0..3 {
        tx.execute("CREATE (:Event {action: 'deploy', outcome: 'success', importance: 0.8})")
            .await?;
    }
    tx.execute("CREATE (:Event {action: 'deploy', outcome: 'failure', importance: 0.5})")
        .await?;
    tx.commit().await?;

    let program = r#"
        CREATE RULE pattern_detector AS
            MATCH (e:Event)
            WHERE e.importance > 0.3
            FOLD n = COUNT(*)
            WHERE n >= 3
            YIELD KEY e.action, KEY e.outcome, n AS support

        QUERY pattern_detector RETURN *
    "#;

    let result = session.locy(program).await;
    assert!(result.is_ok(), "got error: {}", result.unwrap_err());

    let result = result.unwrap();
    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);
    assert!(
        !rows.is_empty(),
        "should find (deploy, success) with count >= 3"
    );

    // Verify the FOLD variable 'support' is in the output
    let row = &rows[0];
    assert!(
        row.contains_key("support"),
        "FOLD variable 'support' missing from output. Keys: {:?}",
        row.keys().collect::<Vec<_>>()
    );

    Ok(())
}
