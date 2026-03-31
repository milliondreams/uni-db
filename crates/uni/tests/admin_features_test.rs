// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_admin_features() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // SHOW DATABASE
    let result = db.session().query("SHOW DATABASE").await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name")?, "uni");

    // SHOW CONFIG
    let result = db.session().query("SHOW CONFIG").await?;
    // Should be empty for now
    assert_eq!(result.len(), 0);

    // Insert Data and Checkpoint for Statistics
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (name STRING)").await?;
    tx.execute("CREATE (:User {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // SHOW STATISTICS
    let result = db.session().query("SHOW STATISTICS").await?;
    assert!(!result.is_empty());
    let user_stat = result
        .rows()
        .iter()
        .find(|r| {
            if let Ok(name) = r.get::<String>("name") {
                name == "User"
            } else {
                false
            }
        })
        .expect("User statistics not found");
    assert_eq!(user_stat.get::<i64>("count")?, 1);

    // VACUUM
    let tx = db.session().tx().await?;
    tx.execute("VACUUM").await?;
    tx.commit().await?;
    // Implicitly verifies no error

    Ok(())
}
