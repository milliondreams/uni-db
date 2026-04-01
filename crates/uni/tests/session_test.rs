// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_session_variables() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Create Schema
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (name STRING, tenant STRING)")
        .await?;
    tx.execute("CREATE (n:User {name: 'Alice', tenant: 'A'})")
        .await?;
    tx.execute("CREATE (n:User {name: 'Bob', tenant: 'B'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Create session with scoped parameter
    let session = db.session();
    session.params().set("tenant_id", "A");

    // Query with session variable
    // $session.tenant_id should resolve to "A"
    let results = session
        .query("MATCH (n:User) WHERE n.tenant = $session.tenant_id RETURN n.name")
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results.rows()[0].get::<String>("n.name")?, "Alice");

    Ok(())
}
