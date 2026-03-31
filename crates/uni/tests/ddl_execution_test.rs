// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_ddl_execution() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 1. CREATE LABEL
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING NOT NULL, age INT UNIQUE)")
        .await?;
    tx.commit().await?;

    let schema = db.schema().current();
    assert!(schema.labels.contains_key("Person"));
    let person_props = schema.properties.get("Person").unwrap();
    assert!(person_props.contains_key("name"));
    assert!(person_props.contains_key("age"));
    assert!(!person_props.get("name").unwrap().nullable);

    // Check auto-created unique constraint for age
    assert!(
        schema
            .constraints
            .iter()
            .any(|c| c.name == "Person_age_unique")
    );

    // 2. CREATE EDGE TYPE
    let tx = db.session().tx().await?;
    tx.execute("CREATE EDGE TYPE FOLLOWS (since STRING) FROM Person TO Person")
        .await?;
    tx.commit().await?;

    let schema = db.schema().current();
    assert!(schema.edge_types.contains_key("FOLLOWS"));
    let follows_meta = schema.edge_types.get("FOLLOWS").unwrap();
    assert_eq!(follows_meta.src_labels, &["Person".to_string()]);

    // 3. ALTER LABEL
    let tx = db.session().tx().await?;
    tx.execute("ALTER LABEL Person ADD PROPERTY bio STRING")
        .await?;
    tx.commit().await?;
    let schema = db.schema().current();
    assert!(schema.properties.get("Person").unwrap().contains_key("bio"));

    // 4. CREATE CONSTRAINT
    let tx = db.session().tx().await?;
    tx.execute("CREATE CONSTRAINT name_unique ON (p:Person) ASSERT p.name IS UNIQUE")
        .await?;
    tx.commit().await?;
    let schema = db.schema().current();
    assert!(schema.constraints.iter().any(|c| c.name == "name_unique"));

    // 5. SHOW CONSTRAINTS
    let result = db.session().query("SHOW CONSTRAINTS").await?;
    // Person_age_unique and name_unique
    assert_eq!(result.len(), 2);

    // 6. DROP LABEL
    let tx = db.session().tx().await?;
    tx.execute("DROP LABEL Person").await?;
    tx.commit().await?;
    let schema = db.schema().current();
    assert!(schema.labels.contains_key("Person"));
    assert!(matches!(
        schema.labels.get("Person").unwrap().state,
        uni_common::core::schema::SchemaElementState::Tombstone { .. }
    ));
    // Properties are kept for historical snapshots but should be considered part of tombstoned label
    assert!(schema.properties.contains_key("Person"));

    Ok(())
}
