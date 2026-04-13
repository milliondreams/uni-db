// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for CommitNotification — async change streams with filtering.

use anyhow::Result;
use std::time::Duration;
use uni_db::{DataType, Uni};

async fn setup_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .label("Car")
        .property("model", DataType::String)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .edge_type("DRIVES", &["Person"], &["Car"])
        .apply()
        .await?;
    Ok(db)
}

#[tokio::test]
async fn test_watch_basic_notification() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let mut stream = session.watch();

    // Commit in a separate task
    let commit_session = db.session();
    tokio::spawn(async move {
        let tx = commit_session.tx().await.unwrap();
        tx.execute("CREATE (:Person {name: 'Alice'})").await.unwrap();
        tx.commit().await.unwrap();
    });

    // Wait for notification with timeout
    let notification = tokio::time::timeout(Duration::from_secs(5), stream.next()).await?;

    assert!(
        notification.is_some(),
        "Should receive a commit notification"
    );
    let n = notification.unwrap();
    assert!(n.mutation_count > 0, "Mutation count should be > 0");
    assert!(
        n.labels_affected.contains(&"Person".to_string()),
        "labels_affected should contain 'Person', got: {:?}",
        n.labels_affected
    );

    Ok(())
}

#[tokio::test]
async fn test_watch_label_filter() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let mut stream = session.watch_with().labels(&["Car"]).build();

    // Commit a Person (should NOT pass filter)
    let s1 = db.session();
    let tx1 = s1.tx().await?;
    tx1.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx1.commit().await?;

    // Commit a Car (SHOULD pass filter)
    let s2 = db.session();
    let tx2 = s2.tx().await?;
    tx2.execute("CREATE (:Car {model: 'Tesla'})").await?;
    tx2.commit().await?;

    // The first notification we receive should be the Car commit
    let notification =
        tokio::time::timeout(Duration::from_secs(5), stream.next()).await?;

    assert!(notification.is_some());
    let n = notification.unwrap();
    assert!(
        n.labels_affected.contains(&"Car".to_string()),
        "Filtered notification should be for Car, got: {:?}",
        n.labels_affected
    );

    Ok(())
}

#[tokio::test]
async fn test_watch_exclude_session() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let session_id = session.id().to_string();

    // Watch excluding our own session
    let mut stream = session
        .watch_with()
        .exclude_session(&session_id)
        .build();

    // Commit from same session (should be excluded)
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Self'})").await?;
    tx.commit().await?;

    // Commit from different session (should arrive)
    let other_session = db.session();
    let tx2 = other_session.tx().await?;
    tx2.execute("CREATE (:Person {name: 'Other'})").await?;
    tx2.commit().await?;

    // Should receive the "Other" commit, not the "Self" commit
    let notification =
        tokio::time::timeout(Duration::from_secs(5), stream.next()).await?;
    assert!(notification.is_some());
    let n = notification.unwrap();
    assert_ne!(
        n.session_id, session_id,
        "Should not receive notification from excluded session"
    );

    Ok(())
}

#[tokio::test]
async fn test_watch_edge_type_filter() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let mut stream = session.watch_with().edge_types(&["DRIVES"]).build();

    // Create nodes and a KNOWS edge (should NOT pass filter)
    let s1 = db.session();
    let tx1 = s1.tx().await?;
    tx1.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .await?;
    tx1.commit().await?;

    // Create a DRIVES edge (SHOULD pass filter)
    let s2 = db.session();
    let tx2 = s2.tx().await?;
    tx2.execute("CREATE (p:Person {name: 'Charlie'})-[:DRIVES]->(c:Car {model: 'BMW'})")
        .await?;
    tx2.commit().await?;

    let notification =
        tokio::time::timeout(Duration::from_secs(5), stream.next()).await?;
    assert!(notification.is_some());
    let n = notification.unwrap();
    assert!(
        n.edge_types_affected.contains(&"DRIVES".to_string()),
        "Filtered notification should be for DRIVES edge type, got: {:?}",
        n.edge_types_affected
    );

    Ok(())
}
