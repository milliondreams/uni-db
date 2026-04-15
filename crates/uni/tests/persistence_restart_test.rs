// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Verifies that data written to the database persists to disk and survives restart.

use anyhow::Result;
use tempfile::tempdir;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_data_survives_restart() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path().to_str().unwrap().to_string();

    // --- PHASE 1: Write data and shut down ---
    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("Person")
            .property("name", DataType::String)
            .apply()
            .await?;

        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Person {name: 'PersistenceCheck'})")
            .await?;
        tx.commit().await?;

        // Flush to disk
        db.flush().await?;

        // db is dropped here, simulating shutdown
    }

    // --- PHASE 2: Restart and verify data survived ---
    {
        let db = Uni::open(&path).build().await?;

        let result = db
            .session()
            .query("MATCH (n:Person) RETURN n.name AS name")
            .await?;

        assert_eq!(result.len(), 1, "Data should persist after restart");
        assert_eq!(result.rows()[0].get::<String>("name")?, "PersistenceCheck");
    }

    Ok(())
}
