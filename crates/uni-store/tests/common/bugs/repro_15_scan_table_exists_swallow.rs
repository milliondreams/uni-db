// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for manager.rs:1295 (finding [15]).
//!
//! `scan_vertex_table` (and the sibling scan/search paths at 1365/1428/1797/
//! 2142) map a `table_exists()` error to "table absent" via
//! `.unwrap_or(false)`. `table_exists` genuinely CAN return `Err` (its Lance
//! impl does a `table_names()` directory listing that can fail transiently).
//! When it does, the scan silently returns `Ok(None)` — a legitimately-present
//! table's rows vanish during an I/O blip instead of surfacing the error.
//! Contrast the sibling paths at 1877/1962/1992/2076 that use `?`.

#![cfg(feature = "lance-backend")]

use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::config::UniConfig;
use uni_common::core::schema::SchemaManager;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::backend::table_names;
use uni_store::backend::traits::StorageBackend;
use uni_store::storage::manager::StorageManager;

use super::fault_backend::FaultBackend;

#[tokio::test]
async fn repro_scan_vertex_table_swallows_transient_list_error() {
    let dir = tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    // Real Lance backend with a populated `vertices_Person` table.
    let lance = LanceDbBackend::connect(&uri, None).await.unwrap();
    let table = table_names::vertex_table_name("Person");
    let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "_vid",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![1u64, 2, 3]))],
    )
    .unwrap();
    lance.create_table(&table, vec![batch]).await.unwrap();

    // Wrap the backend so we can inject a transient `table_exists` failure.
    let fault = Arc::new(FaultBackend::new(Arc::new(lance)));

    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store.clone(), &ObjectStorePath::from("schema.json"))
            .await
            .unwrap(),
    );
    schema_manager.add_label("Person").unwrap();
    let sm = StorageManager::new_with_backend(
        &uri,
        store,
        fault.clone(),
        schema_manager,
        UniConfig::default(),
    )
    .await
    .unwrap();

    // Control: healthy backend -> the table's rows are returned.
    let ok = sm
        .scan_vertex_table("Person", &["_vid"], None)
        .await
        .unwrap();
    assert!(
        ok.is_some_and(|b| b.num_rows() == 3),
        "control: healthy scan returns the 3 rows"
    );

    // Inject a transient LIST failure into table_exists.
    fault.set_fail_table_exists(true);
    let res = sm.scan_vertex_table("Person", &["_vid"], None).await;

    // Fixed (manager.rs:1295): the transient failure now surfaces as Err
    // instead of being swallowed to a silent Ok(None) that would make a
    // present table's rows vanish during an I/O blip.
    assert!(
        res.is_err(),
        "transient table_exists failure must surface as Err, not Ok(None); got {res:?}"
    );
}
