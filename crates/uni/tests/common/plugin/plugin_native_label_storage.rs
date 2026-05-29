#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Workstream J acceptance — `PluginRegistry::register_label_storage`
//! routes native-label vertex scans through a plugin `Storage`
//! instead of the host's native backend.
//!
//! Confirms that:
//! 1. `register_label_storage` makes the FakeStorage reachable.
//! 2. A `MATCH (n:NativeLabel) RETURN n.foo` query lands in
//!    `Storage::read_batch` (counter goes up).
//! 3. Returned rows show in the Cypher result set.

// Rust guideline compliant

use std::sync::{Arc, Mutex};

use arrow_array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use smol_str::SmolStr;
use uni_db::{DataType as UDt, Uni};
use uni_plugin::traits::storage::{Storage, WriteHandle};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};

#[derive(Debug, Default)]
struct StorageCalls {
    read_batch_count: usize,
    last_table: Option<String>,
}

struct FakeStorage {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    calls: Mutex<StorageCalls>,
}

impl std::fmt::Debug for FakeStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeStorage").finish()
    }
}

#[async_trait]
impl Storage for FakeStorage {
    async fn read_batch(
        &self,
        table: &str,
        _predicate: Option<&Expr>,
    ) -> Result<SendableRecordBatchStream, FnError> {
        {
            let mut g = self.calls.lock().expect("calls mutex");
            g.read_batch_count += 1;
            g.last_table = Some(table.to_owned());
        }
        let batches: Vec<_> = self.batches.iter().cloned().map(Ok).collect();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream::iter(batches),
        )))
    }

    async fn write_batch(
        &self,
        _table: &str,
        _batch: &RecordBatch,
    ) -> Result<WriteHandle, FnError> {
        Err(FnError::new(1, "fake storage is read-only"))
    }

    async fn list_tables(&self) -> Result<Vec<String>, FnError> {
        Ok(vec!["Person".to_owned()])
    }

    async fn delete(&self, _table: &str, _predicate: &Expr) -> Result<u64, FnError> {
        Err(FnError::new(1, "fake storage is read-only"))
    }
}

fn person_batch() -> (SchemaRef, RecordBatch) {
    // Matches the native vertex-table column shape so MVCC dedup +
    // downstream column mapping in `columnar_scan_vertex_batch_static`
    // accept it without modification.
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("_vid", DataType::UInt64, false),
        Field::new("_deleted", DataType::Boolean, false),
        Field::new("_version", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
            Arc::new(BooleanArray::from(vec![false, false, false])),
            Arc::new(UInt64Array::from(vec![1u64, 1, 1])),
            Arc::new(StringArray::from(vec![
                Some("alpha"),
                Some("beta"),
                Some("gamma"),
            ])),
        ],
    )
    .expect("person fixture batch");
    (schema, batch)
}

#[tokio::test]
async fn native_label_routed_through_plugin_storage() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    // Declare the label natively so the planner accepts the MATCH.
    db.schema()
        .label("Person")
        .property("name", UDt::String)
        .apply()
        .await?;

    let (schema, batch) = person_batch();
    let fake = Arc::new(FakeStorage {
        schema,
        batches: vec![batch],
        calls: Mutex::new(StorageCalls::default()),
    });

    // Register the plugin storage for label "Person".
    let registry = db.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Storage]);
    let mut r = PluginRegistrar::new(PluginId::new("test-native-label-storage"), &caps, registry);
    r.label_storage(
        SmolStr::new("Person"),
        (Arc::clone(&fake)) as Arc<dyn Storage>,
    )?;
    r.commit_to_registry()?;

    // Query — must route through FakeStorage.
    let rows = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name")
        .await?;

    let names: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(names, vec!["alpha", "beta", "gamma"]);

    let calls = fake.calls.lock().expect("calls mutex");
    assert!(
        calls.read_batch_count >= 1,
        "plugin Storage::read_batch must have been reached \
         (count = {})",
        calls.read_batch_count
    );
    assert_eq!(calls.last_table.as_deref(), Some("Person"));
    Ok(())
}

#[tokio::test]
async fn unregistered_label_uses_native_backend() -> anyhow::Result<()> {
    // Negative case: registering plugin storage for label "Foo" must
    // not affect scans of label "Person". This protects against a
    // typo or misregistration accidentally intercepting unrelated
    // labels.
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", UDt::String)
        .apply()
        .await?;

    let (schema, batch) = person_batch();
    let fake = Arc::new(FakeStorage {
        schema,
        batches: vec![batch],
        calls: Mutex::new(StorageCalls::default()),
    });

    let registry = db.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Storage]);
    let mut r = PluginRegistrar::new(PluginId::new("test-foo-label-storage"), &caps, registry);
    r.label_storage(SmolStr::new("Foo"), (Arc::clone(&fake)) as Arc<dyn Storage>)?;
    r.commit_to_registry()?;

    // Insert one Person via the native path.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'native'})").await?;
    tx.commit().await?;

    // Query — must NOT route through FakeStorage (registered for "Foo").
    let rows = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name")
        .await?;
    let names: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(names, vec!["native"]);

    let calls = fake.calls.lock().expect("calls mutex");
    assert_eq!(
        calls.read_batch_count, 0,
        "label-storage for 'Foo' must not intercept scans of 'Person'"
    );
    Ok(())
}
