// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #200 — a failed flush barrier says how many, not why.
//!
//! `flush_to_l1` refuses to claim a durability barrier it cannot honour, which
//! is the right behaviour and is why the condition is visible at all. But the
//! error carried only a count. One occurrence under ordinary full-suite load
//! could not be diagnosed afterwards: it did not reproduce, disk and `TMPDIR`
//! were both ruled out, and by the time anyone looked there was nothing left
//! but the number.
//!
//! The cause is now captured where it is already in hand — the finalizer's
//! failure arm — and printed in the error that stops the run. That does not
//! explain the original occurrence, which remains unexplained; it means the
//! next one arrives with its reason attached.

// Rust guideline compliant
#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::config::UniConfig;
use uni_common::core::schema::SchemaManager;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

use super::fault_backend::FaultBackend;

/// A barrier failure names the failure that caused it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_barrier_reports_the_cause() -> Result<()> {
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let lance = LanceDbBackend::connect(&uri, None).await?;
    let fault = Arc::new(FaultBackend::new(Arc::new(lance)));
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let sm = Arc::new(
        SchemaManager::load_from_store(store.clone(), &ObjectStorePath::from("schema.json"))
            .await?,
    );
    sm.add_label("N")?;
    sm.save().await?;

    // Async flush is the path with a stream phase that can fail independently
    // of the caller; the inline path propagates its error directly.
    let config = UniConfig {
        async_flush_enabled: true,
        ..UniConfig::default()
    };
    let storage = Arc::new(
        StorageManager::new_with_backend(&uri, store, fault.clone(), sm.clone(), config).await?,
    );
    let writer = Arc::new(Writer::new(storage, sm.clone(), 1).await?);

    for _ in 0..4 {
        let vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(vid, HashMap::new(), &["N".to_string()], None)
            .await?;
    }

    // Refuse the append the stream phase is about to make.
    fault.set_fail_write(true);
    let ticket = writer.flush_to_l1_async(None).await?;
    let _ = ticket.await_finalize().await;

    // Reads work again; only the stranded L0 remains.
    fault.set_fail_write(false);

    let err = writer
        .flush_to_l1(None)
        .await
        .expect_err("a stranded L0 must fail the barrier");
    let msg = err.to_string();

    assert!(
        msg.contains("barrier not established"),
        "expected the barrier error, got: {msg}"
    );
    assert!(
        msg.contains("Most recent failure:"),
        "the barrier error must carry the cause, not just a count. Got: {msg}"
    );
    assert!(
        msg.contains("injected write failure"),
        "the cause must be the failure that actually happened — the injected \
         write refusal — rather than a placeholder. Got: {msg}"
    );
    Ok(())
}
