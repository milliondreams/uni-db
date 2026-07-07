// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::RwLock;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

pub async fn import_semantic_scholar(
    papers_path: &Path,
    citations_path: &Path,
    output_path: &Path,
) -> Result<()> {
    println!("Initializing storage at {output_path:?}");

    // 1. Setup Schema
    let schema_file = output_path.join("schema.json");
    // `create_dir_all` is recursive, so this also creates any missing parents.
    tokio::fs::create_dir_all(output_path).await?;

    let schema_manager = SchemaManager::load(&schema_file).await?;

    // Define Schema
    // Paper — the label id is unused; we only need it registered.
    let _ = schema_manager.add_label("Paper");

    // Ensure properties
    let _ = schema_manager.add_property("Paper", "title", DataType::String, false);
    let _ = schema_manager.add_property("Paper", "year", DataType::Int32, false);
    let _ = schema_manager.add_property("Paper", "citation_count", DataType::Int32, false);
    // Embeddings are optional enrichment: the importer copies `embedding`
    // only when the JSONL record carries it (see the property loop below),
    // so the column must be nullable — otherwise attaching the `Paper` label
    // to an embedding-less paper trips a non-null constraint on import.
    let _ = schema_manager.add_property(
        "Paper",
        "embedding",
        DataType::Vector { dimensions: 768 },
        true,
    );
    // Additional properties
    let _ = schema_manager.add_property("Paper", "venue", DataType::String, true);

    // CITES Edge — register it, falling back to the existing id if already present.
    let cites_type = schema_manager
        .add_edge_type("CITES", vec!["Paper".into()], vec!["Paper".into()])
        .unwrap_or_else(|_| schema_manager.schema().edge_types["CITES"].id);

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage_dir = output_path.join("storage");
    let storage =
        Arc::new(StorageManager::new(storage_dir.to_str().unwrap(), schema_manager.clone()).await?);

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    // 2. Load Papers
    println!("Loading papers from {papers_path:?}");
    for_each_jsonl(&writer, papers_path, "papers", |w, _count, json| {
        Box::pin(async move {
            let vid_u64 = json
                .get("vid")
                .and_then(|v| v.as_u64())
                .ok_or(anyhow!("Missing vid"))?;
            let vid = Vid::new(vid_u64);

            // Copy the known scalar/vector properties straight through; any
            // key not present in the JSON is simply omitted.
            let mut props = HashMap::new();
            for key in ["title", "year", "citation_count", "embedding"] {
                if let Some(v) = json.get(key) {
                    props.insert(key.to_string(), v.clone());
                }
            }

            // Insert vertex — convert serde_json values to uni_common values.
            let uni_props: uni_common::Properties =
                props.into_iter().map(|(k, v)| (k, v.into())).collect();
            // Attach the `Paper` label the demo schema registers; passing no
            // labels would insert label-less vertices that `MATCH (p:Paper)`
            // cannot find.
            w.insert_vertex_with_labels(vid, uni_props, &["Paper".to_string()], None)
                .await?;
            Ok(())
        })
    })
    .await?;

    // 3. Load Citations
    println!("Loading citations from {citations_path:?}");
    for_each_jsonl(&writer, citations_path, "citations", |w, count, json| {
        Box::pin(async move {
            let src_u64 = json
                .get("src_vid")
                .and_then(|v| v.as_u64())
                .ok_or(anyhow!("Missing src_vid"))?;
            let dst_u64 = json
                .get("dst_vid")
                .and_then(|v| v.as_u64())
                .ok_or(anyhow!("Missing dst_vid"))?;

            let src_vid = Vid::new(src_u64);
            let dst_vid = Vid::new(dst_u64);

            // Generate EID based on count (simple).
            let eid = Eid::new(count);

            w.insert_edge(
                src_vid,
                dst_vid,
                cites_type,
                eid,
                HashMap::new(),
                Some("CITES".to_string()),
                None,
            )
            .await?;
            Ok(())
        })
    })
    .await?;

    println!("Import complete!");
    Ok(())
}

/// Stream a JSONL file line-by-line, invoking `f` for each parsed record.
///
/// Acquires the writer guard for the whole stream, prints progress every
/// 1000 records under `label`, then flushes to L1. `f` receives the held
/// writer guard, the zero-based record index, and the parsed JSON value.
///
/// # Errors
///
/// Returns an error if the file cannot be opened, a line is not valid
/// JSON, `f` fails, or the final flush fails.
async fn for_each_jsonl<F>(
    writer: &Arc<RwLock<Writer>>,
    path: &Path,
    label: &str,
    mut f: F,
) -> Result<()>
where
    F: for<'a> FnMut(
        &'a Writer,
        u64,
        Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>>,
{
    let file = File::open(path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let mut count: u64 = 0;
    {
        let w = writer.write().await;
        while let Some(line) = lines.next_line().await? {
            let json: Value = serde_json::from_str(&line)?;
            f(&w, count, json).await?;

            count += 1;
            if count.is_multiple_of(1000) {
                // `print!` writes through stdout's `LineWriter`, which only
                // flushes on a newline. Flush explicitly so the carriage-return
                // progress line is visible on a TTY during the loop.
                print!("\rProcessed {count} {label}");
                let _ = std::io::stdout().flush();
            }
        }
    }
    println!("\nFlushing {label}...");
    {
        let w = writer.write().await;
        w.flush_to_l1(None).await?;
    }
    Ok(())
}
