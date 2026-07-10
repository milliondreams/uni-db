// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{
    FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder, ListBuilder, UInt64Builder,
};
use arrow_array::{
    LargeBinaryArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
};
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::storage::manager::StorageManager;
use uni_db::{Uni, Value};

#[tokio::test]
async fn test_hybrid_vector_graph_query() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _paper_label_id = schema_manager.add_label("Paper")?;
    let _author_label_id = schema_manager.add_label("Author")?;
    let _wrote_edge_id =
        schema_manager.add_edge_type("WROTE", vec!["Author".into()], vec!["Paper".into()])?;

    schema_manager.add_property(
        "Paper",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.add_property("Paper", "title", DataType::String, false)?;
    schema_manager.add_property("Author", "name", DataType::String, false)?;

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 1000);

    // 2. Insert Data: Papers
    // Note: VIDs must be globally unique across all labels in the new storage model.
    {
        let dataset = storage.vertex_dataset("Paper")?;
        let schema = dataset.get_arrow_schema(&schema_manager.schema())?;

        let p1 = Vid::new(0); // Paper 0
        let p2 = Vid::new(1); // Paper 1
        let vids = UInt64Array::from(vec![p1.as_u64(), p2.as_u64()]);
        let versions = UInt64Array::from(vec![1, 1]);
        let deleted = arrow_array::BooleanArray::from(vec![false, false]);

        // UIDs
        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        for _ in 0..2 {
            uid_builder.append_value([0u8; 32]).unwrap();
        }
        let uids = uid_builder.finish();

        // Vectors
        let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        // Paper 1: [0.1, 0.1]
        vector_builder.values().append_value(0.1);
        vector_builder.values().append_value(0.1);
        vector_builder.append(true);
        // Paper 2: [0.9, 0.9]
        vector_builder.values().append_value(0.9);
        vector_builder.values().append_value(0.9);
        vector_builder.append(true);
        let vectors = vector_builder.finish();

        // Titles
        let titles = StringArray::from(vec!["Vector DBs", "Cooking"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(vids),
                Arc::new(uids),
                Arc::new(deleted),
                Arc::new(versions),
                Arc::new(StringArray::from(vec![None::<&str>; 2])), // ext_id
                // _labels
                {
                    let mut lb = arrow_array::builder::ListBuilder::new(
                        arrow_array::builder::StringBuilder::new(),
                    );
                    for _ in 0..2 {
                        lb.values().append_value("Paper");
                        lb.append(true);
                    }
                    Arc::new(lb.finish())
                },
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 2]).with_timezone("UTC")), // _created_at
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 2]).with_timezone("UTC")), // _updated_at
                Arc::new(vectors),
                Arc::new(titles),
                Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 2])), // overflow_json
            ],
        )?;
        dataset
            .write_batch(storage.backend(), batch, &schema_manager.schema())
            .await?;
    }

    // 3. Insert Data: Authors
    // Note: Author VID must be different from Paper VIDs for global uniqueness.
    {
        let dataset = storage.vertex_dataset("Author")?;
        let schema = dataset.get_arrow_schema(&schema_manager.schema())?;

        let a1 = Vid::new(100); // Author VID (unique from Papers)
        let vids = UInt64Array::from(vec![a1.as_u64()]);
        let versions = UInt64Array::from(vec![1]);
        let deleted = arrow_array::BooleanArray::from(vec![false]);

        // UIDs
        let mut uid_builder = FixedSizeBinaryBuilder::new(32);
        uid_builder.append_value([0u8; 32]).unwrap();
        let uids = uid_builder.finish();

        // Names
        let names = StringArray::from(vec!["Alice"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(vids),
                Arc::new(uids),
                Arc::new(deleted),
                Arc::new(versions),
                Arc::new(StringArray::from(vec![None::<&str>; 1])), // ext_id
                // _labels
                {
                    let mut lb = arrow_array::builder::ListBuilder::new(
                        arrow_array::builder::StringBuilder::new(),
                    );
                    lb.values().append_value("Author");
                    lb.append(true);
                    Arc::new(lb.finish())
                },
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 1]).with_timezone("UTC")), // _created_at
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 1]).with_timezone("UTC")), // _updated_at
                Arc::new(names),
                Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 1])), // overflow_json
            ],
        )?;
        dataset
            .write_batch(storage.backend(), batch, &schema_manager.schema())
            .await?;
    }

    // 4. Insert Edge: Alice (Author:100) -> WROTE -> Vector DBs (Paper:0)
    {
        let src_vid = Vid::new(100); // Author VID
        let dst_vid = Vid::new(0); // Paper VID
        let eid = Eid::new(1);

        // Forward: Author -> Paper
        {
            let adj_ds = storage.adjacency_dataset("WROTE", "Author", "fwd")?;
            let schema = adj_ds.get_arrow_schema();

            let src_vids = UInt64Array::from(vec![src_vid.as_u64()]);

            let mut neighbors_builder = ListBuilder::new(UInt64Builder::new());
            neighbors_builder.values().append_value(dst_vid.as_u64());
            neighbors_builder.append(true);
            let neighbors = neighbors_builder.finish();

            let mut eids_builder = ListBuilder::new(UInt64Builder::new());
            eids_builder.values().append_value(eid.as_u64());
            eids_builder.append(true);
            let eids = eids_builder.finish();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(src_vids), Arc::new(neighbors), Arc::new(eids)],
            )?;

            adj_ds.write_chunk(storage.backend(), batch).await?;
        }

        // Backward: Paper -> Author
        {
            let adj_ds = storage.adjacency_dataset("WROTE", "Paper", "bwd")?;
            let schema = adj_ds.get_arrow_schema();

            let src_vids = UInt64Array::from(vec![dst_vid.as_u64()]); // Source is Paper

            let mut neighbors_builder = ListBuilder::new(UInt64Builder::new());
            neighbors_builder.values().append_value(src_vid.as_u64()); // Neighbor is Author
            neighbors_builder.append(true);
            let neighbors = neighbors_builder.finish();

            let mut eids_builder = ListBuilder::new(UInt64Builder::new());
            eids_builder.values().append_value(eid.as_u64());
            eids_builder.append(true);
            let eids = eids_builder.finish();

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(src_vids), Arc::new(neighbors), Arc::new(eids)],
            )?;

            adj_ds.write_chunk(storage.backend(), batch).await?;
        }
    }

    // Warm the adjacency cache for both directions
    use uni_db::storage::direction::Direction as CacheDir;
    let wrote_edge_type_id = schema_manager
        .schema()
        .edge_type_id_by_name("WROTE")
        .unwrap();
    storage
        .warm_adjacency(wrote_edge_type_id, CacheDir::Outgoing, None)
        .await?;
    storage
        .warm_adjacency(wrote_edge_type_id, CacheDir::Incoming, None)
        .await?;

    // 5. Run Query
    // We must start MATCH with the bound variable 'p' for the current planner to pick it up without Scan.
    let query_sql = "
        CALL uni.vector.query('Paper', 'embedding', [0.1, 0.1], 1) YIELD p, dist
        MATCH (p)<-[:WROTE]-(a:Author)
        RETURN a.name, p.title, dist
    ";

    let query = uni_cypher::parse(query_sql)?;

    let planner = QueryPlanner::new(schema_manager.schema_arc());
    let plan = planner.plan(query)?;

    let executor = Executor::new(storage.clone());
    let results = executor
        .execute(plan, &prop_manager, &std::collections::HashMap::new())
        .await?;

    // 6. Verify
    assert_eq!(results.len(), 1);
    let row = &results[0];

    let name = row.get("a.name").unwrap().as_str().unwrap();
    assert_eq!(name, "Alice");

    let title = row.get("p.title").unwrap().as_str().unwrap();
    assert_eq!(title, "Vector DBs");

    let dist = row.get("dist").unwrap().as_f64().unwrap();
    assert!(dist < 0.001);

    Ok(())
}

trait SchemaManagerExt {
    fn schema_arc(&self) -> Arc<uni_db::core::schema::Schema>;
}
impl SchemaManagerExt for Arc<SchemaManager> {
    fn schema_arc(&self) -> Arc<uni_db::core::schema::Schema> {
        // This is a hack because QueryPlanner needs Arc<Schema> and SchemaManager has &Schema.
        // QueryPlanner takes Arc<Schema>.
        // SchemaManager::schema() now returns Arc<Schema> (Phase 4 Part B).
        self.schema()
    }
}

// ---------------------------------------------------------------------------
// Two-way (dense vector + FTS) `uni.search` fusion — high-level ergonomic API.
//
// Mirrors the three-way `setup_hybrid`/`run_hybrid` in `sparse_scoring.rs` but
// omits the sparse arm: `{vector, fts}` only. Asserts both branch scores
// (`vector_score`, `fts_score`) surface and the dual-relevant doc ranks first
// under RRF and weighted fusion.
// ---------------------------------------------------------------------------

/// `Doc(title, content, embedding)` + FTS on `content`, with a 2-doc corpus.
/// `target` matches both the query vector `[0.9, 0.1]` and the FTS term `zebra`;
/// `other` matches neither.
async fn setup_two_way(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: $t, content: $c, embedding: [0.9, 0.1]})")
        .param("t", Value::String("target".into()))
        .param("c", Value::String("zebra stripes pattern".into()))
        .run()
        .await?;
    tx.execute_with("CREATE (:Doc {title: $t, content: $c, embedding: [0.1, 0.9]})")
        .param("t", Value::String("other".into()))
        .param("c", Value::String("a quiet meadow".into()))
        .run()
        .await?;
    tx.commit().await?;

    let tx2 = db.session().tx().await?;
    tx2.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.content]")
        .await?;
    tx2.commit().await?;

    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;
    Ok(())
}

/// Run a two-way `uni.search` with the given fusion `options` and return
/// `(title, score, vector_score, fts_score)` rows in engine order.
async fn run_two_way(
    db: &Uni,
    options: &str,
) -> anyhow::Result<Vec<(String, f64, Option<f64>, Option<f64>)>> {
    let cypher = format!(
        "CALL uni.search('Doc', {{vector: 'embedding', fts: 'content'}}, \
         'zebra', $qvec, 5, null, {options}) \
         YIELD node, score, vector_score, fts_score \
         RETURN node.title AS title, score, vector_score, fts_score"
    );
    let rows = db
        .session()
        .query_with(&cypher)
        .param(
            "qvec",
            Value::List(vec![Value::Float(0.9), Value::Float(0.1)]),
        )
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
                r.get::<f64>("vector_score").ok(),
                r.get::<f64>("fts_score").ok(),
            )
        })
        .collect())
}

#[tokio::test]
async fn hybrid_two_way_rrf_ranks_dual_relevant_first() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    setup_two_way(&db).await?;

    let results = run_two_way(&db, "{method: 'rrf'}").await?;
    assert!(!results.is_empty(), "hybrid returned no rows");

    // `target` is the unique vector+FTS maximizer → ranked first under RRF.
    assert_eq!(
        results[0].0, "target",
        "target should rank first under RRF: {results:?}"
    );
    // Both branch scores surface for the target row.
    let target = results
        .iter()
        .find(|(t, ..)| t == "target")
        .expect("target present");
    assert!(
        target.2.is_some(),
        "vector_score populated for target: {target:?}"
    );
    assert!(
        target.3.is_some(),
        "fts_score populated for target: {target:?}"
    );
    Ok(())
}

#[tokio::test]
async fn hybrid_two_way_weighted_favors_vector() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    setup_two_way(&db).await?;

    // alpha=0.7 favors the vector branch; target wins on both branches anyway.
    let results = run_two_way(&db, "{method: 'weighted', alpha: 0.7}").await?;
    assert!(!results.is_empty(), "hybrid returned no rows");
    assert_eq!(
        results[0].0, "target",
        "target should rank first under weighted fusion: {results:?}"
    );
    Ok(())
}

#[tokio::test]
async fn hybrid_two_way_dbsf_ranks_dual_relevant_first() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    setup_two_way(&db).await?;

    // Distribution-Based Score Fusion: target is the dual maximizer, so it has
    // the highest z-score in both the (sign-flipped) vector and the fts arm.
    let results = run_two_way(&db, "{method: 'dbsf'}").await?;
    assert!(!results.is_empty(), "hybrid returned no rows");
    assert_eq!(
        results[0].0, "target",
        "target should rank first under DBSF: {results:?}"
    );
    Ok(())
}

#[tokio::test]
async fn hybrid_two_way_relative_score_ranks_dual_relevant_first() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    setup_two_way(&db).await?;

    // Weaviate-style relative-score fusion (min-max per source, weighted sum).
    // The `rsf` alias must resolve to the same path.
    for method in ["relative_score", "rsf"] {
        let results = run_two_way(&db, &format!("{{method: '{method}'}}")).await?;
        assert!(!results.is_empty(), "hybrid returned no rows for {method}");
        assert_eq!(
            results[0].0, "target",
            "target should rank first under {method}: {results:?}"
        );
    }
    Ok(())
}
