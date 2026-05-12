// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression test for issue #49: Auto-embed insert latency grows O(n) when
// label has a DateTime property.
//
// Run with: cargo nextest run --features provider-onnx --test issue49_datetime_autoembed_latency --run-ignored all --no-capture

#[cfg(feature = "provider-onnx")]
mod tests {
    use std::time::Instant;
    use uni_db::api::schema::EmbeddingCfg;
    use uni_db::{
        DataType, IndexType, ModelAliasSpec, ModelTask, Uni, Value, VectorAlgo, VectorIndexCfg,
        VectorMetric, WarmupPolicy,
    };

    const NUM_INSERTS: usize = 200;
    const TEXT: &str = "I went to a LGBTQ support group yesterday and it was so powerful. \
         The transgender stories were so inspiring and I felt really accepted.";

    #[tokio::test]
    #[ignore]
    async fn autoembed_datetime_causes_on_growth() {
        let db = Uni::in_memory()
            .xervo_catalog(vec![ModelAliasSpec {
                alias: "embed/default".into(),
                task: ModelTask::Embed,
                provider_id: "local/onnx".into(),
                model_id: "NomicEmbedTextV15".into(),
                revision: None,
                warmup: WarmupPolicy::Lazy,
                required: false,
                timeout: None,
                load_timeout: None,
                retry: None,
                options: serde_json::json!({}),
            }])
            .build()
            .await
            .unwrap();

        db.schema()
            .label("Message")
            .property("message_id", DataType::String)
            .property("content", DataType::String)
            .property("timestamp", DataType::DateTime)
            .property_nullable("embedding", DataType::Vector { dimensions: 768 })
            .index("content", IndexType::FullText)
            .index(
                "embedding",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::HnswSq {
                        m: 16,
                        ef_construction: 100,
                        partitions: None,
                    },
                    metric: VectorMetric::Cosine,
                    embedding: Some(EmbeddingCfg {
                        alias: "embed/default".into(),
                        source_properties: vec!["content".into()],
                        batch_size: 32,
                        document_prefix: Some("search_document: ".into()),
                        query_prefix: Some("search_query: ".into()),
                    }),
                }),
            )
            .done()
            .apply()
            .await
            .unwrap();

        let session = db.session();
        let mut latencies_ms = Vec::with_capacity(NUM_INSERTS);

        for i in 0..NUM_INSERTS {
            let tx = session.tx().await.unwrap();
            let start = Instant::now();
            tx.execute_with("CREATE (:Message {message_id: $p0, content: $p1, timestamp: $p2})")
                .param("p0", Value::String(format!("msg-{i:04}")))
                .param("p1", Value::String(TEXT.into()))
                .param("p2", Value::String(chrono::Utc::now().to_rfc3339()))
                .run()
                .await
                .unwrap();
            tx.commit().await.unwrap();
            latencies_ms.push(start.elapsed().as_millis() as f64);
        }

        let avg = |s: &[f64]| s.iter().sum::<f64>() / s.len() as f64;
        let first = avg(&latencies_ms[..20]);
        let last = avg(&latencies_ms[NUM_INSERTS - 20..]);
        let ratio = last / first.max(1.0);

        eprintln!("first 20 avg: {first:.1}ms, last 20 avg: {last:.1}ms, ratio: {ratio:.2}x");
        db.shutdown().await.unwrap();

        assert!(ratio < 2.0, "grew {ratio:.1}x with DateTime property");
    }
}
