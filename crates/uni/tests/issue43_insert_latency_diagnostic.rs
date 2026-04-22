// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Diagnostic test for issue #43: Auto-embed insert latency jumps 25x after ~150 rows.
//
// This test instruments the insert path to isolate whether the latency jump is caused by:
// 1. ONNX/FastEmbed runtime degradation after ~150 calls
// 2. Auto-flush (5-second interval) triggering flush_to_l1
// 3. Some interaction between the two
//
// Run with: cargo nextest run --features provider-fastembed --test issue43_insert_latency_diagnostic --run-ignored all --no-capture

#[cfg(feature = "provider-fastembed")]
mod tests {
    use anyhow::Result;
    use std::time::Instant;
    use uni_db::api::schema::{EmbeddingCfg, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};
    use uni_db::{DataType, ModelAliasSpec, ModelTask, Uni, UniConfig, WarmupPolicy};

    const NUM_INSERTS: usize = 200;
    const CONTENT_TEMPLATE: &str =
        "Message about business strategy and collaborative dance performance";

    fn fastembed_alias() -> ModelAliasSpec {
        ModelAliasSpec {
            alias: "embed/default".to_string(),
            task: ModelTask::Embed,
            provider_id: "local/fastembed".to_string(),
            model_id: "AllMiniLML6V2".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            options: serde_json::json!({}),
        }
    }

    async fn setup_schema(db: &Uni) -> Result<()> {
        db.schema()
            .label("Message")
            .property("content", DataType::String)
            .property_nullable("embedding", DataType::Vector { dimensions: 384 })
            .index(
                "embedding",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: Some(EmbeddingCfg {
                        alias: "embed/default".to_string(),
                        source_properties: vec!["content".to_string()],
                        batch_size: 32,
                    }),
                }),
            )
            .done()
            .apply()
            .await?;
        Ok(())
    }

    fn report(latencies: &[u64]) {
        let first_50_avg = latencies[1..50].iter().sum::<u64>() / 49; // skip cold start
        let last_50_avg =
            latencies[150..].iter().sum::<u64>() / (latencies.len() - 150) as u64;
        let ratio = last_50_avg as f64 / first_50_avg.max(1) as f64;

        eprintln!("---");
        eprintln!("Cold start (insert 0):  {}ms", latencies[0]);
        eprintln!("First 50 avg (1-49):    {}ms", first_50_avg);
        eprintln!("Last 50 avg (150-199):  {}ms", last_50_avg);
        eprintln!("Ratio: {:.1}x", ratio);
    }

    /// Test 1: Baseline — insert with auto-embed, default config.
    /// This should reproduce the 25x latency jump at ~150 rows.
    #[tokio::test]
    #[ignore]
    async fn issue43_baseline_auto_embed_latency() -> Result<()> {
        let db = Uni::temporary()
            .xervo_catalog(vec![fastembed_alias()])
            .build()
            .await?;
        setup_schema(&db).await?;

        eprintln!("\n=== Test 1: Baseline (default config, auto-embed ON) ===");
        eprintln!("auto_flush_interval: 5s (default)");
        eprintln!("---");

        let mut latencies = Vec::with_capacity(NUM_INSERTS);
        let session = db.session();

        for i in 0..NUM_INSERTS {
            let start = Instant::now();
            let tx = session.tx().await?;
            let content = format!("{} number {}", CONTENT_TEMPLATE, i);
            tx.execute_with("CREATE (:Message {content: $c})")
                .param("c", content)
                .run()
                .await?;
            tx.commit().await?;
            let elapsed = start.elapsed();
            latencies.push(elapsed.as_millis() as u64);

            if i % 20 == 0 || (i >= 140 && i <= 170) || i == NUM_INSERTS - 1 {
                eprintln!("insert {:>4}: {:>6}ms", i, elapsed.as_millis());
            }
        }

        report(&latencies);

        let ratio =
            latencies[150..].iter().sum::<u64>() as f64 / latencies[1..50].iter().sum::<u64>().max(1) as f64
                * 49.0
                / (latencies.len() - 150) as f64;
        if ratio > 5.0 {
            eprintln!("*** REPRODUCED: {:.1}x slowdown detected ***", ratio);
        } else {
            eprintln!("No significant slowdown detected (ratio {:.1}x)", ratio);
        }

        Ok(())
    }

    /// Test 2: Auto-flush DISABLED — isolates whether the flush triggers the jump.
    #[tokio::test]
    #[ignore]
    async fn issue43_no_auto_flush() -> Result<()> {
        let config = UniConfig {
            auto_flush_interval: None,
            auto_flush_threshold: 100_000,
            ..Default::default()
        };
        let db = Uni::temporary()
            .xervo_catalog(vec![fastembed_alias()])
            .config(config)
            .build()
            .await?;
        setup_schema(&db).await?;

        eprintln!("\n=== Test 2: Auto-flush DISABLED, auto-embed ON ===");
        eprintln!("---");

        let mut latencies = Vec::with_capacity(NUM_INSERTS);
        let session = db.session();

        for i in 0..NUM_INSERTS {
            let start = Instant::now();
            let tx = session.tx().await?;
            let content = format!("{} number {}", CONTENT_TEMPLATE, i);
            tx.execute_with("CREATE (:Message {content: $c})")
                .param("c", content)
                .run()
                .await?;
            tx.commit().await?;
            let elapsed = start.elapsed();
            latencies.push(elapsed.as_millis() as u64);

            if i % 20 == 0 || (i >= 140 && i <= 170) || i == NUM_INSERTS - 1 {
                eprintln!("insert {:>4}: {:>6}ms", i, elapsed.as_millis());
            }
        }

        report(&latencies);

        let first_50_avg = latencies[1..50].iter().sum::<u64>() / 49;
        let last_50_avg =
            latencies[150..].iter().sum::<u64>() / (latencies.len() - 150) as u64;
        let ratio = last_50_avg as f64 / first_50_avg.max(1) as f64;
        if ratio > 5.0 {
            eprintln!("*** JUMP STILL PRESENT without flush → NOT flush related ***");
        } else {
            eprintln!("*** JUMP GONE without flush → flush_to_l1 is the culprit ***");
        }

        Ok(())
    }

    /// Test 3: Early manual flush — flush after 50 inserts, then continue.
    #[tokio::test]
    #[ignore]
    async fn issue43_early_manual_flush() -> Result<()> {
        let config = UniConfig {
            auto_flush_interval: None,
            auto_flush_threshold: 100_000,
            ..Default::default()
        };
        let db = Uni::temporary()
            .xervo_catalog(vec![fastembed_alias()])
            .config(config)
            .build()
            .await?;
        setup_schema(&db).await?;

        eprintln!("\n=== Test 3: Manual flush at insert 50, auto-flush DISABLED ===");
        eprintln!("---");

        let mut latencies = Vec::with_capacity(NUM_INSERTS);
        let session = db.session();

        for i in 0..NUM_INSERTS {
            if i == 50 {
                eprintln!("--- MANUAL FLUSH at insert 50 ---");
                let flush_start = Instant::now();
                db.flush().await?;
                eprintln!("--- Flush took {}ms ---", flush_start.elapsed().as_millis());
            }

            let start = Instant::now();
            let tx = session.tx().await?;
            let content = format!("{} number {}", CONTENT_TEMPLATE, i);
            tx.execute_with("CREATE (:Message {content: $c})")
                .param("c", content)
                .run()
                .await?;
            tx.commit().await?;
            let elapsed = start.elapsed();
            latencies.push(elapsed.as_millis() as u64);

            if i % 20 == 0 || (i >= 48 && i <= 55) || (i >= 140 && i <= 170)
                || i == NUM_INSERTS - 1
            {
                eprintln!("insert {:>4}: {:>6}ms", i, elapsed.as_millis());
            }
        }

        let pre_flush_avg = latencies[1..50].iter().sum::<u64>() / 49;
        let post_flush_avg = latencies[51..100].iter().sum::<u64>() / 49;
        let late_avg =
            latencies[150..].iter().sum::<u64>() / (latencies.len() - 150) as u64;

        eprintln!("---");
        eprintln!("Pre-flush avg (1-49):    {}ms", pre_flush_avg);
        eprintln!("Post-flush avg (51-99):  {}ms", post_flush_avg);
        eprintln!("Late avg (150-199):      {}ms", late_avg);
        eprintln!(
            "Post-flush ratio: {:.1}x",
            post_flush_avg as f64 / pre_flush_avg.max(1) as f64
        );
        eprintln!(
            "Late ratio: {:.1}x",
            late_avg as f64 / pre_flush_avg.max(1) as f64
        );

        Ok(())
    }

    /// Test 6: Timed breakdown — measure execute vs commit separately.
    #[tokio::test]
    #[ignore]
    async fn issue43_timed_breakdown() -> Result<()> {
        let db = Uni::temporary()
            .xervo_catalog(vec![fastembed_alias()])
            .build()
            .await?;
        setup_schema(&db).await?;

        eprintln!("\n=== Test 6: Timed breakdown (execute vs commit) ===");
        eprintln!("---");

        let session = db.session();

        for i in 0..NUM_INSERTS {
            let tx = session.tx().await?;

            let exec_start = Instant::now();
            let content = format!("{} number {}", CONTENT_TEMPLATE, i);
            tx.execute_with("CREATE (:Message {content: $c})")
                .param("c", content)
                .run()
                .await?;
            let exec_ms = exec_start.elapsed().as_millis();

            let commit_start = Instant::now();
            tx.commit().await?;
            let commit_ms = commit_start.elapsed().as_millis();

            if i % 20 == 0 || (i >= 140 && i <= 170) || i == NUM_INSERTS - 1 {
                eprintln!(
                    "insert {:>4}: exec={:>6}ms  commit={:>6}ms  total={:>6}ms",
                    i,
                    exec_ms,
                    commit_ms,
                    exec_ms + commit_ms
                );
            }
        }

        Ok(())
    }

    /// Test 7: Force flush early — set low threshold to trigger flush after 50 mutations.
    /// This verifies that flush_to_l1 causes the latency jump regardless of insert count.
    #[tokio::test]
    #[ignore]
    async fn issue43_forced_early_flush() -> Result<()> {
        let config = UniConfig {
            auto_flush_threshold: 50,        // Flush after 50 mutations
            auto_flush_interval: None,       // Disable time-based flush
            ..Default::default()
        };
        let db = Uni::temporary()
            .xervo_catalog(vec![fastembed_alias()])
            .config(config)
            .build()
            .await?;
        setup_schema(&db).await?;

        eprintln!("\n=== Test 7: Forced early flush (threshold=50) ===");
        eprintln!("---");

        let mut latencies = Vec::with_capacity(NUM_INSERTS);
        let session = db.session();

        for i in 0..NUM_INSERTS {
            let start = Instant::now();
            let tx = session.tx().await?;
            let content = format!("{} number {}", CONTENT_TEMPLATE, i);
            tx.execute_with("CREATE (:Message {content: $c})")
                .param("c", content)
                .run()
                .await?;
            tx.commit().await?;
            let elapsed = start.elapsed();
            latencies.push(elapsed.as_millis() as u64);

            if i % 10 == 0 || (i >= 48 && i <= 55) || (i >= 98 && i <= 105)
                || i == NUM_INSERTS - 1
            {
                eprintln!("insert {:>4}: {:>6}ms", i, elapsed.as_millis());
            }
        }

        let pre_flush_avg = latencies[1..48].iter().sum::<u64>() / 47;
        let around_flush = latencies[48..55].iter().sum::<u64>() / 7;
        let post_flush_avg = latencies[55..95].iter().sum::<u64>() / 40;
        let second_flush = latencies[98..105].iter().sum::<u64>() / 7;
        let late_avg =
            latencies[110..].iter().sum::<u64>() / (latencies.len() - 110) as u64;

        eprintln!("---");
        eprintln!("Pre-flush avg (1-47):     {}ms", pre_flush_avg);
        eprintln!("Around flush (48-54):     {}ms", around_flush);
        eprintln!("Post-flush avg (55-94):   {}ms", post_flush_avg);
        eprintln!("Around 2nd flush (98-104):{}ms", second_flush);
        eprintln!("Late avg (110-199):       {}ms", late_avg);
        eprintln!(
            "Post-flush ratio: {:.1}x",
            post_flush_avg as f64 / pre_flush_avg.max(1) as f64
        );

        Ok(())
    }

    /// Test 8: Manual flush then measure — most direct test.
    /// Insert 50, flush, then measure next 50 inserts.
    #[tokio::test]
    #[ignore]
    async fn issue43_flush_then_measure() -> Result<()> {
        let config = UniConfig {
            auto_flush_interval: None,
            auto_flush_threshold: 100_000,
            ..Default::default()
        };
        let db = Uni::temporary()
            .xervo_catalog(vec![fastembed_alias()])
            .config(config)
            .build()
            .await?;
        setup_schema(&db).await?;

        eprintln!("\n=== Test 8: Insert 50 → flush → measure next 50 ===");
        eprintln!("---");

        let session = db.session();

        // Phase 1: Insert 50 nodes
        let mut phase1 = Vec::new();
        for i in 0..50 {
            let start = Instant::now();
            let tx = session.tx().await?;
            let content = format!("{} number {}", CONTENT_TEMPLATE, i);
            tx.execute_with("CREATE (:Message {content: $c})")
                .param("c", content)
                .run()
                .await?;
            tx.commit().await?;
            phase1.push(start.elapsed().as_millis() as u64);
        }
        let phase1_avg = phase1[1..].iter().sum::<u64>() / 49;
        eprintln!("Phase 1 (pre-flush) avg: {}ms", phase1_avg);

        // Flush
        eprintln!("--- FLUSHING ---");
        let flush_start = Instant::now();
        db.flush().await?;
        eprintln!("Flush took: {}ms", flush_start.elapsed().as_millis());

        // Phase 2: Insert 50 more nodes
        let mut phase2 = Vec::new();
        for i in 50..100 {
            let tx = session.tx().await?;

            let exec_start = Instant::now();
            let content = format!("{} number {}", CONTENT_TEMPLATE, i);
            tx.execute_with("CREATE (:Message {content: $c})")
                .param("c", content)
                .run()
                .await?;
            let exec_ms = exec_start.elapsed().as_millis();

            let commit_start = Instant::now();
            tx.commit().await?;
            let commit_ms = commit_start.elapsed().as_millis();

            let total = exec_ms + commit_ms;
            phase2.push(total as u64);

            eprintln!(
                "insert {:>4}: exec={:>4}ms  commit={:>4}ms  total={:>4}ms",
                i, exec_ms, commit_ms, total
            );
        }
        let phase2_avg = phase2.iter().sum::<u64>() / phase2.len() as u64;
        eprintln!("---");
        eprintln!("Phase 1 avg (pre-flush):  {}ms", phase1_avg);
        eprintln!("Phase 2 avg (post-flush): {}ms", phase2_avg);
        eprintln!(
            "Ratio: {:.1}x",
            phase2_avg as f64 / phase1_avg.max(1) as f64
        );

        Ok(())
    }
}

/// Test 4: No auto-embed — pure graph insert control.
#[tokio::test]
#[ignore]
async fn issue43_control_no_embed() -> anyhow::Result<()> {
    use std::time::Instant;
    use uni_db::Uni;

    const NUM_INSERTS: usize = 200;
    const CONTENT_TEMPLATE: &str =
        "Message about business strategy and collaborative dance performance";

    let db = Uni::temporary().build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Message (content STRING)").await?;
    tx.commit().await?;

    eprintln!("\n=== Test 4: Control — NO auto-embed, default config ===");
    eprintln!("---");

    let mut latencies = Vec::with_capacity(NUM_INSERTS);
    let session = db.session();

    for i in 0..NUM_INSERTS {
        let start = Instant::now();
        let tx = session.tx().await?;
        let content = format!("{} number {}", CONTENT_TEMPLATE, i);
        tx.execute_with("CREATE (:Message {content: $c})")
            .param("c", content)
            .run()
            .await?;
        tx.commit().await?;
        let elapsed = start.elapsed();
        latencies.push(elapsed.as_millis() as u64);

        if i % 20 == 0 || i == NUM_INSERTS - 1 {
            eprintln!("insert {:>4}: {:>6}ms", i, elapsed.as_millis());
        }
    }

    let first_50_avg = latencies[1..50].iter().sum::<u64>() / 49;
    let last_50_avg =
        latencies[150..].iter().sum::<u64>() / (latencies.len() - 150) as u64;
    let ratio = last_50_avg as f64 / first_50_avg.max(1) as f64;

    eprintln!("---");
    eprintln!("First 50 avg (1-49):   {}ms", first_50_avg);
    eprintln!("Last 50 avg (150-199): {}ms", last_50_avg);
    eprintln!("Ratio: {:.1}x (should be ~1.0)", ratio);

    Ok(())
}

/// Test 5: Embedding-only — call embed() 200 times without any DB operations.
#[cfg(feature = "provider-fastembed")]
#[tokio::test]
#[ignore]
async fn issue43_embed_only_no_db() -> anyhow::Result<()> {
    use std::time::Instant;
    use uni_db::{ModelAliasSpec, ModelTask, Uni, WarmupPolicy};

    const NUM_INSERTS: usize = 200;
    const CONTENT_TEMPLATE: &str =
        "Message about business strategy and collaborative dance performance";

    let db = Uni::temporary()
        .xervo_catalog(vec![ModelAliasSpec {
            alias: "embed/test".to_string(),
            task: ModelTask::Embed,
            provider_id: "local/fastembed".to_string(),
            model_id: "AllMiniLML6V2".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            options: serde_json::json!({}),
        }])
        .build()
        .await?;

    let xervo = db.xervo();

    eprintln!("\n=== Test 5: Embedding-only (no DB operations) ===");
    eprintln!("Model: AllMiniLML6V2 (384d)");
    eprintln!("---");

    let mut latencies = Vec::with_capacity(NUM_INSERTS);

    for i in 0..NUM_INSERTS {
        let text = format!("{} number {}", CONTENT_TEMPLATE, i);
        let start = Instant::now();
        let _result = xervo.embed("embed/test", &[text.as_str()]).await?;
        let elapsed = start.elapsed();
        latencies.push(elapsed.as_millis() as u64);

        if i % 20 == 0 || (i >= 140 && i <= 170) || i == NUM_INSERTS - 1 {
            eprintln!("embed {:>4}: {:>6}ms", i, elapsed.as_millis());
        }
    }

    let first_50_avg = latencies[1..50].iter().sum::<u64>() / 49;
    let last_50_avg =
        latencies[150..].iter().sum::<u64>() / (latencies.len() - 150) as u64;
    let ratio = last_50_avg as f64 / first_50_avg.max(1) as f64;

    eprintln!("---");
    eprintln!("Cold start (embed 0):   {}ms", latencies[0]);
    eprintln!("First 50 avg (1-49):    {}ms", first_50_avg);
    eprintln!("Last 50 avg (150-199):  {}ms", last_50_avg);
    eprintln!("Ratio: {:.1}x", ratio);

    if ratio > 3.0 {
        eprintln!("*** ORT/FastEmbed DEGRADES after ~150 calls ***");
    } else {
        eprintln!("ORT/FastEmbed is stable — issue is in the DB layer");
    }

    Ok(())
}
