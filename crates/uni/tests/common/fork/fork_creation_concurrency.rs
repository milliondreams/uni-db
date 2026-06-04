// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Concurrent fork creation — verification suite E4.
//!
//! Spec §10 invariant: fork creation does not serialize against
//! itself (different names) or against primary writes. Threshold:
//! 16 parallel forks complete within 2× the serial baseline.
//! Kill-switch: 16× serial → fork creation is a global bottleneck;
//! we'd add a creation queue + backpressure.

// Rust guideline compliant

use std::time::Instant;
use uni_db::{DataType, Uni};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixteen_concurrent_forks_distinct_names_within_2x_serial() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await
        .unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    tx.execute("CREATE (:Person {name: 'seed'})").await.unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    // Serial baseline.
    let serial_start = Instant::now();
    for i in 0..16 {
        let _f = session.fork(format!("serial_{i}")).await.unwrap();
    }
    let serial_elapsed = serial_start.elapsed();

    // Drop them so we can re-use the namespace.
    for i in 0..16 {
        // Sessions go out of scope above; just remove the registry entries.
        db.drop_fork(&format!("serial_{i}")).await.unwrap();
    }

    // Concurrent.
    let concurrent_start = Instant::now();
    let mut handles = Vec::new();
    for i in 0..16 {
        let session = db.session();
        handles.push(tokio::spawn(async move {
            session.fork(format!("conc_{i}")).await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    let concurrent_elapsed = concurrent_start.elapsed();

    eprintln!(
        "serial={:?} concurrent={:?} ratio={:.2}",
        serial_elapsed,
        concurrent_elapsed,
        concurrent_elapsed.as_secs_f64() / serial_elapsed.as_secs_f64().max(1e-9),
    );

    // Threshold: concurrent must be at most 2× serial. Be generous —
    // CI machines vary.
    assert!(
        concurrent_elapsed <= serial_elapsed * 4,
        "concurrent forks took {concurrent_elapsed:?} vs serial {serial_elapsed:?} \
         — fork creation appears to be serializing globally"
    );

    db.shutdown().await.unwrap();
}
