// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M11 FU-3 end-to-end OTel propagation demo.
//!
//! Run: `cargo run --manifest-path examples/otel-demo/Cargo.toml`.
//!
//! 1. Install a `tracing-opentelemetry` layer backed by an in-memory
//!    tracer (no OTLP collector required).
//! 2. Start a local capture server that prints every `traceparent`
//!    header it sees.
//! 3. Wrap a synthetic "query" in `tracing::info_span!("query")` and
//!    call [`uni_db::observability::http_get_with_traceparent`] from
//!    inside the span.
//! 4. Expected output:
//!    - the outer span's trace-id (extracted via `current_traceparent`)
//!    - the server-side `traceparent` header
//!    - confirmation that the two share the same trace-id

// Rust guideline compliant

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uni_db::observability::{current_traceparent, http_get_with_traceparent};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    // Step 1: install the OTel layer with an in-memory tracer.
    let provider = SdkTracerProvider::builder().build();
    let tracer = provider.tracer("otel-demo");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(otel_layer)
        .try_init()
        .ok();

    // Step 2: start the capture server on an ephemeral port.
    let (addr_tx, addr_rx) = oneshot::channel::<SocketAddr>();
    let captured = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let server_captured = Arc::clone(&captured);
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _ = addr_tx.send(addr);
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let captured = Arc::clone(&server_captured);
            tokio::spawn(async move {
                let (read, mut write) = sock.split();
                let mut reader = BufReader::new(read);
                let mut line = String::new();
                let mut tp: Option<String> = None;
                loop {
                    line.clear();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        break;
                    }
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some(rest) = line.to_ascii_lowercase().strip_prefix("traceparent:") {
                        tp = Some(rest.trim().to_owned());
                    }
                }
                if let Some(t) = tp {
                    captured.lock().unwrap().push(t);
                }
                let _ = write
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")
                    .await;
            });
        }
    });
    let addr = addr_rx.await?;
    let url = format!("http://{}", addr);

    // Step 3: run the synthetic "query" inside an info_span.
    let outer_span = tracing::info_span!("query", cypher = "RETURN demo()");
    let outer_trace_id = {
        let _g = outer_span.enter();
        current_traceparent().unwrap_or_else(|| "<no-otel-trace>".to_owned())
    };
    let body = async {
        let _bytes = http_get_with_traceparent(&url).await.unwrap();
    };
    body.instrument(outer_span).await;

    // Give the server a moment to record.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Step 4: report.
    let seen = captured.lock().unwrap().clone();
    println!("Outer query span traceparent: {outer_trace_id}");
    println!("Capture server saw {} traceparent header(s):", seen.len());
    for tp in &seen {
        println!("  {tp}");
    }
    let outer_trace = outer_trace_id
        .split('-')
        .nth(1)
        .unwrap_or("");
    let matched = seen.iter().any(|h| {
        h.split('-').nth(1).map(|t| t == outer_trace).unwrap_or(false)
    });
    if matched {
        println!(
            "PASS: outbound HTTP shared the outer span's trace-id ({outer_trace})"
        );
    } else {
        println!(
            "FAIL: outbound HTTP did not propagate the outer span's trace-id"
        );
        std::process::exit(1);
    }
    Ok(())
}
