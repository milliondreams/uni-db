// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! OpenTelemetry tracing-subscriber layer for `uni-db`.
//!
//! Per proposal §12.1.1, plugin spans should propagate alongside the
//! host's `tracing` events so a query trace shows up as one continuous
//! span tree in Jaeger / Tempo / Datadog. This module exposes a single
//! initialization helper that constructs a [`tracing_subscriber::Registry`]
//! with the [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry)
//! layer wrapping the standard `fmt` layer.
//!
//! ## Why a helper, not auto-install?
//!
//! Embedders frequently bring their own `tracing` subscriber (server
//! frameworks like axum/tower-http, test harnesses, Python bindings).
//! Auto-installing a global subscriber from `Uni::open` would conflict
//! with those setups and produce the runtime panic
//! "a global default trace dispatcher has already been set". The
//! conservative shape: ship the helper, let embedders opt in.
//!
//! Inside the host, the [`uni_plugin::observability::record_invocation`]
//! function emits `tracing::debug!` events tagged with `kind` / `qname` /
//! plugin id. With the OTel layer installed those events become OTLP
//! spans automatically.
//!
//! ## Usage
//!
//! ```no_run
//! use uni_plugin_host::observability::OtelConfig;
//!
//! let cfg = OtelConfig {
//!     service_name: "my-app".into(),
//!     otlp_endpoint: "http://localhost:4317".into(),
//! };
//! let _guard = uni_plugin_host::observability::init_otel_subscriber(cfg)
//!     .expect("OTel subscriber must initialize once");
//! // ... use Uni normally; events become OTLP spans.
//! ```

// Rust guideline compliant

use std::error::Error as StdError;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Configuration for the OTel tracing subscriber.
///
/// Construct directly with literal fields; no builder is needed at this
/// shape.
#[derive(Clone, Debug)]
pub struct OtelConfig {
    /// `service.name` resource attribute reported to the collector.
    pub service_name: String,
    /// OTLP-gRPC endpoint, e.g. `"http://localhost:4317"`.
    pub otlp_endpoint: String,
}

/// Initialize a `tracing-subscriber::Registry` with an OTel layer
/// pointing at the OTLP endpoint described by `cfg`.
///
/// Returns a [`OtelGuard`] whose `Drop` impl shuts the tracer provider
/// down cleanly. Calls
/// [`tracing_subscriber::util::SubscriberInitExt::try_init`] under the
/// hood — passing the global default subscriber lock through to
/// `tracing-subscriber` semantics.
///
/// # Errors
///
/// Returns an error if the tracer provider cannot be constructed
/// (bad endpoint, missing TLS material) or if a global subscriber has
/// already been installed.
pub fn init_otel_subscriber(cfg: OtelConfig) -> Result<OtelGuard, Box<dyn StdError>> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&cfg.otlp_endpoint)
        .build()?;
    let resource = Resource::builder()
        .with_service_name(cfg.service_name.clone())
        .build();
    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();
    let tracer = provider.tracer("uni-db");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .try_init()?;

    Ok(OtelGuard { provider })
}

/// RAII guard that flushes and shuts down the OTel tracer provider on
/// drop. Keep the returned value alive for the lifetime of the
/// process; dropping it tears down the OTel pipeline.
#[derive(Debug)]
pub struct OtelGuard {
    provider: SdkTracerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        // Best-effort: the SDK's shutdown is idempotent and never
        // panics; if the collector is unreachable, the shutdown just
        // times out internally.
        let _ = self.provider.shutdown();
    }
}

// ── FU-3: trace context extraction + outbound HTTP injection ──────

/// W3C `traceparent` header value extracted from the current
/// `tracing` span, formatted as `00-<trace_id>-<span_id>-<flags>`.
///
/// Returns `None` when no `tracing-opentelemetry` layer is installed
/// (the current span has no associated `SpanContext`). Used by
/// outbound HTTP request paths to propagate the trace across a
/// process boundary — e.g., when a plugin invokes `http-get-with-trace`
/// via the host-net WIT import.
#[must_use]
pub fn current_traceparent() -> Option<String> {
    // Delegates to the single source of truth in `uni-plugin` (built with the
    // `otel` feature) so the host-side outbound-HTTP path and the plugin ABI
    // share one extraction + formatting implementation.
    uni_plugin::observability::current_trace_context().to_traceparent()
}

/// Perform an HTTP GET against `url` with the current span's
/// `traceparent` header injected (FU-3).
///
/// Used by the host's outbound-HTTP request path (and by the
/// `examples/otel_demo` binary) to demonstrate end-to-end trace
/// propagation: `Session::query → plugin span → outbound HTTP`. The
/// receiving server sees a `traceparent` header whose `trace_id`
/// matches the outer query span.
///
/// # Errors
///
/// Returns an error string on any HTTP transport / status failure.
pub async fn http_get_with_traceparent(url: &str) -> Result<Vec<u8>, String> {
    let client = reqwest::Client::new();
    let mut req = client.get(url);
    if let Some(tp) = current_traceparent() {
        req = req.header("traceparent", tp);
    }
    let resp = req.send().await.map_err(|e| format!("send: {e}"))?;
    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("read body: {e}"))?
        .to_vec();
    if !status.is_success() {
        return Err(format!("HTTP {status}: {} bytes", bytes.len()));
    }
    Ok(bytes)
}
