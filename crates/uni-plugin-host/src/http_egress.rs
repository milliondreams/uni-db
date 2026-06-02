// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Blocking HTTP egress for capability-gated plugin host functions.
//!
//! Implements [`uni_plugin::HttpEgress`] with `reqwest::blocking`. The Rhai
//! engine runs scripts synchronously inside DataFusion scalar/procedure
//! execution, which is itself driven on Tokio worker threads — and
//! `reqwest::blocking` panics if used from within a Tokio runtime context. So
//! each request runs on a freshly-spawned OS thread (via [`std::thread::scope`])
//! that carries no Tokio context; the calling thread blocks on its join. URL
//! allow-listing is enforced by the caller against the plugin's granted
//! [`uni_plugin::Capability::Network`]; this layer only honors the `timeout`
//! and `max_bytes` it is handed.

// Rust guideline compliant

use std::io::Read as _;
use std::time::Duration;

use uni_plugin::{FnError, HttpEgress, HttpResponse};

/// FnError code: HTTP client could not be constructed.
const ERR_CLIENT_BUILD: u32 = 0xB00;
/// FnError code: transport / send / read failure (connection, timeout, body).
const ERR_TRANSPORT: u32 = 0xB01;
/// FnError code: the request worker thread panicked.
const ERR_WORKER_PANIC: u32 = 0xB02;

/// `reqwest::blocking`-backed [`HttpEgress`] safe to call from async contexts.
#[derive(Debug, Default, Clone)]
pub struct BlockingHttpEgress;

impl BlockingHttpEgress {
    /// Construct a new egress service.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl HttpEgress for BlockingHttpEgress {
    fn get(
        &self,
        url: &str,
        timeout: Duration,
        max_bytes: usize,
        traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        run_on_dedicated_thread(url, None, timeout, max_bytes, traceparent)
    }

    fn post(
        &self,
        url: &str,
        body: &[u8],
        timeout: Duration,
        max_bytes: usize,
        traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        run_on_dedicated_thread(url, Some(body), timeout, max_bytes, traceparent)
    }
}

/// Run a blocking request on a dedicated OS thread (no inherited Tokio context),
/// blocking the caller until it completes.
fn run_on_dedicated_thread(
    url: &str,
    body: Option<&[u8]>,
    timeout: Duration,
    max_bytes: usize,
    traceparent: Option<&str>,
) -> Result<HttpResponse, FnError> {
    std::thread::scope(|scope| {
        let handle = scope.spawn(|| do_request(url, body, timeout, max_bytes, traceparent));
        match handle.join() {
            Ok(result) => result,
            Err(_) => Err(FnError::new(
                ERR_WORKER_PANIC,
                "http request worker thread panicked",
            )),
        }
    })
}

/// Perform one blocking request, reading at most `max_bytes` of the body.
fn do_request(
    url: &str,
    body: Option<&[u8]>,
    timeout: Duration,
    max_bytes: usize,
    traceparent: Option<&str>,
) -> Result<HttpResponse, FnError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| FnError::new(ERR_CLIENT_BUILD, format!("http client build: {e}")))?;
    let mut request = match body {
        Some(b) => client.post(url).body(b.to_vec()),
        None => client.get(url),
    };
    // Propagate the host's trace context across the plugin boundary when present.
    if let Some(tp) = traceparent {
        request = request.header("traceparent", tp);
    }
    let response = request
        .send()
        .map_err(|e| FnError::new(ERR_TRANSPORT, format!("http send `{url}`: {e}")))?;
    let status = response.status().as_u16();
    // Bound the read so a hostile/large response can't exhaust memory: read one
    // byte past the limit only to know nothing is silently dropped, then
    // truncate to the cap.
    let mut buf = Vec::new();
    let cap = (max_bytes as u64).saturating_add(1);
    response
        .take(cap)
        .read_to_end(&mut buf)
        .map_err(|e| FnError::new(ERR_TRANSPORT, format!("http body `{url}`: {e}")))?;
    buf.truncate(max_bytes);
    Ok(HttpResponse { status, body: buf })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_default() {
        let _ = BlockingHttpEgress::new();
        let _ = BlockingHttpEgress;
    }

    #[test]
    fn invalid_url_is_transport_error_not_panic() {
        // No network and a bogus scheme: must surface as a transport FnError,
        // and — critically — must not panic even though this test harness runs
        // under a Tokio-capable context.
        let egress = BlockingHttpEgress::new();
        let err = egress
            .get(
                "http://127.0.0.1:1/",
                Duration::from_millis(200),
                1024,
                None,
            )
            .expect_err("connection to a dead port must fail");
        assert_eq!(err.code, ERR_TRANSPORT);
    }
}
