//! Host service traits for capability-gated plugin host functions.
//!
//! `uni.kms.*` and `uni.http.*` host functions need a backing host service to
//! perform real work. These traits define that seam in the shared `uni-plugin`
//! crate so every loader (Rhai today; Extism / WASM at the host-fn cutover)
//! binds the *same* abstraction rather than each inventing its own. The host
//! supplies concrete implementations (e.g. a `reqwest`-backed [`HttpEgress`] in
//! `uni-plugin-host`) and hands them to the loader.
//!
//! Secret acquisition has no trait here — it reuses
//! [`crate::secrets::SecretStore`] directly.

use std::sync::Arc;
use std::time::Duration;

use crate::capability::{Capability, CapabilitySet};
use crate::errors::FnError;

/// A signing / verification service backing the `uni.kms.*` host functions.
///
/// Implementations are expected to enforce nothing about *which* key ids are
/// permissible — that attenuation is checked against the plugin's granted
/// [`crate::Capability::Kms`] before this trait is called.
pub trait KmsProvider: Send + Sync {
    /// Sign `data` with the key identified by `key_id`, returning the raw
    /// signature bytes.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the key is unknown or the signing operation
    /// fails.
    fn sign(&self, key_id: &str, data: &[u8]) -> Result<Vec<u8>, FnError>;

    /// Verify `signature` over `data` against the key identified by `key_id`.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the key is unknown or verification cannot be
    /// performed (a *valid* result of "signature does not match" is `Ok(false)`,
    /// not an error).
    fn verify(&self, key_id: &str, data: &[u8], signature: &[u8]) -> Result<bool, FnError>;
}

/// Response returned by an [`HttpEgress`] request.
#[derive(Clone, Debug)]
pub struct HttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response body, truncated to the caller's `max_bytes` limit.
    pub body: Vec<u8>,
}

/// A **blocking** HTTP egress service backing the `uni.http.*` host functions.
///
/// Methods are synchronous because the Rhai engine runs scripts synchronously
/// (inside DataFusion scalar/procedure execution). Implementations must be safe
/// to call from within a Tokio runtime context — e.g. by running the request on
/// a dedicated OS thread rather than blocking a Tokio worker. URL allow-listing,
/// timeout, and response-size limits are enforced by the caller against the
/// plugin's granted [`crate::Capability::Network`]; the `timeout` and
/// `max_bytes` arguments carry those decisions into the request.
///
/// `traceparent`, when `Some`, is injected as the W3C `traceparent` request
/// header so the host's trace context propagates across the plugin boundary
/// into the outbound call (see [`crate::observability::TraceContext::to_traceparent`]).
pub trait HttpEgress: Send + Sync {
    /// Perform a blocking HTTP GET, reading at most `max_bytes` of the body.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on connection, timeout, or transport failure.
    fn get(
        &self,
        url: &str,
        timeout: Duration,
        max_bytes: usize,
        traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError>;

    /// Perform a blocking HTTP POST of `body`, reading at most `max_bytes` of
    /// the response body.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on connection, timeout, or transport failure.
    fn post(
        &self,
        url: &str,
        body: &[u8],
        timeout: Duration,
        max_bytes: usize,
        traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError>;
}

// ---------------------------------------------------------------------------
// Shared `uni.http.*` policy
// ---------------------------------------------------------------------------

/// Default per-call HTTP timeout when the grant carries no
/// [`Capability::WallClockMillisPerCall`].
///
/// Conservative: long enough for a typical API call, short enough to bound a
/// wedged request.
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum response body bytes read before truncation — bounds host memory so a
/// hostile or oversized response cannot exhaust it.
pub const MAX_HTTP_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Why a capability-gated HTTP call was refused.
///
/// The loaders share the *decisions* and keep their own *encoding*: the Extism
/// loader maps these onto the numeric `FnError` codes its guest ABI pins
/// (`0xC20`, `0xC21`, `0xC23`), the Rhai loader onto `EvalAltResult` strings.
/// Splitting it this way is what lets both share the policy without either
/// changing its published error contract.
#[derive(Debug)]
pub enum HttpPolicyError {
    /// The URL is outside the granted [`Capability::Network`] allow-list.
    NotAllowed,
    /// No [`HttpEgress`] implementation was wired in.
    NoEgress,
    /// The transport itself failed.
    Transport(FnError),
    /// The response carried a `>= 400` status.
    Status(u16),
}

/// Resolve the per-call HTTP timeout from the granted capabilities.
///
/// The first [`Capability::WallClockMillisPerCall`] in the set wins; absent
/// one, [`DEFAULT_HTTP_TIMEOUT`].
#[must_use]
pub fn resolve_http_timeout(caps: &CapabilitySet) -> Duration {
    caps.iter()
        .find_map(|c| match c {
            Capability::WallClockMillisPerCall(ms) => Some(Duration::from_millis(*ms)),
            _ => None,
        })
        .unwrap_or(DEFAULT_HTTP_TIMEOUT)
}

/// Run a capability-gated HTTP request: allow-list check, egress presence,
/// timeout resolution, dispatch, then the `>= 400` status gate.
///
/// `body` present selects POST, absent selects GET. `traceparent` is the host's
/// active W3C trace context, threaded through as a parameter rather than read
/// from ambient state so the dispatch stays unit-testable.
///
/// # Errors
///
/// Returns [`HttpPolicyError`] for each refusal reason; see its variants.
pub fn http_request(
    egress: Option<&Arc<dyn HttpEgress>>,
    caps: &CapabilitySet,
    url: &str,
    body: Option<&[u8]>,
    traceparent: Option<&str>,
) -> Result<HttpResponse, HttpPolicyError> {
    if !caps.iter().any(|c| c.network_allows(url)) {
        return Err(HttpPolicyError::NotAllowed);
    }
    let egress = egress.ok_or(HttpPolicyError::NoEgress)?;
    let timeout = resolve_http_timeout(caps);

    let response = match body {
        Some(b) => egress.post(url, b, timeout, MAX_HTTP_RESPONSE_BYTES, traceparent),
        None => egress.get(url, timeout, MAX_HTTP_RESPONSE_BYTES, traceparent),
    }
    .map_err(HttpPolicyError::Transport)?;

    if response.status >= 400 {
        return Err(HttpPolicyError::Status(response.status));
    }
    Ok(response)
}
