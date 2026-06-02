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

use std::time::Duration;

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
pub trait HttpEgress: Send + Sync {
    /// Perform a blocking HTTP GET, reading at most `max_bytes` of the body.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on connection, timeout, or transport failure.
    fn get(&self, url: &str, timeout: Duration, max_bytes: usize) -> Result<HttpResponse, FnError>;

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
    ) -> Result<HttpResponse, FnError>;
}
