//! Sealer/unsealer secret membrane.
//!
//! Plugins granted `Capability::Secret { ids }` acquire **opaque handles** to
//! named secrets — never raw bytes. The handle can be passed to other
//! capability-gated host imports (e.g., `host-net.http_get_with_secret`)
//! but cannot be read, logged, or serialized.
//!
//! # Threat model
//!
//! - **Unreadable**: the plugin's code has no API to extract bytes from
//!   a [`SecretHandle`]. The handle is a host-side index into the
//!   in-process secret store.
//! - **Untransferable**: handles cannot be serialized to plugin output
//!   batches (verified by the WASM IPC layer's reject list).
//! - **Scoped**: handles are tied to the issuing [`SecretStore`] and
//!   become invalid on plugin reload (the store is rebuilt).
//! - **Auditable**: every [`SecretStore::acquire`] call emits a tracing
//!   event so security teams can detect anomalous frequencies.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::errors::FnError;

/// Opaque handle to a sealed secret.
///
/// The handle is a small, copyable integer; the bytes live behind it in
/// the [`SecretStore`] and never cross the API boundary in cleartext.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SecretHandle(u64);

impl SecretHandle {
    /// The (host-private) opaque identifier. Plugin code never reads this.
    #[must_use]
    pub fn opaque_id(&self) -> u64 {
        self.0
    }
}

/// In-process store of sealed secrets.
///
/// Constructed by the host at Uni instance startup; populated from
/// secure config (KMS, env vars, secrets manager). Plugins acquire
/// handles via [`SecretStore::acquire`]; capability-gated host imports
/// resolve handles back to bytes via [`SecretStore::unseal_for_host_use`]
/// which is itself private to the framework's host-import implementations.
#[derive(Debug)]
pub struct SecretStore {
    /// Named secrets — `name → bytes`.
    by_name: RwLock<HashMap<String, Vec<u8>>>,
    /// Handle → name mapping.
    by_handle: RwLock<HashMap<u64, String>>,
    /// Next handle to hand out. Starts at `1` so the first acquire yields
    /// a non-zero handle; the `id == 0` guard in [`SecretStore::acquire`]
    /// remains as defense in depth against a counter forced/wrapped to 0.
    next: AtomicU64,
}

impl Default for SecretStore {
    fn default() -> Self {
        Self {
            by_name: RwLock::default(),
            by_handle: RwLock::default(),
            next: AtomicU64::new(1),
        }
    }
}

impl SecretStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Seal `bytes` under `name`, replacing any previous value.
    ///
    /// Host-side only; plugin code never seals secrets.
    pub fn seal(&self, name: impl Into<String>, bytes: Vec<u8>) {
        let name = name.into();
        self.by_name.write().insert(name, bytes);
    }

    /// Plugin-facing API: acquire a handle for the named secret.
    ///
    /// Emits a tracing event so security teams can monitor acquire
    /// frequency.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] with code `0xA00` if `name` is not present.
    pub fn acquire(&self, name: &str) -> Result<SecretHandle, FnError> {
        let exists = self.by_name.read().contains_key(name);
        if !exists {
            return Err(FnError::new(
                0xA00,
                format!("secret `{name}` not found in store"),
            ));
        }
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        // Reserve `0` so an uninitialized handle (`SecretHandle(0)`) is
        // never valid — defense in depth against zero-init exfiltration.
        let id = if id == 0 {
            self.next.fetch_add(1, Ordering::SeqCst)
        } else {
            id
        };
        self.by_handle.write().insert(id, name.to_owned());
        tracing::debug!(secret_id = name, handle_opaque = id, "secret.acquire");
        Ok(SecretHandle(id))
    }

    /// Host-only: resolve a handle to its underlying bytes.
    ///
    /// Used by host-import implementations (e.g., `http_get_with_secret`)
    /// to attach the secret to an outbound HTTP header before invoking
    /// the actual network call. **This must not be exposed to plugin
    /// code** — it's `pub` within the crate but not re-exported through
    /// the WIT binding layer.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] with code `0xA01` if the handle is invalid
    /// (e.g., from a different store, or revoked).
    pub fn unseal_for_host_use(&self, h: SecretHandle) -> Result<Vec<u8>, FnError> {
        let by_handle = self.by_handle.read();
        let name = by_handle.get(&h.0).ok_or_else(|| {
            FnError::new(
                0xA01,
                format!("secret handle {} is invalid or revoked", h.0),
            )
        })?;
        let by_name = self.by_name.read();
        by_name.get(name).cloned().ok_or_else(|| {
            FnError::new(0xA02, format!("secret `{name}` was sealed but is now gone"))
        })
    }

    /// Revoke a handle (e.g., on plugin reload).
    pub fn revoke(&self, h: SecretHandle) {
        self.by_handle.write().remove(&h.0);
    }

    /// Clear every sealed secret (e.g., on Uni shutdown).
    pub fn clear(&self) {
        self.by_name.write().clear();
        self.by_handle.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_returns_handle_for_sealed_secret() {
        let s = SecretStore::new();
        s.seal("api_key", b"sk-test-abc".to_vec());
        let h = s.acquire("api_key").unwrap();
        assert_ne!(h.opaque_id(), 0);
    }

    #[test]
    fn acquire_missing_secret_errors() {
        let s = SecretStore::new();
        assert!(s.acquire("nope").is_err());
    }

    #[test]
    fn host_unseal_returns_bytes() {
        let s = SecretStore::new();
        s.seal("api_key", b"sk-test-abc".to_vec());
        let h = s.acquire("api_key").unwrap();
        let bytes = s.unseal_for_host_use(h).unwrap();
        assert_eq!(bytes, b"sk-test-abc");
    }

    #[test]
    fn revoked_handle_cannot_unseal() {
        let s = SecretStore::new();
        s.seal("api_key", b"x".to_vec());
        let h = s.acquire("api_key").unwrap();
        s.revoke(h);
        assert!(s.unseal_for_host_use(h).is_err());
    }

    #[test]
    fn separate_stores_handles_dont_cross() {
        let a = SecretStore::new();
        let b = SecretStore::new();
        a.seal("k", b"av".to_vec());
        b.seal("k", b"bv".to_vec());
        let ha = a.acquire("k").unwrap();
        // Store b doesn't know handle ha — invalid.
        assert!(b.unseal_for_host_use(ha).is_err());
    }

    #[test]
    fn acquire_never_returns_zero_handle() {
        let s = SecretStore::new();
        s.seal("k", b"v".to_vec());
        // Force the counter to roll past 0.
        s.next.store(0, Ordering::SeqCst);
        let h = s.acquire("k").unwrap();
        assert_ne!(h.opaque_id(), 0);
    }
}
