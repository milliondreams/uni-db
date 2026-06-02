//! Manifest signing and hash-pinning verification.
//!
//! Production deployments can require:
//!
//! - **Ed25519 signed manifests** — the manifest's `signature` field is
//!   verified against a trust root (configured per Uni instance).
//! - **Blake3 hash pinning** — the manifest's `hash` field must match
//!   a hash recorded at first install; reloads must reproduce.
//!
//! Ed25519 signature verification is always compiled — it is a security
//! primitive, so it is deliberately not a build-time opt-out. The signature
//! covers the whole manifest (see `canonical_payload`), not just the hash
//! pin, which closes a manifest-substitution attack: rewriting `capabilities`
//! or `side_effects` while preserving the hash invalidates the signature.

use crate::errors::PluginError;
use crate::manifest::PluginManifest;

#[cfg(test)]
use crate::manifest::ManifestSignature;

/// Verify a plugin's hash-pin against the payload bytes.
///
/// The manifest's `hash` field, if present, must equal `blake3(payload)`
/// in hex. Returns `Ok(())` if there is no pin (the manifest opted out)
/// or if the pin matches.
///
/// # Errors
///
/// Returns [`PluginError::HashMismatch`] when the pin is set and the
/// computed hash differs.
pub fn verify_hash_pin(manifest: &PluginManifest, payload: &[u8]) -> Result<(), PluginError> {
    let Some(expected_hex) = manifest.hash.as_ref() else {
        return Ok(());
    };
    let actual = blake3::hash(payload);
    let actual_hex = actual.to_hex().to_string();
    if !constant_time_eq(expected_hex, &actual_hex) {
        return Err(PluginError::HashMismatch {
            expected: expected_hex.clone(),
            actual: actual_hex,
        });
    }
    Ok(())
}

/// Verify a manifest's Ed25519 signature against the trust root.
///
/// Validates the signature algorithm, confirms the `key_id` is in the trust
/// root, then cryptographically verifies the signature over the manifest's
/// `canonical_payload` using the trust-root public key. An unsigned manifest
/// passes — whether that is acceptable is the caller's policy decision (see
/// [`verify_manifest_with_policy`]).
///
/// The verifier is fail-closed: a `key_id` present in the trust root but
/// without bound public-key bytes (the [`TrustRoot::allow`] shape-only path) is
/// rejected rather than waved through.
///
/// # Errors
///
/// Returns [`PluginError::SignatureInvalid`] when the signature's `algorithm`
/// is not `"ed25519"`, the `key_id` is not in the trust root, the trust-root
/// entry has no public-key bytes, the manifest cannot be canonicalized, or the
/// cryptographic check fails.
pub fn verify_signed_manifest(
    manifest: &PluginManifest,
    trust_root: &TrustRoot,
) -> Result<(), PluginError> {
    let Some(sig) = manifest.signature.as_ref() else {
        // No signature; whether this is allowed depends on the
        // host's `require_signed_plugins` configuration. The verifier
        // doesn't enforce that policy — the caller does.
        return Ok(());
    };
    // `algorithm` is matched explicitly so the verifier is algorithm-agile:
    // a future scheme adds an arm here without weakening the ed25519 path.
    if sig.algorithm != "ed25519" {
        return Err(PluginError::SignatureInvalid(format!(
            "unsupported algorithm `{}`",
            sig.algorithm
        )));
    }
    if !trust_root.contains(&sig.key_id) {
        return Err(PluginError::SignatureInvalid(format!(
            "key `{}` not in trust root",
            sig.key_id
        )));
    }
    let public_key_bytes = trust_root.public_key(&sig.key_id).ok_or_else(|| {
        PluginError::SignatureInvalid(format!(
            "trust root for key `{}` has no public key bytes",
            sig.key_id
        ))
    })?;
    let signing_payload = canonical_payload(manifest)?;
    verify_ed25519(public_key_bytes, &signing_payload, &sig.value)
}

/// Domain-separation tag + format version prefixed to every signing payload.
///
/// Binding the signature to a tagged, versioned payload prevents cross-protocol
/// signature reuse and gives a clean upgrade path: a future `:v2` encoding can
/// never be confused with a `:v1` one. Changing this value invalidates every
/// existing signature, so only bump the version suffix when the canonical
/// encoding below changes.
const MANIFEST_SIG_DOMAIN_V1: &[u8] = b"uni-plugin-manifest-sig:v1\0";

/// Build the canonical bytes covered by a manifest's Ed25519 signature.
///
/// The payload is [`MANIFEST_SIG_DOMAIN_V1`] followed by the manifest
/// serialized as JSON with its own `signature` field cleared. Clearing the
/// signature avoids the self-reference paradox (a signature cannot cover
/// itself), and every other field — `id`, `version`, `abi`, `capabilities`,
/// `side_effects`, `determinism`, `scope`, `hash`, … — is included, so a
/// manifest-substitution attack that preserves only the `hash` pin no longer
/// verifies.
///
/// Serialization routes through [`serde_json::Value`] so object keys are
/// emitted in sorted order independent of struct field declaration order: the
/// encoding (and therefore existing signatures) stays stable across field
/// reorderings. Determinism holds because every map in the manifest is a
/// `BTreeMap` / `BTreeSet` and every list preserves authored order.
///
/// # Errors
///
/// Returns [`PluginError::SignatureInvalid`] if the manifest cannot be
/// serialized to JSON (not expected for a well-formed manifest).
fn canonical_payload(manifest: &PluginManifest) -> Result<Vec<u8>, PluginError> {
    let mut unsigned = manifest.clone();
    unsigned.signature = None;
    let value = serde_json::to_value(&unsigned).map_err(|e| {
        PluginError::SignatureInvalid(format!("manifest canonicalization failed: {e}"))
    })?;
    let json = serde_json::to_vec(&value).map_err(|e| {
        PluginError::SignatureInvalid(format!("manifest canonicalization failed: {e}"))
    })?;
    let mut bytes = Vec::with_capacity(MANIFEST_SIG_DOMAIN_V1.len() + json.len());
    bytes.extend_from_slice(MANIFEST_SIG_DOMAIN_V1);
    bytes.extend_from_slice(&json);
    Ok(bytes)
}

fn verify_ed25519(
    public_key_bytes: &[u8; 32],
    payload: &[u8],
    signature_b64: &str,
) -> Result<(), PluginError> {
    use base64::Engine;
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};

    let key = VerifyingKey::from_bytes(public_key_bytes)
        .map_err(|e| PluginError::SignatureInvalid(format!("malformed ed25519 public key: {e}")))?;
    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(signature_b64.as_bytes())
        .map_err(|e| PluginError::SignatureInvalid(format!("signature base64: {e}")))?;
    let sig = Signature::from_slice(&sig_bytes)
        .map_err(|e| PluginError::SignatureInvalid(format!("signature parse: {e}")))?;
    key.verify(payload, &sig)
        .map_err(|e| PluginError::SignatureInvalid(format!("ed25519 verify failed: {e}")))?;
    Ok(())
}

/// Trust root for plugin signature verification.
///
/// Configured per-Uni-instance from secure storage (KMS, config file).
#[derive(Debug, Default)]
pub struct TrustRoot {
    /// `key_id → Option<public-key-bytes>` — the bytes are populated
    /// when the `ed25519` feature is enabled and the trust root is
    /// configured with real public-key material.
    allowed_keys: std::collections::BTreeMap<String, Option<[u8; 32]>>,
}

impl TrustRoot {
    /// Construct an empty trust root (rejects every signed manifest).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an allowed key id without binding public-key bytes.
    ///
    /// Useful for tests / shape-only verification. Real builds (with the
    /// `ed25519` feature) should use [`TrustRoot::allow_with_key`].
    pub fn allow(&mut self, key_id: impl Into<String>) {
        self.allowed_keys.insert(key_id.into(), None);
    }

    /// Add an allowed key with its 32-byte Ed25519 public key.
    pub fn allow_with_key(&mut self, key_id: impl Into<String>, public_key: [u8; 32]) {
        self.allowed_keys.insert(key_id.into(), Some(public_key));
    }

    /// Check whether `key_id` is in the trust root.
    #[must_use]
    pub fn contains(&self, key_id: &str) -> bool {
        self.allowed_keys.contains_key(key_id)
    }

    /// Return the 32-byte public key for `key_id`, if known.
    #[must_use]
    pub fn public_key(&self, key_id: &str) -> Option<&[u8; 32]> {
        self.allowed_keys.get(key_id).and_then(|k| k.as_ref())
    }
}

/// Host policy for plugin signature enforcement.
///
/// Wraps [`verify_signed_manifest`] with a "should an unsigned manifest
/// be accepted?" decision so the host can dial enforcement up over time
/// without changing call sites.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SignaturePolicy {
    /// Skip signature checks entirely. Default for v1 back-compat —
    /// unsigned and signed manifests both pass without inspection.
    #[default]
    Disabled,
    /// Verify when a signature is present; log a warning when absent.
    WarnIfUnsigned,
    /// Reject any manifest without a valid signature.
    RequireSigned,
}

/// Apply [`SignaturePolicy`] on top of [`verify_signed_manifest`].
///
/// `Disabled` short-circuits without inspecting the manifest.
/// `WarnIfUnsigned` runs the verifier and emits a `tracing::warn` when
/// the manifest has no signature. `RequireSigned` runs the verifier and
/// converts an absent signature into [`PluginError::SignatureInvalid`].
///
/// # Errors
///
/// Forwards every error from [`verify_signed_manifest`]. Additionally
/// returns [`PluginError::SignatureInvalid`] when the policy requires a
/// signature and the manifest has none.
pub fn verify_manifest_with_policy(
    manifest: &PluginManifest,
    trust_root: &TrustRoot,
    policy: SignaturePolicy,
) -> Result<(), PluginError> {
    match policy {
        SignaturePolicy::Disabled => Ok(()),
        SignaturePolicy::WarnIfUnsigned => {
            if manifest.signature.is_none() {
                tracing::warn!(
                    plugin_id = %manifest.id.as_str(),
                    "plugin manifest has no signature; accepted under WarnIfUnsigned policy",
                );
            }
            verify_signed_manifest(manifest, trust_root)
        }
        SignaturePolicy::RequireSigned => {
            if manifest.signature.is_none() {
                return Err(PluginError::SignatureInvalid(format!(
                    "plugin `{}` has no manifest signature; RequireSigned policy rejects it",
                    manifest.id.as_str()
                )));
            }
            verify_signed_manifest(manifest, trust_root)
        }
    }
}

/// Constant-time string equality.
///
/// Hash-pins are *not* secrets, but constant-time comparison is cheap
/// and defends against the future case where the same primitive is
/// reused for HMAC tags.
fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (ai, bi) in a.bytes().zip(b.bytes()) {
        diff |= ai ^ bi;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::AbiRange;
    use crate::plugin::PluginId;
    use crate::{Determinism, Scope, SideEffects};
    use semver::Version;

    fn empty_manifest() -> PluginManifest {
        PluginManifest {
            id: PluginId::new("test"),
            version: Version::new(0, 1, 0),
            abi: AbiRange::parse("^1").unwrap(),
            depends_on: vec![],
            capabilities: crate::CapabilitySet::new(),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: crate::ProvidedSurfaces::default(),
            docs: String::new(),
            metadata: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn hash_pin_passes_when_unpinned() {
        let m = empty_manifest();
        assert!(verify_hash_pin(&m, b"anything").is_ok());
    }

    #[test]
    fn hash_pin_passes_with_correct_hash() {
        let mut m = empty_manifest();
        let payload = b"hello world";
        m.hash = Some(blake3::hash(payload).to_hex().to_string());
        assert!(verify_hash_pin(&m, payload).is_ok());
    }

    #[test]
    fn hash_pin_fails_with_wrong_hash() {
        let mut m = empty_manifest();
        m.hash = Some(blake3::hash(b"a").to_hex().to_string());
        match verify_hash_pin(&m, b"b") {
            Err(PluginError::HashMismatch { expected, actual }) => {
                assert!(!expected.is_empty());
                assert!(!actual.is_empty());
                assert_ne!(expected, actual);
            }
            other => panic!("expected HashMismatch, got {other:?}"),
        }
    }

    #[test]
    fn signature_verification_rejects_unknown_key_id() {
        let mut m = empty_manifest();
        m.signature = Some(ManifestSignature {
            algorithm: "ed25519".to_owned(),
            key_id: "ops@example.com".to_owned(),
            value: "base64...".to_owned(),
        });
        let tr = TrustRoot::new();
        assert!(verify_signed_manifest(&m, &tr).is_err());
    }

    /// **M11 cutover end-to-end test**: real Ed25519 sign + verify
    /// through `verify_signed_manifest`. Exercises the full flow: build
    /// a manifest with hash-pin, sign the canonical payload with a
    /// trust-root key, base64-encode the signature into
    /// `manifest.signature.value`, and call `verify_signed_manifest`.
    /// With `ed25519` default-on, this performs real crypto.
    #[test]
    fn verify_signed_manifest_real_ed25519_round_trip() {
        use base64::Engine;
        use ed25519_dalek::{Signer, SigningKey};

        let seed: [u8; 32] = [
            0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec,
            0x2c, 0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03,
            0x1c, 0xae, 0x7f, 0x60,
        ];
        let signing_key = SigningKey::from_bytes(&seed);
        let public_key_bytes: [u8; 32] = signing_key.verifying_key().to_bytes();

        let mut m = empty_manifest();
        m.hash = Some(blake3::hash(b"plugin payload").to_hex().to_string());

        let payload = canonical_payload(&m).expect("canonicalize");
        let sig = signing_key.sign(&payload);
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        m.signature = Some(ManifestSignature {
            algorithm: "ed25519".to_owned(),
            key_id: "ops@example.com".to_owned(),
            value: sig_b64,
        });

        let mut tr = TrustRoot::new();
        tr.allow_with_key("ops@example.com", public_key_bytes);

        // Real cryptographic verification — passes only if the signature
        // is valid over the canonical payload.
        verify_signed_manifest(&m, &tr).expect("real Ed25519 verify must succeed");

        // Tampering with the manifest's hash invalidates the signature.
        m.hash = Some(blake3::hash(b"different payload").to_hex().to_string());
        assert!(
            verify_signed_manifest(&m, &tr).is_err(),
            "tampered manifest must fail verification"
        );
    }

    /// Regression for the manifest-substitution attack: the signature must
    /// cover security-relevant fields, not just the hash pin. Mutating
    /// `capabilities` while leaving `hash` unchanged must fail verification.
    /// This is exactly the case the old hash-only payload accepted.
    #[test]
    fn verify_rejects_capability_substitution() {
        use base64::Engine;
        use ed25519_dalek::{Signer, SigningKey};

        let seed: [u8; 32] = [
            0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec,
            0x2c, 0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03,
            0x1c, 0xae, 0x7f, 0x60,
        ];
        let signing_key = SigningKey::from_bytes(&seed);
        let public_key_bytes: [u8; 32] = signing_key.verifying_key().to_bytes();

        // Sign a read-only manifest with a fixed hash pin.
        let mut m = empty_manifest();
        m.hash = Some(blake3::hash(b"plugin payload").to_hex().to_string());
        let payload = canonical_payload(&m).expect("canonicalize");
        let sig_b64 =
            base64::engine::general_purpose::STANDARD.encode(signing_key.sign(&payload).to_bytes());
        m.signature = Some(ManifestSignature {
            algorithm: "ed25519".to_owned(),
            key_id: "ops@example.com".to_owned(),
            value: sig_b64,
        });

        let mut tr = TrustRoot::new();
        tr.allow_with_key("ops@example.com", public_key_bytes);
        verify_signed_manifest(&m, &tr).expect("baseline signed manifest must verify");

        // Attacker escalates capabilities + side-effects but keeps the hash
        // pin (and signature) identical. Must now be rejected.
        m.capabilities.insert(crate::Capability::ProcedureWrites);
        m.side_effects = SideEffects::Writes;
        assert!(
            verify_signed_manifest(&m, &tr).is_err(),
            "capability substitution under a constant hash must fail verification"
        );
    }

    /// Fail-closed: a `key_id` present in the trust root but with no bound
    /// public-key bytes (the shape-only `allow()` path) must be rejected, not
    /// waved through.
    #[test]
    fn verify_fails_closed_without_public_key_bytes() {
        let mut m = empty_manifest();
        m.signature = Some(ManifestSignature {
            algorithm: "ed25519".to_owned(),
            key_id: "ops@example.com".to_owned(),
            value: "AAAA".to_owned(),
        });
        let mut tr = TrustRoot::new();
        tr.allow("ops@example.com"); // membership only, no key bytes
        match verify_signed_manifest(&m, &tr) {
            Err(PluginError::SignatureInvalid(msg)) => {
                assert!(msg.contains("no public key bytes"), "msg: {msg}");
            }
            other => panic!("expected fail-closed SignatureInvalid, got {other:?}"),
        }
    }

    #[test]
    fn signature_with_unknown_algorithm_is_rejected() {
        let mut m = empty_manifest();
        m.signature = Some(ManifestSignature {
            algorithm: "rsa".to_owned(),
            key_id: "any".to_owned(),
            value: String::new(),
        });
        let mut tr = TrustRoot::new();
        tr.allow("any");
        assert!(verify_signed_manifest(&m, &tr).is_err());
    }

    #[test]
    fn unsigned_manifest_passes_signature_verifier() {
        let m = empty_manifest();
        let tr = TrustRoot::new();
        assert!(verify_signed_manifest(&m, &tr).is_ok());
    }

    #[test]
    fn policy_disabled_skips_verification() {
        // A manifest with a bogus signature still passes when the host
        // has signature enforcement disabled.
        let mut m = empty_manifest();
        m.signature = Some(ManifestSignature {
            algorithm: "rsa".to_owned(),
            key_id: "unknown".to_owned(),
            value: String::new(),
        });
        let tr = TrustRoot::new();
        assert!(verify_manifest_with_policy(&m, &tr, SignaturePolicy::Disabled).is_ok());
    }

    #[test]
    fn policy_require_signed_rejects_unsigned_manifest() {
        let m = empty_manifest();
        let tr = TrustRoot::new();
        let err = verify_manifest_with_policy(&m, &tr, SignaturePolicy::RequireSigned)
            .expect_err("RequireSigned must reject unsigned manifest");
        match err {
            PluginError::SignatureInvalid(msg) => {
                assert!(msg.contains("no manifest signature"), "msg: {msg}");
            }
            other => panic!("expected SignatureInvalid, got {other:?}"),
        }
    }

    #[test]
    fn policy_warn_if_unsigned_passes_unsigned_manifest() {
        let m = empty_manifest();
        let tr = TrustRoot::new();
        assert!(verify_manifest_with_policy(&m, &tr, SignaturePolicy::WarnIfUnsigned).is_ok());
    }

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "ab"));
    }

    /// End-to-end ed25519 signing + verification round-trip.
    ///
    /// Uses `ed25519-dalek` directly (a dev-dep) to sign the canonical
    /// payload, populates `TrustRoot` with the public key bytes,
    /// and verifies — proving the M11 cryptographic path works.
    ///
    /// This test exercises the raw `ed25519-dalek` round-trip arithmetic
    /// directly; the `verify_signed_manifest` integration is covered by
    /// `verify_signed_manifest_real_ed25519_round_trip` above.
    #[test]
    fn ed25519_sign_and_verify_round_trip_manually() {
        use base64::Engine;
        use ed25519_dalek::{Signer, SigningKey};

        // Deterministic 32-byte seed → reproducible keypair. Avoids the
        // rand-version-skew dance between workspace rand (0.9) and
        // ed25519-dalek's rand_core (0.6) trait bound.
        let seed: [u8; 32] = [
            0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec,
            0x2c, 0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03,
            0x1c, 0xae, 0x7f, 0x60,
        ];
        let signing_key = SigningKey::from_bytes(&seed);
        let verifying_key = signing_key.verifying_key();
        let public_key_bytes: [u8; 32] = verifying_key.to_bytes();

        // Build a manifest with a hash-pin (the canonical-payload input).
        let mut m = empty_manifest();
        m.hash = Some(blake3::hash(b"plugin payload").to_hex().to_string());

        // Sign the canonical payload.
        let payload = canonical_payload(&m).expect("canonicalize");
        let sig = signing_key.sign(&payload);
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        // Verify via the M11 path using ed25519-dalek directly.
        // (When the `ed25519` cargo feature is enabled, `verify_signed_manifest`
        // performs this verification automatically; this test reproduces it
        // unconditionally to lock the protocol shape.)
        let key = ed25519_dalek::VerifyingKey::from_bytes(&public_key_bytes).unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(sig_b64.as_bytes())
            .unwrap();
        let parsed_sig = ed25519_dalek::Signature::from_slice(&decoded).unwrap();
        use ed25519_dalek::Verifier;
        assert!(key.verify(&payload, &parsed_sig).is_ok());

        // Tampered payload fails verification.
        let mut tampered = payload.clone();
        tampered[0] ^= 0xff;
        assert!(key.verify(&tampered, &parsed_sig).is_err());

        // TrustRoot stores the public key correctly.
        let mut tr = TrustRoot::new();
        tr.allow_with_key("ops@example.com", public_key_bytes);
        assert_eq!(tr.public_key("ops@example.com"), Some(&public_key_bytes));
    }
}
