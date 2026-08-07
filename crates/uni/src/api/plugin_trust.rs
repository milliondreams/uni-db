//! Host-configured plugin trust policy.
//!
//! Plugin trust (the signature-enforcement policy plus the trust root of
//! allowed signing keys) is a per-instance, host-level concern — not a
//! per-plugin capability and not part of the serializable `UniConfig`.
//! `TrustRoot` (`uni_plugin::verify::TrustRoot`) holds raw Ed25519 public
//! keys and is deliberately neither `Clone` nor `Serialize`, so it cannot
//! live in `UniConfig` (which is cloned into every session). It also can
//! not live in `uni-common` without a dependency cycle (`uni-plugin`
//! depends on `uni-common`).
//!
//! Instead it is a builder-level runtime object, mirroring
//! [`WriteLease`](crate::api::multi_agent::WriteLease): set via
//! [`UniBuilder::plugin_trust`](crate::UniBuilder::plugin_trust), stored on
//! `UniInner`, and consulted at every plugin-load site.

use std::collections::BTreeSet;
use std::sync::Arc;

use uni_plugin::verify::{SignaturePolicy, TrustRoot};

/// Host policy governing which plugins may load.
///
/// The default is back-compatible: [`SignaturePolicy::Disabled`] with an
/// empty trust root, which accepts every plugin without inspecting its
/// signature — identical to pre-trust-config behavior.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use uni_db::api::plugin_trust::PluginTrustConfig;
/// use uni_plugin::verify::{SignaturePolicy, TrustRoot};
///
/// let mut root = TrustRoot::new();
/// root.allow_with_key("release-2026", [0u8; 32]);
/// let trust = PluginTrustConfig {
///     signature_policy: SignaturePolicy::RequireSigned,
///     trust_root: Arc::new(root),
/// };
/// // let db = Uni::open("./db").plugin_trust(trust).build().await?;
/// # let _ = trust;
/// ```
#[derive(Clone, Debug)]
pub struct PluginTrustConfig {
    /// How an unsigned (or invalidly-signed) manifest is treated.
    pub signature_policy: SignaturePolicy,
    /// Allowed signing keys. `Arc` because [`TrustRoot`] is not `Clone`
    /// and the config is shared across `at_snapshot`/`at_fork` clones.
    pub trust_root: Arc<TrustRoot>,
    /// Blake3 hex digests of the plugin artifacts this instance may load.
    ///
    /// Empty (the default) disables artifact pinning. When non-empty, every
    /// byte-carrying loader entry point — WASM component, Extism, Rhai,
    /// PyO3 — rejects a payload whose digest is absent from the set.
    ///
    /// This is the *externally supplied* pin, which is the only kind that
    /// carries security weight: a digest embedded in the artifact's own
    /// manifest can be rewritten alongside the payload it describes.
    pub pinned_artifacts: BTreeSet<String>,
}

impl Default for PluginTrustConfig {
    fn default() -> Self {
        Self {
            signature_policy: SignaturePolicy::Disabled,
            trust_root: Arc::new(TrustRoot::new()),
            pinned_artifacts: BTreeSet::new(),
        }
    }
}

impl PluginTrustConfig {
    /// Construct from a policy and an already-populated trust root.
    #[must_use]
    pub fn new(signature_policy: SignaturePolicy, trust_root: TrustRoot) -> Self {
        Self {
            signature_policy,
            trust_root: Arc::new(trust_root),
            pinned_artifacts: BTreeSet::new(),
        }
    }

    /// Enforce the signature policy on a plugin manifest.
    ///
    /// Reuses [`uni_plugin::verify::verify_manifest_with_policy`]: under
    /// [`SignaturePolicy::Disabled`] (the default) this is a no-op; under
    /// `RequireSigned` an unsigned manifest, an untrusted key, or a bad
    /// signature is rejected.
    ///
    /// Content hash-pinning is a separate check: see
    /// [`Self::enforce_artifact_pin`], which every byte-carrying loader entry
    /// point calls with the payload before instantiating it.
    ///
    /// # Errors
    ///
    /// Forwards [`uni_plugin::PluginError`] — a missing-required signature,
    /// an untrusted key, or an invalid signature.
    pub fn enforce(
        &self,
        manifest: &uni_plugin::PluginManifest,
    ) -> Result<(), uni_plugin::PluginError> {
        uni_plugin::verify::verify_manifest_with_policy(
            manifest,
            &self.trust_root,
            self.signature_policy,
        )
    }

    /// Enforce the artifact hash-pin allowlist against a plugin payload.
    ///
    /// A no-op when [`Self::pinned_artifacts`] is empty (the default). When it
    /// is populated, the payload's Blake3 digest must appear in the set, so an
    /// instance can be restricted to a known-good list of plugin binaries
    /// regardless of what the artifacts claim about themselves.
    ///
    /// Called by every loader entry point that receives payload bytes, before
    /// the payload is instantiated or any capability is granted.
    ///
    /// # Errors
    ///
    /// Returns [`uni_plugin::PluginError::HashMismatch`] when the payload's
    /// digest is not in the allowlist.
    pub fn enforce_artifact_pin(&self, payload: &[u8]) -> Result<(), uni_plugin::PluginError> {
        uni_plugin::verify::verify_payload_in_allowlist(&self.pinned_artifacts, payload)
    }
}
