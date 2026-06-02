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
}

impl Default for PluginTrustConfig {
    fn default() -> Self {
        Self {
            signature_policy: SignaturePolicy::Disabled,
            trust_root: Arc::new(TrustRoot::new()),
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
        }
    }

    /// Enforce the signature policy on a plugin manifest.
    ///
    /// Reuses [`uni_plugin::verify::verify_manifest_with_policy`]: under
    /// [`SignaturePolicy::Disabled`] (the default) this is a no-op; under
    /// `RequireSigned` an unsigned manifest, an untrusted key, or a bad
    /// signature is rejected.
    ///
    /// Content hash-pinning ([`verify_hash_pin`](uni_plugin::verify::verify_hash_pin))
    /// is applied separately at load sites that have the plugin payload
    /// bytes the `hash` field covers.
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
}
