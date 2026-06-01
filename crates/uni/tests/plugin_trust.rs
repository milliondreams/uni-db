//! Host plugin trust-policy enforcement on the compile-time `add_plugin`
//! path (Gap B foundation).
//!
//! These assert the *wiring*: the builder's `plugin_trust` config threads
//! into `UniInner` and is enforced when a plugin is installed. The
//! cryptographic accept-path (a valid Ed25519 signature against a trust
//! root) is unit-tested in `uni_plugin::verify`.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use uni_db::Uni;
use uni_db::api::plugin_trust::PluginTrustConfig;
use uni_plugin::verify::{SignaturePolicy, TrustRoot};
use uni_plugin::{
    AbiRange, CapabilitySet, Determinism, Plugin, PluginError, PluginManifest, PluginRegistrar,
    ProvidedSurfaces, Scope, SideEffects,
};

/// A no-op plugin that registers nothing — enough to exercise the trust
/// gate, which runs before `register`. `signature: None` makes it unsigned.
struct UnsignedNoopPlugin {
    manifest: OnceLock<PluginManifest>,
}

impl UnsignedNoopPlugin {
    fn new() -> Self {
        Self {
            manifest: OnceLock::new(),
        }
    }
}

impl Plugin for UnsignedNoopPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(|| PluginManifest {
            id: uni_plugin::PluginId::new("test.unsigned"),
            version: "0.1.0".parse().expect("static version"),
            abi: AbiRange::parse("^1").expect("static abi"),
            depends_on: vec![],
            capabilities: CapabilitySet::new(),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "trust-policy test plugin (unsigned)".to_owned(),
            metadata: BTreeMap::new(),
        })
    }

    fn register(&self, _r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        Ok(())
    }
}

#[tokio::test]
async fn default_policy_accepts_unsigned_plugin() -> anyhow::Result<()> {
    // Default builder => SignaturePolicy::Disabled => back-compat: accept.
    let db = Uni::temporary().build().await?;
    db.add_plugin(UnsignedNoopPlugin::new())?;
    assert!(
        db.plugin(&uni_plugin::PluginId::new("test.unsigned"))
            .is_some(),
        "unsigned plugin should install under the default Disabled policy"
    );
    Ok(())
}

#[tokio::test]
async fn require_signed_rejects_unsigned_plugin() -> anyhow::Result<()> {
    let trust = PluginTrustConfig::new(SignaturePolicy::RequireSigned, TrustRoot::new());
    let db = Uni::temporary().plugin_trust(trust).build().await?;
    let err = db
        .add_plugin(UnsignedNoopPlugin::new())
        .expect_err("RequireSigned must reject an unsigned plugin");
    let msg = err.to_string();
    assert!(
        msg.contains("signature") || msg.contains("RequireSigned") || msg.contains("Signature"),
        "error should mention the missing signature, got: {msg}"
    );
    assert!(
        db.plugin(&uni_plugin::PluginId::new("test.unsigned"))
            .is_none(),
        "rejected plugin must not be installed"
    );
    Ok(())
}

#[tokio::test]
async fn warn_if_unsigned_accepts_unsigned_plugin() -> anyhow::Result<()> {
    let trust = PluginTrustConfig::new(SignaturePolicy::WarnIfUnsigned, TrustRoot::new());
    let db = Uni::temporary().plugin_trust(trust).build().await?;
    db.add_plugin(UnsignedNoopPlugin::new())?;
    assert!(
        db.plugin(&uni_plugin::PluginId::new("test.unsigned"))
            .is_some(),
        "WarnIfUnsigned should accept (and warn about) an unsigned plugin"
    );
    Ok(())
}
