//! KMS host fns — gated by [`Capability::Kms`].
//!
//! `uni_kms_sign(key_id, data) -> string` and
//! `uni_kms_verify(key_id, data, sig) -> bool` dispatch to the loader's
//! [`KmsProvider`]. Signatures cross the Rhai boundary as lowercase hex.
//! Call-time attenuation matches `key_id` against the granted
//! `Capability::Kms { key_ids }` allow-list; a missing provider or an
//! out-of-list key errors loudly.

#![cfg(feature = "rhai-runtime")]

use std::fmt::Write as _;
use std::sync::Arc;

use rhai::Engine;
use uni_plugin::{Capability, CapabilitySet, KmsProvider};

use crate::host_fn_impls::{require_allowed, require_service, rt_err};
use crate::host_fns::RhaiHostFnSpec;
use crate::loader::RhaiLoader;

/// Register `uni_kms_sign` and `uni_kms_verify`.
pub fn register(loader: &mut RhaiLoader) {
    let kms = loader.kms();
    let placeholder = Capability::Kms {
        key_ids: vec!["*".into()],
    };
    let sign_kms = kms.clone();
    loader.host_fns_mut().register(RhaiHostFnSpec::gated(
        "uni.kms.sign",
        placeholder.clone(),
        "Sign bytes with a host-managed key (returns hex signature).",
        move |engine: &mut Engine, caps: &CapabilitySet| {
            register_sign(engine, caps.clone(), sign_kms.clone());
        },
    ));
    loader.host_fns_mut().register(RhaiHostFnSpec::gated(
        "uni.kms.verify",
        placeholder,
        "Verify a hex signature against a host-managed key.",
        move |engine: &mut Engine, caps: &CapabilitySet| {
            register_verify(engine, caps.clone(), kms.clone());
        },
    ));
}

fn register_sign(engine: &mut Engine, caps: CapabilitySet, kms: Option<Arc<dyn KmsProvider>>) {
    engine.register_fn(
        "uni_kms_sign",
        move |key_id: &str, data: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            require_allowed(
                &caps,
                |c| c.kms_allows(key_id),
                format!("uni.kms.sign: key `{key_id}` not in granted Kms allow-list"),
            )?;
            let kms = require_service(&kms, "uni.kms.sign: no KMS provider configured")?;
            let sig = kms
                .sign(key_id, data.as_bytes())
                .map_err(|e| rt_err(format!("uni.kms.sign(`{key_id}`): {e}")))?;
            Ok(to_hex(&sig))
        },
    );
}

fn register_verify(engine: &mut Engine, caps: CapabilitySet, kms: Option<Arc<dyn KmsProvider>>) {
    engine.register_fn(
        "uni_kms_verify",
        move |key_id: &str, data: &str, sig: &str| -> Result<bool, Box<rhai::EvalAltResult>> {
            require_allowed(
                &caps,
                |c| c.kms_allows(key_id),
                format!("uni.kms.verify: key `{key_id}` not in granted Kms allow-list"),
            )?;
            let kms = require_service(&kms, "uni.kms.verify: no KMS provider configured")?;
            let sig_bytes =
                from_hex(sig).map_err(|e| rt_err(format!("uni.kms.verify: signature hex: {e}")))?;
            kms.verify(key_id, data.as_bytes(), &sig_bytes)
                .map_err(|e| rt_err(format!("uni.kms.verify(`{key_id}`): {e}")))
        },
    );
}

/// Lowercase hex encoding for the script boundary.
fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Decode lowercase/uppercase hex; errors on odd length or non-hex digits.
fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd-length hex string".to_owned());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}
