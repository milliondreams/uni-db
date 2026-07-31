//! KMS host fns — gated by [`Capability::Kms`].
//!
//! `uni_kms_sign(key_id, data) -> string` and
//! `uni_kms_verify(key_id, data, sig) -> bool` dispatch to the loader's
//! [`KmsProvider`]. Signatures cross the Rhai boundary as lowercase hex.
//! Call-time attenuation matches `key_id` against the granted
//! `Capability::Kms { key_ids }` allow-list; a missing provider or an
//! out-of-list key errors loudly.

#![cfg(feature = "rhai-runtime")]

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

// Hex codec for the script boundary — shared with the Extism loader.
use uni_plugin::hex::{from_hex, to_hex};

#[cfg(test)]
mod tests {
    use super::{from_hex, to_hex};

    #[test]
    fn hex_round_trips() {
        assert_eq!(
            from_hex(&to_hex(&[0x00, 0xAB, 0xff])).unwrap(),
            vec![0, 0xAB, 0xFF]
        );
    }

    #[test]
    fn from_hex_errors_on_odd_length() {
        assert!(from_hex("abc").is_err());
    }

    #[test]
    fn from_hex_errors_on_even_byte_multibyte_input() {
        // "aéb" = [0x61, 0xC3, 0xA9, 0x62] — even byte length, but 'é' is a
        // multibyte codepoint. Byte-index slicing would panic; decoding on raw
        // bytes returns Err instead.
        let input = "aéb";
        assert_eq!(input.len(), 4);
        assert!(from_hex(input).is_err(), "must return Err, not panic");
    }
}
