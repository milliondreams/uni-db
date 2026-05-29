//! KMS host fns — gated by [`Capability::Kms`].
//!
//! Sign / verify against a host-managed key. v1 ships stubs that error
//! with NotYetImplemented; real KMS wiring is host-specific and follows
//! the same capability-gated registration shape.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use rhai::Engine;
use uni_plugin::Capability;

use crate::host_fns::RhaiHostFnSpec;
use crate::loader::RhaiLoader;

/// Register `uni_kms_sign` and `uni_kms_verify`.
pub fn register(loader: &mut RhaiLoader) {
    let placeholder = Capability::Kms {
        key_ids: vec!["*".into()],
    };
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.kms.sign".into(),
        required_capability: Some(placeholder.clone()),
        docs: "Sign bytes with a host-managed key.".into(),
        register: Arc::new(register_sign),
    });
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.kms.verify".into(),
        required_capability: Some(placeholder),
        docs: "Verify a signature against a host-managed key.".into(),
        register: Arc::new(register_verify),
    });
}

fn register_sign(engine: &mut Engine) {
    engine.register_fn(
        "uni_kms_sign",
        |_key_id: &str, _data: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            Err(Box::new(rhai::EvalAltResult::ErrorRuntime(
                "uni.kms.sign: real KMS wiring is M7-followup".into(),
                rhai::Position::NONE,
            )))
        },
    );
}

fn register_verify(engine: &mut Engine) {
    engine.register_fn(
        "uni_kms_verify",
        |_key_id: &str, _data: &str, _sig: &str| -> Result<bool, Box<rhai::EvalAltResult>> {
            Err(Box::new(rhai::EvalAltResult::ErrorRuntime(
                "uni.kms.verify: real KMS wiring is M7-followup".into(),
                rhai::Position::NONE,
            )))
        },
    );
}
