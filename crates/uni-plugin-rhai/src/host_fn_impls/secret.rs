//! Secret host fns — gated by [`Capability::Secret`].
//!
//! `uni_secret_acquire(id) -> i64` returns an opaque handle for a
//! named secret. v1 returns a placeholder handle (0) so the symbol
//! exists and the capability check works; real secret-store wiring
//! follows the sealer/unsealer pattern described in proposal §10.2b
//! and lands in a follow-up.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use rhai::Engine;
use uni_plugin::Capability;

use crate::host_fns::RhaiHostFnSpec;
use crate::loader::RhaiLoader;

/// Register `uni_secret_acquire`.
pub fn register(loader: &mut RhaiLoader) {
    let placeholder = Capability::Secret {
        ids: vec!["*".into()],
    };
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.secret.acquire".into(),
        required_capability: Some(placeholder),
        docs: "Acquire an opaque handle for a named secret.".into(),
        register: Arc::new(register_acquire),
    });
}

fn register_acquire(engine: &mut Engine) {
    engine.register_fn(
        "uni_secret_acquire",
        |_id: &str| -> Result<i64, Box<rhai::EvalAltResult>> {
            Err(Box::new(rhai::EvalAltResult::ErrorRuntime(
                "uni.secret.acquire: real SecretStore wiring is M7-followup".into(),
                rhai::Position::NONE,
            )))
        },
    );
}
