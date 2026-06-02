//! Secret host fns — gated by [`Capability::Secret`].
//!
//! `uni_secret_acquire(id) -> i64` returns an opaque handle for a named secret,
//! looked up in the loader's [`SecretStore`]. Call-time attenuation matches
//! `id` against the granted `Capability::Secret { ids }` allow-list; a missing
//! store or an out-of-list id errors loudly (never returns a fake handle).

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use rhai::Engine;
use uni_plugin::secrets::SecretStore;
use uni_plugin::{Capability, CapabilitySet};

use crate::host_fn_impls::rt_err;
use crate::host_fns::RhaiHostFnSpec;
use crate::loader::RhaiLoader;

/// Register `uni_secret_acquire`.
pub fn register(loader: &mut RhaiLoader) {
    let store = loader.secret_store();
    let placeholder = Capability::Secret {
        ids: vec!["*".into()],
    };
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.secret.acquire".into(),
        required_capability: Some(placeholder),
        docs: "Acquire an opaque handle for a named secret.".into(),
        register: Arc::new(move |engine: &mut Engine, caps: &CapabilitySet| {
            register_acquire(engine, caps.clone(), store.clone());
        }),
    });
}

fn register_acquire(engine: &mut Engine, caps: CapabilitySet, store: Option<Arc<SecretStore>>) {
    engine.register_fn(
        "uni_secret_acquire",
        move |id: &str| -> Result<i64, Box<rhai::EvalAltResult>> {
            if !caps.iter().any(|c| c.secret_allows(id)) {
                return Err(rt_err(format!(
                    "uni.secret.acquire: id `{id}` not in granted Secret allow-list"
                )));
            }
            let store = store
                .as_ref()
                .ok_or_else(|| rt_err("uni.secret.acquire: no secret store configured"))?;
            let handle = store
                .acquire(id)
                .map_err(|e| rt_err(format!("uni.secret.acquire(`{id}`): {e}")))?;
            // Opaque handle ids start at 1 and increment; the cast is lossless
            // in any realistic run.
            Ok(handle.opaque_id() as i64)
        },
    );
}
