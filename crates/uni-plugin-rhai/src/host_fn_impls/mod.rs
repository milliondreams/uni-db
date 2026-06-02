//! Capability-gated host fns surfaced to Rhai scripts.
//!
//! Each module here exports a `register(loader: &mut RhaiLoader)` helper
//! that adds one or more `RhaiHostFnSpec` entries to the loader's
//! registry. The specs carry a registrar closure that the engine
//! factory only invokes when the corresponding capability is in the
//! plugin's effective grant set (proposal §10.2).

#![cfg(feature = "rhai-runtime")]

use crate::loader::RhaiLoader;

pub mod fs;
pub mod kms;
pub mod net;
pub mod secret;

/// Build a Rhai runtime error from a message — the loud-failure path shared by
/// every capability-gated host fn (denied attenuation, missing host service,
/// or a service-level failure).
pub(crate) fn rt_err(msg: impl Into<String>) -> Box<rhai::EvalAltResult> {
    let msg: String = msg.into();
    Box::new(rhai::EvalAltResult::ErrorRuntime(
        msg.into(),
        rhai::Position::NONE,
    ))
}

/// Register the default capability-gated host fn surface on a loader.
///
/// Equivalent to calling each module's `register` in sequence. Hosts
/// that want a narrower surface can register selectively.
pub fn register_default_host_fns(loader: &mut RhaiLoader) {
    fs::register(loader);
    net::register(loader);
    kms::register(loader);
    secret::register(loader);
}
