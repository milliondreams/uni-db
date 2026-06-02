//! Capability-gated host fns surfaced to Rhai scripts.
//!
//! Each module here exports a `register(loader: &mut RhaiLoader)` helper
//! that adds one or more `RhaiHostFnSpec` entries to the loader's
//! registry. The specs carry a registrar closure that the engine
//! factory only invokes when the corresponding capability is in the
//! plugin's effective grant set (proposal §10.2).

#![cfg(feature = "rhai-runtime")]

use uni_plugin::CapabilitySet;

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

/// Layer-3 (call-time) attenuation guard: error loudly unless some granted
/// capability `allows` the requested target.
///
/// `allows` is the per-capability matcher (`Capability::kms_allows`,
/// `secret_allows`, `network_allows`, …). The check runs against the
/// **effective grant set** and must happen *before* any host service is
/// touched, so a disallowed target can't even probe the backend. `deny_msg`
/// is the loud-failure message produced on refusal.
pub(crate) fn require_allowed(
    caps: &CapabilitySet,
    allows: impl Fn(&uni_plugin::Capability) -> bool,
    deny_msg: impl Into<String>,
) -> Result<(), Box<rhai::EvalAltResult>> {
    if caps.iter().any(allows) {
        Ok(())
    } else {
        Err(rt_err(deny_msg))
    }
}

/// Resolve an optional host service handle, erroring loudly when the host
/// configured none. Returns the `&T` so the caller can dispatch against it.
///
/// Always call *after* [`require_allowed`] so a disallowed request never
/// learns whether the backing service is present.
pub(crate) fn require_service<T: ?Sized>(
    svc: &Option<std::sync::Arc<T>>,
    absent_msg: impl Into<String>,
) -> Result<&std::sync::Arc<T>, Box<rhai::EvalAltResult>> {
    svc.as_ref().ok_or_else(|| rt_err(absent_msg))
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
