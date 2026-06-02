//! Capability-gated host services for Extism plugins.
//!
//! Surfaces the same `uni.{kms,secret,http}.*` host functions the Rhai loader
//! exposes, binding the shared [`uni_plugin::KmsProvider`] /
//! [`uni_plugin::HttpEgress`] / [`uni_plugin::secrets::SecretStore`] traits. The
//! Extism wire is opaque bytes, so each fn takes a JSON request string and
//! returns a JSON response string (binary fields carried as lowercase hex).
//!
//! # Two-layer enforcement
//!
//! - **Link-time (variant gate):** [`register_default_host_svc`] registers the
//!   metadata [`HostFnSpec`]s; [`crate::loader::ExtismLoader::prepare`] only
//!   marks a fn as allowed when its required-capability *variant* is in the
//!   plugin's effective set, so an ungranted plugin never sees the import.
//! - **Call-time (pattern gate):** each `do_*` body matches the requested
//!   key-id / secret-id / URL against the granted attenuation patterns via
//!   [`uni_plugin::Capability::kms_allows`] etc., mirroring the Rhai loader.
//!
//! The concrete `extism::Function`s are built **per load** (via
//! `build_service_fn`) so each carries that load's effective
//! [`CapabilitySet`] plus the host's service handles in its
//! `extism::UserData`; an unconfigured service fails loudly at call time.

#![cfg(feature = "extism-runtime")]

use std::sync::Arc;

use extism::{Function, UserData, ValType};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uni_plugin::secrets::SecretStore;
use uni_plugin::{Capability, CapabilitySet, FnError, HttpEgress, KmsProvider};

use crate::host_fns::HostFnSpec;
use crate::loader::ExtismLoader;

pub mod kms;
pub mod net;
pub mod secret;

/// Import name for `uni.kms.sign`.
pub(crate) const FN_KMS_SIGN: &str = "uni_kms_sign";
/// Import name for `uni.kms.verify`.
pub(crate) const FN_KMS_VERIFY: &str = "uni_kms_verify";
/// Import name for `uni.secret.acquire`.
pub(crate) const FN_SECRET_ACQUIRE: &str = "uni_secret_acquire";
/// Import name for `uni.http.get`.
pub(crate) const FN_HTTP_GET: &str = "uni_http_get";
/// Import name for `uni.http.post`.
pub(crate) const FN_HTTP_POST: &str = "uni_http_post";

/// Per-load context carried in each service `Function`'s [`UserData`].
///
/// Cloned into every service fn at build time; the `Arc` service handles make
/// the clone cheap. `effective` is the load's intersected grant set used for
/// call-time pattern attenuation.
#[derive(Clone)]
pub(crate) struct HostSvcCtx {
    /// Effective (declared ∩ granted) capability set for this load.
    pub effective: CapabilitySet,
    /// KMS provider backing `uni.kms.*`; `None` → those fns error loudly.
    pub kms: Option<Arc<dyn KmsProvider>>,
    /// Secret store backing `uni.secret.acquire`.
    pub secrets: Option<Arc<SecretStore>>,
    /// HTTP egress backing `uni.http.*`.
    pub http: Option<Arc<dyn HttpEgress>>,
}

/// Lowercase hex encoding for the JSON wire boundary.
pub(crate) fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Decode lowercase/uppercase hex; errors on odd length or non-hex digits.
pub(crate) fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("odd-length hex string".to_owned());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

/// Deserialize a JSON request, run `f`, and re-serialize its response as
/// JSON — the shared `host_fn!`-shell body for every `uni.{kms,secret,http}`
/// service fn.
///
/// `label` is the host-fn name used in error messages (e.g.
/// `"uni.kms.sign"`). Each `do_*` body holds the capability attenuation and
/// dispatch; this only owns the JSON (de)serialization the kms / secret /
/// net shells previously duplicated verbatim.
///
/// # Errors
///
/// Returns [`FnError`] when the request JSON is malformed, the `f` dispatch
/// fails, or the response cannot be serialized.
pub(crate) fn dispatch_json<Req, Resp, F>(
    ctx: &HostSvcCtx,
    req_json: &str,
    label: &str,
    f: F,
) -> Result<String, FnError>
where
    Req: DeserializeOwned,
    Resp: Serialize,
    F: FnOnce(&HostSvcCtx, Req) -> Result<Resp, FnError>,
{
    let req: Req = serde_json::from_str(req_json)
        .map_err(|e| FnError::new(0xC30, format!("{label}: bad request json: {e}")))?;
    let resp = f(ctx, req)?;
    serde_json::to_string(&resp)
        .map_err(|e| FnError::new(0xC31, format!("{label}: response json: {e}")))
}

/// Build the concrete `extism::Function` for a known service-fn `name`.
///
/// Returns `None` for unrecognized names (so the loader falls back to its
/// static `runtime_fns` map). The `UserData` captures a clone of `ctx`, so the
/// returned fn enforces *this* load's attenuation patterns and dispatches to
/// *this* load's service handles.
pub(crate) fn build_service_fn(name: &str, ctx: &HostSvcCtx) -> Option<Function> {
    let f = match name {
        FN_KMS_SIGN => Function::new(
            FN_KMS_SIGN,
            [ValType::I64],
            [ValType::I64],
            UserData::new(ctx.clone()),
            kms::uni_kms_sign,
        ),
        FN_KMS_VERIFY => Function::new(
            FN_KMS_VERIFY,
            [ValType::I64],
            [ValType::I64],
            UserData::new(ctx.clone()),
            kms::uni_kms_verify,
        ),
        FN_SECRET_ACQUIRE => Function::new(
            FN_SECRET_ACQUIRE,
            [ValType::I64],
            [ValType::I64],
            UserData::new(ctx.clone()),
            secret::uni_secret_acquire,
        ),
        FN_HTTP_GET => Function::new(
            FN_HTTP_GET,
            [ValType::I64],
            [ValType::I64],
            UserData::new(ctx.clone()),
            net::uni_http_get,
        ),
        FN_HTTP_POST => Function::new(
            FN_HTTP_POST,
            [ValType::I64],
            [ValType::I64],
            UserData::new(ctx.clone()),
            net::uni_http_post,
        ),
        _ => return None,
    };
    Some(f)
}

/// Register the default capability-gated host-service surface on a loader.
///
/// Adds the metadata [`HostFnSpec`]s only — the concrete functions are built
/// per load. Mirrors `uni_plugin_rhai::host_fn_impls::register_default_host_fns`
/// so both loaders expose the same `uni.{kms,secret,http}.*` surface. Hosts
/// wire the backing services via
/// [`ExtismLoader::with_kms`](crate::loader::ExtismLoader::with_kms) etc.;
/// without them, a granted call fails loudly ("no … configured").
pub fn register_default_host_svc(loader: &mut ExtismLoader) {
    let specs = [
        (
            FN_KMS_SIGN,
            Capability::Kms {
                key_ids: Vec::new(),
            },
            "Sign bytes with a host-managed key (hex in/out).",
        ),
        (
            FN_KMS_VERIFY,
            Capability::Kms {
                key_ids: Vec::new(),
            },
            "Verify a hex signature against a host-managed key.",
        ),
        (
            FN_SECRET_ACQUIRE,
            Capability::Secret { ids: Vec::new() },
            "Acquire an opaque handle for a named secret.",
        ),
        (
            FN_HTTP_GET,
            Capability::Network { allow: Vec::new() },
            "HTTP GET against a URL in the granted allow-list.",
        ),
        (
            FN_HTTP_POST,
            Capability::Network { allow: Vec::new() },
            "HTTP POST against a URL in the granted allow-list.",
        ),
    ];
    for (name, cap, docs) in specs {
        loader.host_fns_mut().register(HostFnSpec {
            name: name.to_owned(),
            required_capability: Some(cap),
            docs: docs.to_owned(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips() {
        let bytes = [0x00u8, 0x0f, 0xa5, 0xff];
        let hex = to_hex(&bytes);
        assert_eq!(hex, "000fa5ff");
        assert_eq!(from_hex(&hex).unwrap(), bytes);
    }

    #[test]
    fn from_hex_rejects_odd_length() {
        assert!(from_hex("abc").is_err());
    }

    #[test]
    fn register_default_host_svc_registers_five_specs() {
        let mut loader = ExtismLoader::new();
        register_default_host_svc(&mut loader);
        assert_eq!(loader.host_fns().len(), 5);
        assert!(loader.host_fns().get(FN_KMS_SIGN).is_some());
        assert!(loader.host_fns().get(FN_HTTP_POST).is_some());
    }

    #[test]
    fn build_service_fn_unknown_name_is_none() {
        let ctx = HostSvcCtx {
            effective: CapabilitySet::new(),
            kms: None,
            secrets: None,
            http: None,
        };
        assert!(build_service_fn("not_a_service_fn", &ctx).is_none());
        assert!(build_service_fn(FN_KMS_SIGN, &ctx).is_some());
    }
}
