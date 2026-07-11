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

pub mod graph;
pub mod kms;
pub mod net;
pub mod secret;

/// Import name for the GraphCompute kernel dispatch host fn.
pub(crate) const FN_GRAPH_CALL: &str = "uni_graph_call";

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
    /// GraphCompute session registry backing `uni_graph_call`.
    pub graph: Option<uni_plugin_builtin::algorithms::graph_compute::SharedRegistry>,
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
///
/// Operates on raw bytes (`chunks_exact(2)`), NOT `&s[i..i+2]` string slicing:
/// the guest controls this string, and byte-index slicing panics on a multibyte
/// UTF-8 codepoint that happens to make the byte length even. A non-ASCII byte
/// simply fails the hex-digit test and returns `Err`.
pub(crate) fn from_hex(s: &str) -> Result<Vec<u8>, String> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err("odd-length hex string".to_owned());
    }
    fn nibble(b: u8) -> Result<u8, String> {
        match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => Err("invalid hex digit".to_owned()),
        }
    }
    bytes
        .chunks_exact(2)
        .map(|pair| Ok((nibble(pair[0])? << 4) | nibble(pair[1])?))
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
        FN_GRAPH_CALL => Function::new(
            FN_GRAPH_CALL,
            [ValType::I64],
            [ValType::I64],
            UserData::new(ctx.clone()),
            graph::uni_graph_call,
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
        (
            FN_GRAPH_CALL,
            Capability::GraphCompute,
            "Dispatch one GraphCompute kernel call (JSON in / JSON out).",
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

    /// Repro for `crates/uni-plugin-extism/src/host_svc/mod.rs:86`.
    ///
    /// `from_hex` slices the guest-controlled string with BYTE indices
    /// (`&s[i..i + 2]`) after only checking that the BYTE length is even. An
    /// even-byte-length input containing a multibyte UTF-8 codepoint passes the
    /// `len % 2` guard but the byte slice lands mid-codepoint, so `str` range
    /// indexing PANICS ("byte index N is not a char boundary") instead of
    /// returning the documented `Err`. Callers (kms/net `do_*`) wrap the result
    /// in `.map_err(...)` expecting a recoverable `Result` — they cannot catch a
    /// panic, so a guest can trigger a host-fn panic (DoS) with a crafted hex
    /// field.
    ///
    /// Regression: `from_hex` decodes on raw bytes, so an even-byte-length input
    /// containing a multibyte UTF-8 codepoint returns `Err` instead of panicking
    /// on a non-char-boundary byte slice (a guest-triggerable host-fn DoS).
    #[test]
    fn from_hex_errors_on_even_byte_multibyte_input() {
        // "aéb" = [0x61, 0xC3, 0xA9, 0x62] — 4 bytes (even, passes len%2), but
        // 'é' occupies byte indices 1..=2, which byte-index slicing would split.
        let input = "aéb";
        assert_eq!(input.len(), 4, "precondition: even byte length");
        let res = from_hex(input);
        assert!(
            res.is_err(),
            "from_hex must return Err on non-hex multibyte input, not panic; got {res:?}"
        );
    }

    #[test]
    fn register_default_host_svc_registers_six_specs() {
        let mut loader = ExtismLoader::new();
        register_default_host_svc(&mut loader);
        assert_eq!(loader.host_fns().len(), 6);
        assert!(loader.host_fns().get(FN_KMS_SIGN).is_some());
        assert!(loader.host_fns().get(FN_HTTP_POST).is_some());
        assert!(loader.host_fns().get(FN_GRAPH_CALL).is_some());
    }

    #[test]
    fn build_service_fn_unknown_name_is_none() {
        let ctx = HostSvcCtx {
            effective: CapabilitySet::new(),
            kms: None,
            secrets: None,
            http: None,
            graph: None,
        };
        assert!(build_service_fn("not_a_service_fn", &ctx).is_none());
        assert!(build_service_fn(FN_KMS_SIGN, &ctx).is_some());
    }
}
