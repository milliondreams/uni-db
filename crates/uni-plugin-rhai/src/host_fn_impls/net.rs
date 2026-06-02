//! Network host fns — gated by [`Capability::Network`].
//!
//! `uni_http_get(url) -> string` and `uni_http_post(url, body) -> string`
//! dispatch to the loader's [`HttpEgress`]. Call-time attenuation matches the
//! requested URL against the granted `Capability::Network { allow }` patterns
//! *before* any socket is opened; the per-call timeout is taken from
//! `Capability::WallClockMillisPerCall` (else a conservative default), and the
//! response body is size-capped. A missing egress or an out-of-allow-list URL
//! errors loudly.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;
use std::time::Duration;

use rhai::Engine;
use uni_plugin::{Capability, CapabilitySet, HttpEgress};

use crate::host_fn_impls::rt_err;
use crate::host_fns::RhaiHostFnSpec;
use crate::loader::RhaiLoader;

/// Default per-call HTTP timeout when the grant carries no
/// `WallClockMillisPerCall`. Conservative: long enough for a typical API call,
/// short enough to bound a wedged request.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum response body bytes read before truncation — bounds memory so a
/// hostile/large response can't exhaust the host.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Register `uni_http_get` and `uni_http_post`.
pub fn register(loader: &mut RhaiLoader) {
    let http = loader.http();
    let placeholder = Capability::Network {
        allow: vec!["**".into()],
    };
    let get_http = http.clone();
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.http.get".into(),
        required_capability: Some(placeholder.clone()),
        docs: "HTTP GET against a URL in the granted allow-list.".into(),
        register: Arc::new(move |engine: &mut Engine, caps: &CapabilitySet| {
            let http = get_http.clone();
            let caps = caps.clone();
            engine.register_fn(
                "uni_http_get",
                move |url: &str| -> Result<String, Box<rhai::EvalAltResult>> {
                    http_request(&http, &caps, url, None)
                },
            );
        }),
    });
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.http.post".into(),
        required_capability: Some(placeholder),
        docs: "HTTP POST against a URL in the granted allow-list.".into(),
        register: Arc::new(move |engine: &mut Engine, caps: &CapabilitySet| {
            let http = http.clone();
            let caps = caps.clone();
            engine.register_fn(
                "uni_http_post",
                move |url: &str, body: &str| -> Result<String, Box<rhai::EvalAltResult>> {
                    http_request(&http, &caps, url, Some(body.as_bytes()))
                },
            );
        }),
    });
}

/// Shared GET/POST body: enforce allow-list, resolve timeout, dispatch, map the
/// response to a UTF-8 string.
fn http_request(
    http: &Option<Arc<dyn HttpEgress>>,
    caps: &CapabilitySet,
    url: &str,
    body: Option<&[u8]>,
) -> Result<String, Box<rhai::EvalAltResult>> {
    if !caps.iter().any(|c| c.network_allows(url)) {
        return Err(rt_err(format!(
            "uni.http: url `{url}` not in granted Network allow-list"
        )));
    }
    let egress = http
        .as_ref()
        .ok_or_else(|| rt_err("uni.http: no HTTP egress configured"))?;
    let timeout = caps
        .iter()
        .find_map(|c| match c {
            Capability::WallClockMillisPerCall(ms) => Some(Duration::from_millis(*ms)),
            _ => None,
        })
        .unwrap_or(DEFAULT_TIMEOUT);
    let response = match body {
        Some(b) => egress.post(url, b, timeout, MAX_RESPONSE_BYTES),
        None => egress.get(url, timeout, MAX_RESPONSE_BYTES),
    }
    .map_err(|e| rt_err(format!("uni.http(`{url}`): {e}")))?;
    if response.status >= 400 {
        return Err(rt_err(format!(
            "uni.http(`{url}`): HTTP status {}",
            response.status
        )));
    }
    Ok(String::from_utf8_lossy(&response.body).into_owned())
}
