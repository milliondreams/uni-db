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

use rhai::Engine;
use uni_plugin::host_services::{self, HttpPolicyError};
use uni_plugin::{Capability, CapabilitySet, HttpEgress};

use crate::host_fn_impls::rt_err;
use crate::host_fns::RhaiHostFnSpec;
use crate::loader::RhaiLoader;

/// Register `uni_http_get` and `uni_http_post`.
pub fn register(loader: &mut RhaiLoader) {
    let http = loader.http();
    let placeholder = Capability::Network {
        allow: vec!["**".into()],
    };
    let get_http = http.clone();
    loader.host_fns_mut().register(RhaiHostFnSpec::gated(
        "uni.http.get",
        placeholder.clone(),
        "HTTP GET against a URL in the granted allow-list.",
        move |engine: &mut Engine, caps: &CapabilitySet| {
            let http = get_http.clone();
            let caps = caps.clone();
            engine.register_fn(
                "uni_http_get",
                move |url: &str| -> Result<String, Box<rhai::EvalAltResult>> {
                    http_request(&http, &caps, url, None)
                },
            );
        },
    ));
    loader.host_fns_mut().register(RhaiHostFnSpec::gated(
        "uni.http.post",
        placeholder,
        "HTTP POST against a URL in the granted allow-list.",
        move |engine: &mut Engine, caps: &CapabilitySet| {
            let http = http.clone();
            let caps = caps.clone();
            engine.register_fn(
                "uni_http_post",
                move |url: &str, body: &str| -> Result<String, Box<rhai::EvalAltResult>> {
                    http_request(&http, &caps, url, Some(body.as_bytes()))
                },
            );
        },
    ));
}

/// Shared GET/POST body: enforce allow-list, resolve timeout, dispatch, map the
/// response to a UTF-8 string.
fn http_request(
    http: &Option<Arc<dyn HttpEgress>>,
    caps: &CapabilitySet,
    url: &str,
    body: Option<&[u8]>,
) -> Result<String, Box<rhai::EvalAltResult>> {
    // Propagate the host's trace context (W3C traceparent) into the outbound
    // call when one is active (real value only in `otel`-enabled builds; `None`
    // otherwise — no fabricated trace ids).
    let traceparent = uni_plugin::observability::current_trace_context().to_traceparent();

    // The allow-list / egress / timeout / status policy is shared with the
    // Extism loader; only the mapping onto Rhai's error surface is ours.
    let response =
        host_services::http_request(http.as_ref(), caps, url, body, traceparent.as_deref())
            .map_err(|e| match e {
                HttpPolicyError::NotAllowed => rt_err(format!(
                    "uni.http: url `{url}` not in granted Network allow-list"
                )),
                HttpPolicyError::NoEgress => {
                    rt_err("uni.http: no HTTP egress configured".to_string())
                }
                HttpPolicyError::Transport(err) => rt_err(format!("uni.http(`{url}`): {err}")),
                HttpPolicyError::Status(status) => {
                    rt_err(format!("uni.http(`{url}`): HTTP status {status}"))
                }
            })?;

    Ok(String::from_utf8_lossy(&response.body).into_owned())
}
