//! Network host fns — gated by [`Capability::Network`].
//!
//! v1 ships a deliberately minimal `uni_http_get(url) -> string` stub
//! that errors with "not yet implemented". The real HTTP client wiring
//! (with allow-list URL validation and timeout-from-CapabilitySet) lands
//! in a follow-up; v1 just verifies the symbol is registered only when
//! the capability is granted. This is sufficient to exercise the
//! sandbox + capability-gate tests.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use rhai::Engine;
use uni_plugin::Capability;

use crate::host_fns::RhaiHostFnSpec;
use crate::loader::RhaiLoader;

/// Register `uni_http_get` and `uni_http_post`.
pub fn register(loader: &mut RhaiLoader) {
    let placeholder = Capability::Network {
        allow: vec!["**".into()],
    };
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.http.get".into(),
        required_capability: Some(placeholder.clone()),
        docs: "HTTP GET against a URL in the granted allow-list.".into(),
        register: Arc::new(register_http_get),
    });
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.http.post".into(),
        required_capability: Some(placeholder),
        docs: "HTTP POST against a URL in the granted allow-list.".into(),
        register: Arc::new(register_http_post),
    });
}

fn register_http_get(engine: &mut Engine) {
    engine.register_fn(
        "uni_http_get",
        |_url: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            Err(Box::new(rhai::EvalAltResult::ErrorRuntime(
                "uni.http.get: real HTTP client wiring is M7-followup".into(),
                rhai::Position::NONE,
            )))
        },
    );
}

fn register_http_post(engine: &mut Engine) {
    engine.register_fn(
        "uni_http_post",
        |_url: &str, _body: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            Err(Box::new(rhai::EvalAltResult::ErrorRuntime(
                "uni.http.post: real HTTP client wiring is M7-followup".into(),
                rhai::Position::NONE,
            )))
        },
    );
}
