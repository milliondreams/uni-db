//! Integration tests for the capability-gated Extism host-service surface
//! (`uni_kms_*`, `uni_secret_acquire`, `uni_http_*`).
//!
//! These exercise the **link-time** half of enforcement: that
//! `register_default_host_svc` + `prepare` only mark a service fn as allowed
//! when the matching capability *variant* is in the effective set, and that the
//! per-load function set builds against a real wasmtime store. The **call-time**
//! attenuation + dispatch logic (pattern matching, provider calls, traceparent
//! injection) is unit-tested directly against the `do_*` fns in
//! `src/host_svc/{kms,secret,net}.rs`; a guest that actually *imports* these
//! host fns is not part of this suite (no such fixture exists yet — the geo
//! example imports nothing), so end-to-end guest invocation is not exercised
//! here.

use uni_plugin::{Capability, CapabilitySet};
use uni_plugin_extism::{ExtismLoader, register_default_host_svc};

/// Minimal valid wasm that imports nothing — builds cleanly even when the
/// loader offers the service host fns (extra imports are simply unused).
const TRIVIAL_WAT: &str = r#"
(module
  (func (export "answer") (result i32)
    i32.const 42))
"#;

fn trivial_wasm() -> Vec<u8> {
    wat::parse_str(TRIVIAL_WAT).expect("WAT must parse")
}

fn loader() -> ExtismLoader {
    let mut l = ExtismLoader::new();
    register_default_host_svc(&mut l);
    l
}

#[test]
fn default_surface_registers_six_specs() {
    // KMS sign/verify, secret acquire, HTTP get/post, and the GraphCompute
    // `uni_graph_call` host fn.
    assert_eq!(loader().host_fns().len(), 6);
}

#[test]
fn kms_grant_exposes_kms_fns_only() {
    let loader = loader();
    // Manifest declares kms + network (bare names → zero-attenuation variants);
    // host grants only Kms. Effective therefore contains Kms but not Network.
    let manifest = r#"{"id":"a.b","version":"0.0.1","capabilities":["kms","network"]}"#;
    let grants = CapabilitySet::from_iter_of([Capability::Kms {
        key_ids: vec!["*".into()],
    }]);
    let prepared = loader.prepare(manifest.as_bytes(), &grants).unwrap();

    assert!(
        prepared
            .allowed_host_fns
            .contains(&"uni_kms_sign".to_owned())
    );
    assert!(
        prepared
            .allowed_host_fns
            .contains(&"uni_kms_verify".to_owned())
    );
    // Network was declared but not granted → its fns are gated out.
    assert!(
        !prepared
            .allowed_host_fns
            .contains(&"uni_http_get".to_owned())
    );
    assert!(
        prepared
            .denied_capabilities
            .iter()
            .any(|c| c.contains("Network"))
    );
}

#[test]
fn empty_grant_exposes_no_service_fns() {
    let loader = loader();
    let manifest = r#"{"id":"a.b","version":"0.0.1","capabilities":["kms"]}"#;
    let prepared = loader
        .prepare(manifest.as_bytes(), &CapabilitySet::new())
        .unwrap();
    assert!(prepared.allowed_host_fns.is_empty());
}

#[test]
fn granted_service_fns_build_against_real_wasm() {
    let loader = loader();
    let manifest = r#"{"id":"a.b","version":"0.0.1","capabilities":["kms","secret","network"]}"#;
    let grants = CapabilitySet::from_iter_of([
        Capability::Kms {
            key_ids: vec!["*".into()],
        },
        Capability::Secret {
            ids: vec!["*".into()],
        },
        Capability::Network {
            allow: vec!["**".into()],
        },
    ]);
    let prepared = loader.prepare(manifest.as_bytes(), &grants).unwrap();
    assert_eq!(prepared.allowed_host_fns.len(), 5);

    // The per-load function set (which materializes all five service fns with
    // a None-service ctx here) must build cleanly; the trivial module imports
    // none of them, mirroring Extism's "linker absence" gating model.
    let _plugin = loader
        .build_plugin(&trivial_wasm(), &prepared)
        .expect("plugin builds with all service fns present");
}
