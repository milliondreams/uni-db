// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end wiring of the capability-gated `uni.kms.*`, `uni.secret.*`, and
//! `uni.http.*` host fns, including layer-3 (call-time) attenuation: a granted
//! capability whose pattern does not cover the requested key/secret/URL must be
//! rejected, and an unconfigured host service must fail loudly.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;
use std::time::Duration;

use uni_plugin::secrets::SecretStore;
use uni_plugin::{Capability, CapabilitySet, FnError, HttpEgress, HttpResponse, KmsProvider};
use uni_plugin_rhai::host_fn_impls::register_default_host_fns;
use uni_plugin_rhai::{RhaiLoader, build_engine};

/// Deterministic fake: "signature" is the input bytes reversed.
struct FakeKms;
impl KmsProvider for FakeKms {
    fn sign(&self, _key_id: &str, data: &[u8]) -> Result<Vec<u8>, FnError> {
        Ok(data.iter().rev().copied().collect())
    }
    fn verify(&self, _key_id: &str, data: &[u8], signature: &[u8]) -> Result<bool, FnError> {
        let expected: Vec<u8> = data.iter().rev().copied().collect();
        Ok(expected == signature)
    }
}

/// Fake egress echoing the request — no real socket.
struct FakeHttp;
impl HttpEgress for FakeHttp {
    fn get(
        &self,
        url: &str,
        _t: Duration,
        _m: usize,
        _tp: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        Ok(HttpResponse {
            status: 200,
            body: format!("GET {url}").into_bytes(),
        })
    }
    fn post(
        &self,
        url: &str,
        body: &[u8],
        _t: Duration,
        _m: usize,
        _tp: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        Ok(HttpResponse {
            status: 200,
            body: format!("POST {url} {}", body.len()).into_bytes(),
        })
    }
}

fn loader_with_services() -> RhaiLoader {
    let store = Arc::new(SecretStore::new());
    store.seal("db-password", b"hunter2".to_vec());
    let mut loader = RhaiLoader::new()
        .with_kms(Arc::new(FakeKms))
        .with_secret_store(store)
        .with_http(Arc::new(FakeHttp));
    register_default_host_fns(&mut loader);
    loader
}

#[test]
fn secret_acquire_returns_handle_when_granted() {
    let loader = loader_with_services();
    let caps = CapabilitySet::from_iter_of([Capability::Secret {
        ids: vec!["db-*".into()],
    }]);
    let engine = build_engine(&caps, loader.host_fns());
    let h: i64 = engine
        .eval(r#"uni_secret_acquire("db-password")"#)
        .expect("acquire");
    assert!(h > 0, "handle must be a positive opaque id, got {h}");
}

#[test]
fn secret_acquire_denied_out_of_allowlist() {
    let loader = loader_with_services();
    let caps = CapabilitySet::from_iter_of([Capability::Secret {
        ids: vec!["db-*".into()],
    }]);
    let engine = build_engine(&caps, loader.host_fns());
    let err = engine
        .eval::<i64>(r#"uni_secret_acquire("api-token")"#)
        .expect_err("out-of-list id must be denied");
    assert!(
        err.to_string().contains("not in granted Secret allow-list"),
        "{err}"
    );
}

#[test]
fn secret_acquire_absent_store_errors_loudly() {
    let mut loader = RhaiLoader::new();
    register_default_host_fns(&mut loader);
    let caps = CapabilitySet::from_iter_of([Capability::Secret {
        ids: vec!["*".into()],
    }]);
    let engine = build_engine(&caps, loader.host_fns());
    let err = engine
        .eval::<i64>(r#"uni_secret_acquire("db-password")"#)
        .expect_err("no store configured must error");
    assert!(
        err.to_string().contains("no secret store configured"),
        "{err}"
    );
}

#[test]
fn kms_sign_and_verify_round_trip() {
    let loader = loader_with_services();
    let caps = CapabilitySet::from_iter_of([Capability::Kms {
        key_ids: vec!["*".into()],
    }]);
    let engine = build_engine(&caps, loader.host_fns());
    let sig: String = engine.eval(r#"uni_kms_sign("k1", "hello")"#).expect("sign");
    assert!(!sig.is_empty());
    let ok: bool = engine
        .eval(&format!(r#"uni_kms_verify("k1", "hello", "{sig}")"#))
        .expect("verify");
    assert!(ok, "round-trip signature must verify");
}

#[test]
fn kms_denied_key_errors() {
    let loader = loader_with_services();
    let caps = CapabilitySet::from_iter_of([Capability::Kms {
        key_ids: vec!["allowed-*".into()],
    }]);
    let engine = build_engine(&caps, loader.host_fns());
    let err = engine
        .eval::<String>(r#"uni_kms_sign("forbidden-key", "x")"#)
        .expect_err("out-of-list key must be denied");
    assert!(
        err.to_string().contains("not in granted Kms allow-list"),
        "{err}"
    );
}

#[test]
fn http_get_allowed_and_denied() {
    let loader = loader_with_services();
    let caps = CapabilitySet::from_iter_of([Capability::Network {
        allow: vec!["https://api.example/**".into()],
    }]);
    let engine = build_engine(&caps, loader.host_fns());
    let body: String = engine
        .eval(r#"uni_http_get("https://api.example/v1/x")"#)
        .expect("allowed GET");
    assert_eq!(body, "GET https://api.example/v1/x");

    let err = engine
        .eval::<String>(r#"uni_http_get("https://evil.example/x")"#)
        .expect_err("out-of-allow-list URL must be denied");
    assert!(
        err.to_string()
            .contains("not in granted Network allow-list"),
        "{err}"
    );
}

#[test]
fn fs_read_write_enforce_glob_allowlist() {
    let loader = loader_with_services();
    let caps = CapabilitySet::from_iter_of([Capability::Filesystem {
        read: vec!["/allowed/**".into()],
        write: vec!["/allowed/**".into()],
    }]);
    let engine = build_engine(&caps, loader.host_fns());

    // Out-of-allow-list path: denied before any filesystem access.
    let read_denied = engine
        .eval::<String>(r#"uni_fs_read("/forbidden/secret")"#)
        .expect_err("read outside allow-list must be denied");
    assert!(
        read_denied
            .to_string()
            .contains("not in granted Filesystem read allow-list"),
        "{read_denied}"
    );
    let write_denied = engine
        .eval::<()>(r#"uni_fs_write("/forbidden/x", "data")"#)
        .expect_err("write outside allow-list must be denied");
    assert!(
        write_denied
            .to_string()
            .contains("not in granted Filesystem write allow-list"),
        "{write_denied}"
    );

    // In-allow-list but nonexistent path: passes the gate, so the error is a
    // genuine filesystem error, NOT the allow-list rejection.
    let read_allowed_missing = engine
        .eval::<String>(r#"uni_fs_read("/allowed/does-not-exist")"#)
        .expect_err("missing file is an fs error");
    assert!(
        !read_allowed_missing.to_string().contains("allow-list"),
        "in-allow-list path must pass the gate, got: {read_allowed_missing}"
    );
}

#[test]
fn http_absent_egress_errors_loudly() {
    let mut loader = RhaiLoader::new();
    register_default_host_fns(&mut loader);
    let caps = CapabilitySet::from_iter_of([Capability::Network {
        allow: vec!["**".into()],
    }]);
    let engine = build_engine(&caps, loader.host_fns());
    let err = engine
        .eval::<String>(r#"uni_http_get("https://x/")"#)
        .expect_err("no egress configured must error");
    assert!(
        err.to_string().contains("no HTTP egress configured"),
        "{err}"
    );
}
