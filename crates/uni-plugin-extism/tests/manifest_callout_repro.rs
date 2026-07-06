//! Repro for `crates/uni-plugin-extism/src/loader.rs:416`.
//!
//! Pass 1 of `ExtismLoader::load` builds the bootstrap plugin with
//! `effective = host_grants` (the host's full offered set, un-intersected) and
//! materializes every host-offered service fn, then invokes the guest's
//! `manifest` export. The loader's doc comment asserts this is safe because
//! "pass 1 invokes only the pure `manifest` export" — but nothing enforces
//! guest-export purity.
//!
//! The `example-extism-manifest-callout` guest calls the capability-gated
//! `uni_http_post` host fn from inside its `manifest` export, then declares
//! `capabilities: []`. A correct loader would compute `effective = ∅` for a
//! zero-declaring plugin and never let it reach the network. This test proves
//! the host's HTTP egress fires anyway, during pass 1, before declared-cap
//! intersection — even though the load itself subsequently fails in pass 2 for
//! lack of the `uni_http_post` import.
//!
//! Build the fixture first:
//!   (cd examples/example-extism-manifest-callout && \
//!      cargo build --target wasm32-unknown-unknown --release)

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use uni_plugin::{
    Capability, CapabilitySet, FnError, HttpEgress, HttpResponse, PluginId, PluginRegistrar,
    PluginRegistry,
};
use uni_plugin_extism::{ExtismLoader, register_default_host_svc};

const WASM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-extism-manifest-callout/target/wasm32-unknown-unknown/release/example_extism_manifest_callout.wasm",
);

fn load_wasm_bytes() -> Vec<u8> {
    std::fs::read(WASM_PATH).unwrap_or_else(|e| {
        panic!(
            "repro wasm artifact missing at {WASM_PATH}: {e}\n\
             Build it first:\n  (cd examples/example-extism-manifest-callout && \
             cargo build --target wasm32-unknown-unknown --release)"
        );
    })
}

/// Egress that COUNTS calls and records the URLs/bodies it was asked to send.
/// A real socket is never opened; recording a call is enough to prove the
/// side effect fired.
#[derive(Default)]
struct CountingEgress {
    posts: AtomicUsize,
    gets: AtomicUsize,
    last_post: Mutex<Option<(String, Vec<u8>)>>,
}

impl HttpEgress for CountingEgress {
    fn get(
        &self,
        _url: &str,
        _timeout: Duration,
        _max_bytes: usize,
        _traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        Ok(HttpResponse {
            status: 200,
            body: b"pong".to_vec(),
        })
    }
    fn post(
        &self,
        url: &str,
        body: &[u8],
        _timeout: Duration,
        _max_bytes: usize,
        _traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        self.posts.fetch_add(1, Ordering::SeqCst);
        *self.last_post.lock().unwrap() = Some((url.to_owned(), body.to_vec()));
        Ok(HttpResponse {
            status: 200,
            body: Vec::new(),
        })
    }
}

/// The host offers a broad Network grant — as a permissive host commonly
/// would. The *plugin* declares no capabilities, so a correct loader must
/// still deny it egress.
fn permissive_host_grants() -> CapabilitySet {
    CapabilitySet::from_iter_of([
        Capability::ScalarFn,
        Capability::Network {
            allow: vec!["**".into()],
        },
    ])
}

#[test]
fn manifest_export_side_effects_fire_with_uninterecting_host_grants() {
    let bytes = load_wasm_bytes();
    let egress = std::sync::Arc::new(CountingEgress::default());
    let mut loader = ExtismLoader::new().with_http(egress.clone());
    register_default_host_svc(&mut loader);

    let registry = PluginRegistry::new();
    let reg_caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(
        PluginId::new("extism.manifest.callout"),
        &reg_caps,
        &registry,
    );

    // The plugin declares `capabilities: []`. A correct loader would compute
    // `effective = ∅` and NEVER materialize `uni_http_post` for the guest, so
    // no egress could ever fire. Load may well end in Err (pass 2 cannot
    // resolve the `uni_http_post` import once effective = ∅) — that's fine; the
    // security-relevant fact is what happened during pass 1.
    let load_result = loader.load(&bytes, &permissive_host_grants(), &mut r);

    let posts = egress.posts.load(Ordering::SeqCst);

    // FIXED (loader.rs): the bootstrap pass builds the host fns with an EMPTY
    // effective grant set, so the guest's manifest-export uni_http_post call is
    // denied at the Network allow-list check — no egress fires during pass 1 even
    // though the host offered a broad Network grant. (The load itself still fails
    // in pass 2 once effective = ∅ for the zero-declaring plugin.)
    assert_eq!(
        posts, 0,
        "a zero-declaring plugin's manifest export must NOT reach the network during \
         pass 1; observed {posts} POSTs. load_result = {load_result:?}"
    );
    assert!(
        egress.last_post.lock().unwrap().is_none(),
        "no POST body should have been recorded during bootstrap"
    );
}
