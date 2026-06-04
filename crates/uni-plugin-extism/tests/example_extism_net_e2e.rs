//! End-to-end test against the real built `example_extism_net.wasm`.
//!
//! Proves the Extism host-service cutover end to end — the coverage gap the
//! WASM Component path already closed via `example_wasm_net_e2e`. A plain-wasm
//! Extism guest that *imports* `uni_http_get` is loaded through the normal
//! scalar path, and its `invoke` calls back into the host's capability-gated
//! network egress. Covers all three gates:
//!
//!   1. **Granted + egress wired** — the call round-trips; the host's fake
//!      egress records the requested URL, and the guest sees the status.
//!   2. **Granted but no egress configured** — call-time loud failure.
//!   3. **Network not granted** — `uni_http_get` is omitted from the linker, so
//!      the guest's import is unresolved (link-time gating).
//!
//! Build the fixture first: `./scripts/build-wasm-fixtures.sh`.

// Rust guideline compliant

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::Float64Array;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::{
    Capability, CapabilitySet, FnError, HttpEgress, HttpResponse, PluginId, PluginRegistrar,
    PluginRegistry, QName,
};
use uni_plugin_extism::{ExtismLoader, register_default_host_svc};

const WASM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-extism-net/target/wasm32-unknown-unknown/release/example_extism_net.wasm",
);

fn load_wasm_bytes() -> Vec<u8> {
    std::fs::read(WASM_PATH).unwrap_or_else(|e| {
        panic!(
            "wasm artifact missing at {WASM_PATH}: {e}\n\
             Run `./scripts/build-wasm-fixtures.sh` from the repo root first."
        );
    })
}

/// Fake egress: records the last requested URL and returns a fixed status. No
/// real socket.
#[derive(Default)]
struct RecordingEgress {
    last_url: Mutex<Option<String>>,
}

impl HttpEgress for RecordingEgress {
    fn get(
        &self,
        url: &str,
        _timeout: Duration,
        _max_bytes: usize,
        _traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        *self.last_url.lock().unwrap() = Some(url.to_owned());
        Ok(HttpResponse {
            status: 200,
            body: b"pong".to_vec(),
        })
    }
    fn post(
        &self,
        url: &str,
        _body: &[u8],
        _timeout: Duration,
        _max_bytes: usize,
        _traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        *self.last_url.lock().unwrap() = Some(url.to_owned());
        Ok(HttpResponse {
            status: 200,
            body: Vec::new(),
        })
    }
}

/// The grant only needs the Network *variant*; the effective allow-list is the
/// intersection, which keeps the guest manifest's declared patterns.
fn network_grant() -> CapabilitySet {
    CapabilitySet::from_iter_of([
        Capability::ScalarFn,
        Capability::Network {
            allow: vec!["**".into()],
        },
    ])
}

fn invoke_status(registry: &PluginRegistry) -> Result<f64, String> {
    let qname = QName::parse("ai.example.net.fetch_status").expect("qname");
    let entry = registry.scalar_fn(&qname).expect("registered");
    let args = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0)))];
    let out = entry
        .function
        .invoke(&args, 1)
        .map_err(|e| format!("{e}"))?;
    let arr = match out {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => panic!("expected array, got {s:?}"),
    };
    let f = arr
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64Array");
    Ok(f.value(0))
}

#[test]
fn granted_with_egress_round_trips_through_uni_http_get() {
    let bytes = load_wasm_bytes();
    let egress = Arc::new(RecordingEgress::default());
    let mut loader = ExtismLoader::new().with_http(egress.clone());
    register_default_host_svc(&mut loader);

    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("extism.net"), &caps, &registry);
    let outcome = loader.load(&bytes, &network_grant(), &mut r).expect("load");
    r.commit_to_registry().expect("commit");

    assert_eq!(outcome.plugin_id, "ai.example.net");
    assert!(
        outcome
            .scalars_registered
            .iter()
            .any(|q| q == "ai.example.net.fetch_status"),
        "scalars_registered: {:?}",
        outcome.scalars_registered
    );

    let status = invoke_status(&registry).expect("invoke");
    assert!((status - 200.0).abs() < f64::EPSILON, "status: {status}");
    assert_eq!(
        egress.last_url.lock().unwrap().as_deref(),
        Some("https://api.example.com/ping"),
        "uni_http_get must have dispatched the guest's request"
    );
}

#[test]
fn granted_without_egress_fails_loudly_at_call_time() {
    let bytes = load_wasm_bytes();
    // host fns are registered (Network granted) but no egress is configured.
    let mut loader = ExtismLoader::new();
    register_default_host_svc(&mut loader);

    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("extism.net.noegress"), &caps, &registry);
    loader
        .load(&bytes, &network_grant(), &mut r)
        .expect("load (manifest reads; uni_http_get present but unconfigured)");
    r.commit_to_registry().expect("commit");

    // The host fn returns the loud "no HTTP egress configured" error; on the
    // Extism boundary a host-fn error aborts the guest call as a wasm trap, so
    // the host's invoke sees a failed `invoke_…` call rather than the verbatim
    // host-side string (this differs from the WASM Component path, whose typed
    // `fn-error` carries the message back). The security-relevant property —
    // an unconfigured egress fails loudly instead of silently succeeding — is
    // what we assert here.
    let err = invoke_status(&registry).expect_err("must fail loudly without egress");
    assert!(
        err.contains("invoke_ai_example_net_fetch_status") || err.to_lowercase().contains("egress"),
        "expected the host-net call to fail loudly when egress is unconfigured, got: {err}"
    );
}

#[test]
fn network_not_granted_fails_at_link_time() {
    let bytes = load_wasm_bytes();
    let egress = Arc::new(RecordingEgress::default());
    let mut loader = ExtismLoader::new().with_http(egress);
    register_default_host_svc(&mut loader);

    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("extism.net.ungranted"), &caps, &registry);
    // No Network in the grant → effective set lacks the Network variant →
    // `uni_http_get` is omitted from the linker → the guest's import cannot
    // resolve. Depending on the runtime this surfaces at instantiate (load) or
    // at first invoke; either is a link-time failure mentioning the missing fn.
    let scalar_caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let load_result = loader.load(&bytes, &scalar_caps, &mut r);
    let msg = match load_result {
        Err(e) => format!("{e}"),
        Ok(_) => {
            r.commit_to_registry().expect("commit");
            invoke_status(&registry).expect_err("ungranted call must fail")
        }
    };
    let lower = msg.to_lowercase();
    assert!(
        lower.contains("uni_http_get") || lower.contains("import") || lower.contains("function"),
        "expected a link-time import failure mentioning uni_http_get, got: {msg}"
    );
}
