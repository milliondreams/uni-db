//! End-to-end test against the real built `example_wasm_net.wasm`.
//!
//! Proves the `host-net` cutover end to end: a Component Model guest that
//! *imports* `uni:plugin/host-net` is loaded through the normal scalar path,
//! and its `invoke-scalar` calls back into the host's capability-gated network
//! egress. Covers all three gates:
//!
//!   1. **Granted + egress wired** — the call round-trips; the host's fake
//!      egress records the requested URL and the injected traceparent.
//!   2. **Granted but no egress configured** — call-time loud failure.
//!   3. **Network not granted** — the guest's `host-net` import is absent from
//!      the linker, so the component fails to instantiate (link-time gating).
//!
//! Build the fixture first: `./scripts/build-wasm-fixtures.sh`.

// Rust guideline compliant

use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::{
    Capability, CapabilitySet, FnError, HttpEgress, HttpResponse, PluginId, PluginRegistrar,
    PluginRegistry, QName,
};
use uni_plugin_wasm::WasmLoader;

const WASM_MODULE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-wasm-net/target/wasm32-wasip2/release/example_wasm_net.wasm",
);

fn load_component_bytes() -> Vec<u8> {
    std::fs::read(WASM_MODULE_PATH).unwrap_or_else(|e| {
        panic!(
            "wasm component missing at {WASM_MODULE_PATH}: {e}\n\
             Run `./scripts/build-wasm-fixtures.sh` from the repo root first."
        );
    })
}

/// Fake egress: records the last request (url + traceparent) and returns a
/// fixed status + body. No real socket.
#[derive(Default)]
struct RecordingEgress {
    last_url: Mutex<Option<String>>,
    last_traceparent: Mutex<Option<String>>,
}

impl HttpEgress for RecordingEgress {
    fn get(
        &self,
        url: &str,
        _timeout: Duration,
        _max_bytes: usize,
        traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        *self.last_url.lock().unwrap() = Some(url.to_owned());
        *self.last_traceparent.lock().unwrap() = traceparent.map(str::to_owned);
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
        traceparent: Option<&str>,
    ) -> Result<HttpResponse, FnError> {
        *self.last_url.lock().unwrap() = Some(url.to_owned());
        *self.last_traceparent.lock().unwrap() = traceparent.map(str::to_owned);
        Ok(HttpResponse {
            status: 200,
            body: Vec::new(),
        })
    }
}

fn network_grant() -> CapabilitySet {
    // The grant only needs the Network *variant*; the effective allow-list is
    // the intersection, which keeps the plugin's declared patterns.
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
        .downcast_ref::<arrow_array::Float64Array>()
        .expect("Float64Array");
    Ok(f.value(0))
}

#[test]
fn granted_with_egress_round_trips_through_host_net() {
    let bytes = load_component_bytes();
    let egress = Arc::new(RecordingEgress::default());
    let loader = WasmLoader::new().with_http(egress.clone());

    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("wasm.net"), &caps, &registry);
    let outcome = loader.load(&bytes, &network_grant(), &mut r).expect("load");
    r.commit_to_registry().expect("commit");

    assert_eq!(outcome.plugin_id, "ai.example.net");
    assert!(
        outcome
            .scalars_registered
            .iter()
            .any(|q| q == "ai.example.net.fetch_status")
    );

    let status = invoke_status(&registry).expect("invoke");
    assert!((status - 200.0).abs() < f64::EPSILON, "status: {status}");
    assert_eq!(
        egress.last_url.lock().unwrap().as_deref(),
        Some("https://api.example.com/ping"),
        "host-net must have dispatched the guest's request"
    );
    // otel feature is off in this test build → no active trace → None injected
    // (no fabricated id). The injection *path* is exercised; the value is None.
    assert_eq!(*egress.last_traceparent.lock().unwrap(), None);
}

#[test]
fn granted_without_egress_fails_loudly_at_call_time() {
    let bytes = load_component_bytes();
    // host-net is linked (Network granted) but no egress is configured.
    let loader = WasmLoader::new();

    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("wasm.net.noegress"), &caps, &registry);
    loader
        .load(&bytes, &network_grant(), &mut r)
        .expect("load (manifest reads; host-net present but unconfigured)");
    r.commit_to_registry().expect("commit");

    let err = invoke_status(&registry).expect_err("must fail without egress");
    assert!(
        err.contains("no HTTP egress configured"),
        "expected loud egress error, got: {err}"
    );
}

#[test]
fn network_not_granted_fails_at_link_time() {
    let bytes = load_component_bytes();
    let egress = Arc::new(RecordingEgress::default());
    let loader = WasmLoader::new().with_http(egress);

    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let mut r = PluginRegistrar::new(PluginId::new("wasm.net.ungranted"), &caps, &registry);
    // No Network in the grant set → host-net is absent from the linker → the
    // guest's `uni:plugin/host-net` import cannot resolve → instantiate fails.
    let err = loader
        .load(
            &bytes,
            &CapabilitySet::from_iter_of([Capability::ScalarFn]),
            &mut r,
        )
        .expect_err("must fail: host-net import unresolved");
    let msg = format!("{err}");
    assert!(
        msg.contains("host-net") || msg.to_lowercase().contains("import"),
        "expected a link-time import failure mentioning host-net, got: {msg}"
    );
}
