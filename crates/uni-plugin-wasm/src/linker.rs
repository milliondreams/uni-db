//! Per-major `Linker` builder for the scalar-plugin world.
//!
//! Each `world` in the WIT package needs its own typed `Linker<HostState>`
//! that wires up the host-side implementations of every `import`. For
//! the scalar/aggregate/procedure worlds the imports are minimal:
//!
//!   - **`host-log`** — always-available; routes plugin tracing into
//!     the host's `tracing` macros.
//!
//! Capability-gated host fns (`host-fs-*`, `host-net-*`, `host-kms-*`,
//! …) are intentionally NOT added to the linker unless the plugin's
//! effective-capability set includes the matching capability. A plugin
//! that imports an absent host fn fails at `Linker::instantiate_pre`
//! time — the structural half of capability enforcement (proposal
//! §5.6.2). The runtime check (`HostState::effective_caps` consulted
//! inside the host fn body) is defense-in-depth.
//!
//! Per-major versioning: this module currently builds linkers for
//! `uni:plugin@0.1`. When `0.2` ships an ABI-breaking change, this
//! file gains a second `build_scalar_linker_v2` and the loader
//! consults the plugin's declared `abi` field to choose.

use std::time::Duration;

use uni_plugin::Capability;
use wasmtime::Engine;
use wasmtime::component::{ComponentType, Lift, Linker, Lower};

use crate::error::WasmError;
use crate::host_state::HostState;

/// Default per-call HTTP timeout ceiling when the grant carries no
/// `WallClockMillisPerCall`. The guest may request *less* via `timeout-ms`.
const DEFAULT_TIMEOUT_MS: u64 = 10_000;

/// Maximum response body bytes the host will read — bounds host memory
/// regardless of the guest's `max-bytes` request.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Host mirror of the WIT `host-net.http-response` record.
///
/// Hand-derived (rather than bindgen-generated) so the capability-gated
/// `host-net` interface stays out of the always-linked plugin worlds; the
/// canonical ABI matches the WIT structurally (field names + order + types).
#[derive(ComponentType, Lower, Lift)]
#[component(record)]
struct WasmHttpResponse {
    status: u16,
    body: Vec<u8>,
}

/// Host mirror of the shared WIT `types.fn-error` record.
#[derive(ComponentType, Lower, Lift)]
#[component(record)]
struct WasmFnError {
    code: u32,
    message: String,
    retryable: bool,
}

fn fn_err(code: u32, message: impl Into<String>) -> WasmFnError {
    WasmFnError {
        code,
        message: message.into(),
        retryable: false,
    }
}

/// Build the `Linker<HostState>` for the `scalar-plugin` world.
///
/// Adds `host-log` (always-available) plus any capability-gated
/// host fns whose required capability is present in
/// `effective_caps`. WASI is added unconditionally so plugins
/// compiled against `wasm32-wasip2` can resolve their toolchain
/// imports (the actual `host-fs` / `host-net` access still goes
/// through our capability-gated host fns, not raw WASI preopens).
///
/// # Errors
///
/// - [`WasmError::Instantiate`] if any host-fn registration fails
///   (e.g., interface mismatch with the bindings).
pub fn build_scalar_linker_v1(
    engine: &Engine,
    effective_caps: &uni_plugin::CapabilitySet,
) -> Result<Linker<HostState>, WasmError> {
    let mut linker = Linker::<HostState>::new(engine);
    // Wire WASI Preview 2 imports. The standard Rust→wasm32-wasip2
    // toolchain emits components that import `wasi:cli`, `wasi:io`,
    // `wasi:clocks`, etc. — even pure-compute plugins pull them in
    // transitively via std. Without this, instantiation fails with
    // "component imports instance `wasi:io/poll@0.2.6`, but a
    // matching implementation was not found in the linker".
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
        .map_err(|e| WasmError::Instantiate(format!("link wasi: {e}")))?;
    add_host_log(&mut linker)?;
    // `host-trace-context` is always available — it returns only the host's
    // own W3C traceparent (or none), leaking no capability-gated state.
    add_host_trace_context(&mut linker)?;
    // `host-net` is added only when `Capability::Network` is granted. A plugin
    // importing `uni:plugin/host-net` without the grant therefore fails at
    // `instantiate` (linker absence) — the structural half of enforcement; the
    // URL allow-list is the call-time half, checked inside the host fn body.
    if effective_caps
        .iter()
        .any(|c| matches!(c, Capability::Network { .. }))
    {
        add_host_net(&mut linker)?;
    }
    // `host-graph` is added here too (when granted) so the bootstrap pass — which
    // instantiates any plugin against the scalar linker to read its `manifest` /
    // `register` exports — can satisfy an algorithm component's `host-graph`
    // import. The algorithm pool uses its own linker for the invoke path.
    if effective_caps
        .iter()
        .any(|c| matches!(c, Capability::GraphCompute))
    {
        add_host_graph(&mut linker)?;
    }
    Ok(linker)
}

/// Build the `Linker<HostState>` for the `algorithm-plugin` world.
///
/// Adds WASI + `host-log` + `host-trace-context` like the other worlds, plus the
/// capability-gated `host-graph` interface when `Capability::GraphCompute` is
/// granted. A guest importing `host-graph` without the grant fails at
/// instantiate (structural enforcement), exactly like `host-net`.
///
/// # Errors
/// [`WasmError::Instantiate`] if any host-fn registration fails.
pub fn build_algorithm_linker_v1(
    engine: &Engine,
    effective_caps: &uni_plugin::CapabilitySet,
) -> Result<Linker<HostState>, WasmError> {
    let mut linker = Linker::<HostState>::new(engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
        .map_err(|e| WasmError::Instantiate(format!("link wasi: {e}")))?;
    add_host_log(&mut linker)?;
    add_host_trace_context(&mut linker)?;
    if effective_caps
        .iter()
        .any(|c| matches!(c, Capability::GraphCompute))
    {
        add_host_graph(&mut linker)?;
    }
    Ok(linker)
}

/// Back-compat shim — delegates to [`build_scalar_linker_v1`].
///
/// The original single-major entry point. Kept so call sites built
/// before the per-major split keep compiling unchanged. New callers
/// should go through [`crate::multi_version::MultiVersionLinker`] so
/// the major selection reflects the plugin's declared `abi` range.
pub fn build_scalar_linker(
    engine: &Engine,
    effective_caps: &uni_plugin::CapabilitySet,
) -> Result<Linker<HostState>, WasmError> {
    build_scalar_linker_v1(engine, effective_caps)
}

/// Build the `Linker<HostState>` for the **v2** scalar-plugin world.
///
/// v2 is a placeholder: no v2-only host fn is defined yet, so the
/// linker is identical to v1 plus a second `host-log-v2` instance the
/// `multi_version_abi` test fixture uses to confirm dispatch picks
/// the right linker.
///
/// # Errors
///
/// - [`WasmError::Instantiate`] if host-fn registration fails.
pub fn build_scalar_linker_v2(
    engine: &Engine,
    effective_caps: &uni_plugin::CapabilitySet,
) -> Result<Linker<HostState>, WasmError> {
    let mut linker = build_scalar_linker_v1(engine, effective_caps)?;
    // v2 differentiation marker: an additional `host-log-v2` interface
    // the test harness uses to assert the right linker was selected.
    let mut instance = linker
        .instance("uni:plugin/host-log-v2")
        .map_err(|e| WasmError::Instantiate(format!("link host-log-v2 instance: {e}")))?;
    instance
        .func_wrap(
            "log",
            |_store: wasmtime::StoreContextMut<'_, HostState>,
             (level, message): (String, String)|
             -> wasmtime::Result<()> {
                emit_log(&level, &format!("[v2] {message}"));
                Ok(())
            },
        )
        .map_err(|e| WasmError::Instantiate(format!("link host-log-v2: {e}")))?;
    Ok(linker)
}

fn add_host_log(linker: &mut Linker<HostState>) -> Result<(), WasmError> {
    // Hand-roll the `uni:plugin/host-log` interface with `func_wrap`
    // (no bindgen helper): the single `log` function routes the guest's
    // message into the host's `tracing` macros at the matching level.
    let mut instance = linker
        .instance("uni:plugin/host-log")
        .map_err(|e| WasmError::Instantiate(format!("link host-log instance: {e}")))?;
    instance
        .func_wrap(
            "log",
            |_store: wasmtime::StoreContextMut<'_, HostState>,
             (level, message): (String, String)|
             -> wasmtime::Result<()> {
                emit_log(&level, &message);
                Ok(())
            },
        )
        .map_err(|e| WasmError::Instantiate(format!("link host-log: {e}")))?;
    Ok(())
}

/// Add the capability-gated `uni:plugin/host-net` interface.
///
/// Both functions read the egress + effective caps from `store.data()`
/// (`HostState`), enforce the URL allow-list, clamp the timeout / size to the
/// granted ceiling, inject the host traceparent, and dispatch through the
/// shared [`uni_plugin::HttpEgress`]. Unlike the Rhai/Extism loaders this
/// returns the response (incl. a `>= 400` status) to the guest rather than
/// erroring — the typed `http-response.status` field exists precisely so the
/// guest can branch on it.
fn add_host_net(linker: &mut Linker<HostState>) -> Result<(), WasmError> {
    // The instance name MUST carry the package version: a guest that actually
    // *calls* host-net imports `uni:plugin/host-net@0.1.0` (an unused import is
    // tree-shaken away, which is why host-log's unversioned name never had to
    // match a real import).
    let mut instance = linker
        .instance("uni:plugin/host-net@0.1.0")
        .map_err(|e| WasmError::Instantiate(format!("link host-net instance: {e}")))?;
    instance
        .func_wrap(
            "http-get",
            |store: wasmtime::StoreContextMut<'_, HostState>,
             (url, timeout_ms, max_bytes): (String, u64, u32)|
             -> wasmtime::Result<(Result<WasmHttpResponse, WasmFnError>,)> {
                Ok((host_http(store.data(), &url, None, timeout_ms, max_bytes),))
            },
        )
        .map_err(|e| WasmError::Instantiate(format!("link host-net http-get: {e}")))?;
    instance
        .func_wrap(
            "http-post",
            |store: wasmtime::StoreContextMut<'_, HostState>,
             (url, body, timeout_ms, max_bytes): (String, Vec<u8>, u64, u32)|
             -> wasmtime::Result<(Result<WasmHttpResponse, WasmFnError>,)> {
                Ok((host_http(
                    store.data(),
                    &url,
                    Some(body),
                    timeout_ms,
                    max_bytes,
                ),))
            },
        )
        .map_err(|e| WasmError::Instantiate(format!("link host-net http-post: {e}")))?;
    Ok(())
}

/// Add the capability-gated `uni:plugin/host-graph` interface.
///
/// The single `graph-call` function dispatches one GraphCompute kernel call
/// through the session registry on `HostState`, returning the JSON response.
/// Per-kernel errors are reported in-band inside the JSON (never a WIT-level
/// error), so only a missing registry surfaces as `fn-error` (proposal §5.4).
fn add_host_graph(linker: &mut Linker<HostState>) -> Result<(), WasmError> {
    let mut instance = linker
        .instance("uni:plugin/host-graph@0.1.0")
        .map_err(|e| WasmError::Instantiate(format!("link host-graph instance: {e}")))?;
    instance
        .func_wrap(
            "graph-call",
            |store: wasmtime::StoreContextMut<'_, HostState>,
             (req,): (String,)|
             -> wasmtime::Result<(Result<String, WasmFnError>,)> {
                let result = match &store.data().graph {
                    Some(registry) => Ok(registry.call_json(&req)),
                    None => Err(fn_err(
                        0x86C,
                        "host-graph: no GraphCompute registry configured",
                    )),
                };
                Ok((result,))
            },
        )
        .map_err(|e| WasmError::Instantiate(format!("link host-graph graph-call: {e}")))?;
    Ok(())
}

/// Add the always-available `uni:plugin/host-trace-context` interface.
fn add_host_trace_context(linker: &mut Linker<HostState>) -> Result<(), WasmError> {
    let mut instance = linker
        .instance("uni:plugin/host-trace-context@0.1.0")
        .map_err(|e| WasmError::Instantiate(format!("link host-trace-context instance: {e}")))?;
    instance
        .func_wrap(
            "get-traceparent",
            |_store: wasmtime::StoreContextMut<'_, HostState>,
             (): ()|
             -> wasmtime::Result<(Option<String>,)> {
                Ok((uni_plugin::observability::current_trace_context().to_traceparent(),))
            },
        )
        .map_err(|e| {
            WasmError::Instantiate(format!("link host-trace-context get-traceparent: {e}"))
        })?;
    Ok(())
}

/// Shared `host-net` GET/POST body: enforce the allow-list, resolve bounds,
/// inject the traceparent, and dispatch to the host's [`uni_plugin::HttpEgress`].
fn host_http(
    state: &HostState,
    url: &str,
    body: Option<Vec<u8>>,
    timeout_ms: u64,
    max_bytes: u32,
) -> Result<WasmHttpResponse, WasmFnError> {
    if !state.effective.iter().any(|c| c.network_allows(url)) {
        return Err(fn_err(
            0xD20,
            format!("host-net: url `{url}` not in granted Network allow-list"),
        ));
    }
    let Some(egress) = state.http.as_ref() else {
        return Err(fn_err(0xD21, "host-net: no HTTP egress configured"));
    };
    // Host-authoritative bounds: the granted ceiling (cap `WallClockMillisPerCall`
    // else default) caps the guest-requested timeout; the response size is hard
    // capped regardless of the guest's request.
    let ceiling_ms = state
        .effective
        .iter()
        .find_map(|c| match c {
            Capability::WallClockMillisPerCall(ms) => Some(*ms),
            _ => None,
        })
        .unwrap_or(DEFAULT_TIMEOUT_MS);
    let timeout = Duration::from_millis(if timeout_ms == 0 {
        ceiling_ms
    } else {
        timeout_ms.min(ceiling_ms)
    });
    let max = if max_bytes == 0 {
        MAX_RESPONSE_BYTES
    } else {
        (max_bytes as usize).min(MAX_RESPONSE_BYTES)
    };
    // Propagate the host's trace context (real only in `otel`-enabled builds;
    // `None` otherwise — no fabricated trace ids).
    let traceparent = uni_plugin::observability::current_trace_context().to_traceparent();
    let tp = traceparent.as_deref();
    let response = match body {
        Some(b) => egress.post(url, &b, timeout, max, tp),
        None => egress.get(url, timeout, max, tp),
    }
    .map_err(|e| fn_err(0xD22, format!("host-net(`{url}`): {e}")))?;
    Ok(WasmHttpResponse {
        status: response.status,
        body: response.body,
    })
}

fn emit_log(level: &str, message: &str) {
    match level {
        "error" => tracing::error!(target: "wasm-plugin", "{message}"),
        "warn" => tracing::warn!(target: "wasm-plugin", "{message}"),
        "info" => tracing::info!(target: "wasm-plugin", "{message}"),
        "debug" => tracing::debug!(target: "wasm-plugin", "{message}"),
        _ => tracing::trace!(target: "wasm-plugin", "{message}"),
    }
}
