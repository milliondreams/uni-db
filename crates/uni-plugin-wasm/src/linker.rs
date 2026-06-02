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

use wasmtime::Engine;
use wasmtime::component::Linker;

use crate::error::WasmError;
use crate::host_state::HostState;

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
    // Capability-gated host fns would be added here based on
    // effective_caps. M6b ships scalar/aggregate/procedure with
    // only host-log; future commits add host-fs / host-net / …
    // gated on caps.
    let _ = effective_caps;
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
    // The bindgen-generated `scalar_host_log::add_to_linker_get_host`
    // registers `host-log` on the linker. Each linker call routes the
    // log into the host's `tracing` macros at the matching level.
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

fn emit_log(level: &str, message: &str) {
    match level {
        "error" => tracing::error!(target: "wasm-plugin", "{message}"),
        "warn" => tracing::warn!(target: "wasm-plugin", "{message}"),
        "info" => tracing::info!(target: "wasm-plugin", "{message}"),
        "debug" => tracing::debug!(target: "wasm-plugin", "{message}"),
        _ => tracing::trace!(target: "wasm-plugin", "{message}"),
    }
}
