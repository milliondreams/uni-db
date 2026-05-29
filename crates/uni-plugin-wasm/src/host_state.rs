//! Per-`Store` host state.
//!
//! Every wasmtime `Store<T>` carries a `T` that the host's `Linker`
//! references when wiring up imports. Our `T` is `HostState` — it
//! holds:
//!
//! - **WASI context** — most Rust→wasm32-wasip2 plugins import
//!   `wasi:cli`, `wasi:io`, `wasi:clocks`, etc., even when their
//!   user-facing logic doesn't need filesystem / network access
//!   (the standard library pulls these in transitively). Without
//!   `wasmtime-wasi` linked, instantiation fails with
//!   `component imports instance "wasi:io/poll@0.2.6", but a
//!   matching implementation was not found in the linker`.
//! - **Effective capability set** — so capability-gated host fns
//!   can dispatch on the granted set.

use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

/// State threaded through every wasmtime `Store<HostState>`.
pub struct HostState {
    /// Effective capability set granted to the plugin instance.
    pub effective_caps: Vec<String>,
    /// WASI context — minimal, no preopens, no inherited stdio.
    /// Plugins requesting filesystem / network access go through
    /// capability-gated host fns, not raw WASI preopens.
    pub wasi: WasiCtx,
    /// WASI resource table.
    pub table: ResourceTable,
}

impl HostState {
    /// Construct a fresh `HostState` with the given effective caps.
    ///
    /// The WASI context starts minimal — no preopens, no inherited
    /// stdio, no environment.
    #[must_use]
    pub fn new(effective_caps: Vec<String>) -> Self {
        let wasi = WasiCtxBuilder::new().build();
        Self {
            effective_caps,
            wasi,
            table: ResourceTable::new(),
        }
    }
}

impl std::fmt::Debug for HostState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostState")
            .field("effective_caps", &self.effective_caps)
            .finish_non_exhaustive()
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}
