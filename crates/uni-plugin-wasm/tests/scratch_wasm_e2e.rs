// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mode B-seq WASM guest e2e (plugin-compute proposal §7b).
//!
//! Drives a real `wasm32-wasip2` guest (`example-wasm-scratch`) through wasmtime,
//! backing its imported `host-graph` interface with a
//! [`ScratchRegistry`](uni_plugin_builtin::algorithms::graph_compute::ScratchRegistry).
//! The host opens a session-local scratch graph, hands the guest its id, the
//! guest builds and walks the mutable graph purely through `graph-call` JSON ops,
//! and the host reads the guest's returned summary — the compiled-body (WASM)
//! arm of the Mode B-seq guest binding (`Q-6`), analogous to the Mode-A per-loader
//! `L` family.
//!
//! The fixture is built by `scripts/build-wasm-fixtures.sh`; a missing artifact
//! panics with a build hint (no silent skip — the `e9e3784a1` freshness lesson).
#![cfg(feature = "wasmtime-runtime")]

use std::sync::Arc;

use uni_plugin_builtin::algorithms::graph_compute::scratch::{ScratchGraph, ScratchRegistry};
use uni_plugin_builtin::algorithms::graph_compute::{Arena, WorkBudget};
use wasmtime::component::{Component, ComponentType, Lift, Linker, Lower};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

// Host mirror of the guest's `host-graph` `result<string, fn-error>` error arm.
// The canonical ABI matches the WIT record structurally (field names/order/types).
#[derive(ComponentType, Lower, Lift)]
#[component(record)]
struct WasmFnError {
    code: u32,
    message: String,
    retryable: bool,
}

// Host bindings for the guest's `scratch-guest` world (export `run`, import
// `host-graph`). We satisfy the `host-graph` import manually (mirroring the
// loader's `add_host_graph`), so only the export side is used from these.
wasmtime::component::bindgen!({
    inline: "
        package uni:scratch@0.1.0;
        interface types { record fn-error { code: u32, message: string, retryable: bool } }
        interface host-graph {
            use types.{fn-error};
            graph-call: func(req: string) -> result<string, fn-error>;
        }
        world scratch-guest {
            import host-graph;
            export run: func(session: u64) -> string;
        }
    ",
});

/// Store state: a WASI context plus the scratch registry backing `host-graph`.
struct HarnessState {
    registry: Arc<ScratchRegistry>,
    wasi: WasiCtx,
    table: ResourceTable,
}

impl WasiView for HarnessState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

const WASM_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/example-wasm-scratch/target/wasm32-wasip2/release/example_wasm_scratch.wasm"
);

fn load_wasm() -> Vec<u8> {
    std::fs::read(WASM_PATH).unwrap_or_else(|e| {
        panic!(
            "missing WASM scratch fixture at {WASM_PATH}: {e}\n\
             Run ./scripts/build-wasm-fixtures.sh"
        )
    })
}

#[test]
fn wasm_guest_drives_the_scratch_graph_abi() {
    let bytes = load_wasm();
    let registry = Arc::new(ScratchRegistry::new());

    let mut cfg = Config::new();
    cfg.wasm_component_model(true);
    let engine = Engine::new(&cfg).expect("engine");
    let component = Component::from_binary(&engine, &bytes).expect("component compiles");

    // Linker: WASI plus the manually-wired `host-graph` import → scratch registry.
    let mut linker: Linker<HarnessState> = Linker::new(&engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker).expect("wasi linked");
    linker
        .instance("uni:scratch/host-graph@0.1.0")
        .expect("host-graph instance")
        .func_wrap(
            "graph-call",
            |store: wasmtime::StoreContextMut<'_, HarnessState>,
             (req,): (String,)|
             -> wasmtime::Result<(Result<String, WasmFnError>,)> {
                // Per-op errors ride in-band inside the JSON, so this never
                // returns the WIT error arm (proposal §5.4).
                Ok((Ok(store.data().registry.call_json(&req)),))
            },
        )
        .expect("host-graph graph-call linked");

    let mut store = Store::new(
        &engine,
        HarnessState {
            registry: Arc::clone(&registry),
            wasi: WasiCtxBuilder::new().build(),
            table: ResourceTable::new(),
        },
    );

    // The host opens the session-local scratch graph under a budget/arena; the
    // guest receives only its id.
    let sid = registry.open(ScratchGraph::new(
        WorkBudget::new(1_000_000),
        Arena::new(1 << 20, 1 << 20),
        0x5C4A,
    ));

    let bindings =
        ScratchGuest::instantiate(&mut store, &component, &linker).expect("guest instantiates");
    let summary_json = bindings
        .call_run(&mut store, sid)
        .expect("guest run() succeeds");

    // The guest built `0->1,0->2,1->3`, set field[1]=42, sampled at p=1.0.
    let summary: serde_json::Value =
        serde_json::from_str(&summary_json).expect("guest returns JSON");
    assert_eq!(summary["nodes"].as_u64(), Some(4), "guest added 4 nodes");
    assert_eq!(summary["field1"].as_f64(), Some(42.0), "guest set field[1]");
    assert_eq!(summary["deg0"].as_u64(), Some(2), "node 0 has out-degree 2");
    assert_eq!(
        summary["sampled"].as_bool(),
        Some(true),
        "sample(p=1.0) fires"
    );

    // Host closes the session and reads the mutated graph back — it saw exactly
    // what the guest built through the ABI.
    let graph = registry.close(sid).expect("session closes");
    assert_eq!(graph.node_count(), 4);
    assert_eq!(graph.edge_count(), 3);
}
