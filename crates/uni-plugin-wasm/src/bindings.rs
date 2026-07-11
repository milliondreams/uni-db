//! wasmtime-generated bindings for the `uni:plugin` WIT worlds.
//!
//! `wasmtime::component::bindgen!` walks the `wit/` directory at
//! compile time and emits typed wrappers for each `world`. The
//! generated types live in submodules named after the world.
//!
//! Downstream adapters import e.g.
//! `crate::bindings::scalar_plugin::ScalarPlugin` to get the typed
//! `invoke_scalar(qname, ipc) -> Result<Vec<u8>, FnError>` wrapper.

#![allow(missing_docs)]
#![allow(missing_debug_implementations)]
#![allow(clippy::all)]

/// Scalar plugin world. Implements one or more Cypher scalar fns
/// over the Arrow IPC bytes wire format.
pub mod scalar {
    wasmtime::component::bindgen!({
        world: "scalar-plugin",
        path: "wit",
    });
}

/// Aggregate plugin world. Implements Cypher aggregates with opaque
/// state passed between calls.
pub mod aggregate {
    wasmtime::component::bindgen!({
        world: "aggregate-plugin",
        path: "wit",
    });
}

/// Procedure plugin world. Implements `CALL ... YIELD ...`
/// procedures returning zero or more `yields`-shaped batches.
pub mod procedure {
    wasmtime::component::bindgen!({
        world: "procedure-plugin",
        path: "wit",
    });
}

/// GraphCompute algorithm plugin world. The guest drives coarse kernels
/// through the imported `host-graph` interface and emits its result.
pub mod algorithm {
    wasmtime::component::bindgen!({
        world: "algorithm-plugin",
        path: "wit",
    });
}
