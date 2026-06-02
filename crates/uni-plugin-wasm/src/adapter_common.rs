//! Helpers shared by the scalar / aggregate / procedure adapters.
//!
//! These are crate-local (the same names live in `uni-plugin-extism`, but the
//! two crates keep independent copies rather than coupling through the shared
//! `uni-plugin` crate): the IPC-error→[`FnError`] mapper and the pool-acquire
//! helper used identically across [`crate::adapter`],
//! [`crate::adapter_aggregate`], and [`crate::adapter_procedure`].

use std::sync::Arc;

use uni_plugin::errors::FnError;
use uni_plugin_wasm_rt::IpcError;

use crate::pool::{PooledInstance, WasmInstancePool};

/// Map an Arrow-IPC boundary error to a type-coercion [`FnError`].
pub(crate) fn ipc_to_fn_err(e: IpcError) -> FnError {
    FnError::new(FnError::CODE_TYPE_COERCION, format!("wasm IPC: {e}"))
}

/// Lease a warm instance from `pool`, mapping pool exhaustion to a
/// resource-limit [`FnError`]. `label` names the surface for diagnostics
/// (e.g. `"aggregate"`, `"procedure"`, `"plugin"`).
pub(crate) fn acquire<I: Send + 'static>(
    pool: &Arc<WasmInstancePool<I>>,
    label: &str,
) -> Result<PooledInstance<I>, FnError> {
    PooledInstance::acquire(Arc::clone(pool)).map_err(|e| {
        FnError::new(
            FnError::CODE_RESOURCE_LIMIT,
            format!("acquire {label} instance: {e}"),
        )
    })
}
