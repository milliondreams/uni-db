// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! GraphCompute kernel-dispatch host function for Extism guests.
//!
//! A guest algorithm imports a single host fn, `uni_graph_call`, and issues one
//! kernel call per invocation as a JSON string (proposal §4.5). The host resolves
//! the guest's session by id and dispatches the kernel through the shared
//! [`GraphComputeRegistry`](uni_plugin_builtin::algorithms::graph_compute::GraphComputeRegistry),
//! returning the result (handle / scalar / error) as JSON. Only handles and
//! scalars cross the boundary — the "conductor, not worker" property that makes
//! the same guest portable across loaders.
//
// Rust guideline compliant

use uni_plugin::FnError;

use super::HostSvcCtx;

/// Dispatches one GraphCompute kernel call for a guest, JSON in / JSON out.
///
/// # Errors
/// Returns [`FnError`] `0xC33` only if no graph registry was configured on the
/// loader; per-kernel errors (bad handle, drained budget, …) are reported
/// in-band inside the returned JSON, never as a host error, so a hostile guest
/// cannot crash the worker (proposal §5.4).
pub(crate) fn do_graph_call(ctx: &HostSvcCtx, req_json: &str) -> Result<String, FnError> {
    match &ctx.graph {
        Some(registry) => Ok(registry.call_json(req_json)),
        None => Err(FnError::new(
            0xC33,
            "uni_graph_call: no GraphCompute registry configured on the loader",
        )),
    }
}

// The `host_fn!`-generated shell is thin: recover the ctx, run `do_graph_call`.
extism::host_fn!(pub(crate) uni_graph_call(ctx: HostSvcCtx; req_json: String) -> String {
    let bundle = ctx.get()?;
    let bundle = bundle
        .lock()
        .map_err(|_| extism::Error::msg("uni_graph_call: host service ctx poisoned"))?;
    do_graph_call(&bundle, &req_json).map_err(|e| extism::Error::msg(e.to_string()))
});
