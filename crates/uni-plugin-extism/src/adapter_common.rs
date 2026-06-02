//! Shared helpers for the three Extism adapter modules.
//!
//! `acquire`, `extism_err_to_fn_err`, and the qname → export-symbol
//! sanitizer were copy-pasted across [`crate::adapter`],
//! [`crate::adapter_aggregate`], and [`crate::adapter_procedure`]. They
//! live here once so the lease/error/symbol conventions stay in lock-step
//! across the scalar, aggregate, and procedure adapters.

// Rust guideline compliant

use std::sync::Arc;

use uni_plugin::QName;
use uni_plugin::errors::FnError;
use uni_plugin_wasm_rt::IpcError;

use crate::pool::{ExtismInstancePool, PooledInstance};

/// Sanitize a qname into the `.`-free stem used in plugin export symbols.
///
/// Plugin authors expose `invoke_<stem>` / `agg_<stem>_*` /
/// `proc_<stem>_invoke` where every `.` in the qname is replaced by `_`,
/// because Rust identifiers cannot contain `.`. The mapping is
/// deterministic and shared by all three adapters so the host always
/// derives the same export symbol from a canonical qname.
///
/// # Examples
///
/// ```
/// use uni_plugin::QName;
/// use uni_plugin_extism::adapter_common::sanitize_qname;
///
/// let q = QName::parse("geo.haversine").unwrap();
/// assert_eq!(sanitize_qname(&q), "geo_haversine");
/// ```
#[must_use]
pub fn sanitize_qname(qname: &QName) -> String {
    qname.to_string().replace('.', "_")
}

/// Lease one warm instance from `pool`, mapping pool exhaustion to a
/// [`FnError`] carrying [`FnError::CODE_RESOURCE_LIMIT`].
///
/// # Errors
///
/// Returns [`FnError`] when the pool cannot hand out an instance (e.g.
/// `max_instances` reached).
pub fn acquire(
    pool: &Arc<ExtismInstancePool<extism::Plugin>>,
) -> Result<PooledInstance<extism::Plugin>, FnError> {
    PooledInstance::acquire(Arc::clone(pool)).map_err(|e| {
        FnError::new(
            FnError::CODE_RESOURCE_LIMIT,
            format!("acquire plugin instance: {e}"),
        )
    })
}

/// Map an Arrow-IPC boundary error to a [`FnError`] carrying
/// [`FnError::CODE_TYPE_COERCION`].
#[must_use]
pub fn extism_err_to_fn_err(e: IpcError) -> FnError {
    FnError::new(FnError::CODE_TYPE_COERCION, format!("extism IPC: {e}"))
}
