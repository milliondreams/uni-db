//! Built-in procedure registrations.
//!
//! The 50+ hardcoded procedures in
//! `crates/uni-query/src/query/df_graph/procedure_call.rs`
//! migrate into per-namespace submodules. APOC-equivalent namespaces
//! (`apoc.bitwise`, `apoc.text`, `apoc.coll`, …) live in
//! `uni-plugin-apoc-core` instead; this crate covers the closed-enum
//! retirement set only (`uni.admin.*`, `uni.schema.*`, `uni.vector.*`,
//! `uni.fts.*`, `uni.temporal.*`, `uni.algo.*` adapters).
//!
//! M4 scaffolding ships the module hierarchy plus one representative
//! procedure (`uni.system.echo`) that demonstrates the
//! [`uni_plugin::traits::procedure::ProcedurePlugin`] implementation
//! pattern. Subsequent commits port real built-ins one namespace at a
//! time, deleting the corresponding match arms in `procedure_call.rs`.

pub mod periodic;
pub mod system;

use arrow_array::{Array, StringArray};
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_plugin::{FnError, PluginError, PluginRegistrar};

/// Extract a non-null Utf8 argument at `idx` from a procedure's
/// `ColumnarValue` arguments.
///
/// Accepts both a `Utf8` scalar and the first row of a `StringArray`.
/// `prefix` names the procedure (e.g. `"uni.system.echo"`) and `field`
/// names the argument (e.g. `"message"`) for the error messages.
pub(crate) fn extract_utf8_arg(
    args: &[ColumnarValue],
    idx: usize,
    prefix: &str,
    field: &str,
) -> Result<String, FnError> {
    match args.get(idx) {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))) => Ok(s.clone()),
        Some(ColumnarValue::Array(arr)) => {
            let a = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("{prefix}: arg `{field}` must be Utf8"),
                )
            })?;
            if a.is_empty() || a.is_null(0) {
                Err(FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("{prefix}: arg `{field}` must not be null"),
                ))
            } else {
                Ok(a.value(0).to_owned())
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{prefix}: missing or non-Utf8 arg `{field}`"),
        )),
    }
}

/// Register all built-in procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a built-in qname is
/// already taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    system::register_into(r)?;
    // Subsequent commits add:
    // admin::register_into(r)?;
    // schema::register_into(r)?;
    // vector::register_into(r)?;
    // fts::register_into(r)?;
    // temporal::register_into(r)?;
    // algo::register_into(r)?;
    Ok(())
}
