//! Built-in scalar function registrations.
//!
//! Each submodule registers a category of scalar functions (string, math,
//! time, list, vector, …). The entry point is [`register_into`], called
//! once by `crate::BuiltinPlugin::register`.
//!
//! As of M2 only a placeholder identity function is wired; subsequent M2
//! commits migrate the existing built-ins from
//! `crates/uni-query/src/query/df_expr.rs:2130` and
//! `crates/uni-query/src/query/df_udfs.rs:79` into this module hierarchy.

use std::sync::{Arc, OnceLock};

use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling, ScalarPluginFn};
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName};

/// Register all built-in scalar functions into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a built-in qname is
/// already registered (would only happen on a misconfigured loader).
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.scalar_fn(
        QName::builtin("identity"),
        FnSignature {
            args: vec![ArgType::Primitive(DataType::Float64)],
            returns: ArgType::Primitive(DataType::Float64),
            volatility: Volatility::Immutable,
            null_handling: NullHandling::PropagateNulls,
        },
        Arc::new(Identity),
    )?;
    Ok(())
}

/// Placeholder identity scalar function used to smoke-test the
/// registration path. M2 replaces this with real migrations.
#[derive(Debug)]
struct Identity;

impl ScalarPluginFn for Identity {
    fn signature(&self) -> &FnSignature {
        static SIG: OnceLock<FnSignature> = OnceLock::new();
        SIG.get_or_init(|| FnSignature {
            args: vec![ArgType::Primitive(DataType::Float64)],
            returns: ArgType::Primitive(DataType::Float64),
            volatility: Volatility::Immutable,
            null_handling: NullHandling::PropagateNulls,
        })
    }

    fn invoke(&self, args: &[ColumnarValue], _rows: usize) -> Result<ColumnarValue, FnError> {
        args.first()
            .cloned()
            .ok_or_else(|| FnError::new(FnError::CODE_UNEXPECTED_NULL, "identity expected 1 arg"))
    }
}
