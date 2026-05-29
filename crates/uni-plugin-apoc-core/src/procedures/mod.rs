//! APOC-equivalent procedure registrations.
//!
//! Per the `docs/proposals/plugin_framework.md` APOC parity matrix, this
//! module hosts the Rust-implemented `apoc.*` analogues. Each submodule
//! covers one APOC namespace.
//!
//! Currently shipped:
//! - [`bitwise`] — `apoc.bitwise.*` analogue (and/or/xor/not/shiftLeft/shiftRight).
//! - [`text`] — `apoc.text.*` analogue (toUpper/toLower/replace/reverse).
//! - [`math`] — `apoc.math.*` analogue (sigmoid/tanh/cosh/sinh/coth).
//!
//! Planned (M4 onwards):
//! - `coll` — `apoc.coll.*` (collection helpers).
//! - `refactor` — `apoc.refactor.*` (needs internal mutation APIs).
//! - `atomic` — `apoc.atomic.*` (CAS retry contracts).
//! - `schema` / `meta` — schema and catalog introspection.

pub mod bitwise;
pub mod convert;
pub mod create;
pub mod math;
pub mod number;
pub mod text;

use uni_plugin::{PluginError, PluginRegistrar};

/// Register every APOC-core procedure into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    bitwise::register_into(r)?;
    text::register_into(r)?;
    math::register_into(r)?;
    number::register_into(r)?;
    convert::register_into(r)?;
    create::register_into(r)?;
    Ok(())
}
