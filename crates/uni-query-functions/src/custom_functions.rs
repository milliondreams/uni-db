// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! User-defined scalar function registry.
//!
//! Custom scalar functions can be registered at the database level and are
//! available in both the DataFusion columnar execution path and the direct
//! expression evaluator.
//!
//! This is a dependency-light leaf type: it stores `(name, fn)` pairs and
//! nothing else. The plugin-framework shadow (`uni_plugin::PluginRegistry`)
//! that the DataFusion adapter consumes is built on demand by
//! `uni_query::query::df_udfs_plugin::plugin_registry_for_custom_functions`,
//! which lives in `uni-query` because it depends on `uni-plugin`.

use std::collections::HashMap;
use std::sync::Arc;

use uni_common::{Result, Value};

/// Type alias for a custom scalar function.
///
/// The function receives evaluated arguments and returns a single value.
pub type CustomScalarFn = Arc<dyn Fn(&[Value]) -> Result<Value> + Send + Sync>;

/// Reserved plugin id used for legacy `CustomFunctionRegistry` registrations
/// when they are mirrored into a `uni_plugin::PluginRegistry` shadow.
///
/// All legacy `register(name, fn)` calls land under this id in the shadow
/// registry built by the `uni-query` plugin adapter. User plugins may not
/// use this id. Defined here (a plain `&str` constant, no plugin dependency)
/// so both the leaf crate and the host crate share the single source of
/// truth.
pub const LEGACY_USER_PLUGIN_ID: &str = "user.legacy";

/// Registry of user-defined scalar functions.
///
/// Functions are stored with uppercased names for case-insensitive lookup.
#[derive(Default, Clone)]
pub struct CustomFunctionRegistry {
    functions: HashMap<String, CustomScalarFn>,
}

impl CustomFunctionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a custom scalar function.
    ///
    /// If a function with the same name already exists, it is replaced.
    pub fn register(&mut self, name: String, func: CustomScalarFn) {
        self.functions.insert(name.to_uppercase(), func);
    }

    /// Look up a custom function by name (case-insensitive).
    pub fn get(&self, name: &str) -> Option<&CustomScalarFn> {
        self.functions.get(&name.to_uppercase())
    }

    /// Iterate over all registered functions.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &CustomScalarFn)> {
        self.functions.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Remove a custom function by name. Returns true if it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        self.functions.remove(&name.to_uppercase()).is_some()
    }

    /// Returns `true` if no functions are registered.
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

impl std::fmt::Debug for CustomFunctionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomFunctionRegistry")
            .field("functions", &self.functions.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_register_and_get_roundtrip() {
        let mut reg = CustomFunctionRegistry::new();
        let f: CustomScalarFn = Arc::new(|_args: &[Value]| Ok(Value::String("ok".to_owned())));
        reg.register("MYFN".to_owned(), f);

        // Case-insensitive lookup.
        assert!(reg.get("myfn").is_some());
        assert!(reg.get("MYFN").is_some());
    }

    #[test]
    fn legacy_remove_clears_entry() {
        let mut reg = CustomFunctionRegistry::new();
        let f: CustomScalarFn = Arc::new(|_| Ok(Value::Null));
        reg.register("MYFN".to_owned(), f);
        assert!(reg.remove("myfn"));
        assert!(reg.get("MYFN").is_none());
    }

    #[test]
    fn legacy_replace_updates_entry() {
        let mut reg = CustomFunctionRegistry::new();
        reg.register(
            "MYFN".to_owned(),
            Arc::new(|_| Ok(Value::String("first".to_owned()))),
        );
        reg.register(
            "MYFN".to_owned(),
            Arc::new(|_| Ok(Value::String("second".to_owned()))),
        );

        let v = (reg.get("MYFN").unwrap())(&[]).unwrap();
        assert_eq!(v, Value::String("second".to_owned()));
    }
}
