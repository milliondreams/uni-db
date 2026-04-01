// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! User-defined scalar function registry.
//!
//! Custom scalar functions can be registered at the database level and are
//! available in both the DataFusion columnar execution path and the direct
//! expression evaluator.

use std::collections::HashMap;
use std::sync::Arc;

use uni_common::{Result, Value};

/// Type alias for a custom scalar function.
///
/// The function receives evaluated arguments and returns a single value.
pub type CustomScalarFn = Arc<dyn Fn(&[Value]) -> Result<Value> + Send + Sync>;

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
