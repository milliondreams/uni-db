// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Custom function facade for registering Cypher scalar functions.

use uni_common::{Result, UniError, Value};

use super::UniInner;

/// Facade for managing custom Cypher scalar functions.
///
/// Obtained via `db.functions()`. Functions are registered at the database
/// level and visible to all sessions.
pub struct Functions<'a> {
    pub(crate) inner: &'a UniInner,
}

impl Functions<'_> {
    /// Register a custom scalar function.
    ///
    /// If a function with the same name already exists, it is replaced.
    pub fn register<F>(&self, name: &str, func: F) -> Result<()>
    where
        F: Fn(&[Value]) -> Result<Value> + Send + Sync + 'static,
    {
        let mut registry = self.inner.custom_functions.write().map_err(|_| {
            UniError::Internal(anyhow::anyhow!("custom function registry lock poisoned"))
        })?;
        registry.register(name.to_string(), std::sync::Arc::new(func));
        Ok(())
    }

    /// Remove a custom function by name. Returns true if it existed.
    pub fn remove(&self, name: &str) -> Result<bool> {
        let mut registry = self.inner.custom_functions.write().map_err(|_| {
            UniError::Internal(anyhow::anyhow!("custom function registry lock poisoned"))
        })?;
        Ok(registry.remove(name))
    }

    /// List names of all registered custom functions.
    pub fn list(&self) -> Vec<String> {
        let registry = self.inner.custom_functions.read().unwrap();
        registry.iter().map(|(name, _)| name.to_string()).collect()
    }

    /// Get the number of registered custom functions.
    pub fn count(&self) -> usize {
        let registry = self.inner.custom_functions.read().unwrap();
        registry.iter().count()
    }
}
