// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Rule registry facade for managing pre-compiled Locy rules.

use std::sync::Arc;
use uni_common::Result;

use super::impl_locy::LocyRuleRegistry;

/// Metadata about a registered Locy rule.
#[derive(Debug, Clone)]
pub struct RuleInfo {
    /// Rule name.
    pub name: String,
    /// Number of clauses in the rule.
    pub clause_count: usize,
    /// Whether the rule is recursive.
    pub is_recursive: bool,
}

/// Facade for managing pre-compiled Locy rules.
///
/// Obtained via `db.rules()`, `session.rules()`, or `tx.rules()`.
/// Rules registered at the database level are cloned into every new Session.
pub struct RuleRegistry<'a> {
    registry: &'a Arc<std::sync::RwLock<LocyRuleRegistry>>,
}

impl<'a> RuleRegistry<'a> {
    pub fn new(registry: &'a Arc<std::sync::RwLock<LocyRuleRegistry>>) -> Self {
        Self { registry }
    }

    /// Register Locy rules from a program string.
    ///
    /// The program can contain multiple rule definitions. They are compiled
    /// and merged into the registry.
    pub fn register(&self, program: &str) -> Result<()> {
        super::impl_locy::register_rules_on_registry(self.registry, program)
    }

    /// Remove a rule by name.
    ///
    /// Returns `true` if the rule was found and removed, `false` if it didn't exist.
    /// Recompiles all remaining rules from source to rebuild strata ordering.
    pub fn remove(&self, name: &str) -> Result<bool> {
        let mut registry = self.registry.write().unwrap();

        if !registry.rules.contains_key(name) {
            return Ok(false);
        }

        // Save sources, then clear everything for recompilation
        let sources = registry.sources.clone();
        registry.rules.clear();
        registry.strata.clear();
        registry.sources.clear();
        drop(registry);

        // Re-register each source program, then remove the target rule from the result
        for source in &sources {
            let _ = super::impl_locy::register_rules_on_registry(self.registry, source);
        }

        // Remove the target rule from the compiled output
        let mut registry = self.registry.write().unwrap();
        registry.rules.remove(name);

        Ok(true)
    }

    /// List names of all registered rules.
    pub fn list(&self) -> Vec<String> {
        let registry = self.registry.read().unwrap();
        let mut names: Vec<String> = registry.rules.keys().cloned().collect();
        names.sort();
        names
    }

    /// Get metadata about a registered rule.
    pub fn get(&self, name: &str) -> Option<RuleInfo> {
        let registry = self.registry.read().unwrap();
        registry.rules.get(name).map(|rule| {
            // Check if this rule appears in a recursive stratum
            let is_recursive = registry
                .strata
                .iter()
                .any(|s| s.is_recursive && s.rules.iter().any(|r| r.name == name));
            RuleInfo {
                name: name.to_string(),
                clause_count: rule.clauses.len(),
                is_recursive,
            }
        })
    }

    /// Clear all registered rules.
    pub fn clear(&self) {
        let mut registry = self.registry.write().unwrap();
        registry.rules.clear();
        registry.strata.clear();
        registry.sources.clear();
    }

    /// Get the number of registered rules.
    pub fn count(&self) -> usize {
        let registry = self.registry.read().unwrap();
        registry.rules.len()
    }

    /// Clone the underlying registry Arc for use in Python bindings.
    ///
    /// This allows creating a `PyRuleRegistry` that outlives the borrow
    /// of `RuleRegistry<'a>`.
    pub fn clone_registry_arc(&self) -> Arc<std::sync::RwLock<LocyRuleRegistry>> {
        self.registry.clone()
    }
}
