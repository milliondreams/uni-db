// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Rule registry facade for managing pre-compiled Locy rules.

use std::sync::Arc;
use uni_common::{Result, UniError};

use super::impl_locy::LocyRuleRegistry;
use super::locy_rule_catalog::LocyRulePersister;

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
/// Obtained via `db.rules()`, `session.rules()`, or `tx.rules()`. Rules
/// registered at the database level are cloned into every new session and,
/// when a persister is attached (database-level handle only), persisted to
/// `catalog/locy_rules.json` so they survive restarts. Session-, transaction-,
/// and fork-level handles carry no persister and stay ephemeral.
///
/// Registry state is a pure function of the registered source programs:
/// every mutation rebuilds the compiled rules from source, so strata ids stay
/// dense and re-registering an identical program is a no-op.
pub struct RuleRegistry<'a> {
    registry: &'a Arc<std::sync::RwLock<LocyRuleRegistry>>,
    persister: Option<&'a Arc<LocyRulePersister>>,
}

impl<'a> RuleRegistry<'a> {
    /// Creates an ephemeral facade with no durable persistence.
    pub fn new(registry: &'a Arc<std::sync::RwLock<LocyRuleRegistry>>) -> Self {
        Self {
            registry,
            persister: None,
        }
    }

    /// Creates a facade that persists mutations through `persister`.
    pub fn with_persister(
        registry: &'a Arc<std::sync::RwLock<LocyRuleRegistry>>,
        persister: &'a Arc<LocyRulePersister>,
    ) -> Self {
        Self {
            registry,
            persister: Some(persister),
        }
    }

    /// Registers Locy rules from a program string.
    ///
    /// The program may contain multiple rule definitions; they are compiled
    /// and merged into the registry. Registering an exact-duplicate program is
    /// a no-op. When this handle carries a persister, a change is written to
    /// `catalog/locy_rules.json` before returning.
    ///
    /// # Errors
    ///
    /// Returns a parse or compile error for an invalid program, or an
    /// I/O error if persistence fails.
    pub async fn register(&self, program: &str) -> Result<()> {
        let changed = super::impl_locy::register_rules_on_registry(self.registry, program)?;
        if changed && let Some(persister) = self.persister {
            persister.save(self.registry).await?;
        }
        Ok(())
    }

    /// Removes a rule by name.
    ///
    /// Returns `true` if the rule was found and removed, `false` if it did not
    /// exist. The registry is rebuilt from the remaining sources, and a
    /// persister, if attached, writes the new state.
    ///
    /// Because registry state is a pure function of registered sources,
    /// removing a rule drops its entire source program. If that program also
    /// defined other rules, removal is rejected with an error rather than
    /// silently dropping the siblings; clear and re-register single-rule
    /// programs to remove one of them.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::Query`] if `name` shares its source program with
    /// other rules, or an I/O error if persistence fails.
    pub async fn remove(&self, name: &str) -> Result<bool> {
        let new_sources = {
            let registry = self.registry.read().unwrap();
            let Some(owning) = registry
                .sources
                .iter()
                .find(|s| s.rule_names.iter().any(|r| r == name))
            else {
                return Ok(false);
            };
            if owning.rule_names.len() > 1 {
                let siblings: Vec<&str> = owning
                    .rule_names
                    .iter()
                    .filter(|r| r.as_str() != name)
                    .map(String::as_str)
                    .collect();
                return Err(UniError::Query {
                    message: format!(
                        "cannot remove rule '{name}': it shares its source program with \
                         {siblings:?}. Clear the registry and re-register single-rule programs \
                         to remove one of them."
                    ),
                    query: None,
                });
            }
            registry
                .sources
                .iter()
                .filter(|s| !s.rule_names.iter().any(|r| r == name))
                .cloned()
                .collect::<Vec<_>>()
        };

        let rebuilt = super::impl_locy::rebuild_registry_from_sources(&new_sources)?;
        *self.registry.write().unwrap() = rebuilt;
        if let Some(persister) = self.persister {
            persister.save(self.registry).await?;
        }
        Ok(true)
    }

    /// Lists names of all registered rules, sorted.
    pub fn list(&self) -> Vec<String> {
        let registry = self.registry.read().unwrap();
        let mut names: Vec<String> = registry.rules.keys().cloned().collect();
        names.sort();
        names
    }

    /// Gets metadata about a registered rule.
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

    /// Clears all registered rules.
    ///
    /// When this handle carries a persister, the emptied registry is written
    /// to `catalog/locy_rules.json`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if persistence fails.
    pub async fn clear(&self) -> Result<()> {
        {
            let mut registry = self.registry.write().unwrap();
            *registry = LocyRuleRegistry::default();
        }
        if let Some(persister) = self.persister {
            persister.save(self.registry).await?;
        }
        Ok(())
    }

    /// Gets the number of registered rules.
    pub fn count(&self) -> usize {
        let registry = self.registry.read().unwrap();
        registry.rules.len()
    }

    /// Clones the underlying registry Arc for use in Python bindings.
    ///
    /// This allows creating a `PyRuleRegistry` that outlives the borrow
    /// of `RuleRegistry<'a>`.
    pub fn clone_registry_arc(&self) -> Arc<std::sync::RwLock<LocyRuleRegistry>> {
        self.registry.clone()
    }

    /// Clones the attached persister Arc, if any, for use in Python bindings.
    ///
    /// Session-, transaction-, and fork-level handles return `None`.
    pub fn clone_persister_arc(&self) -> Option<Arc<LocyRulePersister>> {
        self.persister.cloned()
    }
}
