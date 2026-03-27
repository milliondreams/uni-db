// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Session templates — pre-configured session factories.
//!
//! Templates allow pre-compiling rules, binding parameters, and attaching hooks
//! once at startup, then cheaply stamping out sessions per-request.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use uni_common::{Result, Value};

use crate::api::UniInner;
use crate::api::hooks::SessionHook;
use crate::api::impl_locy::{self, LocyRuleRegistry};
use crate::api::session::Session;

/// A pre-configured session factory.
///
/// Templates pre-compile Locy rules and store parameters and hooks so that
/// `create()` is a cheap, synchronous clone operation.
///
/// # Examples
///
/// ```no_run
/// # use uni_db::Uni;
/// # async fn example(db: &Uni) -> uni_db::Result<()> {
/// let template = db.session_template()
///     .param("tenant", 42)
///     .rules("edge_rule(X,Y) :- knows(X,Y).")?
///     .query_timeout(std::time::Duration::from_secs(30))
///     .build()?;
///
/// // Cheap per-request session creation:
/// let session = template.create();
/// # Ok(())
/// # }
/// ```
pub struct SessionTemplate {
    db: Arc<UniInner>,
    params: HashMap<String, Value>,
    rule_registry: LocyRuleRegistry,
    hooks: Vec<Arc<dyn SessionHook>>,
    pub(crate) query_timeout: Option<Duration>,
    pub(crate) transaction_timeout: Option<Duration>,
}

impl SessionTemplate {
    /// Create a new session from this template.
    ///
    /// This is cheap and synchronous — rules are cloned (not recompiled),
    /// parameters are cloned, and hooks are Arc-cloned.
    pub fn create(&self) -> Session {
        Session::new_from_template(
            self.db.clone(),
            self.params.clone(),
            self.rule_registry.clone(),
            self.hooks.clone(),
            self.query_timeout,
            self.transaction_timeout,
        )
    }
}

/// Builder for constructing a [`SessionTemplate`].
pub struct SessionTemplateBuilder {
    db: Arc<UniInner>,
    params: HashMap<String, Value>,
    rule_registry: LocyRuleRegistry,
    hooks: Vec<Arc<dyn SessionHook>>,
    query_timeout: Option<Duration>,
    transaction_timeout: Option<Duration>,
}

impl SessionTemplateBuilder {
    pub(crate) fn new(db: Arc<UniInner>) -> Self {
        // Start from the global rule registry
        let global_registry = db.locy_rule_registry.read().unwrap();
        let registry = global_registry.clone();
        drop(global_registry);

        Self {
            db,
            params: HashMap::new(),
            rule_registry: registry,
            hooks: Vec::new(),
            query_timeout: None,
            transaction_timeout: None,
        }
    }

    /// Bind a parameter that all sessions created from this template will inherit.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Pre-compile Locy rules into the template.
    ///
    /// Rules are compiled eagerly so that `create()` only needs a cheap clone.
    pub fn rules(mut self, program: &str) -> Result<Self> {
        let temp_registry = Arc::new(std::sync::RwLock::new(self.rule_registry.clone()));
        impl_locy::register_rules_on_registry(&temp_registry, program)?;
        self.rule_registry = temp_registry.read().unwrap().clone();
        Ok(self)
    }

    /// Attach a hook that all sessions created from this template will inherit.
    pub fn hook(mut self, hook: impl SessionHook + 'static) -> Self {
        self.hooks.push(Arc::new(hook));
        self
    }

    /// Set the default query timeout for sessions created from this template.
    pub fn query_timeout(mut self, duration: Duration) -> Self {
        self.query_timeout = Some(duration);
        self
    }

    /// Set the default transaction timeout for sessions created from this template.
    pub fn transaction_timeout(mut self, duration: Duration) -> Self {
        self.transaction_timeout = Some(duration);
        self
    }

    /// Build the session template.
    pub fn build(self) -> Result<SessionTemplate> {
        Ok(SessionTemplate {
            db: self.db,
            params: self.params,
            rule_registry: self.rule_registry,
            hooks: self.hooks,
            query_timeout: self.query_timeout,
            transaction_timeout: self.transaction_timeout,
        })
    }
}
