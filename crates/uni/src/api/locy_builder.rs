// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;

use uni_common::{Result, Value};
use uni_locy::{LocyConfig, LocyResult};

use crate::api::Uni;

/// Builder for constructing and evaluating Locy programs.
///
/// Mirrors [`QueryBuilder`](crate::api::query_builder::QueryBuilder) for Cypher.
/// Supports parameter binding and all `LocyConfig` knobs via a fluent interface.
///
/// # Examples
///
/// ```no_run
/// # use uni_db::Uni;
/// # async fn example(db: &Uni) -> uni_db::Result<()> {
/// let result = db.locy()
///     .evaluate_with("CREATE RULE ep AS MATCH (e:Episode) WHERE e.agent_id = $aid YIELD KEY e")
///     .param("aid", "agent-123")
///     .run()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "LocyBuilder does nothing until .run() is called"]
pub struct LocyBuilder<'a> {
    db: &'a Uni,
    program: String,
    config: LocyConfig,
}

impl<'a> LocyBuilder<'a> {
    pub(crate) fn new(db: &'a Uni, program: &str) -> Self {
        Self {
            db,
            program: program.to_string(),
            config: LocyConfig::default(),
        }
    }

    /// Bind a single parameter.  The name should not include the `$` prefix.
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.config.params.insert(name.to_string(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.config.params.insert(k.to_string(), v);
        }
        self
    }

    /// Bind parameters from a `HashMap`.
    pub fn params_map(mut self, params: HashMap<String, Value>) -> Self {
        self.config.params.extend(params);
        self
    }

    /// Override the evaluation timeout.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.config.timeout = duration;
        self
    }

    /// Override the maximum fixpoint iteration count.
    pub fn max_iterations(mut self, n: usize) -> Self {
        self.config.max_iterations = n;
        self
    }

    /// Apply a fully configured [`LocyConfig`].
    ///
    /// Any parameters already set via `.param()` are merged on top of the
    /// supplied config (builder params take precedence).
    pub fn with_config(mut self, mut config: LocyConfig) -> Self {
        config.params.extend(self.config.params);
        self.config = config;
        self
    }

    /// Evaluate the program and return the full [`LocyResult`].
    pub async fn run(self) -> Result<LocyResult> {
        self.db
            .locy()
            .evaluate_with_config(&self.program, &self.config)
            .await
    }
}
