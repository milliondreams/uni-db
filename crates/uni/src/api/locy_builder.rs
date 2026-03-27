// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;

use tokio_util::sync::CancellationToken;
use uni_common::{Result, Value};
use uni_locy::LocyConfig;

use crate::api::locy_result::LocyResult;

use crate::api::session::Session;
use crate::api::transaction::Transaction;

/// Builder for constructing and evaluating Locy programs via `LocyEngine` (UniInner-level).
///
/// Used by `LocyEngine::evaluate_with()` when only a `&UniInner` is available.
#[must_use = "InnerLocyBuilder does nothing until .run() is called"]
pub struct InnerLocyBuilder<'a> {
    db: &'a crate::api::UniInner,
    program: String,
    config: LocyConfig,
}

impl<'a> InnerLocyBuilder<'a> {
    pub(crate) fn new(db: &'a crate::api::UniInner, program: &str) -> Self {
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
    pub fn with_config(mut self, mut config: LocyConfig) -> Self {
        config.params.extend(self.config.params);
        self.config = config;
        self
    }

    /// Evaluate the program and return the full [`LocyResult`].
    pub async fn run(self) -> Result<LocyResult> {
        let engine = crate::api::impl_locy::LocyEngine {
            db: self.db,
            tx_l0_override: None,
            collect_derive: true,
        };
        engine
            .evaluate_with_config(&self.program, &self.config)
            .await
    }
}

/// Builder for constructing and evaluating Locy programs (Session-level).
///
/// Uses the session's rule registry for compilation and evaluation.
#[must_use = "SessionLocyBuilder does nothing until .run() is called"]
pub struct SessionLocyBuilder<'a> {
    session: &'a Session,
    program: String,
    config: LocyConfig,
    cancellation_token: Option<CancellationToken>,
}

impl<'a> SessionLocyBuilder<'a> {
    pub(crate) fn new(session: &'a Session, program: &str) -> Self {
        Self {
            session,
            program: program.to_string(),
            config: LocyConfig::default(),
            cancellation_token: None,
        }
    }

    /// Bind a single parameter.
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

    /// Attach a cancellation token for cooperative query cancellation.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Apply a fully configured [`LocyConfig`].
    pub fn with_config(mut self, mut config: LocyConfig) -> Self {
        config.params.extend(self.config.params);
        self.config = config;
        self
    }

    /// Evaluate the program and return the full [`LocyResult`].
    pub async fn run(self) -> Result<LocyResult> {
        crate::api::impl_locy::evaluate_with_db_and_config(
            &self.session.db,
            &self.program,
            &self.config,
            self.session.rule_registry(),
        )
        .await
    }
}

/// Builder for constructing and evaluating Locy programs (Transaction-level).
///
/// Uses the transaction's private L0 buffer so the Locy engine sees uncommitted
/// writes. DERIVE commands auto-apply to the private L0.
#[must_use = "TransactionLocyBuilder does nothing until .run() is called"]
pub struct TransactionLocyBuilder<'a> {
    tx: &'a Transaction,
    program: String,
    config: LocyConfig,
    cancellation_token: Option<CancellationToken>,
}

impl<'a> TransactionLocyBuilder<'a> {
    pub(crate) fn new(tx: &'a Transaction, program: &str) -> Self {
        Self {
            tx,
            program: program.to_string(),
            config: LocyConfig::default(),
            cancellation_token: None,
        }
    }

    /// Bind a single parameter.
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

    /// Attach a cancellation token for cooperative query cancellation.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Apply a fully configured [`LocyConfig`].
    pub fn with_config(mut self, mut config: LocyConfig) -> Self {
        config.params.extend(self.config.params);
        self.config = config;
        self
    }

    /// Evaluate the program and return the full [`LocyResult`].
    pub async fn run(self) -> Result<LocyResult> {
        let engine = crate::api::impl_locy::LocyEngine {
            db: &self.tx.db,
            tx_l0_override: Some(self.tx.tx_l0.clone()),
            collect_derive: false,
        };
        engine
            .evaluate_with_config(&self.program, &self.config)
            .await
    }
}
