// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Host implementation of [`uni_plugin_host::host::HostCypherExecutor`].
//!
//! The plugin-host engines (meta-plugin persistence mirror, background-job
//! scheduler) need to run write-mode Cypher against the live database. They
//! hold this executor as a trait object rather than reaching into `UniInner`
//! directly (which would invert the crate dependency).

use std::sync::Weak;

use uni_plugin_host::host::HostCypherExecutor;

use crate::api::UniInner;

/// [`HostCypherExecutor`] over a live [`UniInner`]: opens a fresh `Session` +
/// write `Transaction`, runs the statement, and commits.
///
/// Holds the host as `Weak<UniInner>` so the relationship (the executor is
/// owned by plugin-host sinks that are themselves reachable from `Uni`) does
/// not leak.
#[derive(Debug)]
pub struct UniInnerCypherExecutor {
    inner: Weak<UniInner>,
}

impl UniInnerCypherExecutor {
    /// Wrap a weak reference to the host.
    #[must_use]
    pub fn new(inner: Weak<UniInner>) -> Self {
        Self { inner }
    }
}

impl HostCypherExecutor for UniInnerCypherExecutor {
    fn execute_write_cypher(&self, cypher: &str) -> Result<(), String> {
        let inner = self
            .inner
            .upgrade()
            .ok_or_else(|| "UniInnerCypherExecutor: host inner dropped".to_owned())?;
        // `block_in_place` panics on a current-thread tokio runtime, which
        // `#[tokio::test]` uses by default. Cypher mirroring is best-effort —
        // degrade to a stable Err so callers fall back to the durable sidecar.
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|e| format!("UniInnerCypherExecutor: no tokio handle: {e}"))?;
        if !matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            return Err(
                "UniInnerCypherExecutor: current-thread runtime cannot block_in_place".to_owned(),
            );
        }
        let cypher = cypher.to_owned();
        tokio::task::block_in_place(|| {
            handle.block_on(async move {
                let session = crate::api::session::Session::new(inner);
                let tx = session.tx().await.map_err(|e| format!("open tx: {e}"))?;
                tx.execute(&cypher)
                    .await
                    .map_err(|e| format!("execute: {e}"))?;
                tx.commit().await.map_err(|e| format!("commit: {e}"))?;
                Ok::<(), String>(())
            })
        })
    }
}
