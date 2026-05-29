#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M6a.3 acceptance — `Uni::start_connector` consults the registered
//! `Connector` chain, drives `start()` on a match, and routes
//! `stop_connector` through the same trait object.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use uni_db::{Uni, UniError};
use uni_plugin::errors::FnError;
use uni_plugin::traits::connector::{Connector, ConnectorConfig, ConnectorHandle};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

/// Tracking connector — counts start/stop, fails on demand to exercise
/// host-side error propagation.
struct CountingConnector {
    protocol: String,
    starts: AtomicUsize,
    stops: AtomicUsize,
    fail_start: bool,
    fail_stop: bool,
}

impl CountingConnector {
    fn new(protocol: &str) -> Self {
        Self {
            protocol: protocol.to_owned(),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            fail_start: false,
            fail_stop: false,
        }
    }
}

impl Connector for CountingConnector {
    fn protocol(&self) -> &str {
        &self.protocol
    }

    fn start(&self, _cfg: ConnectorConfig) -> Result<ConnectorHandle, FnError> {
        let n = self.starts.fetch_add(1, Ordering::SeqCst);
        if self.fail_start {
            return Err(FnError::new(0x1000, "start failure (test)"));
        }
        Ok(ConnectorHandle(n as u64))
    }

    fn stop(&self, _handle: ConnectorHandle) -> Result<(), FnError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        if self.fail_stop {
            return Err(FnError::new(0x1001, "stop failure (test)"));
        }
        Ok(())
    }
}

fn register_connector(
    uni: &Uni,
    plugin_id: &str,
    connector: Arc<dyn Connector>,
) -> std::result::Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Connector]);
    let mut r = PluginRegistrar::new(PluginId::new(plugin_id), &caps, registry);
    r.connector(connector)?;
    r.commit_to_registry()
}

#[tokio::test]
async fn start_connector_dispatches_to_registered_protocol() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let bolt = Arc::new(CountingConnector::new("bolt-test"));
    register_connector(&db, "test-conn-bolt", bolt.clone())?;

    let handle = db.start_connector("bolt-test", ConnectorConfig::default())?;
    assert_eq!(bolt.starts.load(Ordering::SeqCst), 1);
    assert!(bolt.stops.load(Ordering::SeqCst) == 0);

    let active = db.active_connectors();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].0, handle);
    assert_eq!(active[0].1, "bolt-test");

    db.stop_connector(handle)?;
    assert_eq!(bolt.stops.load(Ordering::SeqCst), 1);
    assert!(db.active_connectors().is_empty());
    Ok(())
}

#[tokio::test]
async fn start_connector_unknown_protocol_returns_invalid_argument() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let err = db
        .start_connector("nonexistent", ConnectorConfig::default())
        .expect_err("unknown protocol must fail");
    match err {
        UniError::InvalidArgument { arg, message } => {
            assert_eq!(arg, "protocol");
            assert!(message.contains("nonexistent"), "message: {message}");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn stop_connector_unknown_handle_returns_invalid_argument() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let err = db
        .stop_connector(99_999)
        .expect_err("unknown handle must fail");
    match err {
        UniError::InvalidArgument { arg, message } => {
            assert_eq!(arg, "host_handle");
            assert!(message.contains("99999"), "message: {message}");
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn host_handles_are_unique_across_multiple_starts() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let conn = Arc::new(CountingConnector::new("multi-test"));
    register_connector(&db, "test-conn-multi", conn.clone())?;

    let h1 = db.start_connector("multi-test", ConnectorConfig::default())?;
    let h2 = db.start_connector("multi-test", ConnectorConfig::default())?;
    let h3 = db.start_connector("multi-test", ConnectorConfig::default())?;
    assert_ne!(h1, h2);
    assert_ne!(h2, h3);
    assert_ne!(h1, h3);
    assert_eq!(db.active_connectors().len(), 3);

    // Stop in non-FIFO order to prove the map's host_handle is the key,
    // not the plugin-supplied ConnectorHandle.
    db.stop_connector(h2)?;
    assert_eq!(db.active_connectors().len(), 2);
    db.stop_connector(h1)?;
    db.stop_connector(h3)?;
    assert!(db.active_connectors().is_empty());
    assert_eq!(conn.stops.load(Ordering::SeqCst), 3);
    Ok(())
}
