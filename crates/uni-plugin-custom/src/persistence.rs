// Rust guideline compliant
//! Persistence backends for declared-plugin records.
//!
//! M9 stores declarations in a [`DeclaredPluginStore`](super::DeclaredPluginStore)
//! in memory, but the user-visible promise of `apoc.custom`-style
//! `uni.plugin.declareFunction` is that declarations *survive restart*.
//!
//! Proposal §9.7 anchors the persistence schema in a Cypher-visible
//! system label `_DeclaredPlugin`. That label requires write-enabled
//! [`uni_plugin::traits::procedure::ProcedureHost`] execution, which
//! does not exist yet (the host's `execute_inner_query` is read-only
//! and does not bind parameters). Rather than block M9 on that
//! infrastructure, this module ships a [`Persistence`] trait with two
//! concrete implementations:
//!
//! - [`NullPersistence`] — drops declarations on the floor; used in
//!   tests that exercise only the in-memory store.
//! - [`JsonFilePersistence`] — round-trips the [`DeclaredPlugin`]
//!   serde shape through a JSON sidecar file under the instance's
//!   data directory.
//!
//! The schema matches proposal §9.7 field-for-field, so the eventual
//! cutover to `_DeclaredPlugin` (when write-enabled host execution
//! lands) is a drop-in `impl Persistence for SystemLabelPersistence`.

use std::io;
use std::path::PathBuf;
use std::sync::Mutex;

use thiserror::Error;
use uni_sidecar::{SidecarIoError, SystemSidecar};

use crate::DeclaredPlugin;

/// Errors raised by [`Persistence`] backends.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PersistenceError {
    /// I/O failure while reading or writing the sidecar.
    #[error("persistence I/O: {0}")]
    Io(#[from] io::Error),

    /// JSON encode / decode failure.
    #[error("persistence serde: {0}")]
    Serde(#[from] serde_json::Error),
}

impl From<SidecarIoError> for PersistenceError {
    fn from(e: SidecarIoError) -> Self {
        // The sidecar's variants already carry path + cause in their Display;
        // fold them into the I/O arm (the message preserves the detail).
        PersistenceError::Io(io::Error::other(e.to_string()))
    }
}

/// A persistence backend for declared-plugin records.
///
/// Implementations must be `Send + Sync` because the
/// [`crate::CustomPlugin`] holds an `Arc<dyn Persistence>` shared
/// across procedure invocations on every session thread.
pub trait Persistence: Send + Sync + std::fmt::Debug {
    /// Persist a freshly-declared plugin record.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] on I/O or serialization failure.
    fn save(&self, plugin: &DeclaredPlugin) -> Result<(), PersistenceError>;

    /// Remove a previously persisted record by qname.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] on I/O or serialization failure.
    fn delete(&self, qname: &str) -> Result<(), PersistenceError>;

    /// Replay every persisted declaration (in any order — callers
    /// must topologically sort if dependency ordering matters).
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] on I/O or deserialization failure.
    fn load_all(&self) -> Result<Vec<DeclaredPlugin>, PersistenceError>;
}

/// In-memory persistence that drops every record on the floor.
///
/// Used by tests and by `CustomPlugin::new_in_memory()` when the host
/// does not provide a data directory.
#[derive(Debug, Default)]
pub struct NullPersistence;

impl Persistence for NullPersistence {
    fn save(&self, _plugin: &DeclaredPlugin) -> Result<(), PersistenceError> {
        Ok(())
    }

    fn delete(&self, _qname: &str) -> Result<(), PersistenceError> {
        Ok(())
    }

    fn load_all(&self) -> Result<Vec<DeclaredPlugin>, PersistenceError> {
        Ok(Vec::new())
    }
}

/// On-disk JSON-sidecar persistence.
///
/// Records are stored as a JSON array on a single file under the
/// configured path. Reads parse the whole file; writes serialize the
/// whole array. This is intentionally simple — declared plugins are
/// metadata, not throughput-sensitive.
///
/// File format (proposal §9.7 schema, JSON-encoded):
///
/// ```json
/// [
///   {
///     "qname": "mycorp.fullName",
///     "kind": "Function",
///     "body": "$first + ' ' + $last",
///     "signature_json": "{...}",
///     "dependencies": [],
///     "declared_by": "alice",
///     "active": true
///   }
/// ]
/// ```
///
/// The cutover to `_DeclaredPlugin` system-label persistence (proposal
/// §9.7) leaves this struct unchanged — the wire schema is identical.
#[derive(Debug)]
pub struct JsonFilePersistence {
    /// Atomic JSON IO (temp + fsync + rename + parent-dir fsync), shared with
    /// the `uni-plugin-host` persisters via [`uni_sidecar`].
    sidecar: SystemSidecar<Vec<DeclaredPlugin>>,
    /// Serializes the read-modify-write in `save` / `delete`.
    write_guard: Mutex<()>,
}

impl JsonFilePersistence {
    /// Construct a persistence backend at the exact `path`.
    ///
    /// The file is created on first write. If it does not exist at
    /// construction time, [`Self::load_all`] returns an empty vector. The path
    /// is used verbatim (via [`SystemSidecar::at_path`]) so existing
    /// `declared_plugins.json` files keep loading across an upgrade.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            sidecar: SystemSidecar::at_path(path),
            write_guard: Mutex::new(()),
        }
    }
}

impl Persistence for JsonFilePersistence {
    fn save(&self, plugin: &DeclaredPlugin) -> Result<(), PersistenceError> {
        let _guard = self.write_guard.lock().expect("persistence mutex poisoned");
        let mut plugins = self.sidecar.load()?;
        if let Some(slot) = plugins.iter_mut().find(|p| p.qname == plugin.qname) {
            *slot = plugin.clone();
        } else {
            plugins.push(plugin.clone());
        }
        self.sidecar.store(&plugins)?;
        Ok(())
    }

    fn delete(&self, qname: &str) -> Result<(), PersistenceError> {
        let _guard = self.write_guard.lock().expect("persistence mutex poisoned");
        let mut plugins = self.sidecar.load()?;
        plugins.retain(|p| p.qname != qname);
        self.sidecar.store(&plugins)?;
        Ok(())
    }

    fn load_all(&self) -> Result<Vec<DeclaredPlugin>, PersistenceError> {
        let _guard = self.write_guard.lock().expect("persistence mutex poisoned");
        Ok(self.sidecar.load()?)
    }
}
