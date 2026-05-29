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

use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use thiserror::Error;

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
    path: PathBuf,
    write_guard: Mutex<()>,
}

impl JsonFilePersistence {
    /// Construct a persistence backend rooted at `path`.
    ///
    /// The file is created on first write. If it does not exist at
    /// construction time, [`Self::load_all`] returns an empty vector.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            write_guard: Mutex::new(()),
        }
    }

    /// Read the sidecar into a vector.
    fn read_all_unlocked(&self) -> Result<Vec<DeclaredPlugin>, PersistenceError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(&self.path)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let plugins: Vec<DeclaredPlugin> = serde_json::from_slice(&bytes)?;
        Ok(plugins)
    }

    /// Write the vector back to disk atomically (write-then-rename).
    fn write_all_unlocked(&self, plugins: &[DeclaredPlugin]) -> Result<(), PersistenceError> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_vec_pretty(plugins)?;
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&json)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

impl Persistence for JsonFilePersistence {
    fn save(&self, plugin: &DeclaredPlugin) -> Result<(), PersistenceError> {
        let _guard = self.write_guard.lock().expect("persistence mutex poisoned");
        let mut plugins = self.read_all_unlocked()?;
        if let Some(slot) = plugins.iter_mut().find(|p| p.qname == plugin.qname) {
            *slot = plugin.clone();
        } else {
            plugins.push(plugin.clone());
        }
        self.write_all_unlocked(&plugins)
    }

    fn delete(&self, qname: &str) -> Result<(), PersistenceError> {
        let _guard = self.write_guard.lock().expect("persistence mutex poisoned");
        let mut plugins = self.read_all_unlocked()?;
        plugins.retain(|p| p.qname != qname);
        self.write_all_unlocked(&plugins)
    }

    fn load_all(&self) -> Result<Vec<DeclaredPlugin>, PersistenceError> {
        let _guard = self.write_guard.lock().expect("persistence mutex poisoned");
        self.read_all_unlocked()
    }
}

/// Persistence record schema variant — used to attach a typed wrapper
/// for round-trip tests.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistenceEnvelope {
    /// Schema version. Bumped on incompatible changes.
    pub schema_version: u32,
    /// The plugin record.
    pub plugin: DeclaredPlugin,
}
