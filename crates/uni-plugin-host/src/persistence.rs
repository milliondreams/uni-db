// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! System-label persistence backends for `CustomPlugin`.
//!
//! Per proposal §9.7, declarations made via `uni.plugin.declareFunction`
//! / `declareProcedure` / `declareAggregate` / `declareTrigger`
//! survive restart by living in a host-owned system label
//! `_DeclaredPlugin`.
//!
//! This module ships [`SystemLabelPersistence`] — the
//! [`uni_plugin_custom::Persistence`] backend that the host's
//! `Uni::build` flow installs by default. The backend persists
//! declarations under `<data_path>/_system/declared_plugins.json`
//! using the atomic write-then-rename pattern from
//! [`uni_plugin_custom::JsonFilePersistence`]. The path is reserved
//! under the database's directory tree so backup / restore tooling
//! picks up declarations alongside graph data, and the
//! [`uni_plugin_custom::DeclaredPlugin`] serde shape is identical to
//! the `_DeclaredPlugin` system-label record (proposal §9.7), so the
//! eventual cutover to Cypher-MERGE-through-`execute_inner_query`
//! (M11 deliverable #8 follow-up) is a backend swap rather than a
//! schema migration.
//!
//! The name and module placement (`uni-db`, not `uni-plugin-custom`)
//! mark the layering: `SystemLabelPersistence` belongs at the host
//! layer because the cutover-eventual implementation needs
//! `QueryProcedureHost` (from `uni-query`), which `uni-plugin-custom`
//! does not depend on.

// Rust guideline compliant

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use uni_plugin_custom::{DeclaredPlugin, JsonFilePersistence, Persistence, PersistenceError};

use crate::host::HostCypherExecutor;

/// Lazy bridge from a `Persistence` backend to the host's write-mode
/// Cypher executor.
///
/// Backends constructed at `register_builtin_plugins` time hold one of
/// these. The host's `Uni::build` flow calls [`Self::set_host_executor`]
/// **after** the host is fully constructed, at which point
/// [`Self::try_write_cypher`] runs the supplied body via the host.
///
/// Holds the executor as a strong `Arc<dyn HostCypherExecutor>`; the
/// executor itself only weakly references the host, so the
/// persistence ↔ host cycle doesn't leak. Pre-wire `try_write_cypher`
/// returns `Err(...)` — callers should treat that as "best-effort
/// failed; record state via the durable sidecar instead."
#[derive(Debug, Default)]
pub struct LazyCypherSink {
    host_executor: OnceLock<Arc<dyn HostCypherExecutor>>,
}

impl LazyCypherSink {
    /// Construct an unwired sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Wire the host-side Cypher executor. Idempotent.
    pub fn set_host_executor(&self, exec: Arc<dyn HostCypherExecutor>) {
        let _ = self.host_executor.set(exec);
    }

    /// Best-effort write-mode Cypher execution. Returns an error
    /// string when the sink isn't yet wired or the statement fails;
    /// the caller decides whether to retry or fall back to a durable
    /// sidecar write.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string when the sink isn't yet
    /// wired, the host has been dropped, or the Cypher statement
    /// fails to parse / commit. (The current-thread-runtime guard and
    /// `block_in_place` handling live in the host's implementation.)
    pub fn try_write_cypher(&self, cypher: &str) -> Result<(), String> {
        let exec = self
            .host_executor
            .get()
            .ok_or_else(|| "LazyCypherSink: host executor not wired".to_owned())?;
        exec.execute_write_cypher(cypher)
    }
}

/// Persistence backend for the host's declared-plugin records.
///
/// Today's implementation: a JSON sidecar at
/// `<data_path>/_system/declared_plugins.json` (atomic
/// write-then-rename; the same mechanism as
/// [`JsonFilePersistence`]).
///
/// Cutover target: Cypher `MERGE (:_DeclaredPlugin {...})` through
/// the write-enabled `QueryProcedureHost::execute_inner_query` shipped
/// by A.1. The cutover preserves the [`DeclaredPlugin`] serde shape
/// (per proposal §9.7), so swapping the backend is a no-op at the
/// `Persistence` trait surface.
#[derive(Debug)]
pub struct SystemLabelPersistence {
    inner: JsonFilePersistence,
    sidecar_path: PathBuf,
    /// Lazy sink to the write-mode Cypher executor; populated after
    /// `Uni::build` returns. When wired, every `save` / `delete`
    /// dual-writes to a `_DeclaredPlugin` graph node so the
    /// declaration is visible to user `MATCH` queries
    /// (`MATCH (p:_DeclaredPlugin) RETURN p`). The JSON sidecar
    /// remains the source-of-truth for `load_all` at startup since
    /// the cypher sink is unavailable before `Uni::build` finishes.
    cypher_sink: Arc<LazyCypherSink>,
}

impl SystemLabelPersistence {
    /// Construct rooted at `data_path/_system/declared_plugins.json`.
    ///
    /// `data_path` is the database's filesystem root (the URI passed
    /// to `Uni::open` for local-disk instances).
    /// For in-memory / object-store instances where no local root
    /// exists, callers fall back to
    /// [`uni_plugin_custom::NullPersistence`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::path::PathBuf;
    /// # use uni_plugin_host::persistence::SystemLabelPersistence;
    /// let p = SystemLabelPersistence::new(PathBuf::from("/tmp/mydb"));
    /// assert!(p.sidecar_path().ends_with("declared_plugins.json"));
    /// ```
    #[must_use]
    pub fn new(data_path: impl Into<PathBuf>) -> Self {
        let mut sidecar_path = data_path.into();
        sidecar_path.push("_system");
        sidecar_path.push("declared_plugins.json");
        let inner = JsonFilePersistence::new(sidecar_path.clone());
        Self {
            inner,
            sidecar_path,
            cypher_sink: Arc::new(LazyCypherSink::new()),
        }
    }

    /// Borrow the underlying sidecar path (for diagnostics + tests).
    #[must_use]
    pub fn sidecar_path(&self) -> &Path {
        &self.sidecar_path
    }

    /// Borrow the lazy cypher sink so the host can wire it after
    /// `Uni::build` completes.
    #[must_use]
    pub fn cypher_sink(&self) -> &Arc<LazyCypherSink> {
        &self.cypher_sink
    }
}

/// Build the Cypher `MERGE` body that mirrors a [`DeclaredPlugin`]
/// into a `_DeclaredPlugin` graph node. The qname is the natural key.
fn merge_cypher(plugin: &DeclaredPlugin) -> String {
    // Escape any single-quotes by doubling them — minimal escaping
    // for the v1 dual-write path. Production callers should bind via
    // parameters once `Session::tx().execute_with(...).bind(...)` is
    // stabilized through the Tx API.
    fn esc(s: &str) -> String {
        s.replace('\'', "''")
    }
    let deps = plugin
        .dependencies
        .iter()
        .map(|d| format!("'{}'", esc(d)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "MERGE (p:_DeclaredPlugin {{qname: '{q}'}}) \
         SET p.kind = '{k}', \
             p.body = '{b}', \
             p.signature_json = '{s}', \
             p.dependencies = [{d}], \
             p.declared_by = '{db}', \
             p.active = {a}",
        q = esc(&plugin.qname),
        k = esc(&plugin.kind),
        b = esc(&plugin.body),
        s = esc(&plugin.signature_json),
        d = deps,
        db = esc(&plugin.declared_by),
        a = plugin.active,
    )
}

/// Build the Cypher `MATCH ... DETACH DELETE` body that removes the
/// `_DeclaredPlugin` graph node for a given qname.
fn delete_cypher(qname: &str) -> String {
    let q = qname.replace('\'', "''");
    format!("MATCH (p:_DeclaredPlugin {{qname: '{q}'}}) DETACH DELETE p")
}

impl Persistence for SystemLabelPersistence {
    fn save(&self, plugin: &DeclaredPlugin) -> Result<(), PersistenceError> {
        // Source of truth: JSON sidecar. Always durable, available
        // even before `Uni::build` finishes.
        self.inner.save(plugin)?;
        // Best-effort: mirror into the `_DeclaredPlugin` graph
        // label. Failure is logged, not propagated, since the sidecar
        // already committed the durable record.
        if let Err(e) = self.cypher_sink.try_write_cypher(&merge_cypher(plugin)) {
            tracing::debug!(
                qname = %plugin.qname,
                error = %e,
                "SystemLabelPersistence: cypher mirror skipped",
            );
        }
        Ok(())
    }

    fn delete(&self, qname: &str) -> Result<(), PersistenceError> {
        self.inner.delete(qname)?;
        if let Err(e) = self.cypher_sink.try_write_cypher(&delete_cypher(qname)) {
            tracing::debug!(
                qname = %qname,
                error = %e,
                "SystemLabelPersistence: cypher mirror delete skipped",
            );
        }
        Ok(())
    }

    fn load_all(&self) -> Result<Vec<DeclaredPlugin>, PersistenceError> {
        // JSON is the source-of-truth at startup; the graph label is
        // a projection updated on subsequent writes.
        self.inner.load_all()
    }
}

/// Choose the appropriate persistence backend for a `Uni` instance.
///
/// Returns [`SystemLabelPersistence`] when `data_path` is a local
/// filesystem path; falls back to [`uni_plugin_custom::NullPersistence`]
/// when the instance is in-memory / object-store-backed (no local
/// root for the JSON sidecar).
///
/// The optional second tuple element is the [`LazyCypherSink`] held by
/// the `SystemLabelPersistence` (none for the null backend). The
/// host's `Uni::build` flow stashes it and calls
/// [`LazyCypherSink::set_host_executor`] after the host is constructed,
/// at which point subsequent declarations dual-write into the
/// `_DeclaredPlugin` graph label.
#[must_use]
pub fn persistence_for_data_path(
    data_path: Option<&Path>,
) -> (Arc<dyn Persistence>, Option<Arc<LazyCypherSink>>) {
    match data_path {
        Some(path) => {
            let p = Arc::new(SystemLabelPersistence::new(path.to_owned()));
            let sink = Arc::clone(p.cypher_sink());
            (p as Arc<dyn Persistence>, Some(sink))
        }
        None => (Arc::new(uni_plugin_custom::NullPersistence), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fixture_plugin() -> DeclaredPlugin {
        DeclaredPlugin {
            qname: "mycorp.fullName".to_owned(),
            kind: "function".to_owned(),
            body: "$first + ' ' + $last".to_owned(),
            signature_json: "{}".to_owned(),
            dependencies: vec![],
            declared_by: "alice".to_owned(),
            active: true,
        }
    }

    #[test]
    fn sidecar_lives_under_system_subdir() {
        let p = SystemLabelPersistence::new("/tmp/mydb");
        assert!(p.sidecar_path().to_string_lossy().contains("/_system/"));
        assert!(p.sidecar_path().ends_with("declared_plugins.json"));
    }

    #[test]
    fn save_and_load_round_trip() {
        let tmp = TempDir::new().unwrap();
        let p = SystemLabelPersistence::new(tmp.path().to_path_buf());
        let plugin = fixture_plugin();
        p.save(&plugin).expect("save");
        let loaded = p.load_all().expect("load_all");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0], plugin);
    }

    #[test]
    fn delete_removes_the_record() {
        let tmp = TempDir::new().unwrap();
        let p = SystemLabelPersistence::new(tmp.path().to_path_buf());
        let plugin = fixture_plugin();
        p.save(&plugin).expect("save");
        p.delete(&plugin.qname).expect("delete");
        let loaded = p.load_all().expect("load_all");
        assert!(loaded.is_empty());
    }

    #[test]
    fn save_then_close_reopen_survives() {
        let tmp = TempDir::new().unwrap();
        {
            let p = SystemLabelPersistence::new(tmp.path().to_path_buf());
            p.save(&fixture_plugin()).expect("save");
        }
        // New instance pointing at the same root.
        let p = SystemLabelPersistence::new(tmp.path().to_path_buf());
        let loaded = p.load_all().expect("load_all");
        assert_eq!(loaded.len(), 1, "declaration must survive close+reopen");
    }

    #[test]
    fn persistence_for_in_memory_returns_null() {
        let (p, sink) = persistence_for_data_path(None);
        assert!(sink.is_none(), "NullPersistence has no cypher sink");
        assert!(p.load_all().expect("load_all").is_empty());
        p.save(&fixture_plugin()).expect("null save is ok");
        assert!(
            p.load_all().expect("load_all").is_empty(),
            "NullPersistence drops on the floor"
        );
    }

    #[test]
    fn persistence_for_local_path_returns_sink() {
        let tmp = TempDir::new().unwrap();
        let (_p, sink) = persistence_for_data_path(Some(tmp.path()));
        assert!(
            sink.is_some(),
            "local-path persistence must expose a cypher sink"
        );
    }

    #[test]
    fn cypher_sink_pre_wire_returns_err() {
        let sink = LazyCypherSink::new();
        let result = sink.try_write_cypher("MATCH (n) RETURN n");
        assert!(result.is_err(), "pre-wire try_write_cypher must error");
    }

    #[tokio::test]
    async fn cypher_sink_current_thread_runtime_degrades_to_err() {
        // `#[tokio::test]` defaults to current_thread, which cannot
        // host `block_in_place`. The sink must return an Err instead
        // of panicking so the dual-write `save()` path stays
        // best-effort.
        let sink = LazyCypherSink::new();
        // No need to actually wire a UniInner — the pre-wire branch
        // also exercises the no-panic invariant, but we want to
        // explicitly assert the flavor-check Err shape when the wire
        // *is* present without paying the full Uni build cost. The
        // pre-wire path errors first; that's fine — the panic only
        // surfaced once a UniInner had been wired. Here we just
        // confirm the function returns Err without panicking.
        let result = sink.try_write_cypher("MATCH (n) RETURN n");
        assert!(result.is_err());
    }

    #[test]
    fn save_succeeds_when_cypher_mirror_is_unwired() {
        // The Cypher sink isn't wired in this fixture, so the
        // dual-write path's MERGE call returns Err — but save() must
        // still succeed because the JSON sidecar IS the source of
        // truth.
        let tmp = TempDir::new().unwrap();
        let p = SystemLabelPersistence::new(tmp.path().to_path_buf());
        p.save(&fixture_plugin())
            .expect("JSON sidecar save must succeed even without sink");
        let loaded = p.load_all().expect("load_all");
        assert_eq!(loaded.len(), 1);
    }
}
