//! Filesystem host fns — gated by [`Capability::Filesystem`].
//!
//! `uni_fs_read(path) -> string` reads a UTF-8 file; `uni_fs_write(path, data)`
//! writes one. Enforcement is layered:
//!
//! - **Layer 2 (link-time):** the fns are only registered on engines whose
//!   effective capability set holds a `Filesystem` variant.
//! - **Layer 3a (lexical):** each call's `path` is normalized via
//!   [`uni_plugin::normalize_capability_path`] — relative paths and `..`
//!   escapes above the root are refused — and the **normalized** path is
//!   matched against the grant's `read` / `write` glob allow-list *before* any
//!   filesystem access (so a disallowed path cannot even probe existence).
//! - **Layer 3b (symlink hardening):** the path is then canonicalized
//!   (resolving symlinks to the real target; for writes, the parent dir is
//!   canonicalized so a symlinked directory cannot redirect a create) and the
//!   **resolved** path is re-matched against the allow-list. The syscall acts on
//!   that resolved path, so the checked path and the acted-upon path are
//!   identical — closing both `..` traversal and symlink escape.

#![cfg(feature = "rhai-runtime")]

use std::path::PathBuf;
use std::sync::Arc;

use rhai::Engine;
use uni_plugin::{Capability, CapabilitySet, normalize_capability_path};

use crate::host_fn_impls::rt_err;
use crate::host_fns::{RhaiHostFnRegistry, RhaiHostFnSpec};
use crate::loader::RhaiLoader;

/// Register `uni_fs_read` and `uni_fs_write` on the loader's host-fn
/// registry. The engine factory only registers them on engines whose
/// effective capability set contains a `Filesystem` variant.
pub fn register(loader: &mut RhaiLoader) {
    let placeholder = Capability::Filesystem {
        read: vec!["**".into()],
        write: vec!["**".into()],
    };
    loader.host_fns_mut().register(RhaiHostFnSpec::gated(
        "uni.fs.read",
        placeholder.clone(),
        "Read a UTF-8 file from the host filesystem.",
        register_fs_read,
    ));
    loader.host_fns_mut().register(RhaiHostFnSpec::gated(
        "uni.fs.write",
        placeholder,
        "Write a UTF-8 string to a host filesystem path.",
        register_fs_write,
    ));
}

fn register_fs_read(engine: &mut Engine, caps: &CapabilitySet) {
    let caps = caps.clone();
    engine.register_fn(
        "uni_fs_read",
        move |path: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            let resolved = resolve_read_path(&caps, path)?;
            std::fs::read_to_string(&resolved)
                .map_err(|e| rt_err(format!("uni.fs.read({path}): {e}")))
        },
    );
}

fn register_fs_write(engine: &mut Engine, caps: &CapabilitySet) {
    let caps = caps.clone();
    engine.register_fn(
        "uni_fs_write",
        move |path: &str, data: &str| -> Result<(), Box<rhai::EvalAltResult>> {
            let resolved = resolve_write_path(&caps, path)?;
            std::fs::write(&resolved, data)
                .map_err(|e| rt_err(format!("uni.fs.write({path}): {e}")))
        },
    );
}

/// Resolve a read path to a canonical, symlink-free target that the `read`
/// allow-list admits, or error.
///
/// Order matters for security: the allow-list is matched on the lexically
/// normalized path *before* any filesystem access (so a disallowed path cannot
/// probe existence via `canonicalize` errors), then the canonicalized (symlink-
/// resolved) path is re-matched so a symlink inside an allowed tree cannot
/// redirect the read outside it.
///
/// # Errors
/// Errors if `path` is relative / escapes the root, is outside the granted
/// `read` allow-list (before or after symlink resolution), or cannot be
/// canonicalized (e.g. does not exist).
fn resolve_read_path(
    caps: &CapabilitySet,
    path: &str,
) -> Result<PathBuf, Box<rhai::EvalAltResult>> {
    let norm = normalize_capability_path(path).ok_or_else(|| {
        rt_err(format!(
            "uni.fs.read: path `{path}` is not an absolute, traversal-free path"
        ))
    })?;
    if !caps
        .iter()
        .any(|c| c.filesystem_read_allows(&norm.to_string_lossy()))
    {
        return Err(rt_err(format!(
            "uni.fs.read: path `{path}` not in granted Filesystem read allow-list"
        )));
    }
    let canonical =
        std::fs::canonicalize(&norm).map_err(|e| rt_err(format!("uni.fs.read({path}): {e}")))?;
    if !caps
        .iter()
        .any(|c| c.filesystem_read_allows(&canonical.to_string_lossy()))
    {
        return Err(rt_err(format!(
            "uni.fs.read: path `{path}` resolves outside the granted Filesystem read allow-list"
        )));
    }
    Ok(canonical)
}

/// Resolve a write path to a target that the `write` allow-list admits, or
/// error.
///
/// The target may not exist yet, so the existing target is canonicalized when
/// present; otherwise the **parent directory** is canonicalized (it must exist)
/// and the file name re-joined — preventing a symlinked parent from redirecting
/// a create outside the sandbox. The allow-list is matched on both the
/// normalized and the resolved path, mirroring [`resolve_read_path`].
///
/// # Errors
/// Errors if `path` is relative / escapes the root, has no file-name component
/// (e.g. `/`), is outside the granted `write` allow-list (before or after
/// resolution), or its parent directory cannot be canonicalized.
fn resolve_write_path(
    caps: &CapabilitySet,
    path: &str,
) -> Result<PathBuf, Box<rhai::EvalAltResult>> {
    let norm = normalize_capability_path(path).ok_or_else(|| {
        rt_err(format!(
            "uni.fs.write: path `{path}` is not an absolute, traversal-free path"
        ))
    })?;
    if !caps
        .iter()
        .any(|c| c.filesystem_write_allows(&norm.to_string_lossy()))
    {
        return Err(rt_err(format!(
            "uni.fs.write: path `{path}` not in granted Filesystem write allow-list"
        )));
    }
    let resolved = if norm.exists() {
        std::fs::canonicalize(&norm).map_err(|e| rt_err(format!("uni.fs.write({path}): {e}")))?
    } else {
        let parent = norm.parent().ok_or_else(|| {
            rt_err(format!(
                "uni.fs.write: path `{path}` has no parent directory"
            ))
        })?;
        let file_name = norm
            .file_name()
            .ok_or_else(|| rt_err(format!("uni.fs.write: path `{path}` has no file name")))?;
        let canonical_parent = std::fs::canonicalize(parent)
            .map_err(|e| rt_err(format!("uni.fs.write({path}): parent: {e}")))?;
        canonical_parent.join(file_name)
    };
    if !caps
        .iter()
        .any(|c| c.filesystem_write_allows(&resolved.to_string_lossy()))
    {
        return Err(rt_err(format!(
            "uni.fs.write: path `{path}` resolves outside the granted Filesystem write allow-list"
        )));
    }
    Ok(resolved)
}

/// Test-only: register `uni_fs_read` directly into a registry without a
/// full `RhaiLoader`.
#[doc(hidden)]
pub fn _register_for_test(reg: &mut RhaiHostFnRegistry) {
    let cap = Capability::Filesystem {
        read: vec!["**".into()],
        write: vec![],
    };
    reg.register(RhaiHostFnSpec {
        name: "uni.fs.read".into(),
        required_capability: Some(cap),
        docs: String::new(),
        register: Arc::new(register_fs_read),
    });
}
