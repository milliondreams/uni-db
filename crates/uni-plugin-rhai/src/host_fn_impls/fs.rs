//! Filesystem host fns — gated by [`Capability::Filesystem`].
//!
//! `uni_fs_read(path) -> string` reads a UTF-8 file; `uni_fs_write(path, data)`
//! writes one. Enforcement is two-layered: layer 2 only registers the fns when
//! the plugin holds a `Filesystem` capability, and layer 3 (here) matches each
//! call's `path` against the grant's `read` / `write` glob allow-list before
//! touching the filesystem — a path outside the allow-list errors loudly.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use rhai::Engine;
use uni_plugin::{Capability, CapabilitySet};

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
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.fs.read".into(),
        required_capability: Some(placeholder.clone()),
        docs: "Read a UTF-8 file from the host filesystem.".into(),
        register: Arc::new(register_fs_read),
    });
    loader.host_fns_mut().register(RhaiHostFnSpec {
        name: "uni.fs.write".into(),
        required_capability: Some(placeholder),
        docs: "Write a UTF-8 string to a host filesystem path.".into(),
        register: Arc::new(register_fs_write),
    });
}

fn register_fs_read(engine: &mut Engine, caps: &CapabilitySet) {
    let caps = caps.clone();
    engine.register_fn(
        "uni_fs_read",
        move |path: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            if !caps.iter().any(|c| c.filesystem_read_allows(path)) {
                return Err(rt_err(format!(
                    "uni.fs.read: path `{path}` not in granted Filesystem read allow-list"
                )));
            }
            std::fs::read_to_string(path)
                .map_err(|e| rt_err(format!("uni.fs.read({path}): {e}")))
        },
    );
}

fn register_fs_write(engine: &mut Engine, caps: &CapabilitySet) {
    let caps = caps.clone();
    engine.register_fn(
        "uni_fs_write",
        move |path: &str, data: &str| -> Result<(), Box<rhai::EvalAltResult>> {
            if !caps.iter().any(|c| c.filesystem_write_allows(path)) {
                return Err(rt_err(format!(
                    "uni.fs.write: path `{path}` not in granted Filesystem write allow-list"
                )));
            }
            std::fs::write(path, data).map_err(|e| rt_err(format!("uni.fs.write({path}): {e}")))
        },
    );
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
