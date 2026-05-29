//! Filesystem host fns — gated by [`Capability::Filesystem`].
//!
//! `uni_fs_read(path) -> string` reads a UTF-8 file. Validation against
//! the grant's read-glob list is intentionally **not** done here at the
//! call boundary — the layer-2 enforcement of "the function is only
//! registered when the capability is granted" is what gates access; the
//! glob validation in layer 3 is a per-call host-side check that
//! belongs in the closure body. For v1 we accept any path the host
//! process can read while the capability is granted; a follow-up wires
//! per-call glob validation against the capability's `read`/`write`
//! patterns.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use rhai::Engine;
use uni_plugin::Capability;

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

fn register_fs_read(engine: &mut Engine) {
    engine.register_fn(
        "uni_fs_read",
        |path: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            std::fs::read_to_string(path).map_err(|e| {
                Box::new(rhai::EvalAltResult::ErrorRuntime(
                    format!("uni.fs.read({path}): {e}").into(),
                    rhai::Position::NONE,
                ))
            })
        },
    );
}

fn register_fs_write(engine: &mut Engine) {
    engine.register_fn(
        "uni_fs_write",
        |path: &str, data: &str| -> Result<(), Box<rhai::EvalAltResult>> {
            std::fs::write(path, data).map_err(|e| {
                Box::new(rhai::EvalAltResult::ErrorRuntime(
                    format!("uni.fs.write({path}): {e}").into(),
                    rhai::Position::NONE,
                ))
            })
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
