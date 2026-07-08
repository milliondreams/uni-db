//! Plugin framework for uni-db.
//!
//! `uni-plugin` defines the trait surface, registry, manifest, and capability
//! model that every uni-db extension — scalar function, aggregate, procedure,
//! per-label storage, index kind, graph algorithm, CRDT, hook, trigger, background
//! job, logical type, auth provider, authz policy, collation, CDC
//! output, catalog, replacement scan — registers through.
//!
//! The crate intentionally has **no host integration**: it does not depend on
//! `uni-query`, `uni-store`, `uni-crdt`, `uni-algo`, or `uni`. Those crates
//! depend on `uni-plugin` and adapt their existing surfaces to the traits
//! defined here. This direction keeps the dependency graph acyclic and lets
//! the trait surface be reviewed in isolation.
//!
//! # Layout
//!
//! - [`plugin`] — the `Plugin` trait, `PluginManifest`, `PluginHandle`.
//! - [`qname`] — qualified plugin-function names (`namespace.local`).
//! - [`capability`] — `Capability`, `CapabilitySet`, `Determinism`, `Scope`.
//! - [`manifest`] — TOML / JSON manifest (de)serialization.
//! - [`registrar`] — the builder a plugin's `register()` method calls.
//! - [`registry`] — per-surface trait-object tables (`arc-swap`-backed for
//!   wait-free reads).
//! - [`traits`] — one module per extension surface (scalar functions,
//!   aggregates, procedures, hooks, …).
//! - [`errors`] — `PluginError`, `FnError`, plus per-trait error helpers.
//!
//! # Stability
//!
//! Until uni-plugin reaches `1.0.0`, trait shapes may change.
//! The semver guarantees apply only to `0.x` major versions in the meantime.
//!
//! # Examples
//!
//! ```
//! use uni_plugin::{Plugin, PluginManifest, PluginRegistrar, PluginError, QName};
//!
//! struct NoopPlugin;
//!
//! impl Plugin for NoopPlugin {
//!     fn manifest(&self) -> &PluginManifest {
//!         // In real plugins, store the manifest in a `OnceLock` populated
//!         // at construction.
//!         unimplemented!("see crates/uni-plugin-builtin for real examples")
//!     }
//!     fn register(&self, _r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
//!         Ok(())
//!     }
//! }
//! ```

// Rust guideline compliant
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(unsafe_op_in_unsafe_fn)]
#![warn(missing_debug_implementations)]

pub mod adapter_common;
pub mod adapters;
pub mod capability;
pub mod circuit_breaker;
pub mod errors;
pub mod fs_guard;
pub mod host;
pub mod host_services;
pub mod lifecycle;
pub mod manifest;
pub mod observability;
pub mod plugin;
pub mod qname;
pub mod registrar;
pub mod registry;
pub mod reload;
pub mod scheduler;
pub mod secrets;
pub mod surfaces;
pub mod traits;
pub mod verify;

#[doc(inline)]
pub use crate::capability::{
    Capability, CapabilitySet, Determinism, ManifestCapability, Scope, SideEffects,
};
#[doc(inline)]
pub use crate::errors::{FnError, PluginError, ReloadError};
#[doc(inline)]
pub use crate::fs_guard::normalize_capability_path;
#[doc(inline)]
pub use crate::host_services::{HttpEgress, HttpResponse, KmsProvider};
#[doc(inline)]
pub use crate::manifest::{AbiRange, PluginManifest, ProvidedSurfaces};
#[doc(inline)]
pub use crate::plugin::{Plugin, PluginHandle, PluginId, PluginInitContext};
#[doc(inline)]
pub use crate::qname::{QName, RESERVED_PLUGIN_IDS, is_reserved_plugin_id};
#[doc(inline)]
pub use crate::registrar::PluginRegistrar;
#[doc(inline)]
pub use crate::registry::{PluginRecordSnapshot, PluginRegistry};
#[doc(inline)]
pub use crate::reload::{
    CdcHandoff, IndexHandoff, OldProviders, ReloadDispatcher, ReloadKindHandlers, ReloadOutcome,
};
