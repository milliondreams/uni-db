//! Per-surface capability traits.
//!
//! Every extension surface that a plugin can opt into lives in its own
//! submodule here. Plugins implement the relevant trait and register
//! their implementation via the corresponding [`crate::PluginRegistrar`]
//! method.
//!
//! The trait surface is intentionally split: each file is small and
//! focused on one capability. Adding a new surface means a new submodule
//! and a new method on [`crate::PluginRegistrar`].

pub mod aggregate;
pub mod algorithm;
pub mod background;
pub mod catalog;
pub mod cdc;
pub mod collation;
pub mod connector;
pub mod crdt;
pub mod hook;
pub mod index;
pub mod locy;
pub mod operator;
pub mod procedure;
pub use procedure::ProcedureHost;
pub mod pushdown;
pub mod scalar;
pub mod storage;
pub mod trigger;
pub mod types;
pub mod window;
