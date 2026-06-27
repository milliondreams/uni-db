//! Learned-sparse (SPLADE / BGE-M3) vector type for the Uni graph database.
//!
//! A leaf crate with zero `uni-*` dependencies — the same layering as
//! `uni-btic`. It owns the [`SparseVector`] type, its validating constructor,
//! the lossless binary [`mod@encode`] codec, the pure scoring kernels in
//! [`mod@ops`] (notably [`ops::sparse_dot`]), and [`SparseError`]. All
//! glue — `Value`/`DataType` variants, Arrow lowering, CV tag framing, DDL,
//! the index — lives in the integration crates that depend on this one.

pub mod encode;
pub mod error;
pub mod ops;
pub mod sparse;

pub use error::SparseError;
pub use sparse::SparseVector;
