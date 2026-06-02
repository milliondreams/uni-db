//! Vectorized column userdata for Rhai plugins.
//!
//! When a script declares `vectorized: true` on a scalar entry, the
//! adapter passes each input column as a custom Rhai type wrapping the
//! underlying `ArrayRef` — no per-row Dynamic conversion. The script
//! reads cells via the registered indexer and writes the output column
//! into a `MutableFloat64Column` allocated by `uni::float_column(n)`.
//!
//! All wrapper types are `Clone + Send + Sync + 'static` (required by
//! Rhai's `sync` feature). Clones are cheap — they bump the inner Arc.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, StringArray, builder::Float64Builder};
use parking_lot::Mutex;
use rhai::{Engine, EvalAltResult, Position};

/// Define an immutable column wrapper exposing a single Arrow array type to
/// Rhai scripts.
///
/// Rhai's custom-type registration keys on a concrete named type, so each
/// element type needs its own wrapper struct. This macro generates the
/// identical `new` / `len` / `is_empty` / `get` surface; the only per-type
/// difference is how a cell value is mapped into a `rhai::Dynamic`
/// (`$to_dynamic`), e.g. `Utf8Column` allocates a fresh `String` per access.
macro_rules! immutable_column {
    ($(#[$meta:meta])* $name:ident, $array:ty, $to_dynamic:expr) => {
        $(#[$meta])*
        #[derive(Clone, Debug)]
        pub struct $name {
            inner: Arc<$array>,
        }

        impl $name {
            /// Wrap an existing array.
            #[must_use]
            pub fn new(arr: Arc<$array>) -> Self {
                Self { inner: arr }
            }

            /// Number of rows.
            #[must_use]
            pub fn len(&mut self) -> i64 {
                self.inner.len() as i64
            }

            /// Returns true if the column has no rows.
            #[must_use]
            pub fn is_empty(&mut self) -> bool {
                self.inner.is_empty()
            }

            /// Read row `i`; returns `()` for nulls or out-of-range indices.
            pub fn get(&mut self, i: i64) -> rhai::Dynamic {
                let idx = i as usize;
                if idx >= self.inner.len() || self.inner.is_null(idx) {
                    return rhai::Dynamic::UNIT;
                }
                let cell = self.inner.value(idx);
                $to_dynamic(cell)
            }
        }
    };
}

immutable_column!(
    /// Immutable Float64 column wrapper exposed to Rhai scripts.
    Float64Column,
    Float64Array,
    rhai::Dynamic::from
);

immutable_column!(
    /// Immutable Int64 column wrapper.
    Int64Column,
    Int64Array,
    rhai::Dynamic::from
);

immutable_column!(
    /// Immutable Utf8 column wrapper. The indexer allocates a fresh `String`
    /// per access — documented as a perf-conscious choice.
    Utf8Column,
    StringArray,
    |v: &str| rhai::Dynamic::from(v.to_owned())
);

/// Mutable Float64 column the script allocates via
/// `uni::float_column(n)` and writes to via indexer-set.
///
/// Backed by `Vec<Option<f64>>` so `set` is O(1) and nulls round-trip
/// correctly. The previous implementation walked `Float64Builder`'s
/// `values_slice()` on every `set`, which (a) made writes O(n) and (b)
/// silently dropped nulls because `values_slice().get(j).is_some()`
/// always returns `Some` for in-bounds indices.
#[derive(Clone, Debug)]
pub struct MutableFloat64Column {
    values: Arc<Mutex<Vec<Option<f64>>>>,
}

impl MutableFloat64Column {
    /// Pre-allocate an `n`-row column with all-null slots. The script
    /// sets each row explicitly via the indexer.
    #[must_use]
    pub fn with_capacity(n: i64) -> Self {
        let n = n.max(0) as usize;
        Self {
            values: Arc::new(Mutex::new(vec![None; n])),
        }
    }

    /// Length (advertised to scripts; matches allocation size).
    #[must_use]
    pub fn len(&mut self) -> i64 {
        self.values.lock().len() as i64
    }

    /// Returns true if the column was allocated with size 0.
    #[must_use]
    pub fn is_empty(&mut self) -> bool {
        self.values.lock().is_empty()
    }

    /// Replace the value at row `i`. O(1).
    pub fn set(&mut self, i: i64, v: f64) -> Result<(), Box<EvalAltResult>> {
        let idx = i as usize;
        let mut values = self.values.lock();
        if idx >= values.len() {
            return Err(Box::new(EvalAltResult::ErrorIndexNotFound(
                rhai::Dynamic::from(i),
                Position::NONE,
            )));
        }
        values[idx] = Some(v);
        Ok(())
    }

    /// Read row `i`. Returns `()` for null cells or out-of-range
    /// indices (matching `Float64Column::get`).
    pub fn get(&mut self, i: i64) -> rhai::Dynamic {
        let idx = i as usize;
        let values = self.values.lock();
        match values.get(idx).and_then(|cell| cell.as_ref()) {
            Some(&v) => rhai::Dynamic::from(v),
            None => rhai::Dynamic::UNIT,
        }
    }

    /// Finalise into an immutable `Float64Array`.
    #[must_use]
    pub fn freeze(self) -> Arc<Float64Array> {
        let mut values = self.values.lock();
        let mut builder = Float64Builder::with_capacity(values.len());
        for cell in values.drain(..) {
            match cell {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }
}

/// Register column types + the `uni::float_column(n)` allocator on a
/// Rhai engine. Always-available — no capability gate.
pub fn register_column_types(engine: &mut Engine) {
    engine
        .register_type_with_name::<Float64Column>("Float64Column")
        .register_fn("len", Float64Column::len)
        .register_indexer_get(Float64Column::get);

    engine
        .register_type_with_name::<Int64Column>("Int64Column")
        .register_fn("len", Int64Column::len)
        .register_indexer_get(Int64Column::get);

    engine
        .register_type_with_name::<Utf8Column>("Utf8Column")
        .register_fn("len", Utf8Column::len)
        .register_indexer_get(Utf8Column::get);

    engine
        .register_type_with_name::<MutableFloat64Column>("MutableFloat64Column")
        .register_fn("len", MutableFloat64Column::len)
        .register_indexer_get(MutableFloat64Column::get)
        .register_indexer_set(MutableFloat64Column::set);

    // Allocator the script calls to make an output column.
    engine.register_fn("uni_float_column", MutableFloat64Column::with_capacity);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::build_engine;
    use crate::host_fns::RhaiHostFnRegistry;
    use uni_plugin::CapabilitySet;

    fn engine_with_columns() -> Engine {
        let mut e = build_engine(&CapabilitySet::new(), &RhaiHostFnRegistry::new());
        register_column_types(&mut e);
        e
    }

    #[test]
    fn float_column_indexer_round_trips() {
        let arr = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.0)]));
        let col = Float64Column::new(arr);
        let e = engine_with_columns();
        let mut scope = rhai::Scope::new();
        scope.push("col", col);
        let v0: f64 = e.eval_with_scope(&mut scope, "col[0]").unwrap();
        assert_eq!(v0, 1.5);
        let v2: f64 = e.eval_with_scope(&mut scope, "col[2]").unwrap();
        assert_eq!(v2, 3.0);
        // Null cell returns () — convert to bool unit check.
        let v1: rhai::Dynamic = e.eval_with_scope(&mut scope, "col[1]").unwrap();
        assert!(v1.is_unit());
    }

    #[test]
    fn mutable_float_column_round_trips() {
        let e = engine_with_columns();
        let script = r#"
            let out = uni_float_column(3);
            out[0] = 1.5;
            out[1] = 2.5;
            out[2] = 3.5;
            out
        "#;
        let out: MutableFloat64Column = e.eval(script).unwrap();
        let arr = out.freeze();
        assert_eq!(arr.value(0), 1.5);
        assert_eq!(arr.value(1), 2.5);
        assert_eq!(arr.value(2), 3.5);
    }

    /// Regression: previously, `set` rebuilt the underlying
    /// `Float64Builder` by walking `values_slice().get(j).is_some()`,
    /// which is always `Some` for in-bounds indices — the original
    /// null bitmap was discarded, so any slot the script left unset
    /// surfaced as `0.0` after `freeze()`.
    #[test]
    fn unset_slots_remain_null_after_freeze() {
        let e = engine_with_columns();
        let script = r#"
            let out = uni_float_column(4);
            out[0] = 10.0;
            out[2] = 30.0;
            out
        "#;
        let out: MutableFloat64Column = e.eval(script).unwrap();
        let arr = out.freeze();
        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), 10.0);
        assert!(arr.is_null(1), "unset slot must remain null");
        assert!(!arr.is_null(2));
        assert_eq!(arr.value(2), 30.0);
        assert!(arr.is_null(3), "unset slot must remain null");
    }

    /// Setting an out-of-range index must surface as a Rhai
    /// `ErrorIndexNotFound`, not panic or silently no-op.
    #[test]
    fn set_out_of_range_returns_index_error() {
        let e = engine_with_columns();
        let script = r#"
            let out = uni_float_column(2);
            out[5] = 99.0;
            out
        "#;
        let res = e.eval::<MutableFloat64Column>(script);
        assert!(res.is_err(), "expected index error");
    }
}
