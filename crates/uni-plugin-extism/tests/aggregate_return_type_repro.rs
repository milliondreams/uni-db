//! Repro for `crates/uni-plugin-extism/src/adapter_aggregate.rs:325`.
//!
//! `build_returns_field` (adapter_aggregate.rs:324-326) declares the Arrow
//! return field of an Extism aggregate via
//! `uni_plugin::adapter_common::arrow_types::argtype_to_arrow(&sig.returns)`.
//! For an `ArgType::Vector { len, element }` that helper collapses the vector
//! to its BARE ELEMENT type (arrow_types.rs:48 — `Vector { element, .. } =>
//! element.clone()`), so the Extism accumulator advertises (and the sanity
//! check at adapter_aggregate.rs:290 enforces) a scalar element `DataType`.
//!
//! The DataFusion UDAF bridge, however, declares the SAME `ArgType::Vector`
//! return as a `FixedSizeList` — `arg_type_to_arrow`
//! (crates/uni-query/src/query/df_udaf_plugin.rs:214) returns
//! `FixedSizeList(Field("item", element), len)`, and that is what
//! `PluginAggregateUdaf::return_type` advertises to the query engine. So the
//! type the planner expects (`FixedSizeList<Float32, N>`) can never equal the
//! type the Extism accumulator produces/validates (bare `Float32`): a
//! vector-returning Extism aggregate is a guaranteed type mismatch.
//!
//! This test drives the REAL function `build_returns_field` uses
//! (`argtype_to_arrow`) and pins the Extism side of the divergence. The full
//! cross-crate comparison against the DataFusion bridge lives in the sibling
//! test `crates/uni-query/tests/vector_agg_return_type_repro.rs`.

use arrow_schema::{DataType, Field};

use uni_plugin::adapter_common::arrow_types::argtype_to_arrow;
use uni_plugin::traits::scalar::ArgType;

#[test]
fn vector_return_type_collapses_to_bare_element_not_fixed_size_list() {
    // A 3-dim Float32 vector return, exactly as a vector-producing aggregate
    // would declare in its `AggSignature.returns`.
    let returns = ArgType::Vector {
        len: 3,
        element: DataType::Float32,
    };

    // This is the precise call `build_returns_field` makes at
    // adapter_aggregate.rs:325 to build the Extism accumulator's `returns_field`.
    let extism_return_ty = argtype_to_arrow(&returns);

    // What the DataFusion bridge (df_udaf_plugin.rs:214) declares for the SAME
    // signature, and thus what the query planner expects downstream.
    let datafusion_return_ty =
        DataType::FixedSizeList(std::sync::Arc::new(Field::new("item", DataType::Float32, true)), 3);

    // FIXED (adapter_aggregate.rs:325): the Extism side now maps Vector to the
    // same FixedSizeList the DataFusion UDAF advertises, so the two agree on the
    // vector's Arrow representation.
    assert_eq!(
        extism_return_ty, datafusion_return_ty,
        "the Extism return type must match the DataFusion FixedSizeList"
    );
    assert_eq!(
        extism_return_ty,
        DataType::FixedSizeList(std::sync::Arc::new(Field::new("item", DataType::Float32, true)), 3),
        "Extism build_returns_field should map Vector to FixedSizeList<Float32, 3>"
    );
}
