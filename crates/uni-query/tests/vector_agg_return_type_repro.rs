// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
//! Repro for `crates/uni-plugin-extism/src/adapter_aggregate.rs:325`.
//!
//! For a `Vector`-typed aggregate return, two real code paths disagree on the
//! Arrow type:
//!
//! - The extism accumulator's `returns_field` is built by `build_returns_field`
//!   (adapter_aggregate.rs:325) via the shared `argtype_to_arrow`
//!   (uni-plugin `adapter_common::arrow_types`), which maps
//!   `ArgType::Vector { element, .. }` to the **bare element** `DataType`
//!   (a scalar). The accumulator's `evaluate()` then sanity-checks the plugin's
//!   output column against `returns_field.data_type()` (adapter_aggregate.rs:290).
//!
//! - DataFusion's `PluginAggregateUdaf::return_type` (df_udaf_plugin.rs:121)
//!   uses `arg_type_to_arrow` (df_udaf_plugin.rs:214), which maps the same
//!   `ArgType::Vector { len, element }` to `FixedSizeList<element, len>`.
//!
//! So for a Vector return the extism accumulator declares a scalar element type
//! while DataFusion declares a FixedSizeList — the produced value can never
//! satisfy both. This test invokes BOTH real functions with one real
//! `AggSignature` and observes the divergence.

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::logical_expr::{AggregateUDFImpl, Volatility};
use uni_plugin::adapter_common::arrow_types::argtype_to_arrow;
use uni_plugin::traits::aggregate::AggSignature;
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{PluginRegistry, QName};
use uni_query::query::df_udaf_plugin::PluginAggregateUdaf;

#[test]
fn vector_return_type_diverges_extism_vs_datafusion() {
    let vec_ret = ArgType::Vector {
        len: 3,
        element: DataType::Float32,
    };
    let sig = AggSignature {
        args: vec![vec_ret.clone()],
        returns: vec_ret.clone(),
        state_fields: vec![],
        volatility: Volatility::Immutable,
        supports_partial: false,
    };

    // (A) EXTISM SIDE — exactly what `build_returns_field` (adapter_aggregate
    // .rs:325) uses to set the accumulator's `returns_field`, which its
    // `evaluate()` sanity-checks against (line 290).
    let extism_returns_field_type = argtype_to_arrow(&sig.returns);

    // (B) DATAFUSION SIDE — what the query engine declares the UDAF returns.
    let udaf = PluginAggregateUdaf::new(
        QName::parse("test.vec_agg").expect("qname"),
        Arc::new(PluginRegistry::new()),
        sig.clone(),
    );
    let df_return_type = udaf.return_type(&[]).expect("return_type");

    eprintln!("REPRO adapter_aggregate.rs:325");
    eprintln!("  extism accumulator returns_field type = {extism_returns_field_type:?}");
    eprintln!("  datafusion UDAF return_type           = {df_return_type:?}");

    // FIXED: both sides now agree on FixedSizeList<Float32, 3> so `evaluate()`'s
    // line-290 check passes and the scalar DataFusion produced matches its
    // declared return type. (fix for adapter_aggregate.rs:325)
    assert_eq!(
        extism_returns_field_type, df_return_type,
        "the two paths must agree for a Vector return"
    );

    // Pin the exact agreed representation for the record.
    match &df_return_type {
        DataType::FixedSizeList(field, 3) => {
            assert_eq!(field.data_type(), &DataType::Float32);
        }
        other => panic!("expected DataFusion FixedSizeList<Float32,3>, got {other:?}"),
    }
    match &extism_returns_field_type {
        DataType::FixedSizeList(field, 3) => {
            assert_eq!(field.data_type(), &DataType::Float32);
        }
        other => panic!("expected Extism FixedSizeList<Float32,3>, got {other:?}"),
    }
}
