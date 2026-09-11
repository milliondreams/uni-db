// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #228 — reading an edge property converts it through `serde_json`,
//! which has no representation for several of the types it carries.
//!
//! Two independent paths did this, and a property read takes one or the other
//! depending on where the value lives. Both are pinned here, because a test
//! that only covered one would pass while the other kept losing values.
//!
//! ## The delta-table path — `value_codec::decode_column_value`
//!
//! A declared property has a typed column in the per-edge-type delta table, and
//! that read went through `value_from_column`, which returns a
//! `serde_json::Value`. For a float that is `serde_json::json!(v)`, i.e.
//! `Number::from_f64(v).map_or(Value::Null, ..)` — and `from_f64` rejects `NaN`
//! and the infinities. So a non-finite float read back as **`Null`**.
//!
//! This is the arm that actually runs for a declared scalar, which is why the
//! float case below survived the main-edges fix and only fell to the codec one.
//! The same arm serves vertex properties, so the loss was never edge-specific.
//!
//! ## The main-edges blob path — `MainEdgeDataset::parse_props_json`
//!
//! Undeclared and fallback reads come off the `props_json` blob. That decoded
//! to a `Value` and then did `let json: serde_json::Value = v.into();
//! serde_json::from_value(json)` — where `Properties` *is*
//! `HashMap<String, Value>` and the decoded value *is* a `Value::Map`, so the
//! pair was a no-op bought at the price of an intermediate JSON tree per edge.
//!
//! Except it was not a no-op. `impl From<Value> for serde_json::Value` is lossy
//! and silent about it — the result is a well-formed value of the wrong type
//! rather than an error:
//!
//! | written | after the round trip |
//! |---|---|
//! | `Bytes` | `String`, base64-encoded |
//! | `Vector` / `BinaryVector` | `List` of numbers |
//! | `Temporal` | `String` |
//! | `Float(NaN)` / `Float(inf)` | `Null` |
//!
//! The write side had the mirror image (`serde_json::to_value(props)`), whose
//! `unwrap_or(json!({}))` additionally turned a serialization failure into
//! *every* property on the edge disappearing.
//!
//! # These are fidelity tests, not performance tests
//!
//! Removing the blob round trip did buy ~10% of hydration on LDBC SF1; removing
//! the codec one measured as nothing. Neither number is what these assert. They
//! assert that what was written comes back — a property that is silently the
//! wrong type is a worse outcome than a slow one, and it is the outcome that no
//! amount of profiling would have found.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_common::{Properties, Value};
use uni_store::runtime::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

/// Write one edge carrying `props`, flush, and read the named properties back.
///
/// The flush is load-bearing: with values still in L0 the overlay answers from
/// the `Properties` map verbatim and no conversion happens at all, so both
/// defects are invisible. Which path the read then takes depends on the
/// property — a declared scalar comes off the typed delta column, anything
/// without one comes off the main-edges blob — which is why the tests below
/// deliberately span both.
async fn round_trip(
    declared: &[(&str, DataType)],
    props: Properties,
    read: &[&str],
) -> Result<(TempDir, Properties)> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?);

    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    schema_manager.add_label("N")?;
    let type_id =
        schema_manager.add_edge_type("T", vec!["N".to_string()], vec!["N".to_string()])?;
    for (name, dt) in declared {
        schema_manager.add_property("T", name, dt.clone(), true)?;
    }
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    let src = writer.next_vid().await?;
    let dst = writer.next_vid().await?;
    for v in [src, dst] {
        writer
            .insert_vertex_with_labels(v, HashMap::new(), &["N".to_string()], None)
            .await?;
    }
    let eid = writer.next_eid(type_id).await?;
    writer
        .insert_edge(src, dst, type_id, eid, props, None, None)
        .await?;

    // Cold L0 is the point: it forces the read down to the blob.
    writer.flush_to_l1(None).await?;

    let pm = PropertyManager::new(storage.clone(), schema_manager, 0);
    let got = pm.get_batch_edge_props(&[eid], read, None, None).await?;
    let mine = got.into_values().next().unwrap_or_else(Properties::new);
    Ok((temp_dir, mine))
}

/// A `Bytes` edge property must come back as `Bytes`, not as its base64 text.
///
/// This is the sharpest case: nothing errors, and the value is a perfectly
/// good `String` — so every assertion that only checks presence, or compares
/// after a `to_string()`, passes while the type has silently changed.
#[tokio::test]
async fn a_bytes_edge_property_survives_the_read() -> Result<()> {
    let payload = vec![0u8, 1, 2, 250, 255];
    let mut props = Properties::new();
    props.insert("b".to_string(), Value::Bytes(payload.clone()));

    let (_tmp, got) = round_trip(&[("b", DataType::Bytes)], props, &["b"]).await?;

    assert_eq!(
        got.get("b"),
        Some(&Value::Bytes(payload)),
        "a Bytes edge property came back as {:?}. Base64 text here means the \
         read went through `serde_json::Value`, whose `From<Value>` encodes \
         Bytes as a String — a silent type change, not an error.",
        got.get("b")
    );
    Ok(())
}

/// A `Float` edge property keeps its value, including the non-finite ones that
/// JSON cannot represent at all.
#[tokio::test]
async fn a_non_finite_float_edge_property_is_not_silently_nulled() -> Result<()> {
    let mut props = Properties::new();
    props.insert("f".to_string(), Value::Float(f64::INFINITY));
    props.insert("g".to_string(), Value::Float(1.5));

    let (_tmp, got) = round_trip(
        &[("f", DataType::Float), ("g", DataType::Float)],
        props,
        &["f", "g"],
    )
    .await?;

    assert_eq!(
        got.get("g"),
        Some(&Value::Float(1.5)),
        "control: an ordinary float must survive; if this fails the fixture is \
         not reading the edge at all and the assertion below proves nothing"
    );
    assert_eq!(
        got.get("f"),
        Some(&Value::Float(f64::INFINITY)),
        "a non-finite float came back as {:?}. `serde_json::Number::from_f64` \
         returns None for NaN and infinities, and the conversion maps that to \
         Null — so the value is not merely reformatted, it is gone.",
        got.get("f")
    );
    Ok(())
}

/// The ordinary scalars must be unaffected, in both value and type.
///
/// The control for every assertion above: these are the types the JSON round
/// trip does preserve, so they must look identical before and after the fix.
/// A change here would mean the fix altered the common path rather than only
/// the lossy corners.
#[tokio::test]
async fn ordinary_scalar_edge_properties_are_unchanged() -> Result<()> {
    let mut props = Properties::new();
    props.insert("i".to_string(), Value::Int(-42));
    props.insert("s".to_string(), Value::String("hello".to_string()));
    props.insert("t".to_string(), Value::Bool(true));

    let (_tmp, got) = round_trip(
        &[
            ("i", DataType::Int),
            ("s", DataType::String),
            ("t", DataType::Bool),
        ],
        props,
        &["i", "s", "t"],
    )
    .await?;

    assert_eq!(got.get("i"), Some(&Value::Int(-42)));
    assert_eq!(got.get("s"), Some(&Value::String("hello".to_string())));
    assert_eq!(got.get("t"), Some(&Value::Bool(true)));
    Ok(())
}

/// The same loss on the **vertex** side, which is where it is most likely to be
/// met in practice.
///
/// `decode_column_value` is shared: the per-label delta scan reads a vertex
/// property through the same arm that nulled a non-finite edge float. Nothing
/// about the defect was edge-specific, and #228 is written only about edges —
/// so without this test the vertex half would have been fixed silently and
/// could regress with nothing to catch it.
#[tokio::test]
async fn a_non_finite_float_vertex_property_is_not_silently_nulled() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?);
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    schema_manager.add_label("V")?;
    schema_manager.add_property("V", "f", DataType::Float, true)?;
    schema_manager.add_property("V", "g", DataType::Float, true)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    let vid = writer.next_vid().await?;
    let mut props = Properties::new();
    props.insert("f".to_string(), Value::Float(f64::NEG_INFINITY));
    props.insert("g".to_string(), Value::Float(2.5));
    writer
        .insert_vertex_with_labels(vid, props, &["V".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    let pm = PropertyManager::new(storage.clone(), schema_manager, 0);
    let got = pm.get_batch_vertex_props(&[vid], &["f", "g"], None).await?;
    let mine = got.get(&vid).cloned().unwrap_or_else(Properties::new);

    assert_eq!(
        mine.get("g"),
        Some(&Value::Float(2.5)),
        "control: an ordinary float must survive, or this test is not reading \
         the vertex at all"
    );
    assert_eq!(
        mine.get("f"),
        Some(&Value::Float(f64::NEG_INFINITY)),
        "a non-finite vertex float came back as {:?} — the same \
         `serde_json::json!` arm as the edge case, on the vertex read path.",
        mine.get("f")
    );
    Ok(())
}
