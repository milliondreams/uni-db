// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.create.vNode` / `uni.create.vEdge` — ephemeral (transient,
//! in-query) graph entities. The APOC `apoc.create.vNode` /
//! `apoc.create.vRelationship` analogues from proposal §4.13.1.
//!
//! Ephemeral identities are minted from
//! `QueryProcedureHost::allocate_transient_id()` and wrapped in
//! `Vid::ephemeral` / `Eid::ephemeral` so their high bit (M5g
//! `EPHEMERAL_BIT`) is set. Storage write entry points
//! (`execute_set_items_locked`, `execute_delete_vertex`,
//! `execute_delete_edge_from_map`) refuse any id with that bit set,
//! surfacing `UniError::EphemeralWriteAttempt`.
//!
//! **Yield shape (M5g):**
//!
//! * `uni.create.vNode` declares a single canonical `vid` Int64 field
//!   on its signature, tagged with `_yield_kind = node_vid_source`.
//!   That metadata tag opts the procedure into the planner's
//!   node-shaped YIELD expansion: when the caller writes
//!   `YIELD node`, the planner rewrites the column projection to the
//!   canonical Node tuple (`<n>._vid`, `<n>`, `<n>._labels`,
//!   `<n>.<prop>` ...). This is the same surface area as
//!   `uni.vector.query` and friends — downstream Cypher `WITH node ...
//!   node.foo` access works out of the box because the property
//!   columns are physically present on the row.
//! * `uni.create.vEdge` declares a single `edge` field whose Arrow
//!   type is `Struct(_eid, _type_name, _src, _dst, properties)` — the
//!   canonical edge-struct shape used by path materialization
//!   (`df_graph::common::edge_struct_fields`). Unit tests assert the
//!   struct directly; downstream edge-property access is out of
//!   scope for M5g (the round-trip target was `node.foo`, not
//!   `rel.foo`).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow_array::builder::{ListBuilder, StringBuilder, UInt64Builder};
use arrow_array::{ArrayRef, Int64Array, LargeBinaryArray, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use uni_common::Properties;
use uni_common::core::id::{Eid, Vid};
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use crate::query::df_graph::common::edge_struct_fields;
use crate::query::df_graph::procedure_call::map_yield_to_canonical;
use crate::query::df_graph::scan::{build_property_column_static, resolve_property_type};
use crate::query::executor::procedure_host::QueryProcedureHost;

// Rust guideline compliant

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn require_host<'a>(ctx: &ProcedureContext<'a>) -> Result<&'a QueryProcedureHost, FnError> {
    ctx.host
        .and_then(|h| h.as_any().downcast_ref::<QueryProcedureHost>())
        .ok_or_else(|| FnError::new(0x701, "uni.create.*: requires QueryProcedureHost"))
}

/// Decode a positional arg as JSON (LargeBinary-encoded by the
/// dispatcher) or string/scalar fallback. Mirrors
/// `graph.rs::arg_to_json`.
fn arg_to_json(cv: &ColumnarValue) -> serde_json::Value {
    match cv {
        ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(b)))
        | ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => {
            serde_json::from_slice::<serde_json::Value>(b).unwrap_or(serde_json::Value::Null)
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
            serde_json::Value::String(s.clone())
        }
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => serde_json::Value::Bool(*b),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => {
            serde_json::Value::Number((*i).into())
        }
        _ => serde_json::Value::Null,
    }
}

fn arg_as_i64(cv: &ColumnarValue) -> Option<i64> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => Some(*i),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => Some(i64::from(*i)),
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(u))) => i64::try_from(*u).ok(),
        _ => None,
    }
}

fn arg_as_string(cv: &ColumnarValue) -> Option<String> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Some(s.clone()),
        _ => None,
    }
}

fn labels_from_json(jv: &serde_json::Value) -> Vec<String> {
    match jv {
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect(),
        serde_json::Value::String(s) => vec![s.clone()],
        _ => Vec::new(),
    }
}

/// Convert a JSON object into a `Properties` map (HashMap<String, Value>).
fn properties_from_json(jv: &serde_json::Value) -> Properties {
    match jv {
        serde_json::Value::Object(obj) => obj
            .iter()
            .map(|(k, v)| (k.clone(), uni_common::Value::from(v.clone())))
            .collect(),
        _ => Properties::new(),
    }
}

fn one_batch_stream(schema: SchemaRef, batch: RecordBatch) -> SendableRecordBatchStream {
    let stream =
        futures::stream::once(async move { Ok::<_, datafusion::error::DataFusionError>(batch) });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

// ---------------------------------------------------------------------------
// uni.create.vNode(labels, props) — typed Node yield (M5g)
// ---------------------------------------------------------------------------

/// Build the canonical `vid` field with `_yield_kind = node_vid_source`
/// metadata. This is the seam that opts the procedure into the
/// planner's node-shaped YIELD expansion (see
/// `procedure_call::expand_node_yield_fields`).
fn vid_node_yield_field() -> Field {
    let mut md = HashMap::new();
    md.insert("_yield_kind".to_owned(), "node_vid_source".to_owned());
    Field::new("vid", DataType::Int64, false).with_metadata(md)
}

#[derive(Debug)]
pub struct VNodeProcedure;

impl VNodeProcedure {
    fn signature_static() -> &'static ProcedureSignature {
        static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        SIG.get_or_init(|| ProcedureSignature {
            args: vec![
                NamedArgType {
                    name: smol_str::SmolStr::new("labels"),
                    ty: ArgType::Primitive(DataType::LargeBinary),
                    default: Some(ScalarValue::LargeBinary(Some(b"[]".to_vec()))),
                    doc: "List of label names (JSON-encoded array).".to_owned(),
                },
                NamedArgType {
                    name: smol_str::SmolStr::new("props"),
                    ty: ArgType::Primitive(DataType::LargeBinary),
                    default: Some(ScalarValue::LargeBinary(Some(b"{}".to_vec()))),
                    doc: "Property map (JSON-encoded object).".to_owned(),
                },
            ],
            yields: vec![vid_node_yield_field()],
            mode: ProcedureMode::Read,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "uni.create.vNode(labels, props) — mint a transient, \
                   in-query ephemeral node. Yields a single canonical \
                   Node column; when the caller writes `YIELD node` the \
                   planner expands it to the standard \
                   `<n>._vid + <n> + <n>._labels + <n>.<prop>` tuple. \
                   The returned vid has the `EPHEMERAL_BIT` (high bit) \
                   set; writes against it fail with \
                   `EphemeralWriteAttempt`. Not visible to subsequent \
                   MATCH."
                .to_owned(),
        })
    }
}

impl ProcedurePlugin for VNodeProcedure {
    fn signature(&self) -> &ProcedureSignature {
        Self::signature_static()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = require_host(&ctx)?;
        let labels_json = args
            .first()
            .map(arg_to_json)
            .unwrap_or(serde_json::Value::Null);
        let props_json = args
            .get(1)
            .map(arg_to_json)
            .unwrap_or(serde_json::Value::Null);
        let labels = labels_from_json(&labels_json);
        let props = properties_from_json(&props_json);

        let vid = Vid::ephemeral(host.allocate_transient_id());

        // Decide output shape: planner-driven (host has yield_items) or
        // fallback (signature schema: single `vid` Int64).
        let host_yields = host.yield_items();
        if host_yields.is_empty() {
            let schema: SchemaRef = Arc::new(Schema::new(vec![vid_node_yield_field()]));
            #[allow(clippy::cast_possible_wrap)]
            let cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![vid.as_u64() as i64]))];
            let batch = RecordBatch::try_new(Arc::clone(&schema), cols)
                .map_err(|e| FnError::new(0x830, format!("vNode RecordBatch build: {e}")))?;
            return Ok(one_batch_stream(schema, batch));
        }

        // Planner-driven path: emit columns matching expected_schema by
        // dispatching each yield to its canonical column shape.
        let expected_schema = host.expected_schema().cloned().ok_or_else(|| {
            FnError::new(0x830, "vNode: host yield_items set without expected_schema")
        })?;
        let target_properties = host.target_properties();

        let cols = build_vnode_columns(
            host_yields,
            target_properties,
            &expected_schema,
            vid,
            &labels,
            &props,
        )?;
        let batch = RecordBatch::try_new(Arc::clone(&expected_schema), cols)
            .map_err(|e| FnError::new(0x830, format!("vNode RecordBatch build: {e}")))?;
        Ok(one_batch_stream(expected_schema, batch))
    }
}

/// Build the per-yield columns for `uni.create.vNode`, matching the
/// planner-supplied expected schema. The expected schema's column names
/// drive property resolution (a column named `<n>.foo` pulls `foo`
/// from the props map).
fn build_vnode_columns(
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    expected_schema: &SchemaRef,
    vid: Vid,
    labels: &[String],
    props: &Properties,
) -> Result<Vec<ArrayRef>, FnError> {
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(expected_schema.fields().len());
    let vids = [vid];
    let mut props_map: HashMap<Vid, Properties> = HashMap::new();
    props_map.insert(vid, props.clone());

    for (yield_name, alias) in yield_items {
        let output_name = alias.as_ref().unwrap_or(yield_name);
        let canonical = map_yield_to_canonical(yield_name);

        match canonical {
            "node" => {
                // `<n>._vid` UInt64
                let mut vid_builder = UInt64Builder::with_capacity(1);
                vid_builder.append_value(vid.as_u64());
                cols.push(Arc::new(vid_builder.finish()));

                // `<n>` Utf8 (variable column — vid string)
                let mut var_builder = StringBuilder::new();
                var_builder.append_value(vid.to_string());
                cols.push(Arc::new(var_builder.finish()));

                // `<n>._labels` List<Utf8>
                let mut labels_builder = ListBuilder::new(StringBuilder::new());
                for l in labels {
                    labels_builder.values().append_value(l);
                }
                labels_builder.append(true);
                cols.push(Arc::new(labels_builder.finish()));

                // `<n>.<prop>` columns from target_properties.
                if let Some(prop_names) = target_properties.get(output_name) {
                    for prop_name in prop_names {
                        let col_name = format!("{}.{}", output_name, prop_name);
                        // Use the expected_schema's declared type so we
                        // emit the exact Arrow type the planner expects.
                        let data_type = expected_schema
                            .field_with_name(&col_name)
                            .map(|f| f.data_type().clone())
                            .unwrap_or_else(|_| resolve_property_type(prop_name, None));
                        let col =
                            build_property_column_static(&vids, &props_map, prop_name, &data_type)
                                .map_err(|e| {
                                    FnError::new(
                                        0x830,
                                        format!("vNode property column `{prop_name}`: {e}"),
                                    )
                                })?;
                        cols.push(col);
                    }
                }
            }
            "vid" => {
                #[allow(clippy::cast_possible_wrap)]
                let arr = Int64Array::from(vec![vid.as_u64() as i64]);
                cols.push(Arc::new(arr));
            }
            other => {
                return Err(FnError::new(
                    0x830,
                    format!("vNode: unexpected canonical yield `{other}` for `{yield_name}`"),
                ));
            }
        }
    }

    Ok(cols)
}

// ---------------------------------------------------------------------------
// uni.create.vEdge(src, type, props, dst) — typed Edge yield (M5g)
// ---------------------------------------------------------------------------

/// Build the `edge` Struct field — canonical edge-struct shape
/// (`_eid`, `_type_name`, `_src`, `_dst`, `properties`), matching
/// `df_graph::common::edge_struct_fields()` used by path
/// materialization.
fn edge_yield_field() -> Field {
    Field::new("edge", DataType::Struct(edge_struct_fields()), false)
}

#[derive(Debug)]
pub struct VEdgeProcedure;

impl VEdgeProcedure {
    fn signature_static() -> &'static ProcedureSignature {
        static SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        SIG.get_or_init(|| ProcedureSignature {
            args: vec![
                NamedArgType {
                    name: smol_str::SmolStr::new("src"),
                    ty: ArgType::Primitive(DataType::Int64),
                    default: None,
                    doc: "Source vid (stored or ephemeral).".to_owned(),
                },
                NamedArgType {
                    name: smol_str::SmolStr::new("type"),
                    ty: ArgType::Primitive(DataType::Utf8),
                    default: None,
                    doc: "Edge type name.".to_owned(),
                },
                NamedArgType {
                    name: smol_str::SmolStr::new("props"),
                    ty: ArgType::Primitive(DataType::LargeBinary),
                    default: Some(ScalarValue::LargeBinary(Some(b"{}".to_vec()))),
                    doc: "Property map (JSON-encoded object).".to_owned(),
                },
                NamedArgType {
                    name: smol_str::SmolStr::new("dst"),
                    ty: ArgType::Primitive(DataType::Int64),
                    default: None,
                    doc: "Destination vid (stored or ephemeral).".to_owned(),
                },
            ],
            yields: vec![edge_yield_field()],
            mode: ProcedureMode::Read,
            side_effects: SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "uni.create.vEdge(src, type, props, dst) — mint a \
                   transient, in-query ephemeral edge between two \
                   (stored or ephemeral) vids. Yields a single \
                   canonical Edge struct column. The returned `eid` has \
                   the `EPHEMERAL_BIT` set; writes against it fail \
                   with `EphemeralWriteAttempt`."
                .to_owned(),
        })
    }
}

impl ProcedurePlugin for VEdgeProcedure {
    fn signature(&self) -> &ProcedureSignature {
        Self::signature_static()
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = require_host(&ctx)?;
        let src = args
            .first()
            .and_then(arg_as_i64)
            .ok_or_else(|| FnError::new(0x824, "uni.create.vEdge: src (Int) required"))?;
        let edge_type = args
            .get(1)
            .and_then(arg_as_string)
            .ok_or_else(|| FnError::new(0x824, "uni.create.vEdge: type (String) required"))?;
        let props_json = args
            .get(2)
            .map(arg_to_json)
            .unwrap_or(serde_json::Value::Null);
        let dst = args
            .get(3)
            .and_then(arg_as_i64)
            .ok_or_else(|| FnError::new(0x824, "uni.create.vEdge: dst (Int) required"))?;
        let props_value = uni_common::Value::Map(properties_from_json(&props_json));
        let props_bytes = uni_common::cypher_value_codec::encode(&props_value);

        let eid = Eid::ephemeral(host.allocate_transient_id());

        // Build the canonical edge Struct column.
        #[allow(clippy::cast_sign_loss)]
        let src_u64 = src as u64;
        #[allow(clippy::cast_sign_loss)]
        let dst_u64 = dst as u64;

        let edge_struct =
            build_edge_struct_array(eid.as_u64(), &edge_type, src_u64, dst_u64, &props_bytes)
                .map_err(|e| FnError::new(0x830, format!("vEdge struct build: {e}")))?;

        let schema: SchemaRef = Arc::new(Schema::new(vec![edge_yield_field()]));
        let cols: Vec<ArrayRef> = vec![Arc::new(edge_struct)];
        let batch = RecordBatch::try_new(Arc::clone(&schema), cols)
            .map_err(|e| FnError::new(0x830, format!("vEdge RecordBatch build: {e}")))?;
        Ok(one_batch_stream(schema, batch))
    }
}

/// Build a single-row StructArray matching `edge_struct_fields()`:
/// `(_eid, _type_name, _src, _dst, properties)`.
fn build_edge_struct_array(
    eid: u64,
    type_name: &str,
    src: u64,
    dst: u64,
    props_bytes: &[u8],
) -> Result<StructArray, arrow_schema::ArrowError> {
    let fields = edge_struct_fields();

    let eid_arr: ArrayRef = Arc::new(arrow_array::UInt64Array::from(vec![eid]));
    let type_arr: ArrayRef = Arc::new(StringArray::from(vec![type_name.to_owned()]));
    let src_arr: ArrayRef = Arc::new(arrow_array::UInt64Array::from(vec![src]));
    let dst_arr: ArrayRef = Arc::new(arrow_array::UInt64Array::from(vec![dst]));
    let props_arr: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(props_bytes)]));

    StructArray::try_new(
        fields,
        vec![eid_arr, type_arr, src_arr, dst_arr, props_arr],
        None,
    )
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register every `uni.create.v*` procedure into `r`.
///
/// # Errors
///
/// Propagates [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.procedure(
        QName::new("uni", "create.vNode"),
        VNodeProcedure::signature_static().clone(),
        Arc::new(VNodeProcedure),
    )?;
    r.procedure(
        QName::new("uni", "create.vEdge"),
        VEdgeProcedure::signature_static().clone(),
        Arc::new(VEdgeProcedure),
    )?;
    Ok(())
}
