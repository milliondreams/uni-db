// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Propagation of the `uni_raw_bytes` field-metadata marker onto **computed**
//! projection outputs.
//!
//! `DataType::Bytes`, `CypherValue`, and `Duration` all serialize to Arrow
//! `LargeBinary`; they are disambiguated only by the `uni_raw_bytes=true` Arrow
//! field-metadata marker (stamped at scan in `df_graph::scan::property_field`,
//! honored at read in `executor::read::record_batch_to_rows` and in nested
//! containers by `uni_store::storage::arrow_convert`). DataFusion's `ProjectionExec`
//! preserves whatever metadata each expression's `return_field` reports, so a plain
//! column passthrough keeps its marker. A **computed** expression (a `coalesce`
//! `CaseExpr`, a `make_array` list literal, …) returns a fresh field with no marker,
//! so a raw `Bytes` value flowing through it is mis-decoded by the tagged codec.
//!
//! `RawBytesMarkerExpr` is a thin identity wrapper that delegates evaluation to its
//! inner expression and only overrides the output field so it carries the marker —
//! either on the field itself (scalar, e.g. a `coalesce` of raw bytes) or on the
//! child of a `List` field (e.g. a `make_array` of raw bytes, so the element-decode
//! path reads each element verbatim). `bytes_shape` decides, conservatively, when a
//! given Cypher expression's output is uniformly raw bytes and which mode applies.

use std::any::Any;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::array::{Array, ArrayRef, LargeListArray, ListArray};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::PhysicalExpr;
use uni_common::core::schema::raw_bytes_field_metadata;
use uni_cypher::ast::{CypherLiteral, Expr};

/// Identity `PhysicalExpr` wrapper that stamps the `uni_raw_bytes` marker on its
/// inner expression's output field.
///
/// When `on_child` is false the marker is added to the scalar output field; when
/// true it is added to a `List`/`LargeList`/`FixedSizeList` field's child, and
/// [`PhysicalExpr::evaluate`] re-stamps the produced array's child so the array's own
/// `DataType` matches the marked output schema (DataFusion validates this on
/// `RecordBatch::try_new`).
#[derive(Debug)]
pub(crate) struct RawBytesMarkerExpr {
    inner: Arc<dyn PhysicalExpr>,
    on_child: bool,
}

impl RawBytesMarkerExpr {
    /// Wraps `inner`, marking the scalar output field as raw `Bytes`.
    pub(crate) fn scalar(inner: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            inner,
            on_child: false,
        }
    }

    /// Wraps `inner` (a `List`-typed expression), marking the list child field as raw
    /// `Bytes`.
    pub(crate) fn list_child(inner: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            inner,
            on_child: true,
        }
    }
}

/// Returns `dt` with the `uni_raw_bytes` marker added to the list child field, or
/// `dt` unchanged when it is not a list type.
fn mark_list_child_type(dt: DataType) -> DataType {
    let marked = |child: &Arc<Field>| {
        Arc::new(
            child
                .as_ref()
                .clone()
                .with_metadata(raw_bytes_field_metadata()),
        )
    };
    match dt {
        DataType::List(child) => DataType::List(marked(&child)),
        DataType::LargeList(child) => DataType::LargeList(marked(&child)),
        DataType::FixedSizeList(child, n) => DataType::FixedSizeList(marked(&child), n),
        other => other,
    }
}

/// Rebuilds a list array with the `uni_raw_bytes` marker on its child field, reusing
/// the original offsets/values/nulls. Non-list arrays are returned unchanged.
fn restamp_list_child(array: ArrayRef) -> ArrayRef {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let (field, offsets, values, nulls) = list.clone().into_parts();
        let new_field = Arc::new(
            field
                .as_ref()
                .clone()
                .with_metadata(raw_bytes_field_metadata()),
        );
        return Arc::new(ListArray::new(new_field, offsets, values, nulls));
    }
    if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        let (field, offsets, values, nulls) = list.clone().into_parts();
        let new_field = Arc::new(
            field
                .as_ref()
                .clone()
                .with_metadata(raw_bytes_field_metadata()),
        );
        return Arc::new(LargeListArray::new(new_field, offsets, values, nulls));
    }
    array
}

impl Display for RawBytesMarkerExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Identity for plan readability — the wrapper only affects field metadata.
        write!(f, "{}", self.inner)
    }
}

impl PartialEq for RawBytesMarkerExpr {
    fn eq(&self, other: &Self) -> bool {
        self.on_child == other.on_child && Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for RawBytesMarkerExpr {}

impl std::hash::Hash for RawBytesMarkerExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::any::type_name::<Self>().hash(state);
        self.on_child.hash(state);
    }
}

impl PartialEq<dyn Any> for RawBytesMarkerExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl PhysicalExpr for RawBytesMarkerExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DfResult<DataType> {
        let dt = self.inner.data_type(input_schema)?;
        Ok(if self.on_child {
            mark_list_child_type(dt)
        } else {
            dt
        })
    }

    fn nullable(&self, input_schema: &Schema) -> DfResult<bool> {
        self.inner.nullable(input_schema)
    }

    fn return_field(&self, input_schema: &Schema) -> DfResult<Arc<Field>> {
        let field = self.inner.return_field(input_schema)?;
        if self.on_child {
            let dt = mark_list_child_type(field.data_type().clone());
            Ok(Arc::new(
                Field::new(field.name(), dt, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
            ))
        } else {
            let mut metadata = field.metadata().clone();
            metadata.extend(raw_bytes_field_metadata());
            Ok(Arc::new(field.as_ref().clone().with_metadata(metadata)))
        }
    }

    fn evaluate(&self, batch: &datafusion::arrow::array::RecordBatch) -> DfResult<ColumnarValue> {
        let value = self.inner.evaluate(batch)?;
        if !self.on_child {
            return Ok(value);
        }
        // Re-stamp the child metadata into the materialized array so its DataType
        // matches the marked output field, and so downstream consumers that honor
        // child metadata (list element-extraction UDFs) read raw bytes verbatim.
        match value {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(restamp_list_child(array))),
            ColumnarValue::Scalar(ScalarValue::List(arr)) => {
                let restamped = restamp_list_child(arr as ArrayRef);
                match restamped.as_any().downcast_ref::<ListArray>() {
                    Some(list) => Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(
                        list.clone(),
                    )))),
                    None => Err(DataFusionError::Internal(
                        "RawBytesMarkerExpr: restamped scalar list is not a ListArray".to_string(),
                    )),
                }
            }
            other => Ok(other),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DfResult<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "RawBytesMarkerExpr expects exactly 1 child".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            inner: children[0].clone(),
            on_child: self.on_child,
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }
}

// ============================================================================
// bytes_shape analyzer
// ============================================================================

/// The raw-`Bytes` shape of a Cypher expression's projection output.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Shape {
    /// Not a raw-`Bytes` value (or unprovable) — leave unmarked.
    None,
    /// A `NULL`/absent value — neutral; compatible with any raw-bytes branch.
    Null,
    /// A raw-`Bytes` scalar (`LargeBinary` holding unencoded bytes), e.g. a marked
    /// column or a `coalesce`/`CASE` whose branches are all raw bytes or null.
    RawScalar,
    /// A `List` whose elements are raw `Bytes` (the child field should be marked).
    RawList,
}

/// Reads a field's marker into a [`Shape`].
fn shape_of_field(field: &Field) -> Shape {
    if field
        .metadata()
        .get("uni_raw_bytes")
        .is_some_and(|v| v == "true")
    {
        return Shape::RawScalar;
    }
    match field.data_type() {
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _)
            if child
                .metadata()
                .get("uni_raw_bytes")
                .is_some_and(|v| v == "true") =>
        {
            Shape::RawList
        }
        DataType::Null => Shape::Null,
        _ => Shape::None,
    }
}

/// Resolves a bare variable column's shape.
fn variable_shape(schema: &Schema, name: &str) -> Shape {
    match schema.column_with_name(name) {
        Some((_, field)) => shape_of_field(field),
        None => Shape::None,
    }
}

/// Resolves `var.prop`, mirroring `CypherPhysicalExprCompiler::compile_property_access`:
/// a missing struct key or absent flat column is Cypher `NULL`. Checks both the struct
/// child and the flat `"{var}.{prop}"` column and reports a marked shape if either is
/// marked; an absent property in both is `Null`.
fn property_shape(schema: &Schema, var: &str, prop: &str) -> Shape {
    let mut present_unmarked = false;

    if let Ok(idx) = schema.index_of(var)
        && let DataType::Struct(fields) = schema.field(idx).data_type()
        && let Some(field) = fields.iter().find(|f| f.name() == prop)
    {
        // A present struct child: marked → raw shape; otherwise it is a real
        // unmarked value (a missing struct key, handled below, stays `Null`).
        let shape = shape_of_field(field);
        if shape != Shape::None {
            return shape;
        }
        present_unmarked = true;
    }

    let flat = format!("{var}.{prop}");
    if let Some((_, field)) = schema.column_with_name(&flat) {
        let shape = shape_of_field(field);
        if shape != Shape::None {
            return shape;
        }
        present_unmarked = true;
    }

    if present_unmarked {
        Shape::None
    } else {
        Shape::Null
    }
}

/// Merges branch shapes for `coalesce`/`CASE`/list-literal element unification.
///
/// Returns `None` if any branch is `None`; `RawList` if all branches are
/// `RawList`/`Null` with ≥1 `RawList`; `RawScalar` if all are `RawScalar`/`Null` with
/// ≥1 `RawScalar`; `Null` if all are `Null`; otherwise `None` (mixed scalar/list).
fn merge_shapes(shapes: impl IntoIterator<Item = Shape>) -> Shape {
    let mut saw_scalar = false;
    let mut saw_list = false;
    for shape in shapes {
        match shape {
            Shape::None => return Shape::None,
            Shape::Null => {}
            Shape::RawScalar => saw_scalar = true,
            Shape::RawList => saw_list = true,
        }
    }
    match (saw_scalar, saw_list) {
        (false, false) => Shape::Null,
        (true, false) => Shape::RawScalar,
        (false, true) => Shape::RawList,
        (true, true) => Shape::None,
    }
}

/// Conservatively infers the raw-`Bytes` [`Shape`] of a projection expression.
///
/// Marks only what is provably uniform raw bytes: a wrong marker corrupts, while a
/// missing one merely leaves a pre-existing decode bug. Element-extraction shapes
/// (`head`/`last`/`index`) are `None` because those UDFs re-encode their result as a
/// tagged CypherValue, which the codec already reads correctly — they instead need
/// their *input* list child marked (handled at compile time).
pub(crate) fn bytes_shape(expr: &Expr, schema: &Schema) -> Shape {
    match expr {
        Expr::Literal(CypherLiteral::Null) => Shape::Null,
        Expr::Variable(name) => variable_shape(schema, name),
        Expr::Property(base, prop) => match base.as_ref() {
            Expr::Variable(var) => property_shape(schema, var, prop),
            _ => Shape::None,
        },
        Expr::FunctionCall { name, args, .. } if name.eq_ignore_ascii_case("coalesce") => {
            merge_shapes(args.iter().map(|a| bytes_shape(a, schema)))
        }
        Expr::Case {
            when_then,
            else_expr,
            ..
        } => {
            let thens = when_then.iter().map(|(_, then)| bytes_shape(then, schema));
            let els = else_expr
                .as_deref()
                .map(|e| bytes_shape(e, schema))
                .into_iter();
            merge_shapes(thens.chain(els))
        }
        Expr::List(items) => {
            // A list literal is a raw-bytes List iff its elements are all raw scalars
            // or null with ≥1 raw scalar. Such elements (raw-bytes columns, nulls)
            // never trigger the CypherValue-encoded `_make_cypher_list` routing in
            // `translate_list_literal`, so this implies the `make_array` lowering.
            match merge_shapes(items.iter().map(|e| bytes_shape(e, schema))) {
                Shape::RawScalar => Shape::RawList,
                _ => Shape::None,
            }
        }
        // Everything else (head/last/index, tail/reverse/slice, maps, comprehensions,
        // aggregates, arithmetic, …) is not a verbatim raw-bytes producer.
        _ => Shape::None,
    }
}

/// Whether `expr`'s output is a raw-`Bytes` scalar needing an output-field marker
/// (used for `coalesce`/`CASE` at the projection site).
pub(crate) fn is_raw_scalar(expr: &Expr, schema: &Schema) -> bool {
    bytes_shape(expr, schema) == Shape::RawScalar
}

/// Whether `expr` is a markable raw-`Bytes` list literal (used to wrap the
/// `make_array` child at compile time, for direct returns and list-function inputs).
pub(crate) fn is_markable_list(expr: &Expr, schema: &Schema) -> bool {
    bytes_shape(expr, schema) == Shape::RawList
}

/// Whether a coalesce mixes a raw-`Bytes` arg with a non-raw, non-null arg.
///
/// Such a coalesce would otherwise produce a column with both raw and CypherValue-
/// encoded rows (un-markable). Its `THEN`/`ELSE` values must be CypherValue-encoded so
/// every row decodes through the codec (a raw `Bytes` value round-trips to
/// `Value::Bytes`). A coalesce that is uniformly raw-or-null is NOT included here —
/// it keeps its raw output and is marked by [`is_raw_scalar`] at the projection. The
/// two predicates are complementary, so a coalesce is never both CV-unified and marked.
pub(crate) fn coalesce_needs_cv_unify(args: &[Expr], schema: &Schema) -> bool {
    let any_raw = args
        .iter()
        .any(|a| bytes_shape(a, schema) == Shape::RawScalar);
    any_raw && merge_shapes(args.iter().map(|a| bytes_shape(a, schema))) != Shape::RawScalar
}
