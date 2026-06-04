// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Physical execution plans that dispatch graph reads against
//! plugin-registered `CatalogTable`s (M5 Batch 2 follow-up #6 — virtual
//! label-id allocation).
//!
//! When the planner encounters a `MATCH (n:External)` whose label is
//! not in the native schema, follow-up #5 consults registered
//! `CatalogProvider` / `ReplacementScanProvider`s for a claim. This
//! file implements the "what happens after the claim succeeds" leg:
//! a virtual `u16` label-id is allocated on `PluginRegistry`, the
//! claiming `CatalogTable` is stashed alongside, and at physical-plan
//! time `CatalogVertexScanExec` adapts that table's rows into the
//! graph-row schema convention every downstream operator expects
//! (`{var}._vid`, `{var}._labels`, `{var}.<prop>` columns).
//!
//! ## Adaptation contract
//!
//! - `_vid` (UInt64) is **synthesized** per row as
//!   `(virtual_label_id as u64) << 48 | row_offset`. The high-16-bit
//!   encoding makes virtual vids unambiguously distinguishable from
//!   native vids (sequentially allocated from 0, well below
//!   `0xFF00_0000_0000_0000`). Row offset increments across batches
//!   within a single `execute()` call via an `AtomicU64`.
//! - `_labels` is **synthesized** as a single-element `[label_name]`
//!   `List<Utf8>` per row.
//! - Property columns are projected from the catalog table's columns
//!   by name match (`prop` ↔ table column named `prop`), then renamed
//!   to the `{var}.{prop}` convention. Properties the table does not
//!   expose materialize as null columns of `Utf8` type (loose typing
//!   for now; tighten when the planner gains a property-type oracle
//!   for virtual labels).
//! - Reserved system column names (`_vid`, `_labels`, etc., and any
//!   name starting with `_`) on the catalog table are rejected at
//!   constructor time — they would collide with our synthesized
//!   columns and silently produce wrong results.
//!
//! ## Edges
//!
//! `CatalogEdgeScanExec` follows the same pattern but synthesizes
//! `_eid`, `_src_vid`, `_dst_vid` columns. The catalog table MUST
//! declare `src_id` and `dst_id` columns (Int64 or UInt64) so the
//! exec can populate `_src_vid`/`_dst_vid`. Without them the
//! constructor errors immediately.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use arrow_array::builder::ListBuilder;
use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr as DfExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use uni_plugin::traits::catalog::CatalogTable;

use crate::query::df_graph::common::{compute_plan_properties, labels_data_type};

/// Per-row virtual-vid base. Encodes the label id in the high 16 bits.
#[inline]
fn virtual_vid_base(virtual_label_id: u16) -> u64 {
    (virtual_label_id as u64) << 48
}

/// Verify the catalog table's schema has no reserved column names.
/// Returns the offending name on failure so the caller can surface it.
fn check_no_reserved_columns(schema: &SchemaRef) -> Result<(), String> {
    for field in schema.fields() {
        if field.name().starts_with('_') {
            return Err(field.name().clone());
        }
    }
    Ok(())
}

// ── Vertex scan ──────────────────────────────────────────────────────

/// Adapts a virtual-label `CatalogTable` into a graph-row-shaped
/// vertex scan. See module docs for the adaptation contract.
pub struct CatalogVertexScanExec {
    table: Arc<dyn CatalogTable>,
    virtual_label_id: u16,
    label_name: String,
    variable: String,
    /// Properties to project, in output order. Each must either match
    /// a catalog-table column name (case-sensitive) or be served as a
    /// nullable `Utf8` column of nulls.
    properties: Vec<String>,
    /// DataFusion filter expressions to pass to `table.scan(filters=)`.
    /// The catalog is free to ignore them; the planner re-applies the
    /// same predicates as a top-level `FilterExec` for safety.
    pushdown_filters: Vec<DfExpr>,
    /// Limit to pass to `table.scan(limit=)`; same "advisory" semantics.
    pushdown_limit: Option<usize>,
    /// Output schema with graph-row convention.
    schema: SchemaRef,
    properties_plan: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for CatalogVertexScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogVertexScanExec")
            .field("label_name", &self.label_name)
            .field(
                "virtual_label_id",
                &format_args!("{:#x}", self.virtual_label_id),
            )
            .field("variable", &self.variable)
            .field("properties", &self.properties)
            .field("pushdown_filters", &self.pushdown_filters.len())
            .field("pushdown_limit", &self.pushdown_limit)
            .finish()
    }
}

impl CatalogVertexScanExec {
    /// Construct a new catalog-backed vertex scan.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog table's schema contains a column
    /// whose name starts with `_` (reserved for synthesized graph-row
    /// system columns).
    pub fn try_new(
        table: Arc<dyn CatalogTable>,
        virtual_label_id: u16,
        label_name: impl Into<String>,
        variable: impl Into<String>,
        properties: Vec<String>,
        pushdown_filters: Vec<DfExpr>,
        pushdown_limit: Option<usize>,
    ) -> anyhow::Result<Self> {
        let label_name = label_name.into();
        let variable = variable.into();
        let table_schema = table.schema();
        if let Err(bad) = check_no_reserved_columns(&table_schema) {
            return Err(anyhow::anyhow!(
                "CatalogTable for label `{label_name}` declares reserved column \
                 `{bad}` (names starting with `_` are synthesized by the graph-row \
                 adapter — rename it in the underlying table)"
            ));
        }
        let schema = Self::build_output_schema(&variable, &properties, &table_schema);
        let properties_plan = compute_plan_properties(schema.clone());
        Ok(Self {
            table,
            virtual_label_id,
            label_name,
            variable,
            properties,
            pushdown_filters,
            pushdown_limit,
            schema,
            properties_plan,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn build_output_schema(
        variable: &str,
        properties: &[String],
        table_schema: &SchemaRef,
    ) -> SchemaRef {
        let mut fields = vec![
            Field::new(format!("{variable}._vid"), DataType::UInt64, false),
            Field::new(format!("{variable}._labels"), labels_data_type(), false),
        ];
        let table_by_name: HashMap<&str, &Field> = table_schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.as_ref()))
            .collect();
        for prop in properties {
            let col_name = format!("{variable}.{prop}");
            let (dtype, nullable) = match table_by_name.get(prop.as_str()) {
                Some(f) => (f.data_type().clone(), true),
                None => (DataType::Utf8, true),
            };
            fields.push(Field::new(&col_name, dtype, nullable));
        }
        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for CatalogVertexScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CatalogVertexScanExec: label={}, virtual_id={:#x}, variable={}, props={:?}",
            self.label_name, self.virtual_label_id, self.variable, self.properties
        )?;
        if !self.pushdown_filters.is_empty() {
            write!(f, ", filters={}", self.pushdown_filters.len())?;
        }
        if let Some(lim) = self.pushdown_limit {
            write!(f, ", limit={lim}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for CatalogVertexScanExec {
    fn name(&self) -> &str {
        "CatalogVertexScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties_plan
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "CatalogVertexScanExec has no children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        // Build projection: indices into the catalog table's schema for
        // every property name we actually want. Properties the table
        // doesn't expose are populated as null columns by the adapter
        // (no projection index).
        let table_schema = self.table.schema();
        let projection: Vec<usize> = self
            .properties
            .iter()
            .filter_map(|p| table_schema.index_of(p).ok())
            .collect();
        let projection_opt = if projection.is_empty() {
            None
        } else {
            Some(projection.as_slice())
        };
        let stream = self
            .table
            .scan(projection_opt, &self.pushdown_filters, self.pushdown_limit)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "CatalogTable::scan failed: {e}"
                ))
            })?;
        Ok(Box::pin(VertexAdapterStream {
            inner: stream,
            output_schema: self.schema.clone(),
            virtual_label_id: self.virtual_label_id,
            label_name: self.label_name.clone(),
            variable: self.variable.clone(),
            properties: self.properties.clone(),
            next_offset: AtomicU64::new(0),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct VertexAdapterStream {
    inner: SendableRecordBatchStream,
    output_schema: SchemaRef,
    virtual_label_id: u16,
    label_name: String,
    variable: String,
    properties: Vec<String>,
    next_offset: AtomicU64,
    metrics: BaselineMetrics,
}

impl RecordBatchStream for VertexAdapterStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for VertexAdapterStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => {
                let row_count = batch.num_rows();
                let base = virtual_vid_base(self.virtual_label_id)
                    | self
                        .next_offset
                        .fetch_add(row_count as u64, Ordering::SeqCst);
                let adapted = adapt_vertex_batch(
                    &batch,
                    &self.output_schema,
                    base,
                    &self.label_name,
                    &self.variable,
                    &self.properties,
                );
                self.metrics.record_output(row_count);
                Poll::Ready(Some(adapted))
            }
        }
    }
}

/// Build a graph-row-shaped batch from the catalog table's batch. The
/// `vid_start` is the value of `_vid` for the first row.
fn adapt_vertex_batch(
    in_batch: &RecordBatch,
    output_schema: &SchemaRef,
    vid_start: u64,
    label_name: &str,
    variable: &str,
    properties: &[String],
) -> DFResult<RecordBatch> {
    let n = in_batch.num_rows();
    let vid_array: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        (0..n as u64).map(|i| vid_start + i),
    ));
    let labels_array: ArrayRef = {
        let mut b = ListBuilder::new(arrow_array::builder::StringBuilder::new());
        for _ in 0..n {
            b.values().append_value(label_name);
            b.append(true);
        }
        Arc::new(b.finish())
    };
    let in_schema = in_batch.schema();
    let in_by_name: HashMap<&str, ArrayRef> = in_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), in_batch.column(i).clone()))
        .collect();
    let _ = variable; // already embedded in output_schema field names
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());
    columns.push(vid_array);
    columns.push(labels_array);
    for prop in properties {
        let col = in_by_name
            .get(prop.as_str())
            .cloned()
            .unwrap_or_else(|| Arc::new(StringArray::new_null(n)));
        columns.push(col);
    }
    RecordBatch::try_new(output_schema.clone(), columns).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "CatalogVertexScanExec: failed to assemble adapted batch: {e}"
        ))
    })
}

// ── Edge scan ────────────────────────────────────────────────────────

/// Adapts a virtual-edge-type `CatalogTable` into a graph-row-shaped
/// edge scan. The table MUST declare `src_id` and `dst_id` columns;
/// `_eid` is synthesized per row from the virtual edge-type id.
pub struct CatalogEdgeScanExec {
    table: Arc<dyn CatalogTable>,
    virtual_type_id: u32,
    type_name: String,
    variable: String,
    properties: Vec<String>,
    pushdown_filters: Vec<DfExpr>,
    pushdown_limit: Option<usize>,
    schema: SchemaRef,
    properties_plan: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for CatalogEdgeScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogEdgeScanExec")
            .field("type_name", &self.type_name)
            .field(
                "virtual_type_id",
                &format_args!("{:#x}", self.virtual_type_id),
            )
            .field("variable", &self.variable)
            .field("properties", &self.properties)
            .finish()
    }
}

impl CatalogEdgeScanExec {
    /// # Errors
    /// Returns an error if the table's schema lacks `src_id`/`dst_id`,
    /// or if it declares a column whose name starts with `_`.
    pub fn try_new(
        table: Arc<dyn CatalogTable>,
        virtual_type_id: u32,
        type_name: impl Into<String>,
        variable: impl Into<String>,
        properties: Vec<String>,
        pushdown_filters: Vec<DfExpr>,
        pushdown_limit: Option<usize>,
    ) -> anyhow::Result<Self> {
        let type_name = type_name.into();
        let variable = variable.into();
        let table_schema = table.schema();
        if let Err(bad) = check_no_reserved_columns(&table_schema) {
            return Err(anyhow::anyhow!(
                "CatalogTable for edge type `{type_name}` declares reserved column \
                 `{bad}` (names starting with `_` are synthesized by the graph-row adapter)"
            ));
        }
        for required in ["src_id", "dst_id"] {
            if table_schema.index_of(required).is_err() {
                return Err(anyhow::anyhow!(
                    "CatalogTable for edge type `{type_name}` must declare a \
                     `{required}` column (mapped to `_{}_vid` in the graph-row \
                     adapter)",
                    if required == "src_id" { "src" } else { "dst" }
                ));
            }
        }
        let schema = Self::build_output_schema(&variable, &properties, &table_schema);
        let properties_plan = compute_plan_properties(schema.clone());
        Ok(Self {
            table,
            virtual_type_id,
            type_name,
            variable,
            properties,
            pushdown_filters,
            pushdown_limit,
            schema,
            properties_plan,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn build_output_schema(
        variable: &str,
        properties: &[String],
        table_schema: &SchemaRef,
    ) -> SchemaRef {
        let mut fields = vec![
            Field::new(format!("{variable}._eid"), DataType::UInt64, false),
            Field::new(format!("{variable}._src_vid"), DataType::UInt64, false),
            Field::new(format!("{variable}._dst_vid"), DataType::UInt64, false),
        ];
        let table_by_name: HashMap<&str, &Field> = table_schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.as_ref()))
            .collect();
        for prop in properties {
            if prop == "src_id" || prop == "dst_id" {
                // These are surfaced via the synthesized `_src_vid` /
                // `_dst_vid` system columns; don't double-project.
                continue;
            }
            let col_name = format!("{variable}.{prop}");
            let (dtype, nullable) = match table_by_name.get(prop.as_str()) {
                Some(f) => (f.data_type().clone(), true),
                None => (DataType::Utf8, true),
            };
            fields.push(Field::new(&col_name, dtype, nullable));
        }
        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for CatalogEdgeScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CatalogEdgeScanExec: type={}, virtual_id={:#x}, variable={}, props={:?}",
            self.type_name, self.virtual_type_id, self.variable, self.properties
        )
    }
}

impl ExecutionPlan for CatalogEdgeScanExec {
    fn name(&self) -> &str {
        "CatalogEdgeScanExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties_plan
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "CatalogEdgeScanExec has no children".into(),
            ));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let table_schema = self.table.schema();
        // The projection must include src_id/dst_id (so the adapter
        // can populate _src_vid/_dst_vid) plus the requested
        // properties. Build the projection list deterministically.
        let mut wanted: Vec<&str> = vec!["src_id", "dst_id"];
        for p in &self.properties {
            if p != "src_id" && p != "dst_id" {
                wanted.push(p.as_str());
            }
        }
        let projection: Vec<usize> = wanted
            .iter()
            .filter_map(|p| table_schema.index_of(p).ok())
            .collect();
        let projection_opt = if projection.is_empty() {
            None
        } else {
            Some(projection.as_slice())
        };
        let stream = self
            .table
            .scan(projection_opt, &self.pushdown_filters, self.pushdown_limit)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "CatalogTable::scan failed: {e}"
                ))
            })?;
        Ok(Box::pin(EdgeAdapterStream {
            inner: stream,
            output_schema: self.schema.clone(),
            virtual_type_id: self.virtual_type_id,
            variable: self.variable.clone(),
            properties: self.properties.clone(),
            next_offset: AtomicU64::new(0),
            metrics,
        }))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct EdgeAdapterStream {
    inner: SendableRecordBatchStream,
    output_schema: SchemaRef,
    virtual_type_id: u32,
    variable: String,
    properties: Vec<String>,
    next_offset: AtomicU64,
    metrics: BaselineMetrics,
}

impl RecordBatchStream for EdgeAdapterStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for EdgeAdapterStream {
    type Item = DFResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => {
                let row_count = batch.num_rows();
                let base = ((self.virtual_type_id as u64) << 32)
                    | self
                        .next_offset
                        .fetch_add(row_count as u64, Ordering::SeqCst);
                let adapted = adapt_edge_batch(
                    &batch,
                    &self.output_schema,
                    base,
                    &self.variable,
                    &self.properties,
                );
                self.metrics.record_output(row_count);
                Poll::Ready(Some(adapted))
            }
        }
    }
}

fn adapt_edge_batch(
    in_batch: &RecordBatch,
    output_schema: &SchemaRef,
    eid_start: u64,
    variable: &str,
    properties: &[String],
) -> DFResult<RecordBatch> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    let n = in_batch.num_rows();
    let eid: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        (0..n as u64).map(|i| eid_start + i),
    ));
    let in_schema = in_batch.schema();
    let in_by_name: HashMap<&str, ArrayRef> = in_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), in_batch.column(i).clone()))
        .collect();
    let to_u64 = |arr: &ArrayRef| -> DFResult<ArrayRef> {
        match arr.data_type() {
            DataType::UInt64 => Ok(arr.clone()),
            DataType::Int64 => {
                let a = arr.as_primitive::<Int64Type>();
                Ok(Arc::new(UInt64Array::from_iter_values(
                    (0..a.len()).map(|i| a.value(i) as u64),
                )))
            }
            DataType::UInt32 => {
                let a = arr.as_primitive::<arrow_array::types::UInt32Type>();
                Ok(Arc::new(UInt64Array::from_iter_values(
                    (0..a.len()).map(|i| u64::from(a.value(i))),
                )))
            }
            other => Err(datafusion::error::DataFusionError::Execution(format!(
                "CatalogEdgeScanExec: src_id/dst_id must be Int64/UInt64/UInt32, got {other:?}"
            ))),
        }
    };
    let src_arr = in_by_name.get("src_id").ok_or_else(|| {
        datafusion::error::DataFusionError::Execution("missing src_id column".into())
    })?;
    let dst_arr = in_by_name.get("dst_id").ok_or_else(|| {
        datafusion::error::DataFusionError::Execution("missing dst_id column".into())
    })?;
    let src_vid = to_u64(src_arr)?;
    let dst_vid = to_u64(dst_arr)?;

    let _ = variable;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());
    columns.push(eid);
    columns.push(src_vid);
    columns.push(dst_vid);
    for prop in properties {
        if prop == "src_id" || prop == "dst_id" {
            continue;
        }
        let col = in_by_name
            .get(prop.as_str())
            .cloned()
            .unwrap_or_else(|| Arc::new(StringArray::new_null(n)));
        columns.push(col);
    }
    RecordBatch::try_new(output_schema.clone(), columns).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "CatalogEdgeScanExec: failed to assemble adapted batch: {e}"
        ))
    })
}
