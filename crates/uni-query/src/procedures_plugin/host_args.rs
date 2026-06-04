// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shared helpers for host-coupled procedure plugins
//! (`uni.vector.query`, `uni.fts.query`, `uni.search`).
//!
//! Two small responsibilities:
//! - Down-cast `ProcedureContext::host` to [`QueryProcedureHost`] with a
//!   uniform error.
//! - Convert `&[ColumnarValue]` (what the plugin invoke receives) back
//!   into the `Vec<uni_common::Value>` shape the legacy `run_*`
//!   helpers expect. Complex types travel as LargeBinary JSON between
//!   the dispatcher (`value_to_columnar`) and us; decode them back.

use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use uni_common::Value;
use uni_plugin::FnError;
use uni_plugin::traits::procedure::ProcedureContext;

use crate::query::executor::procedure_host::QueryProcedureHost;

// Rust guideline compliant

/// Down-cast the procedure's host to a [`QueryProcedureHost`] or return a
/// uniform error tagged with the calling procedure name.
pub(super) fn require_host<'a>(
    ctx: &'a ProcedureContext<'_>,
    procedure: &str,
) -> Result<&'a QueryProcedureHost, FnError> {
    ctx.host
        .and_then(|h| h.as_any().downcast_ref::<QueryProcedureHost>())
        .ok_or_else(|| {
            FnError::new(
                0x701,
                format!(
                    "{procedure}: requires QueryProcedureHost (host not bound on ProcedureContext)"
                ),
            )
        })
}

/// Convert a `ColumnarValue` (`ScalarValue::*` literal or `LargeBinary`
/// JSON envelope produced by `procedure_call::value_to_columnar`) back
/// into a `uni_common::Value`.
pub(super) fn columnar_to_value(cv: &ColumnarValue) -> Value {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Null) => Value::Null,
        ColumnarValue::Scalar(ScalarValue::Boolean(Some(b))) => Value::Bool(*b),
        ColumnarValue::Scalar(ScalarValue::Boolean(None)) => Value::Null,
        ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => Value::Int(*i),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => Value::Int(i64::from(*i)),
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(u))) => {
            i64::try_from(*u).map(Value::Int).unwrap_or(Value::Null)
        }
        ColumnarValue::Scalar(ScalarValue::Float64(Some(f))) => Value::Float(*f),
        ColumnarValue::Scalar(ScalarValue::Float32(Some(f))) => Value::Float(f64::from(*f)),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Value::String(s.clone()),
        ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => Value::Bytes(b.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(b))) => {
            // The dispatcher encodes complex Cypher values (List / Map /
            // Vector / Node / Edge) as LargeBinary serde_json bytes via
            // `procedure_call::value_to_columnar`. Decode back via the
            // existing `From<serde_json::Value> for Value` impl.
            match serde_json::from_slice::<serde_json::Value>(b) {
                Ok(json) => json.into(),
                Err(_) => Value::Null,
            }
        }
        _ => Value::Null,
    }
}

/// Convert every arg in one shot.
pub(super) fn columnar_args_to_values(args: &[ColumnarValue]) -> Vec<Value> {
    args.iter().map(columnar_to_value).collect()
}
