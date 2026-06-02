// Rust guideline compliant
//! `DeclaredScalarFn` — a [`ScalarPluginFn`] that evaluates a parsed
//! Cypher expression body row-by-row.
//!
//! Constructed by the `uni.plugin.declareFunction` procedure with a
//! pre-parsed [`Expr`] body and a list of declared argument names. On
//! every invocation, each row's input columns are decoded into
//! `uni_common::Value`, bound to the declared parameter names, fed
//! through the [`crate::eval::eval_expr`] interpreter, and re-encoded
//! into the output Arrow column.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use uni_common::Value;
use uni_cypher::ast::Expr;
use uni_plugin::FnError;
use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling, ScalarPluginFn};

use crate::decode::{array_value_at, eval_err_to_fn, stringify};
use crate::eval::eval_expr;

/// A scalar function declared from Cypher via
/// `uni.plugin.declareFunction`.
///
/// Holds a parsed [`Expr`] body, the declared argument names (in
/// positional order — same order as the columns passed to
/// [`ScalarPluginFn::invoke`]), and a precomputed [`FnSignature`].
pub struct DeclaredScalarFn {
    body: Expr,
    arg_names: Vec<String>,
    signature: FnSignature,
}

impl std::fmt::Debug for DeclaredScalarFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeclaredScalarFn")
            .field("arg_names", &self.arg_names)
            .field("return_type", &self.signature.returns)
            .finish_non_exhaustive()
    }
}

impl DeclaredScalarFn {
    /// Construct a declared scalar function.
    ///
    /// `arg_names` must align with the positional arguments — element
    /// 0 of `arg_names` corresponds to column 0 in the invocation's
    /// `args` slice.
    #[must_use]
    pub fn new(body: Expr, arg_names: Vec<String>, signature: FnSignature) -> Self {
        Self {
            body,
            arg_names,
            signature,
        }
    }

    /// Construct a default [`FnSignature`] given an Arrow return type
    /// and a list of `(name, type)` pairs for arguments.
    #[must_use]
    pub fn build_signature(returns: DataType, args: &[(String, DataType)]) -> FnSignature {
        FnSignature {
            args: args
                .iter()
                .map(|(_, t)| ArgType::Primitive(t.clone()))
                .collect(),
            returns: ArgType::Primitive(returns),
            volatility: Volatility::Volatile,
            null_handling: NullHandling::UserHandled,
        }
    }
}

impl ScalarPluginFn for DeclaredScalarFn {
    fn signature(&self) -> &FnSignature {
        &self.signature
    }

    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        if args.len() != self.arg_names.len() {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "declared scalar fn expected {} args, got {}",
                    self.arg_names.len(),
                    args.len()
                ),
            ));
        }
        let row_count = rows.max(1);
        let columns: Vec<ArrayRef> = args
            .iter()
            .map(|cv| columnar_to_array(cv, row_count))
            .collect::<Result<_, _>>()?;

        let return_dt = match &self.signature.returns {
            ArgType::Primitive(dt) => dt.clone(),
            other => {
                return Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("declared fn return type not supported: {other:?}"),
                ));
            }
        };

        let out = build_output(&return_dt, row_count, |row| {
            let mut bindings = std::collections::HashMap::with_capacity(columns.len());
            for (i, col) in columns.iter().enumerate() {
                bindings.insert(self.arg_names[i].clone(), array_value_at(col, row)?);
            }
            eval_expr(&self.body, &bindings).map_err(eval_err_to_fn)
        })?;

        Ok(ColumnarValue::Array(out))
    }
}

fn columnar_to_array(cv: &ColumnarValue, rows: usize) -> Result<ArrayRef, FnError> {
    match cv {
        ColumnarValue::Array(a) => Ok(Arc::clone(a)),
        ColumnarValue::Scalar(s) => s
            .to_array_of_size(rows)
            .map_err(|e| FnError::new(FnError::CODE_TYPE_COERCION, format!("scalar→array: {e}"))),
    }
}

fn build_output(
    dt: &DataType,
    rows: usize,
    mut row_value: impl FnMut(usize) -> Result<Value, FnError>,
) -> Result<ArrayRef, FnError> {
    match dt {
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows, rows * 8);
            for row in 0..rows {
                match row_value(row)? {
                    Value::Null => b.append_null(),
                    Value::String(s) => b.append_value(s),
                    other => b.append_value(stringify(&other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows);
            for row in 0..rows {
                match row_value(row)? {
                    Value::Null => b.append_null(),
                    Value::Int(i) => b.append_value(i),
                    Value::Float(f) => b.append_value(f as i64),
                    other => {
                        return Err(FnError::new(
                            FnError::CODE_TYPE_COERCION,
                            format!("expected Int64, got {other:?}"),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows);
            for row in 0..rows {
                match row_value(row)? {
                    Value::Null => b.append_null(),
                    Value::Int(i) => b.append_value(i as f64),
                    Value::Float(f) => b.append_value(f),
                    other => {
                        return Err(FnError::new(
                            FnError::CODE_TYPE_COERCION,
                            format!("expected Float64, got {other:?}"),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows);
            for row in 0..rows {
                match row_value(row)? {
                    Value::Null => b.append_null(),
                    Value::Bool(v) => b.append_value(v),
                    other => {
                        return Err(FnError::new(
                            FnError::CODE_TYPE_COERCION,
                            format!("expected Boolean, got {other:?}"),
                        ));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("declared fn return type {other:?} not supported"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, StringArray};
    use datafusion::scalar::ScalarValue;
    use uni_cypher::parse_expression;

    fn fn_string(body: &str, arg_names: &[&str]) -> DeclaredScalarFn {
        let body = parse_expression(body).unwrap();
        let arg_names: Vec<String> = arg_names.iter().map(|s| (*s).to_owned()).collect();
        let sig_args: Vec<(String, DataType)> = arg_names
            .iter()
            .map(|n| (n.clone(), DataType::Utf8))
            .collect();
        let sig = DeclaredScalarFn::build_signature(DataType::Utf8, &sig_args);
        DeclaredScalarFn::new(body, arg_names, sig)
    }

    #[test]
    fn invoke_string_concat_via_scalars() {
        let f = fn_string("$first + ' ' + $last", &["first", "last"]);
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Ada".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Lovelace".to_owned()))),
        ];
        let out = f.invoke(&args, 1).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(_) => panic!("expected array"),
        };
        let s = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "Ada Lovelace");
    }

    #[test]
    fn invoke_arity_mismatch() {
        let f = fn_string("$first + ' ' + $last", &["first", "last"]);
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "a".to_owned(),
        )))];
        let err = f.invoke(&args, 1).unwrap_err();
        assert_eq!(err.code, FnError::CODE_TYPE_COERCION);
    }
}
