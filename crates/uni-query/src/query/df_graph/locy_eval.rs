// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! In-memory expression evaluation for Locy commands.
//!
//! Ported from `uni-locy/src/orchestrator/eval.rs`. Used by SLG, QUERY, EXPLAIN,
//! ASSUME, ABDUCE, and DERIVE in the native command dispatch path.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use uni_common::Value;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, UnaryOp};
use uni_cypher::locy_ast::{LocyBinaryOp, LocyExpr};
use uni_locy::{LocyError, Row};

/// Evaluate a Locy expression (which may contain prev references) given current bindings
/// and optional previous-iteration values.
pub fn eval_locy_expr(
    expr: &LocyExpr,
    bindings: &Row,
    prev_values: Option<&Row>,
) -> Result<Value, LocyError> {
    match expr {
        LocyExpr::PrevRef(field) => Ok(prev_values
            .and_then(|prev| prev.get(field).cloned())
            .unwrap_or(Value::Null)),
        LocyExpr::Cypher(cypher_expr) => eval_expr(cypher_expr, bindings),
        LocyExpr::BinaryOp { left, op, right } => {
            let l = eval_locy_expr(left, bindings, prev_values)?;
            let r = eval_locy_expr(right, bindings, prev_values)?;
            eval_locy_binary_op(&l, op, &r)
        }
        LocyExpr::UnaryOp(op, inner) => {
            let v = eval_locy_expr(inner, bindings, prev_values)?;
            eval_unary_op(op, &v)
        }
    }
}

/// Evaluate a Cypher expression given variable bindings.
pub fn eval_expr(expr: &Expr, bindings: &Row) -> Result<Value, LocyError> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),
        Expr::Variable(name) => Ok(bindings.get(name).cloned().unwrap_or(Value::Null)),
        Expr::Property(expr, property) => {
            let base = eval_expr(expr, bindings)?;
            Ok(get_property(&base, property))
        }
        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, bindings)?;
            let r = eval_expr(right, bindings)?;
            eval_binary_op(&l, op, &r)
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, bindings)?;
            eval_unary_op(op, &v)
        }
        Expr::FunctionCall { name, args, .. } => {
            let evaluated_args: Result<Vec<Value>, _> =
                args.iter().map(|a| eval_expr(a, bindings)).collect();
            eval_function(name, &evaluated_args?)
        }
        Expr::Parameter(name) => Ok(bindings.get(name).cloned().unwrap_or(Value::Null)),
        Expr::IsNull(inner) => {
            let v = eval_expr(inner, bindings)?;
            Ok(Value::Bool(v.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let v = eval_expr(inner, bindings)?;
            Ok(Value::Bool(!v.is_null()))
        }
        Expr::List(items) => {
            let vals: Result<Vec<Value>, _> =
                items.iter().map(|i| eval_expr(i, bindings)).collect();
            Ok(Value::List(vals?))
        }
        Expr::Map(entries) => {
            let mut map = HashMap::new();
            for (k, v) in entries {
                map.insert(k.clone(), eval_expr(v, bindings)?);
            }
            Ok(Value::Map(map))
        }
        _ => Err(LocyError::EvaluationError {
            message: format!("unsupported expression in in-memory evaluation: {expr:?}"),
        }),
    }
}

/// Evaluate an aggregate function over a group of rows.
pub fn eval_aggregate_over_group(
    func_name: &str,
    arg_expr: &Expr,
    group: &[Row],
    rule_name: &str,
    fold_name: &str,
) -> Result<Value, LocyError> {
    let upper = func_name.to_uppercase();
    match upper.as_str() {
        "SUM" => {
            let mut total = 0.0_f64;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if let Some(f) = v.as_f64() {
                    total += f;
                }
            }
            if total == total.floor() && total.abs() < i64::MAX as f64 {
                Ok(Value::Int(total as i64))
            } else {
                Ok(Value::Float(total))
            }
        }
        "MSUM" => {
            let mut total = 0.0_f64;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if let Some(f) = v.as_f64() {
                    if f < 0.0 {
                        return Err(LocyError::MsumNegativeValue {
                            rule: rule_name.to_string(),
                            fold: fold_name.to_string(),
                            value: f,
                        });
                    }
                    total += f;
                }
            }
            if total == total.floor() && total.abs() < i64::MAX as f64 {
                Ok(Value::Int(total as i64))
            } else {
                Ok(Value::Float(total))
            }
        }
        "COUNT" | "MCOUNT" => {
            let count = group
                .iter()
                .filter(|row| {
                    eval_expr(arg_expr, row)
                        .map(|v| !v.is_null())
                        .unwrap_or(false)
                })
                .count();
            Ok(Value::Int(count as i64))
        }
        "MIN" | "MMIN" => {
            let mut min_val: Option<Value> = None;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if v.is_null() {
                    continue;
                }
                min_val = Some(match min_val {
                    None => v,
                    Some(cur) => {
                        if value_less_than(&v, &cur) {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            Ok(min_val.unwrap_or(Value::Null))
        }
        "MAX" | "MMAX" => {
            let mut max_val: Option<Value> = None;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if v.is_null() {
                    continue;
                }
                max_val = Some(match max_val {
                    None => v,
                    Some(cur) => {
                        if value_less_than(&cur, &v) {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            Ok(max_val.unwrap_or(Value::Null))
        }
        "AVG" => {
            let mut total = 0.0_f64;
            let mut count = 0;
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if let Some(f) = v.as_f64() {
                    total += f;
                    count += 1;
                }
            }
            if count == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(total / count as f64))
            }
        }
        "COLLECT" => {
            let mut vals = Vec::new();
            for row in group {
                let v = eval_expr(arg_expr, row)?;
                if !v.is_null() {
                    vals.push(v);
                }
            }
            Ok(Value::List(vals))
        }
        _ => Err(LocyError::EvaluationError {
            message: format!("unknown aggregate function: {func_name}"),
        }),
    }
}

pub(crate) fn literal_to_value(lit: &CypherLiteral) -> Value {
    match lit {
        CypherLiteral::Null => Value::Null,
        CypherLiteral::Bool(b) => Value::Bool(*b),
        CypherLiteral::Integer(i) => Value::Int(*i),
        CypherLiteral::Float(f) => Value::Float(*f),
        CypherLiteral::String(s) => Value::String(s.clone()),
        CypherLiteral::Bytes(b) => Value::Bytes(b.clone()),
    }
}

fn get_property(value: &Value, property: &str) -> Value {
    match value {
        Value::Node(n) => n.properties.get(property).cloned().unwrap_or(Value::Null),
        Value::Edge(e) => e.properties.get(property).cloned().unwrap_or(Value::Null),
        Value::Map(m) => m.get(property).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// Evaluate a unary operator on a value.
///
/// Shared by both `eval_locy_expr` and `eval_expr` to avoid duplicating
/// NOT/negation logic.
fn eval_unary_op(op: &UnaryOp, v: &Value) -> Result<Value, LocyError> {
    match op {
        UnaryOp::Not => match v {
            Value::Bool(b) => Ok(Value::Bool(!b)),
            Value::Null => Ok(Value::Null),
            _ => Err(LocyError::TypeError {
                message: format!("NOT requires boolean, got {v:?}"),
            }),
        },
        UnaryOp::Neg => match v {
            Value::Int(i) => Ok(Value::Int(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            Value::Null => Ok(Value::Null),
            _ => Err(LocyError::TypeError {
                message: format!("negation requires numeric, got {v:?}"),
            }),
        },
    }
}

fn eval_locy_binary_op(left: &Value, op: &LocyBinaryOp, right: &Value) -> Result<Value, LocyError> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    match op {
        LocyBinaryOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        LocyBinaryOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        LocyBinaryOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        LocyBinaryOp::Div => {
            let r = right.as_f64().unwrap_or(0.0);
            if r == 0.0 {
                return Err(LocyError::EvaluationError {
                    message: "division by zero".to_string(),
                });
            }
            numeric_op(left, right, |a, b| a / b, |a, b| a / b)
        }
        LocyBinaryOp::Mod => numeric_op(left, right, |a, b| a % b, |a, b| a % b),
        LocyBinaryOp::Pow => {
            let l = left.as_f64().ok_or_else(|| LocyError::TypeError {
                message: format!("pow requires numeric, got {left:?}"),
            })?;
            let r = right.as_f64().ok_or_else(|| LocyError::TypeError {
                message: format!("pow requires numeric, got {right:?}"),
            })?;
            Ok(Value::Float(l.powf(r)))
        }
        LocyBinaryOp::And => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a && b)),
            _ => Ok(Value::Null),
        },
        LocyBinaryOp::Or => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a || b)),
            _ => Ok(Value::Null),
        },
        LocyBinaryOp::Xor => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a ^ b)),
            _ => Ok(Value::Null),
        },
    }
}

fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Result<Value, LocyError> {
    if left.is_null() || right.is_null() {
        return match op {
            BinaryOp::Eq => Ok(Value::Bool(left.is_null() && right.is_null())),
            BinaryOp::NotEq => Ok(Value::Bool(!(left.is_null() && right.is_null()))),
            _ => Ok(Value::Null),
        };
    }
    match op {
        BinaryOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinaryOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinaryOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinaryOp::Div => numeric_op(left, right, |a, b| a / b, |a, b| a / b),
        BinaryOp::Mod => numeric_op(left, right, |a, b| a % b, |a, b| a % b),
        BinaryOp::Pow => {
            let l = left.as_f64().unwrap_or(0.0);
            let r = right.as_f64().unwrap_or(0.0);
            Ok(Value::Float(l.powf(r)))
        }
        BinaryOp::Eq => Ok(Value::Bool(values_equal(left, right))),
        BinaryOp::NotEq => Ok(Value::Bool(!values_equal(left, right))),
        BinaryOp::Lt => Ok(Value::Bool(value_less_than(left, right))),
        BinaryOp::LtEq => Ok(Value::Bool(
            value_less_than(left, right) || values_equal(left, right),
        )),
        BinaryOp::Gt => Ok(Value::Bool(value_less_than(right, left))),
        BinaryOp::GtEq => Ok(Value::Bool(
            value_less_than(right, left) || values_equal(left, right),
        )),
        BinaryOp::And => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a && b)),
            _ => Ok(Value::Null),
        },
        BinaryOp::Or => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a || b)),
            _ => Ok(Value::Null),
        },
        BinaryOp::Xor => match (left.as_bool(), right.as_bool()) {
            (Some(a), Some(b)) => Ok(Value::Bool(a ^ b)),
            _ => Ok(Value::Null),
        },
        BinaryOp::Contains => match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(Value::Bool(l.contains(r))),
            _ => Ok(Value::Null),
        },
        BinaryOp::StartsWith => match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(Value::Bool(l.starts_with(r))),
            _ => Ok(Value::Null),
        },
        BinaryOp::EndsWith => match (left.as_str(), right.as_str()) {
            (Some(l), Some(r)) => Ok(Value::Bool(l.ends_with(r))),
            _ => Ok(Value::Null),
        },
        _ => Err(LocyError::EvaluationError {
            message: format!("unsupported binary op in in-memory evaluation: {op:?}"),
        }),
    }
}

fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Result<Value, LocyError> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(int_op(*a, *b))),
        _ => {
            let a = left.as_f64().ok_or_else(|| LocyError::TypeError {
                message: format!("numeric op requires number, got {left:?}"),
            })?;
            let b = right.as_f64().ok_or_else(|| LocyError::TypeError {
                message: format!("numeric op requires number, got {right:?}"),
            })?;
            Ok(Value::Float(float_op(a, b)))
        }
    }
}

fn eval_function(name: &str, args: &[Value]) -> Result<Value, LocyError> {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "TOINTEGER" | "TOINT" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::Int(i) => Ok(Value::Int(*i)),
                Value::Float(f) => Ok(Value::Int(*f as i64)),
                Value::String(s) => {
                    s.parse::<i64>()
                        .map(Value::Int)
                        .map_err(|_| LocyError::TypeError {
                            message: format!("cannot convert '{s}' to integer"),
                        })
                }
                _ => Ok(Value::Null),
            }
        }
        "TOFLOAT" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::Float(f) => Ok(Value::Float(*f)),
                Value::Int(i) => Ok(Value::Float(*i as f64)),
                Value::String(s) => {
                    s.parse::<f64>()
                        .map(Value::Float)
                        .map_err(|_| LocyError::TypeError {
                            message: format!("cannot convert '{s}' to float"),
                        })
                }
                _ => Ok(Value::Null),
            }
        }
        "TOSTRING" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::String(s) => Ok(Value::String(s.clone())),
                Value::Int(i) => Ok(Value::String(i.to_string())),
                Value::Float(f) => Ok(Value::String(f.to_string())),
                Value::Bool(b) => Ok(Value::String(b.to_string())),
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::String(format!("{v:?}"))),
            }
        }
        "ABS" => {
            let v = args.first().unwrap_or(&Value::Null);
            match v {
                Value::Int(i) => Ok(Value::Int(i.abs())),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                _ => Ok(Value::Null),
            }
        }
        "COALESCE" => {
            for a in args {
                if !a.is_null() {
                    return Ok(a.clone());
                }
            }
            Ok(Value::Null)
        }
        "SIMILAR_TO" | "VECTOR_SIMILARITY" => {
            if args.len() < 2 {
                return Err(LocyError::EvaluationError {
                    message: format!("{name} requires at least 2 arguments"),
                });
            }
            // In Locy context, handle pure vector-vector case directly.
            // Storage-dependent cases (auto-embed, FTS) are not available
            // in the Locy in-memory evaluator.
            crate::query::similar_to::eval_similar_to_pure(&args[0], &args[1]).map_err(|e| {
                LocyError::EvaluationError {
                    message: e.to_string(),
                }
            })
        }
        _ => Err(LocyError::EvaluationError {
            message: format!("unknown function: {name}"),
        }),
    }
}

/// Compare two values for equality (Cypher semantics).
pub fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Float(y)) => (*x as f64) == *y,
        (Value::Float(x), Value::Int(y)) => *x == (*y as f64),
        _ => a == b,
    }
}

/// Compare two values for join equality in IS-ref matching.
///
/// For graph entities (`Value::Node`, `Value::Edge`), compares by identity
/// (VID/EID) rather than full structural equality. This is necessary because
/// the same node may have different property sets across different query
/// executions (e.g., schema mode adds `overflow_json: Null` in some paths
/// but not others). For non-graph values, falls back to `values_equal`.
pub fn values_equal_for_join(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Node(na), Value::Node(nb)) => na.vid == nb.vid,
        (Value::Edge(ea), Value::Edge(eb)) => ea.eid == eb.eid,
        _ => values_equal(a, b),
    }
}

/// Compare two values returning an Ordering.
pub fn value_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    if value_less_than(a, b) {
        std::cmp::Ordering::Less
    } else if value_less_than(b, a) {
        std::cmp::Ordering::Greater
    } else {
        std::cmp::Ordering::Equal
    }
}

/// Compare two values for ordering (less than).
pub fn value_less_than(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::Int(x), Value::Float(y)) => (*x as f64) < *y,
        (Value::Float(x), Value::Int(y)) => *x < (*y as f64),
        (Value::String(x), Value::String(y)) => x < y,
        _ => false,
    }
}

/// Compare two values with NULL handling (NULLS LAST, matching Cypher semantics).
pub fn value_compare(a: &Value, b: &Value, null_last: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let null_order = if null_last {
        Ordering::Greater
    } else {
        Ordering::Less
    };
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => null_order,
        (false, true) => null_order.reverse(),
        (false, false) => value_cmp(a, b),
    }
}

/// Convert a slice of Arrow RecordBatches into a vector of Locy rows (HashMap<String, Value>).
///
/// Handles DateTime and Time struct types via `uni_common` schema helpers so that
/// temporal values round-trip correctly through the Arrow → Value conversion.
///
/// Node/edge struct columns (`_vid`/`_labels`/`_all_props`) are normalized to
/// `Value::Node` / `Value::Edge` and dotted helper columns (e.g. `a._vid`) are
/// stripped, matching the behaviour of `Executor::record_batches_to_rows`.
pub fn record_batches_to_locy_rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut row = HashMap::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let data_type = if uni_common::core::schema::is_datetime_struct(field.data_type()) {
                    Some(&uni_common::DataType::DateTime)
                } else if uni_common::core::schema::is_time_struct(field.data_type()) {
                    Some(&uni_common::DataType::Time)
                } else {
                    None
                };
                let value = uni_store::storage::arrow_convert::arrow_to_value(
                    column.as_ref(),
                    row_idx,
                    data_type,
                );
                row.insert(field.name().clone(), value);
            }
            normalize_graph_row(&mut row);
            rows.push(row);
        }
    }
    rows
}

/// Post-process a raw Arrow-converted row so that graph entities are represented
/// as `Value::Node` / `Value::Edge` and dotted helper columns are removed.
///
/// RecordBatches from graph scans emit both a bare struct column (e.g. `a`) and
/// exploded helper columns (`a._vid`, `a._labels`, `a._all_props`). The bare
/// column is `Value::Map({_vid, _labels, _all_props})` after `arrow_to_value`.
/// This function detects these maps and converts them to proper `Value::Node` or
/// `Value::Edge`, then strips the helpers.
fn normalize_graph_row(row: &mut Row) {
    // Detect bare graph-entity variables: keys without '.' that are Map values
    // containing the internal `_vid` or `_eid` field.
    let entity_vars: Vec<String> = row
        .keys()
        .filter(|k| {
            !k.contains('.')
                && match row.get(*k) {
                    Some(Value::Map(m)) => m.contains_key("_vid") || m.contains_key("_eid"),
                    _ => false,
                }
        })
        .cloned()
        .collect();

    for var in &entity_vars {
        // Merge any dotted helper columns into the bare map (they should already
        // be present from the struct, but merge to be safe).
        let prefix = format!("{}.", var);
        let helper_keys: Vec<String> = row
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        for key in &helper_keys {
            let prop_name = &key[prefix.len()..];
            if let Some(val) = row.get(key).cloned()
                && let Some(Value::Map(m)) = row.get_mut(var)
            {
                m.entry(prop_name.to_string()).or_insert(val);
            }
        }
        // Remove dotted helpers
        for key in helper_keys {
            row.remove(&key);
        }

        // Convert map → Value::Node or Value::Edge
        if let Some(Value::Map(map)) = row.remove(var) {
            row.insert(var.clone(), map_to_graph_entity(map));
        }
    }
}

/// Convert a map with internal graph fields to `Value::Node` or `Value::Edge`.
fn map_to_graph_entity(map: HashMap<String, Value>) -> Value {
    use uni_common::core::id::{Eid, Vid};
    use uni_common::value::{Edge, Node};

    // Edge: has _eid
    if let Some(eid_val) = map.get("_eid") {
        let eid = match eid_val {
            Value::Int(i) => Eid::new(*i as u64),
            _ => return Value::Map(map),
        };
        let edge_type = match map.get("_type") {
            Some(Value::String(s)) => s.clone(),
            _ => String::new(),
        };
        let src = match map.get("_src_vid") {
            Some(Value::Int(i)) => Vid::new(*i as u64),
            _ => Vid::new(0),
        };
        let dst = match map.get("_dst_vid") {
            Some(Value::Int(i)) => Vid::new(*i as u64),
            _ => Vid::new(0),
        };
        let properties = extract_properties_from_map(&map);
        return Value::Edge(Edge {
            eid,
            edge_type,
            src,
            dst,
            properties,
        });
    }

    // Node: has _vid
    if let Some(vid_val) = map.get("_vid") {
        let vid = match vid_val {
            Value::Int(i) => Vid::new(*i as u64),
            _ => return Value::Map(map),
        };
        let labels = match map.get("_labels") {
            Some(Value::List(list)) => list
                .iter()
                .filter_map(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        let properties = extract_properties_from_map(&map);
        return Value::Node(Node {
            vid,
            labels,
            properties,
        });
    }

    Value::Map(map)
}

/// Extract user-visible properties from a raw graph-entity map.
///
/// Properties are stored in `_all_props` (deserialized by `arrow_to_value` from
/// the LargeBinary CypherValue codec). Any non-internal keys at the top level
/// are also included as schema-defined column properties.
fn extract_properties_from_map(map: &HashMap<String, Value>) -> HashMap<String, Value> {
    let mut properties = HashMap::new();

    // Primary source: _all_props contains all properties from storage
    if let Some(Value::Map(all_props)) = map.get("_all_props") {
        for (k, v) in all_props {
            properties.insert(k.clone(), v.clone());
        }
    }

    // Secondary: inline non-internal keys (schema-defined property columns)
    for (k, v) in map {
        if !k.starts_with('_') && k != "properties" {
            properties.entry(k.clone()).or_insert_with(|| v.clone());
        }
    }

    properties
}
