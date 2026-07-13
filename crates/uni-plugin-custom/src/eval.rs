// Rust guideline compliant
//! A small Cypher-expression evaluator for declared scalar functions.
//!
//! M9 acceptance requires `RETURN mycorp.fullName('Ada', 'Lovelace')`
//! to return `'Ada Lovelace'` when the declared body is
//! `'$first + " " + $last'`. To execute that body, we need to walk a
//! parsed Cypher [`Expr`] with the declared arguments bound as
//! parameters.
//!
//! The full Cypher expression surface is enormous
//! (see [`uni_cypher::ast::Expr`]). Reusing
//! `uni_query::query::df_expr::cypher_expr_to_df` would pull DataFusion
//! into this crate. Instead, we ship a tiny interpreter sufficient
//! for the M9 happy path:
//!
//! * Literals (null, bool, int, float, string).
//! * Parameter substitution.
//! * Binary arithmetic / comparison / boolean / string-concat.
//! * Unary negation and `NOT`.
//! * Lists and maps.
//! * A handful of well-known scalar functions (`toString`, `length`,
//!   `upper`, `lower`, `trim`).
//!
//! Expressions that step outside this surface (property access on a
//! `Node`, sub-queries, pattern comprehensions, …) return a clean
//! [`EvalError::Unsupported`] so the user sees a diagnosable message
//! rather than a panic. As M9 matures, callers can either reach for
//! `cypher_expr_to_df` (the DataFusion path) or extend this
//! interpreter in place — both are open lanes.

use std::collections::HashMap;

use thiserror::Error;
use uni_common::Value;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, UnaryOp};

use crate::decode::stringify;

/// Errors produced by the expression evaluator.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EvalError {
    /// The expression references an unbound parameter.
    #[error("unbound parameter `${0}` in declared expression")]
    UnboundParameter(String),

    /// The expression uses a Cypher feature this interpreter does not
    /// (yet) cover. The full Cypher surface lives in
    /// [`uni_query::query::df_expr::cypher_expr_to_df`] — declared
    /// functions can move to that path once they need it.
    #[error("declared expression uses unsupported Cypher feature: {0}")]
    Unsupported(String),

    /// Type mismatch when applying a binary operator.
    #[error("type mismatch: cannot apply `{op}` to {lhs} and {rhs}")]
    TypeMismatch {
        /// The operator that triggered the mismatch.
        op: String,
        /// Left-hand value type name.
        lhs: &'static str,
        /// Right-hand value type name.
        rhs: &'static str,
    },

    /// Division-by-zero or similar arithmetic error.
    #[error("arithmetic error in declared expression: {0}")]
    Arithmetic(String),
}

/// Evaluate a parsed Cypher expression against a parameter binding.
///
/// # Errors
///
/// Returns [`EvalError`] when the expression references an unbound
/// parameter, uses an unsupported Cypher construct, mixes incompatible
/// types in a binary op, or hits an arithmetic error.
pub fn eval_expr(expr: &Expr, params: &HashMap<String, Value>) -> Result<Value, EvalError> {
    match expr {
        Expr::Literal(lit) => Ok(lit_to_value(lit)),
        Expr::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| EvalError::UnboundParameter(name.clone())),
        Expr::BinaryOp { left, op, right } => {
            let l = eval_expr(left, params)?;
            let r = eval_expr(right, params)?;
            apply_binary(*op, l, r)
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, params)?;
            apply_unary(*op, v)
        }
        Expr::List(items) => items
            .iter()
            .map(|e| eval_expr(e, params))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::List),
        Expr::Map(entries) => entries
            .iter()
            .map(|(k, v)| eval_expr(v, params).map(|v| (k.clone(), v)))
            .collect::<Result<HashMap<_, _>, _>>()
            .map(Value::Map),
        Expr::IsNull(inner) => {
            let v = eval_expr(inner, params)?;
            Ok(Value::Bool(matches!(v, Value::Null)))
        }
        Expr::IsNotNull(inner) => {
            let v = eval_expr(inner, params)?;
            Ok(Value::Bool(!matches!(v, Value::Null)))
        }
        Expr::FunctionCall { name, args, .. } => {
            let args: Vec<Value> = args
                .iter()
                .map(|e| eval_expr(e, params))
                .collect::<Result<_, _>>()?;
            apply_function(name, &args)
        }
        Expr::Case {
            expr: scrutinee,
            when_then,
            else_expr,
        } => eval_case(
            scrutinee.as_deref(),
            when_then,
            else_expr.as_deref(),
            params,
        ),
        other => Err(EvalError::Unsupported(format!("{other:?}"))),
    }
}

fn lit_to_value(lit: &CypherLiteral) -> Value {
    match lit {
        CypherLiteral::Null => Value::Null,
        CypherLiteral::Bool(b) => Value::Bool(*b),
        CypherLiteral::Integer(i) => Value::Int(*i),
        CypherLiteral::Float(f) => Value::Float(*f),
        CypherLiteral::String(s) => Value::String(s.clone()),
        CypherLiteral::Bytes(b) => Value::Bytes(b.clone()),
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int(_) => "Int",
        Value::Float(_) => "Float",
        Value::String(_) => "String",
        Value::Bytes(_) => "Bytes",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
        Value::Node(_) => "Node",
        Value::Edge(_) => "Edge",
        Value::Path(_) => "Path",
        Value::Vector(_) => "Vector",
        Value::Temporal(_) => "Temporal",
        _ => "Other",
    }
}

fn apply_binary(op: BinaryOp, l: Value, r: Value) -> Result<Value, EvalError> {
    use BinaryOp::*;
    if matches!(l, Value::Null) || matches!(r, Value::Null) {
        // Cypher three-valued logic: AND/OR still yield a definite result when
        // one operand is NULL if the other operand dominates — `false AND null`
        // = false, `true OR null` = true. Only when NULL cannot be dominated does
        // the result stay NULL. Every other operator propagates NULL.
        return match op {
            And => match (&l, &r) {
                (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
                _ => Ok(Value::Null),
            },
            Or => match (&l, &r) {
                (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                _ => Ok(Value::Null),
            },
            _ => Ok(Value::Null),
        };
    }
    match op {
        Add => add_values(l, r),
        Sub | Mul | Div | Mod | Pow => arith(op, l, r),
        Eq | NotEq | Lt | LtEq | Gt | GtEq => compare(op, l, r),
        And | Or | Xor => boolean_op(op, l, r),
        Contains | StartsWith | EndsWith => string_match(op, l, r),
        Regex | ApproxEq => Err(EvalError::Unsupported(format!("{op}"))),
    }
}

fn add_values(l: Value, r: Value) -> Result<Value, EvalError> {
    match (l, r) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a.saturating_add(b))),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + b as f64)),
        (Value::String(a), Value::String(b)) => Ok(Value::String(a + &b)),
        (Value::String(a), b) => Ok(Value::String(a + &stringify(&b))),
        (a, Value::String(b)) => Ok(Value::String(stringify(&a) + &b)),
        (Value::List(mut a), Value::List(b)) => {
            a.extend(b);
            Ok(Value::List(a))
        }
        (l, r) => Err(EvalError::TypeMismatch {
            op: "+".to_owned(),
            lhs: type_name(&l),
            rhs: type_name(&r),
        }),
    }
}

fn arith(op: BinaryOp, l: Value, r: Value) -> Result<Value, EvalError> {
    // Exact integer path for Int/Int (except Pow, which Cypher yields as Float):
    // routing through f64 loses precision above 2^53 and makes int/int division
    // return a Float instead of Cypher's truncated Int. Overflow is an error, not
    // a silent wrap. (finding uni-plugin-custom[4])
    if let (Value::Int(a), Value::Int(b)) = (&l, &r)
        && op != BinaryOp::Pow
    {
        let (a, b) = (*a, *b);
        let res = match op {
            BinaryOp::Sub => a.checked_sub(b),
            BinaryOp::Mul => a.checked_mul(b),
            BinaryOp::Div => {
                if b == 0 {
                    return Err(EvalError::Arithmetic("divide by zero".to_owned()));
                }
                a.checked_div(b) // truncates toward zero, per Cypher int division
            }
            BinaryOp::Mod => {
                if b == 0 {
                    return Err(EvalError::Arithmetic("mod by zero".to_owned()));
                }
                a.checked_rem(b)
            }
            _ => unreachable!("arith dispatched non-arith op"),
        };
        return res
            .map(Value::Int)
            .ok_or_else(|| EvalError::Arithmetic(format!("integer overflow in {op}")));
    }

    let (lf, rf, both_int) = match (&l, &r) {
        (Value::Int(a), Value::Int(b)) => (*a as f64, *b as f64, true),
        (Value::Float(a), Value::Float(b)) => (*a, *b, false),
        (Value::Int(a), Value::Float(b)) => (*a as f64, *b, false),
        (Value::Float(a), Value::Int(b)) => (*a, *b as f64, false),
        _ => {
            return Err(EvalError::TypeMismatch {
                op: format!("{op}"),
                lhs: type_name(&l),
                rhs: type_name(&r),
            });
        }
    };
    let out = match op {
        BinaryOp::Sub => lf - rf,
        BinaryOp::Mul => lf * rf,
        BinaryOp::Div => {
            if rf == 0.0 {
                return Err(EvalError::Arithmetic("divide by zero".to_owned()));
            }
            lf / rf
        }
        BinaryOp::Mod => {
            if rf == 0.0 {
                return Err(EvalError::Arithmetic("mod by zero".to_owned()));
            }
            lf % rf
        }
        BinaryOp::Pow => lf.powf(rf),
        _ => unreachable!("arith dispatched non-arith op"),
    };
    if both_int && out.fract() == 0.0 && out.is_finite() {
        Ok(Value::Int(out as i64))
    } else {
        Ok(Value::Float(out))
    }
}

fn compare(op: BinaryOp, l: Value, r: Value) -> Result<Value, EvalError> {
    let ord = match (&l, &r) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| EvalError::Arithmetic("NaN comparison".to_owned()))?,
        (Value::Int(a), Value::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| EvalError::Arithmetic("NaN comparison".to_owned()))?,
        (Value::Float(a), Value::Int(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| EvalError::Arithmetic("NaN comparison".to_owned()))?,
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (l, r) => {
            return Err(EvalError::TypeMismatch {
                op: format!("{op}"),
                lhs: type_name(l),
                rhs: type_name(r),
            });
        }
    };
    use std::cmp::Ordering::*;
    let v = match op {
        BinaryOp::Eq => ord == Equal,
        BinaryOp::NotEq => ord != Equal,
        BinaryOp::Lt => ord == Less,
        BinaryOp::LtEq => ord != Greater,
        BinaryOp::Gt => ord == Greater,
        BinaryOp::GtEq => ord != Less,
        _ => unreachable!(),
    };
    Ok(Value::Bool(v))
}

fn boolean_op(op: BinaryOp, l: Value, r: Value) -> Result<Value, EvalError> {
    match (l, r) {
        (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(match op {
            BinaryOp::And => a && b,
            BinaryOp::Or => a || b,
            BinaryOp::Xor => a ^ b,
            _ => unreachable!(),
        })),
        (l, r) => Err(EvalError::TypeMismatch {
            op: format!("{op}"),
            lhs: type_name(&l),
            rhs: type_name(&r),
        }),
    }
}

fn string_match(op: BinaryOp, l: Value, r: Value) -> Result<Value, EvalError> {
    match (l, r) {
        (Value::String(a), Value::String(b)) => Ok(Value::Bool(match op {
            BinaryOp::Contains => a.contains(&b),
            BinaryOp::StartsWith => a.starts_with(&b),
            BinaryOp::EndsWith => a.ends_with(&b),
            _ => unreachable!(),
        })),
        (l, r) => Err(EvalError::TypeMismatch {
            op: format!("{op}"),
            lhs: type_name(&l),
            rhs: type_name(&r),
        }),
    }
}

fn apply_unary(op: UnaryOp, v: Value) -> Result<Value, EvalError> {
    match (op, v) {
        (UnaryOp::Neg, Value::Int(i)) => Ok(Value::Int(-i)),
        (UnaryOp::Neg, Value::Float(f)) => Ok(Value::Float(-f)),
        (UnaryOp::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
        (UnaryOp::Not, Value::Null) => Ok(Value::Null),
        (op, v) => Err(EvalError::TypeMismatch {
            op: format!("{op}"),
            lhs: type_name(&v),
            rhs: "<unary>",
        }),
    }
}

fn apply_function(name: &str, args: &[Value]) -> Result<Value, EvalError> {
    match (name, args) {
        ("toString", [v]) => Ok(Value::String(stringify(v))),
        ("upper" | "toUpper", [Value::String(s)]) => Ok(Value::String(s.to_uppercase())),
        ("lower" | "toLower", [Value::String(s)]) => Ok(Value::String(s.to_lowercase())),
        ("trim", [Value::String(s)]) => Ok(Value::String(s.trim().to_owned())),
        ("length" | "size", [Value::String(s)]) => Ok(Value::Int(s.chars().count() as i64)),
        ("length" | "size", [Value::List(l)]) => Ok(Value::Int(l.len() as i64)),
        ("abs", [Value::Int(i)]) => Ok(Value::Int(i.unsigned_abs() as i64)),
        ("abs", [Value::Float(f)]) => Ok(Value::Float(f.abs())),
        (name, _) => Err(EvalError::Unsupported(format!("function `{name}`"))),
    }
}

fn eval_case(
    scrutinee: Option<&Expr>,
    when_then: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
    params: &HashMap<String, Value>,
) -> Result<Value, EvalError> {
    let scrutinee_val = scrutinee.map(|e| eval_expr(e, params)).transpose()?;
    for (w, t) in when_then {
        let w_val = eval_expr(w, params)?;
        let matched = match &scrutinee_val {
            Some(s) => values_equal(s, &w_val),
            None => matches!(w_val, Value::Bool(true)),
        };
        if matched {
            return eval_expr(t, params);
        }
    }
    if let Some(e) = else_expr {
        eval_expr(e, params)
    } else {
        Ok(Value::Null)
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Int(a), Value::Float(b)) | (Value::Float(b), Value::Int(a)) => *a as f64 == *b,
        (Value::String(a), Value::String(b)) => a == b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_cypher::parse_expression;

    fn ev(src: &str, params: &[(&str, Value)]) -> Value {
        let expr = parse_expression(src).expect("parse");
        let p: HashMap<String, Value> = params
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect();
        eval_expr(&expr, &p).expect("eval")
    }

    #[test]
    fn string_concat_with_params() {
        let v = ev(
            "$first + ' ' + $last",
            &[
                ("first", Value::String("Ada".to_owned())),
                ("last", Value::String("Lovelace".to_owned())),
            ],
        );
        assert_eq!(v, Value::String("Ada Lovelace".to_owned()));
    }

    #[test]
    fn integer_arithmetic() {
        let v = ev("$a * $b + 1", &[("a", Value::Int(3)), ("b", Value::Int(4))]);
        assert_eq!(v, Value::Int(13));
    }

    #[test]
    fn boolean_short_circuit_via_eval() {
        let v = ev("$x > 0 AND $x < 10", &[("x", Value::Int(5))]);
        assert_eq!(v, Value::Bool(true));
    }

    #[test]
    fn case_when_branch() {
        let v = ev(
            "CASE WHEN $x > 0 THEN 'pos' WHEN $x < 0 THEN 'neg' ELSE 'zero' END",
            &[("x", Value::Int(-3))],
        );
        assert_eq!(v, Value::String("neg".to_owned()));
    }

    #[test]
    fn unbound_parameter_errors() {
        let expr = parse_expression("$missing + 1").unwrap();
        let err = eval_expr(&expr, &HashMap::new()).unwrap_err();
        assert!(matches!(err, EvalError::UnboundParameter(ref n) if n == "missing"));
    }

    #[test]
    fn null_propagates_through_arithmetic() {
        let v = ev("$x + 1", &[("x", Value::Null)]);
        assert_eq!(v, Value::Null);
    }

    #[test]
    fn toupper_function() {
        let v = ev("toUpper($s)", &[("s", Value::String("hello".to_owned()))]);
        assert_eq!(v, Value::String("HELLO".to_owned()));
    }
}
