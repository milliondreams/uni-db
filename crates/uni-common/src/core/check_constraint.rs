// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Evaluation of `CHECK` constraint expressions.
//!
//! This is the single evaluator for both write paths. It previously existed as
//! two token-for-token copies — one in `uni-bulk`'s `BulkWriter`, one in
//! `uni-store`'s `Writer` — which had drifted in four places, two of them
//! affecting accept/reject decisions:
//!
//! 1. **Numeric equality.** The bulk copy routed `=` / `!=` operands through
//!    `compare_values` when both sides are numeric, because [`Value`]'s
//!    `PartialEq` is type-strict and has no Int/Float arm. The writer copy used
//!    bare `==`. So `CHECK (score = 5)` against a stored `Float(5.0)` *passed*
//!    through the bulk loader and *failed* through `tx.execute`.
//! 2. **Target-literal fallback.** The writer copy carried an extra
//!    `Number(...)` unwrap for internal-format wrappers. That arm was **dead**:
//!    `val_str` comes from `trim_end_matches(')')`, which strips every trailing
//!    paren, so its `ends_with(')')` guard could never hold. It is dropped here
//!    rather than resurrected — making it reachable would add a capability
//!    neither path has today, which a deduplication commit has no business
//!    doing. Established by testing, not by reading.
//! 3. / 4. The writer copy warned on an unparseable expression and on an
//!    unknown operator; the bulk copy was silent.
//!
//! The divergence was a fix applied to one copy and never propagated — the bulk
//! behaviour is pinned by a landed regression test, and the transactional path
//! was simply never updated. This module takes the union of what actually ran:
//! bulk's numeric coercion plus the writer's warnings.
//!
//! # Shape constraints
//!
//! [`evaluate`] is a free, **synchronous** function returning
//! `anyhow::Result<bool>`, and must stay that way. One of its three call sites
//! is a match *guard* (`if !evaluate(expr, props)? =>`): guards cannot `.await`
//! and cannot take `&mut self`, and the `?` there propagates out of the
//! enclosing function, so an `Err` aborts the write rather than reporting a
//! constraint violation.
//!
//! # Scope
//!
//! The grammar handled is `prop op value` — three whitespace-separated tokens,
//! with optional surrounding parentheses and an optional `variable.` prefix.
//! Anything more complex is *allowed* with a warning rather than rejected, so
//! that an expression this evaluator cannot parse never silently blocks a
//! legitimate write. A missing property also passes; absence is `NOT NULL`'s
//! concern, not `CHECK`'s.

use std::cmp::Ordering;

use anyhow::{Result, anyhow};

use crate::{Properties, Value};

/// Evaluate a `CHECK` constraint expression against a property bag.
///
/// Returns `Ok(true)` when the constraint holds, is inapplicable (missing
/// property), or is outside the supported grammar.
///
/// # Errors
///
/// Returns an error only when the two operands cannot be ordered — e.g.
/// `CHECK (name > 5)` against a string. Callers treat that as a failed write,
/// not as a constraint violation.
pub fn evaluate(expression: &str, properties: &Properties) -> Result<bool> {
    let parts: Vec<&str> = expression.split_whitespace().collect();
    if parts.len() != 3 {
        tracing::warn!(
            "Complex CHECK constraint expression '{}' not fully supported yet; allowing write.",
            expression
        );
        return Ok(true);
    }

    let prop_part = parts[0].trim_start_matches('(');
    // Handle "variable.property" — take the part after the dot.
    let prop_name = match prop_part.find('.') {
        Some(idx) => &prop_part[idx + 1..],
        None => prop_part,
    };

    let op = parts[1];
    let val_str = parts[2].trim_end_matches(')');

    let prop_val = match properties.get(prop_name) {
        Some(v) => v,
        // A missing property passes; that is `NOT NULL`'s job.
        None => return Ok(true),
    };

    let target_val = parse_target(val_str);

    match op {
        // Route numeric equality through `compare_values` so Int/Float coerce,
        // matching the ordering operators below. `Value`'s `PartialEq` is
        // type-strict and has no Int/Float arm, so `Float(5.0) == Int(5)` would
        // otherwise be false. Non-numeric operands keep strict structural
        // equality.
        "=" | "==" => Ok(if prop_val.is_number() && target_val.is_number() {
            compare_values(prop_val, &target_val)?.is_eq()
        } else {
            prop_val == &target_val
        }),
        "!=" | "<>" => Ok(if prop_val.is_number() && target_val.is_number() {
            !compare_values(prop_val, &target_val)?.is_eq()
        } else {
            prop_val != &target_val
        }),
        ">" => Ok(compare_values(prop_val, &target_val)?.is_gt()),
        "<" => Ok(compare_values(prop_val, &target_val)?.is_lt()),
        ">=" => Ok(compare_values(prop_val, &target_val)?.is_ge()),
        "<=" => Ok(compare_values(prop_val, &target_val)?.is_le()),
        _ => {
            tracing::warn!("Unsupported operator '{}' in CHECK constraint", op);
            Ok(true)
        }
    }
}

/// Parse the right-hand token into a [`Value`].
///
/// Note the caller has already applied `trim_end_matches(')')`, so any wrapper
/// syntax of the form `Name(...)` arrives with its closing paren gone. See the
/// module docs on the dropped `Number(...)` arm.
fn parse_target(val_str: &str) -> Value {
    if (val_str.starts_with('\'') && val_str.ends_with('\''))
        || (val_str.starts_with('"') && val_str.ends_with('"'))
    {
        return Value::String(val_str[1..val_str.len() - 1].to_string());
    }
    if let Ok(n) = val_str.parse::<i64>() {
        return Value::Int(n);
    }
    if let Ok(n) = val_str.parse::<f64>() {
        return Value::Float(n);
    }
    if let Ok(b) = val_str.parse::<bool>() {
        return Value::Bool(b);
    }
    Value::String(val_str.to_string())
}

/// Compare two values for ordering.
///
/// Incomparable floats (NaN) compare as [`Ordering::Equal`], matching the
/// branch-based implementations this replaces.
///
/// # Errors
///
/// Returns an error when the two values are not of comparable kinds.
fn compare_values(a: &Value, b: &Value) -> Result<Ordering> {
    match (a, b) {
        (Value::Int(n1), Value::Int(n2)) => Ok(n1.cmp(n2)),
        (Value::Float(f1), Value::Float(f2)) => Ok(f1.partial_cmp(f2).unwrap_or(Ordering::Equal)),
        // Exact i64-vs-f64 order (no lossy `as f64` cast above 2^53); preserve
        // the NaN-as-Equal behaviour for the degenerate case.
        (Value::Int(n), Value::Float(f)) => Ok(if f.is_nan() {
            Ordering::Equal
        } else {
            crate::cmp_i64_f64(*n, *f)
        }),
        (Value::Float(f), Value::Int(n)) => Ok(if f.is_nan() {
            Ordering::Equal
        } else {
            crate::cmp_i64_f64(*n, *f).reverse()
        }),
        (Value::String(s1), Value::String(s2)) => Ok(s1.cmp(s2)),
        _ => Err(anyhow!(
            "Cannot compare incompatible types: {:?} vs {:?}",
            a,
            b
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, Value)]) -> Properties {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// The divergence that mattered: a float-valued property against an
    /// integer literal. The bulk path coerced, the transactional path did not.
    #[test]
    fn numeric_equality_coerces_across_int_and_float() {
        let p = props(&[("score", Value::Float(5.0))]);
        assert!(evaluate("(n.score = 5)", &p).unwrap());
        assert!(!evaluate("(n.score != 5)", &p).unwrap());

        let p = props(&[("score", Value::Int(5))]);
        assert!(evaluate("(n.score = 5.0)", &p).unwrap());
    }

    /// Non-numeric operands keep strict structural equality.
    #[test]
    fn non_numeric_equality_stays_strict() {
        let p = props(&[("name", Value::String("a".into()))]);
        assert!(evaluate("(n.name = 'a')", &p).unwrap());
        assert!(!evaluate("(n.name = 'b')", &p).unwrap());
    }

    /// Exactness above 2^53, where a lossy `as f64` cast would compare equal.
    #[test]
    fn large_integers_compare_exactly() {
        let p = props(&[("v", Value::Int(9_007_199_254_740_993))]);
        assert!(evaluate("(n.v > 9007199254740992.0)", &p).unwrap());
    }

    /// The writer copy's `Number(...)` arm was unreachable — `val_str` has had
    /// every trailing paren stripped before it is inspected — so such a target
    /// degrades to a string and an ordering comparison against it errors. This
    /// pins that pre-existing behaviour rather than the dead branch's intent.
    #[test]
    fn number_wrapper_target_is_not_special_cased() {
        let p = props(&[("v", Value::Int(7))]);
        assert!(evaluate("(n.v < Number(8.5))", &p).is_err());
    }

    #[test]
    fn ordering_operators() {
        let p = props(&[("v", Value::Int(5))]);
        assert!(evaluate("(n.v > 4)", &p).unwrap());
        assert!(evaluate("(n.v >= 5)", &p).unwrap());
        assert!(evaluate("(n.v < 6)", &p).unwrap());
        assert!(evaluate("(n.v <= 5)", &p).unwrap());
        assert!(!evaluate("(n.v > 5)", &p).unwrap());
    }

    /// Unsupported shapes allow the write rather than blocking it.
    #[test]
    fn unsupported_shapes_allow_the_write() {
        let p = props(&[("v", Value::Int(5))]);
        // Missing property.
        assert!(evaluate("(n.other = 1)", &p).unwrap());
        // Not three tokens.
        assert!(evaluate("(n.v > 1 AND n.v < 9)", &p).unwrap());
        // Unknown operator.
        assert!(evaluate("(n.v ~~ 1)", &p).unwrap());
    }

    /// An un-orderable pair is an error, not a violation — the caller aborts
    /// the write rather than reporting a failed constraint.
    #[test]
    fn incomparable_operands_error() {
        let p = props(&[("name", Value::String("a".into()))]);
        assert!(evaluate("(n.name > 5)", &p).is_err());
    }

    /// A bare property name, with no `variable.` prefix.
    #[test]
    fn bare_property_name_is_accepted() {
        let p = props(&[("v", Value::Int(5))]);
        assert!(evaluate("v = 5", &p).unwrap());
    }
}
