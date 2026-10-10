// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Which column of a referenced rule each variable of an IS reference binds.
//!
//! This is the one definition: the compiler's arity checks, the planner's
//! positive and negated references, provenance, the in-memory evaluator and
//! EXPLAIN all bind through it. Before it they used four different rules (KEY
//! columns only, YIELD position, all columns by position), and a program meant
//! different things on different paths.

// Rust guideline compliant

use crate::types::YieldColumn;

/// The columns of a rule in the order an IS reference binds them.
///
/// The KEY columns in YIELD order, then the other columns in YIELD order. An
/// IS reference's subjects take columns from the front, and a `TO` target
/// takes the next one. For a rule whose KEY columns come first this is YIELD
/// order, so `(x, y, cost) IS weighted_path` binds positionally; `x IS r TO
/// y` binds `y` to the second KEY, or to the first value column when `r` has
/// one KEY.
///
/// # Examples
///
/// ```
/// use uni_locy::is_binding::binding_sequence;
/// use uni_locy::types::YieldColumn;
///
/// let col = |name: &str, is_key| YieldColumn { name: name.into(), is_key, is_prob: false };
/// let schema = [col("w", false), col("x", true), col("y", true)];
/// let names: Vec<&str> = binding_sequence(&schema).iter().map(|c| c.name.as_str()).collect();
/// assert_eq!(names, ["x", "y", "w"]);
/// ```
pub fn binding_sequence(yield_schema: &[YieldColumn]) -> Vec<&YieldColumn> {
    yield_schema
        .iter()
        .filter(|c| c.is_key)
        .chain(yield_schema.iter().filter(|c| !c.is_key))
        .collect()
}

/// Reorders a rule's yield schema into its [`binding_sequence`].
///
/// The compiler stores every rule's schema in this order, so for every
/// consumer a column's position is its binding position, and the KEY columns
/// are a prefix. Column order is not observable: facts are read by column
/// name. Much of the evaluator was written assuming KEY-first (the FOLD
/// operator emits KEYs before aggregates, derived scans and anti-joins took
/// fields by position), and a rule yielding a value before a KEY broke each
/// of those assumptions differently.
pub fn into_binding_order(yield_schema: Vec<YieldColumn>) -> Vec<YieldColumn> {
    let (mut keys, values): (Vec<_>, Vec<_>) = yield_schema.into_iter().partition(|c| c.is_key);
    keys.extend(values);
    keys
}

/// The names of [`binding_sequence`], for callers that work with column names.
pub fn binding_column_names(yield_schema: &[YieldColumn]) -> Vec<String> {
    binding_sequence(yield_schema)
        .into_iter()
        .map(|c| c.name.clone())
        .collect()
}

/// The column bound by the variable at `position` of an IS reference.
///
/// Subject `i` is at position `i`; the `TO` target is at `subjects.len()`.
/// Returns `None` when the rule has no column at that position.
pub fn binding_column(yield_schema: &[YieldColumn], position: usize) -> Option<&YieldColumn> {
    binding_sequence(yield_schema).into_iter().nth(position)
}

/// The columns bound by `subjects` subjects followed by an optional target.
///
/// Returns `None` when there are more bindings than the rule has columns.
pub fn bound_columns(
    yield_schema: &[YieldColumn],
    subjects: usize,
    has_target: bool,
) -> Option<Vec<&YieldColumn>> {
    let count = subjects + usize::from(has_target);
    let sequence = binding_sequence(yield_schema);
    (count <= sequence.len()).then(|| sequence.into_iter().take(count).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, is_key: bool) -> YieldColumn {
        YieldColumn {
            name: name.into(),
            is_key,
            is_prob: false,
        }
    }

    fn names(cols: &[&YieldColumn]) -> Vec<String> {
        cols.iter().map(|c| c.name.clone()).collect()
    }

    #[test]
    fn keys_first_is_yield_order() {
        let schema = [col("o", true), col("e", true), col("pct", false)];
        assert_eq!(binding_column_names(&schema), ["o", "e", "pct"]);
    }

    #[test]
    fn keys_bind_before_value_columns_wherever_they_are_yielded() {
        let schema = [col("x", true), col("w", false), col("y", true)];
        assert_eq!(binding_column_names(&schema), ["x", "y", "w"]);
        let schema = [col("b", false), col("a", true)];
        assert_eq!(binding_column_names(&schema), ["a", "b"]);
    }

    #[test]
    fn target_takes_the_next_column() {
        let schema = [col("b", true), col("agg", false)];
        assert_eq!(
            binding_column(&schema, 1).map(|c| c.name.as_str()),
            Some("agg")
        );
        let schema = [col("a", true), col("b", true), col("s", false)];
        assert_eq!(
            binding_column(&schema, 1).map(|c| c.name.as_str()),
            Some("b")
        );
        assert_eq!(binding_column(&schema, 3), None);
    }

    #[test]
    fn into_binding_order_puts_keys_first_and_keeps_each_group_in_order() {
        let schema = vec![
            col("w", false),
            col("x", true),
            col("v", false),
            col("y", true),
        ];
        let names: Vec<String> = into_binding_order(schema)
            .into_iter()
            .map(|c| c.name)
            .collect();
        assert_eq!(names, ["x", "y", "w", "v"]);
    }

    #[test]
    fn bound_columns_refuses_more_bindings_than_columns() {
        let schema = [col("o", true), col("e", true), col("pct", false)];
        let bound = bound_columns(&schema, 2, true).expect("three columns");
        assert_eq!(names(&bound), ["o", "e", "pct"]);
        assert!(bound_columns(&schema, 3, true).is_none());
    }
}
