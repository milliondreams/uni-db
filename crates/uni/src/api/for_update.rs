// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `FOR UPDATE` lock-key extraction (SSI pessimistic escape hatch).
//!
//! Walks a parsed query for `MATCH … FOR UPDATE` clauses and computes a
//! canonical lock key for each lockable match. Only **single keyed nodes**
//! (`MATCH (n:Label {k: v}) FOR UPDATE`) are lockable — the RMW use case the
//! escape hatch targets. Any other `FOR UPDATE` pattern (traversal, multi-node,
//! non-literal properties) is flagged `unsupported` so the caller can surface a
//! warning rather than silently ignore the lock hint. See
//! `docs/proposals/serializable_snapshot_isolation.md` (Component C5).

use std::collections::HashMap;

use uni_common::Value;
use uni_cypher::ast::{Clause, Expr, LabelExpr, NodePattern, PathPattern, PatternElement, Query};

/// Lock keys collected from a query's `FOR UPDATE` matches.
#[derive(Debug, Default)]
pub(crate) struct ForUpdateKeys {
    /// Canonical lock keys, one per lockable keyed-node match.
    pub keys: Vec<Vec<u8>>,
    /// A `FOR UPDATE` match used a pattern that is not lockable (traversal,
    /// multi-element path, missing label, or non-literal properties). Surfaced
    /// as a warning so the hint is never silently dropped.
    pub unsupported: bool,
}

/// Collects `FOR UPDATE` lock keys from a parsed query.
pub(crate) fn collect_for_update_keys(
    query: &Query,
    params: &HashMap<String, Value>,
) -> ForUpdateKeys {
    let mut out = ForUpdateKeys::default();
    collect_from_query(query, params, &mut out);
    out
}

fn collect_from_query(query: &Query, params: &HashMap<String, Value>, out: &mut ForUpdateKeys) {
    match query {
        Query::Single(stmt) => {
            for clause in &stmt.clauses {
                if let Clause::Match(m) = clause
                    && m.for_update
                {
                    for path in &m.pattern.paths {
                        collect_from_path(path, params, out);
                    }
                }
            }
        }
        Query::Union { left, right, .. } => {
            collect_from_query(left, params, out);
            collect_from_query(right, params, out);
        }
        Query::Explain(inner) | Query::TimeTravel { query: inner, .. } => {
            collect_from_query(inner, params, out);
        }
        Query::Schema(_) => {}
    }
}

fn collect_from_path(path: &PathPattern, params: &HashMap<String, Value>, out: &mut ForUpdateKeys) {
    // Only a single keyed node is lockable; traversals and multi-element paths
    // would need per-row resolution and are out of scope for the escape hatch.
    if path.elements.len() != 1 {
        out.unsupported = true;
        return;
    }
    match &path.elements[0] {
        PatternElement::Node(node) => match build_node_key(node, params) {
            Some(key) => out.keys.push(key),
            None => out.unsupported = true,
        },
        _ => out.unsupported = true,
    }
}

/// Builds a deterministic lock key from a node's labels and literal/param
/// properties, or `None` if the node is not a lockable keyed node.
fn build_node_key(node: &NodePattern, params: &HashMap<String, Value>) -> Option<Vec<u8>> {
    let mut labels: Vec<String> = match &node.labels {
        LabelExpr::Conjunction(ls) | LabelExpr::Disjunction(ls) => ls.clone(),
        // A lock must be scoped to a label to be meaningful.
        LabelExpr::Empty => return None,
    };
    if labels.is_empty() {
        return None;
    }

    let Expr::Map(entries) = node.properties.as_ref()? else {
        return None;
    };
    if entries.is_empty() {
        return None;
    }

    let mut kvs: Vec<(String, Value)> = Vec::with_capacity(entries.len());
    for (k, expr) in entries {
        kvs.push((k.clone(), eval_simple(expr, params)?));
    }

    labels.sort();
    kvs.sort_by(|a, b| a.0.cmp(&b.0));

    let mut buf = Vec::new();
    for label in &labels {
        buf.extend_from_slice(label.as_bytes());
        buf.push(0);
    }
    buf.push(1);
    for (k, v) in &kvs {
        buf.extend_from_slice(k.as_bytes());
        buf.push(0);
        buf.extend(serde_json::to_vec(v).ok()?);
        buf.push(0);
    }
    Some(buf)
}

/// Evaluates a property expression to a value for literals and parameters only.
fn eval_simple(expr: &Expr, params: &HashMap<String, Value>) -> Option<Value> {
    match expr {
        Expr::Literal(lit) => Some(lit.to_value()),
        Expr::Parameter(name) => params.get(name).cloned(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys_of(cypher: &str) -> ForUpdateKeys {
        let query = uni_cypher::parse(cypher).expect("parse");
        collect_for_update_keys(&query, &HashMap::new())
    }

    #[test]
    fn keyed_node_produces_one_key() {
        let k = keys_of("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c");
        assert_eq!(k.keys.len(), 1);
        assert!(!k.unsupported);
    }

    #[test]
    fn same_logical_row_is_stable_across_queries() {
        let a = keys_of("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c");
        let b = keys_of("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.id");
        assert_eq!(a.keys, b.keys);
    }

    #[test]
    fn different_key_values_produce_different_keys() {
        let a = keys_of("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c");
        let b = keys_of("MATCH (c:Counter {id: 'y'}) FOR UPDATE RETURN c");
        assert_ne!(a.keys, b.keys);
    }

    #[test]
    fn without_for_update_no_keys() {
        let k = keys_of("MATCH (c:Counter {id: 'x'}) RETURN c");
        assert!(k.keys.is_empty());
        assert!(!k.unsupported);
    }

    #[test]
    fn traversal_for_update_is_unsupported() {
        let k = keys_of("MATCH (a:Counter {id: 'x'})-[:R]->(b) FOR UPDATE RETURN a");
        assert!(k.keys.is_empty());
        assert!(k.unsupported);
    }

    #[test]
    fn unlabeled_for_update_is_unsupported() {
        let k = keys_of("MATCH (c {id: 'x'}) FOR UPDATE RETURN c");
        assert!(k.keys.is_empty());
        assert!(k.unsupported);
    }
}
