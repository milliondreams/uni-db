// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Which of a rule's columns hold relationships.
//!
//! A rule's facts carry a node as its VID and a single-hop relationship as its
//! EID, both unsigned integers, and both are hydrated into entities after the
//! fixpoint. The two ID spaces overlap, so hydration must know which columns
//! hold EIDs: resolving an EID as a VID substitutes whatever node has that
//! number.
//!
//! A column holds relationships when a clause yields, under that column's
//! name, a relationship variable of its MATCH, or a variable an IS reference
//! binds to another rule's relationship column.

// Rust guideline compliant

use std::collections::{HashMap, HashSet};

use uni_cypher::ast::{Expr, PatternElement};
use uni_cypher::locy_ast::{RuleCondition, RuleOutput, resolve_yield_column_names};

use crate::is_binding::binding_column_names;
use crate::types::{CompiledClause, CompiledRule};

/// The relationship columns of every rule in `catalog`, keyed by rule name.
///
/// Rules without one are absent.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// let catalog = HashMap::new();
/// assert!(uni_locy::entity_columns::edge_columns(&catalog).is_empty());
/// ```
#[must_use]
pub fn edge_columns(catalog: &HashMap<String, CompiledRule>) -> HashMap<String, HashSet<String>> {
    let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
    // A column fed through an IS reference depends on the referenced rule's
    // columns, so iterate until nothing changes; each pass only adds.
    loop {
        let mut changed = false;
        for (name, rule) in catalog {
            for clause in &rule.clauses {
                for column in clause_edge_columns(clause, catalog, &edges) {
                    changed |= edges.entry(name.clone()).or_default().insert(column);
                }
            }
        }
        if !changed {
            return edges;
        }
    }
}

/// The YIELD columns one clause fills with relationships.
fn clause_edge_columns(
    clause: &CompiledClause,
    catalog: &HashMap<String, CompiledRule>,
    known: &HashMap<String, HashSet<String>>,
) -> Vec<String> {
    let RuleOutput::Yield(yield_clause) = &clause.output else {
        return Vec::new();
    };
    let mut edge_vars: HashSet<&str> = clause
        .match_pattern
        .paths
        .iter()
        .flat_map(|path| &path.elements)
        .filter_map(|element| match element {
            PatternElement::Relationship(rel) if rel.range.is_none() => rel.variable.as_deref(),
            _ => None,
        })
        .collect();
    for condition in &clause.where_conditions {
        let RuleCondition::IsReference(is_ref) = condition else {
            continue;
        };
        if is_ref.negated {
            continue;
        }
        let referenced = is_ref.rule_name.to_string();
        let (Some(rule), Some(columns)) = (catalog.get(&referenced), known.get(&referenced)) else {
            continue;
        };
        let sequence = binding_column_names(&rule.yield_schema);
        for (variable, column) in is_ref
            .subjects
            .iter()
            .chain(is_ref.target.iter())
            .zip(&sequence)
        {
            if columns.contains(column) {
                edge_vars.insert(variable.as_str());
            }
        }
    }
    resolve_yield_column_names(&yield_clause.items)
        .into_iter()
        .zip(&yield_clause.items)
        .filter(
            |(_, item)| matches!(&item.expr, Expr::Variable(v) if edge_vars.contains(v.as_str())),
        )
        .map(|(name, _)| name)
        .collect()
}
