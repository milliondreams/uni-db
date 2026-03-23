// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! EXPLAIN RULE derivation tree construction.
//!
//! Ported from `uni-locy/src/orchestrator/explain.rs`. Uses `DerivedFactSource`
//! instead of `CypherExecutor`. Uses `RowStore` for row-based fact storage.
//!
//! Implements Mode A (provenance-based, uses DerivationTracker recorded during fixpoint)
//! with fallback to Mode B (re-execution) when tracker has no entries for the rule.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use uni_common::Value;
use uni_cypher::locy_ast::{ExplainRule, RuleCondition};
use uni_locy::types::CompiledRule;
use uni_locy::{CompiledProgram, DerivationNode, LocyConfig, LocyError, LocyStats, Row};

use super::locy_delta::{
    KeyTuple, RowStore, extract_cypher_conditions, extract_key, resolve_clause_with_is_refs,
};

use super::locy_eval::{eval_expr, record_batches_to_locy_rows, values_equal_for_join};
use super::locy_slg::SLGResolver;
use super::locy_traits::DerivedFactSource;

/// Input dependency for a derived fact: IS-ref rule and source fact hash.
#[derive(Clone, Debug)]
pub struct DerivationInput {
    pub is_ref_rule: String,
    pub fact_hash: Vec<u8>,
}

/// Provenance record for a derived fact from fixpoint iteration.
#[derive(Clone, Debug)]
pub struct DerivationEntry {
    /// Name of the rule that derived this fact.
    pub rule_name: String,
    /// Index of the clause within the rule that produced this fact.
    pub clause_index: usize,
    /// Hashes of IS-ref input facts (populated when IS-ref tracking is available).
    pub inputs: Vec<DerivationInput>,
    /// ALONG column values captured at derivation time.
    pub along_values: HashMap<String, Value>,
    /// Fixpoint iteration number when the fact was first derived.
    pub iteration: usize,
    /// Full fact row stored for Mode A filtering/display.
    pub fact_row: Row,
}

/// Tracks provenance of derived facts from fixpoint iteration.
///
/// Enables Mode A (provenance-based) EXPLAIN without re-execution.
/// First-derivation-wins: once a fact hash is recorded, later iterations
/// do not overwrite it.
#[derive(Debug)]
pub struct DerivationTracker {
    entries: RwLock<HashMap<Vec<u8>, DerivationEntry>>,
}

impl DerivationTracker {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Record a derivation entry. First-derivation-wins: if the hash is already
    /// present, the existing entry is kept.
    pub fn record(&self, fact_hash: Vec<u8>, entry: DerivationEntry) {
        if let Ok(mut guard) = self.entries.write() {
            guard.entry(fact_hash).or_insert(entry);
        }
    }

    /// Look up the derivation entry for a fact hash.
    pub fn lookup(&self, fact_hash: &[u8]) -> Option<DerivationEntry> {
        self.entries.read().ok()?.get(fact_hash).cloned()
    }

    /// Get all entries for a specific rule name.
    pub fn entries_for_rule(&self, rule_name: &str) -> Vec<(Vec<u8>, DerivationEntry)> {
        match self.entries.read() {
            Ok(guard) => guard
                .iter()
                .filter(|(_, e)| e.rule_name == rule_name)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            Err(_) => vec![],
        }
    }
}

impl Default for DerivationTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Set of (rule_name, key_tuple) to detect cycles during recursive derivation (Mode B).
type VisitedSet = HashSet<(String, KeyTuple)>;

/// Build a derivation tree for a rule, showing how each fact was derived.
///
/// Tries Mode A (provenance-based, uses DerivationTracker) first when a tracker is
/// provided and has entries for the rule.  Falls through to Mode B (re-execution)
/// when Mode A cannot produce a result.
#[expect(
    clippy::too_many_arguments,
    reason = "explain requires full program context and tracker state"
)]
pub async fn explain_rule(
    query: &ExplainRule,
    program: &CompiledProgram,
    fact_source: &dyn DerivedFactSource,
    config: &LocyConfig,
    derived_store: &mut RowStore,
    stats: &mut LocyStats,
    tracker: Option<&DerivationTracker>,
    approximate_groups: Option<&HashMap<String, Vec<String>>>,
) -> Result<DerivationNode, LocyError> {
    // Mode A: provenance-based (no re-execution required).
    // Falls through to Mode B when tracker is absent or has no matching entries.
    if let Some(Ok(node)) =
        tracker.map(|t| explain_rule_mode_a(query, program, t, derived_store, approximate_groups))
    {
        return Ok(node);
    }

    // Mode B: re-execution fallback
    explain_rule_mode_b(
        query,
        program,
        fact_source,
        config,
        derived_store,
        stats,
        approximate_groups,
    )
    .await
}

/// Mode A: build derivation tree using recorded provenance from the fixpoint loop.
///
/// Returns `Err` when no tracker entries exist for the rule (signals Mode B fallback).
fn explain_rule_mode_a(
    query: &ExplainRule,
    program: &CompiledProgram,
    tracker: &DerivationTracker,
    _derived_store: &RowStore,
    approximate_groups: Option<&HashMap<String, Vec<String>>>,
) -> Result<DerivationNode, LocyError> {
    let rule_name = query.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::EvaluationError {
            message: format!("rule '{}' not found for EXPLAIN RULE (Mode A)", rule_name),
        })?;

    let tracker_entries = tracker.entries_for_rule(&rule_name);
    if tracker_entries.is_empty() {
        return Err(LocyError::EvaluationError {
            message: format!("no tracker entries for rule '{rule_name}' (falling back to Mode B)"),
        });
    }

    // Filter tracker entries by WHERE expression
    let matching_entries: Vec<_> = tracker_entries
        .into_iter()
        .filter(|(_, entry)| {
            eval_expr(&query.where_expr, &entry.fact_row)
                .map(|v| v.as_bool().unwrap_or(false))
                .unwrap_or(false)
        })
        .collect();

    if matching_entries.is_empty() {
        return Err(LocyError::EvaluationError {
            message: format!("no tracker entries match WHERE clause for rule '{rule_name}'"),
        });
    }

    let is_approximate = approximate_groups
        .map(|ag| ag.contains_key(&rule_name))
        .unwrap_or(false);

    let mut root = DerivationNode {
        rule: rule_name.clone(),
        clause_index: 0,
        priority: rule.priority,
        bindings: HashMap::new(),
        along_values: HashMap::new(),
        children: Vec::new(),
        graph_fact: None,
        approximate: is_approximate,
    };

    for (_, entry) in matching_entries {
        let along_values = extract_along_values(&entry.fact_row, rule);
        let clause_priority = rule
            .clauses
            .get(entry.clause_index)
            .and_then(|c| c.priority);
        let base_fact = format!(
            "[iter={}] {}",
            entry.iteration,
            format_graph_fact(&entry.fact_row)
        );
        let graph_fact = if is_approximate {
            format!("[APPROXIMATE] {}", base_fact)
        } else {
            base_fact
        };
        let node = DerivationNode {
            rule: rule_name.clone(),
            clause_index: entry.clause_index,
            priority: clause_priority.or(rule.priority),
            bindings: entry.fact_row.clone(),
            along_values,
            // Mode A: children not tracked (inputs list is reserved for future recursion)
            children: vec![],
            graph_fact: Some(graph_fact),
            approximate: is_approximate,
        };
        root.children.push(node);
    }

    Ok(root)
}

/// Mode B: re-execution fallback — re-executes clause queries to find which
/// clause produced each matching fact, then recurses into IS references.
async fn explain_rule_mode_b(
    query: &ExplainRule,
    program: &CompiledProgram,
    fact_source: &dyn DerivedFactSource,
    config: &LocyConfig,
    derived_store: &mut RowStore,
    stats: &mut LocyStats,
    approximate_groups: Option<&HashMap<String, Vec<String>>>,
) -> Result<DerivationNode, LocyError> {
    let rule_name = query.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::EvaluationError {
            message: format!("rule '{}' not found for EXPLAIN RULE", rule_name),
        })?;

    let key_columns: Vec<String> = rule
        .yield_schema
        .iter()
        .filter(|c| c.is_key)
        .map(|c| c.name.clone())
        .collect();

    // Re-evaluate the rule via SLG to obtain rows with full node objects and properties.
    // The native fixpoint's orch_store has VID-only integers that fail property-based
    // WHERE filters (e.g. a.name = 'A') — we need actual Value::Node values here.
    {
        let mut fresh_store = RowStore::new();
        let slg_start = std::time::Instant::now();
        let mut resolver =
            SLGResolver::new(program, fact_source, config, &mut fresh_store, slg_start);
        resolver.resolve_goal(&rule_name, &HashMap::new()).await?;
        stats.queries_executed += resolver.stats.queries_executed;
        // Merge full-node facts into derived_store so IS-ref lookups in
        // build_derivation_node also get proper node objects (not VIDs).
        for (name, relation) in fresh_store {
            derived_store.insert(name, relation);
        }
    }

    // Get all derived facts for this rule (now populated with full node objects)
    let facts = derived_store
        .get(&rule_name)
        .map(|r| r.rows.clone())
        .unwrap_or_default();

    // Filter facts by WHERE expression
    let filtered: Vec<Row> = facts
        .into_iter()
        .filter(|row| {
            eval_expr(&query.where_expr, row)
                .map(|v| v.as_bool().unwrap_or(false))
                .unwrap_or(false)
        })
        .collect();

    let is_approximate = approximate_groups
        .map(|ag| ag.contains_key(&rule_name))
        .unwrap_or(false);

    // Build derivation tree root
    let mut root = DerivationNode {
        rule: rule_name.clone(),
        clause_index: 0,
        priority: rule.priority,
        bindings: HashMap::new(),
        along_values: HashMap::new(),
        children: Vec::new(),
        graph_fact: None,
        approximate: is_approximate,
    };

    // For each matching fact, recursively build a derivation node
    for fact in &filtered {
        let mut visited = VisitedSet::new();
        let mut node = build_derivation_node(
            &rule_name,
            fact,
            &key_columns,
            program,
            fact_source,
            derived_store,
            stats,
            &mut visited,
            config.max_explain_depth,
        )
        .await?;
        if is_approximate {
            node.approximate = true;
            if let Some(ref gf) = node.graph_fact {
                node.graph_fact = Some(format!("[APPROXIMATE] {}", gf));
            }
        }
        root.children.push(node);
    }

    Ok(root)
}

/// Recursively build a derivation node for a single fact of a rule.
///
/// Finds which clause produced this fact, extracts ALONG values,
/// and recurses into IS reference dependencies.
#[expect(
    clippy::too_many_arguments,
    reason = "recursive derivation node builder requires full fact context"
)]
fn build_derivation_node<'a>(
    rule_name: &'a str,
    fact: &'a Row,
    key_columns: &'a [String],
    program: &'a CompiledProgram,
    fact_source: &'a dyn DerivedFactSource,
    derived_store: &'a mut RowStore,
    stats: &'a mut LocyStats,
    visited: &'a mut VisitedSet,
    max_depth: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<DerivationNode, LocyError>> + 'a>> {
    Box::pin(async move {
        let rule =
            program
                .rule_catalog
                .get(rule_name)
                .ok_or_else(|| LocyError::EvaluationError {
                    message: format!("rule '{}' not found during EXPLAIN", rule_name),
                })?;

        let key_tuple = extract_key(fact, key_columns);
        let visit_key = (rule_name.to_string(), key_tuple);

        // Cycle detection
        if !visited.insert(visit_key.clone()) || max_depth == 0 {
            return Ok(DerivationNode {
                rule: rule_name.to_string(),
                clause_index: 0,
                priority: rule.priority,
                bindings: fact.clone(),
                along_values: extract_along_values(fact, rule),
                children: Vec::new(),
                graph_fact: Some("(cycle)".to_string()),
                approximate: false,
            });
        }

        // Match on KEY columns only.  Clause-level resolution returns only
        // base graph bindings (vertex/edge identifiers); non-KEY yield columns
        // (FOLD-aggregated, similar_to, etc.) are absent from those rows.
        // KEY columns uniquely identify a derived fact, so this is sufficient.

        // Try each clause to find the one that produced this fact
        for (clause_idx, clause) in rule.clauses.iter().enumerate() {
            let has_is_refs = clause
                .where_conditions
                .iter()
                .any(|c| matches!(c, RuleCondition::IsReference(_)));
            let has_along = !clause.along.is_empty();

            let resolved = if has_is_refs || has_along {
                let rows = resolve_clause_with_is_refs(clause, fact_source, derived_store).await?;
                stats.queries_executed += 1;
                rows
            } else {
                let cypher_conditions = extract_cypher_conditions(&clause.where_conditions);
                let raw_batches = fact_source
                    .execute_pattern(&clause.match_pattern, &cypher_conditions)
                    .await?;
                stats.queries_executed += 1;
                record_batches_to_locy_rows(&raw_batches)
            };

            // Use values_equal_for_join for VID/EID-based comparison: sidecar
            // schema mode can add `overflow_json: Null` to nodes in some query
            // paths, making structural equality unreliable.
            let matching_row = resolved.iter().find(|row| {
                key_columns.iter().all(|k| match (row.get(k), fact.get(k)) {
                    (Some(v1), Some(v2)) => values_equal_for_join(v1, v2),
                    (None, None) => true,
                    _ => false,
                })
            });

            if let Some(evidence_row) = matching_row {
                let along_values = extract_along_values(fact, rule);

                // Build children by recursing into IS references
                let mut children = Vec::new();
                for cond in &clause.where_conditions {
                    if let RuleCondition::IsReference(is_ref) = cond {
                        if is_ref.negated {
                            continue;
                        }
                        let ref_rule_name = is_ref.rule_name.to_string();
                        if let Some(ref_rule) = program.rule_catalog.get(&ref_rule_name) {
                            let ref_key_columns: Vec<String> = ref_rule
                                .yield_schema
                                .iter()
                                .filter(|c| c.is_key)
                                .map(|c| c.name.clone())
                                .collect();

                            let ref_facts: Vec<Row> = derived_store
                                .get(&ref_rule_name)
                                .map(|r| r.rows.clone())
                                .unwrap_or_default();

                            let matching_ref_facts: Vec<Row> = ref_facts
                                .into_iter()
                                .filter(|ref_fact| {
                                    let subjects_match =
                                        is_ref.subjects.iter().enumerate().all(|(i, subject)| {
                                            binding_matches_key(
                                                evidence_row,
                                                fact,
                                                subject,
                                                ref_fact,
                                                ref_key_columns.get(i),
                                            )
                                        });
                                    let target_matches =
                                        is_ref.target.as_ref().is_none_or(|target| {
                                            binding_matches_key(
                                                evidence_row,
                                                fact,
                                                target,
                                                ref_fact,
                                                ref_key_columns.get(is_ref.subjects.len()),
                                            )
                                        });
                                    subjects_match && target_matches
                                })
                                .collect();

                            for ref_fact in matching_ref_facts {
                                let child = build_derivation_node(
                                    &ref_rule_name,
                                    &ref_fact,
                                    &ref_key_columns,
                                    program,
                                    fact_source,
                                    derived_store,
                                    stats,
                                    visited,
                                    max_depth - 1,
                                )
                                .await?;
                                children.push(child);
                            }
                        }
                    }
                }

                // Backtrack visited set
                visited.remove(&visit_key);

                let mut merged_bindings = evidence_row.clone();
                for (k, v) in fact {
                    merged_bindings.entry(k.clone()).or_insert(v.clone());
                }

                return Ok(DerivationNode {
                    rule: rule_name.to_string(),
                    clause_index: clause_idx,
                    priority: rule.clauses[clause_idx].priority,
                    bindings: merged_bindings,
                    along_values,
                    children,
                    graph_fact: Some(format_graph_fact(evidence_row)),
                    approximate: false,
                });
            }
        }

        // No clause matched — leaf node
        visited.remove(&visit_key);
        Ok(DerivationNode {
            rule: rule_name.to_string(),
            clause_index: 0,
            priority: rule.priority,
            bindings: fact.clone(),
            along_values: extract_along_values(fact, rule),
            children: Vec::new(),
            graph_fact: Some(format_graph_fact(fact)),
            approximate: false,
        })
    })
}

/// Check if a binding variable matches a ref-fact key column via VID-based join.
///
/// Looks up `var_name` in `primary` (falling back to `fallback`), then compares
/// it against `ref_key_col` in `ref_fact` using `values_equal_for_join`.
/// Returns `true` when the key column is out of range or the binding is absent.
fn binding_matches_key(
    primary: &Row,
    fallback: &Row,
    var_name: &str,
    ref_fact: &Row,
    ref_key_col: Option<&String>,
) -> bool {
    let Some(key_col) = ref_key_col else {
        return true;
    };
    let Some(val) = primary.get(var_name).or_else(|| fallback.get(var_name)) else {
        return true;
    };
    ref_fact
        .get(key_col)
        .is_some_and(|rv| values_equal_for_join(rv, val))
}

fn extract_along_values(fact: &Row, rule: &CompiledRule) -> HashMap<String, Value> {
    let mut along_values = HashMap::new();
    for clause in &rule.clauses {
        for along in &clause.along {
            if let Some(v) = fact.get(&along.name) {
                along_values.insert(along.name.clone(), v.clone());
            }
        }
    }
    along_values
}

pub(crate) fn format_graph_fact(row: &Row) -> String {
    let mut entries: Vec<String> = row
        .iter()
        .map(|(k, v)| format!("{}: {}", k, format_value(v)))
        .collect();
    entries.sort();
    format!("{{{}}}", entries.join(", "))
}

fn format_value(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => format!("\"{}\"", s),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::Map(m) => {
            let mut entries: Vec<String> = m
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                .collect();
            entries.sort();
            format!("{{{}}}", entries.join(", "))
        }
        Value::Node(n) => format!("Node({})", n.vid.as_u64()),
        Value::Edge(e) => format!("Edge({})", e.eid.as_u64()),
        _ => format!("{v:?}"),
    }
}
