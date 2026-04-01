use std::collections::HashMap;
use std::time::Duration;

use uni_common::{Properties, Value};

use crate::types::{RuntimeWarning, RuntimeWarningCode};

/// A single row of bindings from a Locy evaluation result.
pub type FactRow = HashMap<String, Value>;

/// The result of evaluating a compiled Locy program.
#[derive(Debug, Clone)]
pub struct LocyResult {
    /// Derived facts per rule name.
    pub derived: HashMap<String, Vec<FactRow>>,
    /// Execution statistics.
    pub stats: LocyStats,
    /// Results from Phase 4 commands.
    pub command_results: Vec<CommandResult>,
    /// Runtime warnings collected during evaluation.
    pub warnings: Vec<RuntimeWarning>,
    /// Groups where BDD computation fell back to independence mode.
    /// Maps rule name → list of human-readable key group descriptions.
    pub approximate_groups: HashMap<String, Vec<String>>,
    /// When present, contains the derived facts from a session-level DERIVE
    /// that have not yet been applied. Use `tx.apply(derived)` to materialize.
    pub derived_fact_set: Option<DerivedFactSet>,
}

/// Result of executing a single Phase 4 command.
#[derive(Debug, Clone)]
pub enum CommandResult {
    Query(Vec<FactRow>),
    Assume(Vec<FactRow>),
    Explain(DerivationNode),
    Abduce(AbductionResult),
    Derive { affected: usize },
    Cypher(Vec<FactRow>),
}

/// A node in a derivation tree, produced by EXPLAIN RULE.
#[derive(Debug, Clone)]
pub struct DerivationNode {
    pub rule: String,
    pub clause_index: usize,
    pub priority: Option<i64>,
    pub bindings: HashMap<String, Value>,
    pub along_values: HashMap<String, Value>,
    pub children: Vec<DerivationNode>,
    pub graph_fact: Option<String>,
    /// True when this node's probability was computed via BDD fallback
    /// (independence mode) because the group exceeded `max_bdd_variables`.
    pub approximate: bool,
    /// Probability of this specific proof path, populated when top-k proof
    /// filtering is active (Scallop, Huang et al. 2021).
    pub proof_probability: Option<f64>,
}

/// Result of an ABDUCE query.
#[derive(Debug, Clone)]
pub struct AbductionResult {
    pub modifications: Vec<ValidatedModification>,
}

/// A modification with validation status and cost.
#[derive(Debug, Clone)]
pub struct ValidatedModification {
    pub modification: Modification,
    pub validated: bool,
    pub cost: f64,
}

/// A proposed graph modification from ABDUCE.
#[derive(Debug, Clone)]
pub enum Modification {
    RemoveEdge {
        source_var: String,
        target_var: String,
        edge_var: String,
        edge_type: String,
        match_properties: HashMap<String, Value>,
    },
    ChangeProperty {
        element_var: String,
        property: String,
        old_value: Box<Value>,
        new_value: Box<Value>,
    },
    AddEdge {
        source_var: String,
        target_var: String,
        edge_type: String,
        properties: HashMap<String, Value>,
    },
}

/// A derived edge to be materialized.
#[derive(Debug, Clone)]
pub struct DerivedEdge {
    pub edge_type: String,
    pub source_label: String,
    pub source_properties: Properties,
    pub target_label: String,
    pub target_properties: Properties,
    pub edge_properties: Properties,
}

/// Pure-data representation of facts derived by a session-level DERIVE.
///
/// Apply to a transaction via `tx.apply(derived)` or `tx.apply_with(derived)`.
#[derive(Debug, Clone)]
pub struct DerivedFactSet {
    /// New vertices grouped by label.
    pub vertices: HashMap<String, Vec<Properties>>,
    /// Derived edges connecting source/target vertices.
    pub edges: Vec<DerivedEdge>,
    /// Evaluation statistics from the DERIVE run.
    pub stats: LocyStats,
    /// Database version at evaluation time (for staleness detection).
    pub evaluated_at_version: u64,
    /// Internal: Cypher ASTs for faithful replay during `tx.apply()`.
    #[doc(hidden)]
    pub mutation_queries: Vec<uni_cypher::ast::Query>,
}

impl DerivedFactSet {
    /// Total number of derived facts (vertices + edges).
    pub fn fact_count(&self) -> usize {
        self.vertices.values().map(|v| v.len()).sum::<usize>() + self.edges.len()
    }

    /// True when no facts were derived.
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty() && self.edges.is_empty()
    }
}

/// Statistics collected during Locy program evaluation.
#[derive(Debug, Clone, Default)]
pub struct LocyStats {
    pub strata_evaluated: usize,
    pub total_iterations: usize,
    pub derived_nodes: usize,
    pub derived_edges: usize,
    pub evaluation_time: Duration,
    pub queries_executed: usize,
    pub mutations_executed: usize,
    /// Peak memory used by derived relations (in bytes).
    pub peak_memory_bytes: usize,
}

impl LocyResult {
    /// Get derived facts for a specific rule.
    pub fn derived_facts(&self, rule: &str) -> Option<&Vec<FactRow>> {
        self.derived.get(rule)
    }

    /// Get rows from the first Query command result.
    pub fn rows(&self) -> Option<&Vec<FactRow>> {
        self.command_results.iter().find_map(|cr| cr.as_query())
    }

    /// Get column names from the first Query command result's first row.
    pub fn columns(&self) -> Option<Vec<String>> {
        self.rows()
            .and_then(|rows| rows.first().map(|row| row.keys().cloned().collect()))
    }

    /// Get execution statistics.
    pub fn stats(&self) -> &LocyStats {
        &self.stats
    }

    /// Get the total number of fixpoint iterations.
    pub fn iterations(&self) -> usize {
        self.stats.total_iterations
    }

    /// Get runtime warnings collected during evaluation.
    pub fn warnings(&self) -> &[RuntimeWarning] {
        &self.warnings
    }

    /// Check whether a specific warning code was emitted.
    pub fn has_warning(&self, code: &RuntimeWarningCode) -> bool {
        self.warnings.iter().any(|w| w.code == *code)
    }
}

impl CommandResult {
    /// If this is an Explain result, return the derivation node.
    pub fn as_explain(&self) -> Option<&DerivationNode> {
        match self {
            CommandResult::Explain(node) => Some(node),
            _ => None,
        }
    }

    /// If this is a Query result, return the rows.
    pub fn as_query(&self) -> Option<&Vec<FactRow>> {
        match self {
            CommandResult::Query(rows) => Some(rows),
            _ => None,
        }
    }

    /// If this is an Abduce result, return it.
    pub fn as_abduce(&self) -> Option<&AbductionResult> {
        match self {
            CommandResult::Abduce(result) => Some(result),
            _ => None,
        }
    }
}
