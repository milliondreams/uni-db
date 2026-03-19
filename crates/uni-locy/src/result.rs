use std::collections::HashMap;
use std::time::Duration;

use uni_common::Value;

use crate::types::{RuntimeWarning, RuntimeWarningCode};

/// A single row of bindings from a query result.
pub type Row = HashMap<String, Value>;

/// Opaque savepoint identifier for transactional rollback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SavepointId(pub u64);

/// The result of evaluating a compiled Locy program.
#[derive(Debug, Clone)]
pub struct LocyResult {
    /// Derived facts per rule name.
    pub derived: HashMap<String, Vec<Row>>,
    /// Execution statistics.
    pub stats: LocyStats,
    /// Results from Phase 4 commands.
    pub command_results: Vec<CommandResult>,
    /// Runtime warnings collected during evaluation.
    pub warnings: Vec<RuntimeWarning>,
}

/// Result of executing a single Phase 4 command.
#[derive(Debug, Clone)]
pub enum CommandResult {
    Query(Vec<Row>),
    Assume(Vec<Row>),
    Explain(DerivationNode),
    Abduce(AbductionResult),
    Derive { affected: usize },
    Cypher(Vec<Row>),
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
    pub fn derived_facts(&self, rule: &str) -> Option<&Vec<Row>> {
        self.derived.get(rule)
    }

    /// Get rows from the first Query command result.
    pub fn rows(&self) -> Option<&Vec<Row>> {
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
    pub fn as_query(&self) -> Option<&Vec<Row>> {
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
