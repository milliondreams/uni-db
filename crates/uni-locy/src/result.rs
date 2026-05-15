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
    /// Compile-time warnings carried over from `CompiledProgram.warnings`.
    /// Phase C C4: surfaces `UncalibratedNeuralPredicate` /
    /// `FoldInRecursivePath` / `UncalibratedLLMLogprobs` /
    /// `MsumNonNegativity` / `ProbabilityDomainViolation` so test
    /// harnesses and downstream tooling can inspect them on the
    /// returned `LocyResult` rather than re-running the compiler.
    pub compile_warnings: Vec<crate::types::CompilerWarning>,
    /// Groups where BDD computation fell back to independence mode.
    /// Maps rule name → list of human-readable key group descriptions.
    pub approximate_groups: HashMap<String, Vec<String>>,
    /// When present, contains the derived facts from a session-level DERIVE
    /// that have not yet been applied. Use `tx.apply(derived)` to materialize.
    pub derived_fact_set: Option<DerivedFactSet>,
    /// True when the evaluation was cut short by a timeout. The `derived` map
    /// contains whatever facts were accumulated before the timeout fired.
    /// Partial results may not satisfy the fixpoint invariant.
    pub timed_out: bool,
}

/// Result of executing a single Phase 4 command.
#[derive(Debug, Clone)]
pub enum CommandResult {
    Query(Vec<FactRow>),
    Assume(Vec<FactRow>),
    Explain(DerivationNode),
    Abduce(AbductionResult),
    Derive {
        affected: usize,
    },
    Cypher(Vec<FactRow>),
    /// Phase C C2: result of a `CALIBRATE` statement — the fitted
    /// calibrator plus pre- and post-calibration holdout metrics.
    Calibrate(CalibrationResult),
    /// Phase C C3: result of a `VALIDATE` statement — the metric
    /// values computed over `(rule_output, ground_truth)` pairs.
    Validate(ValidationResult),
}

/// Outcome of a single `VALIDATE` invocation. Phase C C3.
///
/// `metrics` maps each requested metric to its scalar value. The
/// `n_samples` field reports how many `(prediction, label)` pairs
/// were retained after joining the rule's PROB column with the
/// TARGET expression. Bare `ECE` produces a `EceBinningBias`
/// compile-time warning (surfaced via `LocyResult.compile_warnings`).
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub rule_name: String,
    pub prob_column: String,
    pub n_samples: usize,
    pub metrics: Vec<(uni_cypher::locy_ast::ValidationMetric, f64)>,
}

impl ValidationResult {
    pub fn metric(&self, m: uni_cypher::locy_ast::ValidationMetric) -> Option<f64> {
        self.metrics
            .iter()
            .find(|(name, _)| *name == m)
            .map(|(_, v)| *v)
    }
}

/// Phase C C1a: per-prediction confidence interval surfaced by
/// uncertainty-aware calibrators. For split-conformal, the band is
/// `[p - q, p + q]` clipped to `[0, 1]` where `q` is the
/// `(1 - alpha)`-quantile of holdout nonconformity scores.
#[derive(Debug, Clone, Copy)]
pub struct ConfidenceBand {
    pub lower: f64,
    pub upper: f64,
    pub source: ConfidenceSource,
}

/// Phase C C1a: provenance tag for a [`ConfidenceBand`] — identifies
/// which uncertainty-quantification machinery produced the bounds.
/// Only `Conformal` ships in C1a; ensemble / credal variants are
/// extensibility hooks documented in the implementation plan §3.1.
#[derive(Debug, Clone, Copy)]
pub enum ConfidenceSource {
    Conformal { alpha: f64 },
}

/// Outcome of a single `CALIBRATE` invocation. Phase C C2.
///
/// `calibrator` is the fitted transform; user code typically wraps it
/// over the base classifier via `CalibratedClassifier` and re-registers
/// the wrapped classifier in `LocyConfig::classifier_registry` for
/// subsequent evaluations.
#[derive(Debug, Clone)]
pub struct CalibrationResult {
    pub model_name: String,
    pub method: crate::calibration::CalibrationMethodKind,
    pub n_samples: usize,
    pub holdout_size: usize,
    pub calibrator: std::sync::Arc<dyn crate::calibration::Calibrator>,
    pub raw_brier: f64,
    pub raw_ece: f64,
    pub calibrated_brier: f64,
    pub calibrated_ece: f64,
    /// Phase C C1a: for conformal calibrators, the
    /// `(1 - alpha)`-quantile of holdout nonconformity scores —
    /// the half-width of every confidence band the calibrator will
    /// emit at inference. `None` for non-conformal methods.
    pub confidence_band_quantile: Option<f64>,
}

/// Phase C B1–B3: per neural-model invocation provenance, attached
/// to a [`DerivationNode`] when the derivation's body invoked one
/// or more classifiers. `raw_probability` is the classifier's
/// direct output; `calibrated_probability` is the post-Calibrator
/// value (when any calibrator other than `Identity` is registered).
/// `confidence_band` is populated when the active calibrator is
/// conformal (or any future band-emitting calibrator).
#[derive(Debug, Clone)]
pub struct NeuralProvenance {
    pub model_name: String,
    pub raw_probability: f64,
    pub calibrated_probability: Option<f64>,
    pub confidence_band: Option<ConfidenceBand>,
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
    /// Phase C B1–B3: neural-model invocations that contributed to
    /// this fact's derivation. Empty for purely-symbolic
    /// derivations.
    pub neural_calls: Vec<NeuralProvenance>,
}

/// Result of an ABDUCE query.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AbductionResult {
    pub modifications: Vec<ValidatedModification>,
}

/// A modification with validation status and cost.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ValidatedModification {
    pub modification: Modification,
    /// Whether this modification satisfies the ABDUCE goal when applied via savepoint.
    pub validated: bool,
    /// Cost metric for ranking modifications: RemoveEdge=1.0, ChangeProperty=0.5, AddEdge=1.5.
    pub cost: f64,
}

/// A proposed graph modification from ABDUCE.
#[derive(Debug, Clone, serde::Serialize)]
pub enum Modification {
    RemoveEdge {
        source_var: String,
        target_var: String,
        edge_var: String,
        edge_type: String,
        /// Property constraints used to identify the specific edge to remove.
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
    pub fn compile_warnings(&self) -> &[crate::types::CompilerWarning] {
        &self.compile_warnings
    }

    pub fn command_results(&self) -> &[CommandResult] {
        &self.command_results
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abduce_result_serializes_to_json() {
        let result = AbductionResult {
            modifications: vec![
                ValidatedModification {
                    modification: Modification::ChangeProperty {
                        element_var: "a".into(),
                        property: "flagged".into(),
                        old_value: Box::new(Value::String("false".into())),
                        new_value: Box::new(Value::String("true".into())),
                    },
                    validated: true,
                    cost: 0.5,
                },
                ValidatedModification {
                    modification: Modification::RemoveEdge {
                        source_var: "a".into(),
                        target_var: "b".into(),
                        edge_var: "e".into(),
                        edge_type: "TRANSFERS_TO".into(),
                        match_properties: HashMap::from([("amount".into(), Value::Float(1000.0))]),
                    },
                    validated: false,
                    cost: 1.0,
                },
                ValidatedModification {
                    modification: Modification::AddEdge {
                        source_var: "a".into(),
                        target_var: "b".into(),
                        edge_type: "FLAGGED_BY".into(),
                        properties: HashMap::new(),
                    },
                    validated: true,
                    cost: 1.5,
                },
            ],
        };

        let json = serde_json::to_value(&result).expect("serialization failed");
        let mods = json["modifications"].as_array().unwrap();
        assert_eq!(mods.len(), 3);
        assert_eq!(mods[0]["validated"], true);
        assert_eq!(mods[0]["cost"], 0.5);
        assert!(mods[0]["modification"]["ChangeProperty"].is_object());
        assert!(mods[1]["modification"]["RemoveEdge"].is_object());
        assert!(mods[2]["modification"]["AddEdge"].is_object());
    }
}
