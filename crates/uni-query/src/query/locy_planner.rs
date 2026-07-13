// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Locy plan builder — translates a [`CompiledProgram`] into a `LogicalPlan::LocyProgram`.
//!
//! Stratifies rules, plans clause bodies via [`QueryPlanner::plan_pattern()`],
//! and assembles [`LocyStratum`] / [`LocyRulePlan`] / [`LocyClausePlan`] trees with
//! `LocyDerivedScan` nodes for IS-reference data injection.
//!
//! **Scope:** Strata (rules + clauses) only. Commands (`DERIVE`, `QUERY`, `ASSUME`,
//! `ABDUCE`, `EXPLAIN`) are dispatched by the orchestrator after strata evaluation;
//! dedicated command plan building is a future enhancement.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use parking_lot::RwLock;

use uni_common::core::schema::Schema;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, PatternElement};
use uni_cypher::locy_ast::{
    LocyBinaryOp, LocyExpr, RuleCondition, RuleOutput, resolve_yield_column_names,
};
use uni_locy::types::{
    CompiledClause, CompiledCommand, CompiledProgram, CompiledRule, Stratum, YieldColumn,
};

/// Collect all node-typed variable names from a rule's clauses.
///
/// This includes MATCH pattern node variables AND IS-ref subject/target variables,
/// all of which hold node VIDs (UInt64). Used for yield column type inference.
///
/// NOTE: For IS-ref predicate building (where `{var}._vid` column references are
/// needed), use `collect_match_node_vars` instead — only MATCH pattern variables
/// have expanded `._vid` columns in the graph scan output.
fn collect_node_vars(clauses: &[CompiledClause]) -> HashSet<String> {
    let mut node_vars = HashSet::new();
    for clause in clauses {
        collect_match_node_vars(clause, &mut node_vars);
        // IS-ref subjects and targets are also node VIDs (UInt64)
        for condition in &clause.where_conditions {
            if let RuleCondition::IsReference(is_ref) = condition {
                for subject in &is_ref.subjects {
                    node_vars.insert(subject.clone());
                }
                if let Some(target_var) = &is_ref.target {
                    node_vars.insert(target_var.clone());
                }
            }
        }
    }
    node_vars
}

/// Collect node variable names from a single clause's MATCH pattern only.
///
/// Only these variables have expanded `{var}._vid`, `{var}._labels`, `{var}._all_props`
/// columns in the graph scan output. IS-ref target/subject variables exist as plain
/// columns in the derived scan and should NOT be rewritten to `{var}._vid`.
fn collect_match_node_vars(clause: &CompiledClause, node_vars: &mut HashSet<String>) {
    for path in &clause.match_pattern.paths {
        for elem in &path.elements {
            if let PatternElement::Node(np) = elem
                && let Some(var) = &np.variable
            {
                node_vars.insert(var.clone());
            }
        }
    }
}

/// Map node variables in a clause's MATCH pattern to their first declared label.
///
/// Used to resolve a property-access expression's declared type from the graph
/// schema (e.g. `a.id` where `a` is `(:Node)` → `Node.id`).
fn clause_var_labels(clause: &CompiledClause) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    for path in &clause.match_pattern.paths {
        for elem in &path.elements {
            if let PatternElement::Node(np) = elem
                && let Some(var) = &np.variable
                && let Some(label) = np.labels.names().first()
            {
                labels.entry(var.clone()).or_insert_with(|| label.clone());
            }
        }
    }
    labels
}

/// Resolve the Arrow type of a property access from the graph schema, given the
/// node variable's label. Returns `None` when the label or property is unknown.
fn property_arrow_type(
    schema: &Schema,
    var_labels: &HashMap<String, String>,
    var: &str,
    prop: &str,
) -> Option<DataType> {
    let label = var_labels.get(var)?;
    let meta = schema.properties.get(label)?.get(prop)?;
    Some(meta.r#type.to_arrow())
}

/// Infer the Arrow DataType for a yield column based on its expression in the first clause.
///
/// `rule_catalog` lets a yield column that merely forwards a NON-KEY value column
/// brought in by a positive IS-reference (e.g. `WHERE (p,c) IS pc_mapped YIELD ... infringement`)
/// resolve to that source column's real type instead of defaulting to LargeUtf8.
/// Without this, the derived-scan schema of such a rule mis-types the column as
/// Utf8 while its materialized data carries the true type (Float64/Int64), and a
/// downstream rule that scans it fails with an Arrow schema mismatch.
///
/// `is_key`, `schema`, and `var_labels` let an integer-typed property in a KEY
/// position keep its `Int64` type instead of being widened to the default
/// `Float64` (issue #94); `var_labels` describes `first_clause`'s node vars.
#[allow(clippy::too_many_arguments)]
fn infer_yield_type(
    name: &str,
    first_clause: &CompiledClause,
    node_vars: &HashSet<String>,
    fold_output_names: &HashSet<&str>,
    along_names: &HashSet<&str>,
    rule_catalog: &HashMap<String, CompiledRule>,
    is_key: bool,
    schema: &Schema,
    var_labels: &HashMap<String, String>,
) -> DataType {
    let mut visited = HashSet::new();
    infer_yield_type_rec(
        name,
        first_clause,
        node_vars,
        fold_output_names,
        along_names,
        rule_catalog,
        is_key,
        schema,
        var_labels,
        &mut visited,
    )
}

#[allow(clippy::too_many_arguments)]
fn infer_yield_type_rec(
    name: &str,
    first_clause: &CompiledClause,
    node_vars: &HashSet<String>,
    fold_output_names: &HashSet<&str>,
    along_names: &HashSet<&str>,
    rule_catalog: &HashMap<String, CompiledRule>,
    is_key: bool,
    schema: &Schema,
    var_labels: &HashMap<String, String>,
    visited: &mut HashSet<String>,
) -> DataType {
    // FOLD outputs — type depends on the aggregate function.
    // COUNT/MCOUNT produce Int64; SUM/MSUM/AVG produce Float64.
    if fold_output_names.contains(name) {
        if let Some(fold) = first_clause.fold.iter().find(|fb| fb.name == name)
            && let Expr::FunctionCall { name: fn_name, .. } = &fold.aggregate
        {
            match fn_name.to_uppercase().as_str() {
                "COUNT" | "MCOUNT" => return DataType::Int64,
                _ => {}
            }
        }
        return DataType::Float64;
    }
    // ALONG bindings → Float64 (typically numeric accumulations)
    if along_names.contains(name) {
        return DataType::Float64;
    }
    // Look at the yield expression from the first clause. The UInt64 (node-VID)
    // shortcut is gated on the expression being a bare node Variable — NOT on the
    // column name — so a property-access KEY whose name happens to equal a node
    // var (e.g. `YIELD KEY a.id AS a`) is typed from its property expression
    // instead of being mis-typed as a VID (issue #94).
    if let RuleOutput::Yield(yc) = &first_clause.output {
        let item_names = resolve_yield_column_names(&yc.items);
        for (item, item_name) in yc.items.iter().zip(item_names.iter()) {
            if item_name == name {
                if let Expr::Variable(v) = &item.expr {
                    // A bare Variable naming a node var is a whole-node KEY → UInt64.
                    if node_vars.contains(v) {
                        return DataType::UInt64;
                    }
                    // A bare Variable referencing an ALONG name → Float64
                    // (ALONG bindings are numeric). Without this,
                    // `ew AS link_weight` would infer Variable("ew") as LargeUtf8.
                    if along_names.contains(v.as_str()) {
                        return DataType::Float64;
                    }
                    // A bare Variable naming a NON-KEY value column carried in by a
                    // positive IS-reference: resolve its type from the source rule
                    // rather than defaulting to LargeUtf8.
                    if let Some(dt) =
                        infer_is_ref_value_col_type(v, first_clause, rule_catalog, schema, visited)
                    {
                        return dt;
                    }
                }
                // A bare property-access column carries its schema-declared
                // type. The default `Property → Float64` rule (`infer_expr_type`)
                // is only correct for numeric properties; a non-numeric property
                // (DateTime/Duration/Btic/String/Bytes/Bool) would be widened to
                // Float64 and forced through an unsupported projection cast
                // ("Casting Struct to Float64") or cast-collapsed to Null, and an
                // integer property would lose its Int64 type. Look the real type
                // up from the schema for both KEY and value columns; for a
                // schemaless property the type is unknown, so default to
                // LargeBinary (the cv-encoded storage type) and let it pass
                // through unchanged to be decoded at read time. Generalizes the
                // Int64-only KEY fix from issue #94 to every property (issue
                // #112: a schemaless/typed property KEY projected Null).
                if let Expr::Property(object, prop) = &item.expr
                    && let Expr::Variable(var) = object.as_ref()
                    && let Some(dt) = property_arrow_type(schema, var_labels, var, prop)
                {
                    return dt;
                }
                let _ = is_key;
                return infer_expr_type(&item.expr, node_vars);
            }
        }
    }
    // No explicit yield item matched. A column named after a node var carries its
    // VID (UInt64); otherwise try an IS-ref-forwarded value column.
    if node_vars.contains(name) {
        return DataType::UInt64;
    }
    if let Some(dt) = infer_is_ref_value_col_type(name, first_clause, rule_catalog, schema, visited)
    {
        return dt;
    }
    DataType::LargeUtf8
}

/// If `col` is a NON-KEY value column produced by one of `clause`'s positive
/// IS-references, return that column's type as inferred from the referenced
/// rule. `visited` (keyed `rule::col`) guards against cyclic inference through
/// recursive rules.
fn infer_is_ref_value_col_type(
    col: &str,
    clause: &CompiledClause,
    rule_catalog: &HashMap<String, CompiledRule>,
    schema: &Schema,
    visited: &mut HashSet<String>,
) -> Option<DataType> {
    for cond in &clause.where_conditions {
        let RuleCondition::IsReference(ir) = cond else {
            continue;
        };
        if ir.negated {
            continue;
        }
        let rule_name = ir.rule_name.to_string();
        let Some(rule) = rule_catalog.get(&rule_name) else {
            continue;
        };
        if !rule
            .yield_schema
            .iter()
            .any(|yc| !yc.is_key && yc.name == col)
        {
            continue;
        }
        if !visited.insert(format!("{rule_name}::{col}")) {
            return Some(DataType::LargeUtf8); // cycle guard
        }
        let Some(src_clause) = rule.clauses.first() else {
            return Some(DataType::LargeUtf8);
        };
        let src_node_vars = collect_node_vars(&rule.clauses);
        let src_fold: HashSet<&str> = src_clause.fold.iter().map(|fb| fb.name.as_str()).collect();
        let src_along: HashSet<&str> = src_clause.along.iter().map(|a| a.name.as_str()).collect();
        let src_var_labels = clause_var_labels(src_clause);
        // Forwarded columns are NON-KEY (filtered above), so `is_key = false`.
        return Some(infer_yield_type_rec(
            col,
            src_clause,
            &src_node_vars,
            &src_fold,
            &src_along,
            rule_catalog,
            false,
            schema,
            &src_var_labels,
            visited,
        ));
    }
    None
}

/// Infer Arrow DataType from a Cypher expression.
fn infer_expr_type(expr: &Expr, node_vars: &HashSet<String>) -> DataType {
    match expr {
        Expr::Variable(v) if node_vars.contains(v) => DataType::UInt64,
        Expr::Literal(CypherLiteral::Integer(_)) => DataType::Int64,
        Expr::Literal(CypherLiteral::Float(_)) => DataType::Float64,
        Expr::Literal(CypherLiteral::String(_)) => DataType::LargeUtf8,
        Expr::Literal(CypherLiteral::Bool(_)) => DataType::Boolean,
        Expr::Literal(CypherLiteral::Null) => DataType::LargeUtf8,
        Expr::Property(_, _) => DataType::Float64,
        // Binary operations: infer from operator and operand types.
        Expr::BinaryOp { left, op, right } => {
            use uni_cypher::ast::BinaryOp::*;
            match op {
                // Comparison and logical operators always return Boolean.
                Eq | NotEq | Lt | LtEq | Gt | GtEq | And | Or | Xor | Regex | Contains
                | StartsWith | EndsWith => DataType::Boolean,
                // Arithmetic operators: infer from operands.
                Add | Sub | Mul | Div | Mod | Pow | ApproxEq => {
                    let lt = infer_expr_type(left, node_vars);
                    let rt = infer_expr_type(right, node_vars);
                    // If either operand is Float64, result is Float64.
                    if lt == DataType::Float64 || rt == DataType::Float64 {
                        DataType::Float64
                    } else if lt == DataType::Int64 && rt == DataType::Int64 {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    }
                }
            }
        }
        // Unary operations: infer from inner expression.
        Expr::UnaryOp { op, expr: inner } => {
            use uni_cypher::ast::UnaryOp;
            match op {
                UnaryOp::Not => DataType::Boolean,
                UnaryOp::Neg => infer_expr_type(inner, node_vars),
            }
        }
        Expr::IsNull(_) | Expr::IsNotNull(_) => DataType::Boolean,
        // Function calls: infer return type from function name.
        Expr::FunctionCall { name, args, .. } => {
            match name.to_uppercase().as_str() {
                // Numeric functions → Float64
                "SIMILAR_TO" | "ABS" | "SQRT" | "LOG" | "LOG10" | "EXP" | "CEIL" | "FLOOR"
                | "ROUND" | "SIGN" | "RAND" | "TOFLOAT" | "TODOUBLE" | "COS" | "SIN" | "TAN"
                | "ACOS" | "ASIN" | "ATAN" | "ATAN2" | "DEGREES" | "RADIANS" | "PI" | "E"
                | "DISTANCE" => DataType::Float64,
                // Integer-returning functions
                "TOINTEGER" | "LENGTH" | "SIZE" | "ID" => DataType::Int64,
                // String-returning functions
                "TOSTRING" | "TOLOWER" | "TOUPPER" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE"
                | "SUBSTRING" | "LEFT" | "RIGHT" | "REVERSE" | "TYPE" => DataType::LargeUtf8,
                // Boolean-returning functions
                "EXISTS" | "STARTSWITH" | "ENDSWITH" | "CONTAINS" => DataType::Boolean,
                // Aggregates that return Float64
                "SUM" | "AVG" | "MAX" | "MIN" => DataType::Float64,
                "COUNT" => DataType::Int64,
                // Unknown function: try to infer from first argument
                _ => {
                    if let Some(first_arg) = args.first() {
                        infer_expr_type(first_arg, node_vars)
                    } else {
                        DataType::LargeUtf8
                    }
                }
            }
        }
        _ => DataType::LargeUtf8,
    }
}

use super::df_graph::locy_fixpoint::{DerivedScanEntry, DerivedScanRegistry};
use super::planner::{LogicalPlan, QueryPlanner};
use super::planner_locy_types::{
    LocyClausePlan, LocyCommand, LocyIsRef, LocyRulePlan, LocyStratum, LocyYieldColumn,
};

// ---------------------------------------------------------------------------
// DerivedScanHandle
// ---------------------------------------------------------------------------

/// Internal handle tracking a single derived-scan injection point.
///
/// Each handle corresponds to a `LocyDerivedScan` node in the plan tree and
/// a `DerivedScanEntry` in the resulting registry. The `Arc<RwLock>` data handle
/// is shared between the plan node and registry entry so that `FixpointExec`
/// can inject data that flows into the subplan.
#[derive(Clone)]
struct DerivedScanHandle {
    rule_name: String,
    scan_index: usize,
    is_self_ref: bool,
    data: Arc<RwLock<Vec<RecordBatch>>>,
    schema: SchemaRef,
}

// ---------------------------------------------------------------------------
// LocyPlanBuilder
// ---------------------------------------------------------------------------

/// Builds a `LogicalPlan::LocyProgram` from a [`CompiledProgram`].
///
/// The builder translates each stratum, rule, and clause into corresponding
/// plan-level types, wiring `LocyDerivedScan` nodes for IS-reference data
/// injection. The returned `DerivedScanRegistry` shares `Arc<RwLock>` handles
/// with the plan nodes so that `FixpointExec` can populate derived data at
/// execution time.
pub struct LocyPlanBuilder<'a> {
    planner: &'a QueryPlanner,
    derived_scan_handles: RefCell<Vec<DerivedScanHandle>>,
    /// Plugin registry used to resolve Locy aggregates for the recursive-stratum
    /// monotonicity check. Defaults to the process-wide built-in registry; hosts
    /// with user-registered aggregates should override via
    /// [`Self::with_plugin_registry`].
    plugin_registry: std::sync::Arc<uni_plugin::PluginRegistry>,
}

/// Neural-classifier-related plumbing threaded through `build_stratum` →
/// `build_rule` → `build_clause`. Bundled to avoid the
/// too-many-arguments smell on those builders.
#[derive(Clone)]
struct ClassifierContext {
    registry: Arc<uni_locy::ClassifierRegistry>,
    cache: Option<Arc<uni_locy::ModelInvocationCache>>,
    provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
}

/// Probabilistic-evaluation knobs threaded through the same builders.
/// Fields are not yet read by the clause-level builders (the actual
/// strict-domain / epsilon enforcement happens later in the fixpoint
/// hot path); they ride along here so the planner surface stays
/// consistent with the runtime config and is forward-compatible with
/// plan-time enforcement.
#[derive(Clone, Copy)]
#[allow(
    dead_code,
    reason = "reserved for plan-time probabilistic-config rollout"
)]
struct ProbabilityConfig {
    strict_domain: bool,
    epsilon: f64,
}

/// Lookup state shared by the clause builder: target rule catalog,
/// the in-progress stratum's rule names, and the clause's bound node
/// variables. Bundled to keep `build_clause` under the
/// too-many-arguments threshold.
struct ClauseCtx<'a> {
    stratum_rule_names: &'a HashSet<String>,
    rule_catalog: &'a HashMap<String, CompiledRule>,
    node_vars: &'a HashSet<String>,
}

impl<'a> LocyPlanBuilder<'a> {
    /// Create a new plan builder backed by the given `QueryPlanner`.
    pub fn new(planner: &'a QueryPlanner) -> Self {
        Self {
            planner,
            derived_scan_handles: RefCell::new(Vec::new()),
            plugin_registry: crate::query::df_graph::locy_fold::default_locy_plugin_registry(),
        }
    }

    /// Replace the plugin registry used for aggregate monotonicity resolution.
    #[must_use]
    pub fn with_plugin_registry(
        mut self,
        registry: std::sync::Arc<uni_plugin::PluginRegistry>,
    ) -> Self {
        self.plugin_registry = registry;
        self
    }

    /// Build a full `LogicalPlan::LocyProgram` with embedded `DerivedScanRegistry`.
    #[expect(clippy::too_many_arguments, reason = "mirrors LocyConfig fields")]
    pub fn build_program_plan(
        &self,
        compiled: &CompiledProgram,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
    ) -> Result<LogicalPlan> {
        // Legacy entry point — defaults to AddMultProb. New code paths
        // should call `build_program_plan_with_semiring_and_classifiers`.
        self.build_program_plan_with_semiring_and_classifiers(
            compiled,
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            uni_locy::SemiringKind::AddMultProb,
            Arc::new(uni_locy::ClassifierRegistry::new()),
        )
    }

    /// Build a `LocyProgram` plan threading an explicit semiring through.
    /// Compatibility wrapper; for full Slice 3 wiring use
    /// `build_program_plan_with_semiring_and_classifiers`.
    #[expect(clippy::too_many_arguments, reason = "mirrors LocyConfig fields")]
    pub fn build_program_plan_with_semiring(
        &self,
        compiled: &CompiledProgram,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        semiring_kind: uni_locy::SemiringKind,
    ) -> Result<LogicalPlan> {
        self.build_program_plan_with_semiring_and_classifiers(
            compiled,
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            semiring_kind,
            Arc::new(uni_locy::ClassifierRegistry::new()),
        )
    }

    /// Phase B Slice 3 entry: threads both the active semiring and the
    /// runtime classifier registry through the plan.
    #[expect(clippy::too_many_arguments, reason = "mirrors LocyConfig fields")]
    pub fn build_program_plan_with_semiring_and_classifiers(
        &self,
        compiled: &CompiledProgram,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        semiring_kind: uni_locy::SemiringKind,
        classifier_registry: Arc<uni_locy::ClassifierRegistry>,
    ) -> Result<LogicalPlan> {
        self.build_program_plan_with_full_neural(
            compiled,
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            semiring_kind,
            classifier_registry,
            None,
            None,
        )
    }

    /// Phase B follow-up: full entry threading both the classifier
    /// registry and the optional memoization cache.
    #[expect(clippy::too_many_arguments, reason = "mirrors LocyConfig fields")]
    pub fn build_program_plan_with_full_neural(
        &self,
        compiled: &CompiledProgram,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
        semiring_kind: uni_locy::SemiringKind,
        classifier_registry: Arc<uni_locy::ClassifierRegistry>,
        classifier_cache: Option<Arc<uni_locy::ModelInvocationCache>>,
        classifier_provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    ) -> Result<LogicalPlan> {
        let mut strata = Vec::with_capacity(compiled.strata.len());

        let prob_config = ProbabilityConfig {
            strict_domain: strict_probability_domain,
            epsilon: probability_epsilon,
        };
        let classifiers = ClassifierContext {
            registry: Arc::clone(&classifier_registry),
            cache: classifier_cache.as_ref().map(Arc::clone),
            provenance_store: classifier_provenance_store.as_ref().map(Arc::clone),
        };
        for stratum in &compiled.strata {
            let rule_names: HashSet<String> =
                stratum.rules.iter().map(|r| r.name.clone()).collect();
            let locy_stratum = self.build_stratum(
                stratum,
                &compiled.rule_catalog,
                &rule_names,
                prob_config,
                &classifiers,
            )?;
            strata.push(locy_stratum);
        }

        let registry = self.build_registry();
        let plan = LogicalPlan::LocyProgram {
            strata,
            commands: self.build_commands_with_models(&compiled.commands, &compiled.model_catalog),
            derived_scan_registry: Arc::new(registry),
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
            semiring_kind,
            classifier_registry,
            classifier_cache,
            classifier_provenance_store,
        };

        Ok(plan)
    }

    /// Build `LocyCommand` list from compiled commands.
    ///
    /// Commands carry AST data for dispatch by the caller (e.g., `evaluate_native`)
    /// via the orchestrator after strata evaluation completes.
    fn build_commands_with_models(
        &self,
        commands: &[CompiledCommand],
        model_catalog: &HashMap<String, uni_locy::CompiledModel>,
    ) -> Vec<LocyCommand> {
        commands
            .iter()
            .map(|cmd| match cmd {
                CompiledCommand::GoalQuery(gq) => LocyCommand::GoalQuery {
                    goal_query: gq.clone(),
                },
                CompiledCommand::ExplainRule(er) => LocyCommand::ExplainRule {
                    explain_rule: er.clone(),
                },
                CompiledCommand::Abduce(aq) => LocyCommand::Abduce {
                    abduce_query: aq.clone(),
                },
                CompiledCommand::Assume(ca) => LocyCommand::Assume {
                    compiled_assume: ca.clone(),
                },
                CompiledCommand::DeriveCommand(dc) => LocyCommand::Derive {
                    derive_command: dc.clone(),
                },
                CompiledCommand::Cypher(q) => LocyCommand::Cypher { query: q.clone() },
                CompiledCommand::Calibrate(cc) => {
                    let inputs = model_catalog
                        .get(&cc.model_name)
                        .map(|m| m.inputs.clone())
                        .unwrap_or_default();
                    LocyCommand::Calibrate {
                        calibrate: cc.clone(),
                        model_inputs: inputs,
                    }
                }
                CompiledCommand::Validate(cv) => LocyCommand::Validate {
                    validate: cv.clone(),
                },
            })
            .collect()
    }

    // -- Stratum --------------------------------------------------------

    fn build_stratum(
        &self,
        stratum: &Stratum,
        rule_catalog: &HashMap<String, CompiledRule>,
        stratum_rule_names: &HashSet<String>,
        prob_config: ProbabilityConfig,
        classifiers: &ClassifierContext,
    ) -> Result<LocyStratum> {
        let mut rules = Vec::with_capacity(stratum.rules.len());
        for rule in &stratum.rules {
            // Generator predicates (`name(args) -> (outs)`) bind new variables
            // 1:N and are resolved by the in-memory SLG engine
            // (`locy_slg::apply_generators`), not the columnar fixpoint — which has
            // no row-explosion operator. Skip such rules here so the fixpoint never
            // plans their generator-bound yield columns (which are not produced by
            // the MATCH body); QUERY resolution re-derives them through SLG, and
            // the non-FOLD columnar output would be discarded regardless.
            let has_generator = rule.clauses.iter().any(|c| {
                c.where_conditions
                    .iter()
                    .any(|cond| matches!(cond, RuleCondition::Generator(_)))
            });
            if has_generator {
                continue;
            }
            rules.push(self.build_rule(
                rule,
                stratum.is_recursive,
                stratum_rule_names,
                rule_catalog,
                prob_config,
                classifiers,
            )?);
        }

        Ok(LocyStratum {
            id: stratum.id,
            rules,
            is_recursive: stratum.is_recursive,
            depends_on: stratum.depends_on.clone(),
        })
    }

    // -- Rule -----------------------------------------------------------

    fn build_rule(
        &self,
        rule: &CompiledRule,
        is_recursive: bool,
        stratum_rule_names: &HashSet<String>,
        rule_catalog: &HashMap<String, CompiledRule>,
        prob_config: ProbabilityConfig,
        classifiers: &ClassifierContext,
    ) -> Result<LocyRulePlan> {
        // Collect node variable names from match patterns for VID-based joins
        let node_vars = collect_node_vars(&rule.clauses);

        let mut clauses = Vec::with_capacity(rule.clauses.len());
        for clause in &rule.clauses {
            clauses.push(self.build_clause(
                clause,
                &rule.yield_schema,
                is_recursive,
                ClauseCtx {
                    stratum_rule_names,
                    rule_catalog,
                    node_vars: &node_vars,
                },
                prob_config,
                classifiers,
            )?);
        }

        // All clauses share the same schema; derive metadata from first clause
        let first_clause = rule.clauses.first();
        // HAVING / BEST BY / FOLD-output metadata must come from the SAME clause
        // that provides the FOLD (mirroring `fold_bindings` below), not clause 0:
        // a base clause (no FOLD) followed by a recursive clause with
        // `FOLD ... HAVING ... BEST BY` would otherwise silently lose its HAVING
        // filter and BEST BY pruning. Falls back to clause 0 when no clause folds.
        let fold_clause = rule
            .clauses
            .iter()
            .find(|c| !c.fold.is_empty())
            .or(first_clause);

        // Collect fold bindings from the first clause that has them.
        // A rule may have a base clause (no FOLD) plus recursive clauses (with FOLD);
        // we need the FOLD metadata from whichever clause provides it.
        //
        // Each entry is (fold_name, yield_alias, aggregate_expr). The yield_alias
        // is the output column name from the YIELD clause (may differ from fold_name
        // when the user writes e.g. `YIELD ... n AS support`).
        let fold_bindings: Vec<(String, String, Expr)> = rule
            .clauses
            .iter()
            .find(|c| !c.fold.is_empty())
            .map(|c| {
                // Build fold_name → yield alias mapping from the YIELD items.
                let yield_alias_map: HashMap<&str, &str> = match &c.output {
                    RuleOutput::Yield(yc) => yc
                        .items
                        .iter()
                        .filter_map(|item| {
                            if let Expr::Variable(ref v) = item.expr {
                                item.alias.as_deref().map(|alias| (v.as_str(), alias))
                            } else {
                                None
                            }
                        })
                        .collect(),
                    _ => HashMap::new(),
                };
                c.fold
                    .iter()
                    .map(|fb| {
                        let alias = yield_alias_map
                            .get(fb.name.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| fb.name.clone());
                        (fb.name.clone(), alias, fb.aggregate.clone())
                    })
                    .collect()
            })
            .unwrap_or_default();
        // Build fold_name → yield_alias substitution map for HAVING / BEST BY
        // expressions that reference FOLD outputs by their original name.
        let fold_alias_subs: HashMap<String, String> = fold_bindings
            .iter()
            .filter(|(name, alias, _)| name != alias)
            .map(|(name, alias, _)| (name.clone(), alias.clone()))
            .collect();

        let having = fold_clause
            .map(|c| {
                c.having
                    .iter()
                    .map(|expr| substitute_fold_aliases(expr.clone(), &fold_alias_subs))
                    .collect()
            })
            .unwrap_or_default();
        let best_by_criteria = fold_clause
            .and_then(|c| c.best_by.as_ref())
            .map(|bb| {
                bb.items
                    .iter()
                    .map(|item| {
                        (
                            substitute_fold_aliases(item.expr.clone(), &fold_alias_subs),
                            item.ascending,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();

        let fold_output_names: HashSet<&str> = fold_clause
            .map(|c| c.fold.iter().map(|fb| fb.name.as_str()).collect())
            .unwrap_or_default();
        let along_names: HashSet<&str> = first_clause
            .map(|c| c.along.iter().map(|a| a.name.as_str()).collect())
            .unwrap_or_default();
        let var_labels = first_clause.map(clause_var_labels).unwrap_or_default();
        let graph_schema = self.planner.schema();

        let yield_schema: Vec<LocyYieldColumn> = rule
            .yield_schema
            .iter()
            .map(|yc| {
                let data_type = match first_clause {
                    Some(fc) => infer_yield_type(
                        &yc.name,
                        fc,
                        &node_vars,
                        &fold_output_names,
                        &along_names,
                        rule_catalog,
                        yc.is_key,
                        graph_schema,
                        &var_labels,
                    ),
                    None => DataType::LargeUtf8,
                };
                LocyYieldColumn {
                    name: yc.name.clone(),
                    is_key: yc.is_key,
                    is_prob: yc.is_prob,
                    data_type,
                }
            })
            .collect();

        // Post-fold YIELD projection specs.
        //
        // A YIELD expression that references a FOLD output but is not a bare
        // fold-output variable (e.g. `total * 2.0 AS score`) cannot be produced
        // in the pre-fold body — the aggregate does not exist there. Record the
        // full yield-column projection so it can run POST-fold (`apply_post_fold_
        // projection`). Only populated when at least one column is such a
        // computed expression; otherwise the common path is unchanged (FoldExec
        // output already matches `yield_schema`).
        let yield_item_exprs: HashMap<String, &Expr> = match first_clause.map(|c| &c.output) {
            Some(RuleOutput::Yield(yc)) => resolve_yield_column_names(&yc.items)
                .into_iter()
                .zip(yc.items.iter())
                .map(|(name, item)| (name, &item.expr))
                .collect(),
            _ => HashMap::new(),
        };
        let has_computed_fold_expr = yield_schema.iter().any(|yc| {
            !yc.is_key
                && yield_item_exprs.get(&yc.name).is_some_and(|e| {
                    expr_references_fold_output(e, &fold_output_names)
                        && !matches!(e, Expr::Variable(v) if fold_output_names.contains(v.as_str()))
                })
        });
        let yield_projection: Vec<(String, Expr)> = if has_computed_fold_expr {
            yield_schema
                .iter()
                .map(|yc| {
                    // KEY and bare fold-output columns are already present in the
                    // post-fold batch under their yield name — pass them through.
                    // Computed expressions over fold outputs are evaluated, with
                    // fold-var → alias substitution so references match the
                    // post-fold column names (`FoldBinding.output_name`).
                    let expr = if yc.is_key {
                        Expr::Variable(yc.name.clone())
                    } else {
                        match yield_item_exprs.get(&yc.name) {
                            Some(e) => substitute_fold_aliases((*e).clone(), &fold_alias_subs),
                            None => Expr::Variable(yc.name.clone()),
                        }
                    };
                    (yc.name.clone(), expr)
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(LocyRulePlan {
            name: rule.name.clone(),
            clauses,
            yield_schema,
            priority: rule.priority,
            fold_bindings,
            having,
            best_by_criteria,
            yield_projection,
        })
    }

    // -- Clause ---------------------------------------------------------

    fn build_clause(
        &self,
        clause: &CompiledClause,
        yield_cols: &[YieldColumn],
        is_recursive: bool,
        ctx: ClauseCtx<'_>,
        _prob_config: ProbabilityConfig,
        classifiers: &ClassifierContext,
    ) -> Result<LocyClausePlan> {
        let stratum_rule_names = ctx.stratum_rule_names;
        let rule_catalog = ctx.rule_catalog;
        let node_vars = ctx.node_vars;
        let classifier_registry = Arc::clone(&classifiers.registry);
        let classifier_cache = classifiers.cache.as_ref().map(Arc::clone);
        let classifier_provenance_store = classifiers.provenance_store.as_ref().map(Arc::clone);

        // Reject non-monotone FOLD aggregates in recursive strata using the
        // plugin registry's Semilattice metadata. Defense in depth: the
        // uni-locy typecheck pass usually rejects this upstream, but
        // direct LocyPlanBuilder consumers (in-process tests, future API
        // surfaces) might bypass that pass.
        if is_recursive {
            for fold in &clause.fold {
                let fname = match &fold.aggregate {
                    uni_cypher::ast::Expr::FunctionCall { name, .. } => name.clone(),
                    _ => {
                        anyhow::bail!(
                            "FOLD '{}' aggregate must be a function call (e.g., SUM(x))",
                            fold.name
                        );
                    }
                };
                match crate::query::df_graph::locy_fold::is_monotonic_aggregate(
                    &self.plugin_registry,
                    &fname,
                ) {
                    Some(true) => {}
                    Some(false) | None => {
                        anyhow::bail!(
                            "non-monotonic aggregate '{}' in recursive rule clause (FOLD '{}')",
                            fname,
                            fold.name
                        );
                    }
                }
            }
        }

        // Collect node variables from THIS clause's MATCH pattern only.
        // Used for IS-ref predicates: only variables in the current MATCH
        // have expanded {var}._vid columns in the graph scan output.
        let mut clause_node_vars = HashSet::new();
        collect_match_node_vars(clause, &mut clause_node_vars);

        // Step 1: MATCH pattern → Scan→Traverse chain
        let mut plan = self
            .planner
            .plan_pattern(&clause.match_pattern, &[])
            .context("planning MATCH pattern")?;

        // Step 2: WHERE filter (non-IS conditions only).
        // Conditions referencing IS-ref-introduced variables must be deferred
        // until after the IS-ref CrossJoin resolves. This includes:
        //   - Target variables (e.g., `pub` from `c IS rule TO pub`)
        //   - Non-KEY value columns from the target rule's yield schema
        //     (e.g., `relevance` from a rule that YIELDs KEY c, KEY pub, relevance)
        let mut is_ref_deferred_vars: HashSet<String> = HashSet::new();
        for cond in &clause.where_conditions {
            if let RuleCondition::IsReference(ir) = cond
                && !ir.negated
            {
                if let Some(target) = &ir.target {
                    is_ref_deferred_vars.insert(target.clone());
                }
                let target_rule_name = ir.rule_name.to_string();
                if let Some(target_rule) = rule_catalog.get(&target_rule_name) {
                    for col in &target_rule.yield_schema {
                        if !col.is_key {
                            is_ref_deferred_vars.insert(col.name.clone());
                        }
                    }
                }
            }
        }

        let all_filter_exprs: Vec<&Expr> = clause
            .where_conditions
            .iter()
            .filter_map(|c| match c {
                RuleCondition::Expression(e) => Some(e),
                _ => None,
            })
            .collect();

        let (deferred_filter_exprs, immediate_filter_exprs): (Vec<&Expr>, Vec<&Expr>) =
            if is_ref_deferred_vars.is_empty() {
                (Vec::new(), all_filter_exprs)
            } else {
                all_filter_exprs
                    .into_iter()
                    .partition(|e| expr_references_any(e, &is_ref_deferred_vars))
            };

        if !immediate_filter_exprs.is_empty() {
            let predicate = combine_with_and(&immediate_filter_exprs);
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
                optional_variables: HashSet::new(),
            };
        }

        // Step 3: IS-reference joins
        let mut is_refs = Vec::new();
        let mut along_bindings = Vec::new();
        let mut positive_is_ref_occurrence: usize = 0;

        // Bare→aliased name map for non-KEY value columns of non-first positive
        // IS-refs (whose scan columns get an `__isref{n}_` prefix). Used to
        // rewrite FOLD inputs / YIELD exprs / deferred WHERE filters that
        // reference those columns by bare name. `bare_is_ref_cols` tracks names
        // already exposed unprefixed (by an occurrence-0 ref) so they keep
        // shadowing later identically named columns.
        let mut is_ref_col_aliases: HashMap<String, String> = HashMap::new();
        let mut bare_is_ref_cols: HashSet<String> = HashSet::new();

        for condition in &clause.where_conditions {
            if let RuleCondition::IsReference(is_ref) = condition {
                let target_rule_name = is_ref.rule_name.to_string();

                // Validate: rule exists in catalog
                let target_rule = rule_catalog.get(&target_rule_name).with_context(|| {
                    format!("IS-reference to unknown rule '{}'", target_rule_name)
                })?;

                // Validate: arity — subjects bind to KEY columns positionally.
                // The optional target binds to the next KEY column (if any remain)
                // or the first non-KEY column. So subjects must not exceed KEY count.
                let key_count = target_rule
                    .yield_schema
                    .iter()
                    .filter(|yc| yc.is_key)
                    .count();
                if is_ref.subjects.len() > key_count {
                    bail!(
                        "IS-reference to '{}': arity mismatch — {} subjects \
                         provided but rule has only {} KEY columns",
                        target_rule_name,
                        is_ref.subjects.len(),
                        key_count,
                    );
                }

                let is_self_ref = stratum_rule_names.contains(&target_rule_name);
                let handle = self.get_or_create_derived_scan_handle(
                    &target_rule_name,
                    target_rule,
                    is_self_ref,
                    rule_catalog,
                );

                // Look up target rule's PROB column (if any)
                let (target_has_prob, target_prob_col) = rule_catalog
                    .get(&target_rule_name)
                    .and_then(|r| {
                        r.yield_schema
                            .iter()
                            .find(|c| c.is_prob)
                            .map(|c| (true, Some(c.name.clone())))
                    })
                    .unwrap_or((false, None));

                // Build LocyIsRef metadata
                let locy_is_ref = LocyIsRef {
                    rule_name: target_rule_name.clone(),
                    subjects: is_ref
                        .subjects
                        .iter()
                        .map(|s| Expr::Variable(s.clone()))
                        .collect(),
                    target: is_ref.target.as_ref().map(|t| Expr::Variable(t.clone())),
                    negated: is_ref.negated,
                    target_has_prob,
                    target_prob_col,
                };
                is_refs.push(locy_is_ref);

                // Non-negated: CrossJoin + Filter (inner join semantics)
                // Negated: no plan nodes — FixpointExec handles anti-join
                if !is_ref.negated {
                    // Each positive IS-ref joins a derived scan whose columns
                    // are named after the target rule's yield schema. From the
                    // second positive ref onward those names can collide with
                    // an earlier scan's (always, for two refs to the same
                    // rule), and the unqualified join predicate would silently
                    // resolve against the FIRST scan's columns — contradictory
                    // predicates, empty result. Alias every scan after the
                    // first with a per-occurrence prefix and point its
                    // predicates at the aliased names. The clause's final
                    // yield projection drops non-yield columns, so aliased
                    // names never leak into derived facts.
                    let col_prefix = if positive_is_ref_occurrence == 0 {
                        String::new()
                    } else {
                        format!("__isref{}_", positive_is_ref_occurrence)
                    };
                    positive_is_ref_occurrence += 1;

                    // Record bare→aliased names for this ref's non-KEY value
                    // columns so later FOLD/YIELD/WHERE references resolve to
                    // the prefixed scan column. First-occurrence wins: an
                    // unprefixed (occurrence-0) column shadows identically
                    // named later columns, matching join-predicate resolution.
                    for col in target_rule.yield_schema.iter().filter(|c| !c.is_key) {
                        if col_prefix.is_empty() {
                            bare_is_ref_cols.insert(col.name.clone());
                        } else if !bare_is_ref_cols.contains(&col.name)
                            && !is_ref_col_aliases.contains_key(&col.name)
                        {
                            is_ref_col_aliases
                                .insert(col.name.clone(), format!("{col_prefix}{}", col.name));
                        }
                    }

                    let scan_schema = if col_prefix.is_empty() {
                        handle.schema.clone()
                    } else {
                        alias_derived_schema(&handle.schema, &col_prefix)
                    };
                    let derived_scan = LogicalPlan::LocyDerivedScan {
                        scan_index: handle.scan_index,
                        data: handle.data.clone(),
                        schema: scan_schema,
                    };
                    plan = LogicalPlan::CrossJoin {
                        left: Box::new(plan),
                        right: Box::new(derived_scan),
                    };

                    // Subject-only predicate: n._vid = a (target handled separately).
                    let predicate = build_is_ref_predicate(
                        &is_ref.subjects,
                        &None,
                        &target_rule.yield_schema,
                        &clause_node_vars,
                        &col_prefix,
                    )?;
                    plan = LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate,
                        optional_variables: HashSet::new(),
                    };

                    // If the IS-ref has a target variable, bind it to the derived
                    // scan's target column. When the target var name differs from
                    // the derived column name (e.g., `m` vs `b`), add an implicit
                    // ScanAll for the target so it becomes a proper node column.
                    if let Some(target_var) = &is_ref.target {
                        let key_cols: Vec<&YieldColumn> = target_rule
                            .yield_schema
                            .iter()
                            .filter(|yc| yc.is_key)
                            .collect();
                        let non_key_cols: Vec<&YieldColumn> = target_rule
                            .yield_schema
                            .iter()
                            .filter(|yc| !yc.is_key)
                            .collect();
                        let target_col_name = if is_ref.subjects.len() < key_cols.len() {
                            key_cols.get(is_ref.subjects.len()).map(|c| c.name.clone())
                        } else {
                            non_key_cols.first().map(|c| c.name.clone())
                        };

                        if let Some(col_name) = target_col_name {
                            // Materialize `target_var` as a proper node (with
                            // `._vid`, `._labels`, and property columns) the
                            // FIRST time it appears, so property access (e.g.
                            // `b.embedding`) works and later subjects can use it.
                            // If the target is already a node variable — a
                            // MATCH-bound var, or the shared target of an earlier
                            // IS-ref (`... TO ce, ... TO ce`) — do NOT re-scan it:
                            // a second `ScanAll` cross-joins the node with itself,
                            // inflating any aggregate over the joined value columns
                            // (e.g. `MPROD(mapping_conf)` over the cartesian square
                            // of the shared element set).
                            if !clause_node_vars.contains(target_var) {
                                let target_node_scan = LogicalPlan::ScanAll {
                                    variable: target_var.clone(),
                                    filter: None,
                                    optional: false,
                                };
                                plan = LogicalPlan::CrossJoin {
                                    left: Box::new(plan),
                                    right: Box::new(target_node_scan),
                                };
                            }
                            // Bind: target_var._vid = derived_col (UInt64
                            // equality). For an already-bound target this ties
                            // this ref's derived scan to the same node the earlier
                            // ref bound, turning a shared `TO ce` into a join
                            // constraint rather than a fresh cross product.
                            let target_binding = Expr::BinaryOp {
                                left: Box::new(Expr::Variable(format!("{}._vid", target_var))),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Variable(format!("{col_prefix}{col_name}"))),
                            };
                            plan = LogicalPlan::Filter {
                                input: Box::new(plan),
                                predicate: target_binding,
                                optional_variables: HashSet::new(),
                            };
                            // Record the target as a node variable so chained
                            // IS-refs later in this clause can use it as a subject
                            // (`x IS r TO mid, mid IS r TO z`).
                            clause_node_vars.insert(target_var.clone());
                        }
                    }
                }
            }
        }

        // Step 3.5: Apply deferred WHERE conditions (those referencing IS-ref target vars).
        if !deferred_filter_exprs.is_empty() {
            let predicate = combine_with_and(&deferred_filter_exprs);
            // Rewrite bare references to non-first IS-refs' value columns to
            // their aliased scan names (e.g. `relevance` → `__isref1_relevance`).
            let predicate = rewrite_is_ref_cols(predicate, &is_ref_col_aliases);
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
                optional_variables: HashSet::new(),
            };
        }

        // Collect ALONG binding names
        for along in &clause.along {
            along_bindings.push(along.name.clone());
        }

        // Validate ALONG prev-references against available yield schemas
        if !clause.along.is_empty() {
            let available_prev_fields: HashSet<&str> = is_refs
                .iter()
                .flat_map(|ir| {
                    rule_catalog
                        .get(&ir.rule_name)
                        .map(|r| {
                            r.yield_schema
                                .iter()
                                .map(|yc| yc.name.as_str())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect();

            for along in &clause.along {
                validate_prev_refs(&along.expr, &available_prev_fields)?;
            }
        }

        // Steps 4+5+6: ALONG + YIELD + PRIORITY as a single projection
        let along_map: HashMap<&str, &LocyExpr> = clause
            .along
            .iter()
            .map(|a| (a.name.as_str(), &a.expr))
            .collect();

        // Build FOLD output name → aggregate input expression mapping.
        // FOLD outputs (e.g., `total` from `FOLD total = SUM(r.amount)`) are not
        // columns in the MATCH output; the clause projection must include the
        // aggregate's input expression (e.g., `r.amount`) so the FOLD operator
        // can find it later.
        let fold_output_names: HashSet<&str> =
            clause.fold.iter().map(|fb| fb.name.as_str()).collect();
        let fold_input_map: HashMap<&str, &Expr> = clause
            .fold
            .iter()
            .filter_map(|fb| {
                // Extract the first argument from the aggregate function call
                if let Expr::FunctionCall { args, .. } = &fb.aggregate {
                    args.first().map(|arg| (fb.name.as_str(), arg))
                } else {
                    None
                }
            })
            .collect();

        // Build yield name → original expression mapping from clause output.
        // This preserves literals (`'low' AS label`) and property accesses
        // (`e.cost AS cost`) that would otherwise be lost as bare Variable lookups.
        let yield_expr_map: HashMap<String, &Expr> = match &clause.output {
            RuleOutput::Yield(yc) => resolve_yield_column_names(&yc.items)
                .into_iter()
                .zip(yc.items.iter())
                .map(|(name, item)| (name, &item.expr))
                .collect(),
            _ => HashMap::new(),
        };

        let along_names_set: HashSet<&str> = clause.along.iter().map(|a| a.name.as_str()).collect();

        // Pre-compute rewritten ALONG expressions for variable substitution.
        // When a YIELD expression references an ALONG name (e.g., `ew * 2.0 AS score`
        // where `ALONG ew = e.weight`), the Variable("ew") must be replaced with the
        // underlying expression Property("e", "weight") because "ew" is not a column
        // in the input plan schema.
        let rewritten_along: HashMap<&str, Expr> = along_map
            .iter()
            .filter_map(|(&name, locy_expr)| rewrite_locy_expr(locy_expr).ok().map(|e| (name, e)))
            .collect();

        let mut projections = Vec::new();
        let mut target_types = Vec::new();
        let var_labels = clause_var_labels(clause);
        let graph_schema = self.planner.schema();
        for yc in yield_cols {
            let expr = if let Some(locy_expr) = along_map.get(yc.name.as_str()) {
                rewrite_locy_expr(locy_expr)?
            } else if fold_output_names.contains(yc.name.as_str()) {
                // FOLD output column — produced by FoldExec after aggregation.
                // Its input column is projected separately (see the FOLD-input
                // pass below) AS the FOLD variable name, independent of any
                // YIELD alias, so the runtime always resolves it by name.
                continue;
            } else if let Some(orig_expr) = yield_expr_map.get(&yc.name) {
                let e = (*orig_expr).clone();
                let e = substitute_along_vars(e, &rewritten_along);
                // A YIELD expression that references a FOLD output must NOT be
                // projected in the pre-fold body: a bare fold-output variable
                // (`n AS support`) is produced directly by FoldExec, and a
                // computed expression over fold outputs (`total * 2.0 AS score`)
                // is produced by the post-fold projection stage. Either way the
                // aggregate does not exist here yet.
                if expr_references_fold_output(&e, &fold_output_names) {
                    continue;
                }
                e
            } else {
                let e = Expr::Variable(yc.name.clone());
                substitute_along_vars(e, &rewritten_along)
            };
            // Resolve bare references to non-first IS-refs' aliased value
            // columns (covers FOLD aggregate inputs and plain YIELD exprs).
            let expr = rewrite_is_ref_cols(expr, &is_ref_col_aliases);
            projections.push((expr, Some(yc.name.clone())));
            target_types.push(infer_yield_type(
                &yc.name,
                clause,
                node_vars,
                &fold_output_names,
                &along_names_set,
                rule_catalog,
                yc.is_key,
                graph_schema,
                &var_labels,
            ));
        }

        // FOLD-input projection (issue #145 root fix).
        //
        // Every aggregate's argument column must be present in the body batch
        // under the FOLD variable name (`fb.name`) — that is exactly the name
        // `convert_fold_bindings` records in `FoldBinding::input_col_name` and
        // the runtime (`FoldExec`, `FixpointExec`) resolves by. Projecting the
        // input here, keyed on the FOLD variable rather than the YIELD column
        // name, makes resolution independent of whether YIELD renames the
        // aggregate (`FOLD total = SUM(e.x) ... YIELD ... total AS sum_out`),
        // which is the root cause of the silent-zeroing corruption in #145.
        // The aggregated *output* column (named by the YIELD alias) is produced
        // later by FoldExec; it must not be projected here.
        for fb in &clause.fold {
            let Some(fold_input) = fold_input_map.get(fb.name.as_str()) else {
                // COUNTALL / COUNT(*) has no input column to carry through.
                continue;
            };
            // Mirror the substitutions the YIELD path applies: inline ALONG
            // bindings, then rewrite non-first IS-ref value columns.
            let expr = substitute_along_vars((*fold_input).clone(), &rewritten_along);
            let expr = rewrite_is_ref_cols(expr, &is_ref_col_aliases);
            projections.push((expr, Some(fb.name.clone())));
            target_types.push(infer_yield_type(
                &fb.name,
                clause,
                node_vars,
                &fold_output_names,
                &along_names_set,
                rule_catalog,
                false, // a FOLD input is never a KEY column
                graph_schema,
                &var_labels,
            ));
        }

        // Add __priority literal column if present
        if let Some(priority) = clause.priority {
            projections.push((
                Expr::Literal(CypherLiteral::Integer(priority)),
                Some("__priority".to_string()),
            ));
            target_types.push(DataType::Int64);
        }

        // Hidden YIELD items emitted by `extract_model_invocations` for
        // property feature exprs (e.g. `scorer(s.tier)`). These columns
        // (named `__feat_<var>_<prop>`) flow through the body batch so
        // `apply_model_invocations` can read property values per row;
        // `record_batches_to_locy_rows` strips them by prefix before
        // returning the user-visible rows.
        //
        // We deliberately push to `projections` AFTER `target_types` is
        // already populated for the user-visible columns + priority.
        // `plan_locy_project` reads `target_types.get(i)` and skips
        // coercion when the entry is absent (returns `None`); leaving
        // these out preserves the property's native storage type
        // (Utf8 / LargeBinary / Int64 / Float64 / etc.) end-to-end
        // into `apply_model_invocations`.
        for hidden in &clause.hidden_yield_cols {
            if let Some(orig_expr) = yield_expr_map.get(hidden) {
                let e = (*orig_expr).clone();
                let e = substitute_along_vars(e, &rewritten_along);
                let e = rewrite_is_ref_cols(e, &is_ref_col_aliases);
                projections.push((e, Some(hidden.clone())));
            }
        }

        // Phase B A4 follow-up: when the clause has neural-model
        // invocations (extracted from YIELD / ALONG / FOLD positions
        // and replaced with `Variable("__model_<n>_<idx>")`
        // references), wrap the pre-projection plan with
        // `LocyModelInvoke` so the synthesized columns exist in the
        // input schema of `LocyProject`. Downstream FOLD aggregates
        // also see those columns (FoldExec reads projected columns).
        if !clause.model_invocations.is_empty() {
            // Phase D D3 runtime: for every distinct source rule
            // referenced by a path-context invocation on this clause,
            // mint a `DerivedScanHandle` (non-self-ref → full cross-stratum
            // facts) and surface it as a `PathContextHandle` on the
            // logical plan node. The `data` Arc is shared with
            // `DerivedScanRegistry`, so the fixpoint loop's writes flow
            // through here without further plumbing.
            let mut path_context_handles: HashMap<
                String,
                super::df_graph::locy_model_invoke::PathContextHandle,
            > = HashMap::new();
            for inv in &clause.model_invocations {
                if let Some(pc) = &inv.path_context {
                    if path_context_handles.contains_key(&pc.source_rule) {
                        continue;
                    }
                    let target_rule = rule_catalog.get(&pc.source_rule).ok_or_else(|| {
                        anyhow::anyhow!(
                            "model '{}' path_context references undefined rule '{}'",
                            inv.model_name,
                            pc.source_rule
                        )
                    })?;
                    let handle = self.get_or_create_derived_scan_handle(
                        &pc.source_rule,
                        target_rule,
                        false,
                        rule_catalog,
                    );
                    path_context_handles.insert(
                        pc.source_rule.clone(),
                        super::df_graph::locy_model_invoke::PathContextHandle {
                            source_rule: pc.source_rule.clone(),
                            data: handle.data,
                            schema: handle.schema,
                        },
                    );
                }
            }
            plan = LogicalPlan::LocyModelInvoke {
                input: Box::new(plan),
                invocations: clause.model_invocations.clone(),
                classifier_registry: Arc::clone(&classifier_registry),
                classifier_cache: classifier_cache.as_ref().map(Arc::clone),
                classifier_provenance_store: classifier_provenance_store.as_ref().map(Arc::clone),
                path_context_handles,
            };
        }

        plan = LogicalPlan::LocyProject {
            input: Box::new(plan),
            projections,
            target_types,
        };

        // Step 7: Non-recursive FOLD — handled by apply_post_fixpoint_chain.
        //
        // Fold is always applied post-fixpoint (both recursive and non-recursive).
        // Wrapping the body with LocyFold here would double-apply the aggregate,
        // producing wrong results for COUNT/AVG where f(f(x)) ≠ f(x).

        // Step 8: BEST BY wrapping.
        //
        // Only for non-FOLD clauses. When the clause has a FOLD, BEST BY ranks on
        // the *aggregated* value and must run POST-fold — which the rule-level
        // pipeline already does (`merge_best_by` during fixpoint and the post-
        // fixpoint chain in `locy_fixpoint.rs`, using the alias-substituted
        // criteria). Wrapping the pre-fold body here would instead rank the raw
        // per-row input column, pruning rows before aggregation and corrupting
        // the result (and, for a renamed aggregate, referencing a column that
        // only exists pre-fold). There is no sensible pre-fold reading of
        // "best by an aggregate", so skip it for FOLD clauses.
        if let Some(best_by) = clause.best_by.as_ref().filter(|_| clause.fold.is_empty()) {
            let key_columns: Vec<String> = yield_cols
                .iter()
                .filter(|yc| yc.is_key)
                .map(|yc| yc.name.clone())
                .collect();

            let criteria: Vec<(Expr, bool)> = best_by
                .items
                .iter()
                .map(|item| (item.expr.clone(), item.ascending))
                .collect();

            plan = LogicalPlan::LocyBestBy {
                input: Box::new(plan),
                key_columns,
                criteria,
            };
        }

        Ok(LocyClausePlan {
            body: plan,
            is_refs,
            along_bindings,
            priority: clause.priority,
            model_invocations: clause.model_invocations.clone(),
        })
    }

    // -- DerivedScanHandle management -----------------------------------

    /// Get or create a derived scan handle for a rule.
    ///
    /// Handles are keyed by `(rule_name, is_self_ref)` — a self-referential
    /// scan (receives delta data) and a cross-stratum scan (receives full facts)
    /// for the same rule need separate handles.
    fn get_or_create_derived_scan_handle(
        &self,
        rule_name: &str,
        target_rule: &CompiledRule,
        is_self_ref: bool,
        rule_catalog: &HashMap<String, CompiledRule>,
    ) -> DerivedScanHandle {
        let mut handles = self.derived_scan_handles.borrow_mut();

        // Reuse existing handle with same (rule_name, is_self_ref)
        if let Some(handle) = handles
            .iter()
            .find(|h| h.rule_name == rule_name && h.is_self_ref == is_self_ref)
        {
            return handle.clone();
        }

        let scan_index = handles.len();
        let schema =
            yield_schema_to_arrow_from_rule(target_rule, rule_catalog, self.planner.schema());
        let data = Arc::new(RwLock::new(Vec::new()));
        let handle = DerivedScanHandle {
            rule_name: rule_name.to_string(),
            scan_index,
            is_self_ref,
            data,
            schema,
        };
        handles.push(handle.clone());
        handle
    }

    /// Convert internal handles into a [`DerivedScanRegistry`].
    fn build_registry(&self) -> DerivedScanRegistry {
        let handles = self.derived_scan_handles.borrow();
        let mut registry = DerivedScanRegistry::new();
        for handle in handles.iter() {
            registry.add(DerivedScanEntry {
                scan_index: handle.scan_index,
                rule_name: handle.rule_name.clone(),
                is_self_ref: handle.is_self_ref,
                data: handle.data.clone(),
                schema: handle.schema.clone(),
            });
        }
        registry
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Combine multiple expressions with AND.
fn combine_with_and(exprs: &[&Expr]) -> Expr {
    assert!(!exprs.is_empty());
    let mut result = exprs[0].clone();
    for expr in &exprs[1..] {
        result = Expr::BinaryOp {
            left: Box::new(result),
            op: BinaryOp::And,
            right: Box::new((*expr).clone()),
        };
    }
    result
}

/// Build the join predicate for an IS-reference.
///
/// Returns true if `e` or any sub-expression references a variable in `vars`.
///
/// Used to partition WHERE conditions into those that can be applied before
/// IS-ref joins (immediate) vs those that must wait until after (deferred),
/// when conditions reference IS-ref target variables not yet in the plan.
fn expr_references_any(e: &Expr, vars: &HashSet<String>) -> bool {
    match e {
        Expr::Variable(v) => vars.contains(v.as_str()),
        Expr::Property(inner, _) => expr_references_any(inner, vars),
        Expr::BinaryOp { left, right, .. } => {
            expr_references_any(left, vars) || expr_references_any(right, vars)
        }
        Expr::UnaryOp { expr, .. } => expr_references_any(expr, vars),
        Expr::FunctionCall { args, .. } => args.iter().any(|a| expr_references_any(a, vars)),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::IsUnique(inner) => {
            expr_references_any(inner, vars)
        }
        Expr::In { expr, list } => {
            expr_references_any(expr, vars) || expr_references_any(list, vars)
        }
        Expr::List(exprs) => exprs.iter().any(|a| expr_references_any(a, vars)),
        Expr::Map(entries) => entries.iter().any(|(_, a)| expr_references_any(a, vars)),
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            expr.as_deref()
                .is_some_and(|a| expr_references_any(a, vars))
                || when_then
                    .iter()
                    .any(|(w, t)| expr_references_any(w, vars) || expr_references_any(t, vars))
                || else_expr
                    .as_deref()
                    .is_some_and(|a| expr_references_any(a, vars))
        }
        Expr::ArrayIndex { array, index } => {
            expr_references_any(array, vars) || expr_references_any(index, vars)
        }
        Expr::ArraySlice { array, start, end } => {
            expr_references_any(array, vars)
                || start
                    .as_deref()
                    .is_some_and(|a| expr_references_any(a, vars))
                || end.as_deref().is_some_and(|a| expr_references_any(a, vars))
        }
        _ => false,
    }
}

/// Rename every field of a derived scan's schema with `prefix`.
///
/// Used for the second and later positive IS-refs of a clause so their
/// columns cannot collide with an earlier scan's identically named yield
/// columns (see the aliasing comment in `build_clause` Step 3).
fn alias_derived_schema(schema: &SchemaRef, prefix: &str) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            f.as_ref()
                .clone()
                .with_name(format!("{prefix}{}", f.name()))
        })
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// Maps subjects → KEY yield columns by position, and target → remaining KEY
/// or first non-KEY yield column. For node variables, compares `._vid` property
/// (UInt64) instead of bare variable (which doesn't exist as a column).
///
/// `col_prefix` is the derived scan's column alias prefix (empty for the
/// clause's first positive IS-ref); the yield-column side of every predicate
/// is prefixed so it resolves against the right scan.
fn build_is_ref_predicate(
    subjects: &[String],
    target: &Option<String>,
    yield_schema: &[YieldColumn],
    node_vars: &HashSet<String>,
    col_prefix: &str,
) -> Result<Expr> {
    let key_cols: Vec<&YieldColumn> = yield_schema.iter().filter(|yc| yc.is_key).collect();
    let non_key_cols: Vec<&YieldColumn> = yield_schema.iter().filter(|yc| !yc.is_key).collect();

    let mut predicates = Vec::new();

    /// Build the expression for a variable in an IS-ref predicate.
    /// Node variables use `var._vid` as a raw column reference (UInt64);
    /// scalars use bare `var`.
    fn make_var_expr(var_name: &str, node_vars: &HashSet<String>) -> Expr {
        if node_vars.contains(var_name) {
            // Use "var._vid" as a variable name — Column::from_name won't split
            // on the dot, so DataFusion resolves this to the physical `var._vid`
            // column from the graph scan (UInt64). This baked form compiles
            // correctly both as a whole predicate and as an individually
            // extracted equi-join key, which lets the physical planner recover a
            // HashJoinExec for the IS-ref join: `collect_plan_variables` registers
            // this exact `var._vid` name for node scans, so `classify_join_predicate`
            // recognizes the conjunct as a cross-side equi-pair (#131).
            Expr::Variable(format!("{}._vid", var_name))
        } else {
            Expr::Variable(var_name.to_string())
        }
    }

    // subjects[i] = key_cols[i]
    for (i, subject) in subjects.iter().enumerate() {
        let key_col = key_cols.get(i).with_context(|| {
            format!(
                "IS-ref subject index {} exceeds KEY column count {}",
                i,
                key_cols.len()
            )
        })?;
        predicates.push(Expr::BinaryOp {
            left: Box::new(make_var_expr(subject, node_vars)),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Variable(format!("{col_prefix}{}", key_col.name))),
        });
    }

    // target: bind to remaining KEY column (after subjects) or first non-KEY
    if let Some(target_var) = target {
        let target_col = if subjects.len() < key_cols.len() {
            Some(key_cols[subjects.len()])
        } else {
            non_key_cols.first().copied()
        };
        if let Some(col) = target_col {
            predicates.push(Expr::BinaryOp {
                left: Box::new(make_var_expr(target_var, node_vars)),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Variable(format!("{col_prefix}{}", col.name))),
            });
        }
    }

    if predicates.is_empty() {
        bail!("IS-ref predicate requires at least one subject binding");
    }

    Ok(combine_with_and(&predicates.iter().collect::<Vec<_>>()))
}

/// Rewrite a [`LocyExpr`] into a Cypher [`Expr`].
///
/// `PrevRef(field)` becomes `Expr::Variable(field)` — the column is available
/// directly from the derived scan output after the CrossJoin.
pub(crate) fn rewrite_locy_expr(expr: &LocyExpr) -> Result<Expr> {
    match expr {
        LocyExpr::PrevRef(field) => Ok(Expr::Variable(field.clone())),
        LocyExpr::Cypher(e) => Ok(e.clone()),
        LocyExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_locy_expr(left)?),
            op: locy_op_to_cypher_op(op),
            right: Box::new(rewrite_locy_expr(right)?),
        }),
        LocyExpr::UnaryOp(op, inner) => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_locy_expr(inner)?),
        }),
    }
}

/// Recursively rename `Variable(name)` nodes that refer to a non-first
/// positive IS-ref's non-KEY value column to that column's per-occurrence
/// aliased name (`__isref{n}_name`).
///
/// The second and later positive IS-refs of a clause have their derived-scan
/// columns aliased with an `__isref{n}_` prefix (see `alias_derived_schema`
/// and the aliasing comment in `build_clause` Step 3). FOLD aggregate inputs,
/// YIELD expressions, and deferred WHERE filters that reference such a column
/// by its bare yield name — e.g. `MPROD(mapping_conf)` where `mapping_conf`
/// is yielded by a non-first `... IS element_mapped TO ce` — must be rewritten
/// to the prefixed name, otherwise they resolve against a field that does not
/// exist in the aliased scan schema and planning fails with a "No field named
/// …" DataFusion error.
fn rewrite_is_ref_cols(expr: Expr, aliases: &HashMap<String, String>) -> Expr {
    if aliases.is_empty() {
        return expr;
    }
    let recur = |e: Expr| rewrite_is_ref_cols(e, aliases);
    let boxed = |e: Box<Expr>| Box::new(rewrite_is_ref_cols(*e, aliases));
    match expr {
        Expr::Variable(ref name) if aliases.contains_key(name) => {
            Expr::Variable(aliases[name].clone())
        }
        Expr::Property(inner, prop) => Expr::Property(boxed(inner), prop),
        Expr::List(items) => Expr::List(items.into_iter().map(recur).collect()),
        Expr::Map(entries) => Expr::Map(entries.into_iter().map(|(k, v)| (k, recur(v))).collect()),
        Expr::FunctionCall {
            name,
            args,
            distinct,
            window_spec,
        } => Expr::FunctionCall {
            name,
            args: args.into_iter().map(recur).collect(),
            distinct,
            window_spec,
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: boxed(left),
            op,
            right: boxed(right),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op,
            expr: boxed(inner),
        },
        Expr::Case {
            expr: scrutinee,
            when_then,
            else_expr,
        } => Expr::Case {
            expr: scrutinee.map(boxed),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| (recur(w), recur(t)))
                .collect(),
            else_expr: else_expr.map(boxed),
        },
        Expr::IsNull(inner) => Expr::IsNull(boxed(inner)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(boxed(inner)),
        Expr::IsUnique(inner) => Expr::IsUnique(boxed(inner)),
        Expr::In { expr: e, list } => Expr::In {
            expr: boxed(e),
            list: boxed(list),
        },
        Expr::ArrayIndex { array, index } => Expr::ArrayIndex {
            array: boxed(array),
            index: boxed(index),
        },
        Expr::ArraySlice { array, start, end } => Expr::ArraySlice {
            array: boxed(array),
            start: start.map(boxed),
            end: end.map(boxed),
        },
        other => other,
    }
}

/// Exhaustively rewrite `Variable` nodes in an expression tree.
///
/// At each `Variable(name)`, replaces the node with `f(name)` when that returns
/// `Some`, leaving it unchanged otherwise. This is the single source of truth for
/// variable substitution in Locy planning: the ALONG-inlining and FOLD-alias
/// rewrites are thin wrappers over it. Recursing through *every* `Expr` variant
/// (not just `BinaryOp`/`UnaryOp`/`FunctionCall`) ensures a variable nested inside
/// `CASE`, `IN`, `IS [NOT] NULL`, a list, a map, an index, etc. is rewritten
/// rather than silently dropped — the root cause of the "No field named …" class.
///
/// The variant set below must stay in sync with [`rewrite_is_ref_cols`].
fn map_variables(expr: Expr, f: &dyn Fn(&str) -> Option<Expr>) -> Expr {
    let recur = |e: Expr| map_variables(e, f);
    let boxed = |e: Box<Expr>| Box::new(map_variables(*e, f));
    match expr {
        Expr::Variable(name) => f(&name).unwrap_or(Expr::Variable(name)),
        Expr::Property(inner, prop) => Expr::Property(boxed(inner), prop),
        Expr::List(items) => Expr::List(items.into_iter().map(recur).collect()),
        Expr::Map(entries) => Expr::Map(entries.into_iter().map(|(k, v)| (k, recur(v))).collect()),
        Expr::FunctionCall {
            name,
            args,
            distinct,
            window_spec,
        } => Expr::FunctionCall {
            name,
            args: args.into_iter().map(recur).collect(),
            distinct,
            window_spec,
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: boxed(left),
            op,
            right: boxed(right),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op,
            expr: boxed(inner),
        },
        Expr::Case {
            expr: scrutinee,
            when_then,
            else_expr,
        } => Expr::Case {
            expr: scrutinee.map(boxed),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| (recur(w), recur(t)))
                .collect(),
            else_expr: else_expr.map(boxed),
        },
        Expr::IsNull(inner) => Expr::IsNull(boxed(inner)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(boxed(inner)),
        Expr::IsUnique(inner) => Expr::IsUnique(boxed(inner)),
        Expr::In { expr: e, list } => Expr::In {
            expr: boxed(e),
            list: boxed(list),
        },
        Expr::ArrayIndex { array, index } => Expr::ArrayIndex {
            array: boxed(array),
            index: boxed(index),
        },
        Expr::ArraySlice { array, start, end } => Expr::ArraySlice {
            array: boxed(array),
            start: start.map(boxed),
            end: end.map(boxed),
        },
        other => other,
    }
}

/// Substitute `Variable(name)` nodes matching ALONG binding names with their
/// rewritten expressions, inlining the underlying expression.
///
/// Allows YIELD/FOLD expressions like `ew * 2.0` to reference an ALONG binding
/// (`ALONG ew = e.weight`) by inlining it to `e.weight * 2.0`. Exhaustive over
/// all `Expr` variants via [`map_variables`].
fn substitute_along_vars(expr: Expr, along: &HashMap<&str, Expr>) -> Expr {
    if along.is_empty() {
        return expr;
    }
    map_variables(expr, &|name| along.get(name).cloned())
}

/// Rename FOLD output variables to their YIELD aliases in HAVING / BEST BY exprs.
///
/// When a FOLD binding is aliased (`FOLD n = COUNT(*)` with `YIELD ... n AS support`),
/// HAVING and BEST BY reference `n` but after FoldExec the column is named `support`;
/// this rewrites `Variable("n")` → `Variable("support")`. Exhaustive over all `Expr`
/// variants via [`map_variables`], so a fold output nested in `CASE`/`IN`/etc. is
/// also renamed.
fn substitute_fold_aliases(expr: Expr, aliases: &HashMap<String, String>) -> Expr {
    if aliases.is_empty() {
        return expr;
    }
    map_variables(expr, &|name| {
        aliases.get(name).map(|a| Expr::Variable(a.clone()))
    })
}

/// Return whether `expr` references any FOLD-output variable.
///
/// Reuses the exhaustive [`map_variables`] walk (the single source of truth for
/// expression traversal) so every variant — including a variable nested inside
/// `CASE`/`IN`/a list — is inspected. Used to decide that a YIELD expression
/// must be produced POST-fold rather than in the pre-fold body.
fn expr_references_fold_output(expr: &Expr, fold_output_names: &HashSet<&str>) -> bool {
    let found = std::cell::Cell::new(false);
    let _ = map_variables(expr.clone(), &|name| {
        if fold_output_names.contains(name) {
            found.set(true);
        }
        None
    });
    found.get()
}

/// Map [`LocyBinaryOp`] to Cypher [`BinaryOp`].
pub(crate) fn locy_op_to_cypher_op(op: &LocyBinaryOp) -> BinaryOp {
    match op {
        LocyBinaryOp::Add => BinaryOp::Add,
        LocyBinaryOp::Sub => BinaryOp::Sub,
        LocyBinaryOp::Mul => BinaryOp::Mul,
        LocyBinaryOp::Div => BinaryOp::Div,
        LocyBinaryOp::Mod => BinaryOp::Mod,
        LocyBinaryOp::Pow => BinaryOp::Pow,
        LocyBinaryOp::And => BinaryOp::And,
        LocyBinaryOp::Or => BinaryOp::Or,
        LocyBinaryOp::Xor => BinaryOp::Xor,
    }
}

/// Build an Arrow schema from a compiled rule's yield columns using inferred types.
///
/// Computes target-rule node vars and infers types using the same logic as `build_rule`,
/// ensuring the derived scan schema matches `yield_columns_to_arrow_schema()` in
/// `locy_program.rs`.
fn yield_schema_to_arrow_from_rule(
    target_rule: &CompiledRule,
    rule_catalog: &HashMap<String, CompiledRule>,
    schema: &Schema,
) -> SchemaRef {
    let target_node_vars = collect_node_vars(&target_rule.clauses);
    let first_clause = target_rule.clauses.first();
    let fold_names: HashSet<&str> = first_clause
        .map(|c| c.fold.iter().map(|fb| fb.name.as_str()).collect())
        .unwrap_or_default();
    let along_names: HashSet<&str> = first_clause
        .map(|c| c.along.iter().map(|a| a.name.as_str()).collect())
        .unwrap_or_default();
    let var_labels = first_clause.map(clause_var_labels).unwrap_or_default();

    let fields: Vec<Field> = target_rule
        .yield_schema
        .iter()
        .map(|yc| {
            let dt = match first_clause {
                Some(fc) => infer_yield_type(
                    &yc.name,
                    fc,
                    &target_node_vars,
                    &fold_names,
                    &along_names,
                    rule_catalog,
                    yc.is_key,
                    schema,
                    &var_labels,
                ),
                None => DataType::LargeUtf8,
            };
            Field::new(&yc.name, dt, true)
        })
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// Validate that all `PrevRef` fields in a [`LocyExpr`] reference columns
/// available in the IS-reference yield schemas.
fn validate_prev_refs(expr: &LocyExpr, available: &HashSet<&str>) -> Result<()> {
    match expr {
        LocyExpr::PrevRef(field) => {
            if !available.contains(field.as_str()) {
                bail!(
                    "prev.{} references field '{}' not found in any \
                     IS-reference yield schema",
                    field,
                    field,
                );
            }
            Ok(())
        }
        LocyExpr::Cypher(_) => Ok(()),
        LocyExpr::BinaryOp { left, right, .. } => {
            validate_prev_refs(left, available)?;
            validate_prev_refs(right, available)
        }
        LocyExpr::UnaryOp(_, inner) => validate_prev_refs(inner, available),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use uni_cypher::ast::{LabelExpr, NodePattern, PathPattern, Pattern, PatternElement, UnaryOp};
    use uni_cypher::locy_ast::{
        AlongBinding, BestByClause, BestByItem, FoldBinding, IsReference, LocyBinaryOp, LocyExpr,
        LocyYieldItem, QualifiedName, RuleCondition, RuleOutput, YieldClause,
    };
    use uni_locy::types::{CompiledClause, CompiledProgram, CompiledRule, Stratum, YieldColumn};

    use crate::query::planner::LogicalPlan;

    // -- Test helpers ---------------------------------------------------

    fn test_schema() -> Arc<uni_common::core::schema::Schema> {
        Arc::new(uni_common::core::schema::Schema {
            schema_version: 1,
            labels: HashMap::new(),
            edge_types: HashMap::new(),
            properties: HashMap::new(),
            indexes: vec![],
            constraints: vec![],
            schemaless_registry: Default::default(),
        })
    }

    fn test_planner() -> QueryPlanner {
        QueryPlanner::new(test_schema())
    }

    fn test_classifier_ctx() -> ClassifierContext {
        ClassifierContext {
            registry: Arc::new(uni_locy::ClassifierRegistry::new()),
            cache: None,
            provenance_store: None,
        }
    }

    fn yield_col(name: &str, is_key: bool) -> YieldColumn {
        YieldColumn {
            name: name.to_string(),
            is_key,
            is_prob: false,
        }
    }

    fn qname(name: &str) -> QualifiedName {
        QualifiedName {
            parts: vec![name.to_string()],
        }
    }

    /// A minimal pattern: `(n)` — single node.
    fn node_pattern(var: &str) -> Pattern {
        Pattern {
            paths: vec![PathPattern {
                variable: None,
                elements: vec![PatternElement::Node(NodePattern {
                    variable: Some(var.to_string()),
                    labels: LabelExpr::Empty,
                    properties: None,
                    where_clause: None,
                })],
                shortest_path_mode: None,
            }],
        }
    }

    /// A two-node pattern: `(a)-[e]->(b)`
    fn edge_pattern(a: &str, e: &str, b: &str) -> Pattern {
        use uni_cypher::ast::{Direction, RelationshipPattern};
        Pattern {
            paths: vec![PathPattern {
                variable: None,
                elements: vec![
                    PatternElement::Node(NodePattern {
                        variable: Some(a.to_string()),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Relationship(RelationshipPattern {
                        variable: Some(e.to_string()),
                        types: LabelExpr::Empty,
                        direction: Direction::Outgoing,
                        range: None,
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Node(NodePattern {
                        variable: Some(b.to_string()),
                        labels: LabelExpr::Empty,
                        properties: None,
                        where_clause: None,
                    }),
                ],
                shortest_path_mode: None,
            }],
        }
    }

    fn simple_yield_output(names: &[&str]) -> RuleOutput {
        RuleOutput::Yield(YieldClause {
            items: names
                .iter()
                .map(|n| LocyYieldItem {
                    is_key: false,
                    is_prob: false,
                    expr: Expr::Variable(n.to_string()),
                    alias: None,
                })
                .collect(),
        })
    }

    fn simple_clause(pattern: Pattern, yield_names: &[&str]) -> CompiledClause {
        CompiledClause {
            match_pattern: pattern,
            where_conditions: vec![],
            along: vec![],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(yield_names),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        }
    }

    /// Create a minimal CompiledRule for use in derived scan handle tests.
    fn test_compiled_rule(yield_cols: &[YieldColumn]) -> CompiledRule {
        CompiledRule {
            name: "test".to_string(),
            clauses: vec![simple_clause(node_pattern("n"), &[])],
            yield_schema: yield_cols.to_vec(),
            priority: None,
        }
    }

    fn make_program(
        strata: Vec<Stratum>,
        rule_catalog: HashMap<String, CompiledRule>,
    ) -> CompiledProgram {
        CompiledProgram {
            strata,
            rule_catalog,
            model_catalog: HashMap::new(),
            warnings: vec![],
            commands: vec![],
        }
    }

    fn make_rule(
        name: &str,
        clauses: Vec<CompiledClause>,
        yield_schema: Vec<YieldColumn>,
    ) -> CompiledRule {
        CompiledRule {
            name: name.to_string(),
            clauses,
            yield_schema,
            priority: None,
        }
    }

    fn plan_is_project(plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Project { .. } | LogicalPlan::LocyProject { .. }
        )
    }

    fn plan_is_filter(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Filter { .. })
    }

    fn plan_is_cross_join(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::CrossJoin { .. })
    }

    fn plan_is_derived_scan(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::LocyDerivedScan { .. })
    }

    fn plan_is_fold(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::LocyFold { .. })
    }

    fn plan_is_best_by(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::LocyBestBy { .. })
    }

    // ===================================================================
    // Unit Tests — LocyExpr Rewriting
    // ===================================================================

    #[test]
    fn test_rewrite_prev_ref() {
        let expr = LocyExpr::PrevRef("cost".to_string());
        let result = rewrite_locy_expr(&expr).unwrap();
        assert_eq!(result, Expr::Variable("cost".to_string()));
    }

    #[test]
    fn test_rewrite_cypher_passthrough() {
        let inner = Expr::Literal(CypherLiteral::Integer(42));
        let expr = LocyExpr::Cypher(inner.clone());
        let result = rewrite_locy_expr(&expr).unwrap();
        assert_eq!(result, inner);
    }

    #[test]
    fn test_rewrite_binary_add() {
        let expr = LocyExpr::BinaryOp {
            left: Box::new(LocyExpr::PrevRef("x".to_string())),
            op: LocyBinaryOp::Add,
            right: Box::new(LocyExpr::Cypher(Expr::Literal(CypherLiteral::Integer(1)))),
        };
        let result = rewrite_locy_expr(&expr).unwrap();
        assert_eq!(
            result,
            Expr::BinaryOp {
                left: Box::new(Expr::Variable("x".to_string())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(CypherLiteral::Integer(1))),
            }
        );
    }

    #[test]
    fn test_rewrite_nested_binary() {
        // (prev.a + prev.b) * prev.c
        let expr = LocyExpr::BinaryOp {
            left: Box::new(LocyExpr::BinaryOp {
                left: Box::new(LocyExpr::PrevRef("a".to_string())),
                op: LocyBinaryOp::Add,
                right: Box::new(LocyExpr::PrevRef("b".to_string())),
            }),
            op: LocyBinaryOp::Mul,
            right: Box::new(LocyExpr::PrevRef("c".to_string())),
        };
        let result = rewrite_locy_expr(&expr).unwrap();
        let expected = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Variable("a".to_string())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Variable("b".to_string())),
            }),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Variable("c".to_string())),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_rewrite_unary_not() {
        let expr = LocyExpr::UnaryOp(
            UnaryOp::Not,
            Box::new(LocyExpr::Cypher(Expr::Variable("x".to_string()))),
        );
        let result = rewrite_locy_expr(&expr).unwrap();
        assert_eq!(
            result,
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(Expr::Variable("x".to_string())),
            }
        );
    }

    #[test]
    fn test_locy_op_to_cypher_op_all() {
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Add), BinaryOp::Add);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Sub), BinaryOp::Sub);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Mul), BinaryOp::Mul);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Div), BinaryOp::Div);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Mod), BinaryOp::Mod);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Pow), BinaryOp::Pow);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::And), BinaryOp::And);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Or), BinaryOp::Or);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Xor), BinaryOp::Xor);
    }

    // ===================================================================
    // Unit Tests — DerivedScanHandle Registry
    // ===================================================================

    #[test]
    fn test_handle_allocation_new() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true), yield_col("m", false)];
        let rule = test_compiled_rule(&cols);
        let handle =
            builder.get_or_create_derived_scan_handle("reachable", &rule, false, &HashMap::new());
        assert_eq!(handle.scan_index, 0);
        assert_eq!(handle.rule_name, "reachable");
        assert!(handle.data.read().is_empty());
    }

    #[test]
    fn test_handle_reuse_same_rule() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true), yield_col("m", false)];
        let rule = test_compiled_rule(&cols);
        let h1 =
            builder.get_or_create_derived_scan_handle("reachable", &rule, false, &HashMap::new());
        let h2 =
            builder.get_or_create_derived_scan_handle("reachable", &rule, false, &HashMap::new());
        assert!(Arc::ptr_eq(&h1.data, &h2.data));
        assert_eq!(h1.scan_index, h2.scan_index);
    }

    #[test]
    fn test_handle_different_rules() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true)];
        let rule = test_compiled_rule(&cols);
        let h1 =
            builder.get_or_create_derived_scan_handle("reachable", &rule, false, &HashMap::new());
        let h2 =
            builder.get_or_create_derived_scan_handle("connected", &rule, false, &HashMap::new());
        assert_eq!(h1.scan_index, 0);
        assert_eq!(h2.scan_index, 1);
        assert!(!Arc::ptr_eq(&h1.data, &h2.data));
    }

    #[test]
    fn test_registry_conversion() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true)];
        let rule = test_compiled_rule(&cols);
        builder.get_or_create_derived_scan_handle("rule_a", &rule, false, &HashMap::new());
        builder.get_or_create_derived_scan_handle("rule_b", &rule, true, &HashMap::new());

        let registry = builder.build_registry();
        assert!(registry.get(0).is_some());
        assert!(registry.get(1).is_some());
        assert_eq!(registry.get(0).unwrap().rule_name, "rule_a");
        assert_eq!(registry.get(1).unwrap().rule_name, "rule_b");
    }

    #[test]
    fn test_registry_self_ref_flag() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true)];
        let rule = test_compiled_rule(&cols);
        builder.get_or_create_derived_scan_handle("self_rule", &rule, true, &HashMap::new());
        builder.get_or_create_derived_scan_handle("cross_rule", &rule, false, &HashMap::new());

        let registry = builder.build_registry();
        assert!(registry.get(0).unwrap().is_self_ref);
        assert!(!registry.get(1).unwrap().is_self_ref);
    }

    // ===================================================================
    // Unit Tests — build_stratum
    // ===================================================================

    #[test]
    fn test_non_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let mut catalog = HashMap::new();
        catalog.insert("base".to_string(), rule.clone());

        let stratum = Stratum {
            id: 0,
            rules: vec![rule],
            is_recursive: false,
            depends_on: vec![],
        };
        let names: HashSet<String> = ["base".to_string()].into();
        let result = builder
            .build_stratum(
                &stratum,
                &catalog,
                &names,
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.id, 0);
        assert!(!result.is_recursive);
        assert_eq!(result.rules.len(), 1);
        assert_eq!(result.rules[0].name, "base");
    }

    #[test]
    fn test_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "reach",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let mut catalog = HashMap::new();
        catalog.insert("reach".to_string(), rule.clone());

        let stratum = Stratum {
            id: 1,
            rules: vec![rule],
            is_recursive: true,
            depends_on: vec![0],
        };
        let names: HashSet<String> = ["reach".to_string()].into();
        let result = builder
            .build_stratum(
                &stratum,
                &catalog,
                &names,
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert!(result.is_recursive);
        assert_eq!(result.depends_on, vec![0]);
    }

    #[test]
    fn test_stratum_depends_on() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "derived",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let mut catalog = HashMap::new();
        catalog.insert("derived".to_string(), rule.clone());

        let stratum = Stratum {
            id: 2,
            rules: vec![rule],
            is_recursive: false,
            depends_on: vec![0, 1],
        };
        let names: HashSet<String> = ["derived".to_string()].into();
        let result = builder
            .build_stratum(
                &stratum,
                &catalog,
                &names,
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.depends_on, vec![0, 1]);
    }

    #[test]
    fn test_stratum_multiple_rules() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rules: Vec<CompiledRule> = ["a", "b", "c"]
            .iter()
            .map(|name| {
                make_rule(
                    name,
                    vec![simple_clause(node_pattern("n"), &["n"])],
                    vec![yield_col("n", true)],
                )
            })
            .collect();
        let mut catalog = HashMap::new();
        for r in &rules {
            catalog.insert(r.name.clone(), r.clone());
        }

        let stratum = Stratum {
            id: 0,
            rules: rules.clone(),
            is_recursive: true,
            depends_on: vec![],
        };
        let names: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let result = builder
            .build_stratum(
                &stratum,
                &catalog,
                &names,
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.rules.len(), 3);
        let rule_names: Vec<&str> = result.rules.iter().map(|r| r.name.as_str()).collect();
        assert!(rule_names.contains(&"a"));
        assert!(rule_names.contains(&"b"));
        assert!(rule_names.contains(&"c"));
    }

    // ===================================================================
    // Unit Tests — build_rule
    // ===================================================================

    #[test]
    fn test_rule_name_and_schema() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "m"])],
            vec![yield_col("n", true), yield_col("m", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), rule.clone())]);
        let names: HashSet<String> = ["reachable".to_string()].into();

        let result = builder
            .build_rule(
                &rule,
                false,
                &names,
                &catalog,
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();
        assert_eq!(result.name, "reachable");
        assert_eq!(result.yield_schema.len(), 2);
        assert_eq!(result.yield_schema[0].name, "n");
        assert!(result.yield_schema[0].is_key);
        assert_eq!(result.yield_schema[1].name, "m");
        assert!(!result.yield_schema[1].is_key);
    }

    #[test]
    fn test_rule_priority() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let mut rule = make_rule(
            "prio",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        rule.priority = Some(5);
        let catalog = HashMap::from([("prio".to_string(), rule.clone())]);
        let names: HashSet<String> = ["prio".to_string()].into();

        let result = builder
            .build_rule(
                &rule,
                false,
                &names,
                &catalog,
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();
        assert_eq!(result.priority, Some(5));
    }

    #[test]
    fn test_rule_no_priority() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "noprio",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("noprio".to_string(), rule.clone())]);
        let names: HashSet<String> = ["noprio".to_string()].into();

        let result = builder
            .build_rule(
                &rule,
                false,
                &names,
                &catalog,
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();
        assert_eq!(result.priority, None);
    }

    #[test]
    fn test_rule_multiple_clauses() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "multi",
            vec![
                simple_clause(node_pattern("n"), &["n"]),
                simple_clause(node_pattern("n"), &["n"]),
                simple_clause(node_pattern("n"), &["n"]),
            ],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("multi".to_string(), rule.clone())]);
        let names: HashSet<String> = ["multi".to_string()].into();

        let result = builder
            .build_rule(
                &rule,
                false,
                &names,
                &catalog,
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();
        assert_eq!(result.clauses.len(), 3);
    }

    #[test]
    fn test_rule_yield_schema_key_flags() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "keyed",
            vec![simple_clause(node_pattern("n"), &["a", "b", "c"])],
            vec![
                yield_col("a", true),
                yield_col("b", false),
                yield_col("c", true),
            ],
        );
        let catalog = HashMap::from([("keyed".to_string(), rule.clone())]);
        let names: HashSet<String> = ["keyed".to_string()].into();

        let result = builder
            .build_rule(
                &rule,
                false,
                &names,
                &catalog,
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();
        assert!(result.yield_schema[0].is_key);
        assert!(!result.yield_schema[1].is_key);
        assert!(result.yield_schema[2].is_key);
    }

    // ===================================================================
    // Integration Tests — build_clause
    // ===================================================================

    #[test]
    fn test_clause_simple_match_yield() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = simple_clause(node_pattern("n"), &["n"]);
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert!(plan_is_project(&result.body));
        assert!(result.is_refs.is_empty());
        assert!(result.along_bindings.is_empty());
    }

    #[test]
    fn test_clause_with_where_filter() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![RuleCondition::Expression(Expr::BinaryOp {
                left: Box::new(Expr::Variable("n".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(CypherLiteral::Integer(21))),
            })],
            along: vec![],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        // Project wraps Filter wraps Scan
        assert!(plan_is_project(&result.body));
        if let LogicalPlan::LocyProject { input, .. } | LogicalPlan::Project { input, .. } =
            &result.body
        {
            assert!(plan_is_filter(input));
        }
    }

    #[test]
    fn test_clause_with_single_is_ref() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "m"])],
            vec![yield_col("n", true), yield_col("m", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 1);
        assert_eq!(result.is_refs[0].rule_name, "reachable");
        assert!(!result.is_refs[0].negated);

        // Structure: Project { Filter { CrossJoin { Scan, DerivedScan } } }
        assert!(plan_is_project(&result.body));
        if let LogicalPlan::LocyProject { input, .. } | LogicalPlan::Project { input, .. } =
            &result.body
        {
            assert!(plan_is_filter(input));
            if let LogicalPlan::Filter { input, .. } = input.as_ref() {
                assert!(plan_is_cross_join(input));
                if let LogicalPlan::CrossJoin { right, .. } = input.as_ref() {
                    assert!(plan_is_derived_scan(right));
                }
            }
        }
    }

    #[test]
    fn test_clause_with_is_ref_to_target() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "m"])],
            vec![yield_col("n", true), yield_col("m", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: Some("y".to_string()),
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x", "y"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("x", true), yield_col("y", false)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 1);
        assert_eq!(
            result.is_refs[0].target,
            Some(Expr::Variable("y".to_string()))
        );
    }

    #[test]
    fn test_clause_with_negated_is_ref() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: true,
            })],
            along: vec![],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 1);
        assert!(result.is_refs[0].negated);

        // No CrossJoin for negated — anti-join deferred to fixpoint
        assert!(plan_is_project(&result.body));
        if let LogicalPlan::LocyProject { input, .. } | LogicalPlan::Project { input, .. } =
            &result.body
        {
            assert!(!plan_is_cross_join(input));
            assert!(!plan_is_filter(input));
        }
    }

    #[test]
    fn test_clause_with_multiple_is_refs() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule_a = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let rule_b = make_rule(
            "connected",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([
            ("reachable".to_string(), rule_a),
            ("connected".to_string(), rule_b),
        ]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![
                RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("reachable"),
                    target: None,
                    negated: false,
                }),
                RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("connected"),
                    target: None,
                    negated: false,
                }),
            ],
            along: vec![],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 2);
        assert_eq!(result.is_refs[0].rule_name, "reachable");
        assert_eq!(result.is_refs[1].rule_name, "connected");
    }

    #[test]
    fn test_clause_with_along() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "cost"])],
            vec![yield_col("n", true), yield_col("cost", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: edge_pattern("a", "e", "b"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["a".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![AlongBinding {
                name: "cost".to_string(),
                expr: LocyExpr::BinaryOp {
                    left: Box::new(LocyExpr::PrevRef("cost".to_string())),
                    op: LocyBinaryOp::Add,
                    right: Box::new(LocyExpr::Cypher(Expr::Literal(CypherLiteral::Integer(1)))),
                },
            }],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["a", "b", "cost"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [
            yield_col("a", true),
            yield_col("b", false),
            yield_col("cost", false),
        ];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.along_bindings, vec!["cost".to_string()]);

        // The YIELD projection should contain the rewritten ALONG expression
        if let LogicalPlan::Project { projections, .. } = &result.body {
            let cost_proj = projections
                .iter()
                .find(|(_, alias)| alias.as_deref() == Some("cost"))
                .unwrap();
            assert!(matches!(cost_proj.0, Expr::BinaryOp { .. }));
        }
    }

    #[test]
    fn test_clause_with_fold_non_recursive() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n", "total"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("n", true), yield_col("total", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        // Fold is deferred to apply_post_fixpoint_chain (not in body)
        assert!(!plan_is_fold(&result.body));
        assert!(plan_is_project(&result.body));
    }

    #[test]
    fn test_clause_fold_skipped_recursive() {
        // Probes that a monotone FOLD in a recursive clause is deferred
        // from the body plan to the fixpoint engine (not whether the
        // monotonicity check passes — that's covered by the dedicated
        // validation tests above). Uses `MMAX` because non-monotone
        // aggregates are now rejected outright in recursive strata.
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "best".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "MMAX".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n", "best"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("n", true), yield_col("best", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                true,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        // Recursive: no LocyFold (deferred to FixpointExec)
        assert!(!plan_is_fold(&result.body));
        assert!(plan_is_project(&result.body));
    }

    #[test]
    fn test_clause_with_best_by() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![],
            having: vec![],
            best_by: Some(BestByClause {
                items: vec![BestByItem {
                    expr: Expr::Variable("cost".to_string()),
                    ascending: true,
                }],
            }),
            output: simple_yield_output(&["n", "cost"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("n", true), yield_col("cost", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert!(plan_is_best_by(&result.body));
        if let LogicalPlan::LocyBestBy {
            key_columns,
            criteria,
            ..
        } = &result.body
        {
            assert_eq!(key_columns, &["n".to_string()]);
            assert_eq!(criteria.len(), 1);
            assert!(criteria[0].1); // ascending
        }
    }

    #[test]
    fn test_clause_with_priority() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let mut clause = simple_clause(node_pattern("n"), &["n"]);
        clause.priority = Some(3);
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        assert_eq!(result.priority, Some(3));
        if let LogicalPlan::Project { projections, .. } = &result.body {
            let prio = projections
                .iter()
                .find(|(_, alias)| alias.as_deref() == Some("__priority"));
            assert!(prio.is_some());
            if let Some((Expr::Literal(CypherLiteral::Integer(v)), _)) = prio {
                assert_eq!(*v, 3);
            }
        }
    }

    #[test]
    fn test_clause_mixed_features() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "cost"])],
            vec![yield_col("n", true), yield_col("cost", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![AlongBinding {
                name: "cost".to_string(),
                expr: LocyExpr::PrevRef("cost".to_string()),
            }],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            having: vec![],
            best_by: Some(BestByClause {
                items: vec![BestByItem {
                    expr: Expr::Variable("total".to_string()),
                    ascending: true,
                }],
            }),
            output: simple_yield_output(&["x", "cost", "total"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [
            yield_col("x", true),
            yield_col("cost", false),
            yield_col("total", false),
        ];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            )
            .unwrap();

        // Layered: Project { Filter { CrossJoin { .. } } }.
        //
        // Both FOLD and BEST BY are deferred to the post-fold rule pipeline
        // (`merge_best_by` + `apply_post_fixpoint_chain`). BEST BY here ranks on
        // the aggregated `total`, which only exists post-fold; wrapping a
        // pre-fold `LocyBestBy` around the body would instead rank the raw
        // per-row input column and prune rows before aggregation (the #145
        // BEST-BY corruption). So the clause body top is the YIELD Project, not
        // a LocyBestBy.
        assert!(
            !plan_is_best_by(&result.body),
            "FOLD clause must not wrap a pre-fold BEST BY"
        );
        assert!(plan_is_project(&result.body));
        assert_eq!(result.is_refs.len(), 1);
        assert_eq!(result.along_bindings, vec!["cost".to_string()]);
    }

    // ===================================================================
    // Integration Tests — build_program_plan
    // ===================================================================

    #[test]
    fn test_program_single_non_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("base".to_string(), rule.clone())]);
        let program = make_program(
            vec![Stratum {
                id: 0,
                rules: vec![rule],
                is_recursive: false,
                depends_on: vec![],
            }],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();

        if let LogicalPlan::LocyProgram {
            strata, commands, ..
        } = &plan
        {
            assert_eq!(strata.len(), 1);
            assert!(!strata[0].is_recursive);
            assert!(commands.is_empty());
        } else {
            panic!("Expected LocyProgram");
        }
    }

    #[test]
    fn test_program_single_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "reach",
            vec![CompiledClause {
                match_pattern: edge_pattern("a", "e", "b"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["a".to_string()],
                    rule_name: qname("reach"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["a", "b"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
            }],
            vec![yield_col("a", true), yield_col("b", false)],
        );
        let catalog = HashMap::from([("reach".to_string(), rule.clone())]);
        let program = make_program(
            vec![Stratum {
                id: 0,
                rules: vec![rule],
                is_recursive: true,
                depends_on: vec![],
            }],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        let entry = registry.get(0).expect("should have scan entry");
        assert!(entry.is_self_ref);
        assert_eq!(entry.rule_name, "reach");
    }

    #[test]
    fn test_program_two_strata_topological() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let base_rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let derived_rule = make_rule(
            "derived",
            vec![CompiledClause {
                match_pattern: node_pattern("x"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("base"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["x"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
            }],
            vec![yield_col("x", true)],
        );
        let catalog = HashMap::from([
            ("base".to_string(), base_rule.clone()),
            ("derived".to_string(), derived_rule.clone()),
        ]);
        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![base_rule],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![derived_rule],
                    is_recursive: false,
                    depends_on: vec![0],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();

        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            assert_eq!(strata.len(), 2);
            assert_eq!(strata[0].id, 0);
            assert_eq!(strata[1].id, 1);
            assert_eq!(strata[1].depends_on, vec![0]);
        } else {
            panic!("Expected LocyProgram");
        }
    }

    #[test]
    fn test_program_cross_stratum_is_ref() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let base_rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let user_rule = make_rule(
            "user",
            vec![CompiledClause {
                match_pattern: node_pattern("x"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("base"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["x"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
            }],
            vec![yield_col("x", true)],
        );
        let catalog = HashMap::from([
            ("base".to_string(), base_rule.clone()),
            ("user".to_string(), user_rule.clone()),
        ]);
        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![base_rule],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![user_rule],
                    is_recursive: false,
                    depends_on: vec![0],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        let entry = registry.get(0).expect("should have scan entry");
        assert!(!entry.is_self_ref);
    }

    #[test]
    fn test_program_multi_rule_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule_a = make_rule(
            "rule_a",
            vec![CompiledClause {
                match_pattern: node_pattern("n"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["n".to_string()],
                    rule_name: qname("rule_b"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["n"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
            }],
            vec![yield_col("n", true)],
        );
        let rule_b = make_rule(
            "rule_b",
            vec![CompiledClause {
                match_pattern: node_pattern("n"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["n".to_string()],
                    rule_name: qname("rule_a"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["n"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
            }],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([
            ("rule_a".to_string(), rule_a.clone()),
            ("rule_b".to_string(), rule_b.clone()),
        ]);
        let program = make_program(
            vec![Stratum {
                id: 0,
                rules: vec![rule_a, rule_b],
                is_recursive: true,
                depends_on: vec![],
            }],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            assert_eq!(strata[0].rules.len(), 2);
        }

        let entries_a = registry.entries_for_rule("rule_a");
        let entries_b = registry.entries_for_rule("rule_b");
        assert_eq!(entries_a.len(), 1);
        assert_eq!(entries_b.len(), 1);
    }

    #[test]
    fn test_program_registry_arc_sharing() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "shared",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let user_rule = make_rule(
            "user",
            vec![
                CompiledClause {
                    match_pattern: node_pattern("x"),
                    where_conditions: vec![RuleCondition::IsReference(IsReference {
                        subjects: vec!["x".to_string()],
                        rule_name: qname("shared"),
                        target: None,
                        negated: false,
                    })],
                    along: vec![],
                    fold: vec![],
                    having: vec![],
                    best_by: None,
                    output: simple_yield_output(&["x"]),
                    priority: None,
                    model_invocations: vec![],
                    hidden_yield_cols: vec![],
                },
                CompiledClause {
                    match_pattern: node_pattern("y"),
                    where_conditions: vec![RuleCondition::IsReference(IsReference {
                        subjects: vec!["y".to_string()],
                        rule_name: qname("shared"),
                        target: None,
                        negated: false,
                    })],
                    along: vec![],
                    fold: vec![],
                    having: vec![],
                    best_by: None,
                    output: simple_yield_output(&["y"]),
                    priority: None,
                    model_invocations: vec![],
                    hidden_yield_cols: vec![],
                },
            ],
            vec![yield_col("x", true)],
        );
        let catalog = HashMap::from([
            ("shared".to_string(), target_rule.clone()),
            ("user".to_string(), user_rule.clone()),
        ]);
        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![target_rule],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![user_rule],
                    is_recursive: false,
                    depends_on: vec![0],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        // Both clauses should share the same Arc<RwLock> via the registry
        let entries = registry.entries_for_rule("shared");
        assert_eq!(entries.len(), 1);

        // Verify both DerivedScan nodes in the plan share the same data handle
        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            let user_strat = &strata[1];
            let clauses = &user_strat.rules[0].clauses;
            assert_eq!(clauses.len(), 2);

            fn extract_derived_scan_data(
                plan: &LogicalPlan,
            ) -> Option<Arc<RwLock<Vec<RecordBatch>>>> {
                match plan {
                    LogicalPlan::LocyDerivedScan { data, .. } => Some(data.clone()),
                    LogicalPlan::CrossJoin { left, right } => {
                        extract_derived_scan_data(left).or_else(|| extract_derived_scan_data(right))
                    }
                    LogicalPlan::Filter { input, .. }
                    | LogicalPlan::Project { input, .. }
                    | LogicalPlan::LocyProject { input, .. } => extract_derived_scan_data(input),
                    _ => None,
                }
            }

            let data1 = extract_derived_scan_data(&clauses[0].body)
                .expect("clause 0 should have DerivedScan");
            let data2 = extract_derived_scan_data(&clauses[1].body)
                .expect("clause 1 should have DerivedScan");
            assert!(Arc::ptr_eq(&data1, &data2));
        }
    }

    #[test]
    fn test_program_empty_strata() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let program = make_program(vec![], HashMap::new());

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        if let LogicalPlan::LocyProgram {
            strata, commands, ..
        } = &plan
        {
            assert!(strata.is_empty());
            assert!(commands.is_empty());
        } else {
            panic!("Expected LocyProgram");
        }

        assert!(registry.get(0).is_none());
    }

    #[test]
    fn test_program_complex_3_strata() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let base = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let intermediate = make_rule(
            "intermediate",
            vec![CompiledClause {
                match_pattern: node_pattern("x"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("base"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["x"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
            }],
            vec![yield_col("x", true)],
        );
        let recursive = make_rule(
            "recursive",
            vec![CompiledClause {
                match_pattern: node_pattern("y"),
                where_conditions: vec![
                    RuleCondition::IsReference(IsReference {
                        subjects: vec!["y".to_string()],
                        rule_name: qname("intermediate"),
                        target: None,
                        negated: false,
                    }),
                    RuleCondition::IsReference(IsReference {
                        subjects: vec!["y".to_string()],
                        rule_name: qname("recursive"),
                        target: None,
                        negated: false,
                    }),
                ],
                along: vec![],
                fold: vec![],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["y"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
            }],
            vec![yield_col("y", true)],
        );

        let catalog = HashMap::from([
            ("base".to_string(), base.clone()),
            ("intermediate".to_string(), intermediate.clone()),
            ("recursive".to_string(), recursive.clone()),
        ]);

        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![base],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![intermediate],
                    is_recursive: false,
                    depends_on: vec![0],
                },
                Stratum {
                    id: 2,
                    rules: vec![recursive],
                    is_recursive: true,
                    depends_on: vec![1],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            assert_eq!(strata.len(), 3);
            assert!(!strata[0].is_recursive);
            assert!(!strata[1].is_recursive);
            assert!(strata[2].is_recursive);
            assert_eq!(strata[2].depends_on, vec![1]);
        }

        // "intermediate" is cross-stratum ref (from stratum 2, target in stratum 1)
        let inter_entries = registry.entries_for_rule("intermediate");
        assert_eq!(inter_entries.len(), 1);
        assert!(!inter_entries[0].is_self_ref);

        // "recursive" is self-ref (within stratum 2)
        let rec_entries = registry.entries_for_rule("recursive");
        assert_eq!(rec_entries.len(), 1);
        assert!(rec_entries[0].is_self_ref);
    }

    // ===================================================================
    // Integration Tests — Validation Errors
    // ===================================================================

    #[test]
    fn test_validation_is_ref_unknown_rule() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("nonexistent"),
                target: None,
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("x", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder.build_clause(
            &clause,
            &yield_cols,
            false,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
            },
            ProbabilityConfig {
                strict_domain: false,
                epsilon: 1e-15,
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"), "Error: {}", err);
    }

    #[test]
    fn test_validation_is_ref_arity_mismatch() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "target",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("target".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string(), "y".to_string()],
                rule_name: qname("target"),
                target: None,
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder.build_clause(
            &clause,
            &yield_cols,
            false,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
            },
            ProbabilityConfig {
                strict_domain: false,
                epsilon: 1e-15,
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("arity mismatch"), "Error: {}", err);
    }

    #[test]
    fn test_validation_along_prev_field_not_in_schema() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![AlongBinding {
                name: "cost".to_string(),
                expr: LocyExpr::PrevRef("nonexistent".to_string()),
            }],
            fold: vec![],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["x", "cost"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("x", true), yield_col("cost", false)];
        let names = HashSet::new();

        let result = builder.build_clause(
            &clause,
            &yield_cols,
            false,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
            },
            ProbabilityConfig {
                strict_domain: false,
                epsilon: 1e-15,
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"), "Error: {}", err);
    }

    #[test]
    fn test_validation_fold_non_monotonic_rejected_in_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n", "total"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("n", true), yield_col("total", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder.build_clause(
            &clause,
            &yield_cols,
            true,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
            },
            ProbabilityConfig {
                strict_domain: false,
                epsilon: 1e-15,
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_err(), "SUM in a recursive stratum must reject");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("non-monotonic aggregate 'SUM'"),
            "error must name the aggregate; got: {err}"
        );
    }

    #[test]
    fn test_validation_fold_monotonic_accepted_in_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        for name in ["MMAX", "MMIN", "MNOR", "MPROD", "MSUM"] {
            let clause = CompiledClause {
                match_pattern: node_pattern("n"),
                where_conditions: vec![],
                along: vec![],
                fold: vec![FoldBinding {
                    name: "score".to_string(),
                    aggregate: Expr::FunctionCall {
                        name: name.to_string(),
                        args: vec![Expr::Variable("cost".to_string())],
                        distinct: false,
                        window_spec: None,
                    },
                }],
                having: vec![],
                best_by: None,
                output: simple_yield_output(&["n", "score"]),
                priority: None,
                model_invocations: vec![],
                hidden_yield_cols: vec![],
            };
            let yield_cols = [yield_col("n", true), yield_col("score", false)];
            let catalog = HashMap::new();
            let names = HashSet::new();

            let result = builder.build_clause(
                &clause,
                &yield_cols,
                true,
                ClauseCtx {
                    stratum_rule_names: &names,
                    rule_catalog: &catalog,
                    node_vars: &HashSet::new(),
                },
                ProbabilityConfig {
                    strict_domain: false,
                    epsilon: 1e-15,
                },
                &test_classifier_ctx(),
            );
            assert!(
                result.is_ok(),
                "{name} should be accepted in recursive stratum; got: {:?}",
                result.err()
            );
        }
    }

    #[test]
    fn test_validation_fold_non_monotonic_accepted_in_non_recursive_stratum() {
        // SUM is fine in a non-recursive rule — the check only triggers for
        // is_recursive=true.
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            having: vec![],
            best_by: None,
            output: simple_yield_output(&["n", "total"]),
            priority: None,
            model_invocations: vec![],
            hidden_yield_cols: vec![],
        };
        let yield_cols = [yield_col("n", true), yield_col("total", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder.build_clause(
            &clause,
            &yield_cols,
            false,
            ClauseCtx {
                stratum_rule_names: &names,
                rule_catalog: &catalog,
                node_vars: &HashSet::new(),
            },
            ProbabilityConfig {
                strict_domain: false,
                epsilon: 1e-15,
            },
            &test_classifier_ctx(),
        );
        assert!(result.is_ok(), "SUM in non-recursive stratum is valid");
    }
}
