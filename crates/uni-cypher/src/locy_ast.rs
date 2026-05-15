use serde::{Deserialize, Serialize};

use crate::ast::{Direction, Expr, Pattern, Query, ReturnClause, UnaryOp};

/// A complete Locy program: optional module header, imports, and body statements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocyProgram {
    pub module: Option<ModuleDecl>,
    pub uses: Vec<UseDecl>,
    pub statements: Vec<LocyStatement>,
}

/// A dotted name like `acme.compliance.rules`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QualifiedName {
    pub parts: Vec<String>,
}

impl std::fmt::Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.parts.join("."))
    }
}

/// `MODULE acme.compliance`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModuleDecl {
    pub name: QualifiedName,
}

/// `USE acme.common` or `USE acme.common { control, reachable }`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UseDecl {
    pub name: QualifiedName,
    /// `None` = glob import (all rules), `Some(vec)` = selective imports.
    pub imports: Option<Vec<String>>,
}

/// A top-level statement in a Locy program.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LocyStatement {
    /// A standard Cypher query (passthrough).
    Cypher(Query),
    /// `CREATE RULE ... AS ...`
    Rule(RuleDefinition),
    /// `QUERY ruleName WHERE expr RETURN ...`
    GoalQuery(GoalQuery),
    /// `DERIVE ruleName WHERE ...`
    DeriveCommand(DeriveCommand),
    /// `ASSUME { mutations } THEN body`
    AssumeBlock(AssumeBlock),
    /// `ABDUCE [NOT] ruleName WHERE expr RETURN ...`
    AbduceQuery(AbduceQuery),
    /// `EXPLAIN RULE ruleName WHERE expr RETURN ...`
    ExplainRule(ExplainRule),
    /// `CREATE MODEL name AS INPUT (...) FEATURES ... OUTPUT type name USING xervo('...')`
    /// Phase B neural-predicate preview. The grammar always parses this;
    /// the compiler rejects it unless `LocyConfig::neural_predicates_preview`
    /// is set.
    Model(ModelDefinition),
    /// `CALIBRATE name ON MATCH pattern [WHERE ...] TARGET expr METHOD method [HOLDOUT 0.2]`
    /// Phase C C2 calibration statement.
    Calibrate(CalibrateCommand),
    /// `VALIDATE name ON MATCH pattern [WHERE ...] TARGET expr METRICS m1, m2, ...`
    /// Phase C C3 validation statement.
    Validate(ValidateCommand),
}

// ═══════════════════════════════════════════════════════════════════════════
// RULE DEFINITION
// ═══════════════════════════════════════════════════════════════════════════

/// `CREATE RULE name [PRIORITY n] AS MATCH pattern [WHERE conds] [ALONG ...] [FOLD ...] [WHERE having] [BEST BY ...] YIELD/DERIVE ...`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleDefinition {
    pub name: QualifiedName,
    pub priority: Option<i64>,
    pub match_pattern: Pattern,
    pub where_conditions: Vec<RuleCondition>,
    pub along: Vec<AlongBinding>,
    pub fold: Vec<FoldBinding>,
    /// Post-FOLD filter conditions (HAVING semantics). These filter on
    /// aggregate results after FOLD computation.
    pub having: Vec<Expr>,
    pub best_by: Option<BestByClause>,
    pub output: RuleOutput,
}

/// A condition in a rule WHERE clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RuleCondition {
    /// `x IS rule`, `x IS rule TO y`, `(x,y) IS rule`
    IsReference(IsReference),
    /// A standard Cypher expression used as a boolean condition.
    Expression(Expr),
}

/// An IS rule reference in various forms.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IsReference {
    pub subjects: Vec<String>,
    pub rule_name: QualifiedName,
    pub target: Option<String>,
    pub negated: bool,
}

// ═══════════════════════════════════════════════════════════════════════════
// ALONG (path-carried values)
// ═══════════════════════════════════════════════════════════════════════════

/// `name = along_expression`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlongBinding {
    pub name: String,
    pub expr: LocyExpr,
}

/// Locy expression: extends Cypher expressions with `prev.field`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LocyExpr {
    /// `prev.fieldName` — reference to previous hop's value.
    PrevRef(String),
    /// A standard Cypher expression.
    Cypher(Expr),
    /// Binary operation between Locy expressions.
    BinaryOp {
        left: Box<LocyExpr>,
        op: LocyBinaryOp,
        right: Box<LocyExpr>,
    },
    /// Unary operation (NOT, negation).
    UnaryOp(UnaryOp, Box<LocyExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum LocyBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    And,
    Or,
    Xor,
    // Comparisons are handled via Cypher expression re-parse
}

// ═══════════════════════════════════════════════════════════════════════════
// FOLD (aggregation)
// ═══════════════════════════════════════════════════════════════════════════

/// `name = fold_expression`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FoldBinding {
    pub name: String,
    pub aggregate: Expr,
}

// ═══════════════════════════════════════════════════════════════════════════
// BEST BY (optimized selection)
// ═══════════════════════════════════════════════════════════════════════════

/// Wrapper for the BEST BY clause items.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BestByClause {
    pub items: Vec<BestByItem>,
}

/// `expr [ASC|DESC]`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BestByItem {
    pub expr: Expr,
    pub ascending: bool,
}

// ═══════════════════════════════════════════════════════════════════════════
// YIELD (rule output schema)
// ═══════════════════════════════════════════════════════════════════════════

/// Either YIELD items or DERIVE clause as a rule's output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RuleOutput {
    Yield(YieldClause),
    Derive(DeriveClause),
}

/// Wrapper for the YIELD clause items.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct YieldClause {
    pub items: Vec<LocyYieldItem>,
}

/// A single YIELD item, possibly marked as KEY or PROB.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocyYieldItem {
    pub is_key: bool,
    pub is_prob: bool,
    pub expr: Expr,
    pub alias: Option<String>,
}

// ═══════════════════════════════════════════════════════════════════════════
// DERIVE (graph derivation in rule heads)
// ═══════════════════════════════════════════════════════════════════════════

/// `DERIVE pattern, pattern, ...` or `DERIVE MERGE a, b`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DeriveClause {
    Patterns(Vec<DerivePattern>),
    Merge(String, String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DerivePattern {
    pub direction: Direction,
    pub source: DeriveNodeSpec,
    pub edge: DeriveEdgeSpec,
    pub target: DeriveNodeSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeriveNodeSpec {
    pub is_new: bool,
    pub variable: String,
    pub labels: Vec<String>,
    pub properties: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeriveEdgeSpec {
    pub edge_type: String,
    pub properties: Option<Expr>,
}

// ═══════════════════════════════════════════════════════════════════════════
// GOAL-DIRECTED QUERY
// ═══════════════════════════════════════════════════════════════════════════

/// `QUERY ruleName [WHERE expr] [RETURN ...]`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GoalQuery {
    pub rule_name: QualifiedName,
    pub where_expr: Option<Expr>,
    pub return_clause: Option<ReturnClause>,
}

// ═══════════════════════════════════════════════════════════════════════════
// DERIVE COMMAND (top-level)
// ═══════════════════════════════════════════════════════════════════════════

/// `DERIVE ruleName [WHERE expr]`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeriveCommand {
    pub rule_name: QualifiedName,
    pub where_expr: Option<Expr>,
}

// ═══════════════════════════════════════════════════════════════════════════
// ASSUME BLOCK
// ═══════════════════════════════════════════════════════════════════════════

/// `ASSUME { mutations } THEN body`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssumeBlock {
    pub mutations: Vec<crate::ast::Clause>,
    pub body: Vec<LocyStatement>,
}

// ═══════════════════════════════════════════════════════════════════════════
// ABDUCE QUERY
// ═══════════════════════════════════════════════════════════════════════════

/// `ABDUCE [NOT] ruleName [WHERE expr] [RETURN ...]`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AbduceQuery {
    pub negated: bool,
    pub rule_name: QualifiedName,
    pub where_expr: Option<Expr>,
    pub return_clause: Option<ReturnClause>,
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPLAIN RULE
// ═══════════════════════════════════════════════════════════════════════════

/// `EXPLAIN RULE ruleName [WHERE expr] [RETURN ...]`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExplainRule {
    pub rule_name: QualifiedName,
    pub where_expr: Option<Expr>,
    pub return_clause: Option<ReturnClause>,
}

// ═══════════════════════════════════════════════════════════════════════════
// CREATE MODEL (neural predicate, Phase B preview)
// ═══════════════════════════════════════════════════════════════════════════

/// `CREATE MODEL` declaration. Parses the full surface from impl plan §2.1;
/// `Conformal` / `Dirichlet` calibration methods are deferred to Phase C.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelDefinition {
    pub name: QualifiedName,
    pub inputs: Vec<InputBinding>,
    /// Feature expressions evaluated against input bindings. Empty when
    /// the `FEATURES` clause is omitted (model receives all bound node
    /// properties — interpretation deferred to the runtime adapter).
    pub features: Vec<Expr>,
    pub output: OutputBinding,
    pub xervo_alias: String,
    pub calibration: Option<CalibrationMethod>,
    pub version: Option<String>,
    pub annotations: ModelAnnotations,
}

/// One INPUT binding, e.g. `(s:Supplier)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InputBinding {
    pub variable: String,
    pub label: Option<String>,
}

/// The OUTPUT declaration, e.g. `OUTPUT PROB risk`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutputBinding {
    pub output_type: OutputType,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputType {
    Prob,
    Score,
    Label,
    Vector,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CalibrationMethod {
    PlattScaling,
    IsotonicRegression,
    TemperatureScaling,
    BetaCalibration,
    None,
    /// Phase C C1a: split-conformal predictor. The point prediction
    /// passes through unchanged; the calibrator carries a
    /// `(1 - alpha)`-quantile of holdout nonconformity scores which
    /// gates a per-prediction `ConfidenceBand` at inference. `alpha`
    /// defaults to 0.1 (90% bands) when omitted.
    Conformal {
        alpha: f64,
    },
    // Dirichlet — Phase C M2; deferred.
}

/// Statement-level annotations. Currently only `@independent`, which
/// suppresses Phase-C F2 shared-neural-input warnings. Parsed in Slice
/// 1+2; semantically meaningful when F2 lands.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAnnotations {
    pub independent: bool,
}

// ═══════════════════════════════════════════════════════════════════════════
// CALIBRATE COMMAND  (Phase C C2)
// ═══════════════════════════════════════════════════════════════════════════

/// `CALIBRATE` statement. The runtime collects
/// `(prediction, ground_truth)` pairs by invoking the registered
/// classifier for `model_name` over the MATCH pattern, fits the
/// chosen calibrator on a holdout-split, and returns the fitted
/// transform + holdout metrics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibrateCommand {
    pub model_name: QualifiedName,
    pub pattern: Pattern,
    pub where_expr: Option<Expr>,
    pub target_expr: Expr,
    pub method: CalibrationMethod,
    /// Holdout fraction (must be in `(0, 1)`). `None` → compiler
    /// resolves to default 0.2.
    pub holdout: Option<f64>,
}

// ═══════════════════════════════════════════════════════════════════════════
// VALIDATE COMMAND  (Phase C C3)
// ═══════════════════════════════════════════════════════════════════════════

/// `VALIDATE` statement. Runs the named rule, joins its PROB column
/// output against the TARGET expression (ground truth), and computes
/// the requested metrics. Unlike CALIBRATE, this never fits anything
/// — it just measures.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidateCommand {
    pub rule_name: QualifiedName,
    pub pattern: Pattern,
    pub where_expr: Option<Expr>,
    pub target_expr: Expr,
    pub metrics: Vec<ValidationMetric>,
}

/// Supported metrics in `VALIDATE METRICS ...`. Each metric is a
/// proper scoring rule or a calibration-quality summary; see
/// `crates/uni-locy/src/calibration.rs` for definitions and
/// numerical references.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationMetric {
    BrierScore,
    LogLoss,
    /// Naive equal-width-binning ECE. Triggers
    /// `WarningCode::EceBinningBias` (impl plan §3.4) suggesting
    /// `DebiasedEce` instead.
    Ece,
    /// Debiased ECE per Kumar et al. NeurIPS 2019 — recommended.
    DebiasedEce,
    Accuracy,
    Auc,
}
