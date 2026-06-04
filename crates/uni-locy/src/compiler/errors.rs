use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum LocyCompileError {
    #[error("cyclic negation among rules: {}", rules.join(", "))]
    CyclicNegation { rules: Vec<String> },

    #[error("undefined rule: {name}")]
    UndefinedRule { name: String },

    #[error("prev reference in non-recursive rule '{rule}', field '{field}'")]
    PrevInBaseCase { rule: String, field: String },

    #[error("non-monotonic aggregate '{aggregate}' in recursive rule '{rule}'")]
    NonMonotonicInRecursion { rule: String, aggregate: String },

    #[error("BEST BY with monotonic fold '{fold}' in rule '{rule}'")]
    BestByWithMonotonicFold { rule: String, fold: String },

    #[error("post-FOLD WHERE in rule '{rule}' requires a FOLD clause")]
    HavingWithoutFold { rule: String },

    #[error("wardedness violation: variable '{variable}' in rule '{rule}' not bound by MATCH")]
    WardednessViolation { rule: String, variable: String },

    #[error("YIELD schema mismatch in rule '{rule}': {detail}")]
    YieldSchemaMismatch { rule: String, detail: String },

    #[error("mixed priority in rule '{rule}': some clauses have PRIORITY, others don't")]
    MixedPriority { rule: String },

    #[error("module not found: {name}")]
    ModuleNotFound { name: String },

    #[error("import not found: rule '{rule}' in module '{module}'")]
    ImportNotFound { module: String, rule: String },

    #[error(
        "IS arity mismatch in rule '{rule}': reference to '{target}' provides {actual} bindings, but '{target}' yields {expected} columns"
    )]
    IsArityMismatch {
        rule: String,
        target: String,
        expected: usize,
        actual: usize,
    },

    #[error(
        "prev.{field} in rule '{rule}' references unknown column; available columns from IS references: {available}"
    )]
    PrevFieldNotInSchema {
        rule: String,
        field: String,
        available: String,
    },

    #[error("rule '{rule}' has {count} PROB columns; at most 1 is allowed")]
    MultipleProbColumns { rule: String, count: usize },

    // ─── Phase B (neural predicates preview) ─────────────────────────────
    #[error(
        "CREATE MODEL '{model_name}' parsed but neural_predicates_preview is disabled; \
         set LocyConfig::neural_predicates_preview = true to enable"
    )]
    NeuralPreviewDisabled { model_name: String },

    #[error("model name collision: '{name}' is already declared")]
    ModelNameCollision { name: String },

    #[error(
        "model '{name}' arity mismatch in rule '{rule}': expected {expected} input(s), got {actual}"
    )]
    ModelArityMismatch {
        name: String,
        rule: String,
        expected: usize,
        actual: usize,
    },

    // ─── Phase C C2: CALIBRATE statement ────────────────────────────────
    #[error(
        "CALIBRATE references unknown model '{name}'; declare it with \
         CREATE MODEL first"
    )]
    CalibrateUnknownModel { name: String },

    #[error(
        "CALIBRATE on model '{name}': calibration only applies to PROB \
         outputs, but '{name}' is declared as {declared}"
    )]
    CalibrateOnNonProbModel { name: String, declared: String },

    #[error(
        "CALIBRATE on model '{model_name}': HOLDOUT must be in the open \
         interval (0, 1); got {holdout}"
    )]
    CalibrateInvalidHoldout { model_name: String, holdout: f64 },

    #[error(
        "CALIBRATE '{model_name}' parsed but neural_predicates_preview is \
         disabled; set LocyConfig::neural_predicates_preview = true to enable"
    )]
    CalibratePreviewDisabled { model_name: String },

    // ─── Phase C C3: VALIDATE statement ─────────────────────────────────
    #[error(
        "VALIDATE references unknown rule '{name}'; declare it with \
         CREATE RULE first"
    )]
    ValidateUnknownRule { name: String },

    #[error(
        "VALIDATE rule '{name}' has no PROB column; calibration metrics \
         only apply to probability outputs"
    )]
    ValidateRuleHasNoProbColumn { name: String },

    #[error("VALIDATE rule '{name}' must request at least one metric")]
    ValidateNoMetrics { name: String },

    /// Phase B follow-up: a WHERE clause invokes a neural model.
    /// The lift machinery would require splitting the rule's
    /// `body_logical` into pre-filter and post-filter halves so the
    /// classifier can run between them — a planner refactor we've
    /// scoped out of the current slice. Surface a clear error at
    /// compile time directing the user to move the invocation into
    /// a YIELD item, e.g. as a witness column they can filter on
    /// downstream.
    #[error(
        "rule '{rule}' invokes neural model '{model}' in a WHERE clause, \
         which is not yet supported. Lift the call into YIELD (e.g. \
         `YIELD KEY x, {model}(x) AS p`) and apply the filter on the \
         materialized rule output instead."
    )]
    WhereModelInvocationNotYetSupported { rule: String, model: String },

    /// A neural-model invocation's feature expression is not a plain
    /// variable or a single `node.property` access. Today's runtime
    /// reads features either from match-bound variables (e.g.
    /// `scorer(s)`) or from materialized property columns (e.g.
    /// `scorer(s.tier)`); arithmetic (`scorer(s.tier + 1)`) and nested
    /// calls (`scorer(normalize(s.revenue))`) are deferred to a
    /// follow-up slice.
    #[error(
        "rule '{rule}': neural model '{model}' feature expression \
         {expr} is unsupported — only plain variables and direct \
         property access (`var.prop`) are accepted today"
    )]
    UnsupportedFeatureExpression {
        rule: String,
        model: String,
        expr: String,
    },
}
