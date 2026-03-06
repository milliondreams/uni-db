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
}
