use std::collections::HashMap;

use uni_cypher::ast::{Clause, Pattern, Query};
use uni_cypher::locy_ast::{
    AbduceQuery, AlongBinding, BestByClause, DeriveCommand, ExplainRule, FoldBinding, GoalQuery,
    RuleCondition, RuleOutput,
};

/// A fully validated and stratified Locy program, ready for the orchestrator.
#[derive(Debug, Clone)]
pub struct CompiledProgram {
    pub strata: Vec<Stratum>,
    pub rule_catalog: HashMap<String, CompiledRule>,
    pub warnings: Vec<CompilerWarning>,
    pub commands: Vec<CompiledCommand>,
}

/// A compiled command (non-rule statement) ready for execution.
#[derive(Debug, Clone)]
pub enum CompiledCommand {
    GoalQuery(GoalQuery),
    Assume(CompiledAssume),
    Abduce(AbduceQuery),
    ExplainRule(ExplainRule),
    DeriveCommand(DeriveCommand),
    Cypher(Query),
}

/// A compiled ASSUME block with mutations and body program.
#[derive(Debug, Clone)]
pub struct CompiledAssume {
    pub mutations: Vec<Clause>,
    pub body_program: CompiledProgram,
    pub body_commands: Vec<CompiledCommand>,
}

/// A group of rules that must be evaluated together (one SCC).
#[derive(Debug, Clone)]
pub struct Stratum {
    pub id: usize,
    pub rules: Vec<CompiledRule>,
    pub is_recursive: bool,
    pub depends_on: Vec<usize>,
}

/// A named rule with all its clauses merged and validated.
#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub name: String,
    pub clauses: Vec<CompiledClause>,
    pub yield_schema: Vec<YieldColumn>,
    pub priority: Option<i64>,
}

/// A single clause (one CREATE RULE ... AS ... definition).
#[derive(Debug, Clone)]
pub struct CompiledClause {
    pub match_pattern: Pattern,
    pub where_conditions: Vec<RuleCondition>,
    pub along: Vec<AlongBinding>,
    pub fold: Vec<FoldBinding>,
    pub best_by: Option<BestByClause>,
    pub output: RuleOutput,
    pub priority: Option<i64>,
}

/// A column in a rule's YIELD schema.
#[derive(Debug, Clone, PartialEq)]
pub struct YieldColumn {
    pub name: String,
    pub is_key: bool,
}

/// A non-fatal compiler diagnostic.
#[derive(Debug, Clone)]
pub struct CompilerWarning {
    pub code: WarningCode,
    pub message: String,
    pub rule_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WarningCode {
    MsumNonNegativity,
    ProbabilityDomainViolation,
}
