//! Intermediate representation the oracle evaluates over.
//!
//! These types are the oracle's *own* minimal IR — deliberately independent of
//! the engine's AST/compiled forms — so the oracle and engine share no
//! evaluation code. The [`generator`](crate::generator) emits this IR alongside
//! the equivalent Locy program text from a single source of truth.
//!
//! The IR models only Locy's **monotone core**: a fact is a tuple of `i64`s
//! (the seeded node `id`s), a relation is a set of such tuples, and a rule is a
//! union of clauses, each a relational join over base tuples plus `IS` / `IS NOT`
//! references to other relations.

// Rust guideline compliant

use std::collections::HashMap;

/// One derived or base fact: a tuple of `i64` keys (seeded node `id`s).
///
/// The oracle works entirely in `i64` space; the differential harness recovers
/// these ids from the engine's whole-node `YIELD` output via `properties["id"]`.
pub type Tuple = Vec<i64>;

/// A reference from a clause body to another relation: `subjects IS rule [TO target]`.
///
/// For a positive reference the [`target`](IsRef::target) binds a new variable to
/// the referenced relation's trailing column. For a negated reference (`IS NOT`)
/// all subject variables are already bound and the clause keeps only bindings
/// whose subject tuple is *absent* from the referenced relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsRef {
    /// Name of the referenced relation (rule).
    pub rule: String,
    /// Local variable names supplying the subject (lookup-key) columns, in order.
    pub subjects: Vec<String>,
    /// Local variable bound to the reference's `TO` target column, if any.
    ///
    /// Always `None` for a negated reference.
    pub target: Option<String>,
}

/// One clause (a single `CREATE RULE ... AS` definition) in relational-skeleton form.
///
/// Evaluation of a clause is: start from [`base`](OracleClause::base) bindings,
/// join in each positive reference, anti-join each negated reference, then project
/// to [`yield_vars`](OracleClause::yield_vars).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleClause {
    /// Base tuples the clause's `MATCH` contributes, known by construction from
    /// the generated graph. Each tuple is indexed by [`var_cols`](Self::var_cols).
    pub base: Vec<Tuple>,
    /// Column index, within each [`base`](Self::base) tuple, of each local variable.
    pub var_cols: HashMap<String, usize>,
    /// Positive `IS` references, applied as joins in order.
    pub pos_refs: Vec<IsRef>,
    /// Negated `IS NOT` references, applied as anti-joins.
    pub neg_refs: Vec<IsRef>,
    /// Local variables projected to the `YIELD KEY` columns, in output order.
    pub yield_vars: Vec<String>,
}

/// A rule: a named relation defined as the union of its [`clauses`](OracleRule::clauses).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleRule {
    /// The relation name (matches the engine's derived-relation key).
    pub name: String,
    /// Clauses whose results are unioned into this relation.
    pub clauses: Vec<OracleClause>,
}

/// A stratified program: rules grouped into dependency-ordered strata.
///
/// The generator owns stratum order (it controls program shape); negated
/// references only target rules in strictly earlier strata, so each stratum can
/// be driven to a least fixpoint before the next begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleProgram {
    /// Strata in dependency order; each inner `Vec` is one stratum's rules.
    pub strata: Vec<Vec<OracleRule>>,
}

/// The single-source-of-truth triple emitted by a generator builder.
///
/// The same parameters produce all three faces — the engine consumes
/// [`base_graph_cypher`](Generated::base_graph_cypher) + [`program_text`](Generated::program_text),
/// while the oracle consumes [`oracle_rules`](Generated::oracle_rules) — so the
/// two sides share inputs but no evaluation code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generated {
    /// Cypher that seeds the base graph (nodes carry an integer `id` property).
    pub base_graph_cypher: String,
    /// The Locy program the engine evaluates.
    pub program_text: String,
    /// The oracle IR equivalent of `program_text`.
    pub oracle_rules: OracleProgram,
    /// `YIELD KEY` column names per derived relation; drives `FactRow` extraction.
    pub key_schema: HashMap<String, Vec<String>>,
}
