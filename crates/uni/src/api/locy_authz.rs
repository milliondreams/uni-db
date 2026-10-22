// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Authorization of Locy programs against the registered `AuthzPolicy` chain.
//!
//! Cypher authorizes each statement before it runs. Locy did not: no entry
//! point consulted a policy, so a principal a policy refused could still
//! read through a rule, write through a Cypher command or a `DERIVE`, and
//! run an administrative procedure (`tx.locy("CALL uni.admin.…")`).
//!
//! A program is authorized as a whole, before any of it runs, so a refusal
//! leaves nothing half done. It makes one request per verb it needs, with the
//! program text as the resource path (Cypher uses the statement text):
//!
//! - `read` for everything it evaluates — rule bodies, goal queries,
//!   `EXPLAIN RULE` / `ABDUCE`, `CALIBRATE` / `VALIDATE`, read-only Cypher
//!   commands — when it has any;
//! - one request per command that writes, under the verb Cypher would give
//!   that statement ([`crate::api::transaction::procedure_verb`]): `write`
//!   for a Cypher write and for a `DERIVE`, `schema` for a DDL procedure,
//!   `dbms` for an administrative one. The mutations of an `ASSUME` block
//!   count, as a Cypher write does whether or not its transaction commits.
//!
//! Each request carries the structured fields (labels, relationship types,
//! properties, operations) of the parts it covers.

use uni_common::Result;
use uni_cypher::ast::{Query, Statement};
use uni_cypher::locy_ast::{DeriveClause, RuleOutput};
use uni_locy::CompiledProgram;
use uni_locy::types::{CompiledCommand, CompiledRule, Stratum};
use uni_plugin::traits::connector::Principal;
use uni_plugin::traits::procedure::ProcedureMode;

use crate::api::UniInner;
use crate::api::session::{AuthzFields, authorize_resource};
use crate::api::transaction::{classify_verb, procedure_verb};

/// Authorizes running `compiled`, whose source text is `program`, for
/// `principal` (anonymous when `None`).
///
/// A no-op when no policy is registered.
///
/// # Errors
///
/// [`uni_common::UniError::AuthorizationDenied`] for the first request a
/// policy refuses.
pub(crate) fn authorize_locy_program(
    db: &UniInner,
    principal: Option<&Principal>,
    program: &str,
    compiled: &CompiledProgram,
) -> Result<()> {
    if db.plugin_registry.authz_policies().is_empty() {
        return Ok(());
    }
    let registered_mode = |name: &str| registered_mode(db, name);
    let mut requests = Requests::default();
    requests.add_strata(&compiled.strata);
    requests.add_commands(
        &compiled.commands,
        compiled,
        classify_verb(program),
        &registered_mode,
    );

    if requests.reads_anything {
        let mut fields = requests.reads;
        fields.add_operation("read");
        authorize_resource(db, principal, "read", program, fields)?;
    }
    for (verb, fields) in requests.writes {
        authorize_resource(db, principal, verb, program, fields)?;
    }
    Ok(())
}

/// Authorizes `tx.apply` replaying `queries`, the statements of a
/// `DerivedFactSet`, for `principal`, before any of them runs.
///
/// The set is plain data: it may come from another principal's session, or
/// be built by hand. So its statements are authorized where they execute, one
/// request per statement as Cypher makes, under the path `<locy-apply>` the
/// replay runs them with. A schema command authorizes as `schema`.
///
/// # Errors
///
/// [`uni_common::UniError::AuthorizationDenied`] for the first statement a
/// policy refuses.
pub(crate) fn authorize_replay(
    db: &UniInner,
    principal: Option<&Principal>,
    queries: &[Query],
) -> Result<()> {
    if db.plugin_registry.authz_policies().is_empty() {
        return Ok(());
    }
    let registered_mode = |name: &str| registered_mode(db, name);
    for query in queries {
        let mut fields = AuthzFields::default();
        fields.add_query(query);
        let verb = cypher_command_verb(query, "schema", &registered_mode);
        authorize_resource(db, principal, verb, "<locy-apply>", fields)?;
    }
    Ok(())
}

/// Mode of a registered (non-built-in) procedure, from the db-level registry.
fn registered_mode(db: &UniInner, name: &str) -> Option<ProcedureMode> {
    uni_plugin::QName::parse(name)
        .ok()
        .and_then(|q| db.plugin_registry.procedure(&q))
        .map(|e| e.signature.mode)
}

/// The authorization requests a program needs, gathered before any is made.
#[derive(Default)]
struct Requests {
    /// Whether the program evaluates anything (a rule or a reading command).
    reads_anything: bool,
    /// What the `read` request names.
    reads: AuthzFields,
    /// One request per writing command, in program order.
    writes: Vec<(&'static str, AuthzFields)>,
}

impl Requests {
    fn add_strata(&mut self, strata: &[Stratum]) {
        for rule in strata.iter().flat_map(|s| &s.rules) {
            self.reads_anything = true;
            for clause in &rule.clauses {
                self.reads.add_pattern(&clause.match_pattern);
            }
        }
    }

    /// Adds `commands`, compiled within `program`. `schema_verb` is the verb
    /// of a schema command among them (see [`cypher_command_verb`]).
    fn add_commands(
        &mut self,
        commands: &[CompiledCommand],
        program: &CompiledProgram,
        schema_verb: &'static str,
        registered_mode: &dyn Fn(&str) -> Option<ProcedureMode>,
    ) {
        for command in commands {
            match command {
                CompiledCommand::Cypher(query) => {
                    let verb = cypher_command_verb(query, schema_verb, registered_mode);
                    self.add_statement(query, verb);
                }
                CompiledCommand::DeriveCommand(derive) => {
                    // Evaluates the rule, then writes what it derives.
                    self.reads_anything = true;
                    let mut fields = AuthzFields::default();
                    fields.add_operation("write");
                    if let Some(rule) = program.rule_catalog.get(&derive.rule_name.to_string()) {
                        add_derived(&mut fields, rule);
                    }
                    self.writes.push(("write", fields));
                }
                CompiledCommand::Assume(assume) => {
                    let mutations = Query::Single(Statement {
                        clauses: assume.mutations.clone(),
                    });
                    let verb = cypher_command_verb(&mutations, schema_verb, registered_mode);
                    self.add_statement(&mutations, verb);
                    self.add_strata(&assume.body_program.strata);
                    self.add_commands(
                        &assume.body_commands,
                        &assume.body_program,
                        schema_verb,
                        registered_mode,
                    );
                }
                CompiledCommand::Calibrate(c) => {
                    self.reads_anything = true;
                    self.reads.add_pattern(&c.pattern);
                }
                CompiledCommand::Validate(v) => {
                    self.reads_anything = true;
                    self.reads.add_pattern(&v.pattern);
                }
                CompiledCommand::GoalQuery(_)
                | CompiledCommand::Abduce(_)
                | CompiledCommand::ExplainRule(_) => self.reads_anything = true,
            }
        }
    }

    /// Adds a Cypher statement the program runs under `verb`: into the
    /// `read` request when it reads, as a request of its own otherwise.
    fn add_statement(&mut self, query: &Query, verb: &'static str) {
        if verb == "read" {
            self.reads_anything = true;
            self.reads.add_query(query);
        } else {
            let mut fields = AuthzFields::default();
            fields.add_query(query);
            self.writes.push((verb, fields));
        }
    }
}

/// The labels and relationship types a `DERIVE` of `rule` writes.
fn add_derived(fields: &mut AuthzFields, rule: &CompiledRule) {
    for clause in &rule.clauses {
        if let RuleOutput::Derive(DeriveClause::Patterns(patterns)) = &clause.output {
            for p in patterns {
                for label in p.source.labels.iter().chain(&p.target.labels) {
                    fields.add_label(label);
                }
                fields.add_rel_type(&p.edge.edge_type);
            }
        }
    }
}

/// The verb Cypher's `tx.query` would authorize `query` under: `read` for a
/// read-only statement, otherwise [`procedure_verb`] over the statement's
/// [`classify_verb`] verb.
///
/// `classify_verb` reads every clause statement that writes as `write`. For a
/// schema command (`CREATE INDEX …`) the caller passes `schema_verb`: a
/// program passes `classify_verb` of its text, which for a schema command is
/// that statement — the grammar takes one only as a whole program.
fn cypher_command_verb(
    query: &Query,
    schema_verb: &'static str,
    registered_mode: &dyn Fn(&str) -> Option<ProcedureMode>,
) -> &'static str {
    let is_write_procedure =
        |name: &str| registered_mode(name).is_some_and(|m| !matches!(m, ProcedureMode::Read));
    if uni_query::validate_read_only_with(query, &is_write_procedure).is_ok() {
        return "read";
    }
    let classified = match query {
        Query::Schema(_) => schema_verb,
        _ => "write",
    };
    procedure_verb(
        uni_query::strongest_procedure_mode(query, registered_mode),
        classified,
    )
}
