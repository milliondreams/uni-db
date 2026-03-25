use pest::Parser;
use pest::iterators::Pair;

use super::locy_parser::Rule as LocyRule;
use super::walker;
use super::{CypherParser, ParseError, Rule as CypherRule};
use crate::ast;
use crate::locy_ast::*;

/// Build a LocyProgram from the top-level parse result.
pub fn build_program(pair: Pair<LocyRule>) -> Result<LocyProgram, ParseError> {
    debug_assert_eq!(pair.as_rule(), LocyRule::locy_query);

    let mut module = None;
    let mut uses = Vec::new();
    let mut statements = Vec::new();

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::module_declaration => {
                module = Some(build_module_declaration(child)?);
            }
            LocyRule::use_declaration => {
                uses.push(build_use_declaration(child)?);
            }
            LocyRule::locy_union_query => {
                statements = build_locy_union_query(child)?;
            }
            LocyRule::EOI => {}
            other => {
                return Err(ParseError::new(format!(
                    "Unexpected rule in locy_query: {other:?}"
                )));
            }
        }
    }

    Ok(LocyProgram {
        module,
        uses,
        statements,
    })
}

fn build_locy_union_query(pair: Pair<LocyRule>) -> Result<Vec<LocyStatement>, ParseError> {
    let text = pair.as_str();
    let mut inner = pair.into_inner();

    let first = inner.next().unwrap();
    let has_union = inner.peek().is_some();

    if has_union {
        // UNION query — re-parse the entire text as Cypher
        let cypher_query = reparse_as_cypher_query(text)?;
        return Ok(vec![LocyStatement::Cypher(cypher_query)]);
    }

    // Single query — process through locy_single_query
    build_locy_single_query(first)
}

fn build_locy_single_query(pair: Pair<LocyRule>) -> Result<Vec<LocyStatement>, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        LocyRule::explain_query | LocyRule::schema_command | LocyRule::transaction_command => {
            // Cypher passthrough — re-parse the text
            let cypher_query = reparse_as_cypher_query(inner.as_str())?;
            Ok(vec![LocyStatement::Cypher(cypher_query)])
        }
        LocyRule::locy_statement_block => build_locy_statement_block(inner),
        other => Err(ParseError::new(format!(
            "Unexpected rule in locy_single_query: {other:?}"
        ))),
    }
}

fn build_locy_statement_block(pair: Pair<LocyRule>) -> Result<Vec<LocyStatement>, ParseError> {
    let mut statements = Vec::new();
    let mut cypher_clause_texts: Vec<String> = Vec::new();

    for clause_pair in pair.into_inner() {
        debug_assert_eq!(clause_pair.as_rule(), LocyRule::locy_clause);
        let inner = clause_pair.into_inner().next().unwrap();

        match inner.as_rule() {
            LocyRule::rule_definition => {
                // Flush accumulated Cypher clauses first
                flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                statements.push(LocyStatement::Rule(build_rule_definition(inner)?));
            }
            LocyRule::goal_query => {
                flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                statements.push(LocyStatement::GoalQuery(build_goal_query(inner)?));
            }
            LocyRule::derive_command => {
                flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                statements.push(LocyStatement::DeriveCommand(build_derive_command(inner)?));
            }
            LocyRule::assume_block => {
                flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                statements.push(LocyStatement::AssumeBlock(build_assume_block(inner)?));
            }
            LocyRule::abduce_query => {
                flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                statements.push(LocyStatement::AbduceQuery(build_abduce_query(inner)?));
            }
            LocyRule::explain_rule_query => {
                flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                statements.push(LocyStatement::ExplainRule(build_explain_rule_query(inner)?));
            }
            LocyRule::clause => {
                // Standard Cypher clause — accumulate its text
                cypher_clause_texts.push(inner.as_str().to_string());
            }
            other => {
                return Err(ParseError::new(format!(
                    "Unexpected rule in locy_clause: {other:?}"
                )));
            }
        }
    }

    // Flush any remaining Cypher clauses
    flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;

    Ok(statements)
}

/// Flush accumulated Cypher clause texts into a single Cypher statement.
fn flush_cypher_clauses(
    clause_texts: &mut Vec<String>,
    statements: &mut Vec<LocyStatement>,
) -> Result<(), ParseError> {
    if clause_texts.is_empty() {
        return Ok(());
    }

    // Join clause texts and re-parse as a complete Cypher query
    let combined = clause_texts.join(" ");
    clause_texts.clear();

    let cypher_query = reparse_as_cypher_query(&combined)?;
    statements.push(LocyStatement::Cypher(cypher_query));
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// CYPHER RE-PARSE BRIDGE
// ═══════════════════════════════════════════════════════════════════════════

/// Re-parse a text span as a complete Cypher query.
fn reparse_as_cypher_query(text: &str) -> Result<ast::Query, ParseError> {
    let pairs = CypherParser::parse(CypherRule::query, text)
        .map_err(|e| ParseError::new(format!("Cypher re-parse error: {e}")))?;
    walker::build_query(pairs)
}

/// Re-parse a text span as a Cypher expression.
fn reparse_as_cypher_expression(text: &str) -> Result<ast::Expr, ParseError> {
    let pairs = CypherParser::parse(CypherRule::expression, text)
        .map_err(|e| ParseError::new(format!("Cypher expression re-parse error: {e}")))?;
    walker::build_expression(pairs.into_iter().next().unwrap())
}

/// Re-parse a text span as a Cypher pattern.
fn reparse_as_cypher_pattern(text: &str) -> Result<ast::Pattern, ParseError> {
    let pairs = CypherParser::parse(CypherRule::pattern, text)
        .map_err(|e| ParseError::new(format!("Cypher pattern re-parse error: {e}")))?;
    walker::build_pattern(pairs.into_iter().next().unwrap())
}

/// Re-parse a text span as a Cypher clause.
fn reparse_as_cypher_clause(text: &str) -> Result<ast::Clause, ParseError> {
    let pairs = CypherParser::parse(CypherRule::clause, text)
        .map_err(|e| ParseError::new(format!("Cypher clause re-parse error: {e}")))?;
    walker::build_clause(pairs.into_iter().next().unwrap())
}

/// Re-parse a text span as Cypher return_items.
fn reparse_as_cypher_return_items(text: &str) -> Result<Vec<ast::ReturnItem>, ParseError> {
    let pairs = CypherParser::parse(CypherRule::return_items, text)
        .map_err(|e| ParseError::new(format!("Cypher return_items re-parse error: {e}")))?;
    walker::build_return_items(pairs.into_iter().next().unwrap())
}

/// Re-parse a text span as Cypher sort_items.
fn reparse_as_cypher_sort_items(text: &str) -> Result<Vec<ast::SortItem>, ParseError> {
    let pairs = CypherParser::parse(CypherRule::sort_items, text)
        .map_err(|e| ParseError::new(format!("Cypher sort_items re-parse error: {e}")))?;
    walker::build_sort_items(pairs.into_iter().next().unwrap())
}

/// Re-parse a text span as Cypher properties (map literal or parameter).
fn reparse_as_cypher_properties(text: &str) -> Result<ast::Expr, ParseError> {
    let pairs = CypherParser::parse(CypherRule::properties, text)
        .map_err(|e| ParseError::new(format!("Cypher properties re-parse error: {e}")))?;
    walker::build_properties(pairs.into_iter().next().unwrap())
}

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

fn normalize_locy_identifier(s: &str) -> String {
    s.strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(s)
        .to_string()
}

fn build_qualified_name(pair: Pair<LocyRule>) -> Result<QualifiedName, ParseError> {
    let parts = pair
        .into_inner()
        .map(|p| normalize_locy_identifier(p.as_str()))
        .collect();
    Ok(QualifiedName { parts })
}

// ═══════════════════════════════════════════════════════════════════════════
// MODULE / USE
// ═══════════════════════════════════════════════════════════════════════════

fn build_module_declaration(pair: Pair<LocyRule>) -> Result<ModuleDecl, ParseError> {
    let name_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == LocyRule::locy_qualified_name)
        .unwrap();
    Ok(ModuleDecl {
        name: build_qualified_name(name_pair)?,
    })
}

fn build_use_declaration(pair: Pair<LocyRule>) -> Result<UseDecl, ParseError> {
    let mut name = None;
    let mut imports = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::locy_qualified_name => {
                name = Some(build_qualified_name(child)?);
            }
            LocyRule::use_import_list => {
                let selected: Vec<String> = child
                    .into_inner()
                    .filter(|p| p.as_rule() == LocyRule::locy_identifier)
                    .map(|p| normalize_locy_identifier(p.as_str()))
                    .collect();
                imports = Some(selected);
            }
            _ => {}
        }
    }

    Ok(UseDecl {
        name: name.unwrap(),
        imports,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// RULE DEFINITION
// ═══════════════════════════════════════════════════════════════════════════

fn build_rule_definition(pair: Pair<LocyRule>) -> Result<RuleDefinition, ParseError> {
    let mut name = None;
    let mut priority = None;
    let mut match_pattern = None;
    let mut where_conditions = Vec::new();
    let mut along = Vec::new();
    let mut fold = Vec::new();
    let mut best_by = None;
    let mut output = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::rule_name => {
                let qn_pair = child.into_inner().next().unwrap();
                name = Some(build_qualified_name(qn_pair)?);
            }
            LocyRule::priority_clause => {
                priority = Some(build_priority_clause(child)?);
            }
            LocyRule::rule_match_clause => {
                // The rule_match_clause contains MATCH keyword + pattern
                // Extract the pattern text span and re-parse via Cypher
                let pattern_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::pattern)
                    .unwrap();
                match_pattern = Some(reparse_as_cypher_pattern(pattern_pair.as_str())?);
            }
            LocyRule::rule_where_clause => {
                where_conditions = build_rule_where_clause(child)?;
            }
            LocyRule::along_clause => {
                along = build_along_clause(child)?;
            }
            LocyRule::fold_clause => {
                fold = build_fold_clause(child)?;
            }
            LocyRule::best_by_clause => {
                let items = build_best_by_clause(child)?;
                if !items.is_empty() {
                    best_by = Some(BestByClause { items });
                }
            }
            LocyRule::rule_terminal_clause => {
                output = Some(build_rule_terminal_clause(child)?);
            }
            // Skip keywords: CREATE, RULE, AS
            LocyRule::CREATE | LocyRule::RULE | LocyRule::AS => {}
            _ => {}
        }
    }

    Ok(RuleDefinition {
        name: name.unwrap(),
        priority,
        match_pattern: match_pattern.unwrap(),
        where_conditions,
        along,
        fold,
        best_by,
        output: output.unwrap(),
    })
}

fn build_priority_clause(pair: Pair<LocyRule>) -> Result<i64, ParseError> {
    let int_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == LocyRule::integer)
        .unwrap();
    int_pair
        .as_str()
        .parse::<i64>()
        .map_err(|e| ParseError::new(format!("Invalid priority value: {e}")))
}

// ═══════════════════════════════════════════════════════════════════════════
// RULE WHERE CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_rule_where_clause(pair: Pair<LocyRule>) -> Result<Vec<RuleCondition>, ParseError> {
    let mut conditions = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::rule_condition {
            conditions.push(build_rule_condition(child)?);
        }
    }
    Ok(conditions)
}

fn build_rule_condition(pair: Pair<LocyRule>) -> Result<RuleCondition, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        LocyRule::is_rule_reference => {
            Ok(RuleCondition::IsReference(build_is_rule_reference(inner)?))
        }
        LocyRule::is_not_rule_reference => Ok(RuleCondition::IsReference(
            build_is_not_rule_reference(inner)?,
        )),
        LocyRule::expression => {
            let expr = reparse_as_cypher_expression(inner.as_str())?;
            Ok(RuleCondition::Expression(expr))
        }
        other => Err(ParseError::new(format!(
            "Unexpected rule in rule_condition: {other:?}"
        ))),
    }
}

fn build_is_rule_reference(pair: Pair<LocyRule>) -> Result<IsReference, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();

    // Identify which form we have by looking at the children
    // Form 1: (x, y, ...) IS rule_name  — has parentheses, identifiers before IS, then rule_name
    // Form 2: x IS rule_name TO y       — identifier, IS, rule_name, TO, identifier
    // Form 3: x IS rule_name            — identifier, IS, rule_name

    let mut identifiers = Vec::new();
    let mut rule_name = None;
    let mut target = None;
    let mut saw_to = false;

    for child in &children {
        match child.as_rule() {
            LocyRule::locy_identifier => {
                if rule_name.is_some() && saw_to {
                    // This is the target identifier after TO
                    target = Some(normalize_locy_identifier(child.as_str()));
                } else if rule_name.is_none() {
                    // Subject identifier(s)
                    identifiers.push(normalize_locy_identifier(child.as_str()));
                }
            }
            LocyRule::rule_name => {
                let qn = child.clone().into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::TO => {
                saw_to = true;
            }
            LocyRule::IS => {}
            _ => {}
        }
    }

    Ok(IsReference {
        subjects: identifiers,
        rule_name: rule_name.unwrap(),
        target,
        negated: false,
    })
}

fn build_is_not_rule_reference(pair: Pair<LocyRule>) -> Result<IsReference, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();

    let mut identifiers = Vec::new();
    let mut rule_name = None;
    let mut target = None;
    let mut saw_to = false;

    for child in &children {
        match child.as_rule() {
            LocyRule::locy_identifier => {
                if rule_name.is_some() && saw_to {
                    target = Some(normalize_locy_identifier(child.as_str()));
                } else if rule_name.is_none() {
                    identifiers.push(normalize_locy_identifier(child.as_str()));
                }
            }
            LocyRule::rule_name => {
                let qn = child.clone().into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::TO => {
                saw_to = true;
            }
            LocyRule::IS | LocyRule::NOT => {}
            _ => {}
        }
    }

    Ok(IsReference {
        subjects: identifiers,
        rule_name: rule_name.unwrap(),
        target,
        negated: true,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// ALONG CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_along_clause(pair: Pair<LocyRule>) -> Result<Vec<AlongBinding>, ParseError> {
    let mut bindings = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::along_declaration {
            bindings.push(build_along_declaration(child)?);
        }
    }
    Ok(bindings)
}

fn build_along_declaration(pair: Pair<LocyRule>) -> Result<AlongBinding, ParseError> {
    let mut name = None;
    let mut expr = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::locy_identifier => {
                name = Some(normalize_locy_identifier(child.as_str()));
            }
            LocyRule::along_expression => {
                expr = Some(build_along_expression(child)?);
            }
            LocyRule::eq => {}
            _ => {}
        }
    }

    Ok(AlongBinding {
        name: name.unwrap(),
        expr: expr.unwrap(),
    })
}

fn build_along_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    // along_expression = { locy_or_expression }
    let inner = pair.into_inner().next().unwrap();
    build_locy_or_expression(inner)
}

fn build_locy_or_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let mut children: Vec<_> = pair
        .into_inner()
        .filter(|p| p.as_rule() == LocyRule::locy_xor_expression)
        .collect();

    if children.len() == 1 {
        return build_locy_xor_expression(children.remove(0));
    }

    let mut result = build_locy_xor_expression(children.remove(0))?;
    for child in children {
        let right = build_locy_xor_expression(child)?;
        result = LocyExpr::BinaryOp {
            left: Box::new(result),
            op: LocyBinaryOp::Or,
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn build_locy_xor_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let mut children: Vec<_> = pair
        .into_inner()
        .filter(|p| p.as_rule() == LocyRule::locy_and_expression)
        .collect();

    if children.len() == 1 {
        return build_locy_and_expression(children.remove(0));
    }

    let mut result = build_locy_and_expression(children.remove(0))?;
    for child in children {
        let right = build_locy_and_expression(child)?;
        result = LocyExpr::BinaryOp {
            left: Box::new(result),
            op: LocyBinaryOp::Xor,
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn build_locy_and_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let mut children: Vec<_> = pair
        .into_inner()
        .filter(|p| p.as_rule() == LocyRule::locy_not_expression)
        .collect();

    if children.len() == 1 {
        return build_locy_not_expression(children.remove(0));
    }

    let mut result = build_locy_not_expression(children.remove(0))?;
    for child in children {
        let right = build_locy_not_expression(child)?;
        result = LocyExpr::BinaryOp {
            left: Box::new(result),
            op: LocyBinaryOp::And,
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn build_locy_not_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    let not_count = children
        .iter()
        .filter(|p| p.as_rule() == LocyRule::NOT)
        .count();
    let comparison = children
        .into_iter()
        .find(|p| p.as_rule() == LocyRule::locy_comparison_expression)
        .unwrap();
    let mut result = build_locy_comparison_expression(comparison)?;
    for _ in 0..not_count {
        result = LocyExpr::UnaryOp(crate::ast::UnaryOp::Not, Box::new(result));
    }
    Ok(result)
}

fn build_locy_comparison_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    // locy_comparison_expression = { locy_additive_expression ~ comparison_tail* }
    // If there are comparison_tail elements, the entire thing is better handled as
    // a Cypher expression re-parse since comparisons are complex.
    let children: Vec<_> = pair.into_inner().collect();
    let has_comparison = children
        .iter()
        .any(|p| p.as_rule() == LocyRule::comparison_tail);

    if has_comparison {
        // Re-parse the whole comparison as a Cypher expression using span offsets
        let first_start = children.first().unwrap().as_span().start();
        let last_end = children.last().unwrap().as_span().end();
        let full_input = children.first().unwrap().as_span().get_input();
        let text = &full_input[first_start..last_end];
        let expr = reparse_as_cypher_expression(text)?;
        return Ok(LocyExpr::Cypher(expr));
    }

    build_locy_additive_expression(children.into_iter().next().unwrap())
}

fn build_locy_additive_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    if children.len() == 1 {
        return build_locy_multiplicative_expression(children.into_iter().next().unwrap());
    }

    // Pattern: term (op term)*
    let mut iter = children.into_iter();
    let mut result = build_locy_multiplicative_expression(iter.next().unwrap())?;
    while let Some(op_pair) = iter.next() {
        let op = match op_pair.as_rule() {
            LocyRule::plus => LocyBinaryOp::Add,
            LocyRule::minus => LocyBinaryOp::Sub,
            _ => {
                // This is a multiplicative expression, not an operator
                // This shouldn't happen with correct grammar
                return Err(ParseError::new(format!(
                    "Unexpected token in additive expression: {:?}",
                    op_pair.as_rule()
                )));
            }
        };
        let right = build_locy_multiplicative_expression(iter.next().unwrap())?;
        result = LocyExpr::BinaryOp {
            left: Box::new(result),
            op,
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn build_locy_multiplicative_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    if children.len() == 1 {
        return build_locy_power_expression(children.into_iter().next().unwrap());
    }

    let mut iter = children.into_iter();
    let mut result = build_locy_power_expression(iter.next().unwrap())?;
    while let Some(op_pair) = iter.next() {
        let op = match op_pair.as_rule() {
            LocyRule::star => LocyBinaryOp::Mul,
            LocyRule::slash => LocyBinaryOp::Div,
            LocyRule::percent => LocyBinaryOp::Mod,
            _ => {
                return Err(ParseError::new(format!(
                    "Unexpected token in multiplicative expression: {:?}",
                    op_pair.as_rule()
                )));
            }
        };
        let right = build_locy_power_expression(iter.next().unwrap())?;
        result = LocyExpr::BinaryOp {
            left: Box::new(result),
            op,
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn build_locy_power_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    if children.len() == 1 {
        return build_locy_unary_expression(children.into_iter().next().unwrap());
    }

    let mut iter = children.into_iter();
    let mut result = build_locy_unary_expression(iter.next().unwrap())?;
    while let Some(op_pair) = iter.next() {
        if op_pair.as_rule() == LocyRule::caret {
            let right = build_locy_unary_expression(iter.next().unwrap())?;
            result = LocyExpr::BinaryOp {
                left: Box::new(result),
                op: LocyBinaryOp::Pow,
                right: Box::new(right),
            };
        }
    }
    Ok(result)
}

fn build_locy_unary_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    let has_neg = children.iter().any(|p| p.as_rule() == LocyRule::minus);
    let postfix = children
        .into_iter()
        .find(|p| p.as_rule() == LocyRule::locy_postfix_expression)
        .unwrap();

    let mut result = build_locy_postfix_expression(postfix)?;
    if has_neg {
        result = LocyExpr::UnaryOp(crate::ast::UnaryOp::Neg, Box::new(result));
    }
    Ok(result)
}

fn build_locy_postfix_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    let has_postfix = children
        .iter()
        .any(|p| p.as_rule() == LocyRule::postfix_suffix);

    if has_postfix {
        // If there are postfix suffixes, the whole thing is a standard expression.
        // Re-parse as Cypher.
        let first_start = children.first().unwrap().as_span().start();
        let last_end = children.last().unwrap().as_span().end();
        let full_input = children.first().unwrap().as_span().get_input();
        let text = &full_input[first_start..last_end];
        let expr = reparse_as_cypher_expression(text)?;
        return Ok(LocyExpr::Cypher(expr));
    }

    let primary = children.into_iter().next().unwrap();
    build_locy_primary_expression(primary)
}

fn build_locy_primary_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        LocyRule::prev_reference => {
            // prev.fieldName
            let field = inner
                .into_inner()
                .find(|p| p.as_rule() == LocyRule::identifier_or_keyword)
                .unwrap();
            Ok(LocyExpr::PrevRef(field.as_str().to_string()))
        }
        LocyRule::primary_expression => {
            // Standard Cypher primary — re-parse
            let expr = reparse_as_cypher_expression(inner.as_str())?;
            Ok(LocyExpr::Cypher(expr))
        }
        other => Err(ParseError::new(format!(
            "Unexpected rule in locy_primary_expression: {other:?}"
        ))),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// FOLD CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_fold_clause(pair: Pair<LocyRule>) -> Result<Vec<FoldBinding>, ParseError> {
    let mut bindings = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::fold_declaration {
            bindings.push(build_fold_declaration(child)?);
        }
    }
    Ok(bindings)
}

fn build_fold_declaration(pair: Pair<LocyRule>) -> Result<FoldBinding, ParseError> {
    let mut name = None;
    let mut expr = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::locy_identifier => {
                name = Some(normalize_locy_identifier(child.as_str()));
            }
            LocyRule::fold_expression => {
                // fold_expression = { expression }
                let expr_pair = child.into_inner().next().unwrap();
                expr = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            LocyRule::eq => {}
            _ => {}
        }
    }

    Ok(FoldBinding {
        name: name.unwrap(),
        aggregate: expr.unwrap(),
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// BEST BY CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_best_by_clause(pair: Pair<LocyRule>) -> Result<Vec<BestByItem>, ParseError> {
    let mut items = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::best_by_item {
            items.push(build_best_by_item(child)?);
        }
    }
    Ok(items)
}

fn build_best_by_item(pair: Pair<LocyRule>) -> Result<BestByItem, ParseError> {
    let mut expr = None;
    let mut ascending = true; // default

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::expression => {
                expr = Some(reparse_as_cypher_expression(child.as_str())?);
            }
            LocyRule::ASC => {
                ascending = true;
            }
            LocyRule::DESC => {
                ascending = false;
            }
            _ => {}
        }
    }

    Ok(BestByItem {
        expr: expr.unwrap(),
        ascending,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// RULE OUTPUT (YIELD / DERIVE)
// ═══════════════════════════════════════════════════════════════════════════

fn build_rule_terminal_clause(pair: Pair<LocyRule>) -> Result<RuleOutput, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        LocyRule::locy_yield_clause => {
            let items = build_locy_yield_clause(inner)?;
            Ok(RuleOutput::Yield(YieldClause { items }))
        }
        LocyRule::derive_clause => {
            let derive = build_derive_clause(inner)?;
            Ok(RuleOutput::Derive(derive))
        }
        other => Err(ParseError::new(format!(
            "Unexpected rule in rule_terminal_clause: {other:?}"
        ))),
    }
}

fn build_locy_yield_clause(pair: Pair<LocyRule>) -> Result<Vec<LocyYieldItem>, ParseError> {
    let mut items = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::locy_yield_item {
            items.push(build_locy_yield_item(child)?);
        }
    }
    Ok(items)
}

fn build_locy_yield_item(pair: Pair<LocyRule>) -> Result<LocyYieldItem, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    let first = &children[0];

    if first.as_rule() == LocyRule::key_projection {
        let ident = first
            .clone()
            .into_inner()
            .find(|p| p.as_rule() == LocyRule::locy_identifier)
            .unwrap();
        return Ok(LocyYieldItem {
            is_key: true,
            is_prob: false,
            expr: ast::Expr::Variable(normalize_locy_identifier(ident.as_str())),
            alias: None,
        });
    }

    if first.as_rule() == LocyRule::prob_projection {
        return build_prob_projection(first.clone());
    }

    // expression ~ (AS ~ alias_identifier)?
    let expr = reparse_as_cypher_expression(first.as_str())?;
    let alias = children
        .iter()
        .find(|p| p.as_rule() == LocyRule::alias_identifier)
        .map(|p| normalize_locy_identifier(p.as_str()));

    Ok(LocyYieldItem {
        is_key: false,
        is_prob: false,
        expr,
        alias,
    })
}

/// Parse a `prob_projection` node into a `LocyYieldItem` with `is_prob = true`.
///
/// Grammar forms:
///   `expression AS alias_identifier PROB` → explicit alias
///   `expression AS PROB`                  → alias derived from expression
///   `expression PROB`                     → alias derived from expression
fn build_prob_projection(pair: Pair<LocyRule>) -> Result<LocyYieldItem, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();

    // First child is always the expression
    let expr_pair = &children[0];
    let expr = reparse_as_cypher_expression(expr_pair.as_str())?;

    // Look for an explicit alias_identifier (present only in form 1)
    let alias = children
        .iter()
        .find(|p| p.as_rule() == LocyRule::alias_identifier)
        .map(|p| normalize_locy_identifier(p.as_str()));

    // For forms without explicit alias, derive from expression
    let alias = alias.or_else(|| match &expr {
        ast::Expr::Variable(name) => Some(name.clone()),
        ast::Expr::Property(_, key) => Some(key.clone()),
        _ => None,
    });

    Ok(LocyYieldItem {
        is_key: false,
        is_prob: true,
        expr,
        alias,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// DERIVE CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_derive_clause(pair: Pair<LocyRule>) -> Result<DeriveClause, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();

    // Check for MERGE form: DERIVE MERGE ident, ident
    let has_merge = children.iter().any(|p| p.as_rule() == LocyRule::MERGE);
    if has_merge {
        let idents: Vec<_> = children
            .iter()
            .filter(|p| p.as_rule() == LocyRule::locy_identifier)
            .map(|p| normalize_locy_identifier(p.as_str()))
            .collect();
        return Ok(DeriveClause::Merge(idents[0].clone(), idents[1].clone()));
    }

    // Pattern form: DERIVE pattern, pattern, ...
    let patterns: Vec<_> = children
        .into_iter()
        .filter(|p| p.as_rule() == LocyRule::derive_pattern)
        .map(build_derive_pattern)
        .collect::<Result<_, _>>()?;

    Ok(DeriveClause::Patterns(patterns))
}

fn build_derive_pattern(pair: Pair<LocyRule>) -> Result<DerivePattern, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    let direction = match inner.as_rule() {
        LocyRule::derive_forward_pattern => crate::ast::Direction::Outgoing,
        LocyRule::derive_backward_pattern => crate::ast::Direction::Incoming,
        other => {
            return Err(ParseError::new(format!(
                "Unexpected rule in derive_pattern: {other:?}"
            )));
        }
    };

    let mut nodes = Vec::new();
    let mut edge = None;

    for child in inner.into_inner() {
        match child.as_rule() {
            LocyRule::derive_node_spec => {
                nodes.push(build_derive_node_spec(child)?);
            }
            LocyRule::derive_edge_spec => {
                edge = Some(build_derive_edge_spec(child)?);
            }
            _ => {}
        }
    }

    Ok(DerivePattern {
        direction,
        source: nodes.remove(0),
        edge: edge.unwrap(),
        target: nodes.remove(0),
    })
}

fn build_derive_node_spec(pair: Pair<LocyRule>) -> Result<DeriveNodeSpec, ParseError> {
    let mut is_new = false;
    let mut variable = None;
    let mut labels = Vec::new();
    let mut properties = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::NEW => {
                is_new = true;
            }
            LocyRule::locy_identifier => {
                variable = Some(normalize_locy_identifier(child.as_str()));
            }
            LocyRule::node_labels => {
                // node_labels contains : identifier_or_keyword pairs
                for label_child in child.into_inner() {
                    if label_child.as_rule() == LocyRule::identifier_or_keyword {
                        labels.push(label_child.as_str().to_string());
                    }
                }
            }
            LocyRule::properties => {
                properties = Some(reparse_as_cypher_properties(child.as_str())?);
            }
            _ => {}
        }
    }

    Ok(DeriveNodeSpec {
        is_new,
        variable: variable.unwrap(),
        labels,
        properties,
    })
}

fn build_derive_edge_spec(pair: Pair<LocyRule>) -> Result<DeriveEdgeSpec, ParseError> {
    let mut edge_type = None;
    let mut properties = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::identifier_or_keyword => {
                edge_type = Some(child.as_str().to_string());
            }
            LocyRule::properties => {
                properties = Some(reparse_as_cypher_properties(child.as_str())?);
            }
            _ => {}
        }
    }

    Ok(DeriveEdgeSpec {
        edge_type: edge_type.unwrap(),
        properties,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// GOAL QUERY
// ═══════════════════════════════════════════════════════════════════════════

fn build_goal_query(pair: Pair<LocyRule>) -> Result<GoalQuery, ParseError> {
    let mut rule_name = None;
    let mut where_expr = None;
    let mut return_clause = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::rule_name => {
                let qn = child.into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::expression => {
                where_expr = Some(reparse_as_cypher_expression(child.as_str())?);
            }
            LocyRule::goal_return_clause => {
                return_clause = Some(build_locy_return_clause(child)?);
            }
            LocyRule::QUERY_KW | LocyRule::WHERE => {}
            _ => {}
        }
    }

    Ok(GoalQuery {
        rule_name: rule_name.unwrap(),
        where_expr,
        return_clause,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// DERIVE COMMAND (top-level)
// ═══════════════════════════════════════════════════════════════════════════

fn build_derive_command(pair: Pair<LocyRule>) -> Result<DeriveCommand, ParseError> {
    let mut rule_name = None;
    let mut where_expr = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::rule_name => {
                let qn = child.into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::where_clause => {
                // where_clause = { WHERE ~ expression }
                let expr_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::expression)
                    .unwrap();
                where_expr = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            LocyRule::DERIVE => {}
            _ => {}
        }
    }

    Ok(DeriveCommand {
        rule_name: rule_name.unwrap(),
        where_expr,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// ASSUME BLOCK
// ═══════════════════════════════════════════════════════════════════════════

fn build_assume_block(pair: Pair<LocyRule>) -> Result<AssumeBlock, ParseError> {
    let mut mutations = Vec::new();
    let mut body = Vec::new();

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::assume_mutation => {
                let inner = child.into_inner().next().unwrap();
                let clause = reparse_as_cypher_clause(inner.as_str())?;
                mutations.push(clause);
            }
            LocyRule::assume_body => {
                body = build_assume_body(child)?;
            }
            LocyRule::ASSUME | LocyRule::THEN => {}
            _ => {}
        }
    }

    Ok(AssumeBlock { mutations, body })
}

fn build_assume_body(pair: Pair<LocyRule>) -> Result<Vec<LocyStatement>, ParseError> {
    // assume_body = { "{" ~ locy_clause+ ~ "}" | locy_clause }
    let mut statements = Vec::new();
    let mut cypher_clause_texts: Vec<String> = Vec::new();

    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::locy_clause {
            let inner = child.into_inner().next().unwrap();
            match inner.as_rule() {
                LocyRule::clause => {
                    cypher_clause_texts.push(inner.as_str().to_string());
                }
                LocyRule::rule_definition => {
                    flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                    statements.push(LocyStatement::Rule(build_rule_definition(inner)?));
                }
                LocyRule::goal_query => {
                    flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                    statements.push(LocyStatement::GoalQuery(build_goal_query(inner)?));
                }
                _ => {
                    // Treat as Cypher
                    cypher_clause_texts.push(inner.as_str().to_string());
                }
            }
        }
    }

    flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
    Ok(statements)
}

// ═══════════════════════════════════════════════════════════════════════════
// ABDUCE QUERY
// ═══════════════════════════════════════════════════════════════════════════

fn build_abduce_query(pair: Pair<LocyRule>) -> Result<AbduceQuery, ParseError> {
    let mut negated = false;
    let mut rule_name = None;
    let mut where_expr = None;
    let mut return_clause = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::NOT => {
                negated = true;
            }
            LocyRule::rule_name => {
                let qn = child.into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::expression => {
                where_expr = Some(reparse_as_cypher_expression(child.as_str())?);
            }
            LocyRule::abduce_return_clause => {
                return_clause = Some(build_locy_return_clause(child)?);
            }
            LocyRule::ABDUCE | LocyRule::WHERE => {}
            _ => {}
        }
    }

    Ok(AbduceQuery {
        negated,
        rule_name: rule_name.unwrap(),
        where_expr,
        return_clause,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPLAIN RULE QUERY
// ═══════════════════════════════════════════════════════════════════════════

fn build_explain_rule_query(pair: Pair<LocyRule>) -> Result<ExplainRule, ParseError> {
    let mut rule_name = None;
    let mut where_expr = None;
    let mut return_clause = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::rule_name => {
                let qn = child.into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::expression => {
                where_expr = Some(reparse_as_cypher_expression(child.as_str())?);
            }
            LocyRule::explain_rule_return_clause => {
                return_clause = Some(build_locy_return_clause(child)?);
            }
            LocyRule::EXPLAIN | LocyRule::RULE | LocyRule::WHERE => {}
            _ => {}
        }
    }

    Ok(ExplainRule {
        rule_name: rule_name.unwrap(),
        where_expr,
        return_clause,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// SHARED RETURN CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

/// Build a ReturnClause from goal_return_clause, abduce_return_clause,
/// or explain_rule_return_clause (they all share the same structure).
fn build_locy_return_clause(pair: Pair<LocyRule>) -> Result<ast::ReturnClause, ParseError> {
    let mut distinct = false;
    let mut items = Vec::new();
    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::RETURN => {}
            LocyRule::DISTINCT => {
                distinct = true;
            }
            LocyRule::return_items => {
                items = reparse_as_cypher_return_items(child.as_str())?;
            }
            LocyRule::order_clause => {
                // order_clause = { ORDER ~ BY ~ sort_items }
                let sort_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::sort_items)
                    .unwrap();
                order_by = Some(reparse_as_cypher_sort_items(sort_pair.as_str())?);
            }
            LocyRule::skip_clause => {
                let expr_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::expression)
                    .unwrap();
                skip = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            LocyRule::limit_clause => {
                let expr_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::expression)
                    .unwrap();
                limit = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            _ => {}
        }
    }

    Ok(ast::ReturnClause {
        distinct,
        items,
        order_by,
        skip,
        limit,
    })
}
