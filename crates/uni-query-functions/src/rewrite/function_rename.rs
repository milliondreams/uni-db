//! Function-call name-substitution walker.
//!
//! Used by the planner to apply `ReplacementScanProvider`-driven function
//! rewrites (M5 follow-up #5) as an AST pass before logical planning. The
//! walker descends into all expression-bearing positions and calls a
//! caller-supplied closure for each [`Expr::FunctionCall`] name. If the
//! closure returns `Some(new_name)`, the call's name is substituted in
//! place; arguments are recursively walked either way. Errors short-circuit
//! the entire traversal.
//!
//! Mirrors the traversal shape of
//! [`crate::rewrite::walker::ExpressionWalker`] — the two could
//! eventually share a visitor trait, but for now the duplication is
//! deliberate: this walker takes `&mut FnMut` (so it can capture the
//! planner's `&self` plus mutable state for hop-cap enforcement) and
//! propagates `Result` (so a wrong-variant or already-rerouted error
//! aborts cleanly), neither of which the rule-driven walker supports.

use anyhow::Result;
use uni_cypher::ast::{
    Clause, Expr, MapProjectionItem, Pattern, PatternElement, Query, RemoveItem, ReturnItem,
    SetItem, SortItem, Statement,
};

/// Walk `query`, calling `rename` on every [`Expr::FunctionCall`] name
/// (post-order: arguments are visited first). When `rename` returns
/// `Some(new_name)`, the call's `name` is replaced. When it returns
/// `None`, the original name is kept. Errors propagate.
pub fn rewrite_function_calls_in_query<F>(query: Query, rename: &mut F) -> Result<Query>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    match query {
        Query::Single(stmt) => Ok(Query::Single(rewrite_statement(stmt, rename)?)),
        Query::Union { left, right, all } => Ok(Query::Union {
            left: Box::new(rewrite_function_calls_in_query(*left, rename)?),
            right: Box::new(rewrite_function_calls_in_query(*right, rename)?),
            all,
        }),
        Query::Schema(s) => Ok(Query::Schema(s)),
        Query::Explain(inner) => Ok(Query::Explain(Box::new(rewrite_function_calls_in_query(
            *inner, rename,
        )?))),
        Query::TimeTravel { .. } => Ok(query),
    }
}

fn rewrite_statement<F>(stmt: Statement, rename: &mut F) -> Result<Statement>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    let mut clauses = Vec::with_capacity(stmt.clauses.len());
    for c in stmt.clauses {
        clauses.push(rewrite_clause(c, rename)?);
    }
    Ok(Statement { clauses })
}

fn rewrite_clause<F>(clause: Clause, rename: &mut F) -> Result<Clause>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    Ok(match clause {
        Clause::Match(m) => Clause::Match(uni_cypher::ast::MatchClause {
            optional: m.optional,
            for_update: m.for_update,
            pattern: rewrite_pattern(m.pattern, rename)?,
            where_clause: opt_expr(m.where_clause, rename)?,
        }),
        Clause::Create(c) => Clause::Create(uni_cypher::ast::CreateClause {
            pattern: rewrite_pattern(c.pattern, rename)?,
        }),
        Clause::Return(r) => Clause::Return(uni_cypher::ast::ReturnClause {
            distinct: r.distinct,
            items: r
                .items
                .into_iter()
                .map(|item| rewrite_return_item(item, rename))
                .collect::<Result<_>>()?,
            order_by: rewrite_order_by(r.order_by, rename)?,
            skip: opt_expr(r.skip, rename)?,
            limit: opt_expr(r.limit, rename)?,
        }),
        Clause::With(w) => Clause::With(uni_cypher::ast::WithClause {
            distinct: w.distinct,
            items: w
                .items
                .into_iter()
                .map(|item| rewrite_return_item(item, rename))
                .collect::<Result<_>>()?,
            order_by: rewrite_order_by(w.order_by, rename)?,
            skip: opt_expr(w.skip, rename)?,
            limit: opt_expr(w.limit, rename)?,
            where_clause: opt_expr(w.where_clause, rename)?,
        }),
        Clause::Unwind(u) => Clause::Unwind(uni_cypher::ast::UnwindClause {
            expr: rewrite_expr(u.expr, rename)?,
            variable: u.variable,
        }),
        Clause::Set(s) => Clause::Set(uni_cypher::ast::SetClause {
            items: s
                .items
                .into_iter()
                .map(|item| rewrite_set_item(item, rename))
                .collect::<Result<_>>()?,
        }),
        Clause::Delete(d) => Clause::Delete(uni_cypher::ast::DeleteClause {
            detach: d.detach,
            items: d
                .items
                .into_iter()
                .map(|e| rewrite_expr(e, rename))
                .collect::<Result<_>>()?,
        }),
        Clause::Remove(r) => Clause::Remove(uni_cypher::ast::RemoveClause {
            items: r
                .items
                .into_iter()
                .map(|item| rewrite_remove_item(item, rename))
                .collect::<Result<_>>()?,
        }),
        Clause::Call(mut call) => {
            // Procedure arguments and YIELD where-clauses can carry FunctionCalls.
            match &mut call.kind {
                uni_cypher::ast::CallKind::Procedure { arguments, .. } => {
                    let mut new_args = Vec::with_capacity(arguments.len());
                    for a in arguments.drain(..) {
                        new_args.push(rewrite_expr(a, rename)?);
                    }
                    *arguments = new_args;
                }
                uni_cypher::ast::CallKind::Subquery(query) => {
                    let q = std::mem::replace(
                        query.as_mut(),
                        Query::Single(Statement { clauses: vec![] }),
                    );
                    **query = rewrite_function_calls_in_query(q, rename)?;
                }
            }
            if let Some(w) = call.where_clause.take() {
                call.where_clause = Some(rewrite_expr(w, rename)?);
            }
            Clause::Call(call)
        }
        Clause::Merge(m) => Clause::Merge(uni_cypher::ast::MergeClause {
            pattern: rewrite_pattern(m.pattern, rename)?,
            on_match: m
                .on_match
                .into_iter()
                .map(|item| rewrite_set_item(item, rename))
                .collect::<Result<_>>()?,
            on_create: m
                .on_create
                .into_iter()
                .map(|item| rewrite_set_item(item, rename))
                .collect::<Result<_>>()?,
        }),
        Clause::WithRecursive(wr) => Clause::WithRecursive(uni_cypher::ast::WithRecursiveClause {
            name: wr.name,
            query: Box::new(rewrite_function_calls_in_query(*wr.query, rename)?),
            items: wr
                .items
                .into_iter()
                .map(|item| rewrite_return_item(item, rename))
                .collect::<Result<_>>()?,
        }),
    })
}

fn rewrite_set_item<F>(item: SetItem, rename: &mut F) -> Result<SetItem>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    Ok(match item {
        SetItem::Property { expr, value } => SetItem::Property {
            expr: rewrite_expr(expr, rename)?,
            value: rewrite_expr(value, rename)?,
        },
        SetItem::Variable { variable, value } => SetItem::Variable {
            variable,
            value: rewrite_expr(value, rename)?,
        },
        SetItem::VariablePlus { variable, value } => SetItem::VariablePlus {
            variable,
            value: rewrite_expr(value, rename)?,
        },
        SetItem::Labels { variable, labels } => SetItem::Labels { variable, labels },
    })
}

fn rewrite_remove_item<F>(item: RemoveItem, rename: &mut F) -> Result<RemoveItem>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    Ok(match item {
        RemoveItem::Property(e) => RemoveItem::Property(rewrite_expr(e, rename)?),
        RemoveItem::Labels { variable, labels } => RemoveItem::Labels { variable, labels },
    })
}

fn rewrite_return_item<F>(item: ReturnItem, rename: &mut F) -> Result<ReturnItem>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    Ok(match item {
        ReturnItem::All => ReturnItem::All,
        ReturnItem::Expr {
            expr,
            alias,
            source_text,
        } => ReturnItem::Expr {
            expr: rewrite_expr(expr, rename)?,
            alias,
            source_text,
        },
    })
}

fn rewrite_order_by<F>(
    order_by: Option<Vec<SortItem>>,
    rename: &mut F,
) -> Result<Option<Vec<SortItem>>>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    let Some(items) = order_by else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(SortItem {
            expr: rewrite_expr(item.expr, rename)?,
            ascending: item.ascending,
        });
    }
    Ok(Some(out))
}

fn rewrite_pattern<F>(pattern: Pattern, rename: &mut F) -> Result<Pattern>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    let mut paths = Vec::with_capacity(pattern.paths.len());
    for path in pattern.paths {
        paths.push(uni_cypher::ast::PathPattern {
            variable: path.variable,
            elements: path
                .elements
                .into_iter()
                .map(|e| rewrite_pattern_element(e, rename))
                .collect::<Result<_>>()?,
            shortest_path_mode: path.shortest_path_mode,
        });
    }
    Ok(Pattern { paths })
}

fn rewrite_pattern_element<F>(elem: PatternElement, rename: &mut F) -> Result<PatternElement>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    Ok(match elem {
        PatternElement::Node(n) => PatternElement::Node(uni_cypher::ast::NodePattern {
            variable: n.variable,
            labels: n.labels,
            properties: opt_expr(n.properties, rename)?,
            where_clause: opt_expr(n.where_clause, rename)?,
        }),
        PatternElement::Relationship(r) => {
            PatternElement::Relationship(uni_cypher::ast::RelationshipPattern {
                variable: r.variable,
                types: r.types,
                direction: r.direction,
                properties: opt_expr(r.properties, rename)?,
                range: r.range,
                where_clause: opt_expr(r.where_clause, rename)?,
            })
        }
        PatternElement::Parenthesized { pattern, range } => PatternElement::Parenthesized {
            pattern: Box::new(uni_cypher::ast::PathPattern {
                variable: pattern.variable,
                elements: pattern
                    .elements
                    .into_iter()
                    .map(|e| rewrite_pattern_element(e, rename))
                    .collect::<Result<_>>()?,
                shortest_path_mode: pattern.shortest_path_mode,
            }),
            range,
        },
    })
}

fn opt_expr<F>(e: Option<Expr>, rename: &mut F) -> Result<Option<Expr>>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    match e {
        Some(e) => Ok(Some(rewrite_expr(e, rename)?)),
        None => Ok(None),
    }
}

fn rewrite_expr<F>(expr: Expr, rename: &mut F) -> Result<Expr>
where
    F: FnMut(&str) -> Result<Option<String>>,
{
    Ok(match expr {
        Expr::FunctionCall {
            name,
            args,
            distinct,
            window_spec,
        } => {
            let mut new_args = Vec::with_capacity(args.len());
            for a in args {
                new_args.push(rewrite_expr(a, rename)?);
            }
            let new_name = rename(&name)?.unwrap_or(name);
            Expr::FunctionCall {
                name: new_name,
                args: new_args,
                distinct,
                window_spec,
            }
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_expr(*left, rename)?),
            op,
            right: Box::new(rewrite_expr(*right, rename)?),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op,
            expr: Box::new(rewrite_expr(*expr, rename)?),
        },
        Expr::Property(base, prop) => Expr::Property(Box::new(rewrite_expr(*base, rename)?), prop),
        Expr::List(exprs) => Expr::List(
            exprs
                .into_iter()
                .map(|e| rewrite_expr(e, rename))
                .collect::<Result<_>>()?,
        ),
        Expr::Map(entries) => {
            let mut out = Vec::with_capacity(entries.len());
            for (k, v) in entries {
                out.push((k, rewrite_expr(v, rename)?));
            }
            Expr::Map(out)
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            let expr = match expr {
                Some(e) => Some(Box::new(rewrite_expr(*e, rename)?)),
                None => None,
            };
            let mut new_when = Vec::with_capacity(when_then.len());
            for (w, t) in when_then {
                new_when.push((rewrite_expr(w, rename)?, rewrite_expr(t, rename)?));
            }
            let else_expr = match else_expr {
                Some(e) => Some(Box::new(rewrite_expr(*e, rename)?)),
                None => None,
            };
            Expr::Case {
                expr,
                when_then: new_when,
                else_expr,
            }
        }
        Expr::Exists {
            query,
            from_pattern_predicate,
        } => Expr::Exists {
            query: Box::new(rewrite_function_calls_in_query(*query, rename)?),
            from_pattern_predicate,
        },
        Expr::CountSubquery(q) => {
            Expr::CountSubquery(Box::new(rewrite_function_calls_in_query(*q, rename)?))
        }
        Expr::CollectSubquery(q) => {
            Expr::CollectSubquery(Box::new(rewrite_function_calls_in_query(*q, rename)?))
        }
        Expr::IsNull(e) => Expr::IsNull(Box::new(rewrite_expr(*e, rename)?)),
        Expr::IsNotNull(e) => Expr::IsNotNull(Box::new(rewrite_expr(*e, rename)?)),
        Expr::IsUnique(e) => Expr::IsUnique(Box::new(rewrite_expr(*e, rename)?)),
        Expr::In { expr, list } => Expr::In {
            expr: Box::new(rewrite_expr(*expr, rename)?),
            list: Box::new(rewrite_expr(*list, rename)?),
        },
        Expr::ArrayIndex { array, index } => Expr::ArrayIndex {
            array: Box::new(rewrite_expr(*array, rename)?),
            index: Box::new(rewrite_expr(*index, rename)?),
        },
        Expr::ArraySlice { array, start, end } => Expr::ArraySlice {
            array: Box::new(rewrite_expr(*array, rename)?),
            start: match start {
                Some(s) => Some(Box::new(rewrite_expr(*s, rename)?)),
                None => None,
            },
            end: match end {
                Some(e) => Some(Box::new(rewrite_expr(*e, rename)?)),
                None => None,
            },
        },
        Expr::Quantifier {
            quantifier,
            variable,
            list,
            predicate,
        } => Expr::Quantifier {
            quantifier,
            variable,
            list: Box::new(rewrite_expr(*list, rename)?),
            predicate: Box::new(rewrite_expr(*predicate, rename)?),
        },
        Expr::Reduce {
            accumulator,
            init,
            variable,
            list,
            expr,
        } => Expr::Reduce {
            accumulator,
            init: Box::new(rewrite_expr(*init, rename)?),
            variable,
            list: Box::new(rewrite_expr(*list, rename)?),
            expr: Box::new(rewrite_expr(*expr, rename)?),
        },
        Expr::ListComprehension {
            variable,
            list,
            where_clause,
            map_expr,
        } => Expr::ListComprehension {
            variable,
            list: Box::new(rewrite_expr(*list, rename)?),
            where_clause: match where_clause {
                Some(w) => Some(Box::new(rewrite_expr(*w, rename)?)),
                None => None,
            },
            map_expr: Box::new(rewrite_expr(*map_expr, rename)?),
        },
        Expr::PatternComprehension {
            path_variable,
            pattern,
            where_clause,
            map_expr,
        } => Expr::PatternComprehension {
            path_variable,
            pattern: rewrite_pattern(pattern, rename)?,
            where_clause: match where_clause {
                Some(w) => Some(Box::new(rewrite_expr(*w, rename)?)),
                None => None,
            },
            map_expr: Box::new(rewrite_expr(*map_expr, rename)?),
        },
        Expr::ValidAt {
            entity,
            timestamp,
            start_prop,
            end_prop,
        } => Expr::ValidAt {
            entity: Box::new(rewrite_expr(*entity, rename)?),
            timestamp: Box::new(rewrite_expr(*timestamp, rename)?),
            start_prop,
            end_prop,
        },
        Expr::MapProjection { base, items } => {
            let mut new_items = Vec::with_capacity(items.len());
            for item in items {
                new_items.push(match item {
                    MapProjectionItem::LiteralEntry(k, v) => {
                        MapProjectionItem::LiteralEntry(k, Box::new(rewrite_expr(*v, rename)?))
                    }
                    other => other,
                });
            }
            Expr::MapProjection {
                base: Box::new(rewrite_expr(*base, rename)?),
                items: new_items,
            }
        }
        Expr::LabelCheck { expr, labels } => Expr::LabelCheck {
            expr: Box::new(rewrite_expr(*expr, rename)?),
            labels,
        },
        // Leaves.
        leaf @ (Expr::Literal(_) | Expr::Parameter(_) | Expr::Variable(_) | Expr::Wildcard) => leaf,
    })
}
