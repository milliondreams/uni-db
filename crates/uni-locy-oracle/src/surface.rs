//! Locy program text as structured data, rendered under a renaming and an order.
//!
//! The generator used to write each rule's text by hand beside its oracle IR.
//! A [`SurfaceProgram`] holds the same text as templates whose names are
//! marked: body variables ([`Piece::Var`]), rule names ([`Piece::Rule`]) and
//! rule output columns ([`Piece::Col`]). That is what lets a test vary the text
//! without changing its meaning:
//!
//! * [`Renaming`] renames variables per clause, and rules and their output
//!   columns globally;
//! * [`Orders`] permutes the `WHERE` conjuncts of a clause.
//!
//! [`SurfaceProgram::render`] with [`Renaming::identity`] and
//! [`Orders::canonical`] gives the generator's own text, byte for byte.
//!
//! # Template syntax
//!
//! [`Tmpl::parse`] reads `{name}` as a variable, `{@rule}` as a rule name and
//! `{rule:column}` as a rule output column; everything else is literal. Locy
//! rule text never needs a literal brace.
//!
//! # Rendering a `YIELD` item
//!
//! A [`YieldItem`] is an expression and the column it fills. It renders as
//! `expr` when the expression renders to the column's name, and as
//! `expr AS column` otherwise, so a renamed `KEY a` becomes `KEY v AS a` when
//! only the variable is renamed.

// Rust guideline compliant

use std::collections::{BTreeSet, HashMap};

/// One piece of a [`Tmpl`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Piece {
    /// Literal text.
    Lit(String),
    /// A body variable of the enclosing clause.
    Var(String),
    /// A rule name.
    Rule(String),
    /// An output column `(rule, column)`: a `FOLD` or `ALONG` alias, a bare
    /// read of a referenced rule's value column, or a `prev.column`.
    Col(String, String),
}

/// A piece of rule text whose names are marked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tmpl(pub Vec<Piece>);

impl Tmpl {
    /// Parses the [template syntax](self#template-syntax).
    ///
    /// # Panics
    /// Panics on an unbalanced brace: a template is a constant in the
    /// generator, so a malformed one is a programming error.
    #[must_use]
    pub fn parse(text: &str) -> Self {
        let mut pieces = Vec::new();
        let mut rest = text;
        while let Some(open) = rest.find('{') {
            if open > 0 {
                pieces.push(Piece::Lit(rest[..open].to_string()));
            }
            let close = rest[open..]
                .find('}')
                .map(|c| open + c)
                .unwrap_or_else(|| panic!("unbalanced `{{` in template {text:?}"));
            let name = &rest[open + 1..close];
            pieces.push(if let Some(rule) = name.strip_prefix('@') {
                Piece::Rule(rule.to_string())
            } else if let Some((rule, column)) = name.split_once(':') {
                Piece::Col(rule.to_string(), column.to_string())
            } else {
                Piece::Var(name.to_string())
            });
            rest = &rest[close + 1..];
        }
        assert!(!rest.contains('}'), "unbalanced `}}` in template {text:?}");
        if !rest.is_empty() {
            pieces.push(Piece::Lit(rest.to_string()));
        }
        Self(pieces)
    }

    /// Whether the template renders to nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn render(&self, names: &Names<'_>) -> String {
        let mut out = String::new();
        for piece in &self.0 {
            match piece {
                Piece::Lit(s) => out.push_str(s),
                Piece::Var(v) => out.push_str(names.var(v)),
                Piece::Rule(r) => out.push_str(names.rule(r)),
                Piece::Col(r, c) => out.push_str(names.col(r, c)),
            }
        }
        out
    }
}

/// A `subjects IS [NOT] rule [TO target]` conjunct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceRef {
    /// An `IS NOT` reference.
    pub negated: bool,
    /// A negation written `NOT x IS r` rather than `x IS NOT r`.
    pub not_prefix_form: bool,
    /// Subject variables; two or more render as a tuple `(a, b)`.
    pub subjects: Vec<String>,
    /// The referenced rule.
    pub rule: String,
    /// The `TO` variable, if any.
    pub target: Option<String>,
}

/// One `WHERE` conjunct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Conjunct {
    /// A Cypher condition such as `{a}.id < {b}.id`.
    Filter(Tmpl),
    /// An `IS` reference.
    Ref(SurfaceRef),
}

/// One `YIELD` item: an expression filling a rule output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YieldItem {
    /// A `KEY` column.
    pub key: bool,
    /// The expression.
    pub expr: Tmpl,
    /// The rule output column it fills.
    pub column: String,
    /// The item carries the `PROB` modifier.
    pub prob: bool,
}

/// One `CREATE RULE <head> AS MATCH ...` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceClause {
    /// The rule this clause defines.
    pub head: String,
    /// The `MATCH` pattern, without the keyword.
    pub matches: Tmpl,
    /// The `WHERE` conjuncts in canonical order, joined by `", "`.
    pub conjuncts: Vec<Conjunct>,
    /// Text between the conjuncts and `YIELD`: `FOLD`, `ALONG`, `BEST BY`.
    pub tail: Tmpl,
    /// The `YIELD` items, in order.
    pub yields: Vec<YieldItem>,
}

/// One statement of a [`SurfaceProgram`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SurfaceStmt {
    /// A structured clause.
    Clause(SurfaceClause),
    /// Verbatim text defining (part of) `rule`; never renamed or reordered.
    Raw {
        /// The rule the text defines.
        rule: String,
        /// The text.
        text: String,
    },
}

impl SurfaceStmt {
    /// The rule this statement defines.
    #[must_use]
    pub fn rule(&self) -> &str {
        match self {
            Self::Clause(c) => &c.head,
            Self::Raw { rule, .. } => rule,
        }
    }
}

/// A Locy program as a list of statements, one per line.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SurfaceProgram {
    /// The statements, in program order.
    pub stmts: Vec<SurfaceStmt>,
}

/// A renaming of a program's names.
///
/// A name with no entry keeps its spelling, so [`Renaming::identity`] is the
/// empty renaming. Variables are renamed per clause (by index into
/// [`SurfaceProgram::stmts`]); rule names and output columns globally.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Renaming {
    /// Rule renames.
    pub rules: HashMap<String, String>,
    /// Output column renames, keyed by `(rule, column)` under original names.
    pub cols: HashMap<(String, String), String>,
    /// Per statement index, variable renames.
    pub vars: HashMap<usize, HashMap<String, String>>,
}

impl Renaming {
    /// The renaming that changes nothing.
    #[must_use]
    pub fn identity() -> Self {
        Self::default()
    }

    /// The new name of `rule`.
    #[must_use]
    pub fn rule<'a>(&'a self, rule: &'a str) -> &'a str {
        self.rules.get(rule).map_or(rule, String::as_str)
    }

    /// The new name of `rule`'s output column `column`.
    #[must_use]
    pub fn col<'a>(&'a self, rule: &str, column: &'a str) -> &'a str {
        self.cols
            .get(&(rule.to_string(), column.to_string()))
            .map_or(column, String::as_str)
    }
}

/// A permutation of each clause's conjuncts.
///
/// `Orders.0[i] = p` renders statement `i`'s conjuncts as
/// `p.iter().map(|&j| conjuncts[j])`; a statement without an entry keeps the
/// canonical order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Orders(pub HashMap<usize, Vec<usize>>);

impl Orders {
    /// Every clause in its canonical order.
    #[must_use]
    pub fn canonical() -> Self {
        Self::default()
    }

    /// Whether any clause is rendered in a non-canonical order.
    #[must_use]
    pub fn changes_any(&self) -> bool {
        self.0
            .values()
            .any(|p| p.iter().enumerate().any(|(i, &j)| i != j))
    }
}

/// The names in force while rendering one statement.
struct Names<'a> {
    renaming: &'a Renaming,
    vars: Option<&'a HashMap<String, String>>,
}

impl Names<'_> {
    fn var<'s>(&'s self, v: &'s str) -> &'s str {
        self.vars.and_then(|m| m.get(v)).map_or(v, String::as_str)
    }

    fn rule<'s>(&'s self, r: &'s str) -> &'s str {
        self.renaming.rule(r)
    }

    fn col<'s>(&'s self, r: &str, c: &'s str) -> &'s str {
        self.renaming.col(r, c)
    }
}

impl SurfaceRef {
    fn render(&self, names: &Names<'_>) -> String {
        let subjects: Vec<&str> = self.subjects.iter().map(|s| names.var(s)).collect();
        let subject = if subjects.len() == 1 {
            subjects[0].to_string()
        } else {
            format!("({})", subjects.join(", "))
        };
        let target = self
            .target
            .as_ref()
            .map(|t| format!(" TO {}", names.var(t)))
            .unwrap_or_default();
        let rule = names.rule(&self.rule);
        match (self.negated, self.not_prefix_form) {
            (false, _) => format!("{subject} IS {rule}{target}"),
            (true, false) => format!("{subject} IS NOT {rule}{target}"),
            (true, true) => format!("NOT {subject} IS {rule}{target}"),
        }
    }
}

impl Conjunct {
    fn render(&self, names: &Names<'_>) -> String {
        match self {
            Self::Filter(t) => t.render(names),
            Self::Ref(r) => r.render(names),
        }
    }
}

impl SurfaceClause {
    fn render(&self, names: &Names<'_>, order: Option<&Vec<usize>>) -> String {
        let mut out = format!(
            "CREATE RULE {} AS MATCH {}",
            names.rule(&self.head),
            self.matches.render(names)
        );
        if !self.conjuncts.is_empty() {
            let canonical: Vec<usize> = (0..self.conjuncts.len()).collect();
            let order = order.unwrap_or(&canonical);
            let body: Vec<String> = order
                .iter()
                .map(|&j| self.conjuncts[j].render(names))
                .collect();
            out.push_str(" WHERE ");
            out.push_str(&body.join(", "));
        }
        if !self.tail.is_empty() {
            out.push(' ');
            out.push_str(&self.tail.render(names));
        }
        let items: Vec<String> = self
            .yields
            .iter()
            .map(|item| {
                let expr = item.expr.render(names);
                let column = names.col(&self.head, &item.column);
                let mut s = String::new();
                if item.key {
                    s.push_str("KEY ");
                }
                s.push_str(&expr);
                if expr != column {
                    s.push_str(" AS ");
                    s.push_str(column);
                }
                if item.prob {
                    s.push_str(" PROB");
                }
                s
            })
            .collect();
        out.push_str(" YIELD ");
        out.push_str(&items.join(", "));
        out
    }

    /// Every variable the clause mentions, in its pattern, conjuncts, tail or
    /// `YIELD`.
    #[must_use]
    pub fn vars(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for t in self.templates() {
            out.extend(t.0.iter().filter_map(|p| match p {
                Piece::Var(v) => Some(v.clone()),
                _ => None,
            }));
        }
        for (_, r) in self.refs() {
            out.extend(r.subjects.iter().chain(&r.target).cloned());
        }
        out
    }

    /// Every output column the clause's text spells, as `(rule, column)`:
    /// aliases, bare reads, `prev` reads and the `YIELD` columns.
    #[must_use]
    pub fn cols(&self) -> BTreeSet<(String, String)> {
        let mut out = BTreeSet::new();
        for t in self.templates() {
            out.extend(t.0.iter().filter_map(|p| match p {
                Piece::Col(r, c) => Some((r.clone(), c.clone())),
                _ => None,
            }));
        }
        for item in &self.yields {
            out.insert((self.head.clone(), item.column.clone()));
        }
        out
    }

    /// Every template of the clause: pattern, filters, tail, `YIELD` items.
    fn templates(&self) -> impl Iterator<Item = &Tmpl> {
        let filters = self.conjuncts.iter().filter_map(|c| match c {
            Conjunct::Filter(t) => Some(t),
            Conjunct::Ref(_) => None,
        });
        [&self.matches, &self.tail]
            .into_iter()
            .chain(filters)
            .chain(self.yields.iter().map(|y| &y.expr))
    }

    /// The `IS` references among the conjuncts, with their canonical index.
    pub fn refs(&self) -> impl Iterator<Item = (usize, &SurfaceRef)> {
        self.conjuncts
            .iter()
            .enumerate()
            .filter_map(|(i, c)| match c {
                Conjunct::Ref(r) => Some((i, r)),
                Conjunct::Filter(_) => None,
            })
    }
}

impl SurfaceProgram {
    /// Renders the program, one statement per line.
    ///
    /// # Panics
    /// Panics if an [`Orders`] entry is not a permutation of its clause's
    /// conjuncts, a programming error of the caller.
    #[must_use]
    pub fn render(&self, renaming: &Renaming, orders: &Orders) -> String {
        self.render_where(renaming, orders, &|_| true)
    }

    /// Renders only the statements of rules `keep` accepts, with the
    /// renaming and orders of the whole program (statement indices are not
    /// shifted).
    ///
    /// # Panics
    /// As [`render`](Self::render).
    #[must_use]
    pub fn render_where(
        &self,
        renaming: &Renaming,
        orders: &Orders,
        keep: &dyn Fn(&str) -> bool,
    ) -> String {
        let lines: Vec<String> = self
            .stmts
            .iter()
            .enumerate()
            .filter(|(_, stmt)| keep(stmt.rule()))
            .map(|(i, stmt)| match stmt {
                SurfaceStmt::Raw { text, .. } => text.clone(),
                SurfaceStmt::Clause(c) => {
                    let order = orders.0.get(&i);
                    if let Some(p) = order {
                        let mut sorted = p.clone();
                        sorted.sort_unstable();
                        assert!(
                            sorted == (0..c.conjuncts.len()).collect::<Vec<_>>(),
                            "order {p:?} is not a permutation of {} conjuncts",
                            c.conjuncts.len()
                        );
                    }
                    let names = Names {
                        renaming,
                        vars: renaming.vars.get(&i),
                    };
                    c.render(&names, order)
                }
            })
            .collect();
        lines.join("\n")
    }

    /// The program in its canonical form: no renaming, canonical order.
    #[must_use]
    pub fn canonical_text(&self) -> String {
        self.render(&Renaming::identity(), &Orders::canonical())
    }

    /// The structured clauses, with their statement index.
    pub fn clauses(&self) -> impl Iterator<Item = (usize, &SurfaceClause)> {
        self.stmts.iter().enumerate().filter_map(|(i, s)| match s {
            SurfaceStmt::Clause(c) => Some((i, c)),
            SurfaceStmt::Raw { .. } => None,
        })
    }

    /// The program without the statements of rules `keep` rejects.
    #[must_use]
    pub fn retain(&self, keep: &dyn Fn(&str) -> bool) -> Self {
        Self {
            stmts: self
                .stmts
                .iter()
                .filter(|s| keep(s.rule()))
                .cloned()
                .collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Builders: short constructors the generator writes its templates with.
// ---------------------------------------------------------------------------

/// A filter conjunct from the [template syntax](self#template-syntax).
#[must_use]
pub fn filter(template: &str) -> Conjunct {
    Conjunct::Filter(Tmpl::parse(template))
}

/// A positive reference `subjects IS rule [TO target]`.
#[must_use]
pub fn is_ref(subjects: &[&str], rule: &str, target: Option<&str>) -> Conjunct {
    Conjunct::Ref(SurfaceRef {
        negated: false,
        not_prefix_form: false,
        subjects: subjects.iter().map(|s| (*s).to_string()).collect(),
        rule: rule.to_string(),
        target: target.map(str::to_string),
    })
}

/// A negated reference, written `NOT x IS r` when `prefix`, else `x IS NOT r`.
#[must_use]
pub fn is_not_ref(subjects: &[&str], rule: &str, target: Option<&str>, prefix: bool) -> Conjunct {
    Conjunct::Ref(SurfaceRef {
        negated: true,
        not_prefix_form: prefix,
        subjects: subjects.iter().map(|s| (*s).to_string()).collect(),
        rule: rule.to_string(),
        target: target.map(str::to_string),
    })
}

/// `KEY var`, filling the column named like the variable.
#[must_use]
pub fn key_var(var: &str) -> YieldItem {
    YieldItem {
        key: true,
        expr: Tmpl(vec![Piece::Var(var.to_string())]),
        column: var.to_string(),
        prob: false,
    }
}

/// A non-KEY `var`, filling the column named like the variable.
#[must_use]
pub fn value_var(var: &str) -> YieldItem {
    YieldItem {
        key: false,
        ..key_var(var)
    }
}

/// `[KEY] expr AS column`, the expression in template syntax.
#[must_use]
pub fn item(key: bool, expr: &str, column: &str) -> YieldItem {
    YieldItem {
        key,
        expr: Tmpl::parse(expr),
        column: column.to_string(),
        prob: false,
    }
}

/// A non-KEY item naming `rule`'s own column `column` (a `FOLD` or `ALONG`
/// alias): renders as the bare column name.
#[must_use]
pub fn own_col(rule: &str, column: &str) -> YieldItem {
    YieldItem {
        key: false,
        expr: Tmpl(vec![Piece::Col(rule.to_string(), column.to_string())]),
        column: column.to_string(),
        prob: false,
    }
}

/// A clause of `head` from template-syntax pattern and tail.
#[must_use]
pub fn clause(
    head: &str,
    matches: &str,
    conjuncts: Vec<Conjunct>,
    tail: &str,
    yields: Vec<YieldItem>,
) -> SurfaceStmt {
    SurfaceStmt::Clause(SurfaceClause {
        head: head.to_string(),
        matches: Tmpl::parse(matches),
        conjuncts,
        tail: Tmpl::parse(tail),
        yields,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> SurfaceProgram {
        SurfaceProgram {
            stmts: vec![
                clause(
                    "reach",
                    "({a}:Node)-[:EDGE]->({m}:Node)",
                    vec![is_ref(&["m"], "reach", Some("b"))],
                    "",
                    vec![key_var("a"), key_var("b")],
                ),
                clause(
                    "tot",
                    "({a}:Node)",
                    vec![
                        filter("{a}.id < 3"),
                        is_ref(&["a"], "cost", Some("b")),
                        is_not_ref(&["a", "b"], "un", None, true),
                    ],
                    "FOLD {tot:n} = COUNT(*), {tot:s} = SUM({cost:q})",
                    vec![key_var("a"), own_col("tot", "n"), own_col("tot", "s")],
                ),
            ],
        }
    }

    #[test]
    fn identity_renders_canonical_text() {
        assert_eq!(
            sample().canonical_text(),
            "CREATE RULE reach AS MATCH (a:Node)-[:EDGE]->(m:Node) WHERE m IS reach TO b \
             YIELD KEY a, KEY b\n\
             CREATE RULE tot AS MATCH (a:Node) WHERE a.id < 3, a IS cost TO b, \
             NOT (a, b) IS un FOLD n = COUNT(*), s = SUM(q) YIELD KEY a, n, s"
        );
    }

    #[test]
    fn renaming_and_order_apply_everywhere() {
        let mut renaming = Renaming::identity();
        renaming.rules.insert("cost".into(), "c2".into());
        renaming
            .cols
            .insert(("cost".into(), "q".into()), "q2".into());
        renaming.cols.insert(("tot".into(), "a".into()), "k".into());
        renaming
            .vars
            .insert(1, HashMap::from([("a".to_string(), "z".to_string())]));
        let orders = Orders(HashMap::from([(1, vec![2, 0, 1])]));
        let text = sample().render(&renaming, &orders);
        assert_eq!(
            text.lines().nth(1).expect("two lines"),
            "CREATE RULE tot AS MATCH (z:Node) WHERE NOT (z, b) IS un, z.id < 3, \
             z IS c2 TO b FOLD n = COUNT(*), s = SUM(q2) YIELD KEY z AS k, n, s"
        );
        assert!(orders.changes_any());
    }

    #[test]
    fn parse_round_trips_marks() {
        assert_eq!(
            Tmpl::parse("{a}.id < {@r} {r:c}x").0,
            vec![
                Piece::Var("a".into()),
                Piece::Lit(".id < ".into()),
                Piece::Rule("r".into()),
                Piece::Lit(" ".into()),
                Piece::Col("r".into(), "c".into()),
                Piece::Lit("x".into()),
            ]
        );
    }
}
