# Code Simplifier Feedback: `crates/uni-cypher/`

Scope reviewed: `src/lib.rs`, `src/ast.rs`, `src/locy_ast.rs`, `src/plugin_aggregates.rs`, `src/grammar/{mod,walker,locy_walker,locy_parser}.rs`.

---

## 1. Duplication

### 1.1 `is_rule_reference` vs `is_not_rule_reference` are near-identical
- **Refs**: `src/grammar/locy_walker.rs:377-419` (`build_is_rule_reference`) and `src/grammar/locy_walker.rs:421-456` (`build_is_not_rule_reference`).
- **Description**: The two functions differ only in (a) accepting an extra `NOT` token in the match arm and (b) the final `negated` flag. The loop bodies, the subject/target/`saw_to` logic, and the unwraps are byte-for-byte identical.
- **Suggestion**: Collapse into one helper `build_is_reference(pair, negated: bool)` and call it from both grammar arms; add `LocyRule::NOT => {}` unconditionally to the match (harmless when absent). Removes ~40 lines.
- **Effort**: 10 min.

### 1.2 Cypher-vs-Locy error mapping (`map_pest_error` and `map_locy_pest_error`)
- **Refs**: `src/grammar/mod.rs:298-352`.
- **Description**: Both functions repeat the same four heuristic checks (invalid relationship pattern, invalid number literal, invalid unicode char, reserved keyword) with only the error-code prefix differing (`SyntaxError` vs `LocySyntaxError`). The reserved-keyword path differs slightly (Cypher requires `expects_identifier`; Locy doesn't).
- **Suggestion**: Extract a generic `classify_lexical_error(input, pos, prefix, extra_kws, gate_keyword: bool) -> Option<String>` and have both wrappers call it; `map_locy_pest_error` then only adds its context categorization.
- **Effort**: 20 min.

### 1.3 Reserved-keyword extraction maintained in two places
- **Refs**: `src/grammar/mod.rs:186-244` (`CYPHER_RESERVED_KEYWORDS`, `LOCY_RESERVED_KEYWORDS`) and grammar files (`cypher.pest`, `locy.pest`) which are the source of truth.
- **Description**: The keyword list is hand-mirrored from the grammar; comments admit so. Any grammar drift silently desyncs the error categorizer.
- **Suggestion**: Either generate from a single source (`build.rs`), or add a test that parses each keyword as a bare identifier and asserts it fails (keeps lists honest). At minimum: dedupe-test the lists for sorted/unique invariants.
- **Effort**: 30-60 min.

### 1.4 `normalize_identifier` duplicated for Locy
- **Refs**: `src/grammar/walker.rs:15-20` (`normalize_identifier`) and `src/grammar/locy_walker.rs:204-209` (`normalize_locy_identifier`).
- **Description**: Identical implementations (strip backticks).
- **Suggestion**: Move to a shared module (`super::walker::normalize_identifier` is already `pub(crate)`); delete the Locy copy and use the imported one.
- **Effort**: 5 min.

### 1.5 `build_locy_*` precedence helpers replicated four times
- **Refs**: `src/grammar/locy_walker.rs:501-565` — `build_locy_or_expression`, `build_locy_xor_expression`, `build_locy_and_expression`.
- **Description**: Three functions follow the same `(children -> fold left-assoc into BinaryOp{op})` template with only the next-level callee and `LocyBinaryOp` constant changing.
- **Suggestion**: Generic `fold_left_assoc<F>(pair, child_rule, op, recurse: F)` collapses three helpers to one. Same trick applies to `build_locy_additive_expression` / `build_locy_multiplicative_expression` (`locy_walker.rs:606-666`) once the `op` is derived from the token's `Rule`.
- **Effort**: 30 min.

### 1.6 `locy_walker.rs` re-parse helpers
- **Refs**: `src/grammar/locy_walker.rs:152-198` — seven `reparse_as_cypher_*` functions.
- **Description**: All share the shape `CypherParser::parse(rule, text).map_err(prefix).then(builder)`. They differ only in the rule and builder.
- **Suggestion**: One generic helper `fn reparse<T>(rule: CypherRule, text: &str, kind: &str, build: impl Fn(Pair) -> Result<T, ParseError>) -> Result<T, ParseError>`. Removes ~50 lines and one risk class (forgetting to update prefix when copy-pasting).
- **Effort**: 20 min.

### 1.7 `build_assume_body` is a copy of `build_locy_statement_block`
- **Refs**: `src/grammar/locy_walker.rs:77-127` vs `src/grammar/locy_walker.rs:1168-1215`.
- **Description**: The two functions enumerate every `LocyStatement` variant and flush cypher buffers identically. `build_assume_body` adds a `_ => cypher_clause_texts.push(...)` fallback but is otherwise the same dispatcher.
- **Suggestion**: Have `build_assume_body` delegate to `build_locy_statement_block` after the brace structure check. The fallback divergence (treat unknown as cypher) is the only behavioral difference and can be folded in with a flag.
- **Effort**: 20 min.

### 1.8 Locy `goal_query` / `abduce_query` / `explain_rule_query` builders
- **Refs**: `src/grammar/locy_walker.rs:1080-1107`, `1221-1253`, `1259-1286`.
- **Description**: The three builders walk a pair, extract `rule_name`, optional `expression`, optional `*_return_clause`. Body is essentially the same loop with different keyword skip-lists.
- **Suggestion**: Extract `extract_rule_query_parts(pair) -> (QualifiedName, Option<Expr>, Option<ReturnClause>)`. Each caller wraps it into its specific struct (`GoalQuery`, `AbduceQuery` adds `negated`, `ExplainRule`).
- **Effort**: 15 min.

### 1.9 `Expr::substitute_variable`, `is_aggregate`, `for_each_child`, `map_children` all enumerate every `Expr` variant
- **Refs**: `src/ast.rs:1065-1255`, `1258-1326`, `1533-1643`, `1648-1774`.
- **Description**: Four large match statements enumerate ~25 variants each — when a variant is added, four sites must be updated. `substitute_variable` is the only one with non-trivial per-variant logic (shadowing). The other three are mostly "recurse into children".
- **Suggestion**: Re-implement `is_aggregate`, `for_each_child`, `map_children` in terms of a single visitor / `for_each_child` primitive — `is_aggregate` becomes a short-circuit recursion; `map_children` already shadows `for_each_child` shape. Reduces ~400 lines and the maintenance cliff. Keep `substitute_variable` separate due to scoping semantics.
- **Effort**: 1-2 hours.

### 1.10 `ExprSuffix` and `PostfixSuffix` overlap
- **Refs**: `src/ast.rs:881-919` (`ExprSuffix`, `PostfixSuffix`).
- **Description**: Both enums encode property access, indexing, slicing, function calls. `ExprSuffix` adds `Binary/In/IsNull/IsNotNull`; `PostfixSuffix` adds `MapProjection`. Doc-comment on `PostfixSuffix` even calls out the duplication.
- **Suggestion**: Merge into a single `ExprSuffix` enum (superset of variants); resolve `apply_suffix` and `apply_suffixes` into a single function. The two-enum split is a historical artifact, not a real abstraction boundary.
- **Effort**: 45 min (touches parser).

---

## 2. Dead / Unused Code

### 2.1 Discarded `_if_exists` and `_variable`
- **Refs**: `src/grammar/walker.rs:2029` (`_variable`), `walker.rs:2077` (`_variable`), `walker.rs:2098` (`_if_exists`), `walker.rs:2109` (`_if_not_exists`), `walker.rs:2237` (`_if_exists`).
- **Description**: Parser extracts `IF EXISTS` / `IF NOT EXISTS` flags then immediately drops them. The corresponding `DropIndex`, `CreateConstraint`, `DropConstraint` AST nodes have no `if_exists` field at all (see `ast.rs:196-212`), so the semantic is silently lost.
- **Suggestion**: Either (a) add the missing field to the AST and propagate (this is functional gap, not just code smell) or (b) delete the `IF EXISTS` parse rule if it's unsupported. Currently the user-facing behavior is unspecified.
- **Effort**: 1 hour (option a).

### 2.2 Parenthesized-pattern WHERE silently dropped
- **Refs**: `src/grammar/walker.rs:1519-1521`.
- **Description**: `build_parenthesized_pattern` parses an inline `WHERE` clause then `eprintln!`s a warning and discards it. AST has no place for it.
- **Suggestion**: Either extend `PatternElement::Parenthesized` with `where_clause: Option<Expr>` or reject at the parser. `eprintln!` in a library is wrong (it writes to stderr in user processes); replace with `ParseError` if unsupported.
- **Effort**: 30 min.

### 2.3 `WithRecursiveClause.items` always empty
- **Refs**: `src/grammar/walker.rs:268-288` — `let items = vec![];` with comment "could be extracted from the query's RETURN clause".
- **Description**: AST field exists, parser never populates it. Either consumers tolerate this (dead field) or recursion is broken.
- **Suggestion**: If the field is unused downstream, remove it from `WithRecursiveClause`; if used, populate it from the inner query's RETURN.
- **Effort**: 15 min to confirm + remove or 30 min to populate.

### 2.4 `ConstraintDef` enum unused
- **Refs**: `src/ast.rs:227-233`.
- **Description**: `pub enum ConstraintDef { Unique, NodeKey, Exists, Check }` is defined but not referenced anywhere in the file. Looks superseded by `ConstraintType` (line 215).
- **Suggestion**: Verify it has no external uses, then delete.
- **Effort**: 5 min.

### 2.5 CHECK constraint loses property names
- **Refs**: `src/grammar/walker.rs:2172-2176` — `// TODO: Extract property names from expression for properties vec`.
- **Description**: TODO comment, returns empty `vec![]`. Downstream constraint validation likely incomplete.
- **Suggestion**: Either implement (walk expression for `Property` nodes) or document the limitation explicitly. Don't leave it as silently-empty.
- **Effort**: 20 min.

### 2.6 Tests in `src/lib.rs` use `println!` and emojis
- **Refs**: `src/lib.rs:114, 118, 123`.
- **Description**: Tests `println!` decorative banners and emoji status indicators. Adds noise to `cargo nextest` output and is purely aesthetic.
- **Suggestion**: Drop the `println!`s. `panic!` already produces actionable output on failure.
- **Effort**: 5 min.

---

## 3. Complex Functions

### 3.1 `walker::build_comparison_expression` (87 lines)
- **Refs**: `src/grammar/walker.rs:629-715`.
- **Description**: Builds chained comparisons, then re-clones operands while folding into AND chain (`operands[i].clone()` x N). The two-phase approach (collect operands+ops, then re-iterate to build tree) is harder to read than necessary, and the `.clone()`s are avoidable.
- **Suggestion**: Either (a) fold inline as tails are consumed, building the AND chain incrementally without intermediate `Vec`s, or (b) drain `operands`/`ops` into a single fold. Eliminates clones and ~15 lines.
- **Effort**: 30 min.

### 3.2 `walker::build_unary_expression` boundary logic
- **Refs**: `src/grammar/walker.rs:818-860`.
- **Description**: Two separate overflow checks (one for `neg_count == 0`, one for even-count) plus a `wrapping_neg` interplay with the `parse_integer_safe` `i64::MIN` sentinel. Comments are good but the control flow is intricate. The `is_multiple_of(2)` overflow check at line 850 is a subtle invariant.
- **Suggestion**: Encapsulate the integer-literal boundary handling inside a single helper `apply_negation_count(expr, count) -> Result<Expr, ParseError>`. Move the "magnitude is `i64::MAX+1`" sentinel out of the regular AST path (carry it via a separate `Result` arm) so the boundary logic isn't load-bearing on `i64::MIN` value equality.
- **Effort**: 45 min.

### 3.3 `walker::apply_postfix_suffix` nested optional handling
- **Refs**: `src/grammar/walker.rs:885-955`.
- **Description**: Three early-return paths (no args, DISTINCT then nothing, then the rule match), interleaved `Option<Pair>` shuffling. Hard to follow which combination of `DISTINCT`, args, window-spec produces which call.
- **Suggestion**: Collect all child pairs into a `Vec<Pair>` first, then dispatch by examining shape (lengths/rules) — single match instead of three layered conditionals. Or split into `parse_call_suffix` (args/distinct/window) and `parse_index_or_property_suffix`.
- **Effort**: 30 min.

### 3.4 `walker::build_list_expression` for `list_comprehension_body`
- **Refs**: `src/grammar/walker.rs:1245-1278`.
- **Description**: The optional-WHERE / optional-pipe parsing uses a chain of `if let Some(next) = ...` with nested `if next.as_rule() == WHERE` and inner pipe lookup. Easy to get wrong; the comment "After WHERE, check for optional pipe" hints at it.
- **Suggestion**: Use `peekable()` like `build_match_clause` does, with `consume_if_present(WHERE)` and `consume_if_present(pipe)` helpers. Symmetric to other clause builders.
- **Effort**: 20 min.

### 3.5 `mod::extract_token_span_at`
- **Refs**: `src/grammar/mod.rs:70-99`.
- **Description**: Mixes byte-level scanning with `is_token_char` closure including `-` (minus) as part of token, which is semantically odd for identifier detection. The "step left if not token-char" logic is unusual.
- **Suggestion**: Add a doc comment explaining the `-` inclusion (probably for negative numbers in error context), and add a test case for the "step-back" path.
- **Effort**: 15 min (documentation only) or rewrite for clarity.

### 3.6 `locy_walker::build_locy_yield_item` and `build_prob_projection` overlap
- **Refs**: `src/grammar/locy_walker.rs:867-941`.
- **Description**: Both extract `expression` + optional `alias_identifier`. `build_prob_projection` additionally derives alias from expression shape. The "is this key/prob" detection in `build_locy_yield_item` branches on the first child's rule.
- **Suggestion**: Add a private helper `extract_expr_and_alias(children) -> (Expr, Option<String>)` used by both. The is_key/is_prob discrimination stays in `build_locy_yield_item`.
- **Effort**: 15 min.

---

## 4. Unnecessary Abstractions

### 4.1 `ProjectionModifiers` struct
- **Refs**: `src/grammar/walker.rs:184-221`.
- **Description**: Used only by `build_return_clause` and `build_with_clause` to bundle four `Option<…>` fields. The struct adds a layer without much value — the two callers immediately destructure all four fields anyway.
- **Suggestion**: Could be replaced with a tuple return, but the struct is actually clearer here (named fields). **Keep**, but mention only because flagged candidate; no change.
- **Effort**: 0 (no action recommended).

### 4.2 `LabelExpr` Deref to `[String]` plus `IntoIterator`
- **Refs**: `src/ast.rs:485-502`.
- **Description**: `LabelExpr` is a sum type with semantic operator. Implementing `Deref<Target = [String]>` makes operator-erasure ambient. Doc-comment justifies it as compatibility with previous `Vec<String>` callers. This can mask logic bugs where the planner forgets to inspect operator.
- **Suggestion**: Keep the `names()` method, drop `Deref`/`IntoIterator` impls, and migrate remaining callers to explicit `.names()` / `.iter()`. Forces touchpoints to be visible. (Counter-argument: large churn — acceptable to defer.)
- **Effort**: 1-2 hours (depends on call-site count).

### 4.3 `from_conjunction` / `from_disjunction` on `LabelExpr`
- **Refs**: `src/ast.rs:458-482`.
- **Description**: Doc says "Use only when callers genuinely don't know — the parser knows and uses the explicit constructors." Smell: a constructor that exists to be avoided.
- **Suggestion**: Inline the few real users; remove the helper or rename to `from_conjunction_unchecked` to convey it's a fallback.
- **Effort**: 15 min.

### 4.4 `match_query` helper
- **Refs**: `src/grammar/walker.rs:36-44`.
- **Description**: 6-line helper wraps a `Pattern` + optional `where_clause` into `Query::Single(Statement{vec![Clause::Match(...)]})`. Used in exactly 3 sites (count subquery, exists subquery, pattern predicate). Fine.
- **Suggestion**: Keep; correctly factored.
- **Effort**: 0.

### 4.5 `plugin_aggregates` global mutable registry
- **Refs**: `src/plugin_aggregates.rs:30-55`.
- **Description**: Process-wide `OnceLock<RwLock<HashSet>>` accumulating aggregate names. Module doc-comment honestly describes this as a hack to avoid threading `PluginRegistry`. Lifecycle is write-only ("never removed today").
- **Suggestion**: Architectural — keep for now (changing would require ripple-through API change), but file an issue: an AST-level `is_aggregate` check that's stateful relative to plugin load order is a source of subtle race / test-order bugs. At minimum, document on `Expr::is_aggregate` that result depends on global state.
- **Effort**: Tracking issue only.

---

## 5. Parser / AST Inconsistencies

### 5.1 Relationship types are always `Disjunction`, but node labels carry operator
- **Refs**: `src/grammar/walker.rs:1674-1691` (rel types -> `LabelExpr::Disjunction`) vs `walker.rs:1615-1635` (node labels distinguish disjunction/conjunction).
- **Description**: `LabelExpr` is shared between `NodePattern.labels` and `RelationshipPattern.types`, but for relationships only `Disjunction` is ever constructed (per comment line 1675). Three of the four `LabelExpr` operator combinations on `types` are impossible.
- **Suggestion**: Either (a) separate types: `NodeLabels` (sum) vs `RelTypes(Vec<String>)`, or (b) document the invariant on the field and add a debug-assertion. Currently any planner code matching on `types.is_conjunction()` is dead.
- **Effort**: 1 hour (option a) or 5 min (option b).

### 5.2 `SET n:Labels` semantics not enforced
- **Refs**: `src/grammar/walker.rs:350-368` and comment `// REMOVE n:A only ever appears as conjunction…` (`walker.rs:407-412`).
- **Description**: Same pattern: parser accepts both disjunction and conjunction syntax in SET/REMOVE but flattens to a `Vec<String>`. Inconsistent: `(n:A|B)` in SET silently means "set both A and B", which is wrong semantically — `|` denotes "at least one of" in MATCH.
- **Suggestion**: Reject disjunction in SET/REMOVE at parse time with a clear error (`InvalidLabelExpression: ':A|B' is not allowed in SET/REMOVE`). Comment in code already documents the issue.
- **Effort**: 20 min.

### 5.3 `extract_dotted_name` panics vs returns `Option`
- **Refs**: `src/ast.rs:932-941` (returns `Option<String>`), `src/ast.rs:957-961` and `1006-1008` (`unwrap_or_else(|| panic!(...))`).
- **Description**: Function returns `Option` to be defensive; both real callers (`apply_suffix`, `apply_suffixes`) panic on `None`. The `Option` API gives the illusion of recoverability that isn't there.
- **Suggestion**: Either (a) have `extract_dotted_name` return `Result<String, ParseError>` so callers propagate cleanly, or (b) have the function itself panic and switch to `String`. Pick one model.
- **Effort**: 15 min.

### 5.4 Two-pass parsing in Locy: text re-parse via `CypherParser`
- **Refs**: `src/grammar/locy_walker.rs:152-198` and all `reparse_as_*` call-sites.
- **Description**: Locy walker frequently calls `CypherParser::parse(rule, child_text)` instead of consuming the already-parsed `Pair`. This double-parses many subtrees. Comment at `locy_walker.rs:585-600` admits "comparisons are complex" and just re-parses.
- **Suggestion**: For each rule that Locy also has, share grammar fragments (pest supports inclusion) and walk the existing `Pair` directly. Major refactor but eliminates a perf cost and an entire class of "text span looks right but doesn't reparse" bugs.
- **Effort**: Multi-day; tracking issue.

### 5.5 `build_locy_union_query` re-parses entire query text on UNION
- **Refs**: `src/grammar/locy_walker.rs:45-60`.
- **Description**: If a Locy query contains UNION, the entire input string is re-parsed via the Cypher parser, discarding Locy-specific results. Means `MODULE foo\nMATCH … UNION MATCH …` either loses the module declaration or fails — needs verification.
- **Suggestion**: Add a test for `MODULE x \n MATCH (a) RETURN a UNION MATCH (b) RETURN b` and verify behavior. If broken, recurse properly.
- **Effort**: 15 min (test) + variable on fix.

### 5.6 `LocyBinaryOp` is a strict subset of `BinaryOp`
- **Refs**: `src/locy_ast.rs:124-136`.
- **Description**: `LocyBinaryOp` has Add/Sub/Mul/Div/Mod/Pow/And/Or/Xor only (no comparisons — handled by Cypher re-parse). This means downstream code that walks `LocyExpr::BinaryOp` must use a different op type than for `Expr::BinaryOp`.
- **Suggestion**: Reuse `BinaryOp` directly and document that comparisons are unreachable in `LocyExpr::BinaryOp` (or add a debug assertion). Removes a parallel type to maintain.
- **Effort**: 15 min.

### 5.7 `Expr::CountSubquery` and `Expr::CollectSubquery` vs `FunctionCall("count", …)`
- **Refs**: `src/ast.rs:658-659` and `is_aggregate` treats them as aggregates (`ast.rs:1278`).
- **Description**: Two separate AST forms encode the same semantic (counted/collected subquery). Together with `Exists { from_pattern_predicate: bool }` flag (`ast.rs:653-657`), the subquery-expression layer carries multiple flags/variants for very similar things.
- **Suggestion**: Consolidate `CountSubquery`/`CollectSubquery` into a single `SubqueryAggregate { kind: CountKind | CollectKind, query }`, or normalize into `FunctionCall { name: "count"|"collect", args: [SubqueryExpr(...)] }`. Reduces variant count and simplifies `is_aggregate`, `for_each_child`, `substitute_variable`, `map_children`.
- **Effort**: 1 hour.

### 5.8 `from_pattern_predicate: bool` on `Exists`
- **Refs**: `src/ast.rs:653-657`.
- **Description**: Boolean flag distinguishing `EXISTS { … }` from bare `(n)-->()` pattern predicates. Comment makes it a parser metadata leak into the AST.
- **Suggestion**: If planners need this for semantics (e.g., NULL propagation differences), make it an enum (`ExistsKind::{Subquery, BarePattern}`); if it's only used for error messages, drop it.
- **Effort**: 15 min to investigate.

### 5.9 `Wildcard` Expr vs `ReturnItem::All`
- **Refs**: `src/ast.rs:298` (`ReturnItem::All`), `ast.rs:629` (`Expr::Wildcard`).
- **Description**: Two ways to encode `*`. `Expr::Wildcard` is also produced by `count(*)` (`walker.rs:1040`).
- **Suggestion**: Document the invariant on `Expr::Wildcard` (only valid inside aggregate args + RETURN expansion). At minimum, in `is_aggregate`/`map_children` add a comment so future contributors don't break this.
- **Effort**: 10 min.

---

## 6. Smaller issues

- `src/grammar/walker.rs:1520` — `eprintln!` in library: replace with `ParseError` or `tracing::warn!`. **5 min.**
- `src/grammar/walker.rs:1834` — comment "dot_dot consumed; check for upper bound" but the code calls `inner.next()` twice in quick succession; subtle off-by-one risk if grammar ever changes. **Add comment** or refactor to peek explicitly. **10 min.**
- `src/grammar/walker.rs:2046-2052` — `Rule::OPTIONS` arm does nothing; `Rule::map_literal` arm picks up the options. Comment "Next item should be map_literal" is informational only. Replace with single `Rule::OPTIONS => { /* skipped; map_literal follows */ }` and consume both in a `parse_options`-style helper for consistency with `parse_options` at line 1947. **15 min.**
- `src/grammar/walker.rs:2098` — `let _if_exists` for DROP INDEX should propagate to AST (see 2.1).
- `src/lib.rs:14-127` — large pass-or-fail integration test using `Vec<(name, sql)>`. Could be a `#[rstest]`-style data-driven test or split into individual `#[test]` to get per-case isolation. Cosmetic. **20 min.**
- `src/grammar/locy_walker.rs:595-601` and `:713-718` — duplicate "compute span text from first/last children, then reparse as Cypher expression" pattern. Extract `span_text(children) -> &str`. **10 min.**
- `src/grammar/locy_walker.rs:402, 439` — `child.clone().into_inner()` on a borrowed pair; in pest, `Pair` is `Clone`-cheap but the explicit clone in a loop reads as a smell. Restructure loop to consume rather than borrow. **15 min.**
- `src/ast.rs:1413-1415` — `to_string_repr` for `Exists`/`CountSubquery`/`CollectSubquery` returns the placeholder `"EXISTS {...}"`. Acceptable but limits debugging; add a `#[cfg(test)]` richer form or note the limitation. **5 min.**

---

## Priority Summary

**High value, low effort (<= 30 min each)**: 1.1, 1.4, 1.5, 1.6, 1.8, 2.4, 2.6, 5.2, 5.6, 6.* group.

**Medium**: 1.2, 1.3, 1.7, 2.1, 2.2, 2.3, 2.5, 3.1, 3.3, 3.4, 3.6, 4.3, 5.3, 5.7, 5.8.

**Large / architectural**: 1.9 (visitor refactor), 1.10 (suffix enum merge), 3.2 (i64 boundary), 4.2 (LabelExpr Deref), 5.1 (split node/rel label types), 5.4 (Locy/Cypher grammar share), 4.5 (plugin aggregate registry).
