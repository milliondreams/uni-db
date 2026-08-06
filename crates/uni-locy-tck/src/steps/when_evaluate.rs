use crate::LocyWorld;
use cucumber::when;
use uni_locy::LocyConfig;

/// True when a `QUERY ... WHERE` clause cannot remove any row.
///
/// The corpus idiom is `QUERY r WHERE n = n RETURN ...`, a self-comparison used
/// to bind the goal variable rather than to filter. It appears in **56 of the
/// 99** `QUERY ... WHERE` clauses in the feature files; treating every `WHERE`
/// as potentially-filtering would drop the parity check from 71 of 114 queries
/// to 15, which is the difference between a guard with teeth and a decorative
/// one.
///
/// Deliberately conservative: only `x = x` on a bare variable, and `AND` of
/// such terms, count as tautologies. Anything else — a property comparison, a
/// parameter, a function call — is treated as a real filter and skipped.
fn where_clause_cannot_filter(expr: &uni_cypher::ast::Expr) -> bool {
    use uni_cypher::ast::{BinaryOp, Expr};
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOp::Eq => {
                matches!((left.as_ref(), right.as_ref()),
                    (Expr::Variable(a), Expr::Variable(b)) if a == b)
            }
            BinaryOp::And => where_clause_cannot_filter(left) && where_clause_cannot_filter(right),
            _ => false,
        },
        _ => false,
    }
}

/// Asserts that `QUERY <rule>` does not go vacuous while `derived[<rule>]` holds
/// facts — checked on **every** scenario that evaluates a program.
///
/// `derived` is served by the DataFusion semi-naive fixpoint; `QUERY` is served
/// by a separate top-down SLG resolver that re-derives from scratch. The two
/// engines do not have equal expressive power, so a rule can derive facts on one
/// surface and return nothing on the other
/// (<https://github.com/rustic-ai/uni-db/issues/160>). That is a silent
/// wrong-answer bug: an ad-hoc `QUERY` reports "nothing found" about data that
/// does contain matches.
///
/// `SemanticParity.feature` already tried to catch this class with seven
/// hand-written scenarios, and #160 slipped through the gap between them. This
/// is the same idea moved to the funnel every evaluation passes through, so
/// coverage is a property of the corpus rather than of what someone remembered
/// to write.
///
/// **Non-vacuity, not equality.** Strict row-count equality is not assertable
/// generically because `QUERY` carries `WHERE` / `RETURN` / `LIMIT`. But no
/// projection, `DISTINCT`, `ORDER BY` or aggregate can turn a non-empty
/// relation into zero rows — only a `WHERE` or a `LIMIT` can. So for a `QUERY`
/// with neither, non-empty `derived` implies non-empty rows. Every other shape
/// is skipped rather than guessed at.
fn assert_query_derived_parity(program: &str, result: &uni_locy::LocyResult) {
    use uni_cypher::locy_ast::LocyStatement;

    // A program the TCK feeds us that does not re-parse is not this guard's
    // problem — the scenario's own assertions cover it.
    let Ok(ast) = uni_cypher::parse_locy(program) else {
        return;
    };

    let goal_queries: Vec<_> = ast
        .statements
        .iter()
        .filter_map(|s| match s {
            LocyStatement::GoalQuery(gq) => Some(gq),
            _ => None,
        })
        .collect();

    let query_rows: Vec<_> = result
        .command_results
        .iter()
        .filter_map(|cr| match cr {
            uni_locy::CommandResult::Query(rows) => Some(rows),
            _ => None,
        })
        .collect();

    // Both sequences follow program order, so index i is the same QUERY on both
    // sides. If the counts disagree the alignment is unknown — skip rather than
    // risk blaming the wrong rule.
    if goal_queries.len() != query_rows.len() {
        return;
    }

    for (gq, rows) in goal_queries.iter().zip(query_rows) {
        // A real WHERE or a LIMIT can legitimately empty the result; a
        // self-comparison tautology cannot.
        if gq
            .where_expr
            .as_ref()
            .is_some_and(|w| !where_clause_cannot_filter(w))
        {
            continue;
        }
        if gq.return_clause.as_ref().is_some_and(|r| r.limit.is_some()) {
            continue;
        }

        let rule = gq.rule_name.to_string();
        let Some(derived) = result.derived.get(&rule) else {
            continue;
        };
        if derived.is_empty() || !rows.is_empty() {
            continue;
        }

        panic!(
            "QUERY/derived parity violated for rule `{rule}`: the fixpoint derived \
             {} fact(s) but QUERY returned 0 rows, and the query has no WHERE or \
             LIMIT that could explain it.\n\
             This is the issue #160 class: `derived` (fixpoint) and `QUERY` (SLG) \
             disagree. A known trigger is an IS-ref that introduces a variable \
             binding the MATCH pattern does not provide — the SLG resolver can \
             filter on an already-bound subject but cannot bind a fresh one.\n\
             Program:\n{program}",
            derived.len()
        );
    }
}

/// If the Locy evaluation produced a `DerivedFactSet`, apply it to the database
/// via a transaction so that DERIVE mutations are visible to subsequent `then` steps.
///
/// Session-level DERIVE uses `collect_derive: true`, which defers mutations into a
/// `DerivedFactSet` instead of auto-applying. The TCK expects mutations to be
/// visible immediately, so we apply them here and update `stats.mutations_executed`
/// to reflect the actual mutations performed.
async fn apply_derived_and_store(
    world: &mut LocyWorld,
    program: &str,
    result: Result<uni_db::locy::LocyResult, uni_common::UniError>,
) {
    store_result(world, program, result, true).await;
}

/// Store the evaluation result without applying derived facts.
/// Used by scenarios that test DERIVE isolation (e.g. "edges do not persist without tx.apply").
async fn store_without_apply(
    world: &mut LocyWorld,
    program: &str,
    result: Result<uni_db::locy::LocyResult, uni_common::UniError>,
) {
    store_result(world, program, result, false).await;
}

async fn store_result(
    world: &mut LocyWorld,
    program: &str,
    result: Result<uni_db::locy::LocyResult, uni_common::UniError>,
    apply_derived: bool,
) {
    let result = match result {
        Ok(locy_result) => {
            let mut inner = locy_result.into_inner();
            if apply_derived {
                if let Some(derived) = inner.derived_fact_set.clone() {
                    let session = world.db().session();
                    let tx = session
                        .tx()
                        .await
                        .expect("Failed to start transaction for DERIVE apply");
                    let apply_result = tx
                        .apply(derived)
                        .await
                        .expect("Failed to apply derived facts");
                    tx.commit().await.expect("Failed to commit derived facts");
                    inner.stats.mutations_executed += apply_result.facts_applied;
                }
            }
            // Checked on every scenario, not just the parity feature. An `Err`
            // result is skipped — a failed evaluation has no parity to violate.
            assert_query_derived_parity(program, &inner);
            Ok(inner)
        }
        Err(e) => Err(e),
    };
    world.set_locy_result(result);
}

#[when("evaluating the following Locy program:")]
async fn when_evaluating_locy_program(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let result = world.db().session().locy(program).await;
    apply_derived_and_store(world, program, result).await;
}

#[when("evaluating the following Locy program without applying derived facts:")]
async fn when_evaluating_locy_program_without_apply(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let result = world.db().session().locy(program).await;
    store_without_apply(world, program, result).await;
}

/// Initialize the DB, build a `LocyConfig` via `mutate`, run the program in
/// the docstring, and apply derived facts. Used by every `#[when]` handler
/// that needs to set one or more config fields.
async fn run_with_config(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
    mutate: impl FnOnce(&mut LocyConfig),
) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let mut config = LocyConfig::default();
    mutate(&mut config);
    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await;
    apply_derived_and_store(world, program, result).await;
}

#[when(regex = r#"^evaluating the following Locy program with max_iterations (\d+):$"#)]
async fn when_evaluating_with_max_iterations(
    world: &mut LocyWorld,
    max_iter: usize,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| c.max_iterations = max_iter).await;
}

#[when(
    regex = r#"^evaluating the following Locy program with max_iterations (\d+) allowing partial:$"#
)]
async fn when_evaluating_with_max_iterations_allow_partial(
    world: &mut LocyWorld,
    max_iter: usize,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| {
        c.max_iterations = max_iter;
        c.allow_partial = true;
    })
    .await;
}

#[when("evaluating the following Locy program with strict_probability_domain:")]
async fn when_evaluating_with_strict_probability(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| c.strict_probability_domain = true).await;
}

#[when("evaluating the following Locy program with exact_probability:")]
async fn when_evaluating_with_exact_probability(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| c.exact_probability = true).await;
}

#[when(
    regex = r#"^evaluating the following Locy program with exact_probability and max_bdd_variables (\d+):$"#
)]
async fn when_evaluating_with_exact_probability_and_bdd_limit(
    world: &mut LocyWorld,
    max_bdd: usize,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| {
        c.exact_probability = true;
        c.max_bdd_variables = max_bdd;
    })
    .await;
}

#[when(
    regex = r#"^evaluating the following Locy program with exact_probability and top_k_proofs (\d+):$"#
)]
async fn when_evaluating_with_exact_probability_and_top_k(
    world: &mut LocyWorld,
    top_k: usize,
    step: &cucumber::gherkin::Step,
) {
    run_with_config(world, step, |c| {
        c.exact_probability = true;
        c.top_k_proofs = top_k;
    })
    .await;
}

#[when(
    regex = r#"^evaluating the following Locy program with semiring "(AddMultProb|MaxMinProb|BddExact|TopKProofs\(\d+\))":$"#
)]
async fn when_evaluating_with_semiring(
    world: &mut LocyWorld,
    kind: String,
    step: &cucumber::gherkin::Step,
) {
    let semiring = match kind.as_str() {
        "AddMultProb" => uni_locy::SemiringKind::AddMultProb,
        "MaxMinProb" => uni_locy::SemiringKind::MaxMinProb,
        "BddExact" => uni_locy::SemiringKind::BddExact,
        s if s.starts_with("TopKProofs(") && s.ends_with(')') => {
            let k_str = &s["TopKProofs(".len()..s.len() - 1];
            let k: u32 = k_str
                .parse()
                .unwrap_or_else(|_| panic!("invalid k in semiring '{s}'"));
            uni_locy::SemiringKind::TopKProofs { k }
        }
        other => panic!("unknown semiring kind: {other}"),
    };

    run_with_config(world, step, |c| c.semiring = semiring).await;
}

/// Phase C gate-closure: the neural-predicates feature is GA;
/// `neural_predicates_preview` defaults to `true`. This step
/// explicitly sets it to `false` so scenarios can assert the
/// opt-out behavior (CREATE MODEL rejection).
#[when("evaluating the following Locy program with neural_predicates_preview disabled:")]
async fn when_evaluating_with_neural_preview_disabled(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    let classifier_registry = world.classifier_registry.clone();
    run_with_config(world, step, |c| {
        c.neural_predicates_preview = false;
        c.classifier_registry = classifier_registry;
    })
    .await;
}

#[when("evaluating the following Locy program with neural_predicates_preview:")]
async fn when_evaluating_with_neural_preview(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
) {
    // Pull any classifiers staged by `Given a registered mock classifier ...`
    // so neural invocations dispatch correctly.
    let classifier_registry = world.classifier_registry.clone();
    run_with_config(world, step, |c| {
        c.neural_predicates_preview = true;
        c.classifier_registry = classifier_registry;
    })
    .await;
}

#[when("evaluating the following Locy program with params:")]
async fn when_evaluating_with_params(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    let params = world.params().clone();
    run_with_config(world, step, |c| c.params = params).await;
}
