use crate::LocyWorld;
use cucumber::when;
use uni_locy::LocyConfig;

/// If the Locy evaluation produced a `DerivedFactSet`, apply it to the database
/// via a transaction so that DERIVE mutations are visible to subsequent `then` steps.
///
/// Session-level DERIVE uses `collect_derive: true`, which defers mutations into a
/// `DerivedFactSet` instead of auto-applying. The TCK expects mutations to be
/// visible immediately, so we apply them here and update `stats.mutations_executed`
/// to reflect the actual mutations performed.
async fn apply_derived_and_store(
    world: &mut LocyWorld,
    result: Result<uni_db::locy::LocyResult, uni_common::UniError>,
) {
    store_result(world, result, true).await;
}

/// Store the evaluation result without applying derived facts.
/// Used by scenarios that test DERIVE isolation (e.g. "edges do not persist without tx.apply").
async fn store_without_apply(
    world: &mut LocyWorld,
    result: Result<uni_db::locy::LocyResult, uni_common::UniError>,
) {
    store_result(world, result, false).await;
}

async fn store_result(
    world: &mut LocyWorld,
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
    apply_derived_and_store(world, result).await;
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
    store_without_apply(world, result).await;
}

#[when(regex = r#"^evaluating the following Locy program with max_iterations (\d+):$"#)]
async fn when_evaluating_with_max_iterations(
    world: &mut LocyWorld,
    max_iter: usize,
    step: &cucumber::gherkin::Step,
) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let config = uni_locy::LocyConfig {
        max_iterations: max_iter,
        ..Default::default()
    };
    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await;
    apply_derived_and_store(world, result).await;
}

#[when("evaluating the following Locy program with strict_probability_domain:")]
async fn when_evaluating_with_strict_probability(
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

    let config = uni_locy::LocyConfig {
        strict_probability_domain: true,
        ..Default::default()
    };
    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await;
    apply_derived_and_store(world, result).await;
}

#[when("evaluating the following Locy program with exact_probability:")]
async fn when_evaluating_with_exact_probability(
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

    let config = uni_locy::LocyConfig {
        exact_probability: true,
        ..Default::default()
    };
    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await;
    apply_derived_and_store(world, result).await;
}

#[when(
    regex = r#"^evaluating the following Locy program with exact_probability and max_bdd_variables (\d+):$"#
)]
async fn when_evaluating_with_exact_probability_and_bdd_limit(
    world: &mut LocyWorld,
    max_bdd: usize,
    step: &cucumber::gherkin::Step,
) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let config = uni_locy::LocyConfig {
        exact_probability: true,
        max_bdd_variables: max_bdd,
        ..Default::default()
    };
    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await;
    apply_derived_and_store(world, result).await;
}

#[when(
    regex = r#"^evaluating the following Locy program with exact_probability and top_k_proofs (\d+):$"#
)]
async fn when_evaluating_with_exact_probability_and_top_k(
    world: &mut LocyWorld,
    top_k: usize,
    step: &cucumber::gherkin::Step,
) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let config = uni_locy::LocyConfig {
        exact_probability: true,
        top_k_proofs: top_k,
        ..Default::default()
    };
    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await;
    apply_derived_and_store(world, result).await;
}

#[when("evaluating the following Locy program with params:")]
async fn when_evaluating_with_params(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let config = LocyConfig {
        params: world.params().clone(),
        ..Default::default()
    };
    let result = world
        .db()
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await;
    apply_derived_and_store(world, result).await;
}
