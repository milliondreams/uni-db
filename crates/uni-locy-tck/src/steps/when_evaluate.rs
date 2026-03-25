use crate::LocyWorld;
use cucumber::when;
use uni_locy::LocyConfig;

#[when("evaluating the following Locy program:")]
async fn when_evaluating_locy_program(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to evaluate");

    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    let result = world.db().locy().evaluate(program).await;
    world.set_locy_result(result);
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
        .locy()
        .evaluate_with_config(program, &config)
        .await;
    world.set_locy_result(result);
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
        .locy()
        .evaluate_with_config(program, &config)
        .await;
    world.set_locy_result(result);
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
        .locy()
        .evaluate_with_config(program, &config)
        .await;
    world.set_locy_result(result);
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
        .locy()
        .evaluate_with_config(program, &config)
        .await;
    world.set_locy_result(result);
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
        .locy()
        .evaluate_with_config(program, &config)
        .await;
    world.set_locy_result(result);
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
        .locy()
        .evaluate_with_config(program, &config)
        .await;
    world.set_locy_result(result);
}
