use crate::LocyWorld;
use cucumber::when;

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
