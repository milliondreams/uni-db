use crate::LocyWorld;
use cucumber::when;
use uni_common::UniError;

#[when("compiling the following Locy program:")]
async fn when_compiling_locy_program(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to compile");

    let result = uni_cypher::parse_locy(program);
    match result {
        Err(e) => {
            world.set_compile_result(Err(UniError::Parse {
                message: format!("LocyParseError: {e}"),
                position: None,
                line: None,
                column: None,
                context: None,
            }));
        }
        Ok(ast) => {
            // Compile through the registry-backed oracle, matching every host
            // path. A bare `uni_locy::compile` uses the six-name default, so
            // the TCK could not exercise the registry at all — which is how
            // this feature's prose came to describe behaviour the suite never
            // checked.
            let plugins = uni_query::query::df_graph::locy_fold::default_locy_plugin_registry();
            let oracle = |name: &str| {
                uni_query::query::df_graph::locy_fold::locy_monotonicity_verdict(&plugins, name)
            };
            let compile_result = uni_locy::compile_with_oracle(
                &ast,
                &std::collections::HashMap::new(),
                &[],
                &oracle,
            )
            .map_err(|e| UniError::Query {
                message: format!("LocyCompileError: {e}"),
                query: None,
            });
            world.set_compile_result(compile_result);
        }
    }
}
