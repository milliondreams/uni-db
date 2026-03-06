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
            let compile_result = uni_locy::compile(&ast).map_err(|e| UniError::Query {
                message: format!("LocyCompileError: {e}"),
                query: None,
            });
            world.set_compile_result(compile_result);
        }
    }
}
