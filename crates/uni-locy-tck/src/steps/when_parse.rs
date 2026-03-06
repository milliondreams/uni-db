use crate::LocyWorld;
use cucumber::when;

#[when("parsing the following Locy program:")]
async fn when_parsing_locy_program(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    if let Some(program) = step.docstring() {
        let result = uni_cypher::parse_locy(program);
        world.set_parse_result(result);
    } else {
        panic!("Expected a docstring with the Locy program to parse");
    }
}
