use crate::steps::assertions::{assert_err, assert_err_mentions, assert_ok};
use crate::LocyWorld;
use cucumber::then;

#[then("the program should parse successfully")]
async fn program_should_parse_successfully(world: &mut LocyWorld) {
    let parse_result = world
        .parse_result()
        .expect("No parse result found - did you forget to parse a program?");
    assert_ok(parse_result, "parse");
}

#[then("the program should fail to parse")]
async fn program_should_fail_to_parse(world: &mut LocyWorld) {
    let parse_result = world
        .parse_result()
        .expect("No parse result found - did you forget to parse a program?");
    assert_err(parse_result, "parse");
}

#[then(regex = r#"^the parse error should mention ['"](.+)['"]$"#)]
async fn parse_error_should_mention(world: &mut LocyWorld, expected_text: String) {
    let parse_result = world
        .parse_result()
        .expect("No parse result found - did you forget to parse a program?");
    assert_err_mentions(parse_result, "parse", &expected_text);
}
