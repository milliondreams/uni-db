use crate::LocyWorld;
use cucumber::then;

#[then("the program should parse successfully")]
async fn program_should_parse_successfully(world: &mut LocyWorld) {
    let parse_result = world
        .parse_result()
        .expect("No parse result found - did you forget to parse a program?");

    match parse_result {
        Ok(_) => {
            // Success - test passes
        }
        Err(err) => {
            panic!("Expected successful parse, but got error: {}", err);
        }
    }
}

#[then("the program should fail to parse")]
async fn program_should_fail_to_parse(world: &mut LocyWorld) {
    let parse_result = world
        .parse_result()
        .expect("No parse result found - did you forget to parse a program?");

    match parse_result {
        Ok(_) => {
            panic!("Expected parse failure, but parsing succeeded");
        }
        Err(_) => {
            // Failure expected - test passes
        }
    }
}

#[then(regex = r#"^the parse error should mention ['"](.+)['"]$"#)]
async fn parse_error_should_mention(world: &mut LocyWorld, expected_text: String) {
    let parse_result = world
        .parse_result()
        .expect("No parse result found - did you forget to parse a program?");

    match parse_result {
        Ok(_) => {
            panic!(
                "Expected parse error mentioning '{}', but parsing succeeded",
                expected_text
            );
        }
        Err(err) => {
            let error_message = err.to_string();
            if !error_message.contains(&expected_text) {
                panic!(
                    "Expected error message to contain '{}', but got: {}",
                    expected_text, error_message
                );
            }
        }
    }
}
