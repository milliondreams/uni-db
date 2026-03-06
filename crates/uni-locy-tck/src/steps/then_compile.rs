use crate::LocyWorld;
use cucumber::then;

#[then("the program should compile successfully")]
async fn program_should_compile_successfully(world: &mut LocyWorld) {
    let compile_result = world
        .compile_result()
        .expect("No compile result found - did you forget to compile a program?");

    match compile_result {
        Ok(_) => {}
        Err(err) => {
            panic!("Expected successful compilation, but got error: {}", err);
        }
    }
}

#[then("the program should fail to compile")]
async fn program_should_fail_to_compile(world: &mut LocyWorld) {
    let compile_result = world
        .compile_result()
        .expect("No compile result found - did you forget to compile a program?");

    if compile_result.is_ok() {
        panic!("Expected compile failure, but compilation succeeded");
    }
}

#[then(regex = r#"^the compile error should mention ['"](.+)['"]$"#)]
async fn compile_error_should_mention(world: &mut LocyWorld, expected_text: String) {
    let compile_result = world
        .compile_result()
        .expect("No compile result found - did you forget to compile a program?");

    match compile_result {
        Ok(_) => {
            panic!(
                "Expected compile error mentioning '{}', but compilation succeeded",
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

#[then(regex = r#"^the program should have (\d+) strata$"#)]
async fn program_should_have_n_strata(world: &mut LocyWorld, expected_count: usize) {
    let compile_result = world
        .compile_result()
        .expect("No compile result found - did you forget to compile a program?");

    match compile_result {
        Ok(compiled) => {
            let actual = compiled.strata.len();
            assert_eq!(
                actual, expected_count,
                "Expected {} strata, but got {}",
                expected_count, actual
            );
        }
        Err(err) => {
            panic!(
                "Expected successful compilation with {} strata, but got error: {}",
                expected_count, err
            );
        }
    }
}

#[then(regex = r#"^the stratum (\d+) should be recursive$"#)]
async fn stratum_should_be_recursive(world: &mut LocyWorld, stratum_idx: usize) {
    let compile_result = world.compile_result().expect("No compile result found");

    let compiled = compile_result.as_ref().expect("Compilation failed");
    let stratum = compiled
        .strata
        .get(stratum_idx)
        .unwrap_or_else(|| panic!("No stratum at index {}", stratum_idx));

    assert!(
        stratum.is_recursive,
        "Expected stratum {} to be recursive, but it is not",
        stratum_idx
    );
}

#[then(regex = r#"^the stratum (\d+) should not be recursive$"#)]
async fn stratum_should_not_be_recursive(world: &mut LocyWorld, stratum_idx: usize) {
    let compile_result = world.compile_result().expect("No compile result found");

    let compiled = compile_result.as_ref().expect("Compilation failed");
    let stratum = compiled
        .strata
        .get(stratum_idx)
        .unwrap_or_else(|| panic!("No stratum at index {}", stratum_idx));

    assert!(
        !stratum.is_recursive,
        "Expected stratum {} to not be recursive, but it is",
        stratum_idx
    );
}
