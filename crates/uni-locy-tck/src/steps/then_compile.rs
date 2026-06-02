use crate::steps::assertions::{assert_err, assert_err_mentions, assert_ok};
use crate::LocyWorld;
use cucumber::then;

#[then("the program should compile successfully")]
async fn program_should_compile_successfully(world: &mut LocyWorld) {
    let compile_result = world
        .compile_result()
        .expect("No compile result found - did you forget to compile a program?");
    assert_ok(compile_result, "compilation");
}

#[then("the program should fail to compile")]
async fn program_should_fail_to_compile(world: &mut LocyWorld) {
    let compile_result = world
        .compile_result()
        .expect("No compile result found - did you forget to compile a program?");
    assert_err(compile_result, "compile");
}

#[then(regex = r#"^the compile error should mention ['"](.+)['"]$"#)]
async fn compile_error_should_mention(world: &mut LocyWorld, expected_text: String) {
    let compile_result = world
        .compile_result()
        .expect("No compile result found - did you forget to compile a program?");
    assert_err_mentions(compile_result, "compile", &expected_text);
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

#[then(regex = r#"^the stratum (\d+) should( not)? be recursive$"#)]
async fn stratum_recursive(world: &mut LocyWorld, stratum_idx: usize, negation: String) {
    let expect_recursive = negation.is_empty();
    let compile_result = world.compile_result().expect("No compile result found");

    let compiled = compile_result.as_ref().expect("Compilation failed");
    let stratum = compiled
        .strata
        .get(stratum_idx)
        .unwrap_or_else(|| panic!("No stratum at index {}", stratum_idx));

    if expect_recursive {
        assert!(
            stratum.is_recursive,
            "Expected stratum {} to be recursive, but it is not",
            stratum_idx
        );
    } else {
        assert!(
            !stratum.is_recursive,
            "Expected stratum {} to not be recursive, but it is",
            stratum_idx
        );
    }
}
