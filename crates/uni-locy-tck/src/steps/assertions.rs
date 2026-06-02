//! Shared assertion and parsing helpers for the Locy TCK step definitions.

use std::fmt::Display;
use uni_common::Value;

/// Parse a Gherkin value literal into a [`Value`].
///
/// Quoted text becomes a string; otherwise the literal is parsed as an
/// integer, then a float, then the `true`/`false`/`null` keywords, falling
/// back to a bare string.
pub(crate) fn parse_gherkin_value(s: &str) -> Value {
    let t = s.trim();
    if (t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')) {
        Value::String(t[1..t.len() - 1].to_string())
    } else if let Ok(i) = t.parse::<i64>() {
        Value::Int(i)
    } else if let Ok(f) = t.parse::<f64>() {
        Value::Float(f)
    } else if t == "true" {
        Value::Bool(true)
    } else if t == "false" {
        Value::Bool(false)
    } else if t == "null" {
        Value::Null
    } else {
        Value::String(t.to_string())
    }
}

/// Assert that `result` is `Ok`, panicking with `stage` context on `Err`.
///
/// `stage` names the phase being checked (e.g. `"parse"`, `"compilation"`,
/// `"evaluation"`) so the panic message reads naturally.
pub(crate) fn assert_ok<T, E: Display>(result: &Result<T, E>, stage: &str) {
    if let Err(err) = result {
        panic!("Expected successful {stage}, but got error: {err}");
    }
}

/// Assert that `result` is `Err`, panicking with `stage` context on `Ok`.
pub(crate) fn assert_err<T, E>(result: &Result<T, E>, stage: &str) {
    if result.is_ok() {
        panic!("Expected {stage} failure, but {stage} succeeded");
    }
}

/// Assert that `result` is `Err` and its `Display` text contains `expected`.
///
/// Panics if the result is `Ok` or if the error message does not mention
/// `expected`. `stage` names the phase for the panic message.
pub(crate) fn assert_err_mentions<T, E: Display>(
    result: &Result<T, E>,
    stage: &str,
    expected: &str,
) {
    match result {
        Ok(_) => panic!("Expected {stage} error mentioning '{expected}', but {stage} succeeded"),
        Err(err) => {
            let message = err.to_string();
            assert!(
                message.contains(expected),
                "Expected error message to contain '{expected}', but got: {message}"
            );
        }
    }
}
