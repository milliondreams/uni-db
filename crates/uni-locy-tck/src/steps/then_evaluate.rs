use crate::LocyWorld;
use cucumber::then;
use std::collections::HashMap;
use uni_common::Value;
use uni_locy::result::CommandResult;

/// Extract a value from a Row (HashMap<String, Value>), supporting dotted
/// property access such as `a.name` → row["a"] → Node.properties["name"].
fn extract_field_value<'a>(row: &'a HashMap<String, Value>, field_path: &str) -> Option<&'a Value> {
    if let Some((col, prop)) = field_path.split_once('.') {
        match row.get(col)? {
            Value::Node(node) => node.properties.get(prop),
            Value::Edge(edge) => edge.properties.get(prop),
            Value::Map(map) => map.get(prop),
            _ => None,
        }
    } else {
        row.get(field_path)
    }
}

/// Parse a Gherkin value literal into a Value.
fn parse_gherkin_value(s: &str) -> Value {
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

/// Flexible value comparison (int/float cross-compare, etc.)
///
/// Tolerance is 1e-6 rather than 1e-9 to accommodate f32-precision values that
/// come from the similar_to() computation path (cosine is computed in f32 and
/// widened to f64, introducing ~2e-8 rounding error for unit vectors).
fn values_match(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < 1e-6,
        (Value::Int(a), Value::Float(b)) => (*a as f64 - b).abs() < 1e-6,
        (Value::Float(a), Value::Int(b)) => (a - *b as f64).abs() < 1e-6,
        _ => actual == expected,
    }
}

#[then("evaluation should succeed")]
async fn evaluation_should_succeed(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    match locy_result {
        Ok(_) => {}
        Err(err) => {
            panic!("Expected successful evaluation, but got error: {}", err);
        }
    }
}

#[then(regex = r#"^evaluation should succeed with timed_out (true|false)$"#)]
async fn evaluation_should_succeed_with_timed_out(world: &mut LocyWorld, expected: String) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    let result = locy_result
        .as_ref()
        .unwrap_or_else(|e| panic!("Expected successful evaluation, but got error: {e}"));

    let expected_flag = expected == "true";
    assert_eq!(
        result.timed_out, expected_flag,
        "expected timed_out={expected_flag}, got timed_out={}",
        result.timed_out,
    );
}

#[then("evaluation should fail")]
async fn evaluation_should_fail(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    if locy_result.is_ok() {
        panic!("Expected evaluation failure, but evaluation succeeded");
    }
}

#[then(regex = r#"^the evaluation error should mention ['"](.+)['"]$"#)]
async fn evaluation_error_should_mention(world: &mut LocyWorld, expected_text: String) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    match locy_result {
        Ok(_) => {
            panic!(
                "Expected evaluation error mentioning '{}', but evaluation succeeded",
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

#[then(regex = r#"^the derived relation ['"](.+)['"] should have (\d+) facts$"#)]
async fn derived_relation_should_have_n_facts(
    world: &mut LocyWorld,
    relation_name: String,
    expected_count: usize,
) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    match locy_result {
        Ok(result) => {
            let facts = result.derived.get(&relation_name);
            let actual = facts.map(|f| f.len()).unwrap_or(0);
            assert_eq!(
                actual, expected_count,
                "Expected derived relation '{}' to have {} facts, but got {}. Available relations: {:?}",
                relation_name,
                expected_count,
                actual,
                result.derived.keys().collect::<Vec<_>>()
            );
        }
        Err(err) => {
            panic!(
                "Expected successful evaluation with derived relation '{}', but got error: {}",
                relation_name, err
            );
        }
    }
}

#[then(regex = r#"^the derived relation ['"](.+)['"] should contain at least (\d+) facts$"#)]
async fn derived_relation_should_contain_at_least_n_facts(
    world: &mut LocyWorld,
    relation_name: String,
    min_count: usize,
) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    match locy_result {
        Ok(result) => {
            let facts = result.derived.get(&relation_name);
            let actual = facts.map(|f| f.len()).unwrap_or(0);
            assert!(
                actual >= min_count,
                "Expected derived relation '{}' to have at least {} facts, but got {}",
                relation_name,
                min_count,
                actual
            );
        }
        Err(err) => {
            panic!(
                "Expected successful evaluation with derived relation '{}', but got error: {}",
                relation_name, err
            );
        }
    }
}

// ── Value-Level Assertions ────────────────────────────────────────────────

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should contain a fact where ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null) and ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null) and ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null)$"#
)]
#[allow(clippy::too_many_arguments)]
async fn derived_relation_should_contain_fact_where_and_and(
    world: &mut LocyWorld,
    relation: String,
    f1: String,
    v1: String,
    f2: String,
    v2: String,
    f3: String,
    v3: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");
    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .unwrap_or_else(|| panic!("No derived relation '{}'", relation));

    let expected1 = parse_gherkin_value(&v1);
    let expected2 = parse_gherkin_value(&v2);
    let expected3 = parse_gherkin_value(&v3);

    let found = facts.iter().any(|row| {
        let m1 = extract_field_value(row, f1.trim())
            .map(|v| values_match(v, &expected1))
            .unwrap_or(false);
        let m2 = extract_field_value(row, f2.trim())
            .map(|v| values_match(v, &expected2))
            .unwrap_or(false);
        let m3 = extract_field_value(row, f3.trim())
            .map(|v| values_match(v, &expected3))
            .unwrap_or(false);
        m1 && m2 && m3
    });

    assert!(
        found,
        "Expected derived relation '{}' to contain a fact where {} = {} and {} = {} and {} = {}, but no match found in {} facts",
        relation, f1, v1, f2, v2, f3, v3, facts.len()
    );
}

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should contain a fact where ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null) and ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null)$"#
)]
async fn derived_relation_should_contain_fact_where_and(
    world: &mut LocyWorld,
    relation: String,
    f1: String,
    v1: String,
    f2: String,
    v2: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .unwrap_or_else(|| panic!("No derived relation '{}'", relation));

    let expected1 = parse_gherkin_value(&v1);
    let expected2 = parse_gherkin_value(&v2);

    let found = facts.iter().any(|row| {
        let m1 = extract_field_value(row, f1.trim())
            .map(|v| values_match(v, &expected1))
            .unwrap_or(false);
        let m2 = extract_field_value(row, f2.trim())
            .map(|v| values_match(v, &expected2))
            .unwrap_or(false);
        m1 && m2
    });

    assert!(
        found,
        "Expected derived relation '{}' to contain a fact where {} = {} and {} = {}, but no match found in {} facts",
        relation, f1, v1, f2, v2, facts.len()
    );
}

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should contain a fact where ([^ ]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null)$"#
)]
async fn derived_relation_should_contain_fact_where(
    world: &mut LocyWorld,
    relation: String,
    field: String,
    value_str: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .unwrap_or_else(|| panic!("No derived relation '{}'", relation));

    let expected = parse_gherkin_value(&value_str);

    let found = facts.iter().any(|row| {
        extract_field_value(row, field.trim())
            .map(|v| values_match(v, &expected))
            .unwrap_or(false)
    });

    assert!(
        found,
        "Expected derived relation '{}' to contain a fact where {} = {}, but no match found in {} facts",
        relation, field, value_str, facts.len()
    );
}

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should not contain a fact where (.+) = (.+) and (.+) = (.+)$"#
)]
async fn derived_relation_should_not_contain_fact_where_and(
    world: &mut LocyWorld,
    relation: String,
    f1: String,
    v1: String,
    f2: String,
    v2: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .map(|f| f.as_slice())
        .unwrap_or(&[]);

    let expected1 = parse_gherkin_value(&v1);
    let expected2 = parse_gherkin_value(&v2);

    let found = facts.iter().any(|row| {
        let m1 = extract_field_value(row, f1.trim())
            .map(|v| values_match(v, &expected1))
            .unwrap_or(false);
        let m2 = extract_field_value(row, f2.trim())
            .map(|v| values_match(v, &expected2))
            .unwrap_or(false);
        m1 && m2
    });

    assert!(
        !found,
        "Expected derived relation '{}' NOT to contain a fact where {} = {} and {} = {}, but match was found",
        relation, f1, v1, f2, v2
    );
}

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should not contain a fact where ([^ ]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null)$"#
)]
async fn derived_relation_should_not_contain_fact_where(
    world: &mut LocyWorld,
    relation: String,
    field: String,
    value_str: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .map(|f| f.as_slice())
        .unwrap_or(&[]);

    let expected = parse_gherkin_value(&value_str);

    let found = facts.iter().any(|row| {
        extract_field_value(row, field.trim())
            .map(|v| values_match(v, &expected))
            .unwrap_or(false)
    });

    assert!(
        !found,
        "Expected derived relation '{}' NOT to contain a fact where {} = {}, but match was found",
        relation, field, value_str
    );
}

// ── Stats Assertions ──────────────────────────────────────────────────────

#[then(regex = r#"^the evaluation stats should show (\d+) total iterations$"#)]
async fn stats_total_iterations(world: &mut LocyWorld, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    assert_eq!(
        result.stats.total_iterations, expected,
        "Expected {} total iterations, got {}",
        expected, result.stats.total_iterations
    );
}

#[then(regex = r#"^the evaluation stats should show (\d+) queries executed$"#)]
async fn stats_queries_executed(world: &mut LocyWorld, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    assert_eq!(
        result.stats.queries_executed, expected,
        "Expected {} queries executed, got {}",
        expected, result.stats.queries_executed
    );
}

#[then(regex = r#"^the evaluation stats should show (\d+) mutations executed$"#)]
async fn stats_mutations_executed(world: &mut LocyWorld, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    assert_eq!(
        result.stats.mutations_executed, expected,
        "Expected {} mutations executed, got {}",
        expected, result.stats.mutations_executed
    );
}

// ── Command Result Assertions ─────────────────────────────────────────────

#[then(regex = r#"^the command result (\d+) should be a Query with (\d+) rows$"#)]
async fn command_result_query_rows(world: &mut LocyWorld, idx: usize, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Query(rows) => {
            assert_eq!(
                rows.len(),
                expected,
                "Expected Query command result {} to have {} rows, got {}",
                idx,
                expected,
                rows.len()
            );
        }
        other => panic!(
            "Expected command result {} to be a Query, got {:?}",
            idx, other
        ),
    }
}

#[then(regex = r#"^the command result (\d+) should be a Query containing row where (.+) = (.+)$"#)]
async fn command_result_query_containing_row(
    world: &mut LocyWorld,
    idx: usize,
    field: String,
    value_str: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Query(rows) => {
            let expected = parse_gherkin_value(&value_str);
            let found = rows.iter().any(|row| {
                extract_field_value(row, field.trim())
                    .map(|v| values_match(v, &expected))
                    .unwrap_or(false)
            });
            assert!(
                found,
                "Expected Query result {} to contain row where {} = {}, but not found in {} rows",
                idx,
                field,
                value_str,
                rows.len()
            );
        }
        other => panic!(
            "Expected command result {} to be a Query, got {:?}",
            idx, other
        ),
    }
}

#[then(regex = r#"^the command result (\d+) should be an Assume with (\d+) rows$"#)]
async fn command_result_assume_rows(world: &mut LocyWorld, idx: usize, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Assume(rows) => {
            assert_eq!(
                rows.len(),
                expected,
                "Expected Assume command result {} to have {} rows, got {}",
                idx,
                expected,
                rows.len()
            );
        }
        other => panic!(
            "Expected command result {} to be an Assume, got {:?}",
            idx, other
        ),
    }
}

#[then(
    regex = r#"^the command result (\d+) should be an Assume containing row where (.+) = (.+)$"#
)]
async fn command_result_assume_containing_row(
    world: &mut LocyWorld,
    idx: usize,
    field: String,
    value_str: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Assume(rows) => {
            let expected = parse_gherkin_value(&value_str);
            let found = rows.iter().any(|row| {
                extract_field_value(row, field.trim())
                    .map(|v| values_match(v, &expected))
                    .unwrap_or(false)
            });
            assert!(
                found,
                "Expected Assume result {} to contain row where {} = {}, but not found in {} rows",
                idx,
                field,
                value_str,
                rows.len()
            );
        }
        other => panic!(
            "Expected command result {} to be an Assume, got {:?}",
            idx, other
        ),
    }
}

#[then(regex = r#"^the command result (\d+) should be an Explain with rule ['"](.+)['"]$"#)]
async fn command_result_explain_rule(world: &mut LocyWorld, idx: usize, rule_name: String) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Explain(node) => {
            assert_eq!(
                node.rule, rule_name,
                "Expected Explain root rule '{}', got '{}'",
                rule_name, node.rule
            );
        }
        other => panic!(
            "Expected command result {} to be an Explain, got {:?}",
            idx, other
        ),
    }
}

#[then(regex = r#"^the command result (\d+) should be an Explain with (\d+) children$"#)]
async fn command_result_explain_children(world: &mut LocyWorld, idx: usize, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Explain(node) => {
            assert_eq!(
                node.children.len(),
                expected,
                "Expected Explain with {} children, got {}",
                expected,
                node.children.len()
            );
        }
        other => panic!(
            "Expected command result {} to be an Explain, got {:?}",
            idx, other
        ),
    }
}

#[then(
    regex = r#"^the command result (\d+) should be an Abduce with at least (\d+) modifications$"#
)]
async fn command_result_abduce_modifications(world: &mut LocyWorld, idx: usize, min: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Abduce(abduce_result) => {
            assert!(
                abduce_result.modifications.len() >= min,
                "Expected Abduce with at least {} modifications, got {}",
                min,
                abduce_result.modifications.len()
            );
        }
        other => panic!(
            "Expected command result {} to be an Abduce, got {:?}",
            idx, other
        ),
    }
}

#[then(regex = r#"^the command result (\d+) should be a Derive affecting (\d+) elements$"#)]
async fn command_result_derive_affecting(world: &mut LocyWorld, idx: usize, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Derive { affected } => {
            assert_eq!(
                *affected, expected,
                "Expected Derive affecting {} elements, got {}",
                expected, affected
            );
        }
        other => panic!(
            "Expected command result {} to be a Derive, got {:?}",
            idx, other
        ),
    }
}

// ── Cypher Command Result Assertions ─────────────────────────────────────

#[then(regex = r#"^the command result (\d+) should be a Cypher with at least (\d+) rows$"#)]
async fn command_result_cypher_at_least_rows(world: &mut LocyWorld, idx: usize, min: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Cypher(rows) => {
            assert!(
                rows.len() >= min,
                "Expected Cypher command result {} to have at least {} rows, got {}",
                idx,
                min,
                rows.len()
            );
        }
        other => panic!(
            "Expected command result {} to be a Cypher, got {:?}",
            idx, other
        ),
    }
}

#[then(regex = r#"^the command result (\d+) should be a Cypher containing row where (.+) = (.+)$"#)]
async fn command_result_cypher_containing_row(
    world: &mut LocyWorld,
    idx: usize,
    field: String,
    value_str: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Cypher(rows) => {
            let expected = parse_gherkin_value(&value_str);
            let found = rows.iter().any(|row| {
                extract_field_value(row, field.trim())
                    .map(|v| values_match(v, &expected))
                    .unwrap_or(false)
            });
            assert!(
                found,
                "Cypher command result {} has no row where {} = {} (out of {} rows)",
                idx,
                field,
                value_str,
                rows.len()
            );
        }
        other => panic!(
            "Expected command result {} to be a Cypher, got {:?}",
            idx, other
        ),
    }
}

// ── "at least" Variants for Query and Derive ─────────────────────────────

#[then(regex = r#"^the command result (\d+) should be a Query with at least (\d+) rows$"#)]
async fn command_result_query_at_least_rows(world: &mut LocyWorld, idx: usize, min: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Query(rows) => {
            assert!(
                rows.len() >= min,
                "Expected Query command result {} to have at least {} rows, got {}",
                idx,
                min,
                rows.len()
            );
        }
        other => panic!(
            "Expected command result {} to be a Query, got {:?}",
            idx, other
        ),
    }
}

#[then(regex = r#"^the command result (\d+) should be a Derive with at least (\d+) affected$"#)]
async fn command_result_derive_at_least_affected(world: &mut LocyWorld, idx: usize, min: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Derive { affected } => {
            assert!(
                *affected >= min,
                "Expected Derive command result {} to have at least {} affected, got {}",
                idx,
                min,
                affected
            );
        }
        other => panic!(
            "Expected command result {} to be a Derive, got {:?}",
            idx, other
        ),
    }
}

// ── Graph State Assertions ────────────────────────────────────────────────

#[then(regex = r#"^the graph should contain (\d+) nodes with label ['"](.+)['"]$"#)]
async fn graph_should_contain_n_nodes_with_label(
    world: &mut LocyWorld,
    expected: usize,
    label: String,
) {
    let query = format!("MATCH (n:{}) RETURN count(n) AS cnt", label);
    let result = world
        .db()
        .session()
        .query(&query)
        .await
        .expect("graph query failed");
    let cnt: i64 = result.rows()[0].get("cnt").expect("missing cnt column");
    assert_eq!(
        cnt as usize, expected,
        "Expected {} nodes with label '{}', got {}",
        expected, label, cnt
    );
}

#[then(
    regex = r#"^the graph should contain an edge from ['"](.+)['"] to ['"](.+)['"] with type ['"](.+)['"]$"#
)]
async fn graph_should_contain_edge(
    world: &mut LocyWorld,
    from: String,
    to: String,
    edge_type: String,
) {
    let query = format!(
        "MATCH (a {{name: '{}'}})-[r:{}]->(b {{name: '{}'}}) RETURN count(r) AS cnt",
        from, edge_type, to
    );
    let result = world
        .db()
        .session()
        .query(&query)
        .await
        .expect("graph query failed");
    let cnt: i64 = result.rows()[0].get("cnt").expect("missing cnt column");
    assert!(
        cnt > 0,
        "Expected edge from '{}' to '{}' with type '{}', but none found",
        from,
        to,
        edge_type
    );
}

#[then(
    regex = r#"^the graph should not contain an edge from ['"](.+)['"] to ['"](.+)['"] with type ['"](.+)['"]$"#
)]
async fn graph_should_not_contain_edge(
    world: &mut LocyWorld,
    from: String,
    to: String,
    edge_type: String,
) {
    let query = format!(
        "MATCH (a {{name: '{}'}})-[r:{}]->(b {{name: '{}'}}) RETURN count(r) AS cnt",
        from, edge_type, to
    );
    let result = world
        .db()
        .session()
        .query(&query)
        .await
        .expect("graph query failed");
    let cnt: i64 = result.rows()[0].get("cnt").expect("missing cnt column");
    assert_eq!(
        cnt, 0,
        "Expected no edge from '{}' to '{}' with type '{}', but found {}",
        from, to, edge_type, cnt
    );
}

#[then(regex = r#"^the graph should NOT contain an edge with type ['"](.+)['"]$"#)]
async fn graph_should_not_contain_edge_type(world: &mut LocyWorld, edge_type: String) {
    let query = format!("MATCH ()-[r:{}]->() RETURN count(r) AS cnt", edge_type);
    let result = world
        .db()
        .session()
        .query(&query)
        .await
        .expect("graph query failed");
    let cnt: i64 = result.rows()[0].get("cnt").expect("missing cnt column");
    assert_eq!(
        cnt, 0,
        "Expected no edges with type '{}', but found {}",
        edge_type, cnt
    );
}

// ── Warning Assertions ──────────────────────────────────────────────────

#[then(
    regex = r#"^the result should contain a SharedProbabilisticDependency warning for rule ['"](.+)['"]$"#
)]
async fn result_should_contain_shared_dep_warning(world: &mut LocyWorld, rule_name: String) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let found = locy_result.warnings().iter().any(|w| {
        w.code == uni_locy::RuntimeWarningCode::SharedProbabilisticDependency
            && w.rule_name == rule_name
    });
    assert!(
        found,
        "Expected SharedProbabilisticDependency warning for rule '{}', but warnings were: {:?}",
        rule_name,
        locy_result
            .warnings()
            .iter()
            .map(|w| format!("{:?} ({})", w.code, w.rule_name))
            .collect::<Vec<_>>()
    );
}

#[then("the result should not contain a SharedProbabilisticDependency warning")]
async fn result_should_not_contain_shared_dep_warning(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let found = locy_result
        .warnings()
        .iter()
        .any(|w| w.code == uni_locy::RuntimeWarningCode::SharedProbabilisticDependency);
    assert!(
        !found,
        "Expected no SharedProbabilisticDependency warning, but found: {:?}",
        locy_result
            .warnings()
            .iter()
            .filter(|w| w.code == uni_locy::RuntimeWarningCode::SharedProbabilisticDependency)
            .map(|w| format!("rule={}", w.rule_name))
            .collect::<Vec<_>>()
    );
}

#[then(regex = r#"^the result should contain a BddLimitExceeded warning for rule ['"](.+)['"]$"#)]
async fn result_should_contain_bdd_limit_warning(world: &mut LocyWorld, rule_name: String) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let found = locy_result.warnings().iter().any(|w| {
        w.code == uni_locy::RuntimeWarningCode::BddLimitExceeded && w.rule_name == rule_name
    });
    assert!(
        found,
        "Expected BddLimitExceeded warning for rule '{}', but warnings were: {:?}",
        rule_name,
        locy_result
            .warnings()
            .iter()
            .map(|w| format!("{:?} ({})", w.code, w.rule_name))
            .collect::<Vec<_>>()
    );
}

#[then("the result should not contain a BddLimitExceeded warning")]
async fn result_should_not_contain_bdd_limit_warning(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let found = locy_result
        .warnings()
        .iter()
        .any(|w| w.code == uni_locy::RuntimeWarningCode::BddLimitExceeded);
    assert!(
        !found,
        "Expected no BddLimitExceeded warning, but found: {:?}",
        locy_result
            .warnings()
            .iter()
            .filter(|w| w.code == uni_locy::RuntimeWarningCode::BddLimitExceeded)
            .map(|w| format!("rule={}", w.rule_name))
            .collect::<Vec<_>>()
    );
}

// ── CrossGroupCorrelationNotExact Assertions ────────────────────────────

#[then(
    regex = r#"^the result should contain a CrossGroupCorrelationNotExact warning for rule ['"](.+)['"]$"#
)]
async fn result_should_contain_cross_group_warning(world: &mut LocyWorld, rule_name: String) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let found = locy_result.warnings().iter().any(|w| {
        w.code == uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact
            && w.rule_name == rule_name
    });
    assert!(
        found,
        "Expected CrossGroupCorrelationNotExact warning for rule '{}', but warnings were: {:?}",
        rule_name,
        locy_result
            .warnings()
            .iter()
            .map(|w| format!("{:?} ({})", w.code, w.rule_name))
            .collect::<Vec<_>>()
    );
}

#[then("the result should not contain a CrossGroupCorrelationNotExact warning")]
async fn result_should_not_contain_cross_group_warning(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let found = locy_result
        .warnings()
        .iter()
        .any(|w| w.code == uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact);
    assert!(
        !found,
        "Expected no CrossGroupCorrelationNotExact warning, but found: {:?}",
        locy_result
            .warnings()
            .iter()
            .filter(|w| w.code == uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact)
            .map(|w| format!("rule={}", w.rule_name))
            .collect::<Vec<_>>()
    );
}

// ── BddLimitExceeded Metadata Assertions ────────────────────────────────

#[then(
    regex = r#"^the BddLimitExceeded warning for rule ['"](.+)['"] should have variable_count >= (\d+)$"#
)]
async fn bdd_warning_should_have_variable_count(
    world: &mut LocyWorld,
    rule_name: String,
    min_count: usize,
) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let warning = locy_result.warnings().iter().find(|w| {
        w.code == uni_locy::RuntimeWarningCode::BddLimitExceeded && w.rule_name == rule_name
    });
    let warning = warning.unwrap_or_else(|| {
        panic!(
            "Expected BddLimitExceeded warning for rule '{}', but none found",
            rule_name
        )
    });
    let vc = warning.variable_count.unwrap_or_else(|| {
        panic!(
            "BddLimitExceeded warning for rule '{}' has no variable_count",
            rule_name
        )
    });
    assert!(
        vc >= min_count,
        "Expected variable_count >= {} for rule '{}', but got {}",
        min_count,
        rule_name,
        vc
    );
}

#[then(regex = r#"^the BddLimitExceeded warning for rule ['"](.+)['"] should have a key_group$"#)]
async fn bdd_warning_should_have_key_group(world: &mut LocyWorld, rule_name: String) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let warning = locy_result.warnings().iter().find(|w| {
        w.code == uni_locy::RuntimeWarningCode::BddLimitExceeded && w.rule_name == rule_name
    });
    let warning = warning.unwrap_or_else(|| {
        panic!(
            "Expected BddLimitExceeded warning for rule '{}', but none found",
            rule_name
        )
    });
    assert!(
        warning.key_group.is_some(),
        "BddLimitExceeded warning for rule '{}' has no key_group field",
        rule_name
    );
}

// ── Approximate Fact Assertions ──────────────────────────────────────────

#[then(regex = r#"^the derived relation ['"](.+)['"] should not contain any approximate facts$"#)]
async fn derived_relation_should_not_contain_approximate_facts(
    world: &mut LocyWorld,
    relation: String,
) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found")
        .as_ref()
        .expect("Evaluation failed");

    let facts = locy_result
        .derived
        .get(&relation)
        .unwrap_or_else(|| panic!("No derived relation '{}'", relation));

    let approximate_count = facts
        .iter()
        .filter(|row| {
            row.get("_approximate")
                .map(|v| matches!(v, Value::Bool(true)))
                .unwrap_or(false)
        })
        .count();

    assert!(
        approximate_count == 0,
        "Expected no approximate facts in '{}', but found {} approximate fact(s)",
        relation,
        approximate_count
    );
}

#[then(
    regex = r#"^the command result (\d+) should be an Explain where child (\d+) has proof_probability approximately (.+)$"#
)]
async fn command_result_explain_child_proof_probability(
    world: &mut LocyWorld,
    idx: usize,
    child_idx: usize,
    expected_str: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Explain(node) => {
            let child = node
                .children
                .get(child_idx)
                .unwrap_or_else(|| panic!("No child at index {}", child_idx));
            let expected: f64 = expected_str.parse().expect("Invalid float");
            let actual = child.proof_probability.unwrap_or_else(|| {
                panic!(
                    "Child {} has no proof_probability (expected ~{})",
                    child_idx, expected
                )
            });
            assert!(
                (actual - expected).abs() < 1e-6,
                "Child {} proof_probability: expected ~{}, got {}",
                child_idx,
                expected,
                actual
            );
        }
        other => panic!(
            "Expected command result {} to be an Explain, got {:?}",
            idx, other
        ),
    }
}

#[then(
    regex = r#"^the command result (\d+) should be an Explain where all children have proof_probability$"#
)]
async fn command_result_explain_all_children_have_proof_probability(
    world: &mut LocyWorld,
    idx: usize,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let cmd = result
        .command_results
        .get(idx)
        .unwrap_or_else(|| panic!("No command result at index {}", idx));

    match cmd {
        CommandResult::Explain(node) => {
            for (i, child) in node.children.iter().enumerate() {
                assert!(
                    child.proof_probability.is_some(),
                    "Child {} is missing proof_probability",
                    i
                );
            }
        }
        other => panic!(
            "Expected command result {} to be an Explain, got {:?}",
            idx, other
        ),
    }
}
