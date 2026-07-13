use crate::world::SideEffects;
use crate::UniWorld;
use cucumber::then;

/// The eight canonical openCypher side-effect counters, paired with the actual
/// value observed in `effects`.
///
/// `+labels`/`-labels` are derived from the before/after label-set difference;
/// the remaining six read the corresponding gross counter directly.
fn canonical_counters(effects: &SideEffects) -> [(&'static str, i64); 8] {
    let new_labels = effects
        .labels_after
        .difference(&effects.labels_before)
        .count() as i64;
    let removed_labels = effects
        .labels_before
        .difference(&effects.labels_after)
        .count() as i64;
    [
        ("+nodes", effects.nodes_created as i64),
        ("-nodes", effects.nodes_deleted as i64),
        ("+relationships", effects.edges_created as i64),
        ("-relationships", effects.edges_deleted as i64),
        ("+labels", new_labels),
        ("-labels", removed_labels),
        ("+properties", effects.properties_added as i64),
        ("-properties", effects.properties_removed as i64),
    ]
}

/// Evaluate the `no side effects` assertion against captured `effects`.
///
/// Returns `Ok(())` only when the statement left the graph completely
/// untouched, otherwise `Err(message)` describing the first observed side
/// effect. Beyond the net node/edge counts and label set, every gross counter
/// (`nodes_created`/`nodes_deleted`, `edges_created`/`edges_deleted`,
/// `properties_added`/`properties_removed`) must be zero: a statement that
/// churns entities or overwrites properties without changing net counts is
/// still a side effect.
///
/// # Errors
///
/// Returns the first side-effect violation as a human-readable string.
pub fn check_no_side_effects(effects: &SideEffects) -> Result<(), String> {
    if effects.nodes_before != effects.nodes_after {
        return Err(format!(
            "Node count changed: {} -> {}",
            effects.nodes_before, effects.nodes_after
        ));
    }
    if effects.edges_before != effects.edges_after {
        return Err(format!(
            "Edge count changed: {} -> {}",
            effects.edges_before, effects.edges_after
        ));
    }
    if effects.labels_before != effects.labels_after {
        return Err(format!(
            "Labels changed: {:?} -> {:?}",
            effects.labels_before, effects.labels_after
        ));
    }
    for (label, value) in [
        ("nodes_created", effects.nodes_created),
        ("nodes_deleted", effects.nodes_deleted),
        ("edges_created", effects.edges_created),
        ("edges_deleted", effects.edges_deleted),
        ("properties_added", effects.properties_added),
        ("properties_removed", effects.properties_removed),
    ] {
        if value != 0 {
            return Err(format!(
                "Expected no side effects, but {label}={value} (gross mutation occurred)"
            ));
        }
    }
    Ok(())
}

/// Evaluate the `the side effects should be:` assertion.
///
/// `declared` holds the `(header, value)` pairs parsed from the scenario table.
/// Per openCypher TCK semantics, any canonical counter NOT listed in the table
/// is required to be zero, so this checks every counter in
/// [`canonical_counters`] against its declared value (defaulting to `0`).
/// Unknown headers in `declared` are ignored (they may be comments).
///
/// # Errors
///
/// Returns the first counter whose observed value differs from the declared
/// (or implicit-zero) expectation.
pub fn check_side_effects_should_be(
    effects: &SideEffects,
    declared: &[(String, i64)],
) -> Result<(), String> {
    use std::collections::HashMap;

    // Normalize edge aliases so `+edges` and `+relationships` collapse together.
    let mut expected: HashMap<&str, i64> = HashMap::new();
    for (header, value) in declared {
        let canonical = match header.trim() {
            "+relationships" | "+edges" => "+relationships",
            "-relationships" | "-edges" => "-relationships",
            other => other,
        };
        expected.insert(canonical, *value);
    }

    for (header, actual) in canonical_counters(effects) {
        let want = expected.get(header).copied().unwrap_or(0);
        if actual != want {
            return Err(format!("Expected {header}={want}, but got {actual}"));
        }
    }
    Ok(())
}

#[then("no side effects")]
async fn no_side_effects(world: &mut UniWorld) {
    if let Err(message) = check_no_side_effects(world.side_effects()) {
        panic!("{message}");
    }
}

#[then(regex = r"^the side effects should be:$")]
async fn side_effects_should_be(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let Some(table) = step.table() else {
        return;
    };

    // Table format is: | header | value | (each row is a header-value pair).
    let declared: Vec<(String, i64)> = table
        .rows
        .iter()
        .filter(|row| row.len() >= 2)
        .map(|row| {
            (
                row[0].trim().to_string(),
                row[1].trim().parse().unwrap_or(0),
            )
        })
        .collect();

    if let Err(message) = check_side_effects_should_be(world.side_effects(), &declared) {
        panic!("{message}");
    }
}
