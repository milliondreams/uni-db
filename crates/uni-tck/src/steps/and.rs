use crate::UniWorld;
use cucumber::then;

/// Assert a side-effect counter matches the expected value, reporting `ctx`
/// (extra snapshot detail) on mismatch.
fn assert_effect(label: &str, actual: i64, expected: i64, ctx: &str) {
    assert_eq!(
        actual, expected,
        "Expected {label}={expected}, but got {actual} ({ctx})"
    );
}

#[then("no side effects")]
async fn no_side_effects(world: &mut UniWorld) {
    let effects = world.side_effects();

    assert_eq!(
        effects.nodes_before, effects.nodes_after,
        "Node count changed: {} -> {}",
        effects.nodes_before, effects.nodes_after
    );

    assert_eq!(
        effects.edges_before, effects.edges_after,
        "Edge count changed: {} -> {}",
        effects.edges_before, effects.edges_after
    );

    assert_eq!(
        effects.labels_before, effects.labels_after,
        "Labels changed: {:?} -> {:?}",
        effects.labels_before, effects.labels_after
    );
}

#[then(regex = r"^the side effects should be:$")]
async fn side_effects_should_be(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let Some(table) = step.table() else {
        return;
    };

    let effects = world.side_effects();

    // Table format is: | header | value | (each row is a header-value pair)
    for row in &table.rows {
        if row.len() < 2 {
            continue;
        }
        let header = row[0].trim();
        let expected: i64 = row[1].trim().parse().unwrap_or(0);

        let node_ctx = format!(
            "before={}, after={}, created={}, deleted={}",
            effects.nodes_before, effects.nodes_after, effects.nodes_created, effects.nodes_deleted
        );
        let edge_ctx = format!(
            "before={}, after={}, created={}, deleted={}",
            effects.edges_before, effects.edges_after, effects.edges_created, effects.edges_deleted
        );
        let prop_ctx = format!(
            "before={}, after={}, added={}, removed={}",
            effects.properties_before,
            effects.properties_after,
            effects.properties_added,
            effects.properties_removed
        );

        match header {
            "+nodes" => assert_effect("+nodes", effects.nodes_created as i64, expected, &node_ctx),
            "-nodes" => assert_effect("-nodes", effects.nodes_deleted as i64, expected, &node_ctx),
            "+relationships" | "+edges" => assert_effect(
                "+relationships",
                effects.edges_created as i64,
                expected,
                &edge_ctx,
            ),
            "-relationships" | "-edges" => assert_effect(
                "-relationships",
                effects.edges_deleted as i64,
                expected,
                &edge_ctx,
            ),
            "+labels" => {
                let new_labels: Vec<_> = effects
                    .labels_after
                    .difference(&effects.labels_before)
                    .collect();
                assert_effect(
                    "+labels",
                    new_labels.len() as i64,
                    expected,
                    &format!("new labels: {:?}", new_labels),
                );
            }
            "-labels" => {
                let removed_labels: Vec<_> = effects
                    .labels_before
                    .difference(&effects.labels_after)
                    .collect();
                assert_effect(
                    "-labels",
                    removed_labels.len() as i64,
                    expected,
                    &format!("removed labels: {:?}", removed_labels),
                );
            }
            // Gross additions: properties whose value was set (new or changed).
            "+properties" => assert_effect(
                "+properties",
                effects.properties_added as i64,
                expected,
                &prop_ctx,
            ),
            // Gross removals: properties whose value was deleted or overwritten.
            "-properties" => assert_effect(
                "-properties",
                effects.properties_removed as i64,
                expected,
                &prop_ctx,
            ),
            _ => {
                // Unknown column - ignore silently (may be comments or other data)
            }
        }
    }
}
