use crate::UniWorld;
use cucumber::when;

/// Execute a Cypher query via transaction (supports both reads and mutations).
async fn execute_via_tx(
    world: &mut UniWorld,
    query: &str,
) -> Result<uni_db::QueryResult, uni_db::UniError> {
    let session = world.db().session();
    let tx = session.tx().await?;
    let mut builder = tx.query_with(query);
    for (key, value) in world.params() {
        builder = builder.param(key, value.clone());
    }
    let result = builder.fetch_all().await?;
    tx.commit().await?;
    Ok(result)
}

#[when("executing query:")]
async fn executing_query(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let Some(query) = step.docstring() else {
        return;
    };

    if let Err(e) = world.capture_state_before().await {
        panic!("Failed to capture state before: {}", e);
    }

    match execute_via_tx(world, query).await {
        Ok(result) => {
            world.set_result(result);
            if let Err(e) = world.capture_state_after().await {
                panic!("Failed to capture state after: {}", e);
            }
        }
        Err(e) => world.set_error(e),
    }
}

#[when("executing control query:")]
async fn executing_control_query(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let Some(query) = step.docstring() else {
        return;
    };

    if let Err(e) = world.capture_state_before().await {
        panic!("Failed to capture state before: {}", e);
    }

    match execute_via_tx(world, query).await {
        Ok(result) => {
            world.set_result(result);
            if let Err(e) = world.capture_state_after().await {
                panic!("Failed to capture state after: {}", e);
            }
        }
        Err(e) => world.set_error(e),
    }
}

#[when(regex = r"^executing query with parameters (.+):$")]
async fn executing_query_with_params(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let Some(query) = step.docstring() else {
        return;
    };

    if let Err(e) = world.capture_state_before().await {
        panic!("Failed to capture state before: {}", e);
    }

    match execute_via_tx(world, query).await {
        Ok(result) => {
            world.set_result(result);
            if let Err(e) = world.capture_state_after().await {
                panic!("Failed to capture state after: {}", e);
            }
        }
        Err(e) => world.set_error(e),
    }
}
