use crate::fixtures::load_graph;
use crate::LocyWorld;
use cucumber::given;
use uni_common::Value;

#[given("an empty graph")]
async fn an_empty_graph(world: &mut LocyWorld) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
}

#[given("any graph")]
async fn any_graph(world: &mut LocyWorld) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
}

#[given(regex = r"^the (.+) graph$")]
async fn named_graph(world: &mut LocyWorld, graph_name: String) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
    load_graph(world.db(), &graph_name)
        .await
        .unwrap_or_else(|e| panic!("Failed to load graph '{}': {}", graph_name, e));
}

#[given("having executed:")]
async fn having_executed(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    if let Some(query) = step.docstring() {
        let session = world.db().session();
        let tx = session
            .tx()
            .await
            .unwrap_or_else(|e| panic!("Failed to start transaction: {}", e));
        tx.execute(query)
            .await
            .unwrap_or_else(|e| panic!("Setup query failed: {}", e));
        tx.commit()
            .await
            .unwrap_or_else(|e| panic!("Failed to commit setup query: {}", e));
    }
}

#[given(regex = r#"^the parameter (\w+) = (.+)$"#)]
fn set_parameter(world: &mut LocyWorld, name: String, value_str: String) {
    let t = value_str.trim();
    let value =
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
        } else {
            Value::String(t.to_string())
        };
    world.add_param(name, value);
}
