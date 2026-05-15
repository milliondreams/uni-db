use std::sync::Arc;

use crate::fixtures::load_graph;
use crate::LocyWorld;
use cucumber::given;
use uni_common::Value;
use uni_locy::{FeatureValue, MockClassifier, NeuralClassifier};

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

// ───────────────────────────────────────────────────────────────────────
// Phase B Slice 3: neural classifier registration for tests
// ───────────────────────────────────────────────────────────────────────

/// Register a constant-output mock classifier under a model name. The
/// model declaration in the Locy program supplies INPUT/OUTPUT/USING —
/// this step provides the actual runtime classifier instance.
#[given(regex = r#"^a registered mock classifier ['"](.+)['"] returning ([0-9]*\.?[0-9]+)$"#)]
fn register_mock_classifier_constant(world: &mut LocyWorld, name: String, value: f64) {
    let classifier: Arc<dyn NeuralClassifier> =
        Arc::new(MockClassifier::constant("mock/constant", value));
    world.classifier_registry.insert(name, classifier);
}

/// Register a feature-driven mock that pulls a single Float feature
/// named `feature_name` and clamps it to `[0, 1]`. Useful when scenarios
/// want per-row variation without standing up a full provider.
#[given(
    regex = r#"^a registered mock classifier ['"](.+)['"] driven by Float feature ['"](.+)['"]$"#
)]
fn register_mock_classifier_feature(world: &mut LocyWorld, name: String, feature_name: String) {
    let classifier: Arc<dyn NeuralClassifier> =
        Arc::new(MockClassifier::new("mock/feature", move |inp| {
            match inp.features.get(&feature_name) {
                Some(FeatureValue::Float(v)) => v.clamp(0.0, 1.0),
                Some(FeatureValue::Int(v)) => (*v as f64).clamp(0.0, 1.0),
                _ => 0.0,
            }
        }));
    world.classifier_registry.insert(name, classifier);
}

/// Register a mock that classifies a **string** feature: returns
/// `0.9` if `feature_name == "high"`, `0.1` otherwise. Lets TCK
/// scenarios verify that string-typed property-access feature
/// expressions (Phase B Slice 3 follow-up) feed the classifier.
#[given(
    regex = r#"^a registered mock classifier ['"](.+)['"] returning 0\.9 when string feature ['"](.+)['"] equals ['"](.+)['"]$"#
)]
fn register_mock_classifier_string_match(
    world: &mut LocyWorld,
    name: String,
    feature_name: String,
    target_value: String,
) {
    let classifier: Arc<dyn NeuralClassifier> = Arc::new(MockClassifier::new(
        "mock/string-match",
        move |inp| match inp.features.get(&feature_name) {
            Some(FeatureValue::String(s)) if *s == target_value => 0.9,
            _ => 0.1,
        },
    ));
    world.classifier_registry.insert(name, classifier);
}

/// Phase C B1-B3 follow-up: register a `CalibratedClassifier`
/// that wraps a constant mock with a conformal-style identity
/// calibrator (returns the input unchanged but exposes a
/// confidence band of `[p-q, p+q]` clipped to [0,1] with a
/// pinned quantile). Used to assert that EXPLAIN surfaces both
/// `calibrated_probability` and `confidence_band`.
#[given(
    regex = r#"^a registered Calibrated mock classifier ['"](.+)['"] returning ([0-9]*\.?[0-9]+) with conformal quantile ([0-9]*\.?[0-9]+)$"#
)]
fn register_calibrated_mock_classifier(
    world: &mut LocyWorld,
    name: String,
    value: f64,
    quantile: f64,
) {
    use uni_locy::calibration::ConformalPredictor;
    use uni_locy::CalibratedClassifier;
    let base: Arc<dyn NeuralClassifier> = Arc::new(MockClassifier::constant(
        format!("mock/base-{}", name),
        value,
    ));
    let calibrator: Arc<dyn uni_locy::calibration::Calibrator> = Arc::new(ConformalPredictor {
        alpha: 0.1,
        quantile,
    });
    let wrapped = CalibratedClassifier::new(name.clone(), base, calibrator);
    let arc: Arc<dyn NeuralClassifier> = Arc::new(wrapped);
    world.classifier_registry.insert(name, arc);
}

/// Phase B A3: register a real Candle-backed
/// `CandleLinearClassifier` with a deterministic single-feature
/// logistic-regression fixture. Weights are chosen so that an input
/// of 1.0 produces sigmoid ≈ 0.9 and 0.0 produces sigmoid ≈ 0.1.
/// The fixture is written to a temp file per-scenario, so no
/// checked-in binary blobs are required.
///
/// The `binding_name` matches the model's `INPUT (<name>)` clause —
/// that's the key under which the runtime populates `ClassifyInput`.
/// For `FEATURES s.score`, the binding name is `s` and the
/// classifier receives `features["s"] = Float(<row's score>)`.
#[given(regex = r#"^a Candle classifier ['"](.+)['"] over Float input binding ['"](.+)['"]$"#)]
fn register_candle_classifier_float(world: &mut LocyWorld, name: String, binding_name: String) {
    use candle_core::{Device, Tensor};
    use std::collections::HashMap;
    use uni_locy::CandleLinearClassifier;

    // Solve logit(0.9) ≈ 2.197, logit(0.1) ≈ -2.197 for f(1.0)/f(0.0):
    // weight = 4.394, bias = -2.197.
    const WEIGHT: f32 = 4.394;
    const BIAS: f32 = -2.197;

    let device = Device::Cpu;
    let w = Tensor::from_slice(&[WEIGHT], (1,), &device).unwrap();
    let b = Tensor::from_slice(&[BIAS], (1,), &device).unwrap();
    let mut tensors: HashMap<String, Tensor> = HashMap::new();
    tensors.insert("weight".to_string(), w);
    tensors.insert("bias".to_string(), b);

    let temp = tempfile::Builder::new()
        .prefix("candle-fixture-")
        .suffix(".safetensors")
        .tempfile()
        .expect("create temp fixture");
    candle_core::safetensors::save(&tensors, temp.path()).expect("save fixture safetensors");

    let classifier =
        CandleLinearClassifier::load(format!("candle/{}", name), vec![binding_name], temp.path())
            .expect("load Candle fixture");
    // Persist the tempfile by leaking the handle — the OS will clean
    // up the temp dir at process exit. We need the file to outlive
    // the Given step.
    let _ = temp.into_temp_path().keep();

    let arc: Arc<dyn NeuralClassifier> = Arc::new(classifier);
    world.classifier_registry.insert(name, arc);
}

/// Register a mock that increments a shared counter every time
/// `classify` is invoked, then returns a constant value. Used to
/// observe memoization / cross-clause batching behavior.
#[given(regex = r#"^a counting mock classifier ['"](.+)['"] returning ([0-9]*\.?[0-9]+)$"#)]
fn register_counting_mock(world: &mut LocyWorld, name: String, value: f64) {
    let counter: Arc<std::sync::atomic::AtomicUsize> =
        Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter_ref = Arc::clone(&counter);
    let value = value.clamp(0.0, 1.0);
    let classifier: Arc<dyn NeuralClassifier> =
        Arc::new(MockClassifier::new("mock/counting", move |_| {
            counter_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            value
        }));
    world.classifier_registry.insert(name.clone(), classifier);
    world.classifier_call_counts.insert(name, counter);
}
