// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Python-callable bridge for Locy's [`NeuralClassifier`] trait.
//!
//! Locy's `CREATE MODEL m USING xervo('alias')` looks the alias up in
//! [`uni_locy::LocyConfig::classifier_registry`] at the first invocation
//! and dispatches through the resulting `Arc<dyn NeuralClassifier>`. The
//! Rust side already supports any type that implements the trait
//! (`MockClassifier`, `CalibratedClassifier`, Candle-backed models). This
//! module adds the missing Python entry point: a Python callable wrapped
//! in [`PyClassifier`] satisfies the trait by acquiring the GIL on a
//! blocking thread and dispatching one batch per call.
//!
//! Contract for the Python callable:
//!
//! - Input: `list[dict[str, Any]]` — one dict per row, mapping the
//!   `FEATURES (...)` identifiers to scalar / list values.
//! - Output: `list[float]` — one probability per input row, in `[0, 1]`,
//!   in the same order. NaN or out-of-range values raise
//!   [`uni_locy::ClassifierError::DomainViolation`]; a length mismatch
//!   raises [`uni_locy::ClassifierError::ArityMismatch`]; any Python
//!   exception is forwarded as [`uni_locy::ClassifierError::Provider`].
//!
//! The Python callable runs under the GIL inside
//! `tokio::task::spawn_blocking` so the surrounding async runtime can
//! make progress on other tasks while inference executes. Heavy
//! per-batch inference (e.g. ONNX Runtime calls) should release the GIL
//! inside the Python callable for parallelism, but that is the user's
//! responsibility.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use uni_locy::{ClassifierError, ClassifierResult, ClassifyInput, FeatureValue, NeuralClassifier};

/// Adapter that satisfies [`NeuralClassifier`] by dispatching to a
/// Python callable. Constructed by [`build_classifier_registry`] from
/// the `classifier_registry` field of a `LocyConfig` dict.
pub struct PyClassifier {
    name: String,
    callable: Py<PyAny>,
}

impl PyClassifier {
    pub fn new(name: impl Into<String>, callable: Py<PyAny>) -> Self {
        Self {
            name: name.into(),
            callable,
        }
    }
}

impl std::fmt::Debug for PyClassifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyClassifier")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl NeuralClassifier for PyClassifier {
    fn name(&self) -> &str {
        &self.name
    }

    async fn classify(&self, inputs: &[ClassifyInput]) -> ClassifierResult<Vec<f64>> {
        // `Py<PyAny>` and `ClassifyInput` are both `Send` (Py is a
        // refcounted pointer; ClassifyInput is owned data), so we can
        // move them into the spawn_blocking closure. The Python
        // callable will run under the GIL on the blocking thread.
        let name = self.name.clone();
        let callable = Python::attach(|py| self.callable.clone_ref(py));
        let owned_inputs: Vec<ClassifyInput> = inputs.to_vec();

        let join = tokio::task::spawn_blocking(move || {
            Python::attach(|py| classify_under_gil(py, &name, &callable, &owned_inputs))
        })
        .await;

        match join {
            Ok(result) => result,
            Err(join_err) => Err(ClassifierError::Provider(format!(
                "classifier '{name}' spawn_blocking failed: {join_err}",
                name = self.name,
            ))),
        }
    }
}

/// Single GIL-held step: build the `list[dict]` argument, call the
/// Python callable, extract and validate the result.
fn classify_under_gil(
    py: Python<'_>,
    name: &str,
    callable: &Py<PyAny>,
    inputs: &[ClassifyInput],
) -> ClassifierResult<Vec<f64>> {
    // Build Python argument: list of dicts.
    let py_inputs = PyList::empty(py);
    for inp in inputs {
        let row = PyDict::new(py);
        for (feature_name, feature_value) in &inp.features {
            let value_obj = feature_value_to_py(py, feature_value)
                .map_err(|e| provider_err(name, "feature serialization", e))?;
            row.set_item(feature_name, value_obj)
                .map_err(|e| provider_err(name, "feature dict insert", e))?;
        }
        py_inputs
            .append(row)
            .map_err(|e| provider_err(name, "input list build", e))?;
    }

    // Invoke the Python callable. Anything that raises becomes a
    // Provider error carrying the formatted Python exception.
    let result_obj = callable
        .call1(py, (py_inputs,))
        .map_err(|e| provider_err(name, "callable invocation", e))?;

    // Extract list[float]. Reject non-list returns up front.
    let result_list: Vec<f64> = result_obj
        .extract(py)
        .map_err(|e| provider_err(name, "result extraction (expected list[float])", e))?;

    if result_list.len() != inputs.len() {
        return Err(ClassifierError::ArityMismatch {
            expected: inputs.len(),
            actual: result_list.len(),
        });
    }

    // Domain check — same policy MockClassifier applies (NaN rejected,
    // out-of-range values surface as DomainViolation). The runtime
    // also clamps internally, but failing loudly here gives users a
    // clear signal that their classifier needs work.
    for v in &result_list {
        if v.is_nan() || !(0.0..=1.0).contains(v) {
            return Err(ClassifierError::DomainViolation { value: *v });
        }
    }

    Ok(result_list)
}

fn provider_err(name: &str, stage: &str, e: impl std::fmt::Display) -> ClassifierError {
    ClassifierError::Provider(format!("classifier '{name}' {stage}: {e}"))
}

fn feature_value_to_py(py: Python<'_>, value: &FeatureValue) -> PyResult<Py<PyAny>> {
    match value {
        FeatureValue::Float(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        FeatureValue::Int(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        FeatureValue::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        FeatureValue::Vector(v) => {
            let list = PyList::new(py, v)?;
            Ok(list.into_any().unbind())
        }
        FeatureValue::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        FeatureValue::Null => Ok(py.None()),
    }
}

/// Convert a Python-side `{alias: callable}` mapping into the Rust
/// classifier registry type that [`uni_locy::LocyConfig`] expects.
///
/// Each value must be callable; non-callables raise a `TypeError`. The
/// returned `HashMap` is ready to assign to
/// `LocyConfig::classifier_registry`.
pub fn build_classifier_registry(
    py: Python<'_>,
    raw: HashMap<String, Py<PyAny>>,
) -> PyResult<HashMap<String, Arc<dyn NeuralClassifier>>> {
    let mut out: HashMap<String, Arc<dyn NeuralClassifier>> = HashMap::with_capacity(raw.len());
    for (alias, callable) in raw {
        let bound = callable.bind(py);
        if !bound.is_callable() {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "classifier_registry['{alias}'] must be callable; got {ty}",
                ty = bound
                    .get_type()
                    .name()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "<unknown>".to_string()),
            )));
        }
        out.insert(alias.clone(), Arc::new(PyClassifier::new(alias, callable)));
    }
    Ok(out)
}
