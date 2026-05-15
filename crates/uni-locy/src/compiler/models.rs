// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Compilation of `CREATE MODEL` declarations (Phase B preview).
//!
//! Lowers `uni_cypher::locy_ast::ModelDefinition` statements into
//! [`CompiledModel`] catalog entries, gated by
//! [`crate::LocyConfig::neural_predicates_preview`]. Name-collision and
//! G1-lite (`UncalibratedLLMLogprobs`) checks are also performed here;
//! rule-body model-invocation validation lives in `typecheck.rs` where it
//! can read the assembled `rule_catalog`.

use std::collections::HashMap;

use uni_cypher::locy_ast::{CalibrationMethod, LocyProgram, LocyStatement};

use super::errors::LocyCompileError;
use crate::types::{CompiledInputBinding, CompiledModel, CompilerWarning, WarningCode};

/// Lower `CREATE MODEL` statements into a catalog.
///
/// When `neural_predicates_preview` is `false`, any `CREATE MODEL`
/// statement is rejected with [`LocyCompileError::NeuralPreviewDisabled`].
/// When the catalog has duplicate names, returns
/// [`LocyCompileError::ModelNameCollision`].
///
/// G1-lite (`UncalibratedLLMLogprobs`) is emitted when a model declares
/// `CALIBRATION None` (or omits the clause) and its `xervo_alias` starts
/// with `"generate/"`, `"chat/"`, or `"llm/"`.
pub fn compile_models(
    program: &LocyProgram,
    neural_predicates_preview: bool,
) -> Result<(HashMap<String, CompiledModel>, Vec<CompilerWarning>), LocyCompileError> {
    let mut catalog: HashMap<String, CompiledModel> = HashMap::new();
    let mut warnings: Vec<CompilerWarning> = Vec::new();

    for stmt in &program.statements {
        if let LocyStatement::Model(model_def) = stmt {
            let model_name = model_def.name.to_string();
            if !neural_predicates_preview {
                return Err(LocyCompileError::NeuralPreviewDisabled {
                    model_name: model_name.clone(),
                });
            }
            if catalog.contains_key(&model_name) {
                return Err(LocyCompileError::ModelNameCollision { name: model_name });
            }

            // G1-lite: uncalibrated LLM-alias heuristic.
            let alias_is_llm = is_llm_alias_heuristic(&model_def.xervo_alias);
            let uncalibrated =
                matches!(model_def.calibration, None | Some(CalibrationMethod::None));
            if alias_is_llm && uncalibrated {
                warnings.push(CompilerWarning {
                    code: WarningCode::UncalibratedLLMLogprobs,
                    message: format!(
                        "model '{}' uses xervo alias '{}' (looks LLM-backed) without \
                         CALIBRATION; raw LLM logprobs are not calibrated probabilities \
                         (rollout D-10) — add CALIBRATION platt_scaling or run CALIBRATE",
                        model_name, model_def.xervo_alias
                    ),
                    rule_name: model_name.clone(),
                });
            }

            let inputs: Vec<CompiledInputBinding> = model_def
                .inputs
                .iter()
                .map(|b| CompiledInputBinding {
                    variable: b.variable.clone(),
                    label: b.label.clone(),
                })
                .collect();

            catalog.insert(
                model_name.clone(),
                CompiledModel {
                    name: model_name,
                    inputs,
                    features: model_def.features.clone(),
                    path_context: model_def.path_context.clone(),
                    output_type: model_def.output.output_type,
                    output_name: model_def.output.name.clone(),
                    xervo_alias: model_def.xervo_alias.clone(),
                    calibration: model_def.calibration,
                    version: model_def.version.clone(),
                    annotations: model_def.annotations.clone(),
                },
            );
        }
    }

    Ok((catalog, warnings))
}

/// Heuristic: a Xervo alias is "LLM-backed" if it starts with `generate/`,
/// `chat/`, or `llm/`. Documented placeholder until Xervo exposes
/// `calibration_source` (impl plan D-10). Intentionally narrow so this
/// doesn't fire on `classify/...`, `embed/...`, or `rerank/...`.
fn is_llm_alias_heuristic(alias: &str) -> bool {
    let lower = alias.to_ascii_lowercase();
    lower.starts_with("generate/") || lower.starts_with("chat/") || lower.starts_with("llm/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn llm_alias_heuristic_recognizes_common_prefixes() {
        assert!(is_llm_alias_heuristic("generate/gpt-4o"));
        assert!(is_llm_alias_heuristic("chat/claude-haiku"));
        assert!(is_llm_alias_heuristic("llm/local-llama"));
        assert!(!is_llm_alias_heuristic("classify/supplier-risk-v3"));
        assert!(!is_llm_alias_heuristic("embed/text-3-large"));
        assert!(!is_llm_alias_heuristic("rerank/cohere"));
    }
}
