// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Local ONNX cross-encoder reranker provider.
//!
//! Implements `ModelProvider` for cross-encoder models such as
//! `cross-encoder/ms-marco-MiniLM-L6-v2`. Handles tokenization
//! (via the `tokenizers` crate) and batched ONNX inference.
//!
//! # Catalog example
//!
//! ```json
//! {
//!     "alias": "rerank/minilm",
//!     "task": "Rerank",
//!     "provider_id": "local/onnx-reranker",
//!     "model_id": "cross-encoder/ms-marco-MiniLM-L6-v2"
//! }
//! ```

use async_trait::async_trait;
use hf_hub::api::tokio::ApiBuilder;
use hf_hub::{Repo, RepoType};
use ndarray::{Array2, Axis};
use ort::session::Session;
use ort::session::builder::GraphOptimizationLevel;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::info;
use uni_xervo::api::{ModelAliasSpec, ModelTask};
use uni_xervo::cache::resolve_cache_dir;
use uni_xervo::error::{Result, RuntimeError};
use uni_xervo::traits::{
    LoadedModelHandle, ModelProvider, ProviderCapabilities, ProviderHealth, RerankerModel,
    ScoredDoc,
};

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

/// A `ModelProvider` that loads ONNX cross-encoder models for reranking.
///
/// Registered with `provider_id = "local/onnx-reranker"`. Expects models
/// that accept `input_ids`, `attention_mask`, and `token_type_ids` tensors
/// and produce a single logit per (query, document) pair.
pub struct OnnxCrossEncoderProvider;

#[async_trait]
impl ModelProvider for OnnxCrossEncoderProvider {
    fn provider_id(&self) -> &'static str {
        "local/onnx-reranker"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::Rerank],
        }
    }

    async fn load(&self, spec: &ModelAliasSpec) -> Result<LoadedModelHandle> {
        if spec.task != ModelTask::Rerank {
            return Err(RuntimeError::CapabilityMismatch(format!(
                "ONNX cross-encoder provider does not support task {:?}",
                spec.task
            )));
        }
        let reranker = OnnxCrossEncoder::load(spec).await?;
        let handle: Arc<dyn RerankerModel> = Arc::new(reranker);
        Ok(Arc::new(handle) as LoadedModelHandle)
    }

    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

// ---------------------------------------------------------------------------
// Cross-Encoder Model
// ---------------------------------------------------------------------------

/// Default max sequence length for BERT-based cross-encoders.
const DEFAULT_MAX_SEQ_LEN: usize = 512;

/// ONNX cross-encoder that tokenizes (query, document) pairs and runs
/// batched inference to produce relevance scores.
struct OnnxCrossEncoder {
    session: Mutex<Session>,
    tokenizer: tokenizers::Tokenizer,
    max_seq_len: usize,
    alias: String,
}

impl OnnxCrossEncoder {
    /// Load the ONNX model and tokenizer from a HuggingFace repo.
    async fn load(spec: &ModelAliasSpec) -> Result<Self> {
        let max_seq_len = spec
            .options
            .get("max_seq_len")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(DEFAULT_MAX_SEQ_LEN);

        let cache_dir = resolve_cache_dir("onnx-reranker", &spec.model_id, &spec.options);
        let (model_path, tokenizer_path) = download_model_files(
            &spec.alias,
            &spec.model_id,
            spec.revision.as_deref(),
            &cache_dir,
        )
        .await?;

        info!(
            alias = %spec.alias,
            model_id = %spec.model_id,
            model_path = %model_path.display(),
            "Loading ONNX cross-encoder"
        );

        let tokenizer = tokenizers::Tokenizer::from_file(&tokenizer_path).map_err(|e| {
            RuntimeError::OnnxLoadFailure {
                alias: spec.alias.clone(),
                path: tokenizer_path,
                cause: format!("Failed to load tokenizer: {e}"),
            }
        })?;

        let session = build_session(&model_path, spec)?;

        Ok(Self {
            session: Mutex::new(session),
            tokenizer,
            max_seq_len,
            alias: spec.alias.clone(),
        })
    }

    /// Tokenize a batch of (query, document) pairs into padded tensors.
    ///
    /// Returns `(input_ids, attention_mask, token_type_ids)` as i64 2D arrays,
    /// each with shape `[batch_size, padded_seq_len]`.
    fn tokenize_batch(
        &self,
        query: &str,
        documents: &[&str],
    ) -> Result<(Array2<i64>, Array2<i64>, Array2<i64>)> {
        let batch_size = documents.len();

        // Tokenize each (query, doc) pair
        let encodings: Vec<tokenizers::Encoding> = documents
            .iter()
            .map(|doc| {
                self.tokenizer.encode((query, *doc), true).map_err(|e| {
                    RuntimeError::OnnxInvocationFailure {
                        alias: self.alias.clone(),
                        cause: format!("Tokenization failed: {e}"),
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Determine padded sequence length (capped at max_seq_len)
        let padded_len = encodings
            .iter()
            .map(|e| e.get_ids().len().min(self.max_seq_len))
            .max()
            .unwrap_or(0);

        // Build padded tensors
        let mut input_ids = Array2::<i64>::zeros((batch_size, padded_len));
        let mut attention_mask = Array2::<i64>::zeros((batch_size, padded_len));
        let mut token_type_ids = Array2::<i64>::zeros((batch_size, padded_len));

        for (i, enc) in encodings.iter().enumerate() {
            let ids = enc.get_ids();
            let mask = enc.get_attention_mask();
            let types = enc.get_type_ids();
            let seq_len = ids.len().min(self.max_seq_len);

            for j in 0..seq_len {
                input_ids[[i, j]] = ids[j] as i64;
                attention_mask[[i, j]] = mask[j] as i64;
                token_type_ids[[i, j]] = types[j] as i64;
            }
        }

        Ok((input_ids, attention_mask, token_type_ids))
    }
}

#[async_trait]
impl RerankerModel for OnnxCrossEncoder {
    async fn rerank(&self, query: &str, docs: &[&str]) -> Result<Vec<ScoredDoc>> {
        if docs.is_empty() {
            return Ok(vec![]);
        }

        let (input_ids, attention_mask, token_type_ids) = self.tokenize_batch(query, docs)?;

        // Run inference (Session::run is blocking, so we hold the lock briefly)
        let logits = {
            let mut session =
                self.session
                    .lock()
                    .map_err(|e| RuntimeError::OnnxInvocationFailure {
                        alias: self.alias.clone(),
                        cause: format!("Session lock poisoned: {e}"),
                    })?;

            let map_err = |e: ort::Error, ctx: &str| RuntimeError::OnnxInvocationFailure {
                alias: self.alias.clone(),
                cause: format!("{ctx}: {e}"),
            };

            let input_ids_tensor = ort::value::Tensor::from_array(input_ids.into_dyn())
                .map_err(|e| map_err(e, "input_ids tensor"))?;
            let attention_mask_tensor = ort::value::Tensor::from_array(attention_mask.into_dyn())
                .map_err(|e| map_err(e, "attention_mask tensor"))?;
            let token_type_ids_tensor = ort::value::Tensor::from_array(token_type_ids.into_dyn())
                .map_err(|e| map_err(e, "token_type_ids tensor"))?;

            let inputs: Vec<(String, ort::value::DynTensor)> = vec![
                ("input_ids".to_string(), input_ids_tensor.upcast()),
                ("attention_mask".to_string(), attention_mask_tensor.upcast()),
                ("token_type_ids".to_string(), token_type_ids_tensor.upcast()),
            ];

            // Get output name before run() to avoid borrow conflict
            let output_name = session
                .outputs()
                .first()
                .map(|o| o.name().to_string())
                .unwrap_or_else(|| "logits".to_string());

            let outputs = session
                .run(inputs)
                .map_err(|e| map_err(e, "ONNX inference"))?;

            // Extract logits from first output — shape is typically [batch, 1] or [batch]
            let output =
                outputs
                    .get(&output_name)
                    .ok_or_else(|| RuntimeError::OnnxInvocationFailure {
                        alias: self.alias.clone(),
                        cause: format!("Missing output tensor '{output_name}'"),
                    })?;
            let view = output.try_extract_array::<f32>().map_err(|e| {
                RuntimeError::OnnxInvocationFailure {
                    alias: self.alias.clone(),
                    cause: format!("Failed to extract output array: {e}"),
                }
            })?;

            // Handle both [batch, 1] and [batch] output shapes
            let scores: Vec<f32> = if view.ndim() == 2 {
                view.axis_iter(Axis(0)).map(|row| row[[0]]).collect()
            } else {
                view.iter().copied().collect()
            };

            scores
        };

        // Build ScoredDoc results sorted by score descending
        let mut scored: Vec<ScoredDoc> = logits
            .into_iter()
            .enumerate()
            .map(|(index, score)| ScoredDoc {
                index,
                score,
                text: None,
            })
            .collect();
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(scored)
    }
}

// ---------------------------------------------------------------------------
// Model file download
// ---------------------------------------------------------------------------

/// Download the ONNX model and tokenizer files from HuggingFace.
///
/// Returns `(model_path, tokenizer_path)`. Uses `hf-hub` with the same
/// caching strategy as the `LocalOnnxProvider`.
async fn download_model_files(
    alias: &str,
    model_id: &str,
    revision: Option<&str>,
    cache_dir: &Path,
) -> Result<(PathBuf, PathBuf)> {
    let api = ApiBuilder::new()
        .with_cache_dir(cache_dir.to_path_buf())
        .build()
        .map_err(|e| RuntimeError::OnnxDownloadFailure {
            alias: alias.to_string(),
            cause: e.to_string(),
        })?;

    let repo = match revision {
        Some(rev) => Repo::with_revision(model_id.to_string(), RepoType::Model, rev.to_string()),
        None => Repo::model(model_id.to_string()),
    };
    let api_repo = api.repo(repo);

    // Download model file — try `onnx/model.onnx` first, then `model.onnx`
    let model_path =
        match api_repo.get("onnx/model.onnx").await {
            Ok(path) => path,
            Err(_) => api_repo.get("model.onnx").await.map_err(|e| {
                RuntimeError::OnnxDownloadFailure {
                    alias: alias.to_string(),
                    cause: format!(
                        "Could not download ONNX model (tried onnx/model.onnx and model.onnx): {e}"
                    ),
                }
            })?,
        };

    // Download tokenizer
    let tokenizer_path =
        api_repo
            .get("tokenizer.json")
            .await
            .map_err(|e| RuntimeError::OnnxDownloadFailure {
                alias: alias.to_string(),
                cause: format!("Could not download tokenizer.json: {e}"),
            })?;

    Ok((model_path, tokenizer_path))
}

// ---------------------------------------------------------------------------
// ORT session builder
// ---------------------------------------------------------------------------

/// Build an ORT session with sensible defaults for cross-encoder inference.
fn build_session(path: &Path, spec: &ModelAliasSpec) -> Result<Session> {
    let builder = Session::builder().map_err(|e| RuntimeError::OnnxLoadFailure {
        alias: spec.alias.clone(),
        path: path.to_path_buf(),
        cause: e.to_string(),
    })?;

    let builder = builder
        .with_optimization_level(GraphOptimizationLevel::Level3)
        .map_err(|e| RuntimeError::OnnxLoadFailure {
            alias: spec.alias.clone(),
            path: path.to_path_buf(),
            cause: e.to_string(),
        })?;

    builder
        .commit_from_file(path)
        .map_err(|e| RuntimeError::OnnxLoadFailure {
            alias: spec.alias.clone(),
            path: path.to_path_buf(),
            cause: e.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sigmoid_known_values() {
        // sigmoid(0) = 0.5
        assert!((sigmoid(0.0) - 0.5).abs() < 1e-6);
        // sigmoid(large positive) ≈ 1.0
        assert!((sigmoid(10.0) - 1.0).abs() < 1e-4);
        // sigmoid(large negative) ≈ 0.0
        assert!(sigmoid(-10.0).abs() < 1e-4);
    }
}

/// Apply sigmoid to normalize a raw cross-encoder logit to [0, 1].
#[cfg(test)]
fn sigmoid(x: f32) -> f32 {
    1.0 / (1.0 + (-x).exp())
}
