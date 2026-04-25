// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;

use crate::api::Uni;
use uni_common::{Result, UniError};
use uni_xervo::runtime::ModelRuntime;
pub use uni_xervo::traits::{
    AudioOutput, ContentBlock, GeneratedImage, GenerationOptions, GenerationResult, ImageInput,
    Message, MessageRole, RerankerModel, ScoredDoc, TokenUsage,
};
#[cfg(feature = "provider-onnx")]
pub use uni_xervo::traits::{OnnxRunner, TensorBatch, TensorSpec, TensorValue};

fn into_uni_error<E: std::fmt::Display>(err: E) -> UniError {
    UniError::Internal(anyhow::anyhow!(err.to_string()))
}

fn not_configured() -> UniError {
    UniError::Internal(anyhow::anyhow!("Uni-Xervo runtime is not configured"))
}

/// Facade for using Uni-Xervo runtime from the Uni API surface.
///
/// Returned by [`Uni::xervo()`]. When no Xervo catalog was configured at
/// build time, the facade is still returned (infallible accessor) but all
/// operation methods will return an error. Use [`is_available()`](Self::is_available)
/// to check upfront.
#[derive(Clone)]
pub struct UniXervo {
    runtime: Option<Arc<ModelRuntime>>,
}

impl UniXervo {
    /// Whether a Xervo runtime is configured and available for use.
    pub fn is_available(&self) -> bool {
        self.runtime.is_some()
    }

    /// Embed text inputs using a configured model alias.
    pub async fn embed(&self, alias: &str, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let embedder = runtime.embedding(alias).await.map_err(into_uni_error)?;
        embedder.embed(texts.to_vec()).await.map_err(into_uni_error)
    }

    /// Generate using a configured model alias with structured messages.
    pub async fn generate(
        &self,
        alias: &str,
        messages: &[Message],
        options: GenerationOptions,
    ) -> Result<GenerationResult> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let generator = runtime.generator(alias).await.map_err(into_uni_error)?;
        generator
            .generate(messages, options)
            .await
            .map_err(into_uni_error)
    }

    /// Generate text using plain string messages (convenience wrapper).
    ///
    /// Each string is treated as a user message. For multi-role conversations
    /// or multimodal inputs, use [`generate`](Self::generate) with [`Message`] directly.
    pub async fn generate_text(
        &self,
        alias: &str,
        messages: &[&str],
        options: GenerationOptions,
    ) -> Result<GenerationResult> {
        let structured: Vec<Message> = messages.iter().map(|s| Message::user(*s)).collect();
        self.generate(alias, &structured, options).await
    }

    /// Obtain an [`OnnxRunner`] for the given model alias.
    ///
    /// The runner provides tensor-in/tensor-out ONNX inference via the
    /// [`LocalOnnxProvider`](uni_xervo::provider::LocalOnnxProvider).
    /// Models are downloaded from HuggingFace and cached on first use.
    ///
    /// # Errors
    ///
    /// Returns [`UniError`] if the runtime is not configured or the alias
    /// is not registered in the catalog.
    #[cfg(feature = "provider-onnx")]
    pub async fn onnx_runner(&self, alias: &str) -> Result<Arc<dyn uni_xervo::traits::OnnxRunner>> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        runtime.onnx_runner(alias).await.map_err(into_uni_error)
    }

    /// Rerank documents against a query using a configured cross-encoder model.
    ///
    /// Returns [`ScoredDoc`]s sorted by relevance score (descending).
    /// The model alias must point to a catalog entry with `task: Rerank`.
    ///
    /// # Errors
    ///
    /// Returns [`UniError`] if the runtime is not configured, the alias
    /// is not registered, or inference fails.
    pub async fn rerank(
        &self,
        alias: &str,
        query: &str,
        documents: &[&str],
    ) -> Result<Vec<uni_xervo::traits::ScoredDoc>> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let reranker = runtime.reranker(alias).await.map_err(into_uni_error)?;
        reranker
            .rerank(query, documents)
            .await
            .map_err(into_uni_error)
    }

    /// Pre-load and cache every model in the Xervo catalog.
    ///
    /// Models already loaded are skipped. Fails fast on the first error.
    /// Call this during application startup to avoid cold-start latency on
    /// first inference.
    pub async fn prefetch_all(&self) -> Result<()> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        runtime.prefetch_all().await.map_err(into_uni_error)
    }

    /// Pre-load and cache specific model aliases.
    ///
    /// Returns an error immediately if an alias is not found in the catalog
    /// or if any model fails to load. Models already loaded are skipped.
    pub async fn prefetch(&self, aliases: &[&str]) -> Result<()> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        runtime.prefetch(aliases).await.map_err(into_uni_error)
    }

    /// Access the underlying Uni-Xervo runtime, if configured.
    pub fn raw_runtime(&self) -> Option<&Arc<ModelRuntime>> {
        self.runtime.as_ref()
    }
}

impl Uni {
    /// Access Uni-Xervo runtime facade configured for this database.
    ///
    /// Always succeeds — returns a facade even when no Xervo catalog is
    /// configured. Individual methods (`embed`, `generate`, etc.) will return
    /// an error in that case. Use [`UniXervo::is_available()`] to check upfront.
    pub fn xervo(&self) -> UniXervo {
        UniXervo {
            runtime: self.inner.xervo_runtime.clone(),
        }
    }
}
