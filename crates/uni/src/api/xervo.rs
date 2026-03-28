// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;

use crate::api::Uni;
use uni_common::{Result, UniError};
use uni_xervo::runtime::ModelRuntime;
pub use uni_xervo::traits::{
    AudioOutput, ContentBlock, GeneratedImage, GenerationOptions, GenerationResult, ImageInput,
    Message, MessageRole, TokenUsage,
};

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
