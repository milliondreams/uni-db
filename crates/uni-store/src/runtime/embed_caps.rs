// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Embedding-capability model: which vector heads an alias's task can produce, and
//! which heads a schema's auto-embed columns require.
//!
//! Auto-embed always sources from a *text* column, so an alias is usable for a column
//! only if its Uni-Xervo `ModelTask` can produce — from text — the head that column
//! needs (dense / sparse / multi-vector). This module is the single source of truth for
//! that mapping, shared by open-time schema validation in the `uni-db` crate and by
//! write-time routing in [`crate::runtime::writer`].
//!
//! The task→heads mapping is a deliberate allow-list. `ModelTask` is
//! `#[non_exhaustive]`, so any task that is not explicitly an embed-from-text task
//! (image/audio/multimodal embedders, rerankers, generators, …) maps to *no* heads and
//! is rejected, rather than silently slipping through when a future variant is added
//! upstream. The mapping returns the task's *upper bound*; a specific hybrid model may
//! expose fewer heads (its `available_heads()` is the runtime ground truth), which the
//! writer enforces at inference time.

use std::collections::BTreeMap;

use uni_common::core::schema::{IndexDefinition, Schema};
use uni_xervo::api::ModelTask;
use uni_xervo::traits::HeadSet;

/// Vector heads an alias's `task` can produce from a text source column.
///
/// Returns the task's *upper bound*: an `EmbedHybrid` alias maps to all three heads,
/// even though a given hybrid model may expose fewer. Tasks that do not embed from text
/// (image/audio/multimodal embedders, rerank, generate, raw, nlp, transcribe, ocr) and
/// any future task variant map to an empty `HeadSet`.
///
/// # Examples
///
/// ```ignore
/// assert_eq!(text_embedding_heads(ModelTask::Embed), HeadSet::DENSE);
/// assert!(text_embedding_heads(ModelTask::Rerank).is_empty());
/// ```
pub fn text_embedding_heads(task: ModelTask) -> HeadSet {
    match task {
        ModelTask::Embed => HeadSet::DENSE,
        ModelTask::EmbedSparse => HeadSet::SPARSE,
        ModelTask::EmbedMultiVector => HeadSet::MULTI_VECTOR,
        ModelTask::EmbedHybrid => HeadSet::ALL,
        // Image/audio/multimodal embed from non-text inputs; rerank/generate/raw/nlp/
        // transcribe/ocr are not text embeddings. None can auto-embed a text column.
        _ => HeadSet::empty(),
    }
}

/// Whether `property` on `label` is a multi-vector (`List<Vector>`) column.
///
/// The late-interaction (ColBERT) shape auto-embeds per-token via the multi-vector
/// head; a plain `Vector` column uses the dense head (issue #104).
pub(crate) fn is_multivector_property(schema: &Schema, label: &str, property: &str) -> bool {
    schema
        .properties
        .get(label)
        .and_then(|p| p.get(property))
        .is_some_and(|m| {
            matches!(&m.r#type, uni_common::DataType::List(inner)
                if matches!(**inner, uni_common::DataType::Vector { .. }))
        })
}

/// Embedding heads a single alias must produce, with the columns that require them.
#[derive(Debug, Clone)]
pub struct RequiredHeads {
    /// Union of heads needed by every auto-embed column bound to the alias.
    pub heads: HeadSet,
    /// `(column, head)` contributors, in schema order, for diagnostics.
    pub columns: Vec<(String, HeadSet)>,
}

/// Per-alias embedding-head requirements implied by a schema's auto-embed indexes.
///
/// Walks both `Vector` (classified dense vs multi-vector by column type) and `Sparse`
/// index definitions that carry an embedding config, unioning the heads each alias must
/// produce. The result drives the open-time capability check
/// `required ⊆ text_embedding_heads(task)` and names offending columns on failure.
///
/// # Examples
///
/// ```ignore
/// let required = required_embed_heads(&schema);
/// for (alias, req) in &required {
///     assert!(text_embedding_heads(task_of(alias)).contains(req.heads));
/// }
/// ```
pub fn required_embed_heads(schema: &Schema) -> BTreeMap<String, RequiredHeads> {
    let mut out: BTreeMap<String, RequiredHeads> = BTreeMap::new();
    for idx in &schema.indexes {
        let (alias, column, head) = match idx {
            IndexDefinition::Vector(cfg) => {
                let Some(emb) = cfg.embedding_config.as_ref() else {
                    continue;
                };
                let head = if is_multivector_property(schema, &cfg.label, &cfg.property) {
                    HeadSet::MULTI_VECTOR
                } else {
                    HeadSet::DENSE
                };
                (emb.alias.clone(), cfg.property.clone(), head)
            }
            IndexDefinition::Sparse(cfg) => {
                let Some(emb) = cfg.embedding_config.as_ref() else {
                    continue;
                };
                (emb.alias.clone(), cfg.property.clone(), HeadSet::SPARSE)
            }
            // FullText / Scalar / Inverted / JsonFullText carry no embedding config,
            // and `IndexDefinition` is `#[non_exhaustive]`: only the embed-bearing
            // variants above contribute required heads.
            _ => continue,
        };
        let entry = out.entry(alias).or_insert_with(|| RequiredHeads {
            heads: HeadSet::empty(),
            columns: Vec::new(),
        });
        entry.heads |= head;
        entry.columns.push((column, head));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_tasks_map_to_their_head() {
        assert_eq!(text_embedding_heads(ModelTask::Embed), HeadSet::DENSE);
        assert_eq!(
            text_embedding_heads(ModelTask::EmbedSparse),
            HeadSet::SPARSE
        );
        assert_eq!(
            text_embedding_heads(ModelTask::EmbedMultiVector),
            HeadSet::MULTI_VECTOR
        );
        assert_eq!(text_embedding_heads(ModelTask::EmbedHybrid), HeadSet::ALL);
    }

    #[test]
    fn hybrid_covers_every_single_head() {
        let hybrid = text_embedding_heads(ModelTask::EmbedHybrid);
        for head in [HeadSet::DENSE, HeadSet::SPARSE, HeadSet::MULTI_VECTOR] {
            assert!(hybrid.contains(head), "hybrid must cover {head:?}");
        }
    }

    #[test]
    fn non_text_and_non_embed_tasks_map_to_no_heads() {
        // Image/audio/multimodal embed from non-text inputs; the rest are not
        // embeddings. None is a valid text auto-embed target (issue #129/#130 §4.1).
        for task in [
            ModelTask::EmbedImage,
            ModelTask::EmbedAudio,
            ModelTask::EmbedMultimodal,
            ModelTask::Rerank,
            ModelTask::Generate,
            ModelTask::Raw,
            ModelTask::Nlp,
            ModelTask::DocumentExtract,
            ModelTask::Transcribe,
            ModelTask::Ocr,
        ] {
            assert!(
                text_embedding_heads(task).is_empty(),
                "task {task:?} must produce no text-embedding heads"
            );
        }
    }

    #[test]
    fn single_task_alias_rejects_a_foreign_head() {
        // The open-time invariant `required ⊆ text_embedding_heads(task)`.
        let dense_only = text_embedding_heads(ModelTask::Embed);
        assert!(dense_only.contains(HeadSet::DENSE));
        assert!(!dense_only.contains(HeadSet::SPARSE));
        assert!(!dense_only.contains(HeadSet::MULTI_VECTOR));
        // A dense+sparse mix is only coverable by a hybrid alias.
        let mixed = HeadSet::DENSE | HeadSet::SPARSE;
        assert!(!dense_only.contains(mixed));
        assert!(text_embedding_heads(ModelTask::EmbedHybrid).contains(mixed));
    }
}
