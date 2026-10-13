// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! BM25 for documents a Lance table does not hold yet, scored as its
//! full-text search would score them once stored.
//!
//! A search merges unflushed rows into the hits Lance returns for the flushed
//! ones. Scoring those rows any other way than Lance does makes the ranking,
//! and so which rows make the top `k`, change when they are flushed. Lance
//! scores rows its inverted index does not cover yet (fragments written since
//! the index was built) with a flat BM25 pass whose statistics start from the
//! index's and add those rows', using the index's tokenizer. Unflushed rows
//! are exactly such rows, so they are scored by the same public function, from
//! the same starting statistics. Without an index Lance runs that flat pass
//! over every stored row with a bare simple tokenizer; the statistics then
//! start from the stored rows'.
//!
//! What is not reproduced: rows already flushed but not yet indexed also add
//! to the statistics Lance uses for unindexed rows, and here they do not, so
//! an unflushed row's score can differ from its post-flush score in the idf
//! term while unindexed flushed fragments exist. The tokenizer and formula are
//! the same in every case.

// Rust guideline compliant

use std::sync::Arc;

use super::types::TextScores;
use anyhow::{Result, anyhow};
use arrow_array::{Array, Float32Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::deps::datafusion::error::DataFusionError;
use lance::deps::datafusion::execution::SendableRecordBatchStream;
use lance::deps::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use lance::index::DatasetIndexInternalExt;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::InvertedIndexParams;
use lance_index::scalar::inverted::query::{FtsSearchParams, Operator, collect_query_tokens};
use lance_index::scalar::inverted::tokenizer::document_tokenizer::LanceTokenizer;
use lance_index::scalar::inverted::{
    DocumentGranularity, FlatBm25SearchOptions, InvertedIndex, MemBM25Scorer, Scorer,
    build_global_bm25_scorer, flat_bm25_search_stream_with_options_and_scorer,
};

/// The row-id column Lance's flat search keys its output by.
const ROW_ID: &str = "_rowid";

/// BM25 scores, as `dataset`'s full-text search on `column` would give them
/// for `query` once the `unstored` documents (id, text) were stored: for each
/// unstored document, and for each `stored` hit (id, text).
///
/// The stored hits are re-scored because BM25 is relative to the corpus: once
/// the unstored documents join it, every document's idf and the average
/// length move. They are scored directly from the combined statistics, not
/// fed through the flat pass, which would count them a second time.
///
/// `dataset` is `None` when the table has never been written. `filter` is the
/// predicate the stored-row search runs with; without an index it bounds the
/// stored rows that count toward the statistics, as Lance's prefiltered flat
/// pass does. A document that does not match is absent from the result.
///
/// # Errors
/// Returns an error if an index segment cannot be opened or Lance's flat
/// search fails.
pub(crate) async fn score_unstored_documents(
    dataset: Option<&Dataset>,
    column: &str,
    query: &str,
    filter: Option<&str>,
    unstored: Vec<(u64, String)>,
    stored: Vec<(u64, String)>,
) -> Result<TextScores> {
    if unstored.is_empty() {
        return Ok(TextScores::default());
    }
    let indexes = match dataset {
        Some(ds) => open_segments(ds, column).await?,
        None => Vec::new(),
    };
    let (tokenizer, base) = if let Some(first) = indexes.first() {
        let mut tokenizer = first.tokenizer();
        let tokens = collect_query_tokens(query, &mut tokenizer);
        let base = build_global_bm25_scorer(&indexes, &tokens, &FtsSearchParams::new(), None)
            .await
            .map_err(|e| anyhow!("BM25 statistics for '{column}': {e}"))?;
        (first.tokenizer(), Some(base))
    } else {
        let base = match dataset {
            Some(ds) => Some(stored_statistics(ds, column, query, filter).await?),
            None => None,
        };
        (simple_tokenizer()?, base)
    };
    let mut stored_tokenizer = tokenizer.box_clone();

    let (ids, texts): (Vec<u64>, Vec<String>) = unstored.into_iter().unzip();
    let schema = Arc::new(Schema::new(vec![
        Field::new(ROW_ID, DataType::UInt64, false),
        Field::new(column, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(StringArray::from(texts)),
        ],
    )?;
    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok(batch)]),
    ));
    let (scored, combined) = flat_bm25(input, column, query, tokenizer, base).await?;

    let mut out = Vec::new();
    for batch in &scored {
        let ids = batch
            .column_by_name(ROW_ID)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| anyhow!("flat BM25 output has no {ROW_ID} column"))?;
        let scores = batch
            .column_by_name("_score")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
            .ok_or_else(|| anyhow!("flat BM25 output has no _score column"))?;
        for i in 0..batch.num_rows() {
            if ids.is_valid(i) && scores.is_valid(i) {
                out.push((ids.value(i), scores.value(i)));
            }
        }
    }
    let stored = rescore(&combined, &mut stored_tokenizer, query, stored);
    Ok(TextScores {
        unstored: out,
        stored: Some(stored),
    })
}

/// `docs` scored against `scorer`'s statistics without adding to them, by the
/// formula and token counting of Lance's flat pass (`flat_bm25_score`):
/// the sum over query tokens of `idf * doc_weight(freq, doc_tokens)`.
fn rescore(
    scorer: &MemBM25Scorer,
    tokenizer: &mut Box<dyn LanceTokenizer>,
    query: &str,
    docs: Vec<(u64, String)>,
) -> Vec<(u64, f32)> {
    let tokens = collect_query_tokens(query, tokenizer);
    let mut index_of: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for i in 0..tokens.len() {
        index_of
            .entry(tokens.get_token(i).to_string())
            .or_default()
            .push(i);
    }
    let mut out = Vec::with_capacity(docs.len());
    for (id, text) in docs {
        let mut freqs = vec![0u32; tokens.len()];
        let mut doc_tokens = 0u32;
        let mut stream = tokenizer.token_stream_for_doc(&text);
        while let Some(token) = stream.next() {
            doc_tokens += 1;
            if let Some(indices) = index_of.get(&token.text) {
                for &i in indices {
                    freqs[i] += 1;
                }
            }
        }
        if doc_tokens == 0 {
            continue;
        }
        let score: f32 = (0..tokens.len())
            .map(|i| {
                scorer.query_weight(tokens.get_token(i)) * scorer.doc_weight(freqs[i], doc_tokens)
            })
            .sum();
        if score > 0.0 {
            out.push((id, score));
        }
    }
    out
}

/// Every committed inverted-index segment on `column`, or none.
async fn open_segments(dataset: &Dataset, column: &str) -> Result<Vec<Arc<InvertedIndex>>> {
    let Some(segments) =
        lance::index::scalar::load_segments(dataset, column, DocumentGranularity::Row)
            .await
            .map_err(|e| anyhow!("FTS segments for '{column}': {e}"))?
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(segments.len());
    for segment in &segments {
        let index = dataset
            .open_scalar_index(column, &segment.uuid, &NoOpMetricsCollector)
            .await
            .map_err(|e| anyhow!("open FTS segment {} of '{column}': {e}", segment.uuid))?;
        let inverted = index
            .as_any()
            .downcast_ref::<InvertedIndex>()
            .ok_or_else(|| anyhow!("index {} on '{column}' is not inverted", segment.uuid))?;
        out.push(Arc::new(inverted.clone()));
    }
    Ok(out)
}

/// BM25 statistics of the stored rows, as Lance's no-index flat pass counts
/// them.
async fn stored_statistics(
    dataset: &Dataset,
    column: &str,
    query: &str,
    filter: Option<&str>,
) -> Result<MemBM25Scorer> {
    let mut scanner = dataset.scan();
    scanner
        .project(&[column])
        .map_err(|e| anyhow!("project '{column}': {e}"))?;
    scanner.with_row_id();
    if let Some(sql) = filter {
        scanner
            .filter(sql)
            .map_err(|e| anyhow!("filter '{sql}': {e}"))?;
    }
    let schema = scanner
        .schema()
        .await
        .map_err(|e| anyhow!("scan '{column}': {e}"))?;
    let stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| anyhow!("scan '{column}': {e}"))?;
    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream.map(|b| b.map_err(|e| DataFusionError::External(Box::new(e)))),
    ));
    let (_, scorer) = flat_bm25(input, column, query, simple_tokenizer()?, None).await?;
    Ok(scorer)
}

/// The tokenizer Lance's flat search uses when a column has no index: a bare
/// simple tokenizer, no lowercasing, stemming or stop words.
fn simple_tokenizer() -> Result<Box<dyn LanceTokenizer>> {
    InvertedIndexParams::default()
        .base_tokenizer("simple".to_string())
        .max_token_length(None)
        .lower_case(false)
        .stem(false)
        .remove_stop_words(false)
        .ascii_folding(false)
        .build()
        .map_err(|e| anyhow!("simple tokenizer: {e}"))
}

/// Lance's flat BM25 pass, with `MatchQuery`'s defaults (`OR`, boost 1).
async fn flat_bm25(
    input: SendableRecordBatchStream,
    column: &str,
    query: &str,
    tokenizer: Box<dyn LanceTokenizer>,
    base: Option<MemBM25Scorer>,
) -> Result<(Vec<RecordBatch>, MemBM25Scorer)> {
    let (stream, scorer) = flat_bm25_search_stream_with_options_and_scorer(
        input,
        column.to_string(),
        query.to_string(),
        tokenizer,
        base,
        FlatBm25SearchOptions {
            target_batch_size: 8192,
            elapsed_compute: None,
            document_granularity: DocumentGranularity::Row,
            operator: Operator::Or,
            boost: 1.0,
            phrase_slop: None,
        },
    )
    .await
    .map_err(|e| anyhow!("flat BM25 on '{column}': {e}"))?;
    let batches = stream
        .try_collect()
        .await
        .map_err(|e| anyhow!("flat BM25 on '{column}': {e}"))?;
    Ok((batches, scorer))
}
