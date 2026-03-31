// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Streaming appender — row-by-row data loading for a single label.
//!
//! Wraps `BulkWriter` to provide an ergonomic, buffered append API for
//! loading large volumes of vertices into a single label.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use uni_common::{Result, UniError, Value};

use crate::api::UniInner;
use crate::api::bulk::{BulkStats, BulkWriter, BulkWriterBuilder};

/// Builder for creating a [`StreamingAppender`].
pub struct AppenderBuilder {
    db: Arc<UniInner>,
    write_guard: Arc<AtomicBool>,
    session_id: String,
    is_pinned: bool,
    /// Whether the write guard was already acquired by the caller (Transaction).
    guard_pre_acquired: bool,
    label: String,
    batch_size: usize,
    defer_vector_indexes: bool,
    max_buffer_size_bytes: Option<usize>,
}

impl AppenderBuilder {
    /// Create an appender builder for use within a Transaction.
    ///
    /// The Transaction already holds the session write guard, so the appender
    /// skips guard acquisition and does not release it on finish/drop.
    pub(crate) fn new_from_tx(db: Arc<UniInner>, label: &str) -> Self {
        // Dummy guard — never acquired/released. Transaction owns the real guard.
        let dummy_guard = Arc::new(AtomicBool::new(true));
        Self {
            db,
            write_guard: dummy_guard,
            session_id: String::new(),
            is_pinned: false,
            guard_pre_acquired: true,
            label: label.to_string(),
            batch_size: 5000,
            defer_vector_indexes: true,
            max_buffer_size_bytes: None,
        }
    }

    /// Set the number of rows to buffer before auto-flushing to the bulk writer.
    ///
    /// Default: 5000.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set whether to defer vector index building until commit.
    ///
    /// Default: `true`.
    pub fn defer_vector_indexes(mut self, defer: bool) -> Self {
        self.defer_vector_indexes = defer;
        self
    }

    /// Set the maximum buffer size in bytes before triggering a checkpoint.
    ///
    /// Default: 1 GB (from BulkWriter defaults).
    pub fn max_buffer_size_bytes(mut self, size: usize) -> Self {
        self.max_buffer_size_bytes = Some(size);
        self
    }

    /// Build the streaming appender.
    ///
    /// Acquires the session's write guard (mutual exclusion with transactions
    /// and other bulk writers) unless the guard was pre-acquired by a Transaction.
    pub fn build(self) -> Result<StreamingAppender> {
        if self.is_pinned {
            return Err(UniError::ReadOnly {
                operation: "appender".to_string(),
            });
        }

        // Determine guard ownership and create the appropriate BulkWriterBuilder.
        let (bulk_builder_base, session_write_guard) = if self.guard_pre_acquired {
            // Transaction path: guard already held, use unguarded BulkWriter.
            (BulkWriterBuilder::new_unguarded(self.db), None)
        } else {
            // Session path: acquire the guard.
            let guard = self.write_guard.clone();
            if guard
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return Err(UniError::WriteContextAlreadyActive {
                    session_id: self.session_id,
                    hint: "Only one Transaction, BulkWriter, or Appender can be active per Session at a time. Commit or rollback the active one first, or create a separate Session for concurrent writes.",
                });
            }
            (
                BulkWriterBuilder::new_with_guard(self.db, guard.clone()),
                Some(guard),
            )
        };

        // Apply shared configuration.
        let mut bulk_builder = bulk_builder_base
            .batch_size(self.batch_size)
            .defer_vector_indexes(self.defer_vector_indexes);
        if let Some(max_buf) = self.max_buffer_size_bytes {
            bulk_builder = bulk_builder.max_buffer_size_bytes(max_buf);
        }
        let writer = bulk_builder.build()?;

        Ok(StreamingAppender {
            writer: Some(writer),
            label: self.label,
            batch_size: self.batch_size,
            buffer: Vec::with_capacity(self.batch_size),
            session_write_guard,
            finished: false,
        })
    }
}

/// A streaming appender for buffered, single-label data loading.
///
/// Rows are buffered internally and flushed to the underlying `BulkWriter`
/// when the buffer reaches `batch_size`. Call [`finish()`](Self::finish) to
/// flush remaining rows and commit.
///
/// # Write Guard
///
/// The appender holds the session's write guard for its entire lifetime
/// (unless created from a Transaction, where the Transaction manages the guard).
/// Only one write context (transaction, bulk writer, or appender) can be
/// active per session at a time. The guard is released on `finish()`,
/// `abort()`, or `drop()`.
pub struct StreamingAppender {
    writer: Option<BulkWriter>,
    label: String,
    batch_size: usize,
    buffer: Vec<HashMap<String, Value>>,
    session_write_guard: Option<Arc<AtomicBool>>,
    finished: bool,
}

impl StreamingAppender {
    /// Append a single row of properties.
    ///
    /// The row is buffered internally. When the buffer reaches `batch_size`,
    /// it is automatically flushed to the underlying bulk writer.
    pub async fn append(&mut self, properties: impl Into<HashMap<String, Value>>) -> Result<()> {
        self.buffer.push(properties.into());
        if self.buffer.len() >= self.batch_size {
            self.flush_buffer().await?;
        }
        Ok(())
    }

    /// Append an Arrow `RecordBatch` of rows.
    ///
    /// Each row in the batch is converted to a property map and buffered.
    /// Columns in the batch become property keys; values are converted from
    /// Arrow types to Uni [`Value`]s via `arrow_to_value`.
    pub async fn write_batch(&mut self, batch: &arrow_array::RecordBatch) -> Result<()> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let mut props = HashMap::with_capacity(schema.fields().len());
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value =
                    uni_store::storage::arrow_convert::arrow_to_value(col.as_ref(), row_idx, None);
                if !value.is_null() {
                    props.insert(field.name().clone(), value);
                }
            }
            self.buffer.push(props);
            if self.buffer.len() >= self.batch_size {
                self.flush_buffer().await?;
            }
        }
        Ok(())
    }

    /// Flush all buffered rows and commit the bulk writer.
    ///
    /// Consumes the appender. Returns statistics about the loading operation.
    /// The write guard is released when this method returns (or on error via Drop).
    pub async fn finish(mut self) -> Result<BulkStats> {
        self.flush_buffer().await?;
        let writer = self
            .writer
            .take()
            .ok_or_else(|| UniError::Internal(anyhow::anyhow!("Appender already finished")))?;
        let stats = writer.commit().await.map_err(UniError::Internal)?;
        self.finished = true;
        Ok(stats)
    }

    /// Abort the appender without committing.
    ///
    /// Consumes the appender. Discards all buffered and previously flushed rows.
    /// Releases the write guard.
    pub fn abort(mut self) {
        self.buffer.clear();
        self.writer.take(); // Drop the writer
        self.finished = true;
    }

    /// Get the number of rows currently buffered (not yet flushed).
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let rows = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.batch_size));
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| UniError::Internal(anyhow::anyhow!("Appender already finished")))?;
        writer
            .insert_vertices(&self.label, rows)
            .await
            .map_err(UniError::Internal)?;
        Ok(())
    }
}

impl Drop for StreamingAppender {
    fn drop(&mut self) {
        if !self.finished {
            // Release write guard — buffered data is lost
            if let Some(guard) = &self.session_write_guard {
                guard.store(false, Ordering::SeqCst);
            }
        }
    }
}
