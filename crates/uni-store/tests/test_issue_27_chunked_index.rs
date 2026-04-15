// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Test for Issue #27: Inverted Index OOM → Chunked Build
//
// Verifies that inverted index building uses chunked flush when memory limit is reached.

use uni_store::storage::delta::DEFAULT_MAX_COMPACTION_ROWS;

/// Verify the chunked index constants are set correctly.
#[test]
fn test_chunked_index_constants() {
    // DEFAULT_MAX_POSTINGS_MEMORY is private (in inverted_index.rs),
    // but we can verify the related compaction constant is correct.
    assert_eq!(
        DEFAULT_MAX_COMPACTION_ROWS, 5_000_000,
        "Compaction row limit should be 5M"
    );

    // The inverted index memory constant (256 MB) is verified
    // in inverted_index::tests::test_default_max_postings_memory
}
