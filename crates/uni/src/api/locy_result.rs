// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Spec-compliant wrapper for `uni_locy::LocyResult` (§7.4).
//!
//! Adds `metrics()` and `derived()` accessors while exposing every
//! field and method of the inner type transparently via `Deref`.

use std::ops::Deref;

use uni_locy::DerivedFactSet;
use uni_query::QueryMetrics;

/// Locy evaluation result with spec-compliant accessors.
///
/// Wraps `uni_locy::LocyResult` and adds:
/// - `metrics()` → `&QueryMetrics` (§7.4)
/// - `derived()` → `Option<&DerivedFactSet>` (spec-compliant alias for `derived_fact_set`)
///
/// All fields and methods of the inner `uni_locy::LocyResult` are accessible
/// via `Deref` (e.g. `result.stats`, `result.rows()`, `result.warnings()`).
#[derive(Debug, Clone)]
pub struct LocyResult {
    inner: uni_locy::LocyResult,
    metrics: QueryMetrics,
}

impl LocyResult {
    pub(crate) fn new(inner: uni_locy::LocyResult, metrics: QueryMetrics) -> Self {
        Self { inner, metrics }
    }

    /// Query-level execution metrics (timing, row counts, cache stats).
    pub fn metrics(&self) -> &QueryMetrics {
        &self.metrics
    }

    /// Spec-compliant accessor for the derived fact set (§7.4).
    pub fn derived(&self) -> Option<&DerivedFactSet> {
        self.inner.derived_fact_set.as_ref()
    }

    /// Unwrap into the inner `uni_locy::LocyResult`, discarding metrics.
    pub fn into_inner(self) -> uni_locy::LocyResult {
        self.inner
    }

    /// Decompose into inner result and metrics.
    pub fn into_parts(self) -> (uni_locy::LocyResult, QueryMetrics) {
        (self.inner, self.metrics)
    }
}

impl Deref for LocyResult {
    type Target = uni_locy::LocyResult;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
