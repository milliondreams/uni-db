// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Python bindings for the BTIC temporal interval type.

use pyo3::prelude::*;
use uni_common::uni_btic::{self, Btic};

/// A temporal interval encoded as a half-open range ``[lo, hi)`` in
/// milliseconds since the Unix epoch, plus a packed metadata word
/// carrying per-bound granularity and certainty.
///
/// Construct from an ISO 8601-inspired string literal::
///
///     Btic("1985")
///     Btic("1985-03/2024-06")
///     Btic("~1985")           # approximate
///     Btic("2020-03/")        # ongoing
///     Btic("/")               # fully unbounded
///
/// Or from raw fields::
///
///     Btic.from_raw(lo_ms, hi_ms, meta)
#[pyclass(name = "Btic", frozen, eq, ord, hash)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PyBtic {
    pub(crate) inner: Btic,
}

#[pymethods]
impl PyBtic {
    /// Create a BTIC interval from a string literal.
    #[new]
    fn new(literal: &str) -> PyResult<Self> {
        let btic = uni_btic::parse::parse_btic_literal(literal).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid BTIC literal: {e}"))
        })?;
        Ok(Self { inner: btic })
    }

    /// Create a BTIC interval from raw lo/hi/meta fields.
    #[staticmethod]
    fn from_raw(lo: i64, hi: i64, meta: u64) -> PyResult<Self> {
        let btic = Btic::new(lo, hi, meta).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid BTIC value: {e}"))
        })?;
        Ok(Self { inner: btic })
    }

    // ── Getters ──────────────────────────────────────────────────────

    /// Lower bound in milliseconds since epoch (i64::MIN = -infinity).
    #[getter]
    fn lo(&self) -> i64 {
        self.inner.lo()
    }

    /// Upper bound in milliseconds since epoch (i64::MAX = +infinity).
    #[getter]
    fn hi(&self) -> i64 {
        self.inner.hi()
    }

    /// Raw 64-bit metadata word (granularity + certainty packed).
    #[getter]
    fn meta(&self) -> u64 {
        self.inner.meta()
    }

    /// Lower bound granularity name (e.g. "year", "day", "millisecond").
    #[getter]
    fn lo_granularity(&self) -> &'static str {
        self.inner.lo_granularity().name()
    }

    /// Upper bound granularity name.
    #[getter]
    fn hi_granularity(&self) -> &'static str {
        self.inner.hi_granularity().name()
    }

    /// Lower bound certainty name (e.g. "definite", "approximate").
    #[getter]
    fn lo_certainty(&self) -> &'static str {
        self.inner.lo_certainty().name()
    }

    /// Upper bound certainty name.
    #[getter]
    fn hi_certainty(&self) -> &'static str {
        self.inner.hi_certainty().name()
    }

    /// Duration of the interval in milliseconds, or None if unbounded.
    #[getter]
    fn duration_ms(&self) -> Option<i64> {
        self.inner.duration_ms()
    }

    /// True if the interval is exactly 1 millisecond wide.
    #[getter]
    fn is_instant(&self) -> bool {
        self.inner.is_instant()
    }

    /// True if either bound is infinite.
    #[getter]
    fn is_unbounded(&self) -> bool {
        self.inner.is_unbounded()
    }

    /// True if both bounds are finite.
    #[getter]
    fn is_finite(&self) -> bool {
        self.inner.is_finite()
    }

    // ── Allen predicates ─────────────────────────────────────────────

    /// True if the given millisecond-epoch point falls within [lo, hi).
    fn contains_point(&self, point_ms: i64) -> bool {
        uni_btic::predicates::contains_point(&self.inner, point_ms)
    }

    /// True if the two intervals share at least one tick.
    fn overlaps(&self, other: &PyBtic) -> bool {
        uni_btic::predicates::overlaps(&self.inner, &other.inner)
    }

    /// True if this interval fully contains the other (inclusive bounds).
    fn contains(&self, other: &PyBtic) -> bool {
        uni_btic::predicates::contains(&self.inner, &other.inner)
    }

    /// True if this interval ends before or at the start of the other.
    fn before(&self, other: &PyBtic) -> bool {
        uni_btic::predicates::before(&self.inner, &other.inner)
    }

    /// True if this interval starts at or after the end of the other.
    fn after(&self, other: &PyBtic) -> bool {
        uni_btic::predicates::after(&self.inner, &other.inner)
    }

    /// True if this interval's hi equals the other's lo (exactly adjacent).
    fn meets(&self, other: &PyBtic) -> bool {
        uni_btic::predicates::meets(&self.inner, &other.inner)
    }

    /// True if either meets or met-by (symmetric adjacency).
    fn adjacent(&self, other: &PyBtic) -> bool {
        uni_btic::predicates::adjacent(&self.inner, &other.inner)
    }

    /// True if the intervals share no ticks.
    fn disjoint(&self, other: &PyBtic) -> bool {
        uni_btic::predicates::disjoint(&self.inner, &other.inner)
    }

    // ── Set operations ───────────────────────────────────────────────

    /// The overlapping portion of two intervals, or None if disjoint.
    fn intersection(&self, other: &PyBtic) -> Option<PyBtic> {
        uni_btic::set_ops::intersection(&self.inner, &other.inner).map(|b| PyBtic { inner: b })
    }

    /// The smallest interval spanning both inputs.
    fn span(&self, other: &PyBtic) -> PyBtic {
        PyBtic {
            inner: uni_btic::set_ops::span(&self.inner, &other.inner),
        }
    }

    /// The gap between two disjoint intervals, or None if overlapping.
    fn gap(&self, other: &PyBtic) -> Option<PyBtic> {
        uni_btic::set_ops::gap(&self.inner, &other.inner).map(|b| PyBtic { inner: b })
    }

    // ── Dunder methods ───────────────────────────────────────────────

    fn __repr__(&self) -> String {
        format!("Btic(\"{}\")", self.inner)
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    // __eq__, __hash__, __richcmp__ auto-generated by #[pyclass(eq, ord, hash)]
}
