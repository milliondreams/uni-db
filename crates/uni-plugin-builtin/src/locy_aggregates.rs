//! Built-in Locy aggregate registrations.
//!
//! Implements every built-in Locy fold aggregate as a [`LocyAggregate`]
//! trait object. Each aggregate carries its [`Semilattice`] metadata so
//! the fixpoint engine's monotonicity proofs are explicit.

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use uni_plugin::traits::locy::{LocyAggState, LocyAggregate, Semilattice};
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName};

/// Register all built-in Locy aggregates into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a built-in qname is
/// already taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.locy_aggregate(QName::builtin("MIN"), Arc::new(MinAgg))?;
    r.locy_aggregate(QName::builtin("MAX"), Arc::new(MaxAgg))?;
    r.locy_aggregate(QName::builtin("SUM"), Arc::new(SumAgg))?;
    r.locy_aggregate(QName::builtin("MSUM"), Arc::new(MSumAgg))?;
    r.locy_aggregate(QName::builtin("COUNT"), Arc::new(CountAgg))?;
    r.locy_aggregate(QName::builtin("AVG"), Arc::new(AvgAgg))?;
    r.locy_aggregate(QName::builtin("COLLECT"), Arc::new(CollectAgg))?;
    r.locy_aggregate(QName::builtin("MNOR"), Arc::new(MnorAgg))?;
    r.locy_aggregate(QName::builtin("MPROD"), Arc::new(MprodAgg))?;
    Ok(())
}

// =========================================================================
// MIN — idem ∧ comm ∧ assoc ∧ monotone ∧ has_top
// =========================================================================

/// `MIN` aggregate — bounded floor with monotonic-decreasing semantics.
#[derive(Debug)]
pub struct MinAgg;

impl LocyAggregate for MinAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::BOUNDED_MIN_MAX
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(MinState {
            value: f64::INFINITY,
        })
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(f64::INFINITY)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(if val < accum { val } else { accum })
    }
}

#[derive(Debug)]
struct MinState {
    value: f64,
}

impl LocyAggState for MinState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest(&mut self, batch: &RecordBatch, value_col: usize) -> Result<(), FnError> {
        ingest_f64_column(batch, value_col, |v| {
            if v < self.value {
                self.value = v;
            }
        })
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<MinState>(other)?;
        if o.value < self.value {
            self.value = o.value;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Float64(Some(self.value)))
    }
    fn is_at_top(&self) -> bool {
        // Min over an unbounded float domain has no useful saturation; the
        // fixpoint engine treats this as "never short-circuits."
        false
    }
}

// =========================================================================
// MAX — idem ∧ comm ∧ assoc ∧ monotone ∧ has_top
// =========================================================================

/// `MAX` aggregate — bounded ceiling with monotonic-increasing semantics.
#[derive(Debug)]
pub struct MaxAgg;

impl LocyAggregate for MaxAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::BOUNDED_MIN_MAX
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(MaxState {
            value: f64::NEG_INFINITY,
        })
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(f64::NEG_INFINITY)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(if val > accum { val } else { accum })
    }
}

#[derive(Debug)]
struct MaxState {
    value: f64,
}

impl LocyAggState for MaxState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest(&mut self, batch: &RecordBatch, value_col: usize) -> Result<(), FnError> {
        ingest_f64_column(batch, value_col, |v| {
            if v > self.value {
                self.value = v;
            }
        })
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<MaxState>(other)?;
        if o.value > self.value {
            self.value = o.value;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Float64(Some(self.value)))
    }
}

// =========================================================================
// SUM — comm ∧ assoc (monotone over non-negative; rejected in recursive)
// =========================================================================

/// `SUM` aggregate — additive monoid over `f64`.
///
/// Not monotone in general (negative inputs can decrease totals). The
/// compiler should reject recursive use unless inputs are constrained
/// non-negative; the [`Semilattice::monotone_join`] flag is `false`.
#[derive(Debug)]
pub struct SumAgg;

impl LocyAggregate for SumAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::NON_MONOTONE
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(SumState { value: 0.0 })
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(accum + val)
    }
}

#[derive(Debug)]
struct SumState {
    value: f64,
}

impl LocyAggState for SumState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest(&mut self, batch: &RecordBatch, value_col: usize) -> Result<(), FnError> {
        ingest_f64_column(batch, value_col, |v| {
            self.value += v;
        })
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        self.value += downcast_state::<SumState>(other)?.value;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Float64(Some(self.value)))
    }
}

// =========================================================================
// MSUM — comm ∧ assoc ∧ monotone (caller asserts non-negative inputs)
// =========================================================================

/// `MSUM` aggregate — monotone sum-of-non-negatives.
///
/// Identical runtime to [`SumAgg`] (sum of `f64`s), but declares
/// [`Semilattice::monotone_join`] = `true`. The MSUM contract is that
/// the caller guarantees non-negative inputs; with that guarantee, the
/// running sum is monotonically non-decreasing across fixpoint iterations,
/// so MSUM is sound to use in recursive Locy strata. A separate compiler
/// warning (`uni_locy::compiler::typecheck::check_msum_warning`) flags
/// MSUM call sites whose argument is not a literal so users are reminded
/// of the non-negativity precondition.
///
/// `has_top` is `false` because the sum is unbounded.
#[derive(Debug)]
pub struct MSumAgg;

impl LocyAggregate for MSumAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice {
            idempotent: false,
            commutative: true,
            associative: true,
            monotone_join: true,
            has_top: false,
        }
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(SumState { value: 0.0 })
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(accum + val)
    }
}

// =========================================================================
// COUNT — comm ∧ assoc ∧ monotone (top = ∞)
// =========================================================================

/// `COUNT` aggregate — monotone but unbounded.
#[derive(Debug)]
pub struct CountAgg;

impl LocyAggregate for CountAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::COUNT
    }
    fn output_type(&self) -> DataType {
        DataType::Int64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(CountState { value: 0 })
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    // Row-level fast path: matches the legacy COUNT behavior in
    // `MonotonicAggState` which simply increments by the per-row value
    // (the engine writes `val=1` for COUNT-shaped aggregates today).
    fn update_step(&self, accum: f64, val: f64, _strict: bool) -> Result<f64, FnError> {
        Ok(accum + val)
    }
}

#[derive(Debug)]
struct CountState {
    value: i64,
}

impl LocyAggState for CountState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest(&mut self, batch: &RecordBatch, _value_col: usize) -> Result<(), FnError> {
        self.value += batch.num_rows() as i64;
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        self.value += downcast_state::<CountState>(other)?.value;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Int64(Some(self.value)))
    }
}

// =========================================================================
// AVG — comm ∧ assoc (non-monotone)
// =========================================================================

/// `AVG` aggregate — arithmetic mean; non-monotone.
#[derive(Debug)]
pub struct AvgAgg;

impl LocyAggregate for AvgAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice::NON_MONOTONE
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(AvgState { sum: 0.0, count: 0 })
    }
}

#[derive(Debug)]
struct AvgState {
    sum: f64,
    count: i64,
}

impl LocyAggState for AvgState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest(&mut self, batch: &RecordBatch, value_col: usize) -> Result<(), FnError> {
        let mut local_sum = 0.0;
        let mut local_count = 0_i64;
        ingest_f64_column(batch, value_col, |v| {
            local_sum += v;
            local_count += 1;
        })?;
        self.sum += local_sum;
        self.count += local_count;
        Ok(())
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<AvgState>(other)?;
        self.sum += o.sum;
        self.count += o.count;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        if self.count == 0 {
            return Ok(ScalarValue::Float64(None));
        }
        // M-DOCUMENTED-MAGIC: dividing by `count` as f64 is exact for the
        // sub-2^53 range we expect for fixpoint aggregations.
        Ok(ScalarValue::Float64(Some(self.sum / self.count as f64)))
    }
}

// =========================================================================
// COLLECT — comm ∧ assoc ∧ monotone (multiset semilattice)
// =========================================================================

/// `COLLECT` aggregate — assembles values into a list.
#[derive(Debug)]
pub struct CollectAgg;

impl LocyAggregate for CollectAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice {
            idempotent: false,
            commutative: true,
            associative: true,
            // Monotone under multiset inclusion — adding rows never shrinks.
            monotone_join: true,
            has_top: false,
        }
    }
    fn output_type(&self) -> DataType {
        DataType::Utf8 // Surfaced as JSON-encoded list for the M3 facade
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(CollectState { values: Vec::new() })
    }
}

#[derive(Debug)]
struct CollectState {
    values: Vec<f64>,
}

impl LocyAggState for CollectState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest(&mut self, batch: &RecordBatch, value_col: usize) -> Result<(), FnError> {
        ingest_f64_column(batch, value_col, |v| self.values.push(v))
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<CollectState>(other)?;
        self.values.extend_from_slice(&o.values);
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Utf8(Some(
            serde_json::to_string(&self.values)
                .map_err(|e| FnError::new(0x500, format!("collect serialize: {e}")))?,
        )))
    }
}

// =========================================================================
// MNOR — noisy-OR (1 − ∏(1 − p_i)); idem ∧ comm ∧ assoc ∧ monotone, top=1
// =========================================================================

/// `MNOR` (noisy-OR) aggregate.
///
/// `1 − ∏(1 − pᵢ)`: combines independent probabilistic evidence into a
/// monotone-increasing cumulative belief bounded by 1.0.
#[derive(Debug)]
pub struct MnorAgg;

impl LocyAggregate for MnorAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice {
            idempotent: true,
            commutative: true,
            associative: true,
            monotone_join: true,
            has_top: true,
        }
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(MnorState { value: 0.0 })
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        Some(0.0)
    }
    fn update_step(&self, accum: f64, val: f64, strict: bool) -> Result<f64, FnError> {
        if !(0.0..=1.0).contains(&val) {
            if strict {
                return Err(FnError::new(
                    0x501,
                    format!("strict_probability_domain: MNOR input {val} is outside [0, 1]"),
                ));
            }
            tracing::warn!(
                "MNOR input {val} outside [0,1], clamped to {}",
                val.clamp(0.0, 1.0)
            );
        }
        let p = val.clamp(0.0, 1.0);
        // 1 − (1 − accum)·(1 − p) = accum + p − accum·p
        Ok(1.0 - (1.0 - accum) * (1.0 - p))
    }
    fn is_probability_aggregate(&self) -> bool {
        true
    }
    fn is_noisy_or(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct MnorState {
    /// Current cumulative noisy-OR: `1 − ∏(1 − p_i)` already applied.
    value: f64,
}

impl LocyAggState for MnorState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest(&mut self, batch: &RecordBatch, value_col: usize) -> Result<(), FnError> {
        ingest_f64_column(batch, value_col, |p| {
            // `1 − (1 − cur)·(1 − p)` = `cur + p − cur·p`
            self.value = self.value + p - self.value * p;
        })
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        let o = downcast_state::<MnorState>(other)?;
        // (1 − a) · (1 − b) → 1 − (1 − a)·(1 − b) = a + b − a·b
        self.value = self.value + o.value - self.value * o.value;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Float64(Some(self.value)))
    }
    fn is_at_top(&self) -> bool {
        // Saturated at the noisy-OR top (1.0). Allow a tiny epsilon for
        // floating-point drift accumulated across the fixpoint.
        // M-DOCUMENTED-MAGIC: 1e-12 is well above f64 noise from
        // log/exp-style updates we'd see in long fixpoints; tighter would
        // miss real saturation, looser would short-circuit prematurely.
        self.value >= 1.0 - 1e-12
    }
}

// =========================================================================
// MPROD — bounded product (∏ p_i); idem ∧ comm ∧ assoc ∧ monotone-DOWN, top=0
// =========================================================================

/// `MPROD` (bounded product) aggregate.
#[derive(Debug)]
pub struct MprodAgg;

impl LocyAggregate for MprodAgg {
    fn semilattice(&self) -> Semilattice {
        Semilattice {
            idempotent: true,
            commutative: true,
            associative: true,
            monotone_join: true,
            has_top: true,
        }
    }
    fn output_type(&self) -> DataType {
        DataType::Float64
    }
    fn create(&self) -> Box<dyn LocyAggState> {
        Box::new(MprodState { value: 1.0 })
    }
    fn initial_accum_f64(&self) -> Option<f64> {
        // MonotonicAggState's legacy MPROD path starts accumulators at 1.0
        // before multiplying — matches MprodState::create's default.
        Some(1.0)
    }
    fn update_step(&self, accum: f64, val: f64, strict: bool) -> Result<f64, FnError> {
        if !(0.0..=1.0).contains(&val) {
            if strict {
                return Err(FnError::new(
                    0x501,
                    format!("strict_probability_domain: MPROD input {val} is outside [0, 1]"),
                ));
            }
            tracing::warn!(
                "MPROD input {val} outside [0,1], clamped to {}",
                val.clamp(0.0, 1.0)
            );
        }
        let p = val.clamp(0.0, 1.0);
        Ok(accum * p)
    }
    fn is_probability_aggregate(&self) -> bool {
        true
    }
    // is_noisy_or stays false (default) — MPROD is bounded-product, not noisy-OR.
}

#[derive(Debug)]
struct MprodState {
    value: f64,
}

impl LocyAggState for MprodState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn ingest(&mut self, batch: &RecordBatch, value_col: usize) -> Result<(), FnError> {
        ingest_f64_column(batch, value_col, |p| {
            self.value *= p;
        })
    }
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError> {
        self.value *= downcast_state::<MprodState>(other)?.value;
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Float64(Some(self.value)))
    }
    fn is_at_top(&self) -> bool {
        // Saturated at the product top (0.0) once any input was zero.
        self.value <= 1e-12
    }
}

// =========================================================================
// Helpers
// =========================================================================

fn ingest_f64_column(
    batch: &RecordBatch,
    value_col: usize,
    mut visit: impl FnMut(f64),
) -> Result<(), FnError> {
    use arrow_array::{Array, Float64Array};

    let col = batch.column(value_col);
    let arr = col.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("expected Float64Array at column {value_col}"),
        )
    })?;
    for i in 0..arr.len() {
        if !arr.is_null(i) {
            visit(arr.value(i));
        }
    }
    Ok(())
}

fn downcast_state<S: LocyAggState + 'static>(other: &dyn LocyAggState) -> Result<&S, FnError> {
    other.as_any().downcast_ref::<S>().ok_or_else(|| {
        FnError::new(
            0x501,
            "LocyAggState::merge invoked with mismatched concrete state",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use arrow_schema::{Field, Schema};

    fn one_col_batch(values: Vec<Option<f64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let arr = Arc::new(Float64Array::from(values));
        RecordBatch::try_new(schema, vec![arr]).unwrap()
    }

    #[test]
    fn min_finds_smallest_skipping_nulls() {
        let agg = MinAgg;
        let mut s = agg.create();
        s.ingest(
            &one_col_batch(vec![Some(3.0), None, Some(1.0), Some(2.0)]),
            0,
        )
        .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert_eq!(v, 1.0),
            other => panic!("expected Float64(Some), got {other:?}"),
        }
    }

    #[test]
    fn max_finds_largest_skipping_nulls() {
        let agg = MaxAgg;
        let mut s = agg.create();
        s.ingest(
            &one_col_batch(vec![Some(3.0), None, Some(1.0), Some(2.0)]),
            0,
        )
        .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert_eq!(v, 3.0),
            other => panic!("expected Float64(Some), got {other:?}"),
        }
    }

    #[test]
    fn sum_adds_skipping_nulls() {
        let agg = SumAgg;
        let mut s = agg.create();
        s.ingest(
            &one_col_batch(vec![Some(1.0), None, Some(2.5), Some(0.5)]),
            0,
        )
        .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert_eq!(v, 4.0),
            other => panic!("expected Float64(Some), got {other:?}"),
        }
    }

    #[test]
    fn count_counts_all_rows_in_batch() {
        let agg = CountAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(1.0), None, Some(2.0)]), 0)
            .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Int64(Some(v)) => assert_eq!(v, 3),
            other => panic!("expected Int64(Some), got {other:?}"),
        }
    }

    #[test]
    fn avg_computes_mean() {
        let agg = AvgAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(2.0), Some(4.0), Some(6.0)]), 0)
            .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert_eq!(v, 4.0),
            other => panic!("expected Float64(Some), got {other:?}"),
        }
    }

    #[test]
    fn avg_empty_yields_null() {
        let agg = AvgAgg;
        let s = agg.create();
        match s.finalize().unwrap() {
            ScalarValue::Float64(None) => {}
            other => panic!("expected Float64(None), got {other:?}"),
        }
    }

    #[test]
    fn mnor_saturates_at_one() {
        let agg = MnorAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(0.5), Some(0.5)]), 0)
            .unwrap();
        // 1 − (1 − 0.5)·(1 − 0.5) = 1 − 0.25 = 0.75
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert!((v - 0.75).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
        assert!(!s.is_at_top());

        s.ingest(&one_col_batch(vec![Some(1.0)]), 0).unwrap();
        // Adding a 1.0 saturates noisy-OR.
        assert!(s.is_at_top());
    }

    #[test]
    fn mprod_saturates_at_zero() {
        let agg = MprodAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(0.5), Some(0.4)]), 0)
            .unwrap();
        // 0.5 × 0.4 = 0.2
        match s.finalize().unwrap() {
            ScalarValue::Float64(Some(v)) => assert!((v - 0.2).abs() < 1e-9),
            other => panic!("expected Float64, got {other:?}"),
        }
        assert!(!s.is_at_top());

        s.ingest(&one_col_batch(vec![Some(0.0)]), 0).unwrap();
        assert!(s.is_at_top());
    }

    #[test]
    fn collect_assembles_list() {
        let agg = CollectAgg;
        let mut s = agg.create();
        s.ingest(&one_col_batch(vec![Some(1.0), Some(2.0)]), 0)
            .unwrap();
        match s.finalize().unwrap() {
            ScalarValue::Utf8(Some(s)) => assert_eq!(s, "[1.0,2.0]"),
            other => panic!("expected Utf8(Some), got {other:?}"),
        }
    }

    #[test]
    fn semilattice_metadata_matches_expectations() {
        assert_eq!(MinAgg.semilattice(), Semilattice::BOUNDED_MIN_MAX);
        assert_eq!(MaxAgg.semilattice(), Semilattice::BOUNDED_MIN_MAX);
        assert_eq!(SumAgg.semilattice(), Semilattice::NON_MONOTONE);
        assert_eq!(CountAgg.semilattice(), Semilattice::COUNT);
        assert_eq!(AvgAgg.semilattice(), Semilattice::NON_MONOTONE);
        assert!(MnorAgg.semilattice().monotone_join);
        assert!(MnorAgg.semilattice().has_top);
        assert!(MprodAgg.semilattice().monotone_join);
        assert!(MprodAgg.semilattice().has_top);
    }
}
