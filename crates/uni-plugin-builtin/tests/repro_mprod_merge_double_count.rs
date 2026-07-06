//! Repro for crates/uni-plugin-builtin/src/locy_aggregates.rs:891
//!
//! `MprodState::merge` computes
//!     self.product *= o.product * o.log_sum.exp()   (when o.use_log)
//! but once a state switches to log space, `o.log_sum.exp()` ALREADY equals
//! the complete accumulated product (log_sum was seeded with
//! `ln(product_at_switch)`), while `o.product` is frozen at
//! `product_at_switch`. Multiplying both double-counts the switch-time
//! partial product by an extra factor of exactly `o.product`.
//!
//! This exercises the real public API: `MprodAgg` (pub) -> `create()` ->
//! `ingest_indices` -> `merge` -> `finalize`, all trait methods from
//! `uni_plugin::traits::locy`.

use arrow_array::Float64Array;
use uni_plugin::traits::locy::{FoldContext, FoldSemiring, LocyAggregate};
use uni_plugin_builtin::locy_aggregates::MprodAgg;

// Rust guideline compliant

/// Build an MPROD state that has crossed into log space.
///
/// With epsilon = 0.1 and eight factors of 0.5:
///   step1 product=0.5    (>= 0.1)
///   step2 product=0.25   (>= 0.1)
///   step3 product=0.125  (>= 0.1)
///   step4 product=0.0625 (<  0.1) -> switch: use_log=true,
///                                    product frozen at 0.5^4,
///                                    log_sum = ln(0.5^4)
///   steps 5..8 add ln(0.5) each -> log_sum = ln(0.5^8)
/// So after ingest: use_log=true, product = 0.5^4, log_sum.exp() = 0.5^8.
fn logspace_state() -> Box<dyn uni_plugin::traits::locy::LocyAggState> {
    let cx = FoldContext {
        strict: false,
        epsilon: 0.1,
        semiring: FoldSemiring::AddMult,
    };
    let mut s = MprodAgg.create();
    let col = Float64Array::from(vec![Some(0.5); 8]);
    let idx: Vec<usize> = (0..8).collect();
    s.ingest_indices(&col, &idx, &cx).unwrap();
    s
}

#[test]
fn mprod_merge_double_counts_switch_time_product() {
    // Sanity: the log-space state alone finalizes to the correct 0.5^8.
    let o = logspace_state();
    let alone = match o.finalize().unwrap() {
        datafusion::scalar::ScalarValue::Float64(Some(v)) => v,
        other => panic!("expected Float64(Some), got {other:?}"),
    };
    let correct = 0.5_f64.powi(8); // 0.00390625
    assert!(
        (alone - correct).abs() < 1e-12,
        "log-space state should finalize to 0.5^8={correct}, got {alone}"
    );

    // Merge the log-space state into a FRESH state (product=1.0, use_log=false).
    let mut fresh = MprodAgg.create();
    fresh.merge(o.as_ref()).unwrap();
    let merged = match fresh.finalize().unwrap() {
        datafusion::scalar::ScalarValue::Float64(Some(v)) => v,
        other => panic!("expected Float64(Some), got {other:?}"),
    };

    // FIXED (locy_aggregates.rs): merging an empty accumulator with a state whose
    // true product is 0.5^8 yields 0.5^8 — `merge` folds `other`'s single value
    // (`log_sum.exp()` in log space) instead of multiplying both `o.product` and
    // `o.log_sum.exp()`.
    println!("correct (0.5^8)  = {correct}");
    println!("observed merged  = {merged}");

    assert!(
        (merged - correct).abs() < 1e-12,
        "merge must equal the true product 0.5^8={correct}, got {merged}"
    );
}
