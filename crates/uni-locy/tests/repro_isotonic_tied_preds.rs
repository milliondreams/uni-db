//! Repro for calibration.rs:341 — IsotonicFitter PAV never pools blocks with
//! tied prediction x (merge only fires on strict mean-decrease `ma > mb`), so
//! for tied predictions spanning both label classes the calibrator emits
//! multiple knots at the same x with different y. `apply` at that x then
//! returns the LOWEST block's mean (0.0) instead of the correct pooled mean
//! (0.5), and jumps to 1.0 just above.

use uni_locy::{CalibratorFitter, IsotonicFitter};

#[test]
fn isotonic_tied_predictions_return_lowest_block_not_pooled_mean() {
    // Four identical predictions x=0.7 with labels F,F,T,T. A valid isotonic
    // (monotone) fit CANNOT map a single x to two different y, so the correct
    // fitted value for all four tied points is the pooled weighted mean = 0.5.
    let cal = IsotonicFitter
        .fit(&[0.7, 0.7, 0.7, 0.7], &[false, false, true, true])
        .expect("fit should succeed");

    let at = cal.apply(0.7);
    let just_above = cal.apply(0.7 + 1e-6);

    // FIXED (calibration.rs): PAV now pools equal-x blocks, so all four tied
    // points map to the single pooled mean 0.5, and there is no jump just above.
    assert_eq!(at, 0.5, "tied-x apply must return the pooled mean 0.5, got {at}");
    assert_eq!(
        just_above, 0.5,
        "just above the tie must also be 0.5 (no phantom jump), got {just_above}"
    );
}
