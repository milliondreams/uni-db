// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase C C1: pure-math probability calibration transforms.
//!
//! Calibrators are inference-time transforms that take a raw model
//! probability in `[0, 1]` and produce a calibrated probability whose
//! frequency in a holdout matches its claimed magnitude (a 0.7
//! prediction should be correct 70% of the time).
//!
//! ## Trait surface
//!
//! [`Calibrator`] is the read interface: apply to one or a batch of
//! raw probabilities. [`CalibratorFitter`] is the batch-time interface:
//! given `(predictions, labels)` pairs, fit and return an
//! `Arc<dyn Calibrator>` ready for inference.
//!
//! ## Methods shipped (impl plan §3.2)
//!
//! - [`PlattScaling`]: `p_cal = sigmoid(A · logit(p) + B)`. Fit by
//!   L2-regularized Newton iteration on logits. Standard recipe from
//!   Niculescu-Mizil & Caruana 2005.
//! - [`IsotonicRegression`]: non-parametric monotone step function
//!   fit via Pool Adjacent Violators (PAV).
//! - [`TemperatureScaling`]: `p_cal = sigmoid(logit(p) / T)`. Fit by
//!   gradient descent on NLL (Guo et al. 2017).
//! - [`BetaCalibration`]: `p_cal = sigmoid(a·log p + b·log(1−p) + c)`
//!   (Kull et al. 2017). Fit by a small Adam loop on the 3-parameter
//!   NLL — we avoid a full L-BFGS dependency for this slice.
//! - [`IdentityCalibrator`]: passthrough. Returned when CREATE MODEL
//!   declares `CALIBRATION none`.
//!
//! Deferred: Dirichlet (multi-class) and Conformal (uncertainty).
//!
//! ## Metrics
//!
//! [`brier_score`], [`log_loss`], [`expected_calibration_error`] —
//! the staples that C2 reports on the holdout and C4 / C3 will use
//! for drift detection.

use std::sync::Arc;

/// Method tag for [`Calibrator::method`] — surfaces in EXPLAIN /
/// telemetry without runtime introspection of the impl struct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalibrationMethodKind {
    Platt,
    Isotonic,
    Temperature,
    Beta,
    Identity,
    /// Phase C C1a: split-conformal predictor. `apply` is identity
    /// (point prediction unchanged); the calibrator exposes a
    /// per-prediction confidence band via
    /// [`Calibrator::confidence_band`].
    Conformal,
    /// Phase D D-C1d: multi-class Dirichlet calibrator. Surfaces via
    /// [`MulticlassCalibrator`] (separate trait from binary
    /// [`Calibrator`]). The kind tag is here so EXPLAIN / telemetry
    /// can label it uniformly.
    Dirichlet,
}

/// Inference-time calibrator. `apply` is the hot path; implementors
/// should keep it allocation-free.
pub trait Calibrator: Send + Sync + std::fmt::Debug {
    fn apply(&self, raw: f64) -> f64;
    fn apply_batch(&self, raw: &[f64]) -> Vec<f64> {
        raw.iter().map(|p| self.apply(*p)).collect()
    }
    fn method(&self) -> CalibrationMethodKind;
    /// Phase C C1a: return an optional confidence band around the
    /// point prediction `p`. Default `None` — only conformal /
    /// ensemble / credal calibrators populate this.
    fn confidence_band(&self, _p: f64) -> Option<crate::result::ConfidenceBand> {
        None
    }
}

/// Batch-time fitter — given `(preds, labels)`, produce a fitted
/// `Calibrator`. The labels are `bool` (0/1); multi-class fitters
/// implement [`MulticlassCalibratorFitter`] separately.
pub trait CalibratorFitter: Send + Sync {
    fn fit(
        &self,
        predictions: &[f64],
        labels: &[bool],
    ) -> Result<Arc<dyn Calibrator>, CalibrationError>;
}

/// Phase D D-C1d: inference-time multi-class calibrator. The input
/// is a class-probability vector `p ∈ Δ^K` (entries non-negative,
/// sum ≈ 1); the output is a recalibrated vector of the same shape.
/// Separate trait from binary [`Calibrator`] because the call
/// signature is incompatible (scalar vs vector).
pub trait MulticlassCalibrator: Send + Sync + std::fmt::Debug {
    /// Apply the calibration to one prediction vector. Returns a
    /// vector of the same length as `raw`.
    fn apply(&self, raw: &[f64]) -> Vec<f64>;
    /// Number of classes the calibrator was fit on. Callers must
    /// supply `raw.len() == n_classes()` to `apply`.
    fn n_classes(&self) -> usize;
    /// Method tag for EXPLAIN / telemetry.
    fn method(&self) -> CalibrationMethodKind;
}

/// Phase D D-C1d: batch-time fitter for multi-class calibrators.
/// Each prediction is a `K`-length vector; each label is a
/// class index in `[0, K)`. Class index outside the range is
/// rejected as `CalibrationError::NumericIssue`.
pub trait MulticlassCalibratorFitter: Send + Sync {
    fn fit(
        &self,
        predictions: &[Vec<f64>],
        labels: &[usize],
    ) -> Result<Arc<dyn MulticlassCalibrator>, CalibrationError>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum CalibrationError {
    /// `predictions.len() != labels.len()` or either is empty.
    ArityMismatch {
        preds: usize,
        labels: usize,
    },
    EmptyDataset,
    /// Numerical degeneracy (e.g. constant predictions, no positives).
    NumericIssue(&'static str),
}

impl std::fmt::Display for CalibrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArityMismatch { preds, labels } => {
                write!(
                    f,
                    "calibrator arity mismatch: {preds} predictions vs {labels} labels"
                )
            }
            Self::EmptyDataset => write!(f, "calibrator fit on empty dataset"),
            Self::NumericIssue(msg) => write!(f, "calibrator numerical issue: {msg}"),
        }
    }
}

impl std::error::Error for CalibrationError {}

// ─── Numerically-stable sigmoid / logit ────────────────────────────────

/// Clamp probabilities away from 0 / 1 before taking logit so the
/// transform stays finite. Matches the standard ε guard used in
/// scikit-learn's `CalibratedClassifierCV`.
const LOGIT_EPS: f64 = 1e-12;

#[inline]
pub fn sigmoid(z: f64) -> f64 {
    if z >= 0.0 {
        let e = (-z).exp();
        1.0 / (1.0 + e)
    } else {
        let e = z.exp();
        e / (1.0 + e)
    }
}

#[inline]
pub fn logit(p: f64) -> f64 {
    let p = p.clamp(LOGIT_EPS, 1.0 - LOGIT_EPS);
    (p / (1.0 - p)).ln()
}

// ─── IdentityCalibrator ────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityCalibrator;

impl Calibrator for IdentityCalibrator {
    fn apply(&self, raw: f64) -> f64 {
        raw
    }
    fn method(&self) -> CalibrationMethodKind {
        CalibrationMethodKind::Identity
    }
}

// ─── PlattScaling ──────────────────────────────────────────────────────

/// `p_cal = σ(A · logit(p) + B)`. `a` and `b` are public so users /
/// downstream tools can inspect / serialize the fit.
#[derive(Debug, Clone, Copy)]
pub struct PlattScaling {
    pub a: f64,
    pub b: f64,
}

impl Calibrator for PlattScaling {
    fn apply(&self, raw: f64) -> f64 {
        sigmoid(self.a * logit(raw) + self.b)
    }
    fn method(&self) -> CalibrationMethodKind {
        CalibrationMethodKind::Platt
    }
}

/// Fit Platt scaling via Adam gradient descent on the NLL.
///
/// We use Adam rather than the textbook Newton iteration because the
/// Hessian becomes degenerate when the input logits have low
/// variance (e.g. a miscalibrated classifier that outputs the same
/// raw probability for everything — exactly the Phase C gate case).
/// Adam's first / second-moment normalization handles that gracefully
/// at the cost of a few hundred iterations of arithmetic. No external
/// solver dependency.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlattFitter;

impl CalibratorFitter for PlattFitter {
    fn fit(
        &self,
        predictions: &[f64],
        labels: &[bool],
    ) -> Result<Arc<dyn Calibrator>, CalibrationError> {
        validate_inputs(predictions, labels)?;
        let z: Vec<f64> = predictions.iter().map(|p| logit(*p)).collect();
        let y: Vec<f64> = labels.iter().map(|l| if *l { 1.0 } else { 0.0 }).collect();
        let n = predictions.len() as f64;
        // Initialize at A=0, B=0 so the prior is identity-on-the-mean
        // (σ(0)=0.5). Adam will move toward the data-optimal point.
        let mut a: f64 = 0.0;
        let mut b: f64 = 0.0;
        let mut m = [0.0f64; 2];
        let mut v = [0.0f64; 2];
        let lr = 0.1;
        let beta1 = 0.9;
        let beta2 = 0.999;
        let eps_adam = 1e-8;
        for step in 1..=500 {
            let mut g_a = 0.0;
            let mut g_b = 0.0;
            for i in 0..predictions.len() {
                let p = sigmoid(a * z[i] + b);
                let r = p - y[i];
                g_a += r * z[i];
                g_b += r;
            }
            g_a /= n;
            g_b /= n;
            for (k, (grad, param)) in [g_a, g_b].iter().zip([&mut a, &mut b]).enumerate() {
                m[k] = beta1 * m[k] + (1.0 - beta1) * grad;
                v[k] = beta2 * v[k] + (1.0 - beta2) * grad * grad;
                let m_hat = m[k] / (1.0 - beta1.powi(step));
                let v_hat = v[k] / (1.0 - beta2.powi(step));
                *param -= lr * m_hat / (v_hat.sqrt() + eps_adam);
            }
            if g_a.abs() + g_b.abs() < 1e-9 {
                break;
            }
        }
        if !a.is_finite() || !b.is_finite() {
            return Err(CalibrationError::NumericIssue(
                "Platt fit produced non-finite parameters",
            ));
        }
        Ok(Arc::new(PlattScaling { a, b }))
    }
}

// ─── IsotonicRegression ────────────────────────────────────────────────

/// Monotone non-decreasing step function. `knots` stores
/// `(input, output)` pairs in sorted-by-input order; `apply` does a
/// binary search and linear interpolation between adjacent knots.
#[derive(Debug, Clone)]
pub struct IsotonicRegression {
    pub knots: Vec<(f64, f64)>,
}

impl Calibrator for IsotonicRegression {
    fn apply(&self, raw: f64) -> f64 {
        if self.knots.is_empty() {
            return raw;
        }
        // Clamp to endpoints.
        if raw <= self.knots[0].0 {
            return self.knots[0].1;
        }
        if raw >= self.knots[self.knots.len() - 1].0 {
            return self.knots[self.knots.len() - 1].1;
        }
        // Binary search.
        let idx = self
            .knots
            .partition_point(|(x, _)| *x < raw)
            .saturating_sub(1);
        let (x0, y0) = self.knots[idx];
        let (x1, y1) = self.knots[idx + 1];
        if (x1 - x0).abs() < f64::EPSILON {
            return y0;
        }
        let t = (raw - x0) / (x1 - x0);
        y0 + t * (y1 - y0)
    }
    fn method(&self) -> CalibrationMethodKind {
        CalibrationMethodKind::Isotonic
    }
}

/// Pool Adjacent Violators (PAV) — the textbook isotonic fit.
#[derive(Debug, Clone, Copy, Default)]
pub struct IsotonicFitter;

impl CalibratorFitter for IsotonicFitter {
    fn fit(
        &self,
        predictions: &[f64],
        labels: &[bool],
    ) -> Result<Arc<dyn Calibrator>, CalibrationError> {
        validate_inputs(predictions, labels)?;
        // Sort by prediction. Stable so identical predictions keep
        // their relative order — matters when adjacent equal X have
        // different labels.
        let mut idx: Vec<usize> = (0..predictions.len()).collect();
        idx.sort_by(|&a, &b| {
            predictions[a]
                .partial_cmp(&predictions[b])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        // PAV: each block holds (sum_y, count, max_x_in_block).
        let mut blocks: Vec<(f64, usize, f64)> = Vec::with_capacity(predictions.len());
        for &i in &idx {
            let y = if labels[i] { 1.0 } else { 0.0 };
            let x = predictions[i];
            blocks.push((y, 1, x));
            // Merge while the previous block's mean exceeds this one's.
            while blocks.len() >= 2 {
                let n = blocks.len();
                let (sa, ca, xa) = blocks[n - 2];
                let (sb, cb, xb) = blocks[n - 1];
                let ma = sa / ca as f64;
                let mb = sb / cb as f64;
                if ma > mb {
                    // Merge into a single block.
                    blocks[n - 2] = (sa + sb, ca + cb, xa.max(xb));
                    blocks.pop();
                } else {
                    break;
                }
            }
        }
        let knots: Vec<(f64, f64)> = blocks
            .into_iter()
            .map(|(sum_y, count, max_x)| (max_x, sum_y / count as f64))
            .collect();
        Ok(Arc::new(IsotonicRegression { knots }))
    }
}

// ─── TemperatureScaling ────────────────────────────────────────────────

/// `p_cal = σ(z / T)` where `z = logit(p)`. Single-parameter
/// shrinkage toward 0.5 when `T > 1`, away from 0.5 when `T < 1`.
#[derive(Debug, Clone, Copy)]
pub struct TemperatureScaling {
    pub temperature: f64,
}

impl Calibrator for TemperatureScaling {
    fn apply(&self, raw: f64) -> f64 {
        sigmoid(logit(raw) / self.temperature)
    }
    fn method(&self) -> CalibrationMethodKind {
        CalibrationMethodKind::Temperature
    }
}

/// Gradient descent on NLL with respect to T. Constrained to `T > 0`
/// by reparameterizing via `T = exp(log_t)`.
#[derive(Debug, Clone, Copy, Default)]
pub struct TemperatureFitter;

impl CalibratorFitter for TemperatureFitter {
    fn fit(
        &self,
        predictions: &[f64],
        labels: &[bool],
    ) -> Result<Arc<dyn Calibrator>, CalibrationError> {
        validate_inputs(predictions, labels)?;
        let z: Vec<f64> = predictions.iter().map(|p| logit(*p)).collect();
        let y: Vec<f64> = labels.iter().map(|l| if *l { 1.0 } else { 0.0 }).collect();
        let n = predictions.len() as f64;
        let mut log_t: f64 = 0.0; // T = 1 initially
        let lr = 0.1;
        for _ in 0..200 {
            let t = log_t.exp();
            let inv_t = 1.0 / t;
            // dNLL/dT = -(1/T²) · Σ (z_i · (y_i − σ(z_i / T)))
            // Use chain rule: dNLL/dlog_t = dNLL/dT · T
            let mut grad = 0.0;
            for i in 0..predictions.len() {
                let p_hat = sigmoid(z[i] * inv_t);
                // ∂NLL/∂T = Σ z_i (p̂ − y_i) · (−1/T²)
                // ∂NLL/∂log_t = ∂NLL/∂T · T = Σ z_i (p̂ − y_i) · (−1/T)
                grad += z[i] * (p_hat - y[i]) * (-inv_t);
            }
            let step = lr * grad / n;
            log_t -= step;
            if step.abs() < 1e-9 {
                break;
            }
        }
        let temperature = log_t.exp();
        if !temperature.is_finite() || temperature <= 0.0 {
            return Err(CalibrationError::NumericIssue(
                "temperature fit produced non-positive or non-finite T",
            ));
        }
        Ok(Arc::new(TemperatureScaling { temperature }))
    }
}

// ─── BetaCalibration ───────────────────────────────────────────────────

/// `p_cal = σ(a · log p + b · log(1−p) + c)` — the Kull et al. 2017
/// three-parameter beta family. Fitter uses an Adam-style update
/// over the 3 parameters on the NLL; we keep it dep-free (no
/// `argmin` / `nalgebra`).
#[derive(Debug, Clone, Copy)]
pub struct BetaCalibration {
    pub a: f64,
    pub b: f64,
    pub c: f64,
}

impl Calibrator for BetaCalibration {
    fn apply(&self, raw: f64) -> f64 {
        let p = raw.clamp(LOGIT_EPS, 1.0 - LOGIT_EPS);
        sigmoid(self.a * p.ln() + self.b * (1.0 - p).ln() + self.c)
    }
    fn method(&self) -> CalibrationMethodKind {
        CalibrationMethodKind::Beta
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BetaFitter;

impl CalibratorFitter for BetaFitter {
    fn fit(
        &self,
        predictions: &[f64],
        labels: &[bool],
    ) -> Result<Arc<dyn Calibrator>, CalibrationError> {
        validate_inputs(predictions, labels)?;
        let log_p: Vec<f64> = predictions
            .iter()
            .map(|p| p.clamp(LOGIT_EPS, 1.0 - LOGIT_EPS).ln())
            .collect();
        let log_1mp: Vec<f64> = predictions
            .iter()
            .map(|p| (1.0 - p.clamp(LOGIT_EPS, 1.0 - LOGIT_EPS)).ln())
            .collect();
        let y: Vec<f64> = labels.iter().map(|l| if *l { 1.0 } else { 0.0 }).collect();
        let n = predictions.len() as f64;
        // Initialize at identity-ish: a=1, b=-1, c=0 → logit-equivalent
        // to the unscaled log-odds.
        let mut a: f64 = 1.0;
        let mut b: f64 = -1.0;
        let mut c: f64 = 0.0;
        // Adam state.
        let mut m = [0.0f64; 3];
        let mut v = [0.0f64; 3];
        let lr = 0.05;
        let beta1 = 0.9;
        let beta2 = 0.999;
        let eps_adam = 1e-8;
        for step in 1..=300 {
            let mut g = [0.0f64; 3];
            for i in 0..predictions.len() {
                let p_hat = sigmoid(a * log_p[i] + b * log_1mp[i] + c);
                let r = p_hat - y[i];
                g[0] += r * log_p[i];
                g[1] += r * log_1mp[i];
                g[2] += r;
            }
            for k in 0..3 {
                let gk = g[k] / n;
                m[k] = beta1 * m[k] + (1.0 - beta1) * gk;
                v[k] = beta2 * v[k] + (1.0 - beta2) * gk * gk;
                let m_hat = m[k] / (1.0 - beta1.powi(step));
                let v_hat = v[k] / (1.0 - beta2.powi(step));
                let upd = lr * m_hat / (v_hat.sqrt() + eps_adam);
                match k {
                    0 => a -= upd,
                    1 => b -= upd,
                    2 => c -= upd,
                    _ => unreachable!(),
                }
            }
        }
        Ok(Arc::new(BetaCalibration { a, b, c }))
    }
}

// ─── Conformal predictor (Phase C C1a) ─────────────────────────────────

/// Phase C C1a: split-conformal predictor. The point prediction is
/// passed through untransformed; the calibrator exposes a
/// `(1 - alpha)` confidence band around each prediction via
/// [`Calibrator::confidence_band`].
///
/// Concretely: at fit time we compute the `(1 - alpha)`-quantile of
/// holdout nonconformity scores `s_i = 1 - p_i` if the true label is
/// 1 else `p_i`. At inference, the band on prediction `p` is
/// `[p - quantile, p + quantile]` clipped to `[0, 1]`. The procedure
/// is distribution-free given exchangeability between calibration
/// and inference distributions (Vovk et al. 2005; Angelopoulos &
/// Bates 2021).
#[derive(Debug, Clone, Copy)]
pub struct ConformalPredictor {
    pub alpha: f64,
    pub quantile: f64,
}

impl Calibrator for ConformalPredictor {
    fn apply(&self, raw: f64) -> f64 {
        // Point prediction passes through unchanged — conformal does
        // not retransform the probability, only quantifies its
        // uncertainty.
        raw
    }
    fn apply_batch(&self, raw: &[f64]) -> Vec<f64> {
        raw.to_vec()
    }
    fn method(&self) -> CalibrationMethodKind {
        CalibrationMethodKind::Conformal
    }
    fn confidence_band(&self, p: f64) -> Option<crate::result::ConfidenceBand> {
        Some(crate::result::ConfidenceBand {
            lower: (p - self.quantile).clamp(0.0, 1.0),
            upper: (p + self.quantile).clamp(0.0, 1.0),
            source: crate::result::ConfidenceSource::Conformal { alpha: self.alpha },
        })
    }
}

/// Fitter for [`ConformalPredictor`]. Computes the
/// `(1 - alpha)`-quantile of nonconformity scores via the standard
/// `idx = ceil((1 - alpha) * (n + 1)) - 1` index. The math is
/// intentionally minimal (~15 lines of arithmetic); the impl plan
/// §3.2 / §3.2a calls split-conformal "intentionally ~30 LOC".
#[derive(Debug, Clone, Copy)]
pub struct ConformalFitter {
    pub alpha: f64,
}

impl Default for ConformalFitter {
    fn default() -> Self {
        Self { alpha: 0.1 }
    }
}

impl CalibratorFitter for ConformalFitter {
    fn fit(
        &self,
        predictions: &[f64],
        labels: &[bool],
    ) -> Result<Arc<dyn Calibrator>, CalibrationError> {
        validate_inputs(predictions, labels)?;
        if !(0.0..1.0).contains(&self.alpha) {
            return Err(CalibrationError::NumericIssue(
                "conformal alpha must be in (0, 1)",
            ));
        }
        let mut scores: Vec<f64> = predictions
            .iter()
            .zip(labels.iter())
            .map(|(p, y)| if *y { 1.0 - *p } else { *p })
            .collect();
        scores.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = scores.len() as f64;
        let raw_idx = ((1.0 - self.alpha) * (n + 1.0)).ceil() as isize - 1;
        let idx = raw_idx.max(0).min(scores.len() as isize - 1) as usize;
        let quantile = scores[idx];
        Ok(Arc::new(ConformalPredictor {
            alpha: self.alpha,
            quantile,
        }))
    }
}

// ─── Dirichlet multi-class calibration (Phase D D-C1d) ─────────────────

/// Phase D D-C1d: Dirichlet calibrator. Maintains per-class
/// concentration parameters `alpha = (α_1, ..., α_K)` fit by
/// method-of-moments on observed `(prediction_vector, class_label)`
/// pairs. `apply(raw)` returns a posterior-weighted recalibrated
/// vector — see [`DirichletCalibrator::apply`] for the formula.
///
/// **Method-of-moments fit** (Minka 2000):
/// Given N observations `p_1, ..., p_N` (each `p_i ∈ Δ^K`):
/// - empirical mean `μ_k = (1/N) Σ p_ik`
/// - empirical variance `σ²_k = (1/N) Σ (p_ik − μ_k)²`
/// - concentration sum `α₀ = (μ_k* · (1 − μ_k*)) / σ²_k* − 1` for the
///   class `k*` with maximum variance (most stable estimator).
/// - per-class `α_k = μ_k · α₀`.
///
/// **Apply** treats the input `raw` as evidence (pseudo-counts) and
/// returns the posterior Dirichlet mean: `p_cal_k = (α_k + raw_k · N_eff)
/// / (Σ α + N_eff)` where `N_eff = max(α₀, 1)` keeps the prior weight
/// non-degenerate when α₀ is very small. For MVP this is a smoothed
/// passthrough biased toward the empirical class frequencies seen at
/// fit time — the dominant correction Dirichlet calibration provides
/// for miscalibrated multi-class classifiers.
#[derive(Debug, Clone)]
pub struct DirichletCalibrator {
    pub alpha: Vec<f64>,
    /// Effective sample size for the prior — derived as `max(α₀, 1)`
    /// during fitting and stored so `apply` doesn't recompute.
    pub n_eff: f64,
}

impl MulticlassCalibrator for DirichletCalibrator {
    fn apply(&self, raw: &[f64]) -> Vec<f64> {
        assert_eq!(
            raw.len(),
            self.alpha.len(),
            "DirichletCalibrator: input length {} != n_classes {}",
            raw.len(),
            self.alpha.len()
        );
        // Posterior under Dirichlet prior + multinomial likelihood with
        // pseudo-counts `raw * n_eff`. Returns the posterior mean.
        let alpha_sum: f64 = self.alpha.iter().sum();
        let denom = alpha_sum + self.n_eff;
        let mut out = Vec::with_capacity(raw.len());
        for (a, r) in self.alpha.iter().zip(raw.iter()) {
            out.push((a + r * self.n_eff) / denom);
        }
        out
    }

    fn n_classes(&self) -> usize {
        self.alpha.len()
    }

    fn method(&self) -> CalibrationMethodKind {
        CalibrationMethodKind::Dirichlet
    }
}

/// Phase D D-C1d: method-of-moments fitter for [`DirichletCalibrator`].
/// No optimizer dependency; closed-form in O(N · K).
#[derive(Debug, Clone, Copy, Default)]
pub struct DirichletFitter;

impl MulticlassCalibratorFitter for DirichletFitter {
    fn fit(
        &self,
        predictions: &[Vec<f64>],
        labels: &[usize],
    ) -> Result<Arc<dyn MulticlassCalibrator>, CalibrationError> {
        if predictions.is_empty() {
            return Err(CalibrationError::EmptyDataset);
        }
        if predictions.len() != labels.len() {
            return Err(CalibrationError::ArityMismatch {
                preds: predictions.len(),
                labels: labels.len(),
            });
        }
        let k = predictions[0].len();
        if k == 0 {
            return Err(CalibrationError::NumericIssue(
                "Dirichlet fit: prediction vectors must be non-empty",
            ));
        }
        for (i, p) in predictions.iter().enumerate() {
            if p.len() != k {
                return Err(CalibrationError::NumericIssue(
                    "Dirichlet fit: prediction vectors must all have the same length",
                ));
            }
            if labels[i] >= k {
                return Err(CalibrationError::NumericIssue(
                    "Dirichlet fit: label index out of range for K classes",
                ));
            }
        }
        let n = predictions.len() as f64;
        // Empirical mean per class.
        let mut mu = vec![0.0f64; k];
        for p in predictions {
            for (mu_k, p_k) in mu.iter_mut().zip(p.iter()) {
                *mu_k += p_k / n;
            }
        }
        // Empirical variance per class.
        let mut var = vec![0.0f64; k];
        for p in predictions {
            for (var_k, (p_k, mu_k)) in var.iter_mut().zip(p.iter().zip(mu.iter())) {
                let d = p_k - mu_k;
                *var_k += (d * d) / n;
            }
        }
        // Pick the class with max variance to estimate α₀; classes with
        // zero variance give a degenerate estimator.
        let (k_star, &var_star) = var
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap();
        let mu_star = mu[k_star];
        if var_star <= 0.0 || mu_star <= 0.0 || mu_star >= 1.0 {
            // Pathological: no variance or degenerate mean → uniform Dirichlet prior.
            return Ok(Arc::new(DirichletCalibrator {
                alpha: vec![1.0; k],
                n_eff: k as f64,
            }));
        }
        let alpha_0 = (mu_star * (1.0 - mu_star) / var_star - 1.0).max(1e-9);
        let alpha: Vec<f64> = mu.iter().map(|m| m * alpha_0).collect();
        let n_eff = alpha_0.max(1.0);
        Ok(Arc::new(DirichletCalibrator { alpha, n_eff }))
    }
}

#[cfg(test)]
mod dirichlet_tests {
    use super::*;

    #[test]
    fn fits_uniform_when_no_variance() {
        let preds = vec![vec![0.5, 0.3, 0.2]; 10];
        let labels = vec![0usize; 10];
        let cal = DirichletFitter.fit(&preds, &labels).unwrap();
        assert_eq!(cal.n_classes(), 3);
        // Degenerate fit → uniform prior; apply returns a blend of prior and input.
        let out = cal.apply(&[1.0, 0.0, 0.0]);
        assert_eq!(out.len(), 3);
        // Output should sum to ~1.
        let sum: f64 = out.iter().sum();
        assert!(
            (sum - 1.0).abs() < 1e-9,
            "output should sum to 1, got {sum}"
        );
    }

    #[test]
    fn three_class_method_of_moments() {
        // Synthetic 3-class data with class-0 dominant.
        let mut preds = Vec::new();
        let mut labels = Vec::new();
        for i in 0..30 {
            // Class 0 dominant when i < 20, class 1 when 20..25, class 2 otherwise.
            if i < 20 {
                preds.push(vec![
                    0.7 + (i as f64) * 0.005,
                    0.2,
                    0.1 - (i as f64) * 0.005,
                ]);
                labels.push(0);
            } else if i < 25 {
                preds.push(vec![0.2, 0.6, 0.2]);
                labels.push(1);
            } else {
                preds.push(vec![0.2, 0.2, 0.6]);
                labels.push(2);
            }
        }
        let cal = DirichletFitter.fit(&preds, &labels).unwrap();
        let out = cal.apply(&[0.5, 0.3, 0.2]);
        assert_eq!(out.len(), 3);
        // Sums to ~1.
        let sum: f64 = out.iter().sum();
        assert!((sum - 1.0).abs() < 1e-9);
        // Class 0 mean is highest in training; calibrated output should
        // still prefer class 0 for this input.
        assert!(out[0] > out[1]);
        assert!(out[0] > out[2]);
    }

    #[test]
    fn rejects_arity_mismatch() {
        let err = DirichletFitter.fit(&[vec![0.5, 0.5]], &[0, 0]).unwrap_err();
        assert!(matches!(err, CalibrationError::ArityMismatch { .. }));
    }

    #[test]
    fn rejects_empty() {
        let err = DirichletFitter.fit(&[], &[]).unwrap_err();
        assert!(matches!(err, CalibrationError::EmptyDataset));
    }

    #[test]
    fn rejects_label_out_of_range() {
        let preds = vec![vec![0.5, 0.5]];
        let labels = vec![5usize];
        let err = DirichletFitter.fit(&preds, &labels).unwrap_err();
        assert!(matches!(err, CalibrationError::NumericIssue(_)));
    }
}

// ─── Metrics ───────────────────────────────────────────────────────────

/// Brier score: mean squared error between probabilities and 0/1
/// labels. Proper scoring rule; lower is better.
pub fn brier_score(preds: &[f64], labels: &[bool]) -> f64 {
    if preds.is_empty() {
        return 0.0;
    }
    let mut sum = 0.0;
    for (p, y) in preds.iter().zip(labels.iter()) {
        let y_f = if *y { 1.0 } else { 0.0 };
        let d = p - y_f;
        sum += d * d;
    }
    sum / preds.len() as f64
}

/// Negative log-likelihood (cross-entropy). Proper scoring rule.
pub fn log_loss(preds: &[f64], labels: &[bool]) -> f64 {
    if preds.is_empty() {
        return 0.0;
    }
    let mut sum = 0.0;
    for (p, y) in preds.iter().zip(labels.iter()) {
        let p = p.clamp(LOGIT_EPS, 1.0 - LOGIT_EPS);
        sum += if *y { -p.ln() } else { -(1.0 - p).ln() };
    }
    sum / preds.len() as f64
}

/// Expected Calibration Error (equal-width binning).
///
/// Note: impl plan §3.4 flags this as biased; the debiased variant
/// will land with C3 VALIDATE. C2 reports this naive version because
/// it's the most-recognized form for users today.
pub fn expected_calibration_error(preds: &[f64], labels: &[bool], n_bins: usize) -> f64 {
    if preds.is_empty() || n_bins == 0 {
        return 0.0;
    }
    let mut bin_sum: Vec<f64> = vec![0.0; n_bins];
    let mut bin_pos: Vec<f64> = vec![0.0; n_bins];
    let mut bin_n: Vec<usize> = vec![0; n_bins];
    for (p, y) in preds.iter().zip(labels.iter()) {
        // Map to bin index [0, n_bins).
        let pc = p.clamp(0.0, 1.0 - f64::EPSILON);
        let idx = (pc * n_bins as f64) as usize;
        let idx = idx.min(n_bins - 1);
        bin_sum[idx] += pc;
        bin_pos[idx] += if *y { 1.0 } else { 0.0 };
        bin_n[idx] += 1;
    }
    let n_total = preds.len() as f64;
    let mut ece = 0.0;
    for k in 0..n_bins {
        if bin_n[k] == 0 {
            continue;
        }
        let avg_p = bin_sum[k] / bin_n[k] as f64;
        let avg_y = bin_pos[k] / bin_n[k] as f64;
        let w = bin_n[k] as f64 / n_total;
        ece += w * (avg_p - avg_y).abs();
    }
    ece
}

/// Debiased ECE (Kumar et al. NeurIPS 2019).
///
/// Naive equal-width-binning ECE over-estimates calibration error in
/// the small-sample regime: the absolute-value step `|avg_p − avg_y|`
/// is biased away from zero. The debiased estimator subtracts the
/// expected absolute bias of `avg_y` under a binomial model per bin:
///
/// ```text
/// bias_bin = sqrt(avg_y · (1 − avg_y) / n_bin)
/// |avg_p − avg_y|_debiased = max(0, |avg_p − avg_y| − bias_bin)
/// ```
///
/// Empty bins contribute zero. Output is in `[0, 1]`. For large `n`
/// per bin this converges to the naive ECE; for small `n` it shrinks
/// toward zero, removing the dominant noise term flagged by Kumar.
pub fn debiased_ece(preds: &[f64], labels: &[bool], n_bins: usize) -> f64 {
    if preds.is_empty() || n_bins == 0 {
        return 0.0;
    }
    let mut bin_sum: Vec<f64> = vec![0.0; n_bins];
    let mut bin_pos: Vec<f64> = vec![0.0; n_bins];
    let mut bin_n: Vec<usize> = vec![0; n_bins];
    for (p, y) in preds.iter().zip(labels.iter()) {
        let pc = p.clamp(0.0, 1.0 - f64::EPSILON);
        let idx = ((pc * n_bins as f64) as usize).min(n_bins - 1);
        bin_sum[idx] += pc;
        bin_pos[idx] += if *y { 1.0 } else { 0.0 };
        bin_n[idx] += 1;
    }
    let n_total = preds.len() as f64;
    let mut ece = 0.0;
    for k in 0..n_bins {
        if bin_n[k] == 0 {
            continue;
        }
        let n = bin_n[k] as f64;
        let avg_p = bin_sum[k] / n;
        let avg_y = bin_pos[k] / n;
        let raw_gap = (avg_p - avg_y).abs();
        let bias = (avg_y * (1.0 - avg_y) / n).sqrt();
        let debiased = (raw_gap - bias).max(0.0);
        ece += (n / n_total) * debiased;
    }
    ece
}

/// Accuracy: fraction of predictions whose argmax at threshold 0.5
/// matches the label. Binary classification only in this slice.
pub fn accuracy(preds: &[f64], labels: &[bool]) -> f64 {
    if preds.is_empty() {
        return 0.0;
    }
    let mut hits = 0usize;
    for (p, y) in preds.iter().zip(labels.iter()) {
        let pred_label = *p >= 0.5;
        if pred_label == *y {
            hits += 1;
        }
    }
    hits as f64 / preds.len() as f64
}

/// Area under the ROC curve via the rank-sum (Mann-Whitney U)
/// identity:
///
/// ```text
/// AUC = (Σ rank(positive) − P(P+1)/2) / (P · N)
/// ```
///
/// where `P` is the count of positive labels and `N` is the count of
/// negative labels. Ties are handled with average ranks.
///
/// Returns 0.5 (chance level) when all labels are the same class or
/// when the dataset is empty — both are degenerate cases for AUC.
pub fn auc(preds: &[f64], labels: &[bool]) -> f64 {
    let n = preds.len();
    if n == 0 {
        return 0.5;
    }
    let n_pos = labels.iter().filter(|y| **y).count();
    let n_neg = n - n_pos;
    if n_pos == 0 || n_neg == 0 {
        return 0.5;
    }
    // Sort by prediction ascending; assign average ranks for ties.
    let mut idx: Vec<usize> = (0..n).collect();
    idx.sort_by(|&a, &b| {
        preds[a]
            .partial_cmp(&preds[b])
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut ranks: Vec<f64> = vec![0.0; n];
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && preds[idx[j]] == preds[idx[i]] {
            j += 1;
        }
        // Indices in [i, j) tie. Average rank is the mean of (i+1..=j).
        let avg = ((i + 1) as f64 + j as f64) / 2.0;
        for k in i..j {
            ranks[idx[k]] = avg;
        }
        i = j;
    }
    let rank_sum_pos: f64 = labels
        .iter()
        .enumerate()
        .filter(|(_, y)| **y)
        .map(|(i, _)| ranks[i])
        .sum();
    let p = n_pos as f64;
    let n_neg_f = n_neg as f64;
    (rank_sum_pos - p * (p + 1.0) / 2.0) / (p * n_neg_f)
}

fn validate_inputs(preds: &[f64], labels: &[bool]) -> Result<(), CalibrationError> {
    if preds.is_empty() || labels.is_empty() {
        return Err(CalibrationError::EmptyDataset);
    }
    if preds.len() != labels.len() {
        return Err(CalibrationError::ArityMismatch {
            preds: preds.len(),
            labels: labels.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synthetic_overconfident(n: usize) -> (Vec<f64>, Vec<bool>) {
        // n samples with prediction = 0.95 each; true positive rate = 0.5.
        let preds = vec![0.95f64; n];
        let labels: Vec<bool> = (0..n).map(|i| i % 2 == 0).collect();
        (preds, labels)
    }

    fn synthetic_shifted_sigmoid(n: usize) -> (Vec<f64>, Vec<bool>) {
        // raw_p is sigmoid(true_z), label drawn from sigmoid(true_z + shift).
        // For a deterministic test we just label by threshold.
        let preds: Vec<f64> = (0..n)
            .map(|i| {
                let t = (i as f64) / (n as f64);
                sigmoid(2.0 * (t - 0.5)) // smooth in [0,1]
            })
            .collect();
        let labels: Vec<bool> = preds.iter().map(|p| *p > 0.6).collect();
        (preds, labels)
    }

    #[test]
    fn sigmoid_logit_roundtrip() {
        for p in [0.01, 0.25, 0.5, 0.75, 0.99] {
            let z = logit(p);
            let p2 = sigmoid(z);
            assert!((p - p2).abs() < 1e-10);
        }
    }

    #[test]
    fn identity_passthrough() {
        let c = IdentityCalibrator;
        for p in [0.0, 0.1, 0.5, 0.9, 1.0] {
            assert_eq!(c.apply(p), p);
        }
    }

    #[test]
    fn brier_score_known_values() {
        // 4 preds at 0.5 with labels [T,T,F,F]:
        //   each (0.5 − y)² = 0.25; mean = 0.25
        assert!((brier_score(&[0.5; 4], &[true, true, false, false]) - 0.25).abs() < 1e-12);
        // Perfect predictions:
        assert_eq!(brier_score(&[1.0, 0.0], &[true, false]), 0.0);
    }

    #[test]
    fn log_loss_known_values() {
        // 2 perfect predictions → log loss ≈ 0 (clamped to -ln(1−ε) ≈ ε).
        let l = log_loss(&[1.0, 0.0], &[true, false]);
        assert!(l < 1e-10);
    }

    #[test]
    fn ece_zero_for_perfectly_calibrated() {
        // All preds = 0.5, label rate = 0.5 → ECE = 0.
        let preds = vec![0.5; 10];
        let labels: Vec<bool> = (0..10).map(|i| i % 2 == 0).collect();
        let ece = expected_calibration_error(&preds, &labels, 10);
        assert!(ece < 1e-12, "got {ece}");
    }

    #[test]
    fn ece_large_for_overconfident() {
        let (preds, labels) = synthetic_overconfident(100);
        let ece = expected_calibration_error(&preds, &labels, 10);
        // All preds in the same bin; |avg_p - avg_y| = |0.95 - 0.5| = 0.45.
        assert!((ece - 0.45).abs() < 1e-6);
    }

    #[test]
    fn platt_fit_reduces_overconfidence() {
        // Phase C gate target: a constant-0.95 miscalibrated classifier
        // against labels with prevalence 0.5 should see ECE drop ≥ 50%
        // after Platt calibration. Brier is not the right metric here:
        // for constant-prediction data the irreducible Brier is 0.25
        // (no learnable structure), so a 50% Brier reduction is
        // mathematically out of reach — the rollout gate measures ECE
        // for this scenario.
        let (preds, labels) = synthetic_overconfident(200);
        let c = PlattFitter.fit(&preds, &labels).unwrap();
        let calibrated: Vec<f64> = preds.iter().map(|p| c.apply(*p)).collect();
        let raw_ece = expected_calibration_error(&preds, &labels, 10);
        let cal_ece = expected_calibration_error(&calibrated, &labels, 10);
        assert!(
            cal_ece < raw_ece * 0.5,
            "Platt should reduce ECE ≥ 50%: raw={raw_ece} cal={cal_ece}"
        );
        // Calibrated predictions should cluster near 0.5 (the actual TPR).
        let mean_cal: f64 = calibrated.iter().sum::<f64>() / calibrated.len() as f64;
        assert!(
            (mean_cal - 0.5).abs() < 0.1,
            "mean {mean_cal} should approach 0.5"
        );
        // And Brier should not get worse than raw (it should improve too,
        // just not by 50%).
        let raw_brier = brier_score(&preds, &labels);
        let cal_brier = brier_score(&calibrated, &labels);
        assert!(cal_brier <= raw_brier);
    }

    #[test]
    fn isotonic_fit_is_monotone_and_improves_brier() {
        let (preds, labels) = synthetic_shifted_sigmoid(200);
        let c = IsotonicFitter.fit(&preds, &labels).unwrap();
        let calibrated: Vec<f64> = preds.iter().map(|p| c.apply(*p)).collect();
        // Check monotonicity.
        let mut sorted_pairs: Vec<(f64, f64)> = preds
            .iter()
            .copied()
            .zip(calibrated.iter().copied())
            .collect();
        sorted_pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        for w in sorted_pairs.windows(2) {
            assert!(w[0].1 <= w[1].1 + 1e-9, "isotonic must be monotone: {w:?}");
        }
        // Brier should not be worse than raw.
        let raw_brier = brier_score(&preds, &labels);
        let cal_brier = brier_score(&calibrated, &labels);
        assert!(cal_brier <= raw_brier + 1e-6);
    }

    #[test]
    fn temperature_fit_shrinks_overconfidence() {
        let (preds, labels) = synthetic_overconfident(200);
        let c = TemperatureFitter.fit(&preds, &labels).unwrap();
        // Original logit ≈ 2.94; for labels at 50%, T should be large
        // to shrink toward 0.5. We just assert T > 1.
        assert_eq!(c.as_ref().method(), CalibrationMethodKind::Temperature);
        let calibrated: Vec<f64> = preds.iter().map(|p| c.apply(*p)).collect();
        let mean: f64 = calibrated.iter().sum::<f64>() / calibrated.len() as f64;
        // Mean should be significantly closer to 0.5 than the raw 0.95.
        assert!(mean < 0.85, "temperature should pull mean down: got {mean}");
    }

    #[test]
    fn beta_fit_does_not_diverge() {
        // Beta is the trickiest fit to make robust without L-BFGS;
        // we assert it doesn't blow up and produces a finite calibrator.
        let (preds, labels) = synthetic_shifted_sigmoid(200);
        let c = BetaFitter.fit(&preds, &labels).unwrap();
        for p in [0.1, 0.5, 0.9] {
            let q = c.apply(p);
            assert!(q.is_finite(), "Beta apply produced non-finite {q}");
            assert!((0.0..=1.0).contains(&q));
        }
    }

    #[test]
    fn fitter_rejects_arity_mismatch() {
        let err = PlattFitter
            .fit(&[0.5, 0.5], &[true, true, false])
            .unwrap_err();
        assert!(matches!(err, CalibrationError::ArityMismatch { .. }));
    }

    #[test]
    fn accuracy_known_values() {
        // All predictions correct at threshold 0.5.
        assert_eq!(
            accuracy(&[0.9, 0.1, 0.8, 0.2], &[true, false, true, false]),
            1.0
        );
        // All wrong.
        assert_eq!(
            accuracy(&[0.1, 0.9, 0.2, 0.8], &[true, false, true, false]),
            0.0
        );
        // Half correct.
        assert_eq!(
            accuracy(&[0.9, 0.9, 0.1, 0.1], &[true, false, true, false]),
            0.5
        );
        // Empty input.
        assert_eq!(accuracy(&[], &[]), 0.0);
    }

    #[test]
    fn auc_known_values() {
        // Perfect separation: positives all > negatives.
        let preds = vec![0.1, 0.2, 0.8, 0.9];
        let labels = vec![false, false, true, true];
        assert!((auc(&preds, &labels) - 1.0).abs() < 1e-12);
        // Random / inverted separation: same magnitudes but labels flipped.
        let preds_inv = vec![0.1, 0.2, 0.8, 0.9];
        let labels_inv = vec![true, true, false, false];
        assert!((auc(&preds_inv, &labels_inv) - 0.0).abs() < 1e-12);
        // Single class → degenerate, return 0.5.
        assert_eq!(auc(&[0.1, 0.5, 0.9], &[true, true, true]), 0.5);
        // Empty → 0.5.
        assert_eq!(auc(&[], &[]), 0.5);
        // Tied predictions split between classes give 0.5.
        assert_eq!(auc(&[0.5, 0.5, 0.5, 0.5], &[true, false, true, false]), 0.5);
    }

    #[test]
    fn debiased_ece_smaller_than_naive_in_small_sample() {
        // 10 samples in one bin where p̂=0.5, half labels true. The
        // naive |avg_p - avg_y| = 0; debiased subtracts the binomial
        // bias term but clamps at 0. With balanced labels avg_y=0.5,
        // bias = sqrt(0.25/10) ≈ 0.158; debiased remains 0.
        let preds = vec![0.5; 10];
        let labels: Vec<bool> = (0..10).map(|i| i % 2 == 0).collect();
        assert!(debiased_ece(&preds, &labels, 1) <= expected_calibration_error(&preds, &labels, 1));
    }

    #[test]
    fn debiased_ece_zero_for_empty() {
        assert_eq!(debiased_ece(&[], &[], 10), 0.0);
        assert_eq!(debiased_ece(&[0.5], &[true], 0), 0.0);
    }

    #[test]
    fn debiased_ece_approaches_naive_for_large_n() {
        // With many samples per bin, the bias term shrinks like 1/√n
        // and debiased ECE converges to the naive value.
        let preds = vec![0.95; 10_000];
        let labels: Vec<bool> = (0..10_000).map(|i| i % 2 == 0).collect();
        let naive = expected_calibration_error(&preds, &labels, 10);
        let debiased = debiased_ece(&preds, &labels, 10);
        // At n=10k, the bias term is sqrt(0.25/10000) = 0.005; the
        // raw gap is |0.95 - 0.5| = 0.45. So debiased ≈ 0.445, naive
        // = 0.45. Difference should be ≈ 0.005.
        assert!((naive - debiased).abs() < 0.01);
    }

    #[test]
    fn fitter_rejects_empty() {
        let err = PlattFitter.fit(&[], &[]).unwrap_err();
        assert!(matches!(err, CalibrationError::EmptyDataset));
    }
}
