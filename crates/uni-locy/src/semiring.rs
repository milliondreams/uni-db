// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Probability semiring abstraction for Locy aggregation.
//!
//! The [`LocySemiring`] trait lifts MNOR (noisy-OR) and MPROD (product) off
//! the hard-coded `match FoldAggKind { Nor, Prod }` arms in the runtime and
//! onto a typed abstraction so future semirings (Scallop-style `TopKProofs`,
//! gradient lifts) can drop in without re-shaping the planner.
//!
//! ### Scope
//!
//! The trait is **row-at-a-time**: it composes per-tuple tags via
//! [`plus`](LocySemiring::plus) (disjunction) and [`times`](LocySemiring::times)
//! (conjunction). This covers `AddMultProb` (independent noisy-OR / product)
//! and `MaxMinProb` (Viterbi). It deliberately does **not** cover
//! [`SemiringKind::BddExact`], which operates over a whole aggregation group's
//! lineage at once via weighted model counting (see
//! `crates/uni-query/src/query/df_graph/locy_bdd.rs`). `BddExact` is
//! dispatched at the fixpoint level outside this trait; C0 will absorb it
//! once tag-DNFs land.
//!
//! See `/home/rohit/work/dragonscale/uni-locy-docs/DEEP_LOCY_IMPLEMENTATION_PLAN.md`
//! §1.6 (decision D-7) for the design rationale.

use crate::types::SemiringKind;

/// Domain / unsupported-operation error from a semiring.
///
/// Callers map this into their own error type — the semiring layer is
/// deliberately decoupled from DataFusion's error type so `uni-locy` can
/// remain free of a query-engine dependency.
#[derive(Debug, Clone, PartialEq)]
pub enum SemiringError {
    /// A probability input fell outside `[0, 1]` and `strict_probability_domain`
    /// was set. `op` is `"MNOR"` or `"MPROD"`.
    DomainViolation { value: f64, op: &'static str },
    /// Operation not supported by this semiring (e.g., `negate` on a
    /// non-Boolean-tagged semiring once C0 lands).
    NotSupported {
        op: &'static str,
        kind: SemiringKind,
    },
}

impl std::fmt::Display for SemiringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DomainViolation { value, op } => write!(
                f,
                "strict_probability_domain: {op} input {value} is outside [0, 1]"
            ),
            Self::NotSupported { op, kind } => {
                write!(f, "semiring {kind:?} does not support {op}")
            }
        }
    }
}

impl std::error::Error for SemiringError {}

/// Domain-check a raw scalar before feeding it to `plus` / `times`.
///
/// Returns the value unchanged when it lies in `[0, 1]`. Outside that
/// range it returns [`SemiringError::DomainViolation`] when `strict` is
/// set, otherwise clamps and emits the pre-refactor tracing literal
/// (`"<op> input <raw> outside [0,1], clamped to <clamped>"`) so any
/// string-asserting tests remain stable. Shared by every `f64`-tagged
/// [`LocySemiring::validate_domain`] impl.
pub fn validate_probability_domain(
    raw: f64,
    op: &'static str,
    strict: bool,
) -> Result<f64, SemiringError> {
    if (0.0..=1.0).contains(&raw) {
        return Ok(raw);
    }
    if strict {
        return Err(SemiringError::DomainViolation { value: raw, op });
    }
    let clamped = raw.clamp(0.0, 1.0);
    tracing::warn!("{op} input {raw} outside [0,1], clamped to {clamped}");
    Ok(clamped)
}

/// Consolidated semiring configuration threaded through planner and
/// executors. Constructed by [`crate::LocyConfig::resolve`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedSemiringConfig {
    pub kind: SemiringKind,
    pub strict_probability_domain: bool,
    pub probability_epsilon: f64,
    pub max_bdd_variables: usize,
}

impl ResolvedSemiringConfig {
    /// Returns true when the active semiring is the default `AddMultProb`
    /// path. Phase-3 shared-proof detection and the AddMultProb-specific
    /// complement code paths gate on this.
    pub fn is_add_mult_prob(&self) -> bool {
        matches!(self.kind, SemiringKind::AddMultProb)
    }
}

/// Row-at-a-time probability semiring.
///
/// Implementors carry their own state (e.g., underflow epsilon for
/// `AddMultProb`'s log-space switch). All operations are pure and
/// reentrant so the same semiring instance is safely shared across the
/// fixpoint loop.
pub trait LocySemiring: Send + Sync + 'static {
    /// The per-tuple tag type. For the two Phase-A semirings this is
    /// `f64`; C0's `TopKProofs` will carry a proof-DNF tag.
    type Tag: Clone + Send + Sync;

    fn kind(&self) -> SemiringKind;

    /// Whether this semiring composes pointwise via [`plus`](Self::plus) and
    /// [`times`](Self::times). Returns `true` for `AddMultProb` and
    /// `MaxMinProb`; `false` for whole-group semirings such as `BddExact`
    /// which are dispatched outside this trait.
    fn is_row_at_a_time(&self) -> bool {
        true
    }

    /// Identity for [`plus`](Self::plus) — `0.0` for both Phase-A semirings.
    fn zero_disjunction(&self) -> Self::Tag;

    /// Identity for [`times`](Self::times) — `1.0` for both Phase-A semirings.
    fn one_conjunction(&self) -> Self::Tag;

    /// Disjunction. MNOR / proof-OR.
    fn plus(&self, a: &Self::Tag, b: &Self::Tag) -> Self::Tag;

    /// Conjunction. MPROD / proof-AND.
    fn times(&self, a: &Self::Tag, b: &Self::Tag) -> Self::Tag;

    /// Complement (`1 - p` conventionally). May return
    /// [`SemiringError::NotSupported`] for semirings whose tags do not
    /// admit a complement.
    fn negate(&self, a: &Self::Tag) -> Result<Self::Tag, SemiringError>;

    /// Collapse a tag to a probability in `[0, 1]`.
    fn weight(&self, a: &Self::Tag) -> f64;

    /// Domain-check a raw scalar before feeding it to `plus` / `times`.
    /// Returns the clamped value, or `DomainViolation` when `strict` is
    /// set and `raw` falls outside `[0, 1]`. Emits the same tracing literal
    /// (`"MNOR input ..."` / `"MPROD input ..."`) used in the pre-refactor
    /// code so any string-asserting tests remain stable.
    fn validate_domain(
        &self,
        raw: f64,
        op: &'static str,
        strict: bool,
    ) -> Result<f64, SemiringError>;
}

// ---------------------------------------------------------------------------
// AddMultProb — Phase 1/2 default. Independence-assumed noisy-OR and product.
// ---------------------------------------------------------------------------

/// `(plus = noisy-OR, times = product, negate = 1 - p)`.
///
/// Stateful: carries `probability_epsilon`, the threshold below which
/// `times` switches into log-space accumulation to avoid floating-point
/// underflow (spec §5.3). This keeps the underflow guard inside the
/// semiring rather than scattering it across executors.
#[derive(Debug, Clone, Copy)]
pub struct AddMultProb {
    pub probability_epsilon: f64,
}

impl AddMultProb {
    pub fn new(probability_epsilon: f64) -> Self {
        Self {
            probability_epsilon,
        }
    }
}

impl Default for AddMultProb {
    fn default() -> Self {
        Self {
            probability_epsilon: 1e-15,
        }
    }
}

impl LocySemiring for AddMultProb {
    type Tag = f64;

    fn kind(&self) -> SemiringKind {
        SemiringKind::AddMultProb
    }

    fn zero_disjunction(&self) -> f64 {
        0.0
    }

    fn one_conjunction(&self) -> f64 {
        1.0
    }

    fn plus(&self, a: &f64, b: &f64) -> f64 {
        1.0 - (1.0 - *a) * (1.0 - *b)
    }

    fn times(&self, a: &f64, b: &f64) -> f64 {
        // Log-space switch when the running product drops below epsilon.
        if *a < self.probability_epsilon || *b < self.probability_epsilon {
            let la = a.max(self.probability_epsilon).ln();
            let lb = b.max(self.probability_epsilon).ln();
            (la + lb).exp()
        } else {
            *a * *b
        }
    }

    fn negate(&self, a: &f64) -> Result<f64, SemiringError> {
        Ok(1.0 - *a)
    }

    fn weight(&self, a: &f64) -> f64 {
        *a
    }

    fn validate_domain(
        &self,
        raw: f64,
        op: &'static str,
        strict: bool,
    ) -> Result<f64, SemiringError> {
        validate_probability_domain(raw, op, strict)
    }
}

// ---------------------------------------------------------------------------
// MaxMinProb — Viterbi / fuzzy. Opt-in only; triggers FuzzyNotProbabilistic.
// ---------------------------------------------------------------------------

/// `(plus = max, times = min, negate = 1 - p)`.
///
/// This is **fuzzy logic**, not probability. Any PROB-bearing rule
/// evaluated under this semiring produces a non-suppressible
/// `RuntimeWarningCode::FuzzyNotProbabilistic` (rollout decision D-9).
#[derive(Debug, Clone, Copy, Default)]
pub struct MaxMinProb;

impl LocySemiring for MaxMinProb {
    type Tag = f64;

    fn kind(&self) -> SemiringKind {
        SemiringKind::MaxMinProb
    }

    fn zero_disjunction(&self) -> f64 {
        0.0
    }

    fn one_conjunction(&self) -> f64 {
        1.0
    }

    fn plus(&self, a: &f64, b: &f64) -> f64 {
        a.max(*b)
    }

    fn times(&self, a: &f64, b: &f64) -> f64 {
        a.min(*b)
    }

    fn negate(&self, a: &f64) -> Result<f64, SemiringError> {
        Ok(1.0 - *a)
    }

    fn weight(&self, a: &f64) -> f64 {
        *a
    }

    fn validate_domain(
        &self,
        raw: f64,
        op: &'static str,
        strict: bool,
    ) -> Result<f64, SemiringError> {
        validate_probability_domain(raw, op, strict)
    }
}

// ---------------------------------------------------------------------------
// SemiringDispatch — runtime-selectable concrete type for executors.
// ---------------------------------------------------------------------------

/// Concrete enum dispatching to the active row-at-a-time semiring.
/// Used by `MonotonicAggState` and `FoldExec` instead of a
/// `Box<dyn LocySemiring>` so that the per-row `plus`/`times` calls stay
/// inlineable (the match is a small branch; LLVM specializes through).
///
/// * `SemiringKind::BddExact` maps to `AddMultProb` at the row level —
///   the BDD post-correction runs over the same independence-mode
///   accumulators (see `weighted_model_count` in `locy_bdd.rs`).
/// * `SemiringKind::TopKProofs { k }` likewise dispatches to
///   `AddMultProb` at the row level in **Stage 1** (this Phase C C0
///   slice): the library-layer `TopKProofs<K>` impl in
///   `crate::top_k_proofs` carries true tag math, but the runtime
///   hot-path operates on `f64`. Stage 2 wires `TopKTag` flow through
///   `MonotonicAggState` / `FoldExec` / record-batch encoding.
#[derive(Debug, Clone, Copy)]
pub enum SemiringDispatch {
    AddMultProb(AddMultProb),
    MaxMinProb(MaxMinProb),
    TopKProofs { inner: AddMultProb, k: u32 },
}

impl SemiringDispatch {
    /// Build a dispatch from the resolved kind and the underflow epsilon.
    /// `BddExact` collapses to `AddMultProb` row math (post-correction
    /// runs separately at the fixpoint level); `TopKProofs` likewise
    /// in Stage 1.
    pub fn new(kind: SemiringKind, probability_epsilon: f64) -> Self {
        match kind {
            SemiringKind::AddMultProb | SemiringKind::BddExact => {
                Self::AddMultProb(AddMultProb::new(probability_epsilon))
            }
            SemiringKind::MaxMinProb => Self::MaxMinProb(MaxMinProb),
            SemiringKind::TopKProofs { k } => {
                // Stage 1 of Phase C C0: runtime tag flow is pending,
                // so this dispatches to AddMultProb row math. Library
                // users wanting the true `TopKTag` math should call
                // `crate::top_k_proofs::TopKProofs::<K>` directly.
                //
                // The warn fires per executor creation rather than per
                // row, so cost is negligible; the message helps users
                // understand why their `TopKProofs` results match
                // `AddMultProb` byte-for-byte until Stage 2.
                tracing::warn!(
                    "TopKProofs(k={k}) runtime tag flow pending Stage 2 — \
                     falling back to AddMultProb row math; library-layer \
                     TopKProofs<K> math is available via uni_locy::top_k_proofs"
                );
                Self::TopKProofs {
                    inner: AddMultProb::new(probability_epsilon),
                    k,
                }
            }
        }
    }

    pub fn kind(&self) -> SemiringKind {
        match self {
            Self::AddMultProb(sr) => sr.kind(),
            Self::MaxMinProb(sr) => sr.kind(),
            // Phase C C0 Stage 1 dispatch reports the underlying row
            // math kind so existing AddMultProb-gated runtime paths
            // (Phase-3 detector, BDD correction site) continue to fire
            // correctly. Callers needing the original kind read it
            // from `ResolvedSemiringConfig.kind` instead.
            Self::TopKProofs { inner, .. } => inner.kind(),
        }
    }

    /// Returns the `k` parameter when the dispatch is `TopKProofs`.
    /// Stage 2 callers use this to find the K at the row-eval site
    /// where they materialize tags. `None` for other semirings.
    pub fn top_k(&self) -> Option<u32> {
        match self {
            Self::TopKProofs { k, .. } => Some(*k),
            _ => None,
        }
    }

    pub fn plus(&self, a: f64, b: f64) -> f64 {
        match self {
            Self::AddMultProb(sr) => sr.plus(&a, &b),
            Self::MaxMinProb(sr) => sr.plus(&a, &b),
            Self::TopKProofs { inner, .. } => inner.plus(&a, &b),
        }
    }

    pub fn times(&self, a: f64, b: f64) -> f64 {
        match self {
            Self::AddMultProb(sr) => sr.times(&a, &b),
            Self::MaxMinProb(sr) => sr.times(&a, &b),
            Self::TopKProofs { inner, .. } => inner.times(&a, &b),
        }
    }

    pub fn validate_domain(
        &self,
        raw: f64,
        op: &'static str,
        strict: bool,
    ) -> Result<f64, SemiringError> {
        match self {
            Self::AddMultProb(sr) => sr.validate_domain(raw, op, strict),
            Self::MaxMinProb(sr) => sr.validate_domain(raw, op, strict),
            Self::TopKProofs { inner, .. } => inner.validate_domain(raw, op, strict),
        }
    }

    // -----------------------------------------------------------------
    // Stage 2 tag-flow surface (pending wiring).
    //
    // The methods below (`plus_tag` / `times_tag` / `zero_tag` /
    // `singleton_tag` / `weight_of`) and the [`AggregatorValue`] enum
    // are the typed-tag interface that Stage 2 will plumb through
    // `MonotonicAggState` / `FoldExec` / record-batch encoding so the
    // `TopKProofs` runtime path stops falling back to `AddMultProb` row
    // math (see `SemiringDispatch::new`). They have no non-test callers
    // yet — kept here, compiled and unit-tested, so the Stage 2 wiring
    // lands against an already-validated surface rather than re-deriving
    // it. Do not delete pending Stage 2.
    // -----------------------------------------------------------------

    /// Phase C C0 Stage 2: tag-level `plus` that supports both f64
    /// semirings (AddMultProb / MaxMinProb) and the proof-tag
    /// semiring (TopKProofs). Returns the merged value plus an
    /// optional `PruneNotice` callers use to drive
    /// `RuntimeWarningCode::TopKPruningCrossedDependency` emission.
    /// Existing `plus(a: f64, b: f64) -> f64` stays unchanged for
    /// hot paths that don't carry proof tags.
    pub fn plus_tag(
        &self,
        a: &AggregatorValue,
        b: &AggregatorValue,
    ) -> (AggregatorValue, Option<crate::top_k_proofs::PruneNotice>) {
        match (self, a, b) {
            (Self::AddMultProb(sr), AggregatorValue::F64(x), AggregatorValue::F64(y)) => {
                (AggregatorValue::F64(sr.plus(x, y)), None)
            }
            (Self::MaxMinProb(sr), AggregatorValue::F64(x), AggregatorValue::F64(y)) => {
                (AggregatorValue::F64(sr.plus(x, y)), None)
            }
            (Self::TopKProofs { k, .. }, AggregatorValue::TopK(ta), AggregatorValue::TopK(tb)) => {
                let (proofs, notice) = merge_top_k_dispatch(ta, tb, *k as usize);
                (
                    AggregatorValue::TopK(crate::top_k_proofs::TopKTag { proofs }),
                    Some(notice),
                )
            }
            // Type mismatch between dispatch arm and value variant —
            // indicates a callsite bug (constructed the wrong
            // AggregatorValue for the active semiring).
            _ => unreachable!(
                "SemiringDispatch::plus_tag: type mismatch — dispatch {:?} vs ({:?}, {:?})",
                self.kind(),
                std::mem::discriminant(a),
                std::mem::discriminant(b),
            ),
        }
    }

    /// Phase C C0 Stage 2: tag-level `times`. Same contract as
    /// `plus_tag`.
    pub fn times_tag(
        &self,
        a: &AggregatorValue,
        b: &AggregatorValue,
    ) -> (AggregatorValue, Option<crate::top_k_proofs::PruneNotice>) {
        match (self, a, b) {
            (Self::AddMultProb(sr), AggregatorValue::F64(x), AggregatorValue::F64(y)) => {
                (AggregatorValue::F64(sr.times(x, y)), None)
            }
            (Self::MaxMinProb(sr), AggregatorValue::F64(x), AggregatorValue::F64(y)) => {
                (AggregatorValue::F64(sr.times(x, y)), None)
            }
            (Self::TopKProofs { k, .. }, AggregatorValue::TopK(ta), AggregatorValue::TopK(tb)) => {
                // Cartesian product per the library impl; reuse
                // merge_top_k for dedup + pruning.
                if ta.proofs.is_empty() || tb.proofs.is_empty() {
                    return (
                        AggregatorValue::TopK(crate::top_k_proofs::TopKTag::zero()),
                        None,
                    );
                }
                let mut cart: Vec<crate::top_k_proofs::Proof> =
                    Vec::with_capacity(ta.proofs.len() * tb.proofs.len());
                for pa in &ta.proofs {
                    for pb in &tb.proofs {
                        let mut nc = pa.neural_calls.clone();
                        let existing: std::collections::HashSet<u32> =
                            pa.neural_calls.iter().map(|c| c.0).collect();
                        for c in &pb.neural_calls {
                            if !existing.contains(&c.0) {
                                nc.push(*c);
                            }
                        }
                        cart.push(crate::top_k_proofs::Proof {
                            weight: pa.weight * pb.weight,
                            base_rvs: crate::dependency_dnf::BaseRvSet::union(
                                &pa.base_rvs,
                                &pb.base_rvs,
                            ),
                            neural_calls: nc,
                        });
                    }
                }
                let (proofs, notice) = merge_top_k_dispatch_owned(Vec::new(), cart, *k as usize);
                (
                    AggregatorValue::TopK(crate::top_k_proofs::TopKTag { proofs }),
                    Some(notice),
                )
            }
            _ => unreachable!(
                "SemiringDispatch::times_tag: type mismatch — dispatch {:?} vs ({:?}, {:?})",
                self.kind(),
                std::mem::discriminant(a),
                std::mem::discriminant(b),
            ),
        }
    }

    /// Phase C C0 Stage 2: return the additive-identity value for
    /// the active semiring. Used by `MonotonicAggState` to
    /// initialize new accumulator slots.
    pub fn zero_tag(&self) -> AggregatorValue {
        match self {
            Self::AddMultProb(_) | Self::MaxMinProb(_) => AggregatorValue::F64(0.0),
            Self::TopKProofs { .. } => AggregatorValue::TopK(crate::top_k_proofs::TopKTag::zero()),
        }
    }

    /// Phase C C0 Stage 2: lift a row's f64 weight into the
    /// dispatch's tag type. For `AddMultProb` / `MaxMinProb` this is
    /// just `F64(w)`; for `TopKProofs` the caller supplies the
    /// row's `base_rvs` and `neural_calls` so a single-Proof tag is
    /// materialized.
    pub fn singleton_tag(
        &self,
        weight: f64,
        base_rvs: crate::dependency_dnf::BaseRvSet,
        neural_calls: Vec<crate::top_k_proofs::NeuralCallId>,
    ) -> AggregatorValue {
        match self {
            Self::AddMultProb(_) | Self::MaxMinProb(_) => AggregatorValue::F64(weight),
            Self::TopKProofs { .. } => AggregatorValue::TopK(crate::top_k_proofs::TopKTag {
                proofs: vec![crate::top_k_proofs::Proof {
                    weight,
                    base_rvs,
                    neural_calls,
                }],
            }),
        }
    }

    /// Phase C C0 Stage 2: collapse an aggregator value to its
    /// scalar probability for downstream f64-typed consumers
    /// (record-batch encoding, BDD post-correction site, etc.).
    pub fn weight_of(&self, value: &AggregatorValue) -> f64 {
        match (self, value) {
            (Self::AddMultProb(_) | Self::MaxMinProb(_), AggregatorValue::F64(v)) => *v,
            (Self::TopKProofs { .. }, AggregatorValue::TopK(t)) => {
                // Conservative weight per library impl: noisy-OR
                // over proof weights under independence-mode.
                let mut complement = 1.0;
                for p in &t.proofs {
                    complement *= 1.0 - p.weight;
                }
                (1.0 - complement).clamp(0.0, 1.0)
            }
            _ => unreachable!(
                "SemiringDispatch::weight_of: type mismatch — dispatch {:?} vs {:?}",
                self.kind(),
                std::mem::discriminant(value),
            ),
        }
    }
}

/// Phase C C0 Stage 2: tag-typed accumulator value used by
/// `MonotonicAggState`. The variant must match the active
/// `SemiringDispatch` — `F64` for `AddMultProb` / `MaxMinProb`,
/// `TopK` for `TopKProofs`. Cross-type pairs panic in
/// `plus_tag` / `times_tag` (callsite bug).
#[derive(Debug, Clone)]
pub enum AggregatorValue {
    F64(f64),
    TopK(crate::top_k_proofs::TopKTag),
}

impl AggregatorValue {
    /// Convenience constructor for f64 callers that want to
    /// initialize an accumulator from a row weight under
    /// `SemiringDispatch::AddMultProb` / `MaxMinProb`.
    pub fn f64(v: f64) -> Self {
        AggregatorValue::F64(v)
    }
}

/// Helper that delegates to [`crate::top_k_proofs::merge_top_k_with`]
/// over cloned proof lists. The library impl is generic over `K`
/// (compile-time const); the runtime needs a value-level `k`.
fn merge_top_k_dispatch(
    a: &crate::top_k_proofs::TopKTag,
    b: &crate::top_k_proofs::TopKTag,
    k: usize,
) -> (
    Vec<crate::top_k_proofs::Proof>,
    crate::top_k_proofs::PruneNotice,
) {
    merge_top_k_dispatch_owned(a.proofs.clone(), b.proofs.clone(), k)
}

/// Phase C C0 Stage 2: runtime-K merge over owned proof lists.
/// Delegates to [`crate::top_k_proofs::merge_top_k_with`]; exposed
/// `pub` (re-exported as `merge_top_k_runtime`) so the fixpoint loop
/// can call it directly without constructing `AggregatorValue`
/// wrappers when it has owned `Vec<Proof>` already.
pub fn merge_top_k_dispatch_owned(
    base: Vec<crate::top_k_proofs::Proof>,
    additional: Vec<crate::top_k_proofs::Proof>,
    k: usize,
) -> (
    Vec<crate::top_k_proofs::Proof>,
    crate::top_k_proofs::PruneNotice,
) {
    crate::top_k_proofs::merge_top_k_with(base, additional, k)
}

impl Default for SemiringDispatch {
    fn default() -> Self {
        Self::AddMultProb(AddMultProb::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_mult_prob_matches_pre_refactor_noisy_or() {
        let sr = AddMultProb::default();
        let mut acc = sr.zero_disjunction();
        for p in [0.72, 0.54, 0.56, 0.42] {
            acc = sr.plus(&acc, &p);
        }
        // From DEEP_LOCY.md §4.5: MNOR over [0.72, 0.54, 0.56, 0.42] ≈ 0.9671
        // Hand-computed exact: 1 - 0.28 * 0.46 * 0.44 * 0.58 = 0.96713024
        assert!((acc - 0.967_130_24).abs() < 1e-9, "got {acc}");
    }

    #[test]
    fn add_mult_prob_product_underflow_safe() {
        let sr = AddMultProb::new(1e-12);
        // Drive `times` into log-space.
        let r = sr.times(&1e-20, &1e-20);
        assert!(r.is_finite());
        assert!(r >= 0.0);
    }

    #[test]
    fn max_min_prob_viterbi() {
        let sr = MaxMinProb;
        assert_eq!(sr.plus(&0.3, &0.7), 0.7);
        assert_eq!(sr.times(&0.3, &0.7), 0.3);
    }

    #[test]
    fn strict_domain_violation() {
        let sr = AddMultProb::default();
        assert!(matches!(
            sr.validate_domain(1.5, "MNOR", true),
            Err(SemiringError::DomainViolation { .. })
        ));
        assert_eq!(sr.validate_domain(1.5, "MNOR", false).unwrap(), 1.0);
    }

    #[test]
    fn max_min_prob_strict_domain_violation() {
        let sr = MaxMinProb;
        assert!(matches!(
            sr.validate_domain(-0.1, "MPROD", true),
            Err(SemiringError::DomainViolation { .. })
        ));
        assert_eq!(sr.validate_domain(-0.1, "MPROD", false).unwrap(), 0.0);
        assert_eq!(sr.validate_domain(2.0, "MNOR", false).unwrap(), 1.0);
    }

    #[test]
    fn identities_are_correct() {
        // MNOR over empty set = 0.0 (additive identity).
        // MPROD over empty set = 1.0 (multiplicative identity).
        // Both semirings agree on identities — exercised by FoldExec
        // when a key group has no input rows.
        let add = AddMultProb::default();
        assert_eq!(add.zero_disjunction(), 0.0);
        assert_eq!(add.one_conjunction(), 1.0);
        let max = MaxMinProb;
        assert_eq!(max.zero_disjunction(), 0.0);
        assert_eq!(max.one_conjunction(), 1.0);
    }

    #[test]
    fn dispatch_routes_to_correct_impl() {
        // Same operands, different semirings — verifies SemiringDispatch
        // doesn't accidentally collapse the two semirings.
        let add = SemiringDispatch::new(SemiringKind::AddMultProb, 1e-15);
        let max = SemiringDispatch::new(SemiringKind::MaxMinProb, 1e-15);
        assert_eq!(add.plus(0.3, 0.5), 1.0 - 0.7 * 0.5); // 0.65
        assert_eq!(max.plus(0.3, 0.5), 0.5);
        assert_eq!(add.times(0.3, 0.5), 0.15);
        assert_eq!(max.times(0.3, 0.5), 0.3);

        // BddExact dispatches to AddMultProb at the row level — the BDD
        // post-correction runs separately at the fixpoint level. So
        // `SemiringDispatch::new(BddExact, ε).kind()` is AddMultProb by
        // design; the original kind is tracked separately on
        // `ResolvedSemiringConfig`.
        let bdd = SemiringDispatch::new(SemiringKind::BddExact, 1e-15);
        assert_eq!(bdd.kind(), SemiringKind::AddMultProb);
        assert_eq!(bdd.plus(0.3, 0.5), 1.0 - 0.7 * 0.5);
    }
}
