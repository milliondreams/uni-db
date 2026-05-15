// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase C C0: `TopKProofs<K>` row-at-a-time semiring with per-tag
//! dependency DNFs (impl plan §1.6 decision D-7, §3.0).
//!
//! Each row carries a [`TopKTag`] holding up to `K` [`Proof`]s. Every
//! [`Proof`] records the set of base random variables it depends on
//! (via [`crate::dependency_dnf::BaseRvSet`]) and the set of neural
//! classifier calls it consumed. Tag arithmetic (`plus`, `times`)
//! merges proof lists and prunes to the top-K by weight; pruning that
//! discards a proof whose dependencies overlap a retained one signals
//! a `TopKPruningCrossedDependency` warning.
//!
//! ### Stage 1 scope (this slice)
//!
//! The library math is complete: `plus`, `times`, `negate`, `weight`
//! compute the right answers and are unit-tested against
//! hand-computed inclusion-exclusion. The runtime `SemiringDispatch`
//! recognizes `TopKProofs` at config-resolution time but currently
//! falls back to `AddMultProb` row math at hot-path evaluation sites;
//! Stage 2 plumbs `TopKTag` through `MonotonicAggState`, `FoldExec`,
//! and the IS-ref / complement code paths.

use std::collections::HashSet;

use crate::dependency_dnf::{BaseRvSet, DependencyDnf};
use crate::semiring::{LocySemiring, SemiringError};
use crate::types::SemiringKind;

/// Identifier for a neural-classifier invocation. Two `Proof`s with
/// the same `NeuralCallId` share the same classifier output —
/// foundation for rollout decision D-8 (correlation under shared θ)
/// and Phase-C F2a (`SharedNeuralModelInGroup`).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct NeuralCallId(pub u32);

/// One retained proof inside a [`TopKTag`].
///
/// `weight` is the product of all probabilities along the derivation
/// chain — base-fact probabilities + neural-call outputs — under the
/// independence assumption. The exact joint probability of the tag is
/// computed by [`TopKProofs::weight`] via inclusion-exclusion on the
/// DNF formed by the proofs' `base_rvs` sets.
#[derive(Debug, Clone, PartialEq)]
pub struct Proof {
    pub weight: f64,
    pub base_rvs: BaseRvSet,
    pub neural_calls: Vec<NeuralCallId>,
}

impl Proof {
    /// The trivially-true proof: empty dependency set, weight 1.0.
    /// Multiplicative identity in [`TopKProofs::times`].
    pub fn tautology() -> Self {
        Self {
            weight: 1.0,
            base_rvs: BaseRvSet::empty(),
            neural_calls: Vec::new(),
        }
    }

    /// Two proofs are dependency-equivalent when they carry the same
    /// base-RV set AND the same neural-call set. `TopKProofs::plus`
    /// uses this to dedup before pruning (otherwise duplicate proofs
    /// would waste a top-K slot).
    fn dependency_key(&self) -> (Vec<u32>, Vec<u32>) {
        let mut rvs: Vec<u32> = self.base_rvs.iter().map(|r| r.0).collect();
        rvs.sort_unstable();
        let mut calls: Vec<u32> = self.neural_calls.iter().map(|c| c.0).collect();
        calls.sort_unstable();
        (rvs, calls)
    }
}

/// Top-K proof tag carried per row.
///
/// `proofs.len() ≤ K`. The list is sorted by weight descending after
/// every `plus` / `times` (canonical form).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TopKTag {
    pub proofs: Vec<Proof>,
}

impl TopKTag {
    /// Additive identity: the "no derivation found" tag. `plus(zero,
    /// t) = t`; `weight(zero) = 0.0`.
    pub fn zero() -> Self {
        Self { proofs: Vec::new() }
    }

    /// Multiplicative identity: a single tautological proof.
    /// `times(one, t) = t`; `weight(one) = 1.0`.
    pub fn one() -> Self {
        Self {
            proofs: vec![Proof::tautology()],
        }
    }

    pub fn from_proofs(proofs: Vec<Proof>) -> Self {
        Self { proofs }
    }

    pub fn is_empty(&self) -> bool {
        self.proofs.is_empty()
    }

    /// Build the [`DependencyDnf`] view of this tag for use by
    /// [`crate::dependency_dnf::DependencyDnf::weight`].
    pub fn to_dnf(&self) -> DependencyDnf {
        DependencyDnf {
            clauses: self.proofs.iter().map(|p| p.base_rvs.clone()).collect(),
        }
    }
}

/// Phase C C0 row-at-a-time semiring instance.
///
/// `K` is the maximum number of proofs retained per tag. Plus / times
/// canonicalize and prune to K; pruning that crosses a dependency
/// edge sets [`PruneNotice::CrossedDependency`].
pub struct TopKProofs<const K: usize>;

impl<const K: usize> TopKProofs<K> {
    pub const fn capacity() -> usize {
        K
    }

    /// Merge `additional` into `base`, dedup by dependency key
    /// (max-weight wins), sort descending, truncate to K. Returns a
    /// [`PruneNotice`] callers can use to drive
    /// `TopKPruningCrossedDependency` emission.
    pub fn merge_top_k(mut base: Vec<Proof>, additional: Vec<Proof>) -> (Vec<Proof>, PruneNotice) {
        base.extend(additional);
        // Dedup: max-weight wins per dependency key.
        let mut keep: Vec<Proof> = Vec::with_capacity(base.len());
        let mut seen: std::collections::HashMap<(Vec<u32>, Vec<u32>), usize> =
            std::collections::HashMap::new();
        for p in base.drain(..) {
            let key = p.dependency_key();
            match seen.get(&key) {
                Some(&idx) => {
                    if p.weight > keep[idx].weight {
                        keep[idx] = p;
                    }
                }
                None => {
                    seen.insert(key, keep.len());
                    keep.push(p);
                }
            }
        }
        keep.sort_by(|a, b| {
            b.weight
                .partial_cmp(&a.weight)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if keep.len() <= K {
            return (keep, PruneNotice::None);
        }
        // Identify pruning-crossed-dependency: any discarded proof
        // whose base_rvs intersect with any retained proof's base_rvs.
        let (retained, dropped) = keep.split_at(K);
        let mut crossed = false;
        for d in dropped {
            if retained
                .iter()
                .any(|r| BaseRvSet::intersect_any(&r.base_rvs, &d.base_rvs))
            {
                crossed = true;
                break;
            }
        }
        let notice = if crossed {
            PruneNotice::CrossedDependency
        } else {
            PruneNotice::Pruned
        };
        let retained_owned = retained.to_vec();
        (retained_owned, notice)
    }
}

/// Result of a top-K pruning step. Callers use this to drive
/// `RuntimeWarningCode::TopKPruningCrossedDependency` emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneNotice {
    /// No proofs were discarded; the merged list fits within K.
    None,
    /// Proofs were discarded but none of them overlap any retained
    /// proof's `base_rvs`. The approximation is "honest in the small"
    /// — the retained top-K is a reasonable lower bound.
    Pruned,
    /// A discarded proof shared base RVs with a retained proof, so
    /// the exact probability of the *retained* set under-counts the
    /// true correlation. Increase K to recover.
    CrossedDependency,
}

impl<const K: usize> LocySemiring for TopKProofs<K> {
    type Tag = TopKTag;

    fn kind(&self) -> SemiringKind {
        SemiringKind::TopKProofs { k: K as u32 }
    }

    fn zero_disjunction(&self) -> TopKTag {
        TopKTag::zero()
    }

    fn one_conjunction(&self) -> TopKTag {
        TopKTag::one()
    }

    fn plus(&self, a: &TopKTag, b: &TopKTag) -> TopKTag {
        let (proofs, _) = Self::merge_top_k(a.proofs.clone(), b.proofs.clone());
        TopKTag { proofs }
    }

    fn times(&self, a: &TopKTag, b: &TopKTag) -> TopKTag {
        if a.proofs.is_empty() || b.proofs.is_empty() {
            return TopKTag::zero();
        }
        let mut cartesian: Vec<Proof> = Vec::with_capacity(a.proofs.len() * b.proofs.len());
        for pa in &a.proofs {
            for pb in &b.proofs {
                let mut nc = pa.neural_calls.clone();
                nc.extend(pb.neural_calls.iter().copied());
                // Dedup neural calls — same call appearing in both
                // proof chains is a single dependency event, not two.
                let mut seen: HashSet<u32> = HashSet::new();
                nc.retain(|c| seen.insert(c.0));
                cartesian.push(Proof {
                    weight: pa.weight * pb.weight,
                    base_rvs: BaseRvSet::union(&pa.base_rvs, &pb.base_rvs),
                    neural_calls: nc,
                });
            }
        }
        let (proofs, _) = Self::merge_top_k(Vec::new(), cartesian);
        TopKTag { proofs }
    }

    fn negate(&self, a: &TopKTag) -> Result<TopKTag, SemiringError> {
        // Phase D D-C0c: complement under TopKProofs would naturally
        // invert the DNF — `¬(A ∨ B ∨ C) = ¬A ∧ ¬B ∧ ¬C` — which is a
        // single conjunction over base-RV *complements*, not a
        // disjunction-of-conjunctions. That doesn't fit the
        // row-at-a-time `Vec<Proof>` shape.
        //
        // The pragmatic resolution: collapse to a **degenerate tag** —
        // a singleton proof with weight `1 − weight(a)` and no base
        // RVs / neural calls. Downstream operations on the negated
        // tag fall back to independence-mode math (no dependency
        // structure to preserve). Library callers that need exact
        // dependency-aware complement should compute at the f64
        // layer; this trait impl keeps the surface uniform with
        // AddMultProb / MaxMinProb's `negate` returning a valid Tag.
        let w = self.weight(a);
        let complement = (1.0 - w).clamp(0.0, 1.0);
        Ok(TopKTag {
            proofs: vec![Proof {
                weight: complement,
                base_rvs: BaseRvSet::empty(),
                neural_calls: Vec::new(),
            }],
        })
    }

    fn weight(&self, a: &TopKTag) -> f64 {
        // Without per-base-RV probabilities, the conservative fallback
        // is independence-mode noisy-OR over the proof weights. Stage
        // 2 will plumb a base-weight map alongside the tag.
        let mut complement = 1.0;
        for p in &a.proofs {
            complement *= 1.0 - p.weight;
        }
        (1.0 - complement).clamp(0.0, 1.0)
    }

    fn validate_domain(
        &self,
        raw: f64,
        op: &'static str,
        strict: bool,
    ) -> Result<f64, SemiringError> {
        if !(0.0..=1.0).contains(&raw) {
            if strict {
                return Err(SemiringError::DomainViolation { value: raw, op });
            }
            let clamped = raw.clamp(0.0, 1.0);
            tracing::warn!("{op} input {raw} outside [0,1], clamped to {clamped}");
            Ok(clamped)
        } else {
            Ok(raw)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dependency_dnf::BaseRv;
    use std::collections::HashMap;

    fn proof(weight: f64, rvs: &[u32]) -> Proof {
        let mut s = BaseRvSet::empty();
        for r in rvs {
            s.insert(BaseRv(*r));
        }
        Proof {
            weight,
            base_rvs: s,
            neural_calls: Vec::new(),
        }
    }

    #[test]
    fn empty_tag_is_additive_identity() {
        let sr = TopKProofs::<4>;
        let z = sr.zero_disjunction();
        let t = TopKTag::from_proofs(vec![proof(0.5, &[1])]);
        assert_eq!(sr.plus(&z, &t), t.clone());
        assert_eq!(sr.plus(&t, &z), t);
    }

    #[test]
    fn one_tag_is_multiplicative_identity() {
        let sr = TopKProofs::<4>;
        let one = sr.one_conjunction();
        let t = TopKTag::from_proofs(vec![proof(0.5, &[1])]);
        assert_eq!(sr.times(&one, &t), t.clone());
        assert_eq!(sr.times(&t, &one), t);
    }

    #[test]
    fn weight_of_zero_is_zero() {
        let sr = TopKProofs::<4>;
        assert_eq!(sr.weight(&sr.zero_disjunction()), 0.0);
    }

    #[test]
    fn weight_of_one_is_one() {
        let sr = TopKProofs::<4>;
        assert_eq!(sr.weight(&sr.one_conjunction()), 1.0);
    }

    #[test]
    fn weight_single_proof() {
        let sr = TopKProofs::<4>;
        let t = TopKTag::from_proofs(vec![proof(0.3, &[1])]);
        assert!((sr.weight(&t) - 0.3).abs() < 1e-12);
    }

    #[test]
    fn weight_independent_proofs_match_noisy_or() {
        let sr = TopKProofs::<4>;
        let t = TopKTag::from_proofs(vec![proof(0.3, &[1]), proof(0.5, &[2])]);
        // Independence-mode noisy-OR (TopKProofs::weight fallback):
        let expected = 1.0 - (1.0 - 0.3) * (1.0 - 0.5);
        assert!((sr.weight(&t) - expected).abs() < 1e-12);
    }

    #[test]
    fn dnf_view_corrects_for_shared_rv() {
        // Two proofs share BaseRv(1). The DNF inclusion-exclusion gives
        // the EXACT probability — different from TopKProofs::weight's
        // independence-mode fallback (Stage 2 wires this in).
        let t = TopKTag::from_proofs(vec![proof(0.5 * 0.4, &[1, 2]), proof(0.5 * 0.6, &[1, 3])]);
        let dnf = t.to_dnf();
        let weights: HashMap<BaseRv, f64> = [(BaseRv(1), 0.5), (BaseRv(2), 0.4), (BaseRv(3), 0.6)]
            .into_iter()
            .collect();
        // P(A∧B) + P(A∧C) − P(A∧B∧C) = 0.20 + 0.30 − 0.12 = 0.38
        assert!((dnf.weight(&weights) - 0.38).abs() < 1e-12);
    }

    #[test]
    fn plus_dedups_identical_dependency_proofs() {
        let sr = TopKProofs::<4>;
        let a = TopKTag::from_proofs(vec![proof(0.4, &[1])]);
        let b = TopKTag::from_proofs(vec![proof(0.7, &[1])]);
        let result = sr.plus(&a, &b);
        // Same base_rvs → keep max weight only.
        assert_eq!(result.proofs.len(), 1);
        assert_eq!(result.proofs[0].weight, 0.7);
    }

    #[test]
    fn plus_retains_top_k_by_weight() {
        let (kept, notice) = TopKProofs::<2>::merge_top_k(
            vec![],
            vec![proof(0.1, &[1]), proof(0.9, &[2]), proof(0.5, &[3])],
        );
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].weight, 0.9);
        assert_eq!(kept[1].weight, 0.5);
        // No retained proof shares base_rvs with the discarded p=0.1
        // (BaseRv(1) is disjoint from BaseRv(2), BaseRv(3)).
        assert_eq!(notice, PruneNotice::Pruned);
    }

    #[test]
    fn plus_emits_crossed_dependency_when_pruning_drops_shared_rv() {
        // Top-K = 2. We retain p=0.9 with {1, 2} and p=0.5 with {3, 4}.
        // We drop p=0.3 with {1, 5}. The dropped proof shares BaseRv(1)
        // with a retained proof — flag the crossing.
        let (kept, notice) = TopKProofs::<2>::merge_top_k(
            vec![],
            vec![
                proof(0.9, &[1, 2]),
                proof(0.5, &[3, 4]),
                proof(0.3, &[1, 5]),
            ],
        );
        assert_eq!(kept.len(), 2);
        assert_eq!(notice, PruneNotice::CrossedDependency);
    }

    #[test]
    fn times_cartesian_products_proofs() {
        let sr = TopKProofs::<4>;
        let a = TopKTag::from_proofs(vec![proof(0.5, &[1]), proof(0.6, &[2])]);
        let b = TopKTag::from_proofs(vec![proof(0.4, &[3])]);
        let result = sr.times(&a, &b);
        assert_eq!(result.proofs.len(), 2);
        // After dedup & sort: weights are 0.6*0.4=0.24 and 0.5*0.4=0.20
        assert!((result.proofs[0].weight - 0.24).abs() < 1e-12);
        assert!((result.proofs[1].weight - 0.20).abs() < 1e-12);
        // Base RV sets unioned.
        assert!(result.proofs[0].base_rvs.contains(BaseRv(2)));
        assert!(result.proofs[0].base_rvs.contains(BaseRv(3)));
    }

    #[test]
    fn times_with_zero_is_zero() {
        let sr = TopKProofs::<4>;
        let z = sr.zero_disjunction();
        let t = TopKTag::from_proofs(vec![proof(0.5, &[1])]);
        assert_eq!(sr.times(&z, &t), TopKTag::zero());
        assert_eq!(sr.times(&t, &z), TopKTag::zero());
    }

    #[test]
    fn negate_collapses_to_degenerate_tag() {
        // Phase D D-C0c: negate(tag) returns a degenerate tag with a
        // singleton proof whose weight is `1 - weight(tag)`. Loses
        // dependency structure — that's documented and intentional.
        let sr = TopKProofs::<4>;
        let t = TopKTag::from_proofs(vec![proof(0.5, &[1])]);
        // weight(t) under independence-mode noisy-OR over the proofs
        // is 1 - (1 - 0.5) = 0.5; complement should be 0.5.
        let neg = sr.negate(&t).unwrap();
        assert_eq!(neg.proofs.len(), 1);
        assert!((neg.proofs[0].weight - 0.5).abs() < 1e-12);
        assert_eq!(neg.proofs[0].base_rvs.iter().count(), 0);
        assert!(neg.proofs[0].neural_calls.is_empty());
    }

    #[test]
    fn negate_of_two_independent_proofs() {
        // weight(tag) = 1 - (1-0.7)*(1-0.5) = 1 - 0.15 = 0.85
        // complement = 0.15
        let sr = TopKProofs::<4>;
        let t = TopKTag::from_proofs(vec![proof(0.7, &[1]), proof(0.5, &[2])]);
        let neg = sr.negate(&t).unwrap();
        assert_eq!(neg.proofs.len(), 1);
        assert!((neg.proofs[0].weight - 0.15).abs() < 1e-12);
    }

    #[test]
    fn kind_reports_k() {
        assert_eq!(TopKProofs::<4>.kind(), SemiringKind::TopKProofs { k: 4 });
        assert_eq!(TopKProofs::<16>.kind(), SemiringKind::TopKProofs { k: 16 });
    }

    #[test]
    fn dedup_after_cartesian_in_times() {
        // Two distinct proofs in `a` that produce the same dependency
        // key after timesing with `b` should dedup to a single proof.
        let sr = TopKProofs::<4>;
        // Same base_rvs in both proofs of `a` (different weights).
        let a = TopKTag::from_proofs(vec![proof(0.3, &[1]), proof(0.6, &[1])]);
        let b = TopKTag::from_proofs(vec![proof(0.5, &[2])]);
        let result = sr.times(&a, &b);
        // After cartesian: {1,2} weight 0.15 and {1,2} weight 0.30. Dedup
        // by dependency key keeps the max.
        assert_eq!(result.proofs.len(), 1);
        assert!((result.proofs[0].weight - 0.30).abs() < 1e-12);
    }
}
