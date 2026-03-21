//! BDD-based exact probability computation for shared-proof groups.
//!
//! When MNOR/MPROD groups have shared base facts (violating the
//! independence assumption), this module computes the correct joint
//! probability using Binary Decision Diagrams (BDDs).

use std::collections::{HashMap, HashSet};

use biodivine_lib_bdd::{Bdd, BddPointer, BddVariable, BddVariableSet};

/// Result of BDD-based probability computation for a single group.
#[derive(Debug)]
pub struct BddGroupResult {
    /// The exact (or fallback) probability value.
    pub probability: f64,
    /// True if the computation fell back to independence mode because
    /// the number of unique base facts exceeded `max_bdd_variables`.
    pub fell_back: bool,
    /// Number of unique base fact variables in this group.
    pub variable_count: usize,
}

/// Compute exact probability for an aggregation group using BDDs.
///
/// Each derivation row contributes a set of base facts (identified by
/// opaque byte-string hashes). The function builds a BDD representing
/// the Boolean combination of those facts and evaluates the probability
/// via Shannon expansion.
///
/// - `is_nor = true` → MNOR semantics: P(any row derives) = P(row₁ ∨ row₂ ∨ …)
/// - `is_nor = false` → MPROD semantics: P(all rows derive) = P(row₁ ∧ row₂ ∧ …)
pub fn compute_bdd_probability(
    derivation_base_facts: &[HashSet<Vec<u8>>],
    base_fact_probabilities: &HashMap<Vec<u8>, f64>,
    is_nor: bool,
    max_bdd_variables: usize,
) -> BddGroupResult {
    if derivation_base_facts.is_empty() {
        return BddGroupResult {
            probability: if is_nor { 0.0 } else { 1.0 },
            fell_back: false,
            variable_count: 0,
        };
    }

    // Collect all unique base facts across all derivation rows.
    let mut all_facts: Vec<Vec<u8>> =
        HashSet::<&Vec<u8>>::from_iter(derivation_base_facts.iter().flat_map(|s| s.iter()))
            .into_iter()
            .cloned()
            .collect();
    // Sort for deterministic variable ordering.
    all_facts.sort();

    let variable_count = all_facts.len();

    // Check BDD variable limit.
    if variable_count > max_bdd_variables || variable_count > u16::MAX as usize {
        return BddGroupResult {
            probability: 0.0, // caller should use independence-mode result
            fell_back: true,
            variable_count,
        };
    }

    // Map each base fact to a BDD variable index.
    let fact_to_idx: HashMap<&Vec<u8>, usize> =
        all_facts.iter().enumerate().map(|(i, f)| (f, i)).collect();

    let vars = BddVariableSet::new_anonymous(variable_count as u16);
    let bdd_vars: Vec<BddVariable> = vars.variables();

    // Build a BDD term per derivation row.
    // Each row's term is the AND of its base fact variables:
    //   term_i = base_fact_a ∧ base_fact_b ∧ …
    let mut combined: Option<Bdd> = None;

    for row_facts in derivation_base_facts {
        if row_facts.is_empty() {
            // A row with no base facts is unconditionally true.
            let term = vars.mk_true();
            combined = Some(match combined {
                Some(acc) => {
                    if is_nor {
                        acc.or(&term)
                    } else {
                        acc.and(&term)
                    }
                }
                None => term,
            });
            continue;
        }

        // Build AND of all base fact variables for this row.
        let mut term = vars.mk_true();
        for fact in row_facts {
            if let Some(&idx) = fact_to_idx.get(fact) {
                let var_bdd = vars.mk_var(bdd_vars[idx]);
                term = term.and(&var_bdd);
            }
        }

        // Combine with previous rows: OR for MNOR, AND for MPROD.
        combined = Some(match combined {
            Some(acc) => {
                if is_nor {
                    acc.or(&term)
                } else {
                    acc.and(&term)
                }
            }
            None => term,
        });
    }

    let bdd = match combined {
        Some(b) => b,
        None => {
            return BddGroupResult {
                probability: if is_nor { 0.0 } else { 1.0 },
                fell_back: false,
                variable_count,
            };
        }
    };

    // Build probability map: BddVariable → probability.
    let prob_map: HashMap<BddVariable, f64> = all_facts
        .iter()
        .enumerate()
        .map(|(i, fact)| {
            let p = base_fact_probabilities.get(fact).copied().unwrap_or(0.5);
            (bdd_vars[i], p)
        })
        .collect();

    let probability = eval_bdd_probability(&bdd, &prob_map);

    BddGroupResult {
        probability,
        fell_back: false,
        variable_count,
    }
}

/// Evaluate the probability of a BDD via Shannon expansion.
///
/// For each internal node with variable `v`:
///   P(node) = (1 - p_v) · P(low_child) + p_v · P(high_child)
///
/// Terminal nodes: P(⊥) = 0, P(⊤) = 1.
fn eval_bdd_probability(bdd: &Bdd, prob_map: &HashMap<BddVariable, f64>) -> f64 {
    let mut memo: HashMap<BddPointer, f64> = HashMap::new();
    eval_ptr(bdd, bdd.root_pointer(), prob_map, &mut memo)
}

fn eval_ptr(
    bdd: &Bdd,
    ptr: BddPointer,
    prob_map: &HashMap<BddVariable, f64>,
    memo: &mut HashMap<BddPointer, f64>,
) -> f64 {
    if ptr.is_zero() {
        return 0.0;
    }
    if ptr.is_one() {
        return 1.0;
    }
    if let Some(&cached) = memo.get(&ptr) {
        return cached;
    }

    let var = bdd.var_of(ptr);
    let p = prob_map.get(&var).copied().unwrap_or(0.5);
    let lo = eval_ptr(bdd, bdd.low_link_of(ptr), prob_map, memo);
    let hi = eval_ptr(bdd, bdd.high_link_of(ptr), prob_map, memo);
    let result = (1.0 - p) * lo + p * hi;
    memo.insert(ptr, result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: compute noisy-OR under independence assumption.
    fn noisy_or_independent(probs: &[f64]) -> f64 {
        1.0 - probs.iter().fold(1.0, |acc, &p| acc * (1.0 - p))
    }

    #[test]
    fn independent_facts_mnor_matches_noisy_or() {
        // Two derivation rows with completely independent base facts.
        // Row 0: {A(0.3)}
        // Row 1: {B(0.5)}
        // MNOR = P(A ∨ B) = 1 - (1-0.3)(1-0.5) = 0.65
        let a = b"fact_a".to_vec();
        let b = b"fact_b".to_vec();

        let rows = vec![HashSet::from([a.clone()]), HashSet::from([b.clone()])];
        let probs = HashMap::from([(a, 0.3), (b, 0.5)]);

        let result = compute_bdd_probability(&rows, &probs, true, 1000);
        assert!(!result.fell_back);
        assert_eq!(result.variable_count, 2);

        let expected = noisy_or_independent(&[0.3, 0.5]);
        assert!(
            (result.probability - expected).abs() < 1e-10,
            "BDD={}, expected={}",
            result.probability,
            expected
        );
    }

    #[test]
    fn shared_facts_mnor_differs_from_independence() {
        // Diamond pattern: two paths share base fact C (smelter).
        // Row 0: path through A and C → base facts {A(0.3), C(0.7)}
        // Row 1: path through B and C → base facts {B(0.5), C(0.7)}
        //
        // Independence MNOR: 1 - (1 - 0.3*0.7)(1 - 0.5*0.7) = 1 - 0.79*0.65 = 0.4865
        // Exact BDD:         P(row0 ∨ row1) = P((A∧C) ∨ (B∧C))
        //                  = P(C · (A∨B)) = 0.7 · (1 - 0.7·0.5) = 0.7 · 0.65 = 0.455
        let a = b"fact_a".to_vec();
        let b = b"fact_b".to_vec();
        let c = b"fact_c".to_vec();

        let rows = vec![
            HashSet::from([a.clone(), c.clone()]),
            HashSet::from([b.clone(), c.clone()]),
        ];
        let probs = HashMap::from([(a, 0.3), (b, 0.5), (c, 0.7)]);

        let result = compute_bdd_probability(&rows, &probs, true, 1000);
        assert!(!result.fell_back);
        assert_eq!(result.variable_count, 3);

        // Exact: P(C) * P(A ∨ B) = 0.7 * (1 - (1-0.3)(1-0.5)) = 0.7 * 0.65 = 0.455
        let expected_exact = 0.455;
        assert!(
            (result.probability - expected_exact).abs() < 1e-10,
            "BDD={}, expected={}",
            result.probability,
            expected_exact
        );

        // Verify it differs from independence mode.
        let independence = noisy_or_independent(&[0.3 * 0.7, 0.5 * 0.7]);
        assert!(
            (result.probability - independence).abs() > 0.01,
            "BDD result should differ from independence: BDD={}, indep={}",
            result.probability,
            independence
        );
    }

    #[test]
    fn shared_facts_mprod() {
        // MPROD with shared facts.
        // Row 0: {A(0.3), C(0.7)}
        // Row 1: {B(0.5), C(0.7)}
        //
        // MPROD = P(row0 ∧ row1) = P((A∧C) ∧ (B∧C)) = P(A∧B∧C) = 0.3 * 0.5 * 0.7 = 0.105
        let a = b"fact_a".to_vec();
        let b = b"fact_b".to_vec();
        let c = b"fact_c".to_vec();

        let rows = vec![
            HashSet::from([a.clone(), c.clone()]),
            HashSet::from([b.clone(), c.clone()]),
        ];
        let probs = HashMap::from([(a, 0.3), (b, 0.5), (c, 0.7)]);

        let result = compute_bdd_probability(&rows, &probs, false, 1000);
        assert!(!result.fell_back);
        assert_eq!(result.variable_count, 3);

        let expected = 0.3 * 0.5 * 0.7;
        assert!(
            (result.probability - expected).abs() < 1e-10,
            "BDD={}, expected={}",
            result.probability,
            expected
        );
    }

    #[test]
    fn bdd_limit_exceeded_falls_back() {
        let a = b"fact_a".to_vec();
        let b = b"fact_b".to_vec();

        let rows = vec![HashSet::from([a.clone()]), HashSet::from([b.clone()])];
        let probs = HashMap::from([(a, 0.3), (b, 0.5)]);

        // Set limit to 1 — there are 2 unique facts, so it should fall back.
        let result = compute_bdd_probability(&rows, &probs, true, 1);
        assert!(result.fell_back);
        assert_eq!(result.variable_count, 2);
    }

    #[test]
    fn empty_group_returns_identity() {
        let probs = HashMap::new();

        let nor_result = compute_bdd_probability(&[], &probs, true, 1000);
        assert!(!nor_result.fell_back);
        assert!((nor_result.probability - 0.0).abs() < 1e-10);

        let prod_result = compute_bdd_probability(&[], &probs, false, 1000);
        assert!(!prod_result.fell_back);
        assert!((prod_result.probability - 1.0).abs() < 1e-10);
    }

    #[test]
    fn single_row_returns_product_of_base_facts() {
        // Single row with two base facts: P = A * B = 0.3 * 0.5 = 0.15
        let a = b"fact_a".to_vec();
        let b = b"fact_b".to_vec();

        let rows = vec![HashSet::from([a.clone(), b.clone()])];
        let probs = HashMap::from([(a, 0.3), (b, 0.5)]);

        // For MNOR or MPROD with a single row, the result is the same:
        // P(A ∧ B) = 0.15
        let result = compute_bdd_probability(&rows, &probs, true, 1000);
        assert!((result.probability - 0.15).abs() < 1e-10);

        let result = compute_bdd_probability(&rows, &probs, false, 1000);
        assert!((result.probability - 0.15).abs() < 1e-10);
    }

    // ── New tests (Phase 4 coverage gap closure) ──────────────────────────

    #[test]
    fn independent_facts_mprod_matches_product() {
        // MPROD: two independent rows, no sharing.
        // Row 0: {A(0.3)}, Row 1: {B(0.5)}
        // MPROD = P(A ∧ B) = P(A) * P(B) = 0.3 * 0.5 = 0.15 (independent)
        let a = b"fact_a".to_vec();
        let b = b"fact_b".to_vec();

        let rows = vec![HashSet::from([a.clone()]), HashSet::from([b.clone()])];
        let probs = HashMap::from([(a, 0.3), (b, 0.5)]);

        let result = compute_bdd_probability(&rows, &probs, false, 1000);
        assert!(!result.fell_back);
        assert_eq!(result.variable_count, 2);

        let expected = 0.3 * 0.5;
        assert!(
            (result.probability - expected).abs() < 1e-10,
            "MPROD BDD={}, expected={}",
            result.probability,
            expected
        );
    }

    #[test]
    fn bdd_limit_exceeded_returns_zero_probability() {
        // Three rows with three distinct facts, limit=2 → falls back.
        let a = b"fact_a".to_vec();
        let b = b"fact_b".to_vec();
        let c = b"fact_c".to_vec();

        let rows = vec![
            HashSet::from([a.clone()]),
            HashSet::from([b.clone()]),
            HashSet::from([c.clone()]),
        ];
        let probs = HashMap::from([(a, 0.3), (b, 0.5), (c, 0.7)]);

        let result = compute_bdd_probability(&rows, &probs, true, 2);
        assert!(
            result.fell_back,
            "Expected fell_back=true when limit exceeded"
        );
        assert_eq!(result.variable_count, 3);
        assert!(
            (result.probability - 0.0).abs() < 1e-10,
            "Fallback probability should be 0.0, got {}",
            result.probability
        );
    }

    #[test]
    fn three_way_shared_mnor() {
        // Three derivation rows all sharing base fact D(0.8).
        // Row 0: {A(0.3), D(0.8)}
        // Row 1: {B(0.5), D(0.8)}
        // Row 2: {C(0.4), D(0.8)}
        //
        // Exact: P((A∧D) ∨ (B∧D) ∨ (C∧D)) = P(D ∧ (A∨B∨C))
        //      = P(D) * P(A∨B∨C) = 0.8 * (1-(1-0.3)(1-0.5)(1-0.4))
        //      = 0.8 * (1 - 0.7*0.5*0.6) = 0.8 * (1 - 0.21) = 0.8 * 0.79 = 0.632
        let a = b"fact_a".to_vec();
        let b = b"fact_b".to_vec();
        let c = b"fact_c".to_vec();
        let d = b"fact_d".to_vec();

        let rows = vec![
            HashSet::from([a.clone(), d.clone()]),
            HashSet::from([b.clone(), d.clone()]),
            HashSet::from([c.clone(), d.clone()]),
        ];
        let probs = HashMap::from([(a, 0.3), (b, 0.5), (c, 0.4), (d, 0.8)]);

        let result = compute_bdd_probability(&rows, &probs, true, 1000);
        assert!(!result.fell_back);
        assert_eq!(result.variable_count, 4);

        let expected = 0.8 * (1.0 - (1.0 - 0.3) * (1.0 - 0.5) * (1.0 - 0.4));
        assert!(
            (result.probability - expected).abs() < 1e-10,
            "BDD={}, expected={}",
            result.probability,
            expected
        );

        // Verify it differs from naive independence over row-products.
        let row0_prod = 0.3 * 0.8;
        let row1_prod = 0.5 * 0.8;
        let row2_prod = 0.4 * 0.8;
        let independence = 1.0 - (1.0 - row0_prod) * (1.0 - row1_prod) * (1.0 - row2_prod);
        assert!(
            (result.probability - independence).abs() > 0.01,
            "BDD result should differ from independence: BDD={}, indep={}",
            result.probability,
            independence
        );
    }

    #[test]
    fn partially_overlapping_rows_mnor() {
        // Three rows: two share fact C, one is fully independent.
        // Row 0: {A(0.3), C(0.7)}
        // Row 1: {B(0.5), C(0.7)}
        // Row 2: {E(0.6)}             ← independent
        //
        // Exact: P((A∧C) ∨ (B∧C) ∨ E)
        //      = P(C·(A∨B) ∨ E) where C·(A∨B) and E are independent
        //      = 1 - (1 - P(C·(A∨B))) * (1 - P(E))
        // P(C·(A∨B)) = P(C) * P(A∨B) = 0.7 * (1-(1-0.3)(1-0.5)) = 0.7 * 0.65 = 0.455
        // = 1 - (1-0.455)(1-0.6) = 1 - 0.545*0.4 = 1 - 0.218 = 0.782
        let a = b"fact_a".to_vec();
        let b = b"fact_b".to_vec();
        let c = b"fact_c".to_vec();
        let e = b"fact_e".to_vec();

        let rows = vec![
            HashSet::from([a.clone(), c.clone()]),
            HashSet::from([b.clone(), c.clone()]),
            HashSet::from([e.clone()]),
        ];
        let probs = HashMap::from([(a, 0.3), (b, 0.5), (c, 0.7), (e, 0.6)]);

        let result = compute_bdd_probability(&rows, &probs, true, 1000);
        assert!(!result.fell_back);
        assert_eq!(result.variable_count, 4);

        let p_c_times_a_or_b = 0.7 * (1.0 - (1.0 - 0.3) * (1.0 - 0.5));
        let expected = 1.0 - (1.0 - p_c_times_a_or_b) * (1.0 - 0.6);
        assert!(
            (result.probability - expected).abs() < 1e-10,
            "BDD={}, expected={}",
            result.probability,
            expected
        );
    }

    #[test]
    fn missing_probability_defaults_to_half() {
        // A fact not present in the probability map should default to 0.5.
        // Single row: {unknown_fact}
        // P(unknown) = 0.5 (default)
        let unknown = b"unknown_fact".to_vec();

        let rows = vec![HashSet::from([unknown.clone()])];
        let probs = HashMap::new(); // empty — unknown_fact has no entry

        let result = compute_bdd_probability(&rows, &probs, true, 1000);
        assert!(!result.fell_back);
        assert_eq!(result.variable_count, 1);
        assert!(
            (result.probability - 0.5).abs() < 1e-10,
            "Expected default probability 0.5, got {}",
            result.probability
        );
    }
}
