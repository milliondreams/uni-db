// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The published composition recipes, as executable proofs.
//!
//! [`op_parse::RECIPES`](super::op_parse::RECIPES) tells a guest author how to
//! build an operation the closed kernel catalog does not name directly. A recipe
//! that is merely *documented* is a promise; the failure mode this whole module
//! exists to prevent is a promise that has quietly stopped being true. So every
//! published recipe is executed here through real kernels and checked against an
//! independent scalar oracle, and `every_published_recipe_has_a_proof` fails if
//! a recipe is added without one.
//!
//! Oracle independence is the same invariant the differential suite holds
//! (proposal §9.0): the expected values are computed by a plain Rust loop that
//! shares no code with the kernels.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use uni_algo::algo::GraphProjection;
use uni_common::Value;
use uni_common::core::id::Vid;

use super::handle::Handle;
use super::op_parse::{OpFamily, RECIPES};
use super::session::{AlgoSession, EwiseOp, GraphCompute, MapOp, Predicate};
use super::value::{DType, Scalar};
use super::{Arena, WorkBudget};

/// A session over `n` isolated vertices — enough to carry `[V]` maps.
fn session_of(n: usize) -> (AlgoSession, Handle) {
    let node_rows: Vec<HashMap<String, Value>> = (0..n)
        .map(|id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
        .collect();
    let graph = GraphProjection::from_rows(&node_rows, &[], None, false)
        .expect("projection over isolated vertices");
    let mut session = AlgoSession::new(7, WorkBudget::new(10_000_000), Arena::new(8 << 20, 1024));
    let g = session.bind_graph(Arc::new(graph));
    (session, g)
}

/// Loads an explicit `[V]` map by scattering each value onto a zero map.
fn load(s: &mut AlgoSession, g: Handle, vals: &[f64]) -> Handle {
    let mut m = s.zero_map(g, DType::F64).expect("zero_map");
    for (i, &v) in vals.iter().enumerate() {
        let f = s.frontier(g, &[Vid::new(i as u64)]).expect("frontier");
        m = s.scatter(m, f, Scalar::F64(v)).expect("scatter");
    }
    m
}

fn read(s: &AlgoSession, h: Handle) -> Vec<f64> {
    s.tensor_values_for_test(h)
}

/// `compare(a, b, "gt")` — the ask that was reported as blocking a milestone.
#[test]
fn recipe_gt_builds_a_zero_one_mask() {
    let a_vals = [1.0, 5.0, 3.0, 9.0, 2.0];
    let b_vals = [4.0; 5];
    let (mut s, g) = session_of(5);
    let a = load(&mut s, g, &a_vals);
    let b = load(&mut s, g, &b_vals);

    let diff = s.ewise(a, b, EwiseOp::Axpy(-1.0)).expect("axpy");
    let hits = s.map_to_set(diff, Predicate::Gt(0.0)).expect("map_to_set");
    let mask = s.set_to_map(hits, Scalar::F64(1.0)).expect("set_to_map");

    let want: Vec<f64> = a_vals
        .iter()
        .zip(&b_vals)
        .map(|(x, y)| f64::from(u8::from(x > y)))
        .collect();
    assert_eq!(read(&s, mask), want);
}

/// `step(a, theta)` — thresholding against a constant needs no difference.
#[test]
fn recipe_step_thresholds_against_a_constant() {
    let vals = [1.0, 5.0, 3.0, 9.0, 2.0];
    let theta = 2.5;
    let (mut s, g) = session_of(5);
    let a = load(&mut s, g, &vals);

    let hits = s.map_to_set(a, Predicate::Gt(theta)).expect("map_to_set");
    let mask = s.set_to_map(hits, Scalar::F64(1.0)).expect("set_to_map");

    let want: Vec<f64> = vals
        .iter()
        .map(|x| f64::from(u8::from(*x > theta)))
        .collect();
    assert_eq!(read(&s, mask), want);
}

/// `select(m, a, b) == b + m * (a - b)`.
#[test]
fn recipe_select_blends_on_a_mask() {
    let a_vals = [1.0, 5.0, 3.0, 9.0, 2.0];
    let b_vals = [4.0; 5];
    let (mut s, g) = session_of(5);
    let a = load(&mut s, g, &a_vals);
    let b = load(&mut s, g, &b_vals);

    let diff = s.ewise(a, b, EwiseOp::Axpy(-1.0)).expect("axpy");
    let hits = s.map_to_set(diff, Predicate::Gt(0.0)).expect("map_to_set");
    let mask = s.set_to_map(hits, Scalar::F64(1.0)).expect("set_to_map");
    let scaled = s.ewise(mask, diff, EwiseOp::Mul).expect("mul");
    let sel = s.ewise(b, scaled, EwiseOp::Add).expect("add");

    let want: Vec<f64> = a_vals
        .iter()
        .zip(&b_vals)
        .map(|(x, y)| if x > y { *x } else { *y })
        .collect();
    assert_eq!(read(&s, sel), want);
}

/// `sub` in both positions: tensor-tensor via `axpy`, tensor-constant via `affine`.
#[test]
fn recipe_sub_covers_both_positions() {
    let a_vals = [10.0, 20.0, 30.0];
    let b_vals = [1.0, 2.0, 3.0];
    let c = 4.0;
    let (mut s, g) = session_of(3);
    let a = load(&mut s, g, &a_vals);
    let b = load(&mut s, g, &b_vals);

    let ewise_sub = s.ewise(a, b, EwiseOp::Axpy(-1.0)).expect("axpy");
    let want: Vec<f64> = a_vals.iter().zip(&b_vals).map(|(x, y)| x - y).collect();
    assert_eq!(read(&s, ewise_sub), want);

    let map_sub = s.map_apply(a, MapOp::AxPlusB(1.0, -c)).expect("affine");
    let want_const: Vec<f64> = a_vals.iter().map(|x| x - c).collect();
    assert_eq!(read(&s, map_sub), want_const);
}

/// `relu(a) == a * step(a, 0)`.
#[test]
fn recipe_relu_gates_on_its_own_sign() {
    let vals = [-2.0, -0.5, 0.0, 1.5, 4.0];
    let (mut s, g) = session_of(5);
    let a = load(&mut s, g, &vals);

    let hits = s.map_to_set(a, Predicate::Gt(0.0)).expect("map_to_set");
    let gate = s.set_to_map(hits, Scalar::F64(1.0)).expect("set_to_map");
    let out = s.ewise(a, gate, EwiseOp::Mul).expect("mul");

    let want: Vec<f64> = vals.iter().map(|x| x.max(0.0)).collect();
    assert_eq!(read(&s, out), want);
}

/// `abs(a) == max(a, -a)`.
#[test]
fn recipe_abs_is_max_against_the_negation() {
    let vals = [-2.0, -0.5, 0.0, 1.5, 4.0];
    let (mut s, g) = session_of(5);
    let a = load(&mut s, g, &vals);

    let neg = s.map_apply(a, MapOp::Scale(-1.0)).expect("scale");
    let out = s.ewise(a, neg, EwiseOp::Max).expect("max");

    let want: Vec<f64> = vals.iter().map(|x| x.abs()).collect();
    assert_eq!(read(&s, out), want);
    // `neg` on its own is the published negation recipe.
    let want_neg: Vec<f64> = vals.iter().map(|x| -x).collect();
    assert_eq!(read(&s, neg), want_neg);
}

/// `ceil(a) == -floor(-a)`.
///
/// The one member of the rounding family that composes *exactly* from `floor`,
/// which is why it is published as a recipe rather than given its own op. The
/// oracle is `f64::ceil`, so a half-open value like `-1.5` — where truncation
/// and rounding-up disagree — is what actually pins the identity.
#[test]
fn recipe_ceil_is_the_negated_floor_of_the_negation() {
    let vals = [-2.5, -1.5, -0.5, 0.0, 0.5, 1.5, 4.0];
    let (mut s, g) = session_of(7);
    let a = load(&mut s, g, &vals);

    let neg = s.map_apply(a, MapOp::Scale(-1.0)).expect("scale");
    let floored = s.map_apply(neg, MapOp::Floor).expect("floor");
    let out = s.map_apply(floored, MapOp::Scale(-1.0)).expect("scale");

    let want: Vec<f64> = vals.iter().map(|x| x.ceil()).collect();
    assert_eq!(read(&s, out), want);
}

/// `clip(a, lo, hi) == min(max(a, lo), hi)`.
#[test]
fn recipe_clip_is_min_of_max() {
    let vals = [-5.0, 0.0, 3.0, 7.0, 12.0];
    let (lo_v, hi_v) = (0.0, 8.0);
    let (mut s, g) = session_of(5);
    let a = load(&mut s, g, &vals);
    let lo = load(&mut s, g, &[lo_v; 5]);
    let hi = load(&mut s, g, &[hi_v; 5]);

    let lower = s.ewise(a, lo, EwiseOp::Max).expect("max");
    let out = s.ewise(lower, hi, EwiseOp::Min).expect("min");

    let want: Vec<f64> = vals.iter().map(|x| x.clamp(lo_v, hi_v)).collect();
    assert_eq!(read(&s, out), want);
}

/// `pow(a, 2) == a * a`; higher integer powers repeat the multiply.
#[test]
fn recipe_pow_repeats_multiplication() {
    let vals = [1.0, 2.0, 3.0, 4.0];
    let (mut s, g) = session_of(4);
    let a = load(&mut s, g, &vals);

    let squared = s.ewise(a, a, EwiseOp::Mul).expect("mul");
    let want_sq: Vec<f64> = vals.iter().map(|x| x * x).collect();
    assert_eq!(read(&s, squared), want_sq);

    let cubed = s.ewise(squared, a, EwiseOp::Mul).expect("mul");
    let want_cu: Vec<f64> = vals.iter().map(|x| x * x * x).collect();
    assert_eq!(read(&s, cubed), want_cu);
}

/// `normalize` is spelled with an explicit norm.
#[test]
fn recipe_normalize_names_its_norm() {
    let vals = [1.0, 2.0, 3.0, 4.0];
    let (mut s, g) = session_of(4);
    let a = load(&mut s, g, &vals);

    let l1 = s
        .map_apply(a, MapOp::Normalize(super::session::Norm::L1))
        .expect("normalize_l1");
    let total: f64 = vals.iter().sum();
    let want: Vec<f64> = vals.iter().map(|x| x / total).collect();
    assert_eq!(read(&s, l1), want);
}

/// Sharp edge #1: the `select` blend **poisons on NaN** in the *unselected*
/// branch, because `0.0 * NaN = NaN`.
///
/// The composition is not semantically identical to a primitive `select`, and
/// the published recipe says so. Where either branch may be NaN, scatter over
/// the `VertexSet` instead of multiplying by its lowered mask.
#[test]
fn select_blend_poisons_on_a_nan_in_the_unselected_branch() {
    let a_vals = [1.0, f64::NAN];
    let b_vals = [0.0, 5.0];
    let (mut s, g) = session_of(2);
    let a = load(&mut s, g, &a_vals);
    let b = load(&mut s, g, &b_vals);

    // Select `b` everywhere: mask is all zeros.
    let mask = load(&mut s, g, &[0.0, 0.0]);
    let diff = s.ewise(a, b, EwiseOp::Axpy(-1.0)).expect("axpy");
    let scaled = s.ewise(mask, diff, EwiseOp::Mul).expect("mul");
    let sel = s.ewise(b, scaled, EwiseOp::Add).expect("add");

    let got = read(&s, sel);
    assert_eq!(got[0], 0.0, "the finite lane blends correctly");
    assert!(
        got[1].is_nan(),
        "a NaN in the unselected branch poisons the blend — this is why the \
         recipe carries a caveat; got {got:?}"
    );

    // The scatter form is NaN-safe: it never multiplies by the poisoned lane.
    let chosen = s.map_to_set(mask, Predicate::Gt(0.5)).expect("map_to_set");
    let safe = s.scatter(b, chosen, Scalar::F64(0.0)).expect("scatter");
    assert!(
        read(&s, safe).iter().all(|v| v.is_finite()),
        "the scatter form must not propagate the unselected NaN"
    );
}

/// Sharp edge #2: the comparison recipe charges the native-work meter three
/// times, where a primitive `compare` would charge once.
///
/// Real but not structural at the scales the recipe is used at — and the reason
/// a `compare` kernel is still worth adding later.
#[test]
fn the_comparison_recipe_charges_three_passes() {
    let n = 64;
    let (mut s, g) = session_of(n);
    let a = load(&mut s, g, &vec![1.0; n]);
    let b = load(&mut s, g, &vec![0.0; n]);

    let before = s.work_spent_units();
    let diff = s.ewise(a, b, EwiseOp::Axpy(-1.0)).expect("axpy");
    let hits = s.map_to_set(diff, Predicate::Gt(0.0)).expect("map_to_set");
    let _mask = s.set_to_map(hits, Scalar::F64(1.0)).expect("set_to_map");
    let charged = s.work_spent_units() - before;

    assert_eq!(
        charged,
        3 * n as u64,
        "axpy + map_to_set + set_to_map each charge |V|"
    );
}

/// The coverage lock: a recipe published without a proof breaks the build.
#[test]
fn every_published_recipe_has_a_proof() {
    // Keyed by the first alias of each recipe; the tests above are named for
    // these. `edge_mask` is proven in `differential_tests` alongside the other
    // `[E]`-shaped assertions, where the edge fixtures already live.
    const PROVEN: &[&str] = &[
        "gt",
        "select",
        "sub",
        "relu",
        "abs",
        "clip",
        "pow",
        "neg",
        "ceil",
        "normalize",
        "edge_mask",
    ];
    let proven: HashSet<&str> = PROVEN.iter().copied().collect();
    for (keys, families, _, _) in RECIPES {
        let head = keys[0];
        assert!(
            proven.contains(head),
            "recipe `{head}` ({}) is published with no proof in \
             composition_recipes.rs — add one, or the recipe is only a promise",
            families[0].noun()
        );
    }
    // And nothing claims a proof it does not have.
    let published: HashSet<&str> = RECIPES.iter().map(|(keys, _, _, _)| keys[0]).collect();
    for name in PROVEN {
        assert!(
            published.contains(name),
            "`{name}` claims a proof but is no longer published"
        );
    }
    assert!(OpFamily::ALL.len() >= 7, "all vocabularies are declared");
}

// Rust guideline compliant
