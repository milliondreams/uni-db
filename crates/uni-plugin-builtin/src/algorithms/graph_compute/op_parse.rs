// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! One source of truth for every guest-*supplied* op string.
//!
//! [`kernel_id`](super::kernel_id) does this for kernel *names*; this module is
//! its counterpart for the closed parameter vocabularies a guest passes as
//! strings — `"mul"`, `"gt"`, `"out"`, `"linear_algebra"`, and the rest. Before
//! this module each of the three in-process loader surfaces
//! (`dispatch.rs` for WASM/Extism, `uni-plugin-rhai`, `uni-plugin-pyo3`) carried
//! its own hand-written match per vocabulary — 21 blocks across seven enums —
//! and they had already drifted: two loaders rejected an unknown overlap metric
//! with `bad overlap metric`, the third with `overlap: bad metric`.
//!
//! # The drift guard
//!
//! `op_name` matches every variant **without a wildcard arm**, so adding a
//! variant to one of the enums in [`session`](super::session) without adding its
//! wire name here is a compile error rather than a silently unnameable op. The
//! generated `ALL_NAMES` is built from the same rows as the parse, so the
//! valid-op list printed on rejection cannot disagree with what is accepted.
//!
//! # Recipes
//!
//! The kernel catalog is deliberately coarse and the parameter enums
//! deliberately closed (proposal §4.3, decision D4). The accepted gate for a new
//! kernel is "is this composition-*blocked*?" — but nothing told a guest author
//! what *is* composable, and two consecutive field reports ranked
//! already-expressible operations as their top blocker. Docs did not fix it,
//! because the question is asked at the moment the engine rejects the op, not
//! while reading documentation. So the answer goes in the rejection:
//! [`RECIPES`] maps a requested-but-absent name to the composition that achieves
//! it, and [`rejection`] prints it.
//!
//! # Placement
//!
//! [`Semiring`] is `#[non_exhaustive]`, so its exhaustive match compiles only
//! inside the defining crate. This module can never move to `uni-plugin` or to a
//! loader crate.

use uni_plugin::errors::FnError;

use super::session::{CmpOp, Direction, EwiseOp, MapOp, Norm, OverlapMetric, Predicate, Semiring};

/// Declares each string vocabulary once and derives every projection of it.
///
/// Row grammar is `"name" => Pattern, constructor;`. The pattern feeds the
/// wildcard-free `op_name` match; the constructor feeds `from_op_name` and may
/// mention the scalar identifiers named in `scalars = (…)`. Nullary variants
/// simply do not mention them, so there is no "accept and discard" special case.
macro_rules! op_families {
    ($(
        $fam:ident : $ty:ident, noun = $noun:literal, scalars = ( $( $scalar:ident ),* ) {
            $( $name:literal => $pat:pat , $ctor:expr ; )+
        }
    )+) => {
        /// Which closed vocabulary a guest-supplied string was resolved against.
        ///
        /// Recipes are scoped by family because the same requested name composes
        /// differently in different positions: `"sub"` as an `ewise` op is
        /// `ewise(a, b, "axpy", -1.0)`, but as a `map_apply` op it is
        /// `map_apply(a, "affine", 1.0, -c)`.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum OpFamily {
            $( #[doc = concat!("The `", $noun, "` vocabulary.")] $fam, )+
        }

        impl OpFamily {
            /// Every vocabulary. Generated, so it cannot omit one.
            pub const ALL: &'static [OpFamily] = &[ $( OpFamily::$fam, )+ ];

            /// The noun used in rejection messages (`"map op"`, `"semiring"`, …).
            #[must_use]
            pub fn noun(self) -> &'static str {
                match self { $( Self::$fam => $noun, )+ }
            }

            /// Every name this vocabulary accepts, in declaration order.
            #[must_use]
            pub fn valid_names(self) -> &'static [&'static str] {
                match self { $( Self::$fam => $ty::ALL_NAMES, )+ }
            }
        }

        $(
            impl $ty {
                #[doc = concat!("Every accepted `", $noun, "` name, in declaration order.")]
                ///
                /// Generated from the same rows as the parse, so the valid-op
                /// list printed on rejection cannot drift from what is accepted.
                pub const ALL_NAMES: &'static [&'static str] = &[ $( $name, )+ ];

                #[doc = concat!("The canonical wire name of this `", $noun, "`.")]
                ///
                /// Lossy for payload-carrying variants — `Scale(2.0)` and
                /// `Scale(3.0)` both name `"scale"` — so the round trip that
                /// holds is name → value → name.
                #[must_use]
                pub fn op_name(self) -> &'static str {
                    match self { $( $pat => $name, )+ }
                }

                #[doc = concat!("Resolves a `", $noun, "` name and its scalar operands, or `None`.")]
                #[must_use]
                pub fn from_op_name(name: &str $(, $scalar: f64 )*) -> Option<Self> {
                    match name { $( $name => Some($ctor), )+ _ => None }
                }

                #[doc = concat!("Resolves a `", $noun, "` name, or a typed rejection carrying a recipe.")]
                ///
                /// The single parse every loader delegates to.
                ///
                /// # Errors
                /// Returns the [`rejection`] for an unrecognised name.
                pub fn parse(name: &str $(, $scalar: f64 )*) -> Result<Self, FnError> {
                    Self::from_op_name(name $(, $scalar )*)
                        .ok_or_else(|| rejection(OpFamily::$fam, name))
                }
            }
        )+
    };
}

op_families! {
    Direction: Direction, noun = "direction", scalars = () {
        "out"  => Direction::Out,  Direction::Out;
        "in"   => Direction::In,   Direction::In;
        "both" => Direction::Both, Direction::Both;
    }

    Map: MapOp, noun = "map op", scalars = (a, b) {
        "recip"        => MapOp::Recip,               MapOp::Recip;
        "scale"        => MapOp::Scale(_),            MapOp::Scale(a);
        "log"          => MapOp::Log,                 MapOp::Log;
        "sqrt"         => MapOp::Sqrt,                MapOp::Sqrt;
        "exp"          => MapOp::Exp,                 MapOp::Exp;
        "affine"       => MapOp::AxPlusB(_, _),       MapOp::AxPlusB(a, b);
        "normalize_l1" => MapOp::Normalize(Norm::L1), MapOp::Normalize(Norm::L1);
        "normalize_l2" => MapOp::Normalize(Norm::L2), MapOp::Normalize(Norm::L2);
    }

    Ewise: EwiseOp, noun = "ewise op", scalars = (coef) {
        "add"  => EwiseOp::Add,     EwiseOp::Add;
        "mul"  => EwiseOp::Mul,     EwiseOp::Mul;
        "min"  => EwiseOp::Min,     EwiseOp::Min;
        "max"  => EwiseOp::Max,     EwiseOp::Max;
        "axpy" => EwiseOp::Axpy(_), EwiseOp::Axpy(coef);
        "div"  => EwiseOp::Div,     EwiseOp::Div;
    }

    Cmp: CmpOp, noun = "comparison", scalars = () {
        "gt" => CmpOp::Gt, CmpOp::Gt;
        "ge" => CmpOp::Ge, CmpOp::Ge;
        "lt" => CmpOp::Lt, CmpOp::Lt;
        "le" => CmpOp::Le, CmpOp::Le;
        "eq" => CmpOp::Eq, CmpOp::Eq;
        "ne" => CmpOp::Ne, CmpOp::Ne;
    }

    Predicate: Predicate, noun = "predicate", scalars = (threshold) {
        "is_zero" => Predicate::IsZero, Predicate::IsZero;
        "gt"      => Predicate::Gt(_),  Predicate::Gt(threshold);
        "lt"      => Predicate::Lt(_),  Predicate::Lt(threshold);
        "eq"      => Predicate::Eq(_),  Predicate::Eq(threshold);
    }

    Norm: Norm, noun = "norm", scalars = () {
        "l1" => Norm::L1, Norm::L1;
        "l2" => Norm::L2, Norm::L2;
    }

    Semiring: Semiring, noun = "semiring", scalars = () {
        "reachability"   => Semiring::Reachability,  Semiring::Reachability;
        "shortest_path"  => Semiring::ShortestPath,  Semiring::ShortestPath;
        "propagate"      => Semiring::Propagate,     Semiring::Propagate;
        "linear_algebra" => Semiring::LinearAlgebra, Semiring::LinearAlgebra;
        "min_max"        => Semiring::MinMax,        Semiring::MinMax;
    }

    Overlap: OverlapMetric, noun = "overlap metric", scalars = () {
        "count"       => OverlapMetric::Count,      OverlapMetric::Count;
        "jaccard"     => OverlapMetric::Jaccard,    OverlapMetric::Jaccard;
        "overlap"     => OverlapMetric::Overlap,    OverlapMetric::Overlap;
        "cosine"      => OverlapMetric::Cosine,     OverlapMetric::Cosine;
        "adamic_adar" => OverlapMetric::AdamicAdar, OverlapMetric::AdamicAdar;
    }
}

/// Declares the published compositions once, for both messages and docs.
macro_rules! recipes {
    ($( [ $( $key:literal ),+ ] in [ $( $fam:ident ),+ ] => $label:literal , $recipe:literal ; )+) => {
        /// Every published composition: `(requested names, families, label, recipe)`.
        ///
        /// Keyed by names that are deliberately *absent* from every vocabulary,
        /// so they cannot hang off an enum variant — the same shape as
        /// [`GRAPH_ARENA_OPS`](super::GRAPH_ARENA_OPS), which lists names that
        /// are not [`KernelId`](super::KernelId)s.
        ///
        /// Recipes are written in real kernel-call syntax. There is no `axpy`
        /// kernel, for instance — it is `ewise(a, b, "axpy", coef)` — and a
        /// recipe naming a kernel that does not exist is exactly the failure this
        /// module exists to prevent. `recipes_only_name_real_kernels` enforces it.
        ///
        /// Placeholder convention in the recipe text: bare identifiers are
        /// things the guest supplies — `a`, `b`, `m` are map handles, `g` is the
        /// graph handle, `prop` a property name, and `theta` / `lo` / `hi` / `c`
        /// are scalars. **Quoted** strings are always literal op names, which is
        /// what lets `recipes_only_quote_real_op_names` check them.
        pub const RECIPES: &[(&[&str], &[OpFamily], &str, &str)] = &[
            $( ( &[ $( $key, )+ ], &[ $( OpFamily::$fam, )+ ], $label, $recipe ), )+
        ];
    };
}

recipes! {
    ["gt", "ge", "lt", "le", "eq", "ne", "cmp", "compare"] in [Ewise] =>
        "elementwise comparison",
        "compare(a, b, \"gt\")";

    ["gt", "ge", "lt", "le", "ne", "cmp", "compare", "step", "heaviside", "threshold", "indicator"]
        in [Map] =>
        "thresholding against a constant",
        "set_to_map(map_to_set(a, \"gt\", theta), 1.0)";

    ["select", "where", "blend", "ifelse"] in [Ewise, Map] =>
        "masked selection (a NaN in the unselected branch poisons the blend; \
         scatter over the VertexSet instead when that is possible)",
        "ewise(b, ewise(m, ewise(a, b, \"axpy\", -1.0), \"mul\"), \"add\")";

    ["sub", "subtract", "minus"] in [Ewise] =>
        "subtraction",
        "ewise(a, b, \"axpy\", -1.0)";

    ["sub", "subtract", "minus"] in [Map] =>
        "subtracting a constant",
        "map_apply(a, \"affine\", 1.0, -c)";

    ["relu"] in [Map, Ewise] =>
        "ReLU",
        "ewise(a, set_to_map(map_to_set(a, \"gt\", 0.0), 1.0), \"mul\")";

    ["abs", "fabs"] in [Map, Ewise] =>
        "absolute value",
        "ewise(a, map_apply(a, \"scale\", -1.0), \"max\")";

    ["clip", "clamp"] in [Ewise] =>
        "clipping between two maps",
        "ewise(ewise(a, lo, \"max\"), hi, \"min\")";

    ["pow", "power", "square"] in [Map, Ewise] =>
        "integer powers",
        "repeat ewise(x, x, \"mul\"); x squared is ewise(a, a, \"mul\")";

    ["neg", "negate"] in [Map] =>
        "negation",
        "map_apply(a, \"scale\", -1.0)";

    ["normalize"] in [Map] =>
        "an explicit norm",
        "map_apply(m, \"normalize_l1\") or map_apply(m, \"normalize_l2\")";

    ["edge_mask", "nonzero"] in [Predicate] =>
        "an exact edge mask from a stored 1.0/0.0 selector property",
        "edge_mask_window(edge_property(g, prop), 0.5, 1.5)";
}

/// Compositions published against a *method* name a guest reached for.
///
/// [`RECIPES`] fires when an op **string** is rejected (`ewise(a, b, "gt")`).
/// This fires one layer out, when a guest calls a **method** that does not exist
/// at all (`gc.select(..)`, `gc.arena_spmv(..)`) — a failure the loader's own
/// function resolution produces, which the op parser never sees.
///
/// It is a separate table rather than a reindexing of [`RECIPES`] for three
/// reasons. A method call carries no [`OpFamily`], and `"sub"` composes
/// differently as an `ewise` op than as a `map_apply` op, so a single answer has
/// to be chosen. Some [`RECIPES`] keys are *also* real kernel names now
/// (`compare`, `normalize`), which is fine for op strings but would be a lie
/// here. And the most useful entry — `arena_spmv` — has no op-string form at all.
///
/// Every key must be a name that is **not** a kernel, or the hint would shadow a
/// working method; `no_method_recipe_shadows_a_real_kernel` enforces that.
pub const METHOD_RECIPES: &[(&[&str], &str, &str)] = &[
    (
        &["select", "where", "blend", "ifelse"],
        "a conditional blend",
        "ewise(b, ewise(m, ewise(a, b, \"axpy\", -1.0), \"mul\"), \"add\")",
    ),
    (
        &["sub", "subtract", "minus"],
        "subtraction",
        "ewise(a, b, \"axpy\", -1.0)",
    ),
    (
        &["abs", "fabs"],
        "absolute value",
        "ewise(a, map_apply(a, \"scale\", -1.0), \"max\")",
    ),
    (
        &["relu"],
        "ReLU",
        "ewise(a, set_to_map(map_to_set(a, \"gt\", 0.0), 1.0), \"mul\")",
    ),
    (
        &["clip", "clamp"],
        "clipping between two maps",
        "ewise(ewise(a, lo, \"max\"), hi, \"min\")",
    ),
    (
        &["step", "heaviside", "threshold", "indicator"],
        "thresholding against a constant",
        "set_to_map(map_to_set(a, \"gt\", theta), 1.0)",
    ),
    (
        &["edge_mask", "nonzero"],
        "an exact edge mask from a stored selector column",
        "edge_mask_window(edge_property(g, prop), 0.5, 1.5)",
    ),
    (
        &[
            "arena_to_map",
            "to_map",
            "rebase",
            "project",
            "graph_column",
        ],
        "moving an arena-computed column onto a projection so it can be emitted",
        "rekey(col, g)",
    ),
    (
        &["arena_spmv", "arena_neighbour_sum", "arena_aggregate"],
        "aggregation over links you grew",
        "spmv(arena_freeze(a), col, \"linear_algebra\", \"out\")",
    ),
];

/// Returns the published composition for a method name a guest reached for.
#[must_use]
pub fn method_recipe_for(requested: &str) -> Option<(&'static str, &'static str)> {
    METHOD_RECIPES
        .iter()
        .find(|(keys, _, _)| keys.contains(&requested))
        .map(|(_, label, recipe)| (*label, *recipe))
}

/// Builds the message for a method a guest called that does not exist.
///
/// Returns `None` when the name has no published composition, so the caller
/// keeps its own (loader-specific) wording rather than inventing advice.
#[must_use]
pub fn unknown_method_message(requested: &str) -> Option<String> {
    method_recipe_for(requested).map(|(label, recipe)| {
        format!("`{requested}` is not a kernel. For {label}, compose it: {recipe}")
    })
}

/// The published composition for `requested` in `family`, if there is one.
#[must_use]
pub fn recipe_for(family: OpFamily, requested: &str) -> Option<(&'static str, &'static str)> {
    RECIPES
        .iter()
        .find(|(keys, fams, _, _)| keys.contains(&requested) && fams.contains(&family))
        .map(|(_, _, label, recipe)| (*label, *recipe))
}

/// Builds the rejection for an unknown op name in `family`.
///
/// A name with a published composition earns the recipe *and* the valid list; a
/// plain typo earns the valid list alone. The error code is unchanged from the
/// hand-written sites this replaces — only the message improves.
#[must_use]
pub fn rejection(family: OpFamily, requested: &str) -> FnError {
    let noun = family.noun();
    let valid = family.valid_names().join(", ");
    let message = match recipe_for(family, requested) {
        Some((label, recipe)) => format!(
            "bad {noun} `{requested}` — {label} is composable: {recipe}; \
             valid {noun}s: {valid}"
        ),
        None => format!("bad {noun} `{requested}`; valid {noun}s: {valid}"),
    };
    FnError::new(super::error::KIND_MISMATCH, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// T1 — names are unique within a family and round-trip name → value → name.
    ///
    /// Deliberately not value → name → value: `op_name` is lossy for
    /// payload-carrying variants. Keyed on `&str` because `MapOp`, `EwiseOp` and
    /// `Predicate` carry `f64` payloads and so are `PartialEq` without `Eq`/`Hash`.
    #[test]
    fn every_op_name_is_unique_and_round_trips() {
        for &family in OpFamily::ALL {
            let names = family.valid_names();
            let unique: HashSet<&&str> = names.iter().collect();
            assert_eq!(
                unique.len(),
                names.len(),
                "{} has a duplicate name: {names:?}",
                family.noun()
            );
            assert!(!names.is_empty(), "{} has no names", family.noun());
        }

        // Scalars are arbitrary: the payload does not participate in the name.
        for &n in Direction::ALL_NAMES {
            assert_eq!(Direction::parse(n).expect(n).op_name(), n);
        }
        for &n in MapOp::ALL_NAMES {
            assert_eq!(MapOp::parse(n, 2.0, 3.0).expect(n).op_name(), n);
        }
        for &n in EwiseOp::ALL_NAMES {
            assert_eq!(EwiseOp::parse(n, 5.0).expect(n).op_name(), n);
        }
        for &n in Predicate::ALL_NAMES {
            assert_eq!(Predicate::parse(n, 0.5).expect(n).op_name(), n);
        }
        for &n in CmpOp::ALL_NAMES {
            assert_eq!(CmpOp::parse(n).expect(n).op_name(), n);
        }
        for &n in Norm::ALL_NAMES {
            assert_eq!(Norm::parse(n).expect(n).op_name(), n);
        }
        for &n in Semiring::ALL_NAMES {
            assert_eq!(Semiring::parse(n).expect(n).op_name(), n);
        }
        for &n in OverlapMetric::ALL_NAMES {
            assert_eq!(OverlapMetric::parse(n).expect(n).op_name(), n);
        }
    }

    /// Every op string a method hint quotes must be a real op name.
    ///
    /// The op-string table has had this guard since it was written; the method
    /// table shipped without it, and immediately published
    /// `ewise(a, b, "sub")` — `sub` is not an ewise op, and the `sub` hint itself
    /// correctly says to use `axpy` with a coefficient of -1. Two published
    /// recipes contradicting each other is worse than no recipe: it costs a
    /// reader the time to try it.
    #[test]
    fn method_recipes_only_quote_real_op_names() {
        let mut valid: HashSet<&str> = HashSet::new();
        for &family in OpFamily::ALL {
            valid.extend(family.valid_names());
        }
        for (keys, _, recipe) in METHOD_RECIPES {
            for token in recipe.split('"').skip(1).step_by(2) {
                assert!(
                    valid.contains(token),
                    "method recipe for {keys:?} quotes `{token}`, which is not an op name"
                );
            }
        }
    }

    /// A method hint must never shadow a kernel that actually exists.
    ///
    /// This is the safety precondition for wiring the table into a loader's
    /// unknown-function hook: those hooks fire on resolution failure generally,
    /// so a hint keyed to a *registered* name could start answering for real
    /// errors raised from inside that kernel. `compare` and `normalize` are the
    /// live examples — both are `RECIPES` keys (legal, they are op strings) and
    /// both are now real kernels, so neither may appear here.
    #[test]
    fn no_method_recipe_shadows_a_real_kernel() {
        let kernels: HashSet<&str> = super::super::KernelId::ALL
            .iter()
            .map(|k| k.op_name())
            .collect();
        for (keys, _, _) in METHOD_RECIPES {
            for k in *keys {
                assert!(
                    !kernels.contains(k),
                    "`{k}` is a real kernel; a method hint for it would shadow the \
                     working method and could answer for errors raised inside it"
                );
            }
        }
    }

    /// Every method hint composes from kernels that exist.
    #[test]
    fn method_recipes_only_name_real_kernels() {
        let kernels: HashSet<&str> = super::super::KernelId::ALL
            .iter()
            .map(|k| k.op_name())
            .collect();
        for (keys, _, recipe) in METHOD_RECIPES {
            for call in called_identifiers(recipe) {
                assert!(
                    kernels.contains(call.as_str()),
                    "method recipe for {keys:?} calls `{call}`, which is not a kernel"
                );
            }
        }
    }

    /// A method hint must not be offered for a name with no composition, and
    /// must be offered for every name that has one.
    #[test]
    fn method_recipe_lookup_round_trips() {
        for (keys, label, recipe) in METHOD_RECIPES {
            for k in *keys {
                assert_eq!(
                    method_recipe_for(k),
                    Some((*label, *recipe)),
                    "`{k}` must resolve to its own entry"
                );
                let msg = unknown_method_message(k).expect("a keyed name has a message");
                assert!(
                    msg.contains(recipe),
                    "the message must carry the recipe: {msg}"
                );
            }
        }
        assert!(
            method_recipe_for("definitely_not_a_thing").is_none(),
            "an unknown name earns no invented advice"
        );
    }

    /// T2a — every kernel a recipe calls actually exists.
    ///
    /// This is the check that catches publishing `axpy(a, b, -1.0)` when the
    /// kernel is really `ewise(a, b, "axpy", coef)`.
    #[test]
    fn recipes_only_name_real_kernels() {
        let kernels: HashSet<&str> = super::super::KernelId::ALL
            .iter()
            .map(|k| k.op_name())
            .collect();
        for (keys, _, _, recipe) in RECIPES {
            for call in called_identifiers(recipe) {
                assert!(
                    kernels.contains(call.as_str()),
                    "recipe for {keys:?} calls `{call}`, which is not a kernel"
                );
            }
        }
    }

    /// Identifiers immediately followed by `(` — i.e. the calls a recipe makes.
    /// Skips anything inside a quoted string so op-name arguments are not read
    /// as calls.
    fn called_identifiers(recipe: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        for ch in recipe.chars() {
            if ch == '"' {
                in_quotes = !in_quotes;
                current.clear();
                continue;
            }
            if in_quotes {
                continue;
            }
            if ch.is_alphanumeric() || ch == '_' {
                current.push(ch);
            } else {
                if ch == '(' && !current.is_empty() {
                    out.push(current.clone());
                }
                current.clear();
            }
        }
        out
    }

    /// T2b — every quoted token in a recipe is a real op name somewhere.
    #[test]
    fn recipes_only_quote_real_op_names() {
        let mut valid: HashSet<&str> = HashSet::new();
        for &family in OpFamily::ALL {
            valid.extend(family.valid_names());
        }
        for (keys, _, _, recipe) in RECIPES {
            for token in recipe.split('"').skip(1).step_by(2) {
                assert!(
                    valid.contains(token),
                    "recipe for {keys:?} quotes `{token}`, which is not an op name"
                );
            }
        }
    }

    /// T2c — the dead-recipe guard.
    ///
    /// A recipe is only reachable while its key is *absent* from the family's
    /// vocabulary. The day someone adds `EwiseOp::Sub`, the `"sub"` recipe stops
    /// being reachable — this fails then, instead of the advice silently dying.
    #[test]
    fn no_recipe_shadows_a_real_op_name() {
        for (keys, families, _, _) in RECIPES {
            for &key in *keys {
                for &family in *families {
                    assert!(
                        !family.valid_names().contains(&key),
                        "`{key}` is a real {} — its recipe is unreachable and \
                         should be deleted",
                        family.noun()
                    );
                }
            }
        }
    }

    /// T3 — the rejection carries a recipe when one exists, the vocabulary
    /// always, and the error code the hand-written sites used.
    #[test]
    fn rejection_carries_the_recipe_and_the_vocabulary() {
        let err = EwiseOp::parse("gt", 0.0).expect_err("`gt` is not an ewise op");
        assert_eq!(
            err.code,
            super::super::error::KIND_MISMATCH,
            "the code is ABI; only the message changed"
        );
        assert!(err.message.contains("composable"), "{}", err.message);
        assert!(
            err.message.contains("compare(a, b, \"gt\")"),
            "the rejection must point at the compare kernel, not the 3-pass \
             composition it replaced: {}",
            err.message
        );
        for &n in EwiseOp::ALL_NAMES {
            assert!(err.message.contains(n), "missing `{n}`: {}", err.message);
        }

        let typo = EwiseOp::parse("mux", 0.0).expect_err("`mux` is not an ewise op");
        assert!(
            !typo.message.contains("composable"),
            "a typo has no recipe: {}",
            typo.message
        );
        for &n in EwiseOp::ALL_NAMES {
            assert!(typo.message.contains(n), "missing `{n}`: {}", typo.message);
        }
    }

    /// The family scoping is load-bearing: `"sub"` composes differently
    /// depending on where it was asked for.
    #[test]
    fn recipes_are_scoped_by_family() {
        let ewise = recipe_for(OpFamily::Ewise, "sub").expect("ewise sub recipe");
        let map = recipe_for(OpFamily::Map, "sub").expect("map sub recipe");
        assert_ne!(
            ewise.1, map.1,
            "a flat table would hand a map_apply caller ewise advice"
        );
        assert!(recipe_for(OpFamily::Semiring, "sub").is_none());
    }

    /// The kernel count in the published reference is the real one.
    ///
    /// It had rotted across two consecutive kernel-adding commits before this
    /// guard existed — the same drift the `kernels!` catalog was built to close,
    /// one level up. Derived from `KernelId::ALL`, so the number cannot be typed
    /// wrong again.
    #[test]
    fn the_published_kernel_count_is_the_real_one() {
        const REFERENCE: &str =
            include_str!("../../../../../website/docs/plugins/graph-algorithms.md");
        let total = super::super::KernelId::ALL.len();
        let all_loaders = super::super::KernelId::all_loaders().count();
        assert!(
            REFERENCE.contains(&format!("{total} kernels.")),
            "website/docs/plugins/graph-algorithms.md must say `{total} kernels.`"
        );
        assert_eq!(
            total,
            all_loaders + 2,
            "exactly two kernels (`graph`, `graph_named`) are host-supplied rather than \
             AllLoaders -- both hand the guest a projection handle the host already built"
        );
    }

    /// T7 — every published recipe appears verbatim in the user-facing docs.
    ///
    /// The whole point of this module is that a guest author is told the truth
    /// at the moment of rejection *and* when reading the reference. If the two
    /// drift, we are back to "the doc says composable, the engine says no" —
    /// which is the failure Theme A names. `include_str!` couples this test to
    /// the repo layout on purpose: that coupling is what makes the drift
    /// detectable.
    #[test]
    fn every_recipe_appears_in_the_published_reference() {
        const REFERENCE: &str =
            include_str!("../../../../../website/docs/plugins/graph-algorithms.md");
        for (keys, _, _, recipe) in RECIPES {
            assert!(
                REFERENCE.contains(recipe),
                "recipe for {keys:?} is not in website/docs/plugins/graph-algorithms.md:\n  {recipe}"
            );
        }
        for &family in OpFamily::ALL {
            for name in family.valid_names() {
                assert!(
                    REFERENCE.contains(name),
                    "`{name}` ({}) is accepted by the engine but absent from the reference",
                    family.noun()
                );
            }
        }
    }
}

// Rust guideline compliant
