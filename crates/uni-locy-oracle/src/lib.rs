//! Naive-Datalog reference oracle for differential-testing the Locy engine.
//!
//! Locy is a Datalog dialect, and Datalog's *naive fixpoint* (re-evaluate every
//! rule over every fact until nothing changes) is trivially correct and
//! semantically identical to the optimized semi-naive + `LeftAnti` algorithm the
//! production engine runs. This crate implements that naive evaluator as a
//! reference oracle: generated monotone-core programs are run through both the
//! engine and the oracle, and their derived fact sets must match exactly. A
//! mismatch is, by construction, an engine bug.
//!
//! # Independence invariant
//!
//! The oracle's entire value is sharing *zero* evaluation code with the engine.
//! This crate's **library** therefore depends only on [`uni_common`] (for the
//! shared [`uni_common::Value`] type) and `proptest`. The engine (`uni-db`) is a
//! dev-dependency, reachable only from `tests/`. Do not add `uni-db`, `uni-locy`,
//! or `uni-query` to `[dependencies]` — the boundary is the guarantee.
//!
//! # Scope
//!
//! The oracle covers Locy's **monotone core** — plain rules, `IS` references,
//! stratified `IS NOT`, and `YIELD` — plus **non-recursive `FOLD`** over the
//! bag of a rule's rows (`COUNT(*)`, `COUNT`, `SUM`/`MSUM`, `MIN`/`MMIN`,
//! `MAX`/`MMAX`, `MCOUNT`): every binding counts, so parallel edges contribute
//! once each. A recursive `FOLD` (whose self-reference contributes the target's
//! folded value), `ALONG`, `BEST BY`, `DERIVE`, `ASSUME`, `PROB` and `HAVING`
//! are out of scope; a recursive `FOLD` panics rather than being mis-handled.
//!
//! [`generator::random_program_strategy`] draws random programs over a random
//! multigraph (self-loops, parallel and identical edges, two relationship
//! types, integer weights) that exercise all of the above.

// Rust guideline compliant
pub mod eval;
pub mod generator;
pub mod ir;

#[cfg(test)]
mod tests {
    /// P0 smoke test: proves the crate compiles and its test target runs.
    #[test]
    fn crate_links() {}
}
