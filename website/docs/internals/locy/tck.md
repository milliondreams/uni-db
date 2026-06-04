# Locy TCK and Compliance

## Purpose

The Locy TCK validates language behavior across parse, compile, and evaluate layers.

## Coverage Areas

The feature suites under `crates/uni-locy-tck/tck/features/` cover:

- Core rules and recursion (`rules/`, `combinations/`).
- Stratified negation (`negation/`).
- ALONG / FOLD / BEST BY semantics (`along/`, `fold/`, `bestby/`).
- Monotonic aggregation operators (`monotonic/`).
- QUERY, ASSUME, ABDUCE, DERIVE, EXPLAIN (`query/`, `assume/`, `abduce/`, `derive/`, `explain/`).
- Module composition and priority behaviors (`modules/`, `priority/`).
- Lexical / parse-level and compile-validation coverage (`lexical/`, `compile/`, `evaluate/`).
- Neural predicates — CREATE MODEL, model invocation, CALIBRATE / VALIDATE, graph-structural and retrieval features (`neural/`).
- Probabilistic semirings — AddMultProb, MaxMinProb (Viterbi), TopKProofs (`semiring/`).
- Correlation / shared-evidence warnings — cross-group correlation, shared retrieval context, fold-in-recursive-path (`correlation/`).

## Test Layers

1. Parse tests.
2. Compile validation tests.
3. Evaluation semantics tests.

## How to Use in Development

- Run focused feature suites while implementing one language area.
- Use TCK failures as compatibility signal before release.
- Maintain schema-mode coverage where applicable.

## Related Sources

- `crates/uni-locy-tck/tck/features/*` — the feature files and their `.schema.json` expectation specs.
- `crates/uni-locy-tck/` — the TCK harness crate.
