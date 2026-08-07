# Release Notes — 3.3.0

**Version:** 3.3.0 (`chore(release)` at `d8f6f3abc`) — **untagged** at time of
writing. **Range:** `v3.2.0..HEAD`

Written retrospectively from the commit range.

## Locy — silent wrong answers (#158 / #159 / #160)

This release is dominated by correctness fixes in the Locy runtime, several
**breaking** (`!`):

- **Fail loudly on the #158/#159/#160 silent wrong answers.** These previously
  returned plausible-but-wrong results with no error.
- **`QUERY` is answered from the derived store** instead of re-deriving through
  an independent top-down SLG evaluator. Two evaluators of unequal power was the
  root cause of #160; reading the same store makes `QUERY` and `.derived` agree
  by construction. SLG survives only for generator rules, which the columnar
  fixpoint cannot express.
- **Recursive `FOLD` aggregates across derivations, not distinct values**, and
  folds a child's *value* rather than its derivations.
- **Breaking:** an `IS NOT` subject that is not a node is now rejected at compile
  time (`IsNotSubjectNotANode`).
- `IS NOT` subjects resolve by identity, and IS-refs bind.
- Schemaless `YIELD` property columns infer as `LargeBinary`.

## Query engine

- `LargeUtf8` columns decode correctly, and four silent degradations now fail
  closed.

## Housekeeping

- Dead code removed and oversized modules split.
- `uni-tck`'s integration binaries consolidated from four to two, restoring the
  documented 3-binary cap.

## Upgrade notes

The Locy changes are behaviour-changing by design: a program that previously
returned a wrong answer now either returns the right one or errors. Re-run any
Locy program whose output you have baked into a fixture.
