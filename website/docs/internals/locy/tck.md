# Locy TCK and Compliance

## Purpose

The Locy TCK validates language behavior across parse, compile, and evaluate layers.

## Coverage Areas

- Core rules and recursion.
- Stratified negation.
- ALONG/FOLD/BEST BY semantics.
- QUERY, ASSUME, ABDUCE, EXPLAIN.
- Module composition and priority behaviors.

## Test Layers

1. Parse tests.
2. Compile validation tests.
3. Evaluation semantics tests.

## How to Use in Development

- Run focused feature suites while implementing one language area.
- Use TCK failures as compatibility signal before release.
- Maintain schema-mode coverage where applicable.

## Related Sources

- `crates/uni-locy-tck/tck/features/*`
- `docs/locy/locy-tck-spec.md`
