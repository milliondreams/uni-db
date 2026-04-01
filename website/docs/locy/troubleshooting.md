# Locy Troubleshooting

## Compile Errors

### Undefined Rule

Cause: `IS`/`IS NOT` references a non-existent rule.

Fix: Define referenced rule first or import the module containing it.

### Cyclic Negation

Cause: stratification violation through negation cycles.

Fix: refactor rule dependencies to remove negative cycles.

### Schema Mismatch

Cause: overloaded rule clauses yield incompatible column sets/types.

Fix: align `YIELD` contracts across clauses.

### BestByWithMonotonicFold

Cause: `BEST BY` used in the same rule as `MNOR` or `MPROD`.

Fix: Monotonic folds are incompatible with witness selection. Use a separate rule to compute the probability, then reference it with `IS` in the `BEST BY` rule.

### ProbabilityDomainViolation (warning)

Cause: `MNOR` or `MPROD` used with non-literal arguments that may produce values outside `[0, 1]`.

Fix: The compiler warning is informational — values are clamped at runtime unless `strict_probability_domain = true`. Sanitize upstream scores if clamping is masking a data quality issue.

## Runtime Issues

### Too Many Iterations

Increase `max_iterations` only after verifying recursion converges.

### Timeout

Narrow query goals or reduce branching; tune `timeout` with caution.

### Memory Pressure

Lower result breadth and tune `max_derived_bytes`.

### Probability Domain Failures

If `strict_probability_domain = true`, values outside `[0, 1]` cause evaluation errors instead of being clamped.

Fix: sanitize upstream scores or disable strict mode while investigating the source.

### Shared-Probability Warnings (`SharedProbabilisticDependency`)

Cause: multiple MNOR/MPROD proof paths reuse the same evidence; the independence assumption is violated.

Fix: enable `exact_probability = true` for exact per-group results via BDD evaluation, or accept the independence approximation and inspect `warnings` / `_approximate` markers to understand which groups are affected.

### BDD Limit Exceeded (`BddLimitExceeded`)

Cause: `exact_probability` is enabled but a proof group exceeded `max_bdd_variables`. That group fell back to independence mode.

Fix: increase `max_bdd_variables` (at the cost of memory/CPU), or restructure the rule to reduce the number of independent variables in a single aggregate group.

### Cross-Group Correlation (`CrossGroupCorrelationNotExact`)

Cause: shared evidence spans multiple aggregate key groups. Each group is exact internally (via BDD), but cross-group correlation is still approximate.

Fix: This is a fundamental limitation when shared evidence crosses group boundaries. If exact cross-group probability is required, restructure the query so all correlated facts fall in the same aggregate group.

## Debug Workflow

1. `session.compile_locy(program)` to validate program structure.
2. Run with smaller datasets.
3. Use `EXPLAIN RULE` on specific bindings.
4. Add constraints to `QUERY`/`ABDUCE` scopes.
