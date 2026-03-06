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

## Runtime Issues

### Too Many Iterations

Increase `max_iterations` only after verifying recursion converges.

### Timeout

Narrow query goals or reduce branching; tune `timeout` with caution.

### Memory Pressure

Lower result breadth and tune `max_derived_bytes`.

## Debug Workflow

1. `compile_only` to validate program structure.
2. Run with smaller datasets.
3. Use `EXPLAIN RULE` on specific bindings.
4. Add constraints to `QUERY`/`ABDUCE` scopes.
