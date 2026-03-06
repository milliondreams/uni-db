# Locy Errors and Limits

## Common Error Classes

- Parse errors: invalid Locy syntax.
- Compile errors: invalid dependencies, type/schema mismatches, stratification violations.
- Runtime errors: timeout, iteration limit, memory constraints, mutation constraints.

## Operational Limits (via `LocyConfig`)

- `max_iterations`: recursion cap per recursive stratum.
- `timeout`: overall evaluation budget.
- `max_derived_bytes`: derived fact memory bound.
- `max_explain_depth`: derivation tree depth bound.
- `max_slg_depth`: goal-directed recursion bound.
- `max_abduce_candidates` / `max_abduce_results`: abduction search bounds.

## Recommended Profiles

### Development

- Lower iteration and timeout values.
- Keep deterministic tie-break enabled.

### Production

- Set explicit timeout and memory budgets.
- Monitor command result sizes.
- Restrict unconstrained `QUERY` and `ABDUCE` patterns.

## Escalation Playbook

1. Reduce goal scope.
2. Add stronger filters.
3. Split large programs by module.
4. Profile expensive rule strata.
