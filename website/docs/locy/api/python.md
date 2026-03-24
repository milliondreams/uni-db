# Locy Python API Integration

## Sync API

```python
out = db.locy_evaluate(program)
```

## Async API

```python
out = await adb.locy_evaluate(program)
```

## Optional Config

```python
out = db.locy_evaluate(
    program,
    {
        # Evaluation limits
        "max_iterations": 500,
        "timeout": 60.0,
        "max_explain_depth": 50,
        "max_slg_depth": 500,
        "max_abduce_candidates": 30,
        "max_abduce_results": 10,
        "max_derived_bytes": 64 * 1024 * 1024,
        "deterministic_best_by": True,
        # Probabilistic reasoning
        "strict_probability_domain": True,   # Error on values outside [0, 1] instead of clamping
        "probability_epsilon": 1e-15,        # MPROD log-space threshold
        "exact_probability": True,           # Enable BDD-based exact evaluation for shared-proof groups
        "max_bdd_variables": 1000,           # Per-group BDD variable cap before fallback
    },
)
```

## Return Contract

Returned dict includes:

- `derived`: `dict[str, list[dict]]` — facts derived by each rule (keyed by rule name)
- `stats`: `LocyStats` — iteration counts, timing, stratum info
- `command_results`: `list[dict]` — output rows from `QUERY`, `ABDUCE`, `EXPLAIN RULE`
- `warnings`: `list[dict]` — runtime warnings (e.g., `SharedProbabilisticDependency`)
- `approximate_groups`: `dict[str, list[str]]` — rule/key groups that fell back to approximate probability mode

### Warnings

Each warning dict has:

```python
{
    "code": "SharedProbabilisticDependency",  # or "BddLimitExceeded", "CrossGroupCorrelationNotExact"
    "message": "...",
    "rule_name": "supplier_risk",
    "variable_count": None,   # set for BddLimitExceeded
    "key_group": None,        # set when a specific key group is affected
}
```

Warning codes:

| Code | Meaning |
|------|---------|
| `SharedProbabilisticDependency` | Multiple MNOR/MPROD proof paths reuse the same evidence; result is approximate |
| `BddLimitExceeded` | `exact_probability` was enabled but group exceeded `max_bdd_variables`; fell back to independence mode |
| `CrossGroupCorrelationNotExact` | Shared evidence spans multiple aggregate groups; cross-group correlation is approximate |

Rows from approximate groups are marked with `_approximate = True` in the derived facts.

### Checking Warnings

```python
out = db.locy_evaluate(program)

if out["warnings"]:
    for w in out["warnings"]:
        print(f"[{w['code']}] {w['message']}")

# Check for a specific warning code
has_shared = any(w["code"] == "SharedProbabilisticDependency" for w in out["warnings"])

# Inspect which groups are approximate
for rule, groups in out["approximate_groups"].items():
    print(f"Rule '{rule}' has approximate groups: {groups}")
```

## Notes

- Python API currently materializes query result lists.
- Use focused `QUERY` clauses to keep payloads manageable.

## Full API References

- [Main Python API Reference](../../reference/python-api.md)
- [Generated Python API Docs](../../api/python/index.md)
