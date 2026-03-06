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
        "max_iterations": 500,
        "timeout": 60.0,
        "max_abduce_candidates": 30,
        "max_abduce_results": 10,
        "max_derived_bytes": 64 * 1024 * 1024,
        "deterministic_best_by": True,
    },
)
```

## Return Contract

Returned dict includes:

- `derived`: `dict[str, list[dict]]`
- `stats`: `LocyStats`
- `command_results`: `list[dict]`

## Notes

- Python API currently materializes query result lists.
- Use focused `QUERY` clauses to keep payloads manageable.

## Full API References

- [Main Python API Reference](../../reference/python-api.md)
- [Generated Python API Docs](../../api/python/index.md)
