# Locy Rust API Integration

## Primary Entry Point

```rust
let locy = db.locy();
```

## Core Methods

```rust
let compiled = db.locy().compile_only(program)?;
let result = db.locy().evaluate(program).await?;
let result = db.locy().evaluate_with_config(program, &cfg).await?;
let explain = db.locy().explain(program).await?;
```

## Config (`LocyConfig`)

Common fields:

- `max_iterations`
- `timeout`
- `max_explain_depth`
- `max_slg_depth`
- `max_abduce_candidates`
- `max_abduce_results`
- `max_derived_bytes`
- `deterministic_best_by`
- `strict_probability_domain`
- `probability_epsilon`
- `exact_probability`
- `max_bdd_variables`

## Result Shape (`LocyResult`)

- `derived: HashMap<String, Vec<Row>>`
- `stats: LocyStats`
- `command_results: Vec<CommandResult>`
- `warnings: Vec<RuntimeWarning>`
- `approximate_groups: HashMap<String, Vec<String>>`

Helpers:

- `result.rows()`
- `result.columns()`
- `result.stats()`
- `result.warnings()`
- `result.has_warning(...)`

## Full API References

- [Main Rust API Reference](../../reference/rust-api.md)
- [Internals: Compiler](../../internals/locy/compiler.md)
