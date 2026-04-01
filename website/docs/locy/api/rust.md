# Locy Rust API Integration

## Primary Entry Point

All Locy operations go through a **Session**, created from a `Uni` instance:

```rust
let session = db.session();
```

## Core Methods

```rust
// Simple evaluation
let result = session.locy(program).await?;

// With inline parameters
let result = session.locy(program, Some(&params)).await?;

// Compile-only check (no evaluation)
let compiled = session.compile_locy(program)?;

// Explain a Locy program
let explain = session.explain_locy(program).await?;
```

## Fluent Builder (`LocyBuilder`)

For advanced configuration, use the builder API:

```rust
let result = session.locy_with(program)
    .param("threshold", 0.5)
    .timeout(60.0)
    .max_iterations(500)
    .with_config(&cfg)
    .run()
    .await?;
```

Builder methods:

| Method | Description |
|--------|-------------|
| `.param(name, value)` | Add a named parameter |
| `.params(map)` | Add multiple parameters |
| `.timeout(seconds)` | Set evaluation timeout |
| `.max_iterations(n)` | Set recursion iteration cap |
| `.with_config(&cfg)` | Set full `LocyConfig` options |
| `.cancellation_token(token)` | Attach a cancellation token |
| `.run()` | Execute and return `LocyResult` |

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
- `top_k_proofs`

## Result Shape (`LocyResult`)

- `derived: HashMap<String, Vec<Row>>`
- `stats: LocyStats`
- `command_results: Vec<CommandResult>`
- `warnings: Vec<RuntimeWarning>`
- `approximate_groups: HashMap<String, Vec<String>>`
- `derived_fact_set: DerivedFactSet` — opaque fact set for `tx.apply()` materialization

Helpers:

- `result.rows()`
- `result.columns()`
- `result.stats()`
- `result.warnings()`
- `result.has_warning(...)`
- `result.derived_facts(rule_name)`
- `result.iterations`

## Explain Output (`LocyExplainOutput`)

```rust
let explain = session.explain_locy(program).await?;
```

Returns the logical plan and compilation details for a Locy program without executing it.

## Compile-Only (`CompiledProgram`)

```rust
let compiled = session.compile_locy(program)?;
println!("strata: {}, rules: {}", compiled.num_strata(), compiled.num_rules());
println!("rule names: {:?}", compiled.rule_names());
```

Validates the program structure and returns metadata without evaluation.

## Full API References

- [Main Rust API Reference](../../reference/rust-api.md)
- [Internals: Compiler](../../internals/locy/compiler.md)
