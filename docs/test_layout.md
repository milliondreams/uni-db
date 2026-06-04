# Workspace Test Layout

This workspace consolidates integration tests into a single binary per crate
(or a small number of categorical binaries for very large crates). Without
the convention, Cargo's default behavior — one binary per file in `tests/` —
turns a routine test workload into dozens of independent rustc + link
cycles, each statically linking the full transitive dep set
(datafusion, lance, candle, ...).

This document codifies the convention so new crates and new test files
follow it without per-author discovery.

## The pattern

For any crate with more than ~3 integration test files, configure
`Cargo.toml` as follows:

```toml
[package]
# ...usual fields...
# Integration tests are consolidated into a single binary (`tests/integration.rs`).
# See `docs/test_layout.md` for the rationale and the workspace convention.
autotests = false

[[test]]
name = "integration"
path = "tests/integration.rs"
```

Then create `tests/integration.rs` with one `mod` declaration per
sibling test file:

```rust
// Consolidated integration-test binary for `<crate>`.
//
// Cargo defaults to one binary per `.rs` file in `tests/`. `autotests = false`
// in `Cargo.toml` disables that auto-discovery; a single `[[test]]` entry
// points here, and every sibling test file is pulled in as a module below.

mod foo_test;
mod bar_test;
mod baz_test;
// ...
```

The sibling `.rs` files stay where they are; nothing about their contents
needs to change. Cargo resolves `mod foo_test;` to `tests/foo_test.rs`
because the integration binary's root (`tests/integration.rs`) lives in
the same directory.

## Why this works

- Cargo treats each `.rs` file directly under `tests/` as its own
  integration-test crate by default. Setting `autotests = false`
  disables that, leaving the test author in explicit control.
- With one consolidated binary, rustc builds and links a single test
  executable instead of N. The dep graph is shared; codegen and link
  costs amortize.
- `cargo test --test integration <pattern>` and
  `cargo nextest run -E 'test(<pattern>)'` still filter by test name.
  Test names gain the module prefix (e.g. `foo_test::it_works`) so
  category filters become `cargo test --test integration foo_test::`.
- The Rust test harness still parallelizes test execution across threads
  within the binary, so wall-clock runtime is dominated by the longest
  test, not the binary count.

## Edge cases

### Feature-gated tests

If a test file's contents only compile when a feature is enabled, put
the inner attribute at the top of the file:

```rust
// tests/lance_only_test.rs
#![cfg(feature = "lance-backend")]

#[test]
fn ... { ... }
```

An inner `#![cfg(...)]` at the head of a module file scopes naturally to
that module — when the feature is off, the module body compiles to
empty. **No separate binary is required.** Avoid carving out a
`tests/integration_<feature>.rs` unless the feature gate is genuinely
cross-cutting at the binary level (rare).

This is what `crates/uni-store` does: nine files share
`#![cfg(feature = "lance-backend")]` at their head, all are `mod`-ed
unconditionally from `tests/integration.rs`, and the test count flips
between 239 (feature on) and 0 (feature off) automatically.

### Cross-cutting lint allowances

Inner attributes other than `cfg` (e.g.
`#![allow(clippy::cloned_ref_to_slice_refs)]`) also scope to the
containing module on a module-file, so you can leave them in the
individual test file and they'll still apply post-consolidation.

If you want them workspace-uniform, hoist them to `tests/integration.rs`.

### Genuinely incompatible feature combinations

`crates/uni` is the precedent for the rare case where one binary can't
cover all feature combinations: M7 (`rhai-plugins`) and M8
(`pyo3-plugins`) tests are kept as separate `[[test]]` entries because
their feature gates are mutually exclusive with the default test build
shape. Use this escape hatch sparingly; per-file `#![cfg(...)]` is
almost always sufficient.

### Categorical binaries for very large crates

`crates/uni` carries 280+ integration test files. To keep individual
binary build times reasonable, it splits into 9 categorical shims —
`integration_api`, `integration_cypher`, `integration_search`, etc. —
each `mod`-ing roughly 30 files. The pattern is the same; only the
fan-out differs. Choose categorical splitting when a single binary's
build time becomes painful in dev, not as a default.

## Migration recipe

To convert an unconverted crate:

1. Inspect the test files for risks before converting:
   ```bash
   # Inner attributes worth migrating or surfacing:
   for f in crates/<crate>/tests/*.rs; do head -10 "$f" | grep -H "^#!" "$f"; done
   # Top-level mod declarations (rare; flag any that could collide):
   grep -nE "^(pub )?mod " crates/<crate>/tests/*.rs | grep -v "mod tests\b"
   # `#[path = \"...\"]` attrs (almost never present in flat layouts):
   grep -n "#\[path" crates/<crate>/tests/*.rs
   ```
2. Edit `Cargo.toml`: add `autotests = false` and one `[[test]]` entry as shown above.
3. Generate `tests/integration.rs` with one `mod <basename>;` per existing
   `tests/<basename>.rs`.
4. Run `cargo nextest list -p <crate>` before and after; the test count
   must match.
5. Run `cargo nextest run -p <crate>` to confirm all tests still pass.

## Crates currently following this convention

| Crate | Layout | Notes |
|---|---|---|
| `uni` | 9 categorical shims + 5 feature-gated standalones | Original; 280 files behind 14 binaries |
| `uni-store` | Single `integration` binary | 40 files behind 1 binary; 9 are lance-gated in-file |
| `uni-query` | Single `integration` binary | 27 files behind 1 binary |

Smaller crates (≤4 integration test files) have not been converted —
the per-file binary count is already small enough that the convention's
overhead would outweigh its benefit. New crates should still adopt the
pattern proactively if test file count is expected to grow.
