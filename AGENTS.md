# Repository Guidelines

## Project Structure & Module Organization
- `src/`: main library and binary. Key submodules: `core/` (IDs, schema, snapshots), `storage/` (Lance datasets, LSM deltas), `runtime/` (WAL, L0 buffer, gryf runtime), `query/` (Cypher parser, planner, executor).
- `tests/`: integration tests (`tests/*.rs`).
- `benches/`: Criterion benchmarks (`benches/micro_benchmarks.rs`).
- `docs/`: design and status docs like `DESIGN.md` and `docs/KNOWN_ISSUES.md`.
- `demos/`: runnable demo scripts and walkthroughs.

## Build, Test, and Development Commands
- `cargo build`: compile the project.
- `cargo run`: run the local binary.
- `cargo nextest run`: run all tests (parallel, preferred).
- `cargo nextest run -E 'test(<name>)'`: run a specific test by name.
- `cargo nextest run --run-ignored all`: include ignored/slow perf tests.
- `cargo bench`: run Criterion benchmarks.
- `cargo fmt`: format code.
- `cargo clippy`: lint for common issues.
- Use `cargo nextest` instead of `cargo test` for regular test runs.

## Coding Style & Naming Conventions
- Follow standard Rust style and format with `rustfmt`.
- `snake_case` for modules/functions, `CamelCase` for types/traits, `SCREAMING_SNAKE_CASE` for constants.
- Keep file names aligned with module paths (e.g., `src/runtime/wal.rs`).
- Add brief comments only when logic is not obvious.

## Testing Guidelines
- Unit tests live next to code in `src/**` with `#[cfg(test)] mod tests`.
- Integration tests live in `tests/` and often use `#[tokio::test]`.
- Benchmarks go in `benches/` using Criterion.
- If a test is flaky or sensitive to parallelism, document it in `docs/KNOWN_ISSUES.md`.
- For TCK compliance runs, use `scripts/run_tck_with_report.sh`.
- For filtered TCK subsets, use `scripts/run_tck_with_report.sh "~Match1"` (replace filter as needed).
- TCK run artifacts are written under `target/cucumber/` (results/report) and synced into `compliance_reports/` by mode.

## Commit & Pull Request Guidelines
- Use Conventional Commits as in history: `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`.
- PRs should include a short rationale, tests run, and links to related issues.
- Update `DESIGN.md` and `CYPHER_GAPS.md` when architecture or Cypher support changes.

## Agent Git Safety Rule
- Do not perform any git action unless the user has explicitly instructed it in the current conversation turn.
- This includes all git commands and git-related workflows (for example: `status`, `diff`, `add`, `commit`, `push`, `pull`, `checkout`, `reset`, `merge`, `rebase`, `tag`, `stash`, `cherry-pick`).

## Fork invariants (Phase 2)
- **Registry edits go through `ForkRegistryHandle`.** Never write `catalog/fork_registry.json`, `catalog/fork_schemas/*`, `catalog/fork_tombstones/*`, or `catalog/forks/{fork_id}/id_allocator.json` directly — the 2PC state machine assumes single-writer access through the handle.
- **Fork creation must not hold any global lock during `lance_branch::create_branch`.** The registry mutex covers metadata PUTs only. Spec §10 requires fork creation not to block primary.
- **`lance::Dataset::create_branch` is not idempotent** at the same `parent_version`. Recovery must call `lance_branch::delete_branch` (force-delete wrapper) before re-attempting. Day 1 spike confirmed this contract.
- **Lance compaction retention must not be tightened below the longest live fork chain.** Silent fork-data corruption otherwise. Verified by `crates/uni-store/tests/lance_branch_retention.rs`.
- **Per-fork allocator** lives at `catalog/forks/{fork_id}/id_allocator.json` and is bootstrapped from primary's in-memory HWM at fork creation. Never let it start at 0 against a non-empty primary — Lance read-merge would shadow the fork's writes.
- **Per-fork WAL** lives at `wal_forks/{fork_id}/`, NOT under `wal/` — that prefix collides with primary's listing under recursive `ObjectStore::list`. See `crates/uni-store/src/fork/wal.rs` rustdoc.
- **Same-fork sessions share an `Arc<UniInner>`** via `UniInner.fork_inners` (cached as `Weak`). A commit on one session is immediately visible to siblings — preserve this when refactoring `ForkBuilder::build` or `Uni::drop_fork`.
- **`Uni::drop_fork` must check `inflight_tx_count` before `begin_drop`.** The registry transition to Tombstoned cannot be cleanly rolled back; surfacing `ForkInflightTx` early is the contract. The counter is on `UniInner` and paired with `Transaction::new` / `Transaction::drop` (incremented and decremented unconditionally).
- **On-the-fly fork dataset creation must keep main empty.** `BranchedBackend::ensure_branch_for_new` materializes an empty parent on `main`, branches from it, then writes the actual batches to the branch. Calling `create_dataset_then_branch` directly with the batches would leak fork data into primary's view.
- **Fork compaction is deferred to Phase 5.** Long-lived heavy-write forks should be `drop_fork`-and-recreate. The `uni_fork_l1_flushes` gauge plus `fork_fragment_warn_threshold` warn surface the risk operationally.
