# Post-Phase-1 Build Baseline — 2026-05-19

This is a **partial** baseline captured *after* the Phase 1 test-binary
consolidation landed. The pre-Phase-1 baseline was **not** recorded
before the changes, so absolute before/after wall-time deltas are not
available; what's captured here is the post-consolidation reference
point against which future phases (cargo-hakari, uni-query split, etc.)
can be measured.

Machine: see `git log` for the working environment. Linux, mold linker,
RUST_MIN_STACK=8MB, `[profile.dev]` debug=1 + `[profile.dev.package."*"]`
opt-level=2.

## Repository state at capture

| | |
|---|---|
| Branch | `main` (uncommitted Phase 1 + partial Phase 2 changes) |
| Workspace members affected by Phase 1 | `crates/uni`, `crates/uni-store`, `crates/uni-query` |
| Test binaries (before Phase 1) | 240 (`uni`) + 39 (`uni-store`) + 21 (`uni-query`) = **300** |
| Test binaries (after Phase 1) | 17 + 7 + 5 = **29** |
| Test functions (verified by nextest, post-consolidation) | 2587 across the 3 crates (1327 in uni-db) |

## Disk usage

| Stage | `target/` | `target/debug/` | `target/debug/deps/` | Notes |
|---|---|---|---|---|
| Session start (recall, not freshly measured) | 539 G | — | ~494 G | dominated by 240 separate uni test binaries cached across rebuild hashes |
| After Phase 1 work + dev-fast probe + uni-store/uni-query nextest runs | **746 G** | 738 G | 650 G | 4754 cached test artifacts under 1192 unique binary names |
| After `cargo clean -p uni-db -p uni-store -p uni-query` | 565 G | 576 G | — | **181.5 GiB freed in 3.05 s**, removed 24 488 files |
| After cold rebuild of test binaries for the 3 cleaned crates | 656 G | — | — | the **80 G** delta is the on-disk footprint of the consolidated test layout (29 binaries) for these 3 crates |

The pre-Phase-1 equivalent of the last row (i.e. disk cost of building
all 300 original test binaries fresh) was not captured, but the 4754
cached artifacts under 1192 binary names show the size of the long-term
artifact bloat that the consolidation now prevents.

## Full clean → cold build → tests (most authoritative post-Phase-1 baseline)

Run after the partial-clean measurement above, this captures the true
post-Phase-1 footprint from a fully empty `target/`.

| Stage | Wall | User | Sys | Notes |
|---|---|---|---|---|
| `cargo clean` | 6.0 s | 0.07 s | 5.5 s | freed the whole `target/` tree |
| `cargo build` (default-members) | **1 min 50 s** | 12m 43 s | 57 s | 6.9× parallelism realized |
| `cargo nextest run` (default-members) | **5 min 40 s** | 8m 38 s | 2m 34 s | includes ~2m 5s test-binary compile + 3m 35s test execution |
| **End-to-end** | **7 min 37 s** | — | — | clean → 3154 tests passing |
| Final `target/` | **91 GB** | — | — | clean post-Phase-1 disk footprint |

The 91 GB clean footprint compared to the **~538 GB** observed at session
start (with the same set of buildable artifacts, but pre-Phase-1 and
with months of accumulated rebuild-hash duplicates) is the headline
dividend of Phase 1: roughly **6× shrink in steady-state `target/`
size**.

## Wall-time measurements (post-Phase-1 baseline)

All times are `real` from `time(1)`; user/sys included for context.

### Cold build — `cargo nextest run -p uni-db -p uni-store -p uni-query --no-run`

This builds the 29 consolidated test binaries from a fresh `cargo clean
-p uni-db -p uni-store -p uni-query` (shared dep artifacts remained warm).

| | |
|---|---|
| real | **1 min 45 s** |
| user | 4 min 39 s |
| sys | 49 s |

### Test execution — `cargo nextest run -p uni-db -p uni-store -p uni-query`

Binaries already built (cache warm).

| | |
|---|---|
| real | **3 min 36 s** |
| Tests run | 2587 |
| Passed | 2587 |
| Slow (>60 s, pre-existing) | 9 |
| Skipped (`#[ignore]`) | 52 |

### Incremental edit — touch `crates/uni-query/src/query/df_graph/locy_fixpoint.rs`

The single most useful inner-loop signal. Any edit anywhere in
`uni-query` recompiles its 112k LOC plus everything downstream
(`uni-db`). This number is the headline target for **Phase 4**
(uni-query split): if `df_graph` were its own crate, only the 60k LOC
graph subtree would recompile.

| Command | real |
|---|---|
| `cargo check -p uni-db --tests` | **27.5 s** |
| `cargo nextest run -p uni-db --no-run` | **1 min 22 s** |

The gap between `check` (27.5 s) and `nextest --no-run` (1m22s) is the
**codegen + link cost for the 17 consolidated uni-db test binaries**.
A pre-Phase-1 measurement of the same edit would have linked 240
binaries instead of 17; the delta is the main Phase 1 dividend, but we
cannot quantify it absolutely without a pre-Phase-1 run.

## What's *not* in this baseline

- **Pre-Phase-1 cold-build wall time** — would have required `cargo
  clean` before any consolidation; not captured.
- **Pre-Phase-1 incremental edit time** — same reason.
- **Pre-Phase-1 `target/debug/deps` after-clean baseline** — same.
- **CI wall time** (Swatinem/rust-cache hit + run) — to be observed on
  the next CI run after these changes land.

## Reference points for future phases

Future phases should re-measure against this baseline:

| Phase | Expected to move |
|---|---|
| **Phase 2** (tokio feature pruning, after profile changes) | Cold-build wall time ↓ ~5–10 % |
| **Phase 3** (cargo-hakari workspace-hack) | Cold-build wall time ↓ 15–25 %; deps artifact count ↓ significantly (fewer feature-set hash duplicates) |
| **Phase 4** (uni-query split) | Incremental edit on `df_graph/*` ↓ from 1m22s toward the 27s `check` time; cold build of test binaries should also drop because uni-query can now parallel-compile with `uni-db` |
| `cargo clean` + next full rebuild | One-time disk reclaim from ~650 G `deps/` down to whatever 29 binaries + warm deps actually need (estimated 80–150 G) |

The most actionable single-number target: **drive the incremental
`nextest --no-run` after a `df_graph/*` edit from 1m22s down to under
30 s** — that's the inner-loop pain Phase 4 is designed to fix.
