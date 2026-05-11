# Fork TCK Coverage

Phase 1 forks are exercised by Rust integration tests at `crates/uni/tests/`:

- `fork_read_only.rs` — full lifecycle, snapshot isolation, restart preservation,
  `.new_()` semantics, ForkInUse / ForkNotFound, list/info round-trip.
- `fork_creation_concurrency.rs` — E4 verification (concurrent creates).
- `fork_no_primary_blocking.rs` — spec §10 invariant.

These provide tighter coverage than Cucumber would; the integration tests assert
typed error variants and exact session-handle behavior that Gherkin's string-
matching can't express. A Cucumber feature is intentionally deferred to a
follow-up (Phase 4 will need it for the watch / hook / TTL contracts that are
harder to assert in plain Rust tests).

## Phase 2 and Phase 3 — coverage still in Rust integration tests

Phase 2 (writable forks) and Phase 3 (nested forks) doubled down on the same
rationale: their new surface area is dominated by typed error payloads —
`ForkInUse { holder_count }`, `ForkInflightTx { name }`, `ForkHasChildren
{ children: Vec<String> }`, `ForkSubtreeInUse { blockers: Vec<String> }` —
plus session-handle lifecycle behavior (the `UniInner` cache, holder counts,
in-flight transaction tracking). Gherkin's string-match assertions can't
faithfully express these.

Coverage for both phases lives at `crates/uni/tests/`:

- Phase 2: `fork_writes`, `fork_concurrent_writers`, `fork_new_label`,
  `fork_drop_inflight`, `fork_locy_rules`, `fork_fragment_warn`,
  `fork_flush_known_labels`, `fork_strict_schema`, `fork_writes_soak`
  (ignored).
- Phase 3: `fork_nested` (7 tests covering depth chain, snapshot isolation,
  sibling isolation, child guard, cascade), `fork_nested_perf` (ignored
  depth-5 latency), `fork_nested_recovery` (ignored; nested-create crash and
  cascade-completes-despite-delete-errors).

A Cucumber feature stays parked for Phase 4 when the watch / hook / TTL
surface lands — those *do* lend themselves to scenario-style assertions.
