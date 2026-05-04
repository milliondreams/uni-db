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
