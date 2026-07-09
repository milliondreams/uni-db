//! Repro (BLOCKED) for crates/uni-tck/src/world.rs:320 (collect_ids).
//!
//! `collect_ids` and `collect_property_snapshot` guard the introspection query
//! with `if let Ok(result) = self.db().session().query(query).await` and, on the
//! `Err` branch, silently return the empty set/map initialized at the top of the
//! function instead of propagating the error. Their callers
//! (`capture_state_before` / `capture_state_after`) already return
//! `anyhow::Result<()>`, and the sibling `get_labels` propagates with `?`, so
//! propagation was clearly intended. A swallowed before-capture error yields an
//! empty baseline, making every surviving entity look newly created.
//!
//! Why this is BLOCKED as a runtime repro:
//!   * `collect_ids` / `collect_property_snapshot` are PRIVATE (not `pub`), so
//!     they cannot be called from an external `tests/` binary.
//!   * The queries they run (`MATCH (n) RETURN id(n) AS id`,
//!     `MATCH ()-[r]->() RETURN id(r) AS id`, `MATCH (n) RETURN n`) are trivial
//!     hard-coded strings that never error against a healthy in-memory db.
//!   * There is no public seam to inject a query-engine failure into the
//!     internal introspection query without editing production source
//!     (forbidden here).
//!
//! Demonstrating the defect therefore requires either (a) a `#[cfg(test)]`
//! unit test inside `world.rs` that stubs `session().query()` to return `Err`,
//! or (b) a production change swapping `if let Ok(...)` for `?`-propagation and
//! widening the fn signatures to `anyhow::Result<HashSet<u64>>` /
//! `anyhow::Result<HashMap<...>>` (matching `get_labels`). Both are out of
//! scope for an additive external repro.
//!
//! The static reading is confirmed CONFIRMED by the verifier; impact is Low
//! (test-harness-only, requires an otherwise-impossible introspection failure).

/// Placeholder that documents the blocked repro; ignored so CI stays green.
#[test]
#[ignore = "repro for src/world.rs:320: collect_ids swallows query errors via `if let Ok(...)`; no public seam to inject an introspection failure (private fn, trivial query) -- BLOCKED"]
fn collect_ids_swallows_query_error_blocked() {
    // Nothing to assert at runtime: the buggy branch is unreachable without a
    // forced introspection-query failure, which the public API cannot produce.
    // See the module doc comment for the confirmed static analysis.
}
