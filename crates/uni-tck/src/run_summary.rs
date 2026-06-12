//! Run-summary helpers shared by the nextest TCK harness.
//!
//! These are pure functions kept in the library (rather than the `harness = false`
//! integration-test binary) so they can be unit-tested directly via `cargo test
//! --lib`. The harness binary itself has a custom `main` and does not run inline
//! `#[cfg(test)]` modules.

/// Reports whether a TCK run executed zero scenarios.
///
/// A scenario count of zero (no passed, failed, or skipped scenarios) means the
/// scenario filter matched nothing or the feature failed to load. openCypher TCK
/// semantics require every declared scenario to actually run and be asserted, so
/// callers treat an empty run as a hard failure rather than a silent pass.
///
/// # Examples
///
/// ```
/// use uni_tck::run_summary::scenario_count_is_empty;
/// assert!(scenario_count_is_empty(0));
/// assert!(!scenario_count_is_empty(1));
/// ```
#[must_use]
pub fn scenario_count_is_empty(total_scenarios: usize) -> bool {
    total_scenarios == 0
}

#[cfg(test)]
mod tests {
    use super::scenario_count_is_empty;

    /// A run that executed zero scenarios must be treated as a failure.
    ///
    /// Mirrors the guard in the nextest harness's `run_single_scenario`: when a
    /// filter matches nothing (or a feature fails to load) the cucumber scenario
    /// count is zero, which must surface as a hard failure, not a silent pass.
    #[test]
    fn zero_scenarios_run_is_a_failure() {
        // 0 scenarios => empty run => failure.
        assert!(
            scenario_count_is_empty(0),
            "a run with 0 scenarios must be flagged as empty (failed)"
        );
        // At least one scenario ran => not an empty run.
        assert!(
            !scenario_count_is_empty(1),
            "a run with >=1 scenario must NOT be flagged as empty"
        );
        assert!(!scenario_count_is_empty(42));
    }
}
