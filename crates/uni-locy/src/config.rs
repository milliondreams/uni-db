use std::time::Duration;

/// Configuration for the Locy orchestrator.
#[derive(Debug, Clone)]
pub struct LocyConfig {
    /// Maximum fixpoint iterations per recursive stratum.
    pub max_iterations: usize,
    /// Overall evaluation timeout.
    pub timeout: Duration,
    /// Maximum recursion depth for EXPLAIN derivation trees.
    pub max_explain_depth: usize,
    /// Maximum recursion depth for SLG resolution.
    pub max_slg_depth: usize,
    /// Maximum candidate modifications to generate during ABDUCE.
    pub max_abduce_candidates: usize,
    /// Maximum validated results to return from ABDUCE.
    pub max_abduce_results: usize,
    /// Maximum bytes of derived facts to hold in memory per relation.
    pub max_derived_bytes: usize,
    /// When true, BEST BY applies a secondary sort on remaining columns for
    /// deterministic tie-breaking. Set to false for non-deterministic (faster)
    /// selection.
    pub deterministic_best_by: bool,
}

impl Default for LocyConfig {
    fn default() -> Self {
        Self {
            max_iterations: 1000,
            timeout: Duration::from_secs(300),
            max_explain_depth: 100,
            max_slg_depth: 1000,
            max_abduce_candidates: 20,
            max_abduce_results: 10,
            max_derived_bytes: 256 * 1024 * 1024,
            deterministic_best_by: true,
        }
    }
}
