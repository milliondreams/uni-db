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
    /// When true, MNOR/MPROD reject values outside [0,1] with an error instead
    /// of clamping. When false (default), values are clamped silently.
    pub strict_probability_domain: bool,
    /// Underflow threshold for MPROD log-space switch (spec §5.3).
    ///
    /// When the running product drops below this value, `product_f64`
    /// switches to log-space accumulation to prevent floating-point
    /// underflow.
    pub probability_epsilon: f64,
    /// When true, groups flagged with shared probabilistic dependencies use
    /// exact BDD-based probability computation instead of the independence
    /// assumption (MNOR/MPROD). Defaults to false (independence mode).
    pub exact_probability: bool,
    /// Maximum number of BDD variables (unique base facts) allowed per
    /// aggregation group. If a group exceeds this limit, it falls back to
    /// the independence-mode result and emits a `BddLimitExceeded` warning.
    pub max_bdd_variables: usize,
    /// Top-k proof filtering (Scallop, Huang et al. 2021): retain at most k
    /// proofs per derived fact, ranked by proof probability. When 0 (default),
    /// all proofs are retained (unlimited mode).
    pub top_k_proofs: usize,
    /// Override `top_k_proofs` during training. When `Some(k)`, training
    /// evaluation uses this value instead of `top_k_proofs`. When `None`,
    /// `top_k_proofs` is used for both training and inference.
    pub top_k_proofs_training: Option<usize>,
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
            strict_probability_domain: false,
            probability_epsilon: 1e-15,
            exact_probability: false,
            max_bdd_variables: 1000,
            top_k_proofs: 0,
            top_k_proofs_training: None,
        }
    }
}
