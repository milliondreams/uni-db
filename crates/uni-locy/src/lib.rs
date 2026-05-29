pub mod calibration;
pub mod compiler;
pub mod config;
pub mod dependency_dnf;
pub mod errors;
pub mod neural;
pub mod result;
pub mod semiring;
pub mod top_k_proofs;
pub mod types;

pub use calibration::{
    BetaCalibration, BetaFitter, CalibrationError, CalibrationMethodKind, Calibrator,
    CalibratorFitter, CredalCalibrator, DirichletCalibrator, DirichletFitter,
    EnsembleVarianceCalibrator, IdentityCalibrator, IsotonicFitter, IsotonicRegression,
    MulticlassCalibrator, MulticlassCalibratorFitter, PlattFitter, PlattScaling, TemperatureFitter,
    TemperatureScaling, accuracy, auc, brier_score, debiased_ece, expected_calibration_error,
    log_loss,
};
pub use compiler::compile;
pub use compiler::compile_with_config;
pub use compiler::compile_with_external_rules;
pub use compiler::compile_with_external_rules_and_config;
pub use compiler::compile_with_modules;
pub use compiler::compile_with_oracle;
pub use compiler::errors::LocyCompileError;
pub use compiler::modules::ModuleContext;
pub use compiler::{MonotonicityOracle, default_monotonicity_oracle};
pub use config::{ClassifierRegistry, ConfigError, LocyConfig};
pub use dependency_dnf::{BaseRv, BaseRvSet, DependencyDnf};
pub use errors::LocyError;
pub use neural::{
    CalibratedClassifier, CandleLinearClassifier, ClassifierError, ClassifierResult, ClassifyInput,
    FeatureValue, MockClassifier, ModelInvocationCache, NeuralClassifier, NeuralProvenanceRecord,
    NeuralProvenanceStore,
};
pub use result::{
    AbductionResult, CalibrationResult, CommandResult, ConfidenceBand, ConfidenceSource,
    DerivationNode, DerivedEdge, DerivedFactSet, FactRow, LocyResult, LocyStats, Modification,
    NeuralProvenance, ValidatedModification, ValidationResult,
};
pub use semiring::merge_top_k_dispatch_owned as merge_top_k_runtime;
pub use semiring::{
    AddMultProb, LocySemiring, MaxMinProb, ResolvedSemiringConfig, SemiringDispatch, SemiringError,
};
pub use top_k_proofs::{NeuralCallId, Proof, PruneNotice, TopKProofs, TopKTag};
pub use types::{
    CompiledCalibrate, CompiledCommand, CompiledInputBinding, CompiledModel, CompiledProgram,
    CompiledValidate, ModelInvocation, RuntimeWarning, RuntimeWarningCode, SemiringKind,
};
