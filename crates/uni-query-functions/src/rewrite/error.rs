//! Error types for query rewriting operations

/// Errors that can occur during query rewriting
#[derive(Debug, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum RewriteError {
    /// Function has wrong number of arguments
    #[error("Function arity mismatch: expected {expected} arguments, got {got}")]
    ArityMismatch { expected: usize, got: usize },

    /// Function argument arity is out of expected range
    #[error("Function arity out of range: expected {min}-{max} arguments, got {got}")]
    ArityOutOfRange { min: usize, max: usize, got: usize },

    /// Expected a string literal for property name but got dynamic expression
    #[error("Expected string literal at argument {arg_index}, got dynamic expression")]
    ExpectedStringLiteral { arg_index: usize },

    /// Expected an entity reference (variable) but got different type
    #[error("Expected entity reference at argument {arg_index}, got different type")]
    ExpectedEntityReference { arg_index: usize },

    /// Argument has unexpected type
    #[error("Type error at argument {arg_index}: expected {expected}, got {got}")]
    TypeError {
        arg_index: usize,
        expected: String,
        got: String,
    },

    /// Rewrite rule is not applicable in current context
    #[error("Rewrite not applicable: {reason}")]
    NotApplicable { reason: String },

    /// Internal error during rewrite transformation
    #[error("Transform error: {message}")]
    TransformError { message: String },

    /// Missing required context information
    #[error("Missing required context: {required}")]
    MissingContext { required: String },
}
