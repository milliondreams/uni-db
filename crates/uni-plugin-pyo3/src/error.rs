//! Error types for the PyO3 loader.
//!
//! Mirrors the shape of `uni_plugin_rhai::RhaiError` and
//! `uni_plugin_extism::ExtismError` so the five loaders surface
//! comparable failure modes.

use thiserror::Error;

use uni_plugin::PluginError;

/// Errors specific to the PyO3 loader.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PyPluginError {
    /// The Python callable's signature was not recognized (unknown type
    /// name in `args` / `returns`, or the callable could not be resolved
    /// by name in the module namespace).
    #[error("python callable signature unrecognized: {0}")]
    SignatureUnrecognized(String),

    /// A Python `Exception` was raised inside a plugin callable.
    /// `traceback` carries the formatted Python traceback string.
    #[error("python exception in `{qname}`: {message}\n{traceback}")]
    PythonException {
        /// Qualified plugin function name.
        qname: String,
        /// `repr(e)` of the Python exception.
        message: String,
        /// Formatted Python traceback (best effort).
        traceback: String,
    },

    /// The plugin's declared manifest did not pass validation (missing
    /// required field, unknown type name, duplicate qname).
    #[error("python plugin manifest invalid: {0}")]
    ManifestInvalid(String),

    /// The host's `PluginRegistrar` rejected one of our adapters
    /// (capability missing, qname namespace violation, duplicate).
    #[error("python plugin registrar rejected: {0}")]
    RegistrarRejected(String),

    /// Conversion between an Arrow array and a PyArrow object (via the
    /// PyCapsule C Data Interface) failed: unsupported type, capsule
    /// shape mismatch, or null pointer.
    #[error("arrow <-> pyarrow conversion failure: {0}")]
    ArrowConversion(String),

    /// Internal / unexpected error.
    #[error("uni-plugin-pyo3 internal error: {0}")]
    Internal(String),
}

#[cfg(feature = "pyo3")]
impl From<pyo3::PyErr> for PyPluginError {
    fn from(err: pyo3::PyErr) -> Self {
        use pyo3::types::{PyAnyMethods, PyTracebackMethods};

        // Best-effort traceback capture under the GIL; if the GIL is not
        // held this falls back to `display`.
        pyo3::Python::attach(|py| {
            let traceback = err
                .traceback(py)
                .and_then(|tb| tb.format().ok())
                .unwrap_or_default();
            let value = err.value(py);
            let message = value
                .repr()
                .map(|r| r.to_string())
                .unwrap_or_else(|_| err.to_string());
            PyPluginError::PythonException {
                qname: String::from("<unknown>"),
                message,
                traceback,
            }
        })
    }
}

#[cfg(feature = "pyo3")]
impl PyPluginError {
    /// Tag the embedded `<unknown>` qname with a specific qname when the
    /// caller knows which plugin fn was running.
    #[must_use]
    pub fn with_qname(self, qname: impl Into<String>) -> Self {
        match self {
            PyPluginError::PythonException {
                qname: _,
                message,
                traceback,
            } => PyPluginError::PythonException {
                qname: qname.into(),
                message,
                traceback,
            },
            other => other,
        }
    }
}

impl From<PluginError> for PyPluginError {
    fn from(err: PluginError) -> Self {
        match err {
            PluginError::DuplicateRegistration(q) => {
                PyPluginError::RegistrarRejected(format!("duplicate registration: {q}"))
            }
            PluginError::CapabilityRequired(c) => {
                PyPluginError::RegistrarRejected(format!("registrar caps missing: {c:?}"))
            }
            other => PyPluginError::Internal(format!("registrar: {other}")),
        }
    }
}
