//! Observability helpers for plugin invocations.
//!
//! Per `docs/proposals/plugin_framework.md` §12.1.1 and
//! `docs/plans/plugin_framework_implementation.md` §4 M11, every plugin
//! call should be wrapped in a `tracing` span that carries:
//!
//! - plugin id + version + abi-major
//! - qualified function name and surface kind
//! - input batch row count / byte count
//! - result status (ok / err)
//!
//! When `tracing-opentelemetry` is configured on the host, these spans
//! ship to whichever OTLP collector the user has wired up — preserving
//! a single `TraceId` across `query → plugin → outbound HTTP`.

use std::time::Duration;

use crate::plugin::PluginId;
use crate::qname::QName;

/// Kind label attached to every plugin-call span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InvocationKind {
    /// Cypher scalar function.
    Scalar,
    /// Cypher aggregate function.
    Aggregate,
    /// Cypher window function.
    Window,
    /// Cypher procedure (`CALL`).
    Procedure,
    /// Locy aggregate (`FOLD`).
    LocyAggregate,
    /// Locy predicate.
    LocyPredicate,
    /// Custom physical operator.
    Operator,
    /// Custom index probe / build.
    Index,
    /// Storage backend operation.
    Storage,
    /// Graph algorithm.
    Algorithm,
    /// CRDT operation.
    Crdt,
    /// Session-lifecycle hook.
    Hook,
    /// Fine-grained trigger.
    Trigger,
    /// Background-job execution.
    BackgroundJob,
    /// Logical-type conversion.
    Type,
    /// Authentication.
    Auth,
    /// Authorization.
    Authz,
    /// Wire-protocol connector.
    Connector,
}

impl InvocationKind {
    /// Stable string identifier (for tracing attributes).
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Aggregate => "aggregate",
            Self::Window => "window",
            Self::Procedure => "procedure",
            Self::LocyAggregate => "locy_aggregate",
            Self::LocyPredicate => "locy_predicate",
            Self::Operator => "operator",
            Self::Index => "index",
            Self::Storage => "storage",
            Self::Algorithm => "algorithm",
            Self::Crdt => "crdt",
            Self::Hook => "hook",
            Self::Trigger => "trigger",
            Self::BackgroundJob => "background_job",
            Self::Type => "type",
            Self::Auth => "auth",
            Self::Authz => "authz",
            Self::Connector => "connector",
        }
    }
}

/// Tracing helper: emit a single structured event for one plugin call.
///
/// Real M11 cutover wraps each invocation in a `tracing::info_span!`
/// using these field names; pre-cutover, this function emits a debug
/// event so the wiring is exercised end-to-end through the existing
/// `tracing` subscriber.
pub fn record_invocation(
    plugin: &PluginId,
    qname: &QName,
    kind: InvocationKind,
    rows: u64,
    elapsed: Duration,
    ok: bool,
) {
    tracing::debug!(
        plugin.id = plugin.as_str(),
        plugin.qname = %qname,
        plugin.kind = kind.as_str(),
        batch.rows = rows,
        duration_ms = elapsed.as_millis() as u64,
        result.ok = ok,
        "plugin.invoke"
    );
}

/// OTel-style trace context extracted from the current span.
///
/// Returned by [`current_trace_context`] for propagation into outbound
/// HTTP calls (W3C `traceparent` header). The bytes are opaque to the
/// caller; the host serializes / deserializes via standard OTel APIs.
#[derive(Clone, Debug, Default)]
pub struct TraceContext {
    /// Trace identifier (16 bytes when populated).
    pub trace_id: Vec<u8>,
    /// Span identifier (8 bytes when populated).
    pub span_id: Vec<u8>,
}

/// Return the current trace context if a `tracing-opentelemetry` layer
/// is configured, else an empty context.
///
/// M11 cutover wires this to the real OTel API; the stub today
/// returns an empty context so callers can integrate the propagation
/// shape without depending on OTel being live.
#[must_use]
pub fn current_trace_context() -> TraceContext {
    TraceContext::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invocation_kind_strings_are_stable() {
        assert_eq!(InvocationKind::Scalar.as_str(), "scalar");
        assert_eq!(InvocationKind::Procedure.as_str(), "procedure");
        assert_eq!(InvocationKind::LocyAggregate.as_str(), "locy_aggregate");
        assert_eq!(InvocationKind::BackgroundJob.as_str(), "background_job");
    }

    #[test]
    fn record_invocation_does_not_panic_without_subscriber() {
        record_invocation(
            &PluginId::new("test"),
            &QName::builtin("identity"),
            InvocationKind::Scalar,
            128,
            Duration::from_micros(50),
            true,
        );
    }

    #[test]
    fn trace_context_default_is_empty() {
        let c = current_trace_context();
        assert!(c.trace_id.is_empty());
        assert!(c.span_id.is_empty());
    }
}
