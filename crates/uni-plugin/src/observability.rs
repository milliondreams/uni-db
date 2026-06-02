//! Observability helpers for plugin invocations.
//!
//! Every plugin call should be wrapped in a `tracing` span that carries:
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
/// HTTP calls (W3C `traceparent` header). The IDs are kept as opaque bytes so
/// this type does not leak a specific OTel SDK version into the ABI crate; use
/// [`TraceContext::to_traceparent`] to render the wire form.
///
/// `#[non_exhaustive]` so future fields (e.g. `tracestate`) can be added
/// without breaking downstream struct-literal construction.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct TraceContext {
    /// Trace identifier (16 bytes when populated, empty otherwise).
    pub trace_id: Vec<u8>,
    /// Span identifier (8 bytes when populated, empty otherwise).
    pub span_id: Vec<u8>,
    /// W3C trace flags (bit 0 = sampled). Zero when no context is present.
    pub trace_flags: u8,
}

impl TraceContext {
    /// Render the context as a W3C `traceparent` header value.
    ///
    /// Returns `None` unless both IDs are present and correctly sized — i.e.
    /// there is a real context to propagate. The format is
    /// `00-<32 hex trace-id>-<16 hex span-id>-<2 hex flags>`.
    #[must_use]
    pub fn to_traceparent(&self) -> Option<String> {
        use std::fmt::Write as _;
        if self.trace_id.len() != 16 || self.span_id.len() != 8 {
            return None;
        }
        // "00-" + 32 + "-" + 16 + "-" + 2 = 55 chars.
        let mut s = String::with_capacity(55);
        s.push_str("00-");
        for b in &self.trace_id {
            let _ = write!(s, "{b:02x}");
        }
        s.push('-');
        for b in &self.span_id {
            let _ = write!(s, "{b:02x}");
        }
        let _ = write!(s, "-{:02x}", self.trace_flags);
        Some(s)
    }
}

/// Return the current trace context, or an empty context when none is active.
///
/// With the `otel` feature enabled, this reads the [`opentelemetry`]
/// `SpanContext` bridged onto the current `tracing` span by a
/// `tracing-opentelemetry` layer. It returns an empty context (no leak) when
/// the feature is off, no such layer is installed, or the current span has no
/// valid context. The OTLP exporter pipeline is orthogonal — installing a layer
/// is enough; an exporter is not required.
#[must_use]
pub fn current_trace_context() -> TraceContext {
    #[cfg(feature = "otel")]
    {
        use opentelemetry::trace::TraceContextExt as _;
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        let span = tracing::Span::current();
        let ctx = span.context();
        let span_ref = ctx.span();
        let sc = span_ref.span_context();
        if sc.is_valid() {
            return TraceContext {
                trace_id: sc.trace_id().to_bytes().to_vec(),
                span_id: sc.span_id().to_bytes().to_vec(),
                trace_flags: sc.trace_flags().to_u8(),
            };
        }
    }
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
    fn trace_context_empty_without_otel_layer() {
        // No `tracing-opentelemetry` layer installed in this test, so even an
        // `otel`-enabled build must return an empty context — the no-leak
        // invariant (we never fabricate a trace id).
        let c = current_trace_context();
        assert!(c.trace_id.is_empty());
        assert!(c.span_id.is_empty());
        assert!(c.to_traceparent().is_none());
    }

    #[test]
    fn empty_context_has_no_traceparent() {
        assert!(TraceContext::default().to_traceparent().is_none());
    }

    /// With a real (no-exporter) OTel tracer + `tracing-opentelemetry` layer
    /// installed, `current_trace_context` extracts a valid 16/8-byte context
    /// and renders a well-formed `traceparent`. Exercises the bridge in CI
    /// without any OTLP collector.
    #[cfg(feature = "otel")]
    #[test]
    fn current_trace_context_extracts_valid_context_under_otel_layer() {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::prelude::*;

        let provider = opentelemetry_sdk::trace::TracerProvider::builder().build();
        let tracer = provider.tracer("uni-plugin-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("otel-test-span");
            let _enter = span.enter();
            let c = current_trace_context();
            assert_eq!(c.trace_id.len(), 16, "trace id should be 16 bytes");
            assert_eq!(c.span_id.len(), 8, "span id should be 8 bytes");
            let tp = c
                .to_traceparent()
                .expect("a valid context renders a traceparent");
            assert!(tp.starts_with("00-"), "traceparent: {tp}");
            // 00- (3) + trace (32) + - (1) + span (16) + - (1) + flags (2)
            assert_eq!(tp.len(), 55, "traceparent: {tp}");
        });
    }
}
