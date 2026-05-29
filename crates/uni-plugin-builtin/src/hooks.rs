//! Built-in session-hook registrations.
//!
//! M5e scaffolding: ships a no-op `LoggingHook` that emits a `tracing`
//! span at each phase. Real audit hooks (query-cost accounting, slow-
//! query logging, kill-switch enforcement) arrive in M5e cutover.

use std::sync::Arc;

use uni_plugin::errors::HookOutcome;
use uni_plugin::traits::hook::{
    AbortContext, AnalyzeContext, CommitContext, ExecuteContext, ParseContext, PlanContext,
    QueryMetrics, SessionHook,
};
use uni_plugin::{PluginError, PluginRegistrar};

/// Register the built-in hooks.
///
/// # Errors
///
/// Returns [`PluginError`] on capability gating failure.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.hook(Arc::new(LoggingHook))?;
    Ok(())
}

/// Emits a `tracing::debug!` event at each session-lifecycle phase.
///
/// Useful as a starting template for real audit hooks; the M5e cutover
/// replaces this with a `query.audit` hook that records to
/// `uni_system.audit_log`.
#[derive(Debug)]
pub struct LoggingHook;

impl SessionHook for LoggingHook {
    fn on_parse(&self, ctx: &ParseContext<'_>) -> HookOutcome {
        tracing::debug!(
            session_id = ctx.session_id,
            source_len = ctx.source.len(),
            "uni.hook.on_parse"
        );
        HookOutcome::Continue
    }
    fn on_analyze(&self, ctx: &AnalyzeContext<'_>) -> HookOutcome {
        tracing::debug!(session_id = ctx.session_id, "uni.hook.on_analyze");
        HookOutcome::Continue
    }
    fn on_plan(&self, ctx: &PlanContext<'_>) -> HookOutcome {
        tracing::debug!(session_id = ctx.session_id, "uni.hook.on_plan");
        HookOutcome::Continue
    }
    fn on_execute_start(&self, ctx: &ExecuteContext<'_>) -> HookOutcome {
        tracing::debug!(session_id = ctx.session_id, "uni.hook.on_execute_start");
        HookOutcome::Continue
    }
    fn on_execute_end(&self, ctx: &ExecuteContext<'_>, metrics: &QueryMetrics) {
        tracing::debug!(
            session_id = ctx.session_id,
            elapsed_ms = metrics.elapsed.as_millis() as u64,
            rows_out = metrics.rows_out,
            bytes_read = metrics.bytes_read,
            "uni.hook.on_execute_end"
        );
    }
    fn before_commit(&self, ctx: &CommitContext<'_>) -> HookOutcome {
        tracing::debug!(session_id = ctx.session_id, "uni.hook.before_commit");
        HookOutcome::Continue
    }
    fn after_commit(&self, ctx: &CommitContext<'_>) {
        tracing::debug!(session_id = ctx.session_id, "uni.hook.after_commit");
    }
    fn on_abort(&self, ctx: &AbortContext<'_>) {
        tracing::debug!(
            session_id = ctx.session_id,
            reason = ctx.reason,
            "uni.hook.on_abort"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logging_hook_on_parse_returns_continue() {
        let h = LoggingHook;
        let ctx = ParseContext::new("MATCH (n) RETURN n", "s");
        assert!(matches!(h.on_parse(&ctx), HookOutcome::Continue));
    }

    #[test]
    fn logging_hook_phase_methods_dont_panic() {
        let h = LoggingHook;
        let p = ParseContext::new("x", "s");
        let a = AnalyzeContext::new("s");
        let pl = PlanContext::new("s");
        let e = ExecuteContext::new("s");
        let c = CommitContext::new("s");
        let ab = AbortContext::new("s", "test");

        let _ = h.on_parse(&p);
        let _ = h.on_analyze(&a);
        let _ = h.on_plan(&pl);
        let _ = h.on_execute_start(&e);
        h.on_execute_end(&e, &QueryMetrics::default());
        let _ = h.before_commit(&c);
        h.after_commit(&c);
        h.on_abort(&ab);
    }
}
