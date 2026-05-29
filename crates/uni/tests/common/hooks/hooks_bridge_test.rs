#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5e bridge tests — `LegacyHookAdapter` routes legacy `SessionHook`
//! callbacks onto the phased `uni_plugin::traits::hook::SessionHook`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use uni_common::Result;
use uni_db::api::hooks::{
    CommitHookContext, HookContext, LegacyHookAdapter, QueryType, SessionHook,
};
use uni_db::api::transaction::CommitResult;
use uni_plugin::errors::HookOutcome;
use uni_plugin::traits::hook::{
    AbortContext, AnalyzeContext, CommitContext, ExecuteContext, ParseContext, PlanContext,
    QueryMetrics, SessionHook as PluginSessionHook,
};
use uni_query::QueryMetrics as LegacyQueryMetrics;

/// Test hook that counts callback invocations and lets each tier
/// (`before_query` / `before_commit`) be configured to reject.
#[derive(Default)]
struct CountingHook {
    before_query: AtomicU32,
    after_query: AtomicU32,
    before_commit: AtomicU32,
    after_commit: AtomicU32,
    last_total_time_micros: AtomicU32,
    reject_before_query: bool,
    reject_before_commit: bool,
}

impl SessionHook for CountingHook {
    fn before_query(&self, _ctx: &HookContext) -> Result<()> {
        self.before_query.fetch_add(1, Ordering::SeqCst);
        if self.reject_before_query {
            Err(uni_common::UniError::HookRejected {
                message: "test rejection in before_query".into(),
            })
        } else {
            Ok(())
        }
    }
    fn after_query(&self, _ctx: &HookContext, metrics: &LegacyQueryMetrics) {
        self.after_query.fetch_add(1, Ordering::SeqCst);
        self.last_total_time_micros
            .store(metrics.total_time.as_micros() as u32, Ordering::SeqCst);
    }
    fn before_commit(&self, _ctx: &CommitHookContext) -> Result<()> {
        self.before_commit.fetch_add(1, Ordering::SeqCst);
        if self.reject_before_commit {
            Err(uni_common::UniError::HookRejected {
                message: "test rejection in before_commit".into(),
            })
        } else {
            Ok(())
        }
    }
    fn after_commit(&self, _ctx: &CommitHookContext, _result: &CommitResult) {
        self.after_commit.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn bridge_routes_before_query_through_on_parse() {
    let counter: Arc<CountingHook> = Arc::new(CountingHook::default());
    let adapter = LegacyHookAdapter::new("counter", counter.clone() as Arc<dyn SessionHook>);

    let ctx = ParseContext::new("MATCH (n) RETURN n", "session-1");
    let outcome = adapter.on_parse(&ctx);

    assert!(matches!(outcome, HookOutcome::Continue));
    assert_eq!(counter.before_query.load(Ordering::SeqCst), 1);
}

#[test]
fn bridge_propagates_before_query_rejection_as_phased_reject() {
    let hook = CountingHook {
        reject_before_query: true,
        ..Default::default()
    };
    let adapter = LegacyHookAdapter::new("rejecter", Arc::new(hook));

    let ctx = ParseContext::new("MATCH (n) RETURN n", "s");
    let outcome = adapter.on_parse(&ctx);

    match outcome {
        HookOutcome::Reject { reason } => {
            assert!(reason.contains("before_query"), "got reason: {reason}");
        }
        other => panic!("expected Reject, got {other:?}"),
    }
}

#[test]
fn bridge_routes_after_query_through_on_execute_end_with_metrics() {
    let counter: Arc<CountingHook> = Arc::new(CountingHook::default());
    let adapter = LegacyHookAdapter::new("c", counter.clone() as Arc<dyn SessionHook>);

    let exec_ctx = ExecuteContext::new("session-2");
    let metrics = QueryMetrics {
        elapsed: std::time::Duration::from_micros(1500),
        rows_out: 42,
        bytes_read: 1024,
    };
    adapter.on_execute_end(&exec_ctx, &metrics);

    assert_eq!(counter.after_query.load(Ordering::SeqCst), 1);
    assert_eq!(counter.last_total_time_micros.load(Ordering::SeqCst), 1500);
}

#[test]
fn bridge_propagates_before_commit_rejection() {
    let hook = CountingHook {
        reject_before_commit: true,
        ..Default::default()
    };
    let adapter = LegacyHookAdapter::new("r", Arc::new(hook));

    let ctx = CommitContext::new("s");
    match adapter.before_commit(&ctx) {
        HookOutcome::Reject { .. } => {}
        other => panic!("expected Reject, got {other:?}"),
    }
}

#[test]
fn bridge_calls_after_commit_with_zeroed_commit_result_stub() {
    let counter: Arc<CountingHook> = Arc::new(CountingHook::default());
    let adapter = LegacyHookAdapter::new("c", counter.clone() as Arc<dyn SessionHook>);

    let ctx = CommitContext::new("s");
    adapter.after_commit(&ctx);

    assert_eq!(counter.after_commit.load(Ordering::SeqCst), 1);
}

#[test]
fn bridge_unmapped_phases_pass_through_as_continue() {
    let counter: Arc<CountingHook> = Arc::new(CountingHook::default());
    let adapter = LegacyHookAdapter::new("c", counter.clone() as Arc<dyn SessionHook>);

    let analyze = AnalyzeContext::new("s");
    let plan = PlanContext::new("s");
    let exec = ExecuteContext::new("s");
    let abort = AbortContext::new("s", "test");

    assert!(matches!(
        adapter.on_analyze(&analyze),
        HookOutcome::Continue
    ));
    assert!(matches!(adapter.on_plan(&plan), HookOutcome::Continue));
    assert!(matches!(
        adapter.on_execute_start(&exec),
        HookOutcome::Continue
    ));
    adapter.on_abort(&abort);

    // None of the legacy callbacks should have been invoked by these
    // phased-only events.
    assert_eq!(counter.before_query.load(Ordering::SeqCst), 0);
    assert_eq!(counter.after_query.load(Ordering::SeqCst), 0);
    assert_eq!(counter.before_commit.load(Ordering::SeqCst), 0);
    assert_eq!(counter.after_commit.load(Ordering::SeqCst), 0);
}

#[test]
fn bridge_preserves_arc_sharing_for_state_observation() {
    // The legacy hook is held by Arc; two adapters wrapping the same
    // hook observe a shared counter. This is the property that lets
    // `Session::add_hook` (legacy path) and a future
    // `PluginRegistrar::hook` registration coexist on the same impl.
    let shared: Arc<CountingHook> = Arc::new(CountingHook::default());
    let a = LegacyHookAdapter::new("a", shared.clone() as Arc<dyn SessionHook>);
    let b = LegacyHookAdapter::new("b", shared.clone() as Arc<dyn SessionHook>);

    let ctx = ParseContext::new("x", "s");
    let _ = a.on_parse(&ctx);
    let _ = b.on_parse(&ctx);

    assert_eq!(shared.before_query.load(Ordering::SeqCst), 2);
}

#[test]
fn bridge_query_text_surfaces_via_legacy_hook_context() {
    // Verify ParseContext.source flows into HookContext.query_text.
    struct CaptureText {
        captured: parking_lot::Mutex<Option<String>>,
    }
    impl SessionHook for CaptureText {
        fn before_query(&self, ctx: &HookContext) -> Result<()> {
            *self.captured.lock() = Some(ctx.query_text.clone());
            assert_eq!(ctx.query_type, QueryType::Cypher);
            Ok(())
        }
    }
    let hook = Arc::new(CaptureText {
        captured: parking_lot::Mutex::new(None),
    });
    let adapter = LegacyHookAdapter::new("c", hook.clone() as Arc<dyn SessionHook>);

    let ctx = ParseContext::new("RETURN 1", "s");
    let _ = adapter.on_parse(&ctx);

    assert_eq!(hook.captured.lock().clone(), Some("RETURN 1".to_string()));
}

// ============================================================================
// v1.1 phased-context-shape tests (M5e follow-ups #8, #9, #10).
// ============================================================================

#[test]
fn bridge_routes_query_type_locy_through_to_legacy() {
    use uni_plugin::traits::hook::QueryType as PluginQueryType;

    struct CaptureQt {
        captured: parking_lot::Mutex<Option<QueryType>>,
    }
    impl SessionHook for CaptureQt {
        fn before_query(&self, ctx: &HookContext) -> Result<()> {
            *self.captured.lock() = Some(ctx.query_type);
            Ok(())
        }
    }
    let hook = Arc::new(CaptureQt {
        captured: parking_lot::Mutex::new(None),
    });
    let adapter = LegacyHookAdapter::new("c", hook.clone() as Arc<dyn SessionHook>);

    // Locy classification flows through unchanged.
    let ctx = ParseContext::new("RULE r: a <- b.", "s").with_query_type(PluginQueryType::Locy);
    let _ = adapter.on_parse(&ctx);
    assert_eq!(*hook.captured.lock(), Some(QueryType::Locy));

    // Execute classification flows through unchanged.
    let ctx2 = ParseContext::new("CREATE (n)", "s").with_query_type(PluginQueryType::Execute);
    let _ = adapter.on_parse(&ctx2);
    assert_eq!(*hook.captured.lock(), Some(QueryType::Execute));
}

#[test]
fn bridge_routes_params_through_to_legacy() {
    use datafusion::scalar::ScalarValue;
    use smol_str::SmolStr;
    use uni_common::Value;

    struct CaptureParams {
        captured: parking_lot::Mutex<Option<HashMap<String, Value>>>,
    }
    impl SessionHook for CaptureParams {
        fn before_query(&self, ctx: &HookContext) -> Result<()> {
            *self.captured.lock() = Some(ctx.params.clone());
            Ok(())
        }
    }
    let hook = Arc::new(CaptureParams {
        captured: parking_lot::Mutex::new(None),
    });
    let adapter = LegacyHookAdapter::new("c", hook.clone() as Arc<dyn SessionHook>);

    // A 3-param slice covering Int / String / Bool maps cleanly into
    // the legacy `HashMap<String, Value>` shape.
    let params: Vec<(SmolStr, ScalarValue)> = vec![
        (SmolStr::new("limit"), ScalarValue::Int64(Some(42))),
        (
            SmolStr::new("name"),
            ScalarValue::Utf8(Some("ada".to_owned())),
        ),
        (SmolStr::new("active"), ScalarValue::Boolean(Some(true))),
    ];
    let ctx = ParseContext::new("MATCH (n) RETURN n LIMIT $limit", "s").with_params(&params);
    let _ = adapter.on_parse(&ctx);

    let got = hook.captured.lock().clone().expect("hook fired");
    assert_eq!(got.get("limit"), Some(&Value::Int(42)));
    assert_eq!(got.get("name"), Some(&Value::String("ada".to_owned())));
    assert_eq!(got.get("active"), Some(&Value::Bool(true)));
    assert_eq!(got.len(), 3);
}

#[test]
fn bridge_routes_commit_result_through_to_legacy() {
    use std::time::Duration;
    use uni_plugin::traits::hook::PluginCommitResult;

    struct CaptureCr {
        captured: parking_lot::Mutex<Option<(usize, u64, u64, Duration)>>,
    }
    impl SessionHook for CaptureCr {
        fn after_commit(&self, _ctx: &CommitHookContext, result: &CommitResult) {
            *self.captured.lock() = Some((
                result.mutations_committed,
                result.version,
                result.wal_lsn,
                result.duration,
            ));
        }
    }
    let hook = Arc::new(CaptureCr {
        captured: parking_lot::Mutex::new(None),
    });
    let adapter = LegacyHookAdapter::new("c", hook.clone() as Arc<dyn SessionHook>);

    let commit_result = PluginCommitResult {
        mutations: 7,
        version: 42,
        wal_lsn: 123_456,
        duration: Duration::from_millis(15),
    };
    let ctx = CommitContext::new("s").with_commit_result(&commit_result);
    adapter.after_commit(&ctx);

    let got = hook.captured.lock().expect("hook fired");
    assert_eq!(got.0, 7);
    assert_eq!(got.1, 42);
    assert_eq!(got.2, 123_456);
    assert_eq!(got.3, Duration::from_millis(15));
}

#[test]
fn bridge_after_commit_without_result_keeps_zero_stub() {
    // Back-compat path: `CommitContext::new(...)` (no `with_commit_result`)
    // still produces a zero-filled legacy `CommitResult` so pre-v1.1
    // hosts see no behavior change.
    struct CaptureCr {
        captured: parking_lot::Mutex<Option<(usize, u64)>>,
    }
    impl SessionHook for CaptureCr {
        fn after_commit(&self, _ctx: &CommitHookContext, result: &CommitResult) {
            *self.captured.lock() = Some((result.mutations_committed, result.version));
        }
    }
    let hook = Arc::new(CaptureCr {
        captured: parking_lot::Mutex::new(None),
    });
    let adapter = LegacyHookAdapter::new("c", hook.clone() as Arc<dyn SessionHook>);

    adapter.after_commit(&CommitContext::new("s"));
    assert_eq!(*hook.captured.lock(), Some((0, 0)));
}
