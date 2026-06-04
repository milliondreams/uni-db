#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5 Batch 2 follow-up #5 — end-to-end planner dispatch through a
//! plugin-registered [`ReplacementScanProvider`].
//!
//! Three sites are exercised:
//!
//! 1. **Procedure** (`CALL missing_proc()` → `CALL builtin.system.echo()`).
//!    Verifies the procedure-call site in `QueryPlanner::plan_with_scope`
//!    consults the provider before constructing `LogicalPlan::ProcedureCall`
//!    when the gate is on, and is a strict no-op when the gate is off.
//! 2. **Function** (`RETURN my_fn(x)` → `RETURN UPPER(x)`). Verifies the
//!    AST-pass rewrite descending through every `Expr::FunctionCall` site.
//! 3. **Label** (`MATCH (n:Phantom)`). Verifies strict mode errors when
//!    enabled (with or without a claiming provider) and preserves
//!    silent-empty schemaless behavior when the gate is off.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use uni_db::Uni;
use uni_plugin::traits::catalog::{
    CatalogTable, Replacement, ReplacementRequest, ReplacementScanProvider,
};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, QName};

/// Stub `CatalogTable` returned by the label-replacement provider; this
/// Stub `CatalogTable` returning a single empty `RecordBatch`. With #6
/// landed, label-replacement now lowers to a real `CatalogVertexScanExec`
/// — this minimal table proves the dispatch reaches `scan` and produces
/// (zero) rows successfully. Tests that need actual data live in
/// `plugin_virtual_label_dispatch.rs`.
#[derive(Debug)]
struct StubCatalogTable {
    schema: arrow_schema::SchemaRef,
}

impl StubCatalogTable {
    fn new() -> Self {
        // A property column so the planner can build a `_vid`/`_labels`
        // adapter schema without trying to project against an empty list.
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "placeholder",
            arrow_schema::DataType::Utf8,
            true,
        )]));
        Self { schema }
    }
}

impl CatalogTable for StubCatalogTable {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }
    fn scan(
        &self,
        _proj: Option<&[usize]>,
        _filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, FnError> {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use futures::stream;
        let batch = arrow_array::RecordBatch::new_empty(self.schema.clone());
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream::iter(vec![Ok(batch)]),
        )))
    }
}

/// Fixture provider that reroutes known stub identifiers and counts
/// invocations per request kind. The counters are `Arc`-shared so the
/// test body can assert against them after running the query.
#[derive(Debug)]
struct ReroutingProvider {
    proc_calls: Arc<AtomicUsize>,
    func_calls: Arc<AtomicUsize>,
    label_calls: Arc<AtomicUsize>,
    /// When `true`, function `loop_a` reroutes to `loop_b` *and*
    /// `loop_b` reroutes to `loop_a`. Used for the rewrite-loop guard
    /// test — since the planner caps rewrite depth at one, the second
    /// hop never fires, so the rerouted name is what flows through to
    /// the UDF dispatcher (where it'll fail to resolve).
    loop_mode: bool,
}

impl ReroutingProvider {
    fn new() -> Self {
        Self {
            proc_calls: Arc::new(AtomicUsize::new(0)),
            func_calls: Arc::new(AtomicUsize::new(0)),
            label_calls: Arc::new(AtomicUsize::new(0)),
            loop_mode: false,
        }
    }

    fn with_loop_mode() -> Self {
        Self {
            loop_mode: true,
            ..Self::new()
        }
    }
}

impl ReplacementScanProvider for ReroutingProvider {
    fn replace(&self, request: &ReplacementRequest<'_>) -> Option<Replacement> {
        match request {
            ReplacementRequest::Procedure(q) if q.local() == "missing_proc" => {
                self.proc_calls.fetch_add(1, Ordering::SeqCst);
                Some(Replacement::Procedure(QName::new("builtin", "system.echo")))
            }
            ReplacementRequest::Function(q) if q.local().eq_ignore_ascii_case("my_fn") => {
                self.func_calls.fetch_add(1, Ordering::SeqCst);
                Some(Replacement::Function(QName::new("builtin", "UPPER")))
            }
            ReplacementRequest::Function(q)
                if self.loop_mode && q.local().eq_ignore_ascii_case("loop_a") =>
            {
                self.func_calls.fetch_add(1, Ordering::SeqCst);
                Some(Replacement::Function(QName::new("builtin", "loop_b")))
            }
            ReplacementRequest::Function(q)
                if self.loop_mode && q.local().eq_ignore_ascii_case("loop_b") =>
            {
                self.func_calls.fetch_add(1, Ordering::SeqCst);
                Some(Replacement::Function(QName::new("builtin", "loop_a")))
            }
            ReplacementRequest::Label(name) if *name == "Phantom" => {
                self.label_calls.fetch_add(1, Ordering::SeqCst);
                Some(Replacement::CatalogTable(Arc::new(StubCatalogTable::new())))
            }
            _ => None,
        }
    }
}

/// Register `provider` with the database's plugin registry under
/// `Capability::Catalog`. Returns the committed registrar's commit
/// result.
fn register(db: &Arc<Uni>, provider: Arc<ReroutingProvider>) {
    let caps = CapabilitySet::from_iter_of([Capability::Catalog]);
    let mut r = PluginRegistrar::new(
        PluginId::new("test_replacement_scan"),
        &caps,
        db.plugin_registry(),
    );
    r.replacement_scan(provider as Arc<dyn ReplacementScanProvider>)
        .expect("Capability::Catalog satisfies replacement_scan");
    r.commit_to_registry()
        .expect("registry must accept the registration");
}

async fn fresh_db() -> Arc<Uni> {
    Arc::new(Uni::temporary().build().await.expect("temporary db builds"))
}

// ── Procedure ────────────────────────────────────────────────────────

#[tokio::test]
async fn procedure_rerouted_when_enabled() -> anyhow::Result<()> {
    let db = fresh_db().await;
    let provider = Arc::new(ReroutingProvider::new());
    let calls = Arc::clone(&provider.proc_calls);
    register(&db, Arc::clone(&provider));

    let session = db.session();
    session.set_replacement_scans(true);
    let res = session
        .query("CALL missing_proc('hello') YIELD echo RETURN echo AS out")
        .await?;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "provider must see exactly one Procedure request"
    );
    assert_eq!(res.len(), 1, "echo must yield one row");
    let v: String = res.rows()[0].get("out")?;
    assert_eq!(v, "hello");
    Ok(())
}

#[tokio::test]
async fn procedure_not_rerouted_when_disabled() -> anyhow::Result<()> {
    let db = fresh_db().await;
    let provider = Arc::new(ReroutingProvider::new());
    let calls = Arc::clone(&provider.proc_calls);
    register(&db, Arc::clone(&provider));

    // Gate stays off (the default). The procedure must NOT be consulted
    // and the query must fail with the existing "procedure not found"
    // shape (or otherwise error at execute time — exact text isn't
    // contractual here, just the no-consult behavior).
    let session = db.session();
    let err = session
        .query("CALL missing_proc('hello') YIELD echo RETURN echo")
        .await
        .err();
    assert!(err.is_some(), "missing_proc must error when gate is off");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "provider must not be consulted when the gate is off"
    );
    Ok(())
}

// ── Function ─────────────────────────────────────────────────────────

#[tokio::test]
async fn function_rerouted_when_enabled() -> anyhow::Result<()> {
    let db = fresh_db().await;
    let provider = Arc::new(ReroutingProvider::new());
    let calls = Arc::clone(&provider.func_calls);
    register(&db, Arc::clone(&provider));

    let session = db.session();
    session.set_replacement_scans(true);
    let res = session.query("RETURN my_fn('x') AS r").await?;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "Function must be consulted once"
    );
    assert_eq!(res.len(), 1);
    let v: String = res.rows()[0].get("r")?;
    assert_eq!(v, "X", "UPPER('x') should be 'X'");
    Ok(())
}

#[tokio::test]
async fn function_rewrite_loop_guarded() -> anyhow::Result<()> {
    let db = fresh_db().await;
    let provider = Arc::new(ReroutingProvider::with_loop_mode());
    let calls = Arc::clone(&provider.func_calls);
    register(&db, Arc::clone(&provider));

    // `loop_a` reroutes to `loop_b`. The planner caps rewrite depth at
    // 1 (the rewritten name is NOT re-consulted). So the substituted
    // name `builtin.loop_b` flows to the UDF dispatcher, which has no
    // such function — the query errors at execute time. Crucially the
    // assertions are: (a) we did NOT recurse infinitely (test would
    // hang / stack-overflow if we did) and (b) consult ran exactly
    // once.
    let session = db.session();
    session.set_replacement_scans(true);
    let err = session.query("RETURN loop_a(1) AS r").await.err();
    assert!(
        err.is_some(),
        "loop_b is not a real UDF — execute must error"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "rewrite depth cap should restrict consult to exactly one hop"
    );
    Ok(())
}

// ── Label ────────────────────────────────────────────────────────────

#[tokio::test]
async fn label_unknown_resolves_via_catalog_provider() -> anyhow::Result<()> {
    // Follow-up #6 update: this test previously asserted the
    // "virtual label-id allocation is not yet wired" error. With #6
    // landed, label-replacement now lowers end-to-end — the planner
    // allocates a virtual id, dispatches `CatalogVertexScanExec`, and
    // `scan` produces (zero) rows from the StubCatalogTable.
    let db = fresh_db().await;
    let provider = Arc::new(ReroutingProvider::new());
    let calls = Arc::clone(&provider.label_calls);
    register(&db, Arc::clone(&provider));

    let session = db.session();
    session.set_replacement_scans(true);
    let res = session
        .query("MATCH (n:Phantom) RETURN n")
        .await
        .expect("strict mode + provider claim must succeed via CatalogVertexScanExec");
    assert_eq!(res.len(), 0, "stub catalog table yields zero rows");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "Label consult should fire once"
    );
    Ok(())
}

#[tokio::test]
async fn label_unknown_errors_under_strict_no_provider() -> anyhow::Result<()> {
    let db = fresh_db().await;
    // No provider registered.

    let session = db.session();
    session.set_replacement_scans(true);
    let err = session
        .query("MATCH (n:Phantom) RETURN n")
        .await
        .err()
        .expect("strict mode without provider must error on unknown label");
    let msg = format!("{err}");
    assert!(
        msg.contains("CatalogProvider or ReplacementScanProvider claimed it"),
        "error must mention strict-mode rejection; got: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn label_unknown_silent_when_disabled() -> anyhow::Result<()> {
    let db = fresh_db().await;
    // Provider registered but gate stays off — provider must NOT be consulted.
    let provider = Arc::new(ReroutingProvider::new());
    let calls = Arc::clone(&provider.label_calls);
    register(&db, Arc::clone(&provider));

    let res = db
        .session()
        .query("MATCH (n:Phantom) RETURN n")
        .await
        .expect("schemaless MATCH must succeed when the gate is off");
    assert_eq!(
        res.len(),
        0,
        "no nodes carry :Phantom — schemaless fallback returns empty"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "provider must not be consulted when the gate is off"
    );
    Ok(())
}
