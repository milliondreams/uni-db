#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5e — end-to-end verification of the `Uni::add_plugin(BuiltinHookPlugin::new(...))`
//! sugar path. A legacy `SessionHook` registered through the plugin
//! registry must fire through the phased dispatch chain
//! (`on_parse` / `on_execute_end` / `before_commit` / `after_commit`)
//! without any direct `Session::add_hook` call.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use uni_common::Result;
use uni_db::api::Uni;
use uni_db::api::hooks::{BuiltinHookPlugin, CommitHookContext, HookContext, SessionHook};
use uni_db::api::transaction::CommitResult;
use uni_query::QueryMetrics as LegacyQueryMetrics;

#[derive(Default)]
struct CountingHook {
    before_query: AtomicU32,
    after_query: AtomicU32,
    before_commit: AtomicU32,
    after_commit: AtomicU32,
}

impl SessionHook for CountingHook {
    fn before_query(&self, _ctx: &HookContext) -> Result<()> {
        self.before_query.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn after_query(&self, _ctx: &HookContext, _metrics: &LegacyQueryMetrics) {
        self.after_query.fetch_add(1, Ordering::SeqCst);
    }
    fn before_commit(&self, _ctx: &CommitHookContext) -> Result<()> {
        self.before_commit.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn after_commit(&self, _ctx: &CommitHookContext, _result: &CommitResult) {
        self.after_commit.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn add_plugin_with_builtin_hook_plugin_fires_query_phases() {
    let uni = Uni::in_memory().build().await.expect("Uni::in_memory");

    let counter: Arc<CountingHook> = Arc::new(CountingHook::default());
    uni.add_plugin(BuiltinHookPlugin::new(
        "sugar-test",
        counter.clone() as Arc<dyn SessionHook>,
    ))
    .expect("add_plugin");

    let session = uni.session();
    let _ = session.query("RETURN 1 AS n").await.expect("query");

    assert!(
        counter.before_query.load(Ordering::SeqCst) >= 1,
        "before_query (via on_parse) must fire"
    );
    assert!(
        counter.after_query.load(Ordering::SeqCst) >= 1,
        "after_query (via on_execute_end) must fire"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn add_plugin_with_builtin_hook_plugin_fires_commit_phases() {
    let uni = Uni::in_memory().build().await.expect("Uni::in_memory");

    let counter: Arc<CountingHook> = Arc::new(CountingHook::default());
    uni.add_plugin(BuiltinHookPlugin::new(
        "commit-sugar-test",
        counter.clone() as Arc<dyn SessionHook>,
    ))
    .expect("add_plugin");

    let session = uni.session();
    let tx = session.tx().await.expect("begin tx");
    tx.execute("CREATE (:Probe {id: 1})").await.expect("exec");
    tx.commit().await.expect("commit");

    assert!(
        counter.before_commit.load(Ordering::SeqCst) >= 1,
        "before_commit fires through registry dispatch"
    );
    assert!(
        counter.after_commit.load(Ordering::SeqCst) >= 1,
        "after_commit fires through registry dispatch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn add_plugin_makes_hook_observable_via_plugin_registry() {
    let uni = Uni::in_memory().build().await.expect("Uni::in_memory");

    let before = uni.plugin_registry().hooks().len();
    uni.add_plugin(BuiltinHookPlugin::new(
        "observable",
        Arc::new(CountingHook::default()) as Arc<dyn SessionHook>,
    ))
    .expect("add_plugin");
    let after = uni.plugin_registry().hooks().len();

    assert_eq!(
        after,
        before + 1,
        "BuiltinHookPlugin must register exactly one phased hook"
    );
}
