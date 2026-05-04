// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 12 — fork fragment-count guard rail.
//!
//! This integration test only covers the *primary doesn't emit* half of
//! the contract — verifying that primary flushes never fire the
//! fork-fragment warn even when the threshold is set to fire on every
//! flush. The *fork emits once* half is covered by an inline unit test
//! in `crates/uni-store/src/runtime/writer.rs::tests` (see
//! `fork_fragment_warn_fires_once_then_silences`) because the full
//! fork-flush path is blocked on Day 10's on-the-fly schema overlay
//! growth (writes through `BranchedBackend` to a label without a
//! pre-created branch currently bail). Once Day 10 lands, the fork
//! flush will succeed end-to-end and the unit test's contract carries
//! over.

// Rust guideline compliant

use std::sync::{Arc, Mutex, OnceLock};

use anyhow::Result;
use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};
use uni_db::{DataType, Uni};

#[derive(Default)]
struct WarnCapture {
    messages: Mutex<Vec<String>>,
}

impl WarnCapture {
    fn drain(&self) -> Vec<String> {
        std::mem::take(&mut *self.messages.lock().unwrap())
    }
}

static CAPTURE: OnceLock<Arc<WarnCapture>> = OnceLock::new();

fn install_subscriber() -> Arc<WarnCapture> {
    CAPTURE
        .get_or_init(|| {
            let capture = Arc::new(WarnCapture::default());
            let layer = CaptureLayer(capture.clone());
            let subscriber = Registry::default().with(layer);
            let _ = tracing::subscriber::set_global_default(subscriber);
            capture
        })
        .clone()
}

struct CaptureLayer(Arc<WarnCapture>);

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let target = event.metadata().target();
        if !target.starts_with("uni_store") {
            return;
        }
        let mut visitor = MsgVisitor(String::new());
        event.record(&mut visitor);
        if visitor.0.contains("L1 flush-count threshold") {
            self.0.messages.lock().unwrap().push(visitor.0);
        }
    }
}

struct MsgVisitor(String);
impl Visit for MsgVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0 = value.to_string();
        }
    }
}

#[tokio::test]
async fn primary_writer_does_not_emit_fork_warn() -> Result<()> {
    let capture = install_subscriber();
    capture.drain();

    let config = uni_db::UniConfig {
        auto_flush_threshold: 1,
        fork_fragment_warn_threshold: 1,
        ..Default::default()
    };

    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;

    let session = db.session();
    for i in 0..5 {
        let tx = session.tx().await?;
        tx.execute(&format!("CREATE (:Item {{kind: 'p-{i}'}})"))
            .await?;
        tx.commit().await?;
    }

    let messages = capture.drain();
    assert!(
        messages.is_empty(),
        "primary flushes must never emit fork-fragment warn; saw {messages:?}"
    );

    db.shutdown().await?;
    Ok(())
}
