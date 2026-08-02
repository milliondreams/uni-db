// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for `storage/resilient_store.rs:117` — two defects in one branch.
//!
//! `ResilientObjectStore::retry` decided whether an error was worth retrying by
//! substring-matching the *rendered* message:
//!
//! ```ignore
//! let msg = e.to_string().to_lowercase();
//! if msg.contains("not found") || msg.contains("already exists") { return Err(e); }
//! ```
//!
//! **Defect 1 — the classifier is stringly-typed.** A genuinely *transient*
//! failure wrapped in `Error::Generic` whose text merely reads like a missing
//! object — a proxy returning a 404 HTML body, an S3-compatible endpoint
//! reporting a missing bucket — was declared terminal, so a blip that would
//! have healed on the next attempt was abandoned immediately. This is the same
//! defect as `repro_id_allocator_substring_notfound.rs`, one layer down.
//!
//! **Defect 2 — the classifier ran too late.** The attempt-budget check sat
//! *above* it:
//!
//! ```ignore
//! attempt += 1;
//! if attempt > self.config.max_retries { self.cb.report_failure(); return Err(e); }
//! // classifier here — unreachable on the final attempt
//! ```
//!
//! so a terminal error arriving on the last attempt was charged to the circuit
//! breaker as a system failure and never reached the classifier at all. Five
//! such operations open the breaker for 30s, taking down every path through the
//! store for what were application-level "absent" answers.
//!
//! Both are now decided by the typed `store_utils::is_terminal`, checked before
//! an attempt is spent.

use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use uni_common::config::ObjectStoreConfig;
use uni_store::storage::ResilientObjectStore;

use super::fault_store::{FaultKind, FaultStore};

/// `max_retries: 3` → 4 attempts, with negligible backoff so the tests are fast.
fn config(max_retries: u32) -> ObjectStoreConfig {
    ObjectStoreConfig {
        connect_timeout: Duration::from_secs(5),
        max_retries,
        retry_backoff_base: Duration::from_millis(1),
        retry_backoff_max: Duration::from_millis(2),
        read_timeout: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
    }
}

async fn seed(inner: &Arc<dyn ObjectStore>, path: &Path) {
    inner
        .put(path, PutPayload::from_static(b"payload"))
        .await
        .expect("seed write");
}

/// Defect 1: a transient `Generic` whose message reads "not found" must be
/// retried, not mistaken for an absent object.
#[tokio::test]
async fn transient_generic_reading_as_not_found_is_retried() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("catalog/manifest.json");
    seed(&inner, &path).await;

    let fault = Arc::new(FaultStore::new(inner.clone()));
    // A wrapped S3 bucket error: transient in *kind*, "not found" in *text*.
    fault.set_transient_message("the specified bucket was not found");
    fault.fail_next_gets(2);

    let store = ResilientObjectStore::new(fault.clone(), config(3));

    let result = store.get(&path).await;

    // Before the fix the substring matched on the first attempt and the read
    // failed outright.
    let bytes = result
        .expect("a transient error must be retried regardless of its message")
        .bytes()
        .await
        .expect("payload readable");
    assert_eq!(bytes.as_ref(), b"payload");

    // The decisive assertion. Success alone does not distinguish "the
    // classifier let us retry" from "the fault never armed" — only the attempt
    // count does: 2 injected failures plus the healed third call.
    assert_eq!(
        fault.get_attempts(),
        3,
        "expected 2 retried failures then success"
    );
}

/// Defect 2: a terminal error on the *final* attempt must not be counted
/// against the circuit breaker.
///
/// `script_gets` places the `NotFound` precisely on attempt 4 of a 4-attempt
/// budget — the one position where classifying after the budget check differs
/// from classifying before it.
#[tokio::test]
async fn terminal_on_the_final_attempt_does_not_trip_the_breaker() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("catalog/manifest.json");
    seed(&inner, &path).await;

    let fault = Arc::new(FaultStore::new(inner.clone()));
    let store = ResilientObjectStore::new(fault.clone(), config(3));

    // The breaker threshold is 5. Under the old ordering each round charged one
    // failure, so the fifth round opened it.
    for round in 0..5 {
        fault.script_gets(&[
            FaultKind::Transient,
            FaultKind::Transient,
            FaultKind::Transient,
            FaultKind::NotFound,
        ]);
        let err = store
            .get(&path)
            .await
            .expect_err("scripted faults must fail this round");
        assert!(
            !err.to_string().contains("Circuit breaker open"),
            "round {round} was rejected by the breaker before the script ran: {err}"
        );
    }

    // The store is healthy again. Before the fix this returned
    // "Circuit breaker open" — five application-level `NotFound`s had been
    // logged as system failures.
    fault.reset_counters();
    let bytes = store
        .get(&path)
        .await
        .expect("terminal errors must not open the breaker")
        .bytes()
        .await
        .expect("payload readable");
    assert_eq!(bytes.as_ref(), b"payload");
    assert_eq!(
        fault.get_attempts(),
        1,
        "the healthy read should reach the inner store exactly once"
    );
}

/// Defect 3 (`resilient_store.rs:184`): `put_opts` counted *every* failure
/// against the circuit breaker, with no classifier at all.
///
/// `put_opts` is the conditional-write entry point and does not route through
/// `retry`, so it never saw even the old substring check. A writer losing an OCC
/// race reports `AlreadyExists`; five of those — an ordinary amount of write
/// contention — opened the breaker and took the whole store offline for 30s.
#[tokio::test]
async fn occ_conflicts_do_not_open_the_breaker() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(FaultStore::new(inner.clone()));
    fault.set_fault_kind(FaultKind::AlreadyExists);
    fault.fail_next_puts(5);

    let store = ResilientObjectStore::new(fault.clone(), config(3));
    let path = Path::from("catalog/counters.json");

    // Five conditional writes lose their race. Each is a legitimate
    // application-level answer, not a store failure.
    for round in 0..5 {
        let err = store
            .put(&path, PutPayload::from_static(b"v"))
            .await
            .expect_err("armed conflict must fail");
        assert!(
            matches!(err, object_store::Error::AlreadyExists { .. }),
            "round {round} should report a typed conflict, got: {err}"
        );
    }

    // The sixth write is uncontended and must land.
    store
        .put(&path, PutPayload::from_static(b"v"))
        .await
        .expect("write conflicts must not open the breaker");

    // The decisive assertion: the breaker short-circuits *before* the inner
    // store, so the counter — not the error text — is what proves the sixth
    // write actually reached it. Before the fix this stalled at 5.
    assert_eq!(
        fault.put_attempts(),
        6,
        "the uncontended write should have reached the inner store"
    );
}

/// Control, green before and after: a typed `NotFound` is terminal and must not
/// burn the retry budget.
///
/// This one **passed before the fix too** — the old substring check happened to
/// match `NotFound`'s rendered text. It is a pin on preserved behaviour, not a
/// demonstration of the defect.
#[tokio::test]
async fn typed_not_found_is_not_retried() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(FaultStore::new(inner.clone()));
    fault.set_fault_kind(FaultKind::NotFound);
    fault.set_fail_get(true);

    let store = ResilientObjectStore::new(fault.clone(), config(3));

    let err = store
        .get(&Path::from("absent"))
        .await
        .expect_err("NotFound must surface");
    assert!(
        matches!(err, object_store::Error::NotFound { .. }),
        "expected a typed NotFound, got: {err}"
    );
    assert_eq!(
        fault.get_attempts(),
        1,
        "a terminal error must not be retried"
    );
}
