// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-plugin-host/src/scheduler.rs:145 (delegates to
// crates/uni-plugin/src/scheduler.rs:137 Scheduler::add_scheduled_job).
//
// `Scheduler::add_scheduled_job` unconditionally pushes a new record with
// no dedup-by-id. Re-registering the same qname (reachable from user code
// via `uni.periodic.schedule`) yields TWO in-memory records for one qname,
// so the job double-fires every interval and a single `cancel` (which uses
// `.find` = first match only) leaves one duplicate still firing.

use std::time::Duration;

use uni_plugin::qname::QName;
use uni_plugin::scheduler::Scheduler;
use uni_plugin::traits::background::Schedule;

#[test]
fn duplicate_registration_creates_two_records_and_double_fires() {
    let s = Scheduler::new();
    s.resume();

    let id = QName::new("test", "dup");
    s.add_scheduled_job(id.clone(), Schedule::Periodic(Duration::from_millis(1)));
    s.add_scheduled_job(id.clone(), Schedule::Periodic(Duration::from_millis(1)));

    // FIXED: add_scheduled_job upserts by id, so a re-registration replaces the
    // record instead of creating a duplicate.
    assert_eq!(
        s.list().len(),
        1,
        "re-registering the same qname must upsert, not duplicate"
    );

    // Let the record become due, then tick once — it fires exactly once.
    std::thread::sleep(Duration::from_millis(5));
    let due = s.tick();
    let dup_fires = due.iter().filter(|q| **q == id).count();
    assert_eq!(
        dup_fires, 1,
        "the job must fire once per interval, not twice"
    );
}

#[test]
fn single_cancel_leaves_a_duplicate_still_firing() {
    let s = Scheduler::new();
    s.resume();

    let id = QName::new("test", "dup");
    s.add_scheduled_job(id.clone(), Schedule::Periodic(Duration::from_millis(1)));
    s.add_scheduled_job(id.clone(), Schedule::Periodic(Duration::from_millis(1)));
    // FIXED: only one record exists after re-registration.
    assert_eq!(s.list().len(), 1);

    // A single cancel now fully stops the (single) job.
    assert!(s.cancel(&id), "cancel returns true for the found record");

    std::thread::sleep(Duration::from_millis(5));
    let due = s.tick();
    let still_firing = due.iter().filter(|q| **q == id).count();
    assert_eq!(
        still_firing, 0,
        "after cancel the job must not fire (no surviving duplicate)"
    );
}
