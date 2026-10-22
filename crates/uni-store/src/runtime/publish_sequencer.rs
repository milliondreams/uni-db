// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Releases commit announcements in commit order.
//!
//! A commit becomes visible under the writer's `flush_lock`, but its
//! announcement (the commit notification that watchers and CDC consume) is
//! built and sent after the lock is released, because building it awaits work
//! that must not run under the lock. Two commits can therefore finish their
//! post-commit work in either order. Sent directly, a later commit's
//! announcement could overtake an earlier one's, and CDC — which turns each
//! announcement into an LSN range and checkpoints the newest — would see its
//! LSN sequence go backwards.
//!
//! The sequencer fixes the order at the point the order is decided: a
//! [`PublishTicket`] is issued under `flush_lock`, so ticket order is commit
//! order. A ticket's announcement is held until every earlier ticket has been
//! published or dropped, then released, so announcements leave in ticket order
//! however the post-commit work interleaves. Nothing ever waits: an
//! announcement that is not yet due is buffered and sent by whichever caller
//! completes the gap.
//!
//! A ticket dropped without publishing (its commit's post-commit path errored,
//! panicked or was cancelled) releases its slot, so it can never wedge the
//! announcements behind it.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

/// An announcement waiting for its turn.
type Announcement = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct State {
    /// Sequence number the next issued ticket gets.
    issued: u64,
    /// Sequence number of the next announcement to release.
    next: u64,
    /// Completed tickets not yet released: `None` for a ticket dropped
    /// without an announcement.
    ready: BTreeMap<u64, Option<Announcement>>,
}

/// Orders commit announcements by the order their tickets were issued in.
///
/// One per writer. See the module documentation.
#[derive(Default)]
pub struct PublishSequencer {
    state: Mutex<State>,
}

impl std::fmt::Debug for PublishSequencer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        f.debug_struct("PublishSequencer")
            .field("issued", &state.issued)
            .field("next", &state.next)
            .field("buffered", &state.ready.len())
            .finish()
    }
}

impl PublishSequencer {
    /// A sequencer with no tickets issued.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Issue the next ticket.
    ///
    /// The caller must hold the lock that serializes the commits being
    /// announced (the writer's `flush_lock`), at the point the commit became
    /// visible: ticket order is what fixes announcement order.
    pub fn issue(self: &Arc<Self>) -> PublishTicket {
        let mut state = self.state.lock();
        let seq = state.issued;
        state.issued += 1;
        PublishTicket {
            sequencer: Arc::clone(self),
            seq,
            completed: false,
        }
    }

    /// Record `seq` as complete and release every announcement now due.
    ///
    /// Announcements run under the sequencer's lock, which is what keeps two
    /// completing callers from releasing out of order; they must be cheap and
    /// must not re-enter the sequencer (a broadcast send is both).
    fn complete(&self, seq: u64, announcement: Option<Announcement>) {
        let mut state = self.state.lock();
        state.ready.insert(seq, announcement);
        loop {
            let next = state.next;
            let Some(entry) = state.ready.remove(&next) else {
                break;
            };
            state.next += 1;
            if let Some(announce) = entry {
                // A panicking announcement must not stall the ones behind it.
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(announce)).is_err() {
                    tracing::error!(seq = next, "commit announcement panicked");
                }
            }
        }
    }
}

/// A commit's place in the announcement order; see [`PublishSequencer`].
///
/// Publish it with [`Self::publish`]. Dropping it unpublished gives up the
/// place without announcing anything.
#[must_use = "a ticket orders an announcement; drop it explicitly to give up the slot"]
pub struct PublishTicket {
    sequencer: Arc<PublishSequencer>,
    seq: u64,
    completed: bool,
}

impl std::fmt::Debug for PublishTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishTicket")
            .field("seq", &self.seq)
            .finish()
    }
}

impl PublishTicket {
    /// This ticket's position in the announcement order.
    #[must_use]
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Announce: run `announce` once every earlier ticket has been published
    /// or dropped — immediately, if that is already the case.
    ///
    /// `announce` may run on another caller's thread (whichever completes the
    /// gap) and under the sequencer's lock, so it must be cheap and must not
    /// block or publish another ticket.
    pub fn publish(mut self, announce: impl FnOnce() + Send + 'static) {
        self.completed = true;
        self.sequencer.complete(self.seq, Some(Box::new(announce)));
    }
}

impl Drop for PublishTicket {
    fn drop(&mut self) {
        if !self.completed {
            self.sequencer.complete(self.seq, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Announcements recorded in release order.
    type Log = Arc<Mutex<Vec<u64>>>;

    /// A log, and a maker of announcements that append their number to it.
    fn recorder() -> (Log, impl Fn(u64) -> Announcement) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let l = Arc::clone(&log);
        (log, move |n| {
            let l = Arc::clone(&l);
            Box::new(move || l.lock().push(n))
        })
    }

    #[test]
    fn a_later_ticket_published_first_waits_for_the_earlier_one() {
        let seq = PublishSequencer::new();
        let (log, rec) = recorder();
        let a = seq.issue();
        let b = seq.issue();
        b.publish(rec(1));
        assert!(log.lock().is_empty(), "b released before a");
        a.publish(rec(0));
        assert_eq!(*log.lock(), [0, 1]);
    }

    #[test]
    fn a_dropped_ticket_releases_the_ones_behind_it() {
        let seq = PublishSequencer::new();
        let (log, rec) = recorder();
        let a = seq.issue();
        let b = seq.issue();
        let c = seq.issue();
        c.publish(rec(2));
        b.publish(rec(1));
        drop(a);
        assert_eq!(*log.lock(), [1, 2]);
    }

    #[test]
    fn in_order_publishes_release_immediately() {
        let seq = PublishSequencer::new();
        let (log, rec) = recorder();
        for n in 0..5 {
            seq.issue().publish(rec(n));
            assert_eq!(log.lock().len() as u64, n + 1);
        }
    }

    #[test]
    fn a_panicking_announcement_does_not_stall_the_next() {
        let seq = PublishSequencer::new();
        let (log, rec) = recorder();
        let a = seq.issue();
        let b = seq.issue();
        b.publish(rec(1));
        a.publish(|| panic!("boom"));
        assert_eq!(*log.lock(), [1]);
    }

    #[test]
    fn concurrent_publishers_release_in_ticket_order() {
        let seq = PublishSequencer::new();
        let (log, rec) = recorder();
        let tickets: Vec<_> = (0..64).map(|_| seq.issue()).collect();
        let handles: Vec<_> = tickets
            .into_iter()
            .rev()
            .enumerate()
            .map(|(i, t)| {
                let f = rec(t.seq());
                std::thread::spawn(move || {
                    std::thread::sleep(std::time::Duration::from_micros((i % 7) as u64 * 50));
                    t.publish(f);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(*log.lock(), (0..64).collect::<Vec<_>>());
    }
}
