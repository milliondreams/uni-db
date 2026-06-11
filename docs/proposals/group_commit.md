# Proposal: Group Commit on the WAL Flush Seam

**Status:** Proposed (bench-first — implementation pending the measured
headroom below justifying the risk)
**Date:** 2026-06-10
**Driven by:** architecture review 2026-06-10 §2.5 / §9 item 10

## Problem

Every commit holds the writer `flush_lock` across the entire sequence
(`Writer::commit_transaction_l0`, `crates/uni-store/src/runtime/writer.rs`):

1. OCC validation (read/write-set vs the commit registry)
2. WAL append + **flush** — one object-store PUT of a checksummed segment
   per commit, plus an fsync (file + directory) on local stores
3. Optional clone-on-freeze
4. Merge into main L0
5. Write-set registration + commit-sequence bump

Step 2 dominates the critical section: concurrent committers serialize on
one durable-write round trip each. The WAL layer already supports batching
(`append` pushes to a shared buffer; `flush` drains the WHOLE buffer into
ONE segment with one LSN; replay just concatenates mutations in LSN order
and nothing depends on per-commit segment boundaries) — but because each
commit appends *and* flushes while holding `flush_lock`, no batching ever
happens in practice.

## Invariants the current protocol guarantees (must be preserved)

- **Validation before WAL** — the WAL has no abort record, so a
  transaction must never reach the WAL unless it is certain to commit
  (writer.rs ~550: "aborting after it would resurrect this transaction on
  crash recovery").
- **WAL durable before merge visibility** — the merge makes the
  transaction visible to other transactions; if a crash followed a visible
  merge whose WAL write was lost, recovery would silently drop a commit
  that others may have observed (writer.rs ~740-746).
- **WAL durable before acknowledgment** — `commit()` returning `Ok` is the
  durability contract.
- **Registry order = validation order** — each commit validates against
  every registered write-set newer than its snapshot; registration happens
  under the same lock as validation.

## Design: leader-batch commit

Restructure the critical section into a two-phase protocol:

```
Phase A (under flush_lock, cheap):           Phase B (off-lock, batched):
  validate                                      leader performs ONE
  append mutations to WAL buffer                wal.flush() covering all
  register write-set provisionally              tickets appended since the
  take a ticket (batch seq)                     previous flush
  release flush_lock                          followers await the leader's
                                               flush result
Phase C (under flush_lock, in ticket order):
  merge into main L0
  confirm provisional registration
  ack the caller
```

- The first committer to find no flush in flight becomes the **leader**;
  committers arriving while a flush is in flight append + ticket and wait
  for the NEXT flush (classic group commit).
- Validation of a later transaction in the same batch runs against the
  earlier ones' *provisional* write-sets — conservative and safe: it
  serializes after them; if the batch flush fails, every member aborts
  (the provisional sets are rolled back), so a transaction can at worst be
  aborted unnecessarily, never wrongly committed.
- Merges are released strictly in ticket order after the covering flush
  returns, preserving "durable before visible" per member.

## Risks (why this is not a casual change)

1. **Merge-before-durable** — any reordering bug that lets a member's
   merge land before the covering flush completes silently violates crash
   safety. Failpoint coverage (`commit::after-validate`, `commit::mid-wal`,
   `commit::after-wal-flush`, `commit::after-merge`) must be extended with
   batch variants (crash between member merges of one batch).
2. **Stale validation** — provisional registration must be atomic with
   validation under the lock, or two batch members could miss each other's
   write-sets.
3. **Split-brain abort** — a failed batch flush must abort *all* members
   and roll back their provisional registrations before any new validation
   runs; member errors must surface as retriable.
4. **Sequence/LSN mapping** — one LSN now covers several commit sequences.
   Replay is already order-insensitive within a segment, but WAL truncation
   watermarks and `occ_read_seq` math must be re-audited.
5. **Hung flush** — the leader's flush failure/timeout must release
   followers (bounded by the existing WAL `DEFAULT_TIMEOUT` and
   `commit_timeout`).

## Measurement (go/no-go)

`crates/uni/benches/commit_throughput.rs` measures commits/sec for N ∈
{1, 4, 12, 24} concurrent sessions committing small disjoint-key
transactions, in two series:

- `wal_on` — production path (one durable WAL write per commit).
- `wal_off` — identical protocol minus the WAL write: the ceiling a
  perfect group commit could approach.

**The gap between the series is the group-commit headroom.** Numbers from
the initial run (disk-backed temp dirs on local NVMe/ext4, `--quick`,
2026-06-10; mean wall time for N×25 commits):

| N sessions | `wal_on` | `wal_off` | WAL share |
|---|---|---|---|
| 1  | 14.9 ms (1.68 K commits/s) | 13.6 ms (1.83 K/s) | ~9 % |
| 4  | 38.0 ms (2.63 K/s)         | 32.5 ms (3.07 K/s) | ~15 % |
| 12 | 113.0 ms (2.66 K/s)        | 113.9 ms (2.63 K/s) | ~0 % |
| 24 | 240.0 ms (2.50 K/s)        | 229.0 ms (2.73 K/s) | ~5 % |

(An earlier in-memory run showed wal_on ≈ wal_off at every N — kept here
as a methodology note: an in-memory object store turns the WAL PUT into a
hashmap insert and hides the entire quantity under measurement.)

Decision rule: if `wal_off` throughput at N=12/24 is ≥ 2× `wal_on`, the
headroom justifies the implementation risk; below that, the lock-held
validation+merge dominates and group commit should stay shelved.

## Verdict (2026-06-10): NO-GO on local storage

The measured headroom is ≤ 15 % at low concurrency and within noise at
N=12/24 — an order of magnitude below the 2× threshold. On a local NVMe
filesystem the durable WAL write (PUT + file/dir fsync, ~tens of µs) is a
small fraction of the ~380 µs serialized commit path; **validation + merge
under `flush_lock` are the bottleneck**, and group commit does not touch
them. Shelve the implementation.

Re-open if either changes:
- **High-latency WAL stores** (S3/GCS-backed WAL: one PUT round-trip per
  commit at ~10-100 ms would dominate utterly, and batching would win big).
  Re-run this bench against a LocalStack/S3 `wal_store` first.
- **The lock-held path shrinks** (e.g. validation moves to lock-free
  registry reads), making the WAL write the next bottleneck.

## Rollout plan (when/if implemented)

1. `UniConfig.group_commit: bool`, default **off**; the entire leader-batch
   path behind it, with the current serial path untouched as default.
2. Failpoint matrix for batch crash seams + Hermitage-style anomaly suite
   re-run under `group_commit = true`.
3. Soak under the nightly lane before flipping the default.
