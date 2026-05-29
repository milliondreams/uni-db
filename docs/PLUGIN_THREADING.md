# Plugin Threading Policy

This page describes how the uni-db host drives the synchronous plugin
surfaces — specifically `BackgroundJobProvider::execute` and
`TriggerPlugin::fire` — from the surrounding Tokio runtime, and what
plugin authors must do to cooperate.

## Why synchronous?

Both surfaces ship as `fn` (not `async fn`) on purpose: most plugin
authors run synchronous business logic (write a row, send an HTTP
request, compute a metric). Forcing `async` on every body would push
authors into either `block_on` (deadlocks the runtime) or coloring
every helper they call (large API ripple). Instead the host pays the
async-to-sync cost once, at the dispatch boundary.

## Dispatch model

The host always drives synchronous plugin entry points through
`tokio::task::spawn_blocking`. That gives each invocation its own
blocking-pool worker thread, so a slow plugin cannot stall query
execution, the scheduler tick loop, or other plugins.

| Surface                                        | When                                                        | Thread                          | Cancel signal                            |
| ---------------------------------------------- | ----------------------------------------------------------- | ------------------------------- | ---------------------------------------- |
| `BackgroundJobProvider::execute`               | Scheduler tick fires the job                                | `spawn_blocking` worker         | `JobContext::cancel`                     |
| `TriggerPlugin::fire` (`Synchronous`)          | Inside the transaction commit path                          | `spawn_blocking` worker, inline | Implicit (commit aborts on `Reject`)     |
| `TriggerPlugin::fire` (`Async`)                | Post-commit, decoupled task                                 | `spawn_blocking` worker         | None — cannot reject after commit        |
| `TriggerPlugin::fire` (`EventualConsistency`)  | Batched via the `BackgroundJobProvider` machinery           | `spawn_blocking` worker         | `JobContext::cancel` via the batch job   |

## Author obligations

1. **Do not call `block_on` against the host runtime.** Plugin bodies
   already run on a blocking worker, so any I/O can happen on the
   current thread (blocking HTTP, file I/O, blocking DB drivers). If
   you need async, build a private runtime — never reach into the
   host's.
2. **Cooperate with cancellation.** Background jobs receive a
   `CancellationToken`; check `is_cancelled()` at every loop boundary
   and exit promptly when it trips. Synchronous triggers honor
   `Reject` for the same purpose; async / eventual triggers are
   fire-and-forget and have no cancel signal.
3. **Keep `Synchronous` triggers tight.** They run inline on the
   committer — a slow trigger raises commit latency for every
   transaction matching the subscription. Push expensive work to
   `Async` or `EventualConsistency`.
4. **Panics are caught, not silenced.** The dispatcher converts a
   panic into a failed-run record (jobs) or a logged warning
   (triggers). Hosts do not crash, but the failure is visible in
   telemetry — fix the underlying bug rather than panic-as-control-flow.

## Cancellation primitive

**Phase 6 landed.** `uni_plugin::traits::background::CancellationToken`
is now a re-export of `tokio_util::sync::CancellationToken`. The sync
surface (`new`, `cancel`, `is_cancelled`, `Clone`, `Debug`, `Default`)
is identical to the prior hand-rolled flag, so plugin authors who only
poll `is_cancelled()` keep working unchanged. Async-aware bodies can
additionally `await` `cancelled()` to register for cooperative cancel
without polling.

The host scheduler driver uses the async surface to propagate
cancellation immediately. The per-job `spawn_blocking` is wrapped in a
`tokio::select!` against `cancel.cancelled().await`:

```rust
let blocking = tokio::task::spawn_blocking(move || provider.execute(ctx));
tokio::spawn(async move {
    let success = tokio::select! {
        joined = blocking => joined.map(|o| o.is_ok()).unwrap_or(false),
        () = cancel_for_select.cancelled() => false,
    };
    scheduler_clone.mark_finished(&id, success);
});
```

When `Scheduler::cancel(&id)` (or a shutdown broadcast that forwards
into the per-job token) trips, the outer `tokio::spawn` observes the
signal on the next runtime poll and finalizes the lifecycle / breaker
state — the in-flight synchronous body cannot be preempted, but its
outcome is dropped so downstream bookkeeping stays consistent.
