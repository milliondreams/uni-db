# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Regression guards for correctness-scan region R13 — three GIL/std-Mutex
# lock-order-inversion deadlocks in the PyO3 binding layer, now FIXED:
#
#   [4] types.rs   PyPreparedQuery.execute  — outer Mutex removed (Arc handle),
#                  so no lock is held across py.detach(block_on).
#   [5] types.rs   PyCommitStream.__next__  — stream taken out of the mutex for
#                  the await; close() signals a wakeup + is uncontended.
#   [8] sync_api.rs QueryCursor.next_row    — cursor taken out by value for the
#                  await, then written back (mirrors fetch_all).
#
# Each defect used to FREEZE the whole interpreter, so every scenario runs in an
# isolated subprocess with a 12 s timeout as a hang backstop. With the fix the
# child completes and prints DONE_NO_DEADLOCK; a regressed lock order would
# re-freeze and the guard would fail on timeout.

import subprocess
import sys
import textwrap

# Deadlock manifests within a couple of seconds; give generous headroom, then
# treat a timeout as the reproduction signal.
_HANG_TIMEOUT = 12.0


def _run_child(src: str) -> subprocess.CompletedProcess | None:
    """Return the completed process, or None if it hung (deadlock reproduced)."""
    try:
        return subprocess.run(
            [sys.executable, "-c", textwrap.dedent(src)],
            capture_output=True,
            text=True,
            timeout=_HANG_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return None


def _assert_no_deadlock(result: subprocess.CompletedProcess | None) -> None:
    """Assert the child completed cleanly and reached its DONE sentinel."""
    assert result is not None, "deadlock regressed: child hung past the timeout"
    detail = result.stdout + result.stderr
    assert result.returncode == 0, detail
    assert "DONE_NO_DEADLOCK" in result.stdout, detail


# ---------------------------------------------------------------------------
# [4] PyPreparedQuery.execute — types.rs
# A single prepared query shared by two threads. 100 executes single-threaded
# take ~1.4s, so a >12s hang would be the ABBA deadlock, not slowness.
# ---------------------------------------------------------------------------
def test_prepared_query_shared_across_threads_no_deadlock():
    child = """
    import threading
    import uni_db
    db = uni_db.UniBuilder.temporary().build()
    db.schema().label("N").property("v", "int").done().apply()
    s = db.session(); tx = s.tx()
    for i in range(300):
        tx.execute("CREATE (:N {v:%d})" % i)
    tx.commit()
    pq = s.prepare("MATCH (a:N),(b:N) RETURN count(*) AS c")
    def worker():
        for _ in range(50):
            pq.execute()
    ts = [threading.Thread(target=worker) for _ in range(2)]
    for t in ts: t.start()
    for t in ts: t.join()
    print("DONE_NO_DEADLOCK")
    """
    result = _run_child(child)
    # FIXED: the shared Arc handle means no lock is held across py.detach, so
    # both threads make progress instead of deadlocking on the GIL/mutex. (types.rs)
    _assert_no_deadlock(result)


# ---------------------------------------------------------------------------
# [5] PyCommitStream.__next__ / close() — types.rs
# Reader thread parks in __next__ waiting for a commit; main thread calls
# close(). close() now signals a wakeup and never contends the mutex, so it
# returns promptly and interrupts the parked reader.
# ---------------------------------------------------------------------------
def test_commit_stream_close_during_iteration_no_deadlock():
    child = """
    import threading, time
    import uni_db
    db = uni_db.UniBuilder.temporary().build()
    db.schema().label("N").property("v", "int").done().apply()
    s = db.session()
    stream = s.watch()          # no commits will ever arrive
    def reader():
        for _ in stream:        # __next__ parks in block_on holding the mutex
            pass
    threading.Thread(target=reader, daemon=True).start()
    time.sleep(1.0)             # ensure the reader is parked
    stream.close()              # locks the same std mutex while holding the GIL
    print("DONE_NO_DEADLOCK")
    """
    result = _run_child(child)
    # FIXED: close() signals close_notify (waking the parked reader) and finds
    # the mutex uncontended, so it returns promptly. (types.rs)
    _assert_no_deadlock(result)


# ---------------------------------------------------------------------------
# [8] QueryCursor.next_row — sync_api.rs
# One cursor shared by two threads. 5000 rows single-threaded fetch in ~0.04s,
# so a >12s hang would be the lock-order inversion, not slowness.
# ---------------------------------------------------------------------------
def test_shared_cursor_next_row_no_deadlock():
    child = """
    import threading
    import uni_db
    db = uni_db.UniBuilder.temporary().build()
    db.schema().label("N").property("v", "int").done().apply()
    s = db.session(); tx = s.tx()
    for i in range(5000):
        tx.execute("CREATE (:N {v:%d})" % i)
    tx.commit()
    cur = s.query_with("MATCH (n:N) RETURN n.v AS v").cursor()
    def worker():
        while True:
            try:
                r = cur.fetch_one()
            except Exception:
                return
            if r is None:
                return
    ts = [threading.Thread(target=worker) for _ in range(2)]
    for t in ts: t.start()
    for t in ts: t.join()
    print("DONE_NO_DEADLOCK")
    """
    result = _run_child(child)
    # FIXED: next_row takes the cursor out by value for the await (holding no
    # mutex across block_on), so both fetchers make progress. (sync_api.rs)
    _assert_no_deadlock(result)
