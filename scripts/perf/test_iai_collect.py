#!/usr/bin/env python3
"""Self-checks for iai_collect.py, run as a step of the job that uses it.

There is no lane that runs tests under `scripts/`, so a test file here would
never execute on its own. The perf workflow runs this alongside
`test_iai_gate.py`, for the same reason: the collector decides what every later
number means, and it has reported a confident zero twice.

Both times the shape was identical — a parse that could not find what it wanted
returned 0, and 0 is a perfectly plausible instruction count:

1. `baseline_noop` was dropped from the original Phase 0B pilot because a
   filename without the `.tNN.pN` infix was skipped outright.
2. Under instrumentation gating (see `crates/uni/benches/hot_paths_iai.rs`)
   callgrind writes a degenerate `summary: 0` — one value where `events:`
   declares nine — beside a well-formed `totals:`. Reading `summary:` reported
   **every target in the suite** as zero while the real counts sat in the same
   files.

So these checks are mostly about the difference between "measured zero" and
"failed to measure", which is the distinction the whole perf pipeline rests on.

Dependency-free on purpose: the perf runner has python3 and nothing installed.

    python3 scripts/perf/test_iai_collect.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

COLLECT = Path(__file__).resolve().parent / "iai_collect.py"

EVENTS = "events: Ir Dr Dw I1mr D1mr D1mw ILmr DLmr DLmw"


def out_file(
    iai_dir: Path,
    group: str,
    bench: str,
    thread: int | None,
    *,
    summary: str | None,
    totals: str | None,
) -> Path:
    """Write one callgrind output file with the given cost lines.

    `thread=None` writes the bare `callgrind.<bench>.out` name callgrind uses
    when the process never spawned a thread.
    """
    d = iai_dir / group / bench
    d.mkdir(parents=True, exist_ok=True)
    suffix = ".out" if thread is None else f".t{thread:02d}.p1.out"
    path = d / f"callgrind.{bench}{suffix}"
    body = ["# callgrind format, version: 1", "positions: line", EVENTS]
    if summary is not None:
        body.append(f"summary: {summary}")
    if totals is not None:
        body.append(f"totals: {totals}")
    path.write_text("\n".join(body) + "\n")
    return path


def collect(iai_dir: Path, tmp: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable, str(COLLECT),
            "--iai-dir", str(iai_dir),
            "--out", str(tmp / "run-01.json"),
        ],
        capture_output=True, text=True, env={"PATH": "/usr/bin:/bin"},
    )


def loaded(tmp: Path) -> dict:
    return json.loads((tmp / "run-01.json").read_text())


def check(label: str, cond: bool, detail: str = "") -> None:
    if not cond:
        raise AssertionError(f"{label} FAILED {detail}")
    print(f"ok  {label}")


NINE = "{ir} 2 3 4 5 6 7 8 9"


def main() -> int:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        # The defect this file exists for. Instrumentation gating produces a
        # degenerate one-value `summary:` next to a nine-value `totals:`; the
        # old parser took `summary`, indexed Ir at position 0, and reported 0.
        d = tmp / "iai1"
        out_file(d, "read_paths", "vertex_lookup_by_id.by_id", 1,
                 summary="0", totals=NINE.format(ir=8_789_066))
        r = collect(d, tmp)
        got = loaded(tmp)["read_paths::vertex_lookup_by_id.by_id"]["instructions"]
        check("a degenerate `summary: 0` does not mask a real `totals:`",
              r.returncode == 0 and got == 8_789_066, f"got {got} rc={r.returncode}")

        # Per-thread files are summed, and the off-main-thread share is reported
        # separately. That column is the health check for the instrumentation
        # gate: all-zero means work stopped being attributed across threads.
        d = tmp / "iai2"
        for thread, ir in ((1, 1_000_000), (2, 30_000), (3, 70_000)):
            out_file(d, "write_paths", "l0_to_l1_flush.l0_to_l1", thread,
                     summary="0", totals=NINE.format(ir=ir))
        collect(d, tmp)
        e = loaded(tmp)["write_paths::l0_to_l1_flush.l0_to_l1"]
        check("per-thread files are summed",
              e["instructions"] == 1_100_000, f"got {e['instructions']}")
        check("off-main-thread work is attributed separately",
              e["main_thread"] == 1_000_000 and e["other_threads"] == 100_000, str(e))
        check("thread count is recorded", e["threads"] == 3, str(e))

        # A single-threaded process writes `callgrind.<bench>.out` with no
        # `.tNN.pN` infix. An earlier version required the infix and `continue`d
        # past anything else, silently dropping such benchmarks from the report
        # -- which is how `baseline_noop` vanished from the original pilot.
        d = tmp / "iai3"
        out_file(d, "baselines", "baseline_noop.noop", None,
                 summary="0", totals=NINE.format(ir=4_002))
        collect(d, tmp)
        e = loaded(tmp)["baselines::baseline_noop.noop"]
        check("a file with no thread infix is counted as thread 1",
              e["instructions"] == 4_002 and e["main_thread"] == 4_002, str(e))

        # `summary:` remains the fallback when callgrind writes no `totals:`.
        d = tmp / "iai4"
        out_file(d, "read_paths", "parse_and_plan_cold.cold", 1,
                 summary=NINE.format(ir=555_000), totals=None)
        collect(d, tmp)
        got = loaded(tmp)["read_paths::parse_and_plan_cold.cold"]["instructions"]
        check("`summary:` is still read when there is no `totals:`",
              got == 555_000, f"got {got}")

        # A genuine zero is a legitimate measurement -- a worker thread that ran
        # nothing -- and must not be confused with a parse failure.
        d = tmp / "iai5"
        out_file(d, "read_paths", "hnsw_top10_search.top10", 1,
                 summary="0", totals=NINE.format(ir=17_000))
        out_file(d, "read_paths", "hnsw_top10_search.top10", 2,
                 summary="0", totals="0")
        collect(d, tmp)
        e = loaded(tmp)["read_paths::hnsw_top10_search.top10"]
        check("a thread that genuinely collected nothing contributes 0",
              e["instructions"] == 17_000 and e["other_threads"] == 0, str(e))

        # The core distinction: a cost line too short to hold Ir is a parse
        # failure, and refusing is the whole point. Returning 0 here is what
        # produced a suite-wide zero.
        d = tmp / "iai6"
        out_file(d, "read_paths", "broken.b", 1, summary="0", totals="0 1 2")
        r = collect(d, tmp)
        check("a cost line too short for Ir is an error, not a zero",
              r.returncode != 0 and "refusing" in (r.stdout + r.stderr),
              f"rc={r.returncode} {r.stderr[:200]}")

        # The narrower form of the same hole, and the one an "is Ir in range?"
        # guard misses entirely: `Ir` is the first event, so `idx < len(counts)`
        # holds for any non-empty line. A degenerate `summary:` with no `totals:`
        # beside it must still refuse rather than read position 0.
        d = tmp / "iai6b"
        out_file(d, "read_paths", "degenerate_only.d", 1, summary="0 1 2", totals=None)
        r = collect(d, tmp)
        check("a degenerate `summary:` with no `totals:` refuses too",
              r.returncode != 0 and "refusing" in (r.stdout + r.stderr),
              f"rc={r.returncode} {r.stderr[:200]}")

        # Zero-instruction benchmarks are still surfaced loudly. The exit code
        # stays 0 -- `iai_cross_runner.py` is what makes it fatal -- but the
        # message must be present, because it is the only thing standing between
        # a vacuous run and a published number.
        d = tmp / "iai7"
        out_file(d, "baselines", "empty.e", 1, summary=None, totals=None)
        r = collect(d, tmp)
        check("a benchmark collecting zero is reported on stderr",
              "ZERO" in r.stderr, r.stderr[:200])

        # An empty tree is a failure, not an empty report.
        d = tmp / "iai8"
        d.mkdir(parents=True, exist_ok=True)
        r = collect(d, tmp)
        check("an iai dir with no callgrind output fails",
              r.returncode != 0, f"rc={r.returncode}")

    print("\nall iai_collect self-checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
