#!/usr/bin/env python3
"""Collect instruction counts from one iai-callgrind run.

Parses the raw ``callgrind.*.out`` files under ``target/iai`` rather than
iai-callgrind's own summary output. Three reasons:

1. The callgrind file format is documented and stable (``# callgrind format,
   version: 1``), so this does not break when iai-callgrind changes its
   reporting.
2. Callgrind writes **one file per thread**. Summing them gives the true total,
   and comparing the main thread against the rest reveals work that escaped the
   collection toggle — which is the failure mode that made the first Phase 0B
   run report ``Collected: 0`` on every target while exiting successfully.

   The bench is now instrumentation-gated rather than collection-toggled (see
   ``crates/uni/benches/hot_paths_iai.rs``), so the off-main-thread column
   should be **non-zero** for any target that touches Lance: lance dispatches
   onto tokio's blocking pool via ``spawn_cpu``. An ``off-thr %`` of 0.0 across
   the board means the instrumentation gate stopped working and the numbers are
   main-thread-only again.
3. It needs no flags on the bench invocation, so a plain ``cargo bench`` is
   enough.

Usage::

    iai_collect.py [--iai-dir target/iai] --out run-01.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# `callgrind.<fn>.<id>.t<NN>.p<N>.out` — the thread number is what we want.
#
# Callgrind only adds the `.tNN.pN` infix when the process actually spawned
# threads. A single-threaded benchmark yields a bare `callgrind.<fn>.<id>.out`,
# and an earlier version of this script required the infix and `continue`d past
# anything else — silently dropping such benchmarks from the report entirely.
# Absent data must never look like absent benchmarks.
THREAD_RE = re.compile(r"\.t(\d+)\.p\d+\.out$")


def parse_out_file(path: Path) -> tuple[int, list[str]]:
    """Returns (instruction count, event names) for one callgrind output file.

    Both ``totals:`` and ``summary:`` carry space-separated counts positionally
    matching the preceding ``events:`` line; ``Ir`` (instructions retired) is the
    metric an instruction-count gate is built on.

    ``totals:`` is preferred, and that preference is load-bearing rather than
    cosmetic. ``summary:`` is the cost of one dump part while ``totals:`` is the
    cost of the whole file, and under the instrumentation-gated configuration in
    ``crates/uni/benches/hot_paths_iai.rs`` callgrind writes a *degenerate*
    ``summary: 0`` — a single value where ``events:`` declares nine — next to a
    well-formed ``totals:``. Reading ``summary:`` there yields 0 for every
    benchmark while the real counts sit in the same file.

    A line is usable only if it is callgrind's bare ``0`` shorthand for a thread
    that collected nothing, or carries one value per declared event. Anything in
    between is degenerate and raises rather than being read positionally: the
    previous ``counts[idx] if idx < len(counts) else 0`` turned exactly that
    inconsistency into a plausible-looking measurement, which is the one failure
    mode this whole pipeline is built to make impossible. Note that indexing
    alone is not a sufficient guard — ``Ir`` is the first event, so
    ``idx < len(counts)`` holds for *any* non-empty line, including ``0``.
    """
    events: list[str] = []
    found: dict[str, list[int]] = {}
    with path.open() as fh:
        for line in fh:
            if line.startswith("events:"):
                events = line.split(":", 1)[1].split()
                continue
            for key in ("totals:", "summary:"):
                if not line.startswith(key):
                    continue
                if not events:
                    raise ValueError(f"{path}: {key.rstrip(':')} before events")
                # Last occurrence wins: callgrind may write one per dump part.
                found[key] = [int(x) for x in line.split(":", 1)[1].split()]

    idx = events.index("Ir") if "Ir" in events else 0
    # `totals:` is authoritative for the file, so it is consulted alone when
    # present. Falling back to `summary:` after an unusable `totals:` would be
    # the same fail-open in a narrower dress: under instrumentation gating
    # `summary: 0` is a legitimate all-zero line, so the fallback would quietly
    # convert a malformed authoritative line into a confident zero.
    for key in ("totals:", "summary:"):
        if key not in found:
            continue
        counts = found[key]
        # Callgrind writes a bare `0` as shorthand for "collected nothing" —
        # real, and common, since most worker threads do no work in the measured
        # window. Anything else must carry one value per declared event; a
        # shorter line is degenerate and must not be read positionally.
        if counts == [0]:
            return 0, events
        if len(counts) == len(events):
            return counts[idx], events
        raise ValueError(
            f"{path}: '{key.rstrip(':')}' has {len(counts)} value(s) against "
            f"{len(events)} declared events {events} — refusing to read it "
            f"positionally or to report this as zero"
        )

    # No cost line at all: a thread that genuinely collected nothing.
    return 0, events


def collect(iai_dir: Path) -> dict:
    """Walks the iai output tree, returning per-benchmark instruction totals."""
    benches: dict[str, dict] = {}
    for out in sorted(iai_dir.rglob("callgrind.*.out")):
        m = THREAD_RE.search(out.name)
        # No infix => single-threaded process => the one file is thread 1.
        thread = int(m.group(1)) if m else 1
        # The benchmark identity is the containing directory: <fn>.<bench_id>.
        bench_dir = out.parent
        group = bench_dir.parent.name
        name = f"{group}::{bench_dir.name}"
        ir, _ = parse_out_file(out)
        entry = benches.setdefault(
            name, {"instructions": 0, "main_thread": 0, "other_threads": 0, "threads": 0}
        )
        entry["instructions"] += ir
        entry["threads"] += 1
        if thread == 1:
            entry["main_thread"] += ir
        else:
            entry["other_threads"] += ir
    return benches


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--iai-dir", type=Path, default=Path("target/iai"))
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    if not args.iai_dir.exists():
        print(f"error: {args.iai_dir} does not exist", file=sys.stderr)
        return 1

    benches = collect(args.iai_dir)
    if not benches:
        print(f"error: no callgrind output found under {args.iai_dir}", file=sys.stderr)
        return 1

    zero = [n for n, e in benches.items() if e["instructions"] == 0]
    if zero:
        # Loud, because a zero here is the exact shape of a vacuously green run.
        print(
            f"WARNING: {len(zero)} benchmark(s) collected ZERO instructions: "
            + ", ".join(sorted(zero)),
            file=sys.stderr,
        )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(benches, indent=2, sort_keys=True))
    print(f"wrote {args.out} ({len(benches)} benchmarks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
