#!/usr/bin/env python3
"""Guard: the crates.io publish list in `.github/workflows/release.yml` must be
uni-db's complete publishable dependency closure, in topological order.

`cargo publish` resolves each crate's dependencies against the crates.io index,
so a workspace crate can only be published after every workspace crate it
depends on (normal + build deps; dev-deps are stripped). The release workflow
publishes a hardcoded ordered list. When a new workspace crate is added to
uni-db's dependency graph (e.g. the 2.0 plugin framework) but not to that list,
the release fails mid-publish with "no matching package named ...". This check
recomputes the closure from `cargo metadata` and compares it to the list.

Requires `cargo` on PATH. Exits non-zero with a diff on mismatch.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RELEASE_YML = REPO / ".github/workflows/release.yml"
ROOT_CRATE = "uni-db"


def publishable_closure() -> tuple[set[str], dict[str, set[str]]]:
    """Return (closure, normal+build dep graph) for ROOT_CRATE's workspace deps."""
    meta = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"], cwd=REPO
        )
    )
    pkgs = {p["name"]: p for p in meta["packages"]}
    internal = set(pkgs)
    nopublish = {n for n, p in pkgs.items() if p.get("publish") == []}

    graph: dict[str, set[str]] = {}
    for name, p in pkgs.items():
        graph[name] = {
            d["name"]
            for d in p["dependencies"]
            if d["name"] in internal and d["kind"] in (None, "build")
        }

    seen: set[str] = set()
    stack = [ROOT_CRATE]
    while stack:
        c = stack.pop()
        if c in seen:
            continue
        seen.add(c)
        stack.extend(graph.get(c, ()))
    closure = {c for c in seen if c not in nopublish}
    return closure, graph


def publish_list_from_workflow() -> list[str]:
    text = RELEASE_YML.read_text()
    # Match invocations like `publish_crate uni-foo`, ignoring the function def.
    return re.findall(r"^\s*publish_crate\s+([a-z0-9][a-z0-9-]*)\s*$", text, re.MULTILINE)


def main() -> int:
    closure, graph = publishable_closure()
    listed = publish_list_from_workflow()
    listed_set = set(listed)

    ok = True

    missing = closure - listed_set
    if missing:
        ok = False
        print(f"ERROR: publishable crates MISSING from release.yml: {sorted(missing)}", file=sys.stderr)

    extra = listed_set - closure
    if extra:
        ok = False
        print(f"ERROR: release.yml lists crates NOT in {ROOT_CRATE}'s publishable closure: {sorted(extra)}", file=sys.stderr)

    if len(listed) != len(listed_set):
        ok = False
        dupes = sorted({c for c in listed if listed.count(c) > 1})
        print(f"ERROR: release.yml publishes crates more than once: {dupes}", file=sys.stderr)

    # Topological order: each crate must appear after its in-closure deps.
    pos = {c: i for i, c in enumerate(listed)}
    for c in listed:
        for dep in graph.get(c, ()):
            if dep in closure and dep in pos and pos[dep] > pos[c]:
                ok = False
                print(f"ERROR: {c} is published before its dependency {dep}", file=sys.stderr)

    if not ok:
        print(
            f"\nUpdate the `publish_crate ...` list in {RELEASE_YML.relative_to(REPO)} "
            f"to {ROOT_CRATE}'s full publishable dependency closure in topological order.",
            file=sys.stderr,
        )
        return 1

    print(f"OK: release.yml publishes all {len(closure)} crates in {ROOT_CRATE}'s closure, in topological order.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
