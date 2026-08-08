#!/usr/bin/env python3
"""Assert counts asserted in documentation match the counts asserted in source.

The plugin surface count is the recurring offender: docs said "23 extension
surfaces" in six places while `uni_plugin::surfaces` asserts
`assert_eq!(kinds.len(), 22)`. The structural fact was already pinned in code —
the prose simply was not wired to it.

This wires them. Ground truth is read out of the source assertion itself, so
adding or removing a surface fails here until the docs are updated, rather than
silently drifting.

Historical, dated documents (release notes, `docs/proposals/**`, `docs/plans/**`)
are excluded: they record what was true when published and must not be rewritten.

Exit code 0 = consistent, 1 = drift.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SURFACES_RS = ROOT / "crates/uni-plugin/src/surfaces/mod.rs"

# Directories whose contents are dated records, not live documentation.
EXCLUDED_PARTS = ("release_notes", "proposals", "plans", "archive")

DOC_ROOTS = ("website/docs", "docs", "skills")

# Only *total* claims are checked: `N extension surfaces`, or `all N surfaces`.
#
# Two things this must NOT flag:
#   * subset counts — "the other 19 surfaces are Rust-only" is a correct
#     statement about the non-portable remainder, not a claim about the total;
#   * `surfaces` used as a verb — "line 561 surfaces the same error".
#
# Requiring either the literal word `extension` or a preceding `all` picks out
# the total-claiming phrasings and leaves both of the above alone.
SURFACE_CLAIM = re.compile(
    r"\ball\s+(\d{1,3})\s+surfaces\b|\b(\d{1,3})\s+extension\s+surfaces\b", re.I
)


def source_surface_count() -> int:
    m = re.search(r"assert_eq!\(\s*kinds\.len\(\),\s*(\d+)\s*\)", SURFACES_RS.read_text())
    if not m:
        raise SystemExit(
            f"could not find the surface-count assertion in {SURFACES_RS}; "
            "update this script if the assertion moved"
        )
    return int(m.group(1))


def live_docs() -> list[Path]:
    out: list[Path] = []
    for root in DOC_ROOTS:
        for p in (ROOT / root).rglob("*.md"):
            rel = p.relative_to(ROOT)
            if any(part in EXCLUDED_PARTS for part in rel.parts):
                continue
            out.append(p)
    return sorted(out)


def main() -> int:
    want = source_surface_count()
    failures: list[str] = []

    for path in live_docs():
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            for m in SURFACE_CLAIM.finditer(line):
                got = int(m.group(1) or m.group(2))
                if got != want:
                    failures.append(
                        f"{path.relative_to(ROOT)}:{lineno} claims {got} surfaces, "
                        f"source asserts {want} ({m.group(0)!r})"
                    )

    if failures:
        print("Documented-count check FAILED:\n")
        for f in failures:
            print(f"  - {f}")
        print(
            f"\nGround truth is `assert_eq!(kinds.len(), {want})` in "
            f"{SURFACES_RS.relative_to(ROOT)}.\n"
            "Update the prose, or update the assertion if a surface was genuinely "
            "added or removed."
        )
        return 1

    print(f"OK: documented surface count matches source ({want})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
