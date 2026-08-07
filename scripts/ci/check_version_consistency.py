#!/usr/bin/env python3
"""Assert every Python-visible version tracks the workspace Cargo version.

`release.yml` already checks `bindings/uni-pydantic/pyproject.toml`, but only at
release time and only for that one file. Two gaps let a wrong version ship:

  * `uni_pydantic.__version__` was a hand-written literal that nothing checked.
    It drifted to `2.5.0` against a `3.3.0` package — the exact failure
    `docs/releasing_version_bump.md` warned was un-enforced.
  * The release-time check runs too late to stop the drift being merged.

This runs in `release-guards` on every PR. It asserts the pyproject version
matches the workspace, and that `__init__.py` derives `__version__` rather than
restating it, so the duplicate cannot come back.

Exit code 0 = consistent, 1 = drift.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PYDANTIC_INIT = ROOT / "bindings/uni-pydantic/src/uni_pydantic/__init__.py"
PYDANTIC_PYPROJECT = ROOT / "bindings/uni-pydantic/pyproject.toml"

# A literal assignment such as `__version__ = "2.5.0"`. A derived one
# (`__version__ = _package_version()`) has no quotes and is what we want.
LITERAL_VERSION = re.compile(r"""^__version__\s*=\s*["'][^"']+["']""", re.M)


def workspace_version() -> str:
    with (ROOT / "Cargo.toml").open("rb") as fh:
        return str(tomllib.load(fh)["workspace"]["package"]["version"])


def main() -> int:
    failures: list[str] = []
    want = workspace_version()

    with PYDANTIC_PYPROJECT.open("rb") as fh:
        got = str(tomllib.load(fh)["project"]["version"])
    if got != want:
        failures.append(
            f"bindings/uni-pydantic/pyproject.toml version {got!r} != workspace {want!r}"
        )

    init_src = PYDANTIC_INIT.read_text()
    if (m := LITERAL_VERSION.search(init_src)) is not None:
        failures.append(
            f"{PYDANTIC_INIT.relative_to(ROOT)} hardcodes {m.group(0).strip()!r}. "
            "Derive it from package metadata instead — a literal here is a second "
            "source of truth and drifts silently."
        )

    if failures:
        print("Version consistency check FAILED:\n")
        for f in failures:
            print(f"  - {f}")
        print(f"\nWorkspace version is {want!r}.")
        print("See docs/releasing_version_bump.md for the bump checklist.")
        return 1

    print(f"OK: Python package versions consistent at {want}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
