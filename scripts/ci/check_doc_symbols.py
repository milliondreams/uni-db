#!/usr/bin/env python3
"""Assert Python methods named in the docs actually exist in `__init__.pyi`.

Two hand-maintained references drifting independently is how `xervo.rerank`
(published) and `session.profile_with` (unpublished) came to be documented —
neither has ever existed. Generating the symbol reference
(`gen_python_api_reference.py`) fixes coverage; this fixes the opposite failure,
where prose invents a method the surface does not have.

Scope is deliberately narrow to stay signal-only: it checks calls on a small set
of well-known receiver names (`db`, `session`, `tx`, `xervo`, ...) inside
```python fences of live documentation. Anything else is ignored.

Exit code 0 = every referenced method exists, 1 = an invented method.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PYI = ROOT / "bindings/uni-db/uni_db/__init__.pyi"
# The Pydantic OGM is a *separate* package with its own session type, so its
# methods (`add`, `add_all`, `sync_schema`, ...) are legitimate on a `session`
# receiver in OGM documentation. Both surfaces count as known.
OGM_SRC = ROOT / "bindings/uni-pydantic/src/uni_pydantic"

DOC_ROOTS = ("website/docs", "skills")
# `plugins/loaders` snippets run *inside* a guest plugin, where `db` is the
# plugin registrar rather than `uni_db.Uni`, so its receivers are unrelated.
EXCLUDED_PARTS = ("release_notes", "proposals", "plans", "archive", "examples", "loaders")

# Receivers whose type we are confident about.
RECEIVERS = {
    "db", "session", "tx", "txn", "xervo", "fork", "primary",
    "async_db", "async_session",
}

FENCE = re.compile(r"^\s*```(\w+)")
CALL = re.compile(r"\b(" + "|".join(sorted(RECEIVERS)) + r")\.([a-z_][a-z0-9_]*)\s*\(")

# Attribute-style facades that return another object; the *next* call is on that
# object, so `db.schema().label(...)` should check `schema` not `label`.
# Handled by only checking the first attribute after a known receiver.


def known_methods() -> set[str]:
    """Every public method/function name across both documented packages."""
    names: set[str] = set()
    sources = [PYI, *sorted(OGM_SRC.glob("*.py"))]
    for src in sources:
        try:
            tree = ast.parse(src.read_text())
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                names.add(node.name)
    return names


def live_docs() -> list[Path]:
    out: list[Path] = []
    for root in DOC_ROOTS:
        for p in (ROOT / root).rglob("*.md"):
            rel = p.relative_to(ROOT)
            if any(part in EXCLUDED_PARTS for part in rel.parts):
                continue
            out.append(p)
    return sorted(out)


def python_blocks(text: str):
    """Yield (start_line, block_text) for each ```python fence."""
    lang = None
    buf: list[str] = []
    start = 0
    for i, line in enumerate(text.splitlines(), 1):
        if line.lstrip().startswith("```"):
            if lang in ("python", "py"):
                yield start, "\n".join(buf)
            m = FENCE.match(line)
            lang = m.group(1) if m else None
            buf, start = [], i
        else:
            buf.append(line)
    if lang in ("python", "py"):
        yield start, "\n".join(buf)


def main() -> int:
    known = known_methods()
    failures: list[str] = []

    for path in live_docs():
        text = path.read_text()
        for start, block in python_blocks(text):
            for m in CALL.finditer(block):
                receiver, method = m.group(1), m.group(2)
                if method in known:
                    continue
                offset = block[: m.start()].count("\n")
                failures.append(
                    f"{path.relative_to(ROOT)}:{start + offset + 1} "
                    f"`{receiver}.{method}(` — no `{method}` in __init__.pyi"
                )

    if failures:
        print("Doc symbol check FAILED — documented methods that do not exist:\n")
        for f in dict.fromkeys(failures):
            print(f"  - {f}")
        print(
            f"\nGround truth is {PYI.relative_to(ROOT)}. Either the method was "
            "renamed/removed, or the doc invented it."
        )
        return 1

    print("OK: every documented Python method exists in __init__.pyi")
    return 0


if __name__ == "__main__":
    sys.exit(main())
