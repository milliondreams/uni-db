#!/usr/bin/env python3
"""Generate the Python API symbol reference from `__init__.pyi`.

Why this exists
---------------
The tree carried **two** hand-maintained references for one surface — the
published `website/docs/reference/python-api.md` and the unpublished
`docs/complete_python_api.md` — 10k lines between them, neither generated. They
drifted independently, which is why each had invented methods the other lacked
(`xervo.rerank`, `session.profile_with`). Hand-merging them would have
propagated fiction from both into one file.

`__init__.pyi` is the single thing the bindings are actually typed against, so
it is the only honest source. This emits the complete symbol surface from it.
Prose, quick starts and worked examples stay in the hand-written guide; this
page is the exhaustive index the guide links into.

Usage
-----
    python3 scripts/gen_python_api_reference.py           # write the page
    python3 scripts/gen_python_api_reference.py --check   # CI: fail if stale
"""

from __future__ import annotations

import ast
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYI = ROOT / "bindings/uni-db/uni_db/__init__.pyi"
OUT = ROOT / "website/docs/reference/python-api-symbols.md"

BANNER = (
    "<!-- GENERATED FILE — DO NOT EDIT.\n"
    "     Regenerate with: python3 scripts/gen_python_api_reference.py\n"
    "     Source of truth: bindings/uni-db/uni_db/__init__.pyi -->\n"
)


def workspace_version() -> str:
    with (ROOT / "Cargo.toml").open("rb") as fh:
        return str(tomllib.load(fh)["workspace"]["package"]["version"])


def first_line(node: ast.AST) -> str:
    """First sentence of a docstring, flattened to one line."""
    doc = ast.get_docstring(node)  # type: ignore[arg-type]
    if not doc:
        return ""
    line = " ".join(doc.strip().splitlines()[0].split())
    return line.replace("|", r"\|")


def render_signature(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """Reconstruct `name(args) -> ret` from the stub's AST."""
    try:
        args = ast.unparse(fn.args)
    except Exception:  # noqa: BLE001 - defensive; stub syntax is ours
        args = "..."
    # `self` is noise in a method table.
    for prefix in ("self, ", "self"):
        if args.startswith(prefix):
            args = args[len(prefix) :]
            break
    ret = f" -> {ast.unparse(fn.returns)}" if fn.returns else ""
    prefix = "async " if isinstance(fn, ast.AsyncFunctionDef) else ""
    return f"{prefix}{fn.name}({args}){ret}"


def is_public(name: str) -> bool:
    return not name.startswith("_") or name in {"__enter__", "__exit__", "__getitem__"}


def build() -> str:
    tree = ast.parse(PYI.read_text())
    classes = [n for n in tree.body if isinstance(n, ast.ClassDef)]

    out: list[str] = [
        BANNER,
        "# Python API — Symbol Reference\n",
        f"Complete symbol surface of the `uni_db` Python bindings, "
        f"**generated from `bindings/uni-db/uni_db/__init__.pyi`** at version "
        f"{workspace_version()}.\n",
        "This page is exhaustive and always in sync with the type stubs — it is "
        "regenerated in CI. For narrative documentation, worked examples and the "
        "recommended patterns, start at the [Python API guide](python-api.md).\n",
        f"**{len(classes)} classes.**\n",
        "---\n",
    ]

    for cls in sorted(classes, key=lambda c: c.name):
        if not is_public(cls.name):
            continue
        bases = ", ".join(ast.unparse(b) for b in cls.bases)
        heading = f"## `{cls.name}`" + (f" — extends `{bases}`" if bases else "")
        out.append(heading + "\n")
        if summary := first_line(cls):
            out.append(summary + "\n")

        methods = [
            n
            for n in cls.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and is_public(n.name)
        ]
        if methods:
            out.append("| Signature | Description |")
            out.append("|---|---|")
            for m in methods:
                decorators = {
                    d.id for d in m.decorator_list if isinstance(d, ast.Name)
                }
                mark = ""
                if "staticmethod" in decorators:
                    mark = " *(static)*"
                elif "property" in decorators:
                    mark = " *(property)*"
                out.append(
                    f"| `{render_signature(m)}`{mark} | {first_line(m) or '—'} |"
                )
            out.append("")

        fields = [
            n
            for n in cls.body
            if isinstance(n, ast.AnnAssign)
            and isinstance(n.target, ast.Name)
            and is_public(n.target.id)
        ]
        if fields:
            out.append("**Attributes**\n")
            out.append("| Name | Type |")
            out.append("|---|---|")
            for f in fields:
                assert isinstance(f.target, ast.Name)
                out.append(f"| `{f.target.id}` | `{ast.unparse(f.annotation)}` |")
            out.append("")

        out.append("---\n")

    return "\n".join(out) + "\n"


def main() -> int:
    content = build()
    check = "--check" in sys.argv

    if check:
        current = OUT.read_text() if OUT.exists() else ""
        if current != content:
            print(f"{OUT.relative_to(ROOT)} is STALE.")
            print("Regenerate with: python3 scripts/gen_python_api_reference.py")
            if current:
                diff = subprocess.run(
                    ["diff", "-u", str(OUT), "-"],
                    input=content,
                    text=True,
                    capture_output=True,
                )
                print(diff.stdout[:3000])
            return 1
        print(f"OK: {OUT.relative_to(ROOT)} is up to date")
        return 0

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(content)
    print(f"Wrote {OUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
