#!/usr/bin/env python3
"""Convert Jupyter notebooks to Markdown for Zensical documentation build.

Zensical does not support the mkdocs-jupyter plugin, so we pre-convert
all .ipynb files referenced in the nav to .md files using nbconvert.

Usage:
    python website/scripts/convert_notebooks.py

Run this before `zensical build` or `zensical serve`.
"""

import subprocess
import sys
from pathlib import Path

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"


def find_notebooks(docs_dir: Path) -> list[Path]:
    """Find all .ipynb files under the docs directory."""
    return sorted(docs_dir.rglob("*.ipynb"))


def convert_notebook(notebook_path: Path) -> Path:
    """Convert a single notebook to markdown using nbconvert."""
    output_md = notebook_path.with_suffix(".md")

    cmd = [
        sys.executable,
        "-m",
        "nbconvert",
        "--to",
        "markdown",
        "--output",
        output_md.name,
        str(notebook_path),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR converting {notebook_path.name}: {result.stderr.strip()}")
        return None

    return output_md


def main():
    notebooks = find_notebooks(DOCS_DIR)
    if not notebooks:
        print("No notebooks found.")
        return

    print(f"Converting {len(notebooks)} notebooks to markdown...")

    converted = 0
    failed = 0
    for nb in notebooks:
        rel = nb.relative_to(DOCS_DIR)
        print(f"  {rel} ", end="")
        out = convert_notebook(nb)
        if out:
            print(f"-> {out.name}")
            converted += 1
        else:
            failed += 1

    print(f"\nDone: {converted} converted, {failed} failed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
