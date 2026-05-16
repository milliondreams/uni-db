#!/usr/bin/env python3
"""Execute and validate the flagship DDI Locy notebook."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
from pathlib import Path


DEFAULT_NOTEBOOK = Path(
    "website/docs/examples/python/locy_drug_drug_interaction.ipynb"
)


def execute_notebook(path: Path) -> dict[str, object]:
    nb = json.loads(path.read_text(encoding="utf-8"))
    env: dict[str, object] = {"__name__": "__main__"}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        for idx, cell in enumerate(nb.get("cells", []), start=1):
            if cell.get("cell_type") != "code":
                continue
            source = "".join(cell.get("source", []))
            try:
                exec(compile(source, f"{path.name}:cell{idx}", "exec"), env)
            except Exception as e:
                sys.stderr.write(buf.getvalue())
                raise AssertionError(
                    f"Notebook cell {idx} raised {type(e).__name__}: {e}"
                ) from e
    return env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--notebook", type=Path, default=DEFAULT_NOTEBOOK)
    args = parser.parse_args()

    if not args.notebook.exists():
        sys.stderr.write(
            f"ERROR: notebook not found at {args.notebook}. "
            "Run generate_drug_drug_interaction_flagship_notebook.py first.\n"
        )
        return 1

    env = execute_notebook(args.notebook)

    scored_count = env.get("SCORED_COUNT")
    validate_metrics = env.get("VALIDATE_METRICS")
    explain_produced = env.get("EXPLAIN_PRODUCED")

    assert isinstance(scored_count, int) and scored_count >= 30, (
        f"SCORED_COUNT={scored_count!r}"
    )
    assert isinstance(validate_metrics, dict) and any(
        "Brier" in k or "brier" in k for k in validate_metrics
    ), f"VALIDATE_METRICS missing Brier: {validate_metrics!r}"
    assert (
        isinstance(explain_produced, int) and explain_produced >= 1
    ), f"EXPLAIN_PRODUCED={explain_produced!r}"

    print(
        "DDI flagship notebook validation passed.\n"
        f"Summary: scored_interactions={scored_count}, "
        f"validate_metrics={validate_metrics}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
