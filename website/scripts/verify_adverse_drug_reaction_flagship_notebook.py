#!/usr/bin/env python3
"""Execute and validate the flagship ADR Locy notebook."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
from pathlib import Path


DEFAULT_NOTEBOOK = Path(
    "website/docs/examples/python/locy_adverse_drug_reaction.ipynb"
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
            "Run generate_adverse_drug_reaction_flagship_notebook.py first.\n"
        )
        return 1

    env = execute_notebook(args.notebook)

    scored_count = env.get("SCORED_COUNT")
    mechanistic_path_count = env.get("MECHANISTIC_PATH_COUNT")
    mechanism_plausibility_count = env.get("MECHANISM_PLAUSIBILITY_COUNT")
    investigation_queue_len = env.get("INVESTIGATION_QUEUE_LEN")
    validate_metrics = env.get("VALIDATE_METRICS")
    explain_produced = env.get("EXPLAIN_PRODUCED")

    assert isinstance(scored_count, int) and scored_count >= 80, (
        f"SCORED_COUNT={scored_count!r}"
    )
    # Counts come from the real Hetionet subgraph extract — the
    # mechanistic-path traversal sees the full 6-hop product across
    # 30 compounds × 60 genes × 40 pathways.
    assert (
        isinstance(mechanistic_path_count, int) and mechanistic_path_count >= 100
    ), f"MECHANISTIC_PATH_COUNT={mechanistic_path_count!r}"
    assert (
        isinstance(mechanism_plausibility_count, int) and mechanism_plausibility_count >= 20
    ), f"MECHANISM_PLAUSIBILITY_COUNT={mechanism_plausibility_count!r}"
    assert (
        isinstance(investigation_queue_len, int) and investigation_queue_len >= 5
    ), f"INVESTIGATION_QUEUE_LEN={investigation_queue_len!r}"
    assert isinstance(validate_metrics, dict) and any(
        "Brier" in k or "brier" in k for k in validate_metrics
    ), f"VALIDATE_METRICS missing Brier: {validate_metrics!r}"
    assert (
        isinstance(explain_produced, int) and explain_produced >= 1
    ), f"EXPLAIN_PRODUCED={explain_produced!r}"

    print(
        "ADR signal-detection flagship notebook validation passed.\n"
        f"Summary: scored_reports={scored_count}, "
        f"mechanistic_path={mechanistic_path_count}, "
        f"mechanism_plausibility={mechanism_plausibility_count}, "
        f"investigation_queue={investigation_queue_len}, "
        f"validate_metrics={validate_metrics}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
