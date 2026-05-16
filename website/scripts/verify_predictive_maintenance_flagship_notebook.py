#!/usr/bin/env python3
"""Execute and validate the flagship predictive-maintenance Locy notebook.

CI helper: walks each code cell, exec()s it in a shared namespace, and
asserts that the notebook's own build-time assertions pass. The notebook
synthesizes its dataset inline and uses a deterministic Python classifier
so the run is fully reproducible and requires no external downloads.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import contextlib
from pathlib import Path


DEFAULT_NOTEBOOK = Path(
    "website/docs/examples/python/locy_predictive_maintenance.ipynb"
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
                # Surface cell context for triage.
                sys.stderr.write(buf.getvalue())
                raise AssertionError(
                    f"Notebook cell {idx} raised {type(e).__name__}: {e}"
                ) from e
    return env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--notebook",
        type=Path,
        default=DEFAULT_NOTEBOOK,
        help="Path to the notebook (default: predictive-maintenance flagship).",
    )
    args = parser.parse_args()

    if not args.notebook.exists():
        sys.stderr.write(
            f"ERROR: notebook not found at {args.notebook}. "
            "Run generate_predictive_maintenance_flagship_notebook.py first.\n"
        )
        return 1

    env = execute_notebook(args.notebook)

    # The notebook's own assertions cell would have already raised on
    # failure; reaching here means everything passed. We additionally
    # spot-check a few key bindings to lock the example to deterministic
    # outputs against future drift.

    asset_risk_count = env.get("ASSET_RISK_COUNT")
    validate_metrics = env.get("VALIDATE_METRICS")
    explain_produced = env.get("EXPLAIN_PRODUCED")

    assert asset_risk_count == 12, f"ASSET_RISK_COUNT={asset_risk_count!r}"
    assert isinstance(validate_metrics, dict) and any(
        "Brier" in k or "brier" in k for k in validate_metrics
    ), f"VALIDATE_METRICS missing Brier: {validate_metrics!r}"
    assert (
        isinstance(explain_produced, int) and explain_produced >= 1
    ), f"EXPLAIN_PRODUCED={explain_produced!r}"

    print(
        "Predictive Maintenance flagship notebook validation passed.\n"
        f"Summary: asset_risk={asset_risk_count}, "
        f"validate_metrics={validate_metrics}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
