#!/usr/bin/env python3
"""Execute and validate the flagship cyber exposure Locy notebook."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
from pathlib import Path


DEFAULT_NOTEBOOK = Path("website/docs/examples/python/locy_cyber_exposure_twin.ipynb")


def _require(name: str, env: dict[str, object]) -> object:
    if name not in env:
        raise AssertionError(
            f"Expected variable '{name}' to be defined by notebook execution."
        )
    return env[name]


def _as_int(name: str, value: object) -> int:
    if isinstance(value, bool):
        raise AssertionError(f"Expected '{name}' to be int-like, got bool.")
    if isinstance(value, int):
        return value
    raise AssertionError(
        f"Expected '{name}' to be int-like, got {type(value).__name__}."
    )


def _as_list(name: str, value: object) -> list[object]:
    if isinstance(value, list):
        return value
    raise AssertionError(f"Expected '{name}' to be a list, got {type(value).__name__}.")


def execute_notebook(
    notebook_path: Path,
    write_outputs: bool = False,
) -> tuple[dict[str, object], dict[str, object]]:
    nb = json.loads(notebook_path.read_text(encoding="utf-8"))
    env: dict[str, object] = {"__name__": "__main__"}
    exec_count = 1

    for idx, cell in enumerate(nb.get("cells", []), start=1):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if write_outputs:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                exec(compile(source, f"{notebook_path.name}:cell{idx}", "exec"), env)
            text = buf.getvalue()
            outputs: list[dict[str, object]] = []
            if text:
                outputs.append(
                    {
                        "name": "stdout",
                        "output_type": "stream",
                        "text": text,
                    }
                )
            cell["outputs"] = outputs
            cell["execution_count"] = exec_count
            exec_count += 1
        else:
            exec(compile(source, f"{notebook_path.name}:cell{idx}", "exec"), env)

    return env, nb


def validate_results(env: dict[str, object]) -> dict[str, int]:
    hybrid_rows = _as_list("hybrid_rows", _require("hybrid_rows", env))
    team_rollup = _as_list("team_rollup", _require("team_rollup", env))
    blast_rows = _as_list("blast_rows", _require("blast_rows", env))
    best_plan_rows = _as_list("best_plan_rows", _require("best_plan_rows", env))
    mods = _as_list("mods", _require("mods", env))

    total_critical_assets = _as_int(
        "total_critical_assets", _require("total_critical_assets", env)
    )
    contained_critical_assets = _as_int(
        "contained_critical_assets", _require("contained_critical_assets", env)
    )
    residual_critical_assets = _as_int(
        "residual_critical_assets", _require("residual_critical_assets", env)
    )

    if not hybrid_rows:
        raise AssertionError("Expected non-empty hybrid retrieval rows.")
    if not team_rollup:
        raise AssertionError("Expected non-empty team rollup rows.")
    if not blast_rows:
        raise AssertionError("Expected non-empty blast rows.")
    if not best_plan_rows:
        raise AssertionError("Expected non-empty best plan rows.")
    if total_critical_assets <= 0:
        raise AssertionError("Expected total_critical_assets > 0.")
    if contained_critical_assets <= 0:
        raise AssertionError(
            "Expected contained_critical_assets > 0 for ASSUME scenario."
        )
    if residual_critical_assets >= total_critical_assets:
        raise AssertionError(
            "Expected residual_critical_assets < total_critical_assets."
        )
    if not mods:
        raise AssertionError("Expected ABDUCE to produce at least one modification.")
    if not any(isinstance(item, dict) and item.get("validated") for item in mods):
        raise AssertionError("Expected at least one validated ABDUCE modification.")

    tree = _require("tree", env)
    if not isinstance(tree, dict) or not tree.get("rule"):
        raise AssertionError("Expected EXPLAIN RULE to produce a derivation tree.")
    children = tree.get("children")
    if not isinstance(children, list) or not children:
        raise AssertionError("Expected EXPLAIN RULE tree to include child derivations.")

    return {
        "hybrid_rows": len(hybrid_rows),
        "team_rollup_rows": len(team_rollup),
        "blast_rows": len(blast_rows),
        "best_plan_rows": len(best_plan_rows),
        "total_critical_assets": total_critical_assets,
        "contained_critical_assets": contained_critical_assets,
        "residual_critical_assets": residual_critical_assets,
        "abduce_candidates": len(mods),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--notebook",
        type=Path,
        default=DEFAULT_NOTEBOOK,
        help="Path to notebook to execute and validate.",
    )
    parser.add_argument(
        "--write-outputs",
        action="store_true",
        help="Write execution outputs back into the notebook file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    notebook_path = args.notebook.resolve()
    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook not found: {notebook_path}")

    os.environ.setdefault(
        "LOCY_DATA_DIR",
        str((notebook_path.parent.parent / "data/locy_cyber_exposure_twin").resolve()),
    )

    env, nb = execute_notebook(notebook_path, write_outputs=args.write_outputs)
    summary = validate_results(env)
    if args.write_outputs:
        notebook_path.write_text(
            json.dumps(nb, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        print(f"Wrote executed outputs to: {notebook_path}")
    print("Cyber flagship notebook validation passed.")
    print(
        "Summary:",
        f"hybrid_rows={summary['hybrid_rows']},",
        f"team_rollup_rows={summary['team_rollup_rows']},",
        f"blast_rows={summary['blast_rows']},",
        f"best_plan_rows={summary['best_plan_rows']},",
        f"critical_assets={summary['total_critical_assets']},",
        f"contained={summary['contained_critical_assets']},",
        f"residual={summary['residual_critical_assets']},",
        f"abduce_candidates={summary['abduce_candidates']}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
