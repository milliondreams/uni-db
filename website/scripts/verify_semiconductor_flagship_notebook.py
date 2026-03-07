#!/usr/bin/env python3
"""Execute and validate the flagship semiconductor Locy notebook."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
from pathlib import Path


DEFAULT_NOTEBOOK = Path("website/docs/examples/python/locy_semiconductor_yield_excursion.ipynb")


def _require(name: str, env: dict[str, object]) -> object:
    if name not in env:
        raise AssertionError(f"Expected variable '{name}' to be defined by notebook execution.")
    return env[name]


def _as_int(name: str, value: object) -> int:
    if isinstance(value, bool):
        raise AssertionError(f"Expected '{name}' to be int-like, got bool.")
    if isinstance(value, int):
        return value
    raise AssertionError(f"Expected '{name}' to be int-like, got {type(value).__name__}.")


def _as_str(name: str, value: object) -> str:
    if isinstance(value, str) and value.strip():
        return value
    raise AssertionError(f"Expected '{name}' to be a non-empty string.")


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


def validate_results(env: dict[str, object]) -> dict[str, int | str]:
    hot_tool = _as_str("hot_tool", _require("hot_tool", env))
    hot_tool_rows = _as_list("hot_tool_rows", _require("hot_tool_rows", env))
    if len(hot_tool_rows) < 3:
        raise AssertionError("Expected at least 3 hotspot tool rows from baseline ranking.")

    total_fail_lots = _as_int("total_fail_lots", _require("total_fail_lots", env))
    contained_fail_lots = _as_int("contained_fail_lots", _require("contained_fail_lots", env))
    residual_fail_lots = _as_int("residual_fail_lots", _require("residual_fail_lots", env))
    if total_fail_lots <= 0:
        raise AssertionError("Expected total_fail_lots > 0.")
    if contained_fail_lots <= 0:
        raise AssertionError("Expected contained_fail_lots > 0 for meaningful ASSUME output.")
    if residual_fail_lots >= total_fail_lots:
        raise AssertionError("Expected residual_fail_lots < total_fail_lots.")

    mods = _as_list("mods", _require("mods", env))
    if not mods:
        raise AssertionError("Expected ABDUCE to produce at least one modification.")
    if not any(isinstance(item, dict) and item.get("validated") for item in mods):
        raise AssertionError("Expected at least one validated ABDUCE modification.")

    tree = _require("tree", env)
    if not isinstance(tree, dict) or not tree.get("rule"):
        raise AssertionError("Expected EXPLAIN RULE to produce a derivation tree with a rule name.")
    children = tree.get("children")
    if not isinstance(children, list) or not children:
        raise AssertionError("Expected EXPLAIN RULE tree to include child derivations.")

    return {
        "hot_tool": hot_tool,
        "total_fail_lots": total_fail_lots,
        "contained_fail_lots": contained_fail_lots,
        "residual_fail_lots": residual_fail_lots,
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
        str((notebook_path.parent.parent / "data/locy_semiconductor_yield_excursion").resolve()),
    )

    env, nb = execute_notebook(notebook_path, write_outputs=args.write_outputs)
    summary = validate_results(env)
    if args.write_outputs:
        notebook_path.write_text(json.dumps(nb, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote executed outputs to: {notebook_path}")
    print("Flagship notebook validation passed.")
    print(
        "Summary:",
        f"hot_tool={summary['hot_tool']},",
        f"fail_lots={summary['total_fail_lots']},",
        f"contained={summary['contained_fail_lots']},",
        f"residual={summary['residual_fail_lots']},",
        f"abduce_candidates={summary['abduce_candidates']}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
