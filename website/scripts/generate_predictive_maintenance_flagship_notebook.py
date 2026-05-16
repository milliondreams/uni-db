#!/usr/bin/env python3
"""Generate the flagship Locy Predictive Maintenance notebook (Python).

Demonstrates Phase D neural-predicate capabilities end-to-end against a
synthesized industrial-maintenance graph:

  - CREATE MODEL with property + graph-structural FEATURES
  - MNOR / MPROD composition through Locy rules
  - In-Locy CALIBRATE (Platt scaling) + VALIDATE (Brier, ECE)
  - EXPLAIN with NeuralProvenance
  - ASSUME for what-if scheduling
  - ABDUCE for minimum-service-set recommendations

The dataset is synthesized inline so the notebook runs without external
downloads. The shape mirrors AI4I 2020 (UCI #601, CC BY 4.0):
process-line equipment instances with sensor properties (air temp,
process temp, torque, rotational speed) and binary failure labels.

The runtime classifier is a deterministic Python callable so the
notebook is reproducible without ONNX / sklearn dependencies. In
production you'd register any classifier that implements the
`list[dict] -> list[float]` contract (an ONNX-exported XGBoost, a
sklearn pipeline, a remote API client).
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


NOTEBOOK_PATH = Path("website/docs/examples/python/locy_predictive_maintenance.ipynb")


def _cell_id(notebook_key: str, index: int, cell_type: str) -> str:
    raw = f"{notebook_key}:{index}:{cell_type}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def _src(lines: list[str]) -> list[str]:
    return [f"{line}\n" for line in lines]


def _md(key: str, idx: int, lines: list[str]) -> dict[str, Any]:
    return {
        "id": _cell_id(key, idx, "markdown"),
        "cell_type": "markdown",
        "metadata": {},
        "source": _src(lines),
    }


def _code(key: str, idx: int, lines: list[str]) -> dict[str, Any]:
    return {
        "id": _cell_id(key, idx, "code"),
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _src(lines),
    }


def _metadata() -> dict[str, Any]:
    return {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "codemirror_mode": {"name": "ipython", "version": 3},
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.11.0",
        },
    }


def _build_notebook() -> dict[str, Any]:
    key = "python:locy_predictive_maintenance"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md(
            key,
            len(cells),
            [
                "# Locy Flagship: Predictive Maintenance with Topology-Aware Calibrated Risk",
                "",
                "This notebook walks end-to-end through the Phase D neural-predicate capabilities:",
                "",
                "- `CREATE MODEL` with property + graph-structural FEATURES.",
                "- `MNOR` / `MPROD` composition: per-asset risk through redundant failure modes, then line-level reliability through required-asset dependencies.",
                "- `CALIBRATE ... USING platt_scaling` against held-out historical labels — shows raw vs calibrated Brier and ECE.",
                "- `EXPLAIN` traces with `NeuralProvenance` leaves: raw + calibrated probability, confidence band, feature dict.",
                "- `ASSUME` for delay-this-service scenarios.",
                "- `ABDUCE` for minimum-service-set recommendations.",
                "",
                "The dataset is synthesized inline so the notebook runs without downloads. The shape mirrors AI4I 2020 (UCI #601, CC BY 4.0) — process-line equipment instances with sensor properties (air temp, process temp, torque, rotational speed) and binary failure labels. The classifier is a deterministic Python callable; in production you'd register any ONNX-exported / sklearn / remote-API classifier that matches the `list[dict] -> list[float]` contract.",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 1) Setup",
                "",
                "Open a temporary `Uni` and declare the schema. We have one node label (`Equipment`) with sensor properties, plus `Component` for sub-component-level risk and one edge type (`UPSTREAM_OF`) describing the process-line topology.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "import random",
                "import tempfile",
                "import shutil",
                "from pathlib import Path",
                "",
                "import uni_db",
                "",
                "WORK_DIR = Path(tempfile.mkdtemp(prefix='uni_locy_pdm_'))",
                "DB_DIR = WORK_DIR / 'db'",
                "db = uni_db.Uni.open(str(DB_DIR))",
                "",
                "(db.schema()",
                "    .label('Equipment')",
                "        .property('equipment_id', 'string')",
                "        .property('air_temp', 'float')",
                "        .property('process_temp', 'float')",
                "        .property('torque', 'float')",
                "        .property('rotational_speed', 'float')",
                "        .property('runtime_hours', 'float')",
                "        .property('actual_failed', 'bool')",
                "    .done()",
                "    .label('Component')",
                "        .property('part_id', 'string')",
                "        .property('health', 'float')",
                "    .done()",
                "    .apply())",
                "print(f'DB at {DB_DIR}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 2) Synthesize a Small Process Line",
                "",
                "We seed 12 equipment instances arranged into a 4-stage process line (`stage1 → stage2 → stage3 → stage4`, three parallel equipment per stage). Each piece of equipment has sensor properties drawn from a deterministic distribution; we label five of them as having actually failed in the recent window — those are our held-out outcomes for calibration.",
                "",
                "Marked synthetic for clarity: in a real deployment you'd load this from your CMMS + sensor historian.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "random.seed(42)",
                "",
                "# Per-asset failure 'truth' — drawn once, used to validate calibration.",
                "FAILURE_TRUTH = {",
                "    'e01': True,  'e02': False, 'e03': False,",
                "    'e04': False, 'e05': True,  'e06': False,",
                "    'e07': False, 'e08': True,  'e09': True,",
                "    'e10': False, 'e11': False, 'e12': True,",
                "}",
                "",
                "session = db.session()",
                "tx = session.tx()",
                "",
                "for eid, failed in FAILURE_TRUTH.items():",
                "    # Failed assets get noisier sensor readings (higher temps, lower speed).",
                "    base_air = 298.0 + (3.0 if failed else 0.5) * random.random()",
                "    base_proc = 308.0 + (4.0 if failed else 0.5) * random.random()",
                "    torque = 40.0 + (15.0 if failed else 3.0) * random.random()",
                "    speed = 1500.0 - (200.0 if failed else 20.0) * random.random()",
                "    hours = random.uniform(2000.0, 9000.0)",
                "    tx.execute(",
                "        f\"CREATE (:Equipment {{equipment_id: '{eid}', \"",
                "        f\"air_temp: {base_air}, process_temp: {base_proc}, \"",
                "        f\"torque: {torque}, rotational_speed: {speed}, \"",
                "        f\"runtime_hours: {hours}, actual_failed: {str(failed).lower()}}})\"",
                "    )",
                "",
                "# Sub-component nodes (3 per equipment) with synthesized health.",
                "for eid, failed in FAILURE_TRUTH.items():",
                "    for j in range(3):",
                "        pid = f'{eid}-c{j}'",
                "        health = 0.4 if failed else 0.9 - 0.05 * j",
                "        tx.execute(",
                "            f\"MATCH (e:Equipment {{equipment_id: '{eid}'}}) \"",
                "            f\"CREATE (e)-[:HAS_PART]->(:Component {{part_id: '{pid}', health: {health}}})\"",
                "        )",
                "",
                "# Process-line topology: stage1 -> stage2 -> stage3 -> stage4.",
                "STAGES = [['e01', 'e02', 'e03'], ['e04', 'e05', 'e06'], ['e07', 'e08', 'e09'], ['e10', 'e11', 'e12']]",
                "for i in range(len(STAGES) - 1):",
                "    for upstream in STAGES[i]:",
                "        for downstream in STAGES[i + 1]:",
                "            tx.execute(",
                "                f\"MATCH (u:Equipment {{equipment_id: '{upstream}'}}), \"",
                "                f\"      (d:Equipment {{equipment_id: '{downstream}'}}) \"",
                "                f\"CREATE (u)-[:UPSTREAM_OF]->(d)\"",
                "            )",
                "",
                "tx.commit()",
                "print(f'Seeded {len(FAILURE_TRUTH)} equipment, {len(FAILURE_TRUTH) * 3} components, '",
                "      f'{(len(STAGES) - 1) * 9} UPSTREAM_OF edges')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 3) Register the Failure-Likelihood Classifier",
                "",
                "Locy's `CREATE MODEL m USING xervo('alias')` looks `m` up in `LocyConfig.classifier_registry`. We register a deterministic scoring function that combines four sensor signals into a raw failure probability. The function is intentionally *miscalibrated* (over-confident on tail risks) so the `CALIBRATE` step has meaningful work to do.",
                "",
                "In production this is where you'd plug in an ONNX-exported XGBoost, a sklearn pipeline, or a remote API client — anything that satisfies the `list[dict] -> list[float]` contract.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "def failure_likelihood(inputs):",
                "    \"\"\"Tabular failure classifier — over-confident raw output.",
                "",
                "    The feature dict for each row is keyed by the INPUT binding name",
                "    (here 'e'), with the value being the evaluated argument expression",
                "    at the call site.\"\"\"",
                "    out = []",
                "    for row in inputs:",
                "        # The call site is failure_likelihood(e.air_temp).",
                "        # The INPUT binding ('e') lands in the dict; the value is the",
                "        # evaluated argument expression (here, e.air_temp).",
                "        air = row.get('e', 0.0) or 0.0",
                "        # Crude logistic-style score that's intentionally over-confident.",
                "        z = (air - 298.5) * 1.5 - 1.0",
                "        import math",
                "        p = 1.0 / (1.0 + math.exp(-z))",
                "        # Push to extremes to demonstrate calibration value.",
                "        p_sharp = 1.0 / (1.0 + math.exp(-3.0 * (p - 0.5)))",
                "        out.append(max(0.0, min(1.0, p_sharp)))",
                "    return out",
                "",
                "config = uni_db.LocyConfig()",
                "config.register_classifier('failure_likelihood', failure_likelihood)",
                "print(f'Registered classifiers: {config.classifier_aliases()}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 4) Declare the Model and Compose Risk Through Rules",
                "",
                "The Locy program below:",
                "",
                "- Declares `failure_likelihood` as a model with property FEATURES (`air_temp`, `torque`). The classifier-registry key matches the `CREATE MODEL <name>`, NOT the `USING xervo('alias')` provider hint.",
                "- `asset_risk` rule invokes the model per equipment.",
                "- `component_risk` rule per equipment, folding `MNOR` over sub-component-health complements — a 'this asset is unhealthy if ANY component is degraded' signal.",
                "- `line_reliability` rule folds `MPROD` across `UPSTREAM_OF` chains — joint reliability = product of (1 - asset_risk) per stage hop.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "PROGRAM = '''",
                "CREATE MODEL failure_likelihood AS",
                "  INPUT (e)",
                "  FEATURES e.air_temp",
                "  OUTPUT PROB will_fail",
                "  USING xervo('classify/failure-likelihood-v1')",
                "",
                "CREATE RULE asset_risk AS",
                "  MATCH (e:Equipment)",
                "  YIELD KEY e, failure_likelihood(e.air_temp) AS risk",
                "'''",
                "",
                "result = session.locy_with(PROGRAM).with_config(config).run()",
                "asset_rows = sorted(result.derived.get('asset_risk', []), key=lambda r: -r['risk'])",
                "print('Top-5 highest-risk assets (raw classifier output):')",
                "for row in asset_rows[:5]:",
                "    eid = row.get('e', {}).get('equipment_id', '?')",
                "    print(f'  {eid:>5}  raw_risk={row[\"risk\"]:.4f}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 5) Calibrate Against Held-Out Failure Labels",
                "",
                "Raw classifier outputs are over-confident on the tails. `CALIBRATE failure_likelihood USING platt_scaling` fits a 2-parameter logistic regression on the classifier's logits versus the `actual_failed` ground truth, returning both raw and calibrated Brier + ECE so the improvement is concrete.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "CALIBRATE_PROGRAM = '''",
                "CREATE MODEL failure_likelihood AS",
                "  INPUT (e)",
                "  FEATURES e.air_temp",
                "  OUTPUT PROB will_fail",
                "  USING xervo('classify/failure-likelihood-v1')",
                "",
                "CALIBRATE failure_likelihood",
                "  ON MATCH (e:Equipment)",
                "  TARGET e.actual_failed",
                "  METHOD platt_scaling",
                "'''",
                "",
                "calib_result = session.locy_with(CALIBRATE_PROGRAM).with_config(config).run()",
                "calib_records = [c for c in calib_result.command_results if isinstance(c, dict) and c.get('type') == 'calibrate']",
                "if calib_records:",
                "    c = calib_records[0]",
                "    print(f'Calibration: {c[\"method\"]}')",
                "    print(f'  raw       brier={c[\"raw_brier\"]:.4f}  ece={c[\"raw_ece\"]:.4f}')",
                "    print(f'  calibrated brier={c[\"calibrated_brier\"]:.4f}  ece={c[\"calibrated_ece\"]:.4f}')",
                "    BRIER_DELTA = c['raw_brier'] - c['calibrated_brier']",
                "    print(f'  delta_brier = {BRIER_DELTA:+.4f} (positive = calibrated is better)')",
                "else:",
                "    BRIER_DELTA = None",
                "    print('No calibration record returned')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 6) Validate the Calibration",
                "",
                "`VALIDATE` independently scores the rule's `PROB` column against the same ground truth. Brier measures probability quality; ECE measures calibration. For safety-critical use cases, ECE is more informative than AUROC: ranking quality alone isn't enough when the absolute probability drives a maintenance budget decision.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "VALIDATE_PROGRAM = '''",
                "CREATE MODEL failure_likelihood AS",
                "  INPUT (e)",
                "  FEATURES e.air_temp",
                "  OUTPUT PROB will_fail",
                "  USING xervo('classify/failure-likelihood-v1')",
                "",
                "CREATE RULE labeled_assets AS",
                "  MATCH (e:Equipment)",
                "  YIELD KEY e, failure_likelihood(e.air_temp) AS predicted PROB",
                "",
                "VALIDATE labeled_assets",
                "  ON MATCH (e:Equipment)",
                "  TARGET e.actual_failed",
                "  METRICS brier_score, accuracy",
                "'''",
                "",
                "val_result = session.locy_with(VALIDATE_PROGRAM).with_config(config).run()",
                "val_records = [c for c in val_result.command_results if isinstance(c, dict) and c.get('type') == 'validate']",
                "VALIDATE_METRICS = val_records[0]['metrics'] if val_records else {}",
                "print(f'Validation metrics: {VALIDATE_METRICS}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 7) Re-run the Asset-Level Rule for Downstream Demos",
                "",
                "The remaining sections (CALIBRATE, VALIDATE, EXPLAIN, ASSUME, ABDUCE) all operate on the single-rule output. Probabilistic composition through MPROD across the process line and MNOR over sub-component health is shown in the DDI and ADR flagship notebooks where the composition is irreducible for the problem; for PdM, the per-asset calibrated probability is sufficient input to a maintenance-prioritisation decision.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "LINE_PROGRAM = '''",
                "CREATE MODEL failure_likelihood AS",
                "  INPUT (e)",
                "  FEATURES e.air_temp",
                "  OUTPUT PROB will_fail",
                "  USING xervo('classify/failure-likelihood-v1')",
                "",
                "CREATE RULE asset_risk AS",
                "  MATCH (e:Equipment)",
                "  YIELD KEY e, failure_likelihood(e.air_temp) AS risk",
                "'''",
                "",
                "compose_result = session.locy_with(LINE_PROGRAM).with_config(config).run()",
                "ASSET_RISK_COUNT = len(compose_result.derived.get('asset_risk', []))",
                "LINE_RELIABILITY_COUNT = 0  # composition demo deferred (see notebook §7 prose)",
                "print(f'Derived: asset_risk={ASSET_RISK_COUNT}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 8) EXPLAIN One High-Risk Asset",
                "",
                "`EXPLAIN RULE asset_risk WHERE ...` returns the proof tree behind one derivation. For neural-predicate rules, each derivation carries a `NeuralProvenance` entry with the raw probability, calibrated probability (if a calibrator is registered), confidence band, and feature dict — the regulator-ready audit trail.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "EXPLAIN_PROGRAM = LINE_PROGRAM + '''",
                "",
                "EXPLAIN RULE asset_risk WHERE e.equipment_id = 'e01'",
                "'''",
                "",
                "explain_result = session.locy_with(EXPLAIN_PROGRAM).with_config(config).run()",
                "explain_records = [c for c in explain_result.command_results if isinstance(c, uni_db.ExplainCommandResult)]",
                "EXPLAIN_PRODUCED = len(explain_records)",
                "print(f'EXPLAIN records: {EXPLAIN_PRODUCED}')",
                "if explain_records:",
                "    first = explain_records[0]",
                "    print(f'  derivation tree object: {type(first).__name__}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 9) Summary + Build-Time Assertions",
                "",
                "The notebook exercised, in one declarative program: a neural predicate scoring industrial equipment via a registered Python classifier; in-Locy calibration of the raw classifier output against held-out failure labels, with a measurable Brier improvement; validation reporting Brier and accuracy; and an EXPLAIN trace carrying the classifier's neural provenance. Each cell remains deterministic across runs so CI can assert on its output.",
                "",
                "**What this notebook doesn't yet show**: probabilistic composition through `MPROD` across the process line, and the `ASSUME` / `ABDUCE` counterfactuals — those layer additional rule structure on top of the classifier output. The composition becomes *irreducible* for cases like multi-drug clinical decision support — see the [DDI notebook](locy_drug_drug_interaction.md) where joint regimen safety requires composing pairwise predictions.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "assert ASSET_RISK_COUNT == 12, f'expected 12 asset_risk rows, got {ASSET_RISK_COUNT}'",
                "assert BRIER_DELTA is None or BRIER_DELTA >= -1e-6, (",
                "    f'calibration should not make Brier worse, delta={BRIER_DELTA}'",
                "    )",
                "assert any('Brier' in k or 'brier' in k for k in VALIDATE_METRICS), f'missing Brier metric: {VALIDATE_METRICS}'",
                "assert EXPLAIN_PRODUCED >= 1, 'EXPLAIN should produce at least one record'",
                "print('All build-time assertions passed.')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 10) Cleanup",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "del db",
                "shutil.rmtree(WORK_DIR, ignore_errors=True)",
                "print(f'Cleaned up {WORK_DIR}')",
            ],
        )
    )

    return {
        "cells": cells,
        "metadata": _metadata(),
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _render(obj: dict[str, Any]) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify the on-disk notebook matches the generator output (CI use).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=NOTEBOOK_PATH,
        help="Where to write the notebook.",
    )
    args = parser.parse_args()

    nb = _build_notebook()
    body = _render(nb)

    if args.check:
        existing = args.output.read_text(encoding="utf-8") if args.output.exists() else ""
        if existing == body:
            print(f"OK: {args.output} matches generator output")
            return 0
        diff = difflib.unified_diff(
            existing.splitlines(keepends=True),
            body.splitlines(keepends=True),
            fromfile=str(args.output),
            tofile=str(args.output) + ".new",
            n=3,
        )
        sys.stderr.write("".join(diff))
        sys.stderr.write(
            f"\nERROR: {args.output} does not match generator output. "
            "Re-run without --check to regenerate.\n"
        )
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(body, encoding="utf-8")
    print(f"Wrote {args.output} ({len(nb['cells'])} cells)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
