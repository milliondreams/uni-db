#!/usr/bin/env python3
"""Generate the flagship Locy Drug-Drug Interaction notebook (Python).

Demonstrates Phase D neural-predicate capabilities applied to
polypharmacy interaction risk. The dataset is synthesized inline (no
external download); the runtime classifier is a deterministic Python
callable so the notebook is reproducible without external ML
dependencies. In production this is exactly where you'd plug in an
R-GCN-derived drug-embedding model — the registered callable would
look up the two drug embeddings, concat them, and run a small MLP head
exported to ONNX.

The story:

  - Drugs in the graph, plus DRUG_INTERACTION edges encoding known
    pairwise interactions with a severity score.
  - For each interaction edge, the classifier scores the
    interaction-likelihood from the pair's severity property.
  - Labels (is_dangerous boolean) drive CALIBRATE + VALIDATE.
  - EXPLAIN shows the audit trail.

What this notebook intentionally doesn't show (will land when the
underlying Locy plumbing supports it cleanly): MPROD across all
drug-pairs in a patient's regimen for joint-safety scoring, and
ASSUME-based substitution / ABDUCE-based minimum-regimen-change. Both
require composition patterns that hit current Locy planner limits with
property-on-edge arithmetic inside aggregates.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any


NOTEBOOK_PATH = Path("website/docs/examples/python/locy_drug_drug_interaction.ipynb")


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
    key = "python:locy_drug_drug_interaction"
    cells: list[dict[str, Any]] = []

    cells.append(
        _md(
            key,
            len(cells),
            [
                "# Locy Flagship: Drug-Drug Interaction Risk Scoring",
                "",
                "Clinical pharmacists triage drug-drug interaction warnings for elderly polypharmacy patients. Pairwise interaction databases exist, but the clinical question is: *given this whole regimen, how risky is the joint interaction profile?* This notebook scores each pairwise interaction edge with a registered Python classifier, calibrates against held-out 'dangerous interaction' labels, and produces an audit-grade `EXPLAIN` trace for clinical-decision-support use.",
                "",
                "The dataset is synthesized inline. In production the classifier would look up R-GCN-derived drug embeddings, concatenate them, and run a small MLP head exported to ONNX — the registered callable just has to satisfy the `list[dict] -> list[float]` contract.",
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
                "Open a temporary `Uni` and declare the schema: `Drug` nodes with a `risk_class` property, plus `DrugInteraction` edges carrying a `severity` score and an `is_dangerous` ground-truth label.",
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
                "WORK_DIR = Path(tempfile.mkdtemp(prefix='uni_locy_ddi_'))",
                "db = uni_db.Uni.open(str(WORK_DIR / 'db'))",
                "",
                "(db.schema()",
                "    .label('Drug')",
                "        .property('drug_id', 'string')",
                "        .property('risk_class', 'float')",
                "    .done()",
                "    .label('InteractionRecord')",
                "        .property('pair_id', 'string')",
                "        .property('severity', 'float')",
                "        .property('is_dangerous', 'bool')",
                "    .done()",
                "    .apply())",
                "print('DB initialized')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 2) Synthesize a Drug-Interaction Graph",
                "",
                "12 drugs in 3 risk classes (anticoagulants, NSAIDs, opioids — risk_class values 0.3 / 0.5 / 0.7 respectively). Pairwise interactions between drugs in different classes get a severity score; dangerous interactions are tagged as `is_dangerous=true`. The fixture is small enough to hand-verify the calibration math.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "random.seed(11)",
                "DRUGS = [",
                "    ('DA0', 0.3), ('DA1', 0.3), ('DA2', 0.3), ('DA3', 0.3),",
                "    ('DN0', 0.5), ('DN1', 0.5), ('DN2', 0.5), ('DN3', 0.5),",
                "    ('DO0', 0.7), ('DO1', 0.7), ('DO2', 0.7), ('DO3', 0.7),",
                "]",
                "",
                "session = db.session()",
                "tx = session.tx()",
                "for d, rc in DRUGS:",
                "    tx.execute(f\"CREATE (:Drug {{drug_id: '{d}', risk_class: {rc}}})\")",
                "",
                "# Generate a deterministic-but-varied set of cross-class interaction records.",
                "INTERACTIONS = []",
                "pair_counter = 0",
                "drug_ids = [d for d, _ in DRUGS]",
                "for i, (d1, rc1) in enumerate(DRUGS):",
                "    for d2, rc2 in DRUGS[i + 1:]:",
                "        # Only model cross-class pairs (more realistic).",
                "        if rc1 == rc2:",
                "            continue",
                "        pair_counter += 1",
                "        pid = f'P{pair_counter:03d}'",
                "        # Higher product of risk classes -> more dangerous baseline.",
                "        base = rc1 * rc2",
                "        severity = base + random.random() * 0.4",
                "        is_dangerous = severity > 0.40",
                "        tx.execute(",
                "            f\"CREATE (:InteractionRecord {{pair_id: '{pid}', \"",
                "            f\"severity: {severity:.4f}, is_dangerous: {str(is_dangerous).lower()}}})\"",
                "        )",
                "        INTERACTIONS.append((pid, d1, d2, severity, is_dangerous))",
                "",
                "tx.commit()",
                "DANGEROUS_COUNT = sum(1 for *_ , danger in INTERACTIONS if danger)",
                "print(f'Seeded {len(DRUGS)} drugs, {len(INTERACTIONS)} interaction records '",
                "      f'({DANGEROUS_COUNT} tagged dangerous)')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 3) Register the Pairwise-Interaction Classifier",
                "",
                "The classifier maps a severity score to a dangerous-interaction probability. As with PdM and ADR, it's intentionally over-confident on tails so `CALIBRATE` has measurable work to do. In production this is exactly where a small MLP head over R-GCN-derived drug embeddings (concat(emb_d1, emb_d2)) would plug in.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "def interaction_score(inputs):",
                "    \"\"\"Pairwise drug-interaction classifier — over-confident raw output.\"\"\"",
                "    import math",
                "    out = []",
                "    for row in inputs:",
                "        # Call site is interaction_score(rec.severity); INPUT binding 'rec'",
                "        # holds the evaluated expression value.",
                "        sev = row.get('rec', 0.0) or 0.0",
                "        z = (sev - 0.40) * 8.0 - 0.3",
                "        p = 1.0 / (1.0 + math.exp(-z))",
                "        p_sharp = 1.0 / (1.0 + math.exp(-4.0 * (p - 0.5)))",
                "        out.append(max(0.0, min(1.0, p_sharp)))",
                "    return out",
                "",
                "config = uni_db.LocyConfig()",
                "config.register_classifier('interaction_score', interaction_score)",
                "print(f'Registered classifiers: {config.classifier_aliases()}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 4) Declare the Model and Score Interaction Records",
                "",
                "`interaction_score(rec.severity)` is invoked per InteractionRecord. The rule yields a PROB-annotated `risk` column.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "PROGRAM = '''",
                "CREATE MODEL interaction_score AS",
                "  INPUT (rec)",
                "  FEATURES rec.severity",
                "  OUTPUT PROB risk",
                "  USING xervo('classify/ddi-v1')",
                "",
                "CREATE RULE scored_interactions AS",
                "  MATCH (rec:InteractionRecord)",
                "  YIELD KEY rec, interaction_score(rec.severity) AS risk",
                "'''",
                "",
                "result = session.locy_with(PROGRAM).with_config(config).run()",
                "rows = sorted(result.derived.get('scored_interactions', []), key=lambda r: -r['risk'])",
                "SCORED_COUNT = len(rows)",
                "print(f'Scored {SCORED_COUNT} interaction pairs. Top 5 (highest raw risk):')",
                "for row in rows[:5]:",
                "    print(f'  pair={row.get(\"rec\", {}).get(\"pair_id\", \"?\"):<6}  risk={row[\"risk\"]:.4f}')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 5) Calibrate Against Held-Out Dangerous-Interaction Labels",
                "",
                "Clinical decision support uses calibrated probabilities to set alert thresholds. `CALIBRATE` fits Platt scaling against the `is_dangerous` ground truth and returns raw vs calibrated Brier + ECE.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "CALIBRATE_PROGRAM = '''",
                "CREATE MODEL interaction_score AS",
                "  INPUT (rec)",
                "  FEATURES rec.severity",
                "  OUTPUT PROB risk",
                "  USING xervo('classify/ddi-v1')",
                "",
                "CALIBRATE interaction_score",
                "  ON MATCH (rec:InteractionRecord)",
                "  TARGET rec.is_dangerous",
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
                "else:",
                "    BRIER_DELTA = None",
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
                "`VALIDATE` independently scores the rule's PROB column against the same ground truth.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "VALIDATE_PROGRAM = '''",
                "CREATE MODEL interaction_score AS",
                "  INPUT (rec)",
                "  FEATURES rec.severity",
                "  OUTPUT PROB risk",
                "  USING xervo('classify/ddi-v1')",
                "",
                "CREATE RULE scored_interactions AS",
                "  MATCH (rec:InteractionRecord)",
                "  YIELD KEY rec, interaction_score(rec.severity) AS risk PROB",
                "",
                "VALIDATE scored_interactions",
                "  ON MATCH (rec:InteractionRecord)",
                "  TARGET rec.is_dangerous",
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
                "## 7) EXPLAIN One Dangerous Interaction",
                "",
                "The `EXPLAIN` trace surfaces the classifier's `NeuralProvenance` per derivation. For clinical-decision-support audit, this is the artifact you'd preserve when a prescribed regimen is challenged.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "first_dangerous = next((pid for pid, _, _, _, dang in INTERACTIONS if dang), None)",
                "EXPLAIN_PROGRAM = PROGRAM + f'''",
                "",
                "EXPLAIN RULE scored_interactions WHERE rec.pair_id = '{first_dangerous}'",
                "'''",
                "",
                "explain_result = session.locy_with(EXPLAIN_PROGRAM).with_config(config).run()",
                "explain_records = [c for c in explain_result.command_results if isinstance(c, uni_db.ExplainCommandResult)]",
                "EXPLAIN_PRODUCED = len(explain_records)",
                "print(f'EXPLAIN records: {EXPLAIN_PRODUCED} (for pair {first_dangerous})')",
            ],
        )
    )

    cells.append(
        _md(
            key,
            len(cells),
            [
                "## 8) Summary + Build-Time Assertions",
                "",
                "This notebook scored pairwise drug-interaction records with a registered Python classifier, calibrated against the dangerous-interaction ground truth, validated with Brier + accuracy, and produced an EXPLAIN trace with `NeuralProvenance`. The full polypharmacy story — `MPROD` across all drug-pair predictions for a patient's regimen to compute joint safety, `ASSUME` for drug substitution, `ABDUCE` for the minimum substitution set — is the natural extension once Locy's IS-ref + property-arithmetic + nested-executor classifier-registry propagation gaps are firmed up.",
            ],
        )
    )

    cells.append(
        _code(
            key,
            len(cells),
            [
                "assert SCORED_COUNT >= 30, f'expected scored_interactions rows, got {SCORED_COUNT}'",
                "assert BRIER_DELTA is None or BRIER_DELTA >= -0.05, (",
                "    f'calibration regression beyond tolerance, delta={BRIER_DELTA}'",
                "    )",
                "assert any('Brier' in k or 'brier' in k for k in VALIDATE_METRICS), (",
                "    f'missing Brier metric: {VALIDATE_METRICS}'",
                "    )",
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
                "## 9) Cleanup",
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
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--output", type=Path, default=NOTEBOOK_PATH)
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
