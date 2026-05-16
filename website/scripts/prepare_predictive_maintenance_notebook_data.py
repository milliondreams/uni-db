#!/usr/bin/env python3
"""Prepare notebook-ready predictive-maintenance data from UCI AI4I 2020.

Mirrors the semiconductor prep-script pattern: download once to a cache
dir, filter to a small curated slice, write CSVs + manifest under
website/docs/examples/data/locy_predictive_maintenance/. The notebook
reads from the vendored CSVs at runtime — no network needed in CI.

Dataset: AI4I 2020 Predictive Maintenance (UCI #601, CC BY 4.0).
   Citation:  https://doi.org/10.24432/C5HS5C
   Download:  https://archive.ics.uci.edu/static/public/601/
              ai4i+2020+predictive+maintenance+dataset.zip
   Shape:     10 000 rows × 14 columns (sensor features + failure labels)
   Size:      ~510 KB unzipped CSV

Curated output:
   ai4i_equipment.csv   — 60 rows: 30 failed + 30 healthy, stratified
   ai4i_topology.csv    — synthetic 4-stage process-line edges
   ai4i_components.csv  — 3 synthetic components per equipment
   manifest.json        — source metadata, license, parameter dump
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import random
import urllib.request
import zipfile
from pathlib import Path

AI4I_ZIP_URL = (
    "https://archive.ics.uci.edu/static/public/601/"
    "ai4i+2020+predictive+maintenance+dataset.zip"
)
AI4I_CITATION = "https://doi.org/10.24432/C5HS5C"

# Map the original column names (with spaces / units) to snake_case
# matching what the notebook expects on its `Equipment` nodes.
COLUMN_RENAME = {
    "UDI": "udi",
    "Product ID": "product_id",
    "Type": "type",
    "Air temperature [K]": "air_temp_k",
    "Process temperature [K]": "process_temp_k",
    "Rotational speed [rpm]": "rotational_speed_rpm",
    "Torque [Nm]": "torque_nm",
    "Tool wear [min]": "tool_wear_min",
    "Machine failure": "actual_failed",
    "TWF": "twf_label",
    "HDF": "hdf_label",
    "PWF": "pwf_label",
    "OSF": "osf_label",
    "RNF": "rnf_label",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("website/docs/examples/data/locy_predictive_maintenance"),
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("website/.cache/ai4i"),
    )
    parser.add_argument(
        "--n-failed",
        type=int,
        default=30,
        help="How many failed rows to include in the curated slice.",
    )
    parser.add_argument(
        "--n-healthy",
        type=int,
        default=30,
        help="How many healthy rows to include in the curated slice.",
    )
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def _download_zip(cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    zip_path = cache_dir / "ai4i_2020.zip"
    if zip_path.exists():
        return zip_path
    print(f"Downloading {AI4I_ZIP_URL} -> {zip_path}")
    req = urllib.request.Request(
        AI4I_ZIP_URL, headers={"User-Agent": "uni-locy-data-prep"}
    )
    with urllib.request.urlopen(req) as response:
        zip_path.write_bytes(response.read())
    return zip_path


def _read_csv_from_zip(zip_path: Path) -> list[dict[str, str]]:
    with zipfile.ZipFile(zip_path) as zf:
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        with zf.open(csv_name) as f:
            text = f.read().decode("utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    return list(reader)


def _curate(
    rows: list[dict[str, str]], n_failed: int, n_healthy: int, seed: int
) -> list[dict[str, str]]:
    """Stratified sample of failed + healthy rows, snake_case columns."""
    rng = random.Random(seed)
    failed = [r for r in rows if r.get("Machine failure", "0") == "1"]
    healthy = [r for r in rows if r.get("Machine failure", "0") == "0"]
    rng.shuffle(failed)
    rng.shuffle(healthy)
    selected = failed[:n_failed] + healthy[:n_healthy]
    rng.shuffle(selected)
    out: list[dict[str, str]] = []
    for r in selected:
        renamed = {COLUMN_RENAME[k]: v for k, v in r.items() if k in COLUMN_RENAME}
        # Cast actual_failed to a proper bool string (true/false) for downstream
        # CSV → Locy ingest.
        renamed["actual_failed"] = (
            "true" if renamed.get("actual_failed", "0") == "1" else "false"
        )
        out.append(renamed)
    return out


def _synth_topology(equipment_ids: list[str], n_stages: int, seed: int) -> list[dict[str, str]]:
    """4-stage process line: equipment partitioned into stages; each
    upstream equipment connects to each downstream equipment of the
    next stage. Clearly marked as synthetic in the manifest."""
    rng = random.Random(seed + 1)
    shuffled = list(equipment_ids)
    rng.shuffle(shuffled)
    per_stage = len(shuffled) // n_stages
    stages = [shuffled[i * per_stage : (i + 1) * per_stage] for i in range(n_stages)]
    edges: list[dict[str, str]] = []
    for i in range(n_stages - 1):
        for u in stages[i]:
            for d in stages[i + 1]:
                edges.append({"upstream_id": u, "downstream_id": d})
    return edges


def _synth_components(
    equipment_ids: list[str], failed_ids: set[str], per_equipment: int, seed: int
) -> list[dict[str, str]]:
    """3 components per equipment with deterministic health values.
    Failed equipment gets lower component health to make the
    MNOR composition meaningful."""
    rng = random.Random(seed + 2)
    out: list[dict[str, str]] = []
    for eid in equipment_ids:
        is_failed = eid in failed_ids
        for j in range(per_equipment):
            health = (
                0.30 + 0.10 * j + rng.uniform(0.0, 0.10)
                if is_failed
                else 0.85 + 0.04 * j + rng.uniform(0.0, 0.05)
            )
            out.append(
                {
                    "part_id": f"{eid}-c{j}",
                    "equipment_id": eid,
                    "health": f"{min(0.99, max(0.01, health)):.4f}",
                }
            )
    return out


def _write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def main() -> int:
    args = parse_args()

    zip_path = _download_zip(args.cache_dir)
    raw_rows = _read_csv_from_zip(zip_path)
    print(f"Loaded {len(raw_rows)} raw AI4I rows from {zip_path.name}")

    curated = _curate(raw_rows, args.n_failed, args.n_healthy, args.seed)
    print(f"Curated {len(curated)} equipment rows ({args.n_failed} failed + {args.n_healthy} healthy)")

    eq_ids = [r["udi"] for r in curated]
    failed_ids = {r["udi"] for r in curated if r["actual_failed"] == "true"}

    topology = _synth_topology(eq_ids, n_stages=4, seed=args.seed)
    components = _synth_components(eq_ids, failed_ids, per_equipment=3, seed=args.seed)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        args.output_dir / "ai4i_equipment.csv",
        curated,
        list(COLUMN_RENAME.values()),
    )
    _write_csv(
        args.output_dir / "ai4i_topology.csv",
        topology,
        ["upstream_id", "downstream_id"],
    )
    _write_csv(
        args.output_dir / "ai4i_components.csv",
        components,
        ["part_id", "equipment_id", "health"],
    )

    manifest = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": {
            "name": "AI4I 2020 Predictive Maintenance Dataset",
            "doi": AI4I_CITATION,
            "url": AI4I_ZIP_URL,
            "license": "CC BY 4.0",
            "repository": "UCI Machine Learning Repository, dataset #601",
        },
        "shape": {
            "raw_rows": len(raw_rows),
            "curated_equipment": len(curated),
            "topology_edges": len(topology),
            "components": len(components),
        },
        "params": {
            "n_failed": args.n_failed,
            "n_healthy": args.n_healthy,
            "n_stages": 4,
            "components_per_equipment": 3,
            "seed": args.seed,
        },
        "notes": [
            "ai4i_equipment.csv carries real AI4I 2020 sensor rows.",
            "ai4i_topology.csv is SYNTHETIC: a 4-stage process line",
            "  partitioning the equipment, with all-to-all edges between",
            "  consecutive stages. AI4I has no real topology data.",
            "ai4i_components.csv is SYNTHETIC: 3 components per equipment",
            "  with deterministic health values (lower for failed equipment).",
        ],
    }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    readme = (
        "# Locy Predictive Maintenance Data\n\n"
        "Notebook-ready data for the Locy predictive-maintenance flagship.\n\n"
        "## Source\n\n"
        "- Dataset: AI4I 2020 Predictive Maintenance\n"
        f"- DOI: {AI4I_CITATION}\n"
        f"- Download URL: {AI4I_ZIP_URL}\n"
        "- Repository: UCI Machine Learning Repository, dataset #601\n"
        "- License: CC BY 4.0\n\n"
        "## Files\n\n"
        "- `ai4i_equipment.csv`: curated 60-row stratified sample of real AI4I rows.\n"
        "- `ai4i_topology.csv`: SYNTHETIC 4-stage process-line edges.\n"
        "- `ai4i_components.csv`: SYNTHETIC 3 components per equipment.\n"
        "- `manifest.json`: generation metadata + data shape.\n\n"
        "Regenerate with `python website/scripts/prepare_predictive_maintenance_notebook_data.py`.\n"
    )
    (args.output_dir / "README.md").write_text(readme, encoding="utf-8")
    print(f"Wrote curated data + manifest + README to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
