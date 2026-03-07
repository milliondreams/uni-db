#!/usr/bin/env python3
"""Prepare notebook-ready pharma batch genealogy data from Figshare sources."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import urllib.request
from pathlib import Path

PROCESS_URL = "https://ndownloader.figshare.com/files/30874192"
LAB_URL = "https://ndownloader.figshare.com/files/30966250"
COLLECTION_URL = "https://figshare.com/collections/_/5645578"
DOI_URL = "https://doi.org/10.1038/s41597-022-01203-x"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("website/docs/examples/data/locy_pharma_batch_genealogy"),
        help="Directory for generated notebook data files.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("website/.cache/pharma_batch_genealogy"),
        help="Directory for downloaded raw files.",
    )
    parser.add_argument(
        "--product-code",
        default="17",
        help="Product code to focus on (must exist in both process and laboratory files).",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=180,
        help="Maximum batches to include in the prepared dataset.",
    )
    parser.add_argument(
        "--top-cases",
        type=int,
        default=30,
        help="How many high-risk batches to include in notebook case rows.",
    )
    return parser.parse_args()


def _download(url: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return path
    req = urllib.request.Request(url, headers={"User-Agent": "uni-locy-data-prep"})
    with urllib.request.urlopen(req, timeout=60) as response:
        path.write_bytes(response.read())
    return path


def _read_semicolon_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter=";"))


def _f(value: str) -> float | None:
    token = value.strip().replace(",", ".")
    if not token:
        return None
    try:
        return float(token)
    except ValueError:
        return None


def _s(value: str) -> str:
    return value.strip()


def _batch_int(row: dict[str, str]) -> int:
    token = _s(row.get("batch", "0"))
    return int(float(token)) if token else 0


def _format_float(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.8f}"
    return str(value)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _format_float(v) for k, v in row.items()})


def _risk_components(
    dissolution_min: float | None,
    residual_solvent: float | None,
    impurities_total: float | None,
    batch_yield: float | None,
    total_waste: float | None,
    startup_waste: float | None,
    speed_change: float | None,
    weekend: bool,
) -> tuple[float, list[str], bool]:
    score = 0.0
    reasons: list[str] = []
    hard_deviation = False

    if dissolution_min is not None and dissolution_min < 83.0:
        score += 2.0 + max(0.0, (83.0 - dissolution_min) / 5.0)
        reasons.append("low_dissolution")
        if dissolution_min < 82.0:
            hard_deviation = True
    if residual_solvent is not None and residual_solvent > 0.10:
        score += 2.0 + max(0.0, (residual_solvent - 0.10) / 0.05)
        reasons.append("high_residual_solvent")
        if residual_solvent > 0.12:
            hard_deviation = True
    if impurities_total is not None and impurities_total > 0.17:
        score += 2.0 + max(0.0, (impurities_total - 0.17) / 0.10)
        reasons.append("high_total_impurities")
        if impurities_total > 0.20:
            hard_deviation = True
    if batch_yield is not None and batch_yield < 97.2:
        score += 1.0 + max(0.0, (97.2 - batch_yield))
        reasons.append("low_batch_yield")
    if total_waste is not None and total_waste > 2400.0:
        score += 0.8
        reasons.append("high_total_waste")
    if startup_waste is not None and startup_waste > 4500.0:
        score += 0.8
        reasons.append("high_startup_waste")
    if speed_change is not None and speed_change > 8.0:
        score += 0.6
        reasons.append("unstable_tablet_speed")
    if weekend:
        score += 0.4
        reasons.append("weekend_campaign")

    return score, reasons, hard_deviation


def main() -> int:
    args = parse_args()
    output_dir: Path = args.output_dir
    cache_dir: Path = args.cache_dir

    process_path = _download(PROCESS_URL, cache_dir / "Process.csv")
    lab_path = _download(LAB_URL, cache_dir / "Laboratory.csv")

    process_rows = _read_semicolon_csv(process_path)
    lab_rows = _read_semicolon_csv(lab_path)
    lab_by_key = {(_s(row["batch"]), _s(row["code"])): row for row in lab_rows}

    joined: list[dict[str, object]] = []
    for p in process_rows:
        code = _s(p.get("code", ""))
        if code != args.product_code:
            continue
        key = (_s(p.get("batch", "")), code)
        lab_row = lab_by_key.get(key)
        if lab_row is None:
            continue

        batch_num = _batch_int(p)
        batch_id = f"B{code}-{batch_num:04d}"
        dissolution_min = _f(lab_row.get("dissolution_min", ""))
        residual_solvent = _f(lab_row.get("resodual_solvent", ""))
        impurities_total = _f(lab_row.get("impurities_total", ""))
        batch_yield = _f(lab_row.get("batch_yield", ""))
        total_waste = _f(p.get("total_waste", ""))
        startup_waste = _f(p.get("startup_waste", ""))
        speed_change = _f(p.get("tbl_speed_change", ""))
        weekend = _s(p.get("weekend", "")).lower() == "yes"

        score, reasons, hard_deviation = _risk_components(
            dissolution_min=dissolution_min,
            residual_solvent=residual_solvent,
            impurities_total=impurities_total,
            batch_yield=batch_yield,
            total_waste=total_waste,
            startup_waste=startup_waste,
            speed_change=speed_change,
            weekend=weekend,
        )

        joined.append(
            {
                "batch_num": batch_num,
                "batch_id": batch_id,
                "product_code": code,
                "start": _s(lab_row.get("start", "")),
                "strength": _s(lab_row.get("strength", "")),
                "size": _s(lab_row.get("size", "")),
                "api_code": _s(lab_row.get("api_code", "")),
                "api_batch": _s(lab_row.get("api_batch", "")),
                "smcc_batch": _s(lab_row.get("smcc_batch", "")),
                "lactose_batch": _s(lab_row.get("lactose_batch", "")),
                "starch_batch": _s(lab_row.get("starch_batch", "")),
                "dissolution_min": dissolution_min,
                "residual_solvent": residual_solvent,
                "impurities_total": impurities_total,
                "batch_yield": batch_yield,
                "total_waste": total_waste,
                "startup_waste": startup_waste,
                "tbl_speed_change": speed_change,
                "fom_change": _f(p.get("fom_change", "")),
                "srel_production_max": _f(p.get("SREL_production_max", "")),
                "weekend": weekend,
                "deviation_score": score,
                "deviation_reasons": ",".join(reasons),
                "hard_deviation": hard_deviation,
            }
        )

    joined.sort(key=lambda row: int(row["batch_num"]))
    if not joined:
        raise ValueError(f"No joined rows found for product code '{args.product_code}'.")
    joined = joined[: args.max_batches]

    scores = sorted(float(row["deviation_score"]) for row in joined)
    cutoff_idx = int(max(0, min(len(scores) - 1, round(len(scores) * 0.82))))
    soft_cutoff = scores[cutoff_idx]
    for row in joined:
        is_dev = bool(row["hard_deviation"]) or float(row["deviation_score"]) >= soft_cutoff
        row["quality_state"] = "DEVIATION" if is_dev else "IN_SPEC"
        row["process_risk"] = min(0.98, 0.10 + float(row["deviation_score"]) / 8.0)

    batch_rows: list[dict[str, object]] = []
    for idx, row in enumerate(joined, start=1):
        batch_rows.append(
            {
                "batch_id": row["batch_id"],
                "batch_num": row["batch_num"],
                "campaign_pos": idx,
                "product_code": row["product_code"],
                "start": row["start"],
                "strength": row["strength"],
                "size": row["size"],
                "quality_state": row["quality_state"],
                "deviation_score": row["deviation_score"],
                "deviation_reasons": row["deviation_reasons"],
                "process_risk": row["process_risk"],
                "dissolution_min": row["dissolution_min"],
                "residual_solvent": row["residual_solvent"],
                "impurities_total": row["impurities_total"],
                "batch_yield": row["batch_yield"],
                "total_waste": row["total_waste"],
                "startup_waste": row["startup_waste"],
                "tbl_speed_change": row["tbl_speed_change"],
            }
        )

    material_rows: list[dict[str, object]] = []
    usage_rows: list[dict[str, object]] = []
    material_seen: dict[str, dict[str, object]] = {}

    material_fields = [
        ("API", "api_batch", 1.0),
        ("SMCC", "smcc_batch", 0.75),
        ("LACTOSE", "lactose_batch", 0.65),
        ("STARCH", "starch_batch", 0.55),
    ]
    for row in joined:
        batch_id = str(row["batch_id"])
        for material_type, field, criticality in material_fields:
            source_lot = str(row[field]) if row[field] else "UNKNOWN"
            material_lot_id = f"{material_type}-{row['product_code']}-{source_lot}"
            intrinsic = min(
                0.95,
                0.10
                + (0.35 if material_type == "API" else 0.15)
                + 0.08 * float(row["deviation_score"]),
            )
            material = material_seen.get(material_lot_id)
            if material is None:
                material = {
                    "material_lot_id": material_lot_id,
                    "material_type": material_type,
                    "source_lot": source_lot,
                    "intrinsic_risk": intrinsic,
                    "batches_seen": 1,
                }
                material_seen[material_lot_id] = material
            else:
                material["batches_seen"] = int(material["batches_seen"]) + 1
                material["intrinsic_risk"] = max(float(material["intrinsic_risk"]), intrinsic)

            usage_rows.append(
                {
                    "material_lot_id": material_lot_id,
                    "batch_id": batch_id,
                    "criticality_weight": criticality,
                }
            )

    material_rows.extend(sorted(material_seen.values(), key=lambda x: str(x["material_lot_id"])))

    campaign_edges: list[dict[str, object]] = []
    for i in range(len(joined) - 1):
        src = joined[i]
        dst = joined[i + 1]
        startup = float(src["startup_waste"] or 0.0)
        startup_factor = min(1.0, max(0.0, (startup - 1500.0) / 5000.0))
        carry = min(0.95, 0.20 + 0.55 * float(src["process_risk"]) + 0.25 * startup_factor)
        campaign_edges.append(
            {
                "src_batch_id": src["batch_id"],
                "dst_batch_id": dst["batch_id"],
                "carry_risk": carry,
            }
        )
        if i + 2 < len(joined) and i % 5 == 0:
            campaign_edges.append(
                {
                    "src_batch_id": src["batch_id"],
                    "dst_batch_id": joined[i + 2]["batch_id"],
                    "carry_risk": max(0.05, carry * 0.55),
                }
            )

    action_rows: list[dict[str, object]] = []
    action_templates = [
        ("deep_clean_hold", 14.0, 6.0, 0.72),
        ("targeted_retest", 6.0, 2.0, 0.48),
        ("release_with_sampling", 2.5, 1.0, 0.26),
    ]
    for row in joined:
        if row["quality_state"] != "DEVIATION":
            continue
        base = float(row["process_risk"])
        for action_type, base_cost, downtime, mitigation in action_templates:
            action_rows.append(
                {
                    "action_id": f"{row['batch_id']}::{action_type}",
                    "batch_id": row["batch_id"],
                    "action_type": action_type,
                    "cost_index": base_cost + (2.0 if row["weekend"] else 0.0),
                    "downtime_hours": downtime,
                    "mitigation_factor": mitigation,
                    "residual_risk_estimate": max(0.01, base * (1.0 - mitigation)),
                }
            )

    top_cases = sorted(
        (row for row in batch_rows if row["quality_state"] == "DEVIATION"),
        key=lambda row: (-float(row["deviation_score"]), str(row["batch_id"])),
    )[: args.top_cases]
    notebook_cases_rows = [
        {
            "batch_id": row["batch_id"],
            "deviation_score": row["deviation_score"],
            "deviation_reasons": row["deviation_reasons"],
        }
        for row in top_cases
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        output_dir / "pharma_batches.csv",
        [
            "batch_id",
            "batch_num",
            "campaign_pos",
            "product_code",
            "start",
            "strength",
            "size",
            "quality_state",
            "deviation_score",
            "deviation_reasons",
            "process_risk",
            "dissolution_min",
            "residual_solvent",
            "impurities_total",
            "batch_yield",
            "total_waste",
            "startup_waste",
            "tbl_speed_change",
        ],
        batch_rows,
    )
    _write_csv(
        output_dir / "pharma_material_lots.csv",
        ["material_lot_id", "material_type", "source_lot", "intrinsic_risk", "batches_seen"],
        material_rows,
    )
    _write_csv(
        output_dir / "pharma_usage_edges.csv",
        ["material_lot_id", "batch_id", "criticality_weight"],
        usage_rows,
    )
    _write_csv(
        output_dir / "pharma_campaign_edges.csv",
        ["src_batch_id", "dst_batch_id", "carry_risk"],
        campaign_edges,
    )
    _write_csv(
        output_dir / "pharma_action_plans.csv",
        [
            "action_id",
            "batch_id",
            "action_type",
            "cost_index",
            "downtime_hours",
            "mitigation_factor",
            "residual_risk_estimate",
        ],
        action_rows,
    )
    _write_csv(
        output_dir / "pharma_notebook_cases.csv",
        ["batch_id", "deviation_score", "deviation_reasons"],
        notebook_cases_rows,
    )

    manifest = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "source": {
            "name": "Pharma batch process/lab dataset collection",
            "collection_url": COLLECTION_URL,
            "paper_doi": DOI_URL,
            "files": {
                "process_csv": PROCESS_URL,
                "laboratory_csv": LAB_URL,
            },
            "license": "CC BY 4.0",
        },
        "shape": {
            "batches": len(batch_rows),
            "deviation_batches": sum(1 for row in batch_rows if row["quality_state"] == "DEVIATION"),
            "material_lots": len(material_rows),
            "usage_edges": len(usage_rows),
            "campaign_edges": len(campaign_edges),
            "action_plans": len(action_rows),
            "notebook_cases": len(notebook_cases_rows),
        },
        "parameters": {
            "product_code": args.product_code,
            "max_batches": args.max_batches,
            "top_cases": args.top_cases,
        },
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {output_dir / 'pharma_batches.csv'} ({len(batch_rows)} rows)")
    print(f"wrote {output_dir / 'pharma_material_lots.csv'} ({len(material_rows)} rows)")
    print(f"wrote {output_dir / 'pharma_usage_edges.csv'} ({len(usage_rows)} rows)")
    print(f"wrote {output_dir / 'pharma_campaign_edges.csv'} ({len(campaign_edges)} rows)")
    print(f"wrote {output_dir / 'pharma_action_plans.csv'} ({len(action_rows)} rows)")
    print(f"wrote {output_dir / 'pharma_notebook_cases.csv'} ({len(notebook_cases_rows)} rows)")
    print(f"wrote {output_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
