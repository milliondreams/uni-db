#!/usr/bin/env python3
"""Prepare notebook-ready semiconductor yield excursion data from UCI SECOM."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import urllib.request
import zipfile
from pathlib import Path

SECOM_ZIP_URL = "https://archive.ics.uci.edu/static/public/179/secom.zip"
SECOM_CITATION = "https://doi.org/10.24432/C54305"
LABEL_PATTERN = re.compile(r'^(-?1)\s+"([^"]+)"$')
MODULES = ("lithography", "etch", "deposition", "implant", "cmp", "test")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("website/docs/examples/data/locy_semiconductor_yield_excursion"),
        help="Directory for generated notebook data files.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("website/.cache/secom"),
        help="Directory for downloaded raw files.",
    )
    parser.add_argument(
        "--top-k-features",
        type=int,
        default=18,
        help="How many highest effect-size features to mark as selected.",
    )
    parser.add_argument(
        "--min-class-count",
        type=int,
        default=40,
        help="Minimum non-null samples required in each class for feature selection.",
    )
    parser.add_argument(
        "--zscore-threshold",
        type=float,
        default=2.0,
        help="Absolute z-score threshold for excursion events.",
    )
    return parser.parse_args()


def _download_zip(cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    zip_path = cache_dir / "secom.zip"
    if zip_path.exists():
        return zip_path

    req = urllib.request.Request(
        SECOM_ZIP_URL, headers={"User-Agent": "uni-locy-data-prep"}
    )
    with urllib.request.urlopen(req) as response:
        zip_path.write_bytes(response.read())
    return zip_path


def _load_labels(zf: zipfile.ZipFile) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    raw = zf.read("secom_labels.data").decode("utf-8")
    for i, line in enumerate(raw.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        match = LABEL_PATTERN.match(line)
        if not match:
            raise ValueError(f"Unexpected labels format at line {i}: {line!r}")
        label_token, ts_token = match.groups()
        ts = dt.datetime.strptime(ts_token, "%d/%m/%Y %H:%M:%S")
        rows.append(
            {
                "row_index": i - 1,
                "lot_id": f"LOT_{i:04d}",
                "yield_code": int(label_token),
                "yield_outcome": "FAIL" if int(label_token) == 1 else "PASS",
                "test_timestamp": ts.isoformat(),
            }
        )
    return rows


def _parse_value(token: str) -> float | None:
    token = token.strip()
    if token == "NaN":
        return None
    return float(token)


def _load_features(zf: zipfile.ZipFile) -> list[list[float | None]]:
    rows: list[list[float | None]] = []
    raw = zf.read("secom.data").decode("utf-8")
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append([_parse_value(token) for token in line.split()])
    return rows


def _module_for_feature(index: int, num_features: int) -> str:
    bucket_size = math.ceil(num_features / len(MODULES))
    bucket = min(index // bucket_size, len(MODULES) - 1)
    return MODULES[bucket]


def _tool_for_feature(index: int, module: str) -> str:
    return f"{module}-tool-{(index % 8) + 1:02d}"


def _compute_stats(
    feature_rows: list[list[float | None]], labels: list[dict[str, object]]
) -> list[dict[str, object]]:
    num_features = len(feature_rows[0])
    pass_count = [0] * num_features
    pass_sum = [0.0] * num_features
    pass_sq_sum = [0.0] * num_features
    fail_count = [0] * num_features
    fail_sum = [0.0] * num_features
    fail_sq_sum = [0.0] * num_features

    for row, label in zip(feature_rows, labels):
        is_fail = label["yield_code"] == 1
        for idx, value in enumerate(row):
            if value is None:
                continue
            if is_fail:
                fail_count[idx] += 1
                fail_sum[idx] += value
                fail_sq_sum[idx] += value * value
            else:
                pass_count[idx] += 1
                pass_sum[idx] += value
                pass_sq_sum[idx] += value * value

    out: list[dict[str, object]] = []
    for idx in range(num_features):
        c_pass = pass_count[idx]
        c_fail = fail_count[idx]
        mean_pass = pass_sum[idx] / c_pass if c_pass else None
        mean_fail = fail_sum[idx] / c_fail if c_fail else None

        var_pass = (
            max((pass_sq_sum[idx] / c_pass) - (mean_pass * mean_pass), 0.0)
            if c_pass and mean_pass is not None
            else None
        )
        var_fail = (
            max((fail_sq_sum[idx] / c_fail) - (mean_fail * mean_fail), 0.0)
            if c_fail and mean_fail is not None
            else None
        )
        std_pass = math.sqrt(var_pass) if var_pass is not None else None
        std_fail = math.sqrt(var_fail) if var_fail is not None else None

        effect_size = None
        if (
            mean_pass is not None
            and mean_fail is not None
            and std_pass is not None
            and std_fail is not None
        ):
            pooled = math.sqrt((std_pass * std_pass + std_fail * std_fail) / 2.0)
            if pooled > 0.0:
                effect_size = abs(mean_fail - mean_pass) / pooled

        out.append(
            {
                "feature_index": idx,
                "feature_id": f"F{idx + 1:03d}",
                "non_null_pass": c_pass,
                "non_null_fail": c_fail,
                "mean_pass": mean_pass,
                "std_pass": std_pass,
                "mean_fail": mean_fail,
                "std_fail": std_fail,
                "effect_size": effect_size,
            }
        )
    return out


def _format_float(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.8f}"
    return str(value)


def _write_csv(
    path: Path, fieldnames: list[str], rows: list[dict[str, object]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = {k: _format_float(v) for k, v in row.items()}
            writer.writerow(out)


def main() -> int:
    args = parse_args()
    output_dir: Path = args.output_dir
    cache_dir: Path = args.cache_dir

    zip_path = _download_zip(cache_dir)
    with zipfile.ZipFile(zip_path) as zf:
        labels = _load_labels(zf)
        features = _load_features(zf)

    if len(labels) != len(features):
        raise ValueError(
            f"Label count ({len(labels)}) != feature row count ({len(features)})"
        )
    if not features:
        raise ValueError("No feature rows loaded from SECOM.")

    stats = _compute_stats(features, labels)

    eligible = [
        row
        for row in stats
        if row["effect_size"] is not None
        and int(row["non_null_pass"]) >= args.min_class_count
        and int(row["non_null_fail"]) >= args.min_class_count
    ]
    eligible.sort(key=lambda row: float(row["effect_size"]), reverse=True)
    selected_ids = {row["feature_id"] for row in eligible[: args.top_k_features]}

    num_features = len(features[0])
    feature_catalog_rows: list[dict[str, object]] = []
    for row in stats:
        idx = int(row["feature_index"])
        module = _module_for_feature(idx, num_features)
        tool = _tool_for_feature(idx, module)
        feature_catalog_rows.append(
            {
                "feature_id": row["feature_id"],
                "feature_index": idx,
                "module": module,
                "tool_id": tool,
                "selected": "true" if row["feature_id"] in selected_ids else "false",
                "effect_size": row["effect_size"],
                "non_null_pass": row["non_null_pass"],
                "non_null_fail": row["non_null_fail"],
                "mean_pass": row["mean_pass"],
                "std_pass": row["std_pass"],
                "mean_fail": row["mean_fail"],
                "std_fail": row["std_fail"],
            }
        )

    lots_rows = labels

    stats_by_feature = {row["feature_id"]: row for row in stats}
    excursion_rows: list[dict[str, object]] = []
    for lot, feature_row in zip(labels, features):
        for feature_id in sorted(selected_ids):
            idx = int(feature_id[1:]) - 1
            value = feature_row[idx]
            if value is None:
                continue
            feature_stat = stats_by_feature[feature_id]
            mean_pass = feature_stat["mean_pass"]
            std_pass = feature_stat["std_pass"]
            if mean_pass is None or std_pass is None or std_pass <= 0:
                continue
            zscore = (value - float(mean_pass)) / float(std_pass)
            if abs(zscore) < args.zscore_threshold:
                continue

            module = _module_for_feature(idx, num_features)
            tool = _tool_for_feature(idx, module)
            excursion_rows.append(
                {
                    "lot_id": lot["lot_id"],
                    "row_index": lot["row_index"],
                    "yield_outcome": lot["yield_outcome"],
                    "test_timestamp": lot["test_timestamp"],
                    "feature_id": feature_id,
                    "module": module,
                    "tool_id": tool,
                    "value": value,
                    "zscore_vs_pass": zscore,
                    "direction": "high" if zscore > 0 else "low",
                    "severity": "high" if abs(zscore) >= 3.0 else "medium",
                }
            )

    fail_lot_excursions: dict[str, int] = {}
    for row in excursion_rows:
        if row["yield_outcome"] != "FAIL":
            continue
        lot_id = str(row["lot_id"])
        fail_lot_excursions[lot_id] = fail_lot_excursions.get(lot_id, 0) + 1
    notebook_cases_rows = [
        {"lot_id": lot_id, "fail_excursion_count": count}
        for lot_id, count in sorted(
            fail_lot_excursions.items(), key=lambda x: (-x[1], x[0])
        )[:30]
    ]

    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        output_dir / "secom_lots.csv",
        ["lot_id", "row_index", "yield_code", "yield_outcome", "test_timestamp"],
        lots_rows,
    )
    _write_csv(
        output_dir / "secom_feature_catalog.csv",
        [
            "feature_id",
            "feature_index",
            "module",
            "tool_id",
            "selected",
            "effect_size",
            "non_null_pass",
            "non_null_fail",
            "mean_pass",
            "std_pass",
            "mean_fail",
            "std_fail",
        ],
        feature_catalog_rows,
    )
    _write_csv(
        output_dir / "secom_excursions.csv",
        [
            "lot_id",
            "row_index",
            "yield_outcome",
            "test_timestamp",
            "feature_id",
            "module",
            "tool_id",
            "value",
            "zscore_vs_pass",
            "direction",
            "severity",
        ],
        excursion_rows,
    )
    _write_csv(
        output_dir / "secom_notebook_cases.csv",
        ["lot_id", "fail_excursion_count"],
        notebook_cases_rows,
    )

    manifest = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "source": {
            "name": "SECOM",
            "url": SECOM_ZIP_URL,
            "citation_doi": SECOM_CITATION,
            "license": "CC BY 4.0",
        },
        "shape": {
            "lots": len(lots_rows),
            "features": num_features,
            "selected_features": len(selected_ids),
            "excursion_rows": len(excursion_rows),
            "top_case_rows": len(notebook_cases_rows),
        },
        "parameters": {
            "top_k_features": args.top_k_features,
            "min_class_count": args.min_class_count,
            "zscore_threshold": args.zscore_threshold,
        },
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )

    print(f"wrote {output_dir / 'secom_lots.csv'} ({len(lots_rows)} rows)")
    print(
        f"wrote {output_dir / 'secom_feature_catalog.csv'} ({len(feature_catalog_rows)} rows)"
    )
    print(f"wrote {output_dir / 'secom_excursions.csv'} ({len(excursion_rows)} rows)")
    print(
        f"wrote {output_dir / 'secom_notebook_cases.csv'} ({len(notebook_cases_rows)} rows)"
    )
    print(f"wrote {output_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
