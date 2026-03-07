# Locy Semiconductor Yield Excursion Data

This folder contains notebook-ready data for the Locy manufacturing use case:
Semiconductor Yield Excursion Triage.

## Source Dataset

- Dataset: SECOM
- Repository: UCI Machine Learning Repository
- DOI: https://doi.org/10.24432/C54305
- Download URL: https://archive.ics.uci.edu/static/public/179/secom.zip
- License: CC BY 4.0

## Generated Files

- `secom_lots.csv`: one row per production entity (lot), with pass/fail and test timestamp.
- `secom_feature_catalog.csv`: feature metadata, selected high-signal features, and module/tool mapping used for notebook graph modeling.
- `secom_excursions.csv`: lot-feature excursion events (z-score against pass baseline) for selected features.
- `secom_notebook_cases.csv`: highest-signal failing lots for focused walkthrough sections.
- `manifest.json`: generation metadata and dataset shape summary.

## Rebuild

From repository root:

```bash
uv run python website/scripts/prepare_semiconductor_notebook_data.py
```

This regenerates all files in this folder from the latest downloaded SECOM archive.
