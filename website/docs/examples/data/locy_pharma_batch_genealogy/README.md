# Locy Pharma Batch Genealogy Data

This folder contains notebook-ready data for the Locy flagship use case:
Pharma Batch Genealogy and Deviation Decisioning.

## Source Dataset

- Collection: Pharma process + laboratory datasets (Figshare collection)
- Collection URL: https://figshare.com/collections/_/5645578
- Paper DOI: https://doi.org/10.1038/s41597-022-01203-x
- Files used:
  - Process.csv: https://ndownloader.figshare.com/files/30874192
  - Laboratory.csv: https://ndownloader.figshare.com/files/30966250
- License: CC BY 4.0

## Generated Files

- `pharma_batches.csv`: batch-level quality/process features and computed deviation labels.
- `pharma_material_lots.csv`: material-lot nodes used in genealogy reasoning.
- `pharma_usage_edges.csv`: material lot -> batch usage edges.
- `pharma_campaign_edges.csv`: batch -> batch carryover edges for recursive propagation.
- `pharma_action_plans.csv`: intervention options per deviating batch (cost/risk tradeoffs).
- `pharma_notebook_cases.csv`: top-risk batch IDs for deterministic notebook walkthrough.
- `manifest.json`: generation metadata and shape summary.

## Rebuild

From repository root:

```bash
uv run python website/scripts/prepare_pharma_batch_genealogy_data.py
```

This regenerates all files in this folder from the public source files.
