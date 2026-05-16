# Locy Predictive Maintenance Data

Notebook-ready data for the Locy predictive-maintenance flagship.

## Source

- Dataset: AI4I 2020 Predictive Maintenance
- DOI: https://doi.org/10.24432/C5HS5C
- Download URL: https://archive.ics.uci.edu/static/public/601/ai4i+2020+predictive+maintenance+dataset.zip
- Repository: UCI Machine Learning Repository, dataset #601
- License: CC BY 4.0

## Files

- `ai4i_equipment.csv`: curated 60-row stratified sample of real AI4I rows.
- `ai4i_topology.csv`: SYNTHETIC 4-stage process-line edges.
- `ai4i_components.csv`: SYNTHETIC 3 components per equipment.
- `manifest.json`: generation metadata + data shape.

Regenerate with `python website/scripts/prepare_predictive_maintenance_notebook_data.py`.
