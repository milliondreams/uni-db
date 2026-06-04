# Locy ADR Notebook Data

Notebook-ready Hetionet v1.0 subgraph for the Locy ADR flagship.

## Source

- Dataset: Hetionet v1.0
- DOI: 10.7554/eLife.26726
- License: CC0 1.0 Universal
- Citation: Himmelstein DS, et al. Systematic integration of biomedical knowledge prioritizes drugs for repurposing. eLife. 2017. DOI: 10.7554/eLife.26726

## Files

- `hetionet_adr_compounds.csv` — 50 most-connected drugs (real Hetionet).
- `hetionet_adr_genes.csv` — genes targeted by those drugs.
- `hetionet_adr_pathways.csv` — pathways those genes participate in.
- `hetionet_adr_side_effects.csv` — side effects caused by those drugs.
- `hetionet_adr_cbg_edges.csv` — Compound binds Gene (real CbG edges).
- `hetionet_adr_gppw_edges.csv` — Gene in Pathway (real GpPW edges).
- `hetionet_adr_ccse_edges.csv` — Compound causes Side Effect (real CcSE edges).
- `adr_reports.csv` — SYNTHETIC FAERS-shaped report stream drawn from CcSE pairs.
- `manifest.json` — generation metadata, source provenance, shape.

Regenerate with `python website/scripts/prepare_adverse_drug_reaction_notebook_data.py`.
