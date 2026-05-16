# Locy DDI Notebook Data

Notebook-ready Hetionet-derived drug subgraph + trained embeddings +
ONNX MLP head for the Locy DDI flagship.

## Source

- Dataset: Hetionet v1.0
- License: CC0 1.0 Universal
- Citation: Himmelstein DS, et al. Systematic integration of biomedical knowledge prioritizes drugs for repurposing. eLife. 2017. DOI: 10.7554/eLife.26726

## Files

- `hetionet_ddi_drugs.csv` — 40 real Hetionet compounds.
- `hetionet_ddi_genes.csv` — genes those compounds bind.
- `ddi_pairs.csv` — pseudo-DDI labels from Vilar shared-target heuristic.
- `ddi_patients.csv` — synthesised polypharmacy patient list.
- `ddi_patient_regimens.csv` — synthesised patient-drug TAKES edges.
- `drug_embeddings.parquet` — 64-dim drug embeddings from TruncatedSVD.
- `ddi_mlp_head.onnx` — trained MLP head for runtime inference.
- `manifest.json` — provenance, embedding params, training metadata.

Regenerate with `python website/scripts/prepare_drug_drug_interaction_notebook_data.py`.
