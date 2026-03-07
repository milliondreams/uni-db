# Locy Cyber Exposure Twin Dataset (Flagship #3)

This dataset is a deterministic, documentation-friendly snapshot for the **Cyber Exposure-to-Remediation Decision Twin** notebook.

## What it contains

- `assets.csv`: OT/IT asset inventory with business criticality and internet exposure.
- `vulnerabilities.csv`: CVE metadata (severity, family, attack surface).
- `kev_snapshot.csv`: curated KEV flags.
- `epss_snapshot.csv`: curated EPSS probabilities.
- `vuln_findings.csv`: asset-to-CVE findings and SLA metadata.
- `asset_dependencies.csv`: directed dependency graph for blast propagation.
- `remediation_actions.csv`: candidate remediation actions by CVE.
- `knowledge_docs.csv`: remediation/advisory corpus with text + small demo embeddings.
- `notebook_cases.csv`: deterministic focus assets for notebook narrative.
- `manifest.json`: generation metadata and source references.

## Provenance

The snapshot is inspired by public cyber sources and normalized for reproducible docs/CI execution:

- CISA KEV Catalog
- FIRST EPSS
- NVD CVE records
- MITRE ATT&CK references

See `manifest.json` for URLs and snapshot metadata.

## Regeneration

From repository root:

```bash
uv run python website/scripts/prepare_cyber_exposure_twin_data.py
```

This rewrites all CSVs and `manifest.json` deterministically.
