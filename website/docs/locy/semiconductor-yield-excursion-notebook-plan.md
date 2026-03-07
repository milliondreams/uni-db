# Locy Notebook Plan: Semiconductor Yield Excursion Triage

This plan defines a flagship, notebook-first Locy story for non-automotive manufacturing.
Goal: show how Locy helps process engineers move from noisy telemetry to explainable containment decisions.

## 1) Audience

- New Locy users: need clear schema-first setup and guided rule reading.
- Data/Process engineers: need realistic anomaly and containment logic.
- Advanced logic users: want `DERIVE`, `ASSUME`, `ABDUCE`, and `EXPLAIN` in one coherent workflow.

## 2) Data Backbone

Primary source data:

- UCI SECOM dataset (real semiconductor pass/fail + timestamp + sensor features)
- Citation DOI: `10.24432/C54305`
- License: CC BY 4.0

Notebook-local prepared data:

- `examples/data/locy_semiconductor_yield_excursion/secom_lots.csv`
- `examples/data/locy_semiconductor_yield_excursion/secom_feature_catalog.csv`
- `examples/data/locy_semiconductor_yield_excursion/secom_excursions.csv`
- `examples/data/locy_semiconductor_yield_excursion/secom_notebook_cases.csv`

## 3) Story Arc

Narrative:

1. Fab sees rising end-of-line failures.
2. Engineers need to identify likely process module/tool contributors.
3. They must decide hold/rework scope before scrap spreads.
4. They need a defensible explanation for each action.

Notebook outcome:

- Surface likely excursion paths.
- Simulate containment options.
- Propose root-cause hypotheses.
- Explain each recommendation with rule-level provenance.

## 4) Notebook Structure (Detailed)

### Section A: Setup and Context

- Explain business objective and what the reader will build.
- Import dependencies and open an isolated database.
- State that schema mode is required/recommended.

### Section B: Schema-First Modeling

Define labels:

- `Lot(lot_id, yield_outcome, test_timestamp)`
- `Feature(feature_id, module, tool_id, effect_size, selected)`
- `Tool(tool_id, module)`
- `Module(name)`

Define edges:

- `(:Lot)-[:OBSERVED_EXCURSION]->(:Feature)`
- `(:Feature)-[:MEASURED_ON]->(:Tool)`
- `(:Tool)-[:PART_OF]->(:Module)`

### Section C: Ingest Real Prepared Data

- Load CSV files.
- Insert lots, selected features, tools/modules, and excursion edges.
- Print small sanity checks:
  - lot count
  - fail count
  - excursion edge count

### Section D: Baseline `DERIVE`

Rules:

- `failed_lot` from `Lot(yield_outcome='FAIL')`
- `module_signal` from failed lot excursions projected to module.
- `tool_signal` from failed lot excursions projected to tool.
- Optional transitive relation `module_risk_path` for module-level rollups.

Query examples:

- top modules by number of failed-lot excursions.
- top tools within the leading module.

### Section E: Counterfactual `ASSUME`

Scenarios:

- Assume temporary hold on one tool.
- Assume tighter excursion threshold policy for one module.

Derivations:

- `contained_fail_lot` under the assumption.
- `residual_fail_lot` not contained by the assumption.

Expected teaching point:

- `ASSUME` lets readers test interventions before operational changes.

### Section F: Root Cause Hypothesis with `ABDUCE`

Task:

- For a selected high-failure lot cohort, find smallest tool/module hypotheses that explain observed excursion pattern.

Expected output:

- ranked candidate hypotheses (for example, tool sets or module-level hypotheses).

Teaching point:

- `ABDUCE` provides plausible explanations, not guaranteed truth.

### Section G: Decision Justification with `EXPLAIN`

Explain these:

- why a lot is classified high risk.
- why a lot remains residual risk after a chosen assumption.

Expected output:

- compact proof/trace path showing contributing rules and facts.

Teaching point:

- operational decisions become auditable and teachable.

### Section H: Final Validation Checklist

- one module/tool should clearly dominate risk in this dataset slice.
- assumption should reduce impacted fail-lot set.
- abduced hypotheses should align with derived high-signal module/tool.
- explanations should reference concrete facts and derived relations.

## 5) Markdown-First Pedagogy Requirements

Each code cell must be preceded by markdown that states:

- what this cell does.
- why it matters operationally.
- what output shape to expect.

For Locy cells, add short reading tips:

- how to read each `CREATE RULE`.
- what relation keys represent.
- what would indicate a modeling bug.

## 6) Scope for v1 Notebook

In scope:

- end-to-end reproducible notebook using prepared SECOM-derived data.
- one full thread demonstrating `DERIVE` -> `ASSUME` -> `ABDUCE` -> `EXPLAIN`.
- schema-first modeling and explicit expected outcomes.

Out of scope (v1):

- online feature engineering notebooks.
- training ML models in-notebook.
- real fab-identifiable tool naming.

## 7) Follow-on v2 Ideas

- Add temporal windows (`within last N hours`) for excursion propagation.
- Add maintenance logs as another fact source.
- Add multi-line manufacturing module support and comparative risk scoring.
