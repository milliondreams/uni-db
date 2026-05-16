#!/usr/bin/env python3
"""Prepare notebook-ready DDI data from a real Hetionet v1.0 subgraph.

Builds the artifacts the DDI flagship notebook consumes at runtime:

  - Real Hetionet compound + gene subgraph (CSV).
  - Offline-trained drug embeddings (parquet, 64-dim).
  - Trained MLP head exported to ONNX, accepting concat(emb1, emb2)
    and returning P(interact).
  - Pseudo-DDI labels synthesised from Vilar-style shared-target
    heuristic (drugs sharing ≥2 targeted genes flagged as dangerous).
  - Patient regimen CSV plus InteractionRecord-shaped pair stream.

Embedding algorithm: scikit-learn `TruncatedSVD` over the Compound-Gene
bipartite adjacency matrix. This stands in for a heavier R-GCN: the
deployment pattern in the notebook narrative — "offline graph learning →
lightweight runtime ONNX head" — is identical, but the SVD path keeps
the prep-time dependency footprint to numpy + scipy + scikit-learn
(none of which the runtime notebook imports).

Hetionet (Himmelstein DS et al., eLife 2017, DOI: 10.7554/eLife.26726)
is distributed under CC0 1.0 Universal.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import json
import random
import urllib.request
from collections import defaultdict
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from sklearn.decomposition import TruncatedSVD

NODES_URL = (
    "https://raw.githubusercontent.com/hetio/hetionet/master/"
    "hetnet/tsv/hetionet-v1.0-nodes.tsv"
)
EDGES_URL = (
    "https://media.githubusercontent.com/media/hetio/hetionet/master/"
    "hetnet/tsv/hetionet-v1.0-edges.sif.gz"
)
CITATION = (
    "Himmelstein DS, et al. Systematic integration of biomedical "
    "knowledge prioritizes drugs for repurposing. eLife. 2017. "
    "DOI: 10.7554/eLife.26726"
)
LICENSE = "CC0 1.0 Universal"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("website/docs/examples/data/locy_drug_drug_interaction"),
    )
    p.add_argument(
        "--cache-dir", type=Path, default=Path("website/.cache/hetionet")
    )
    # Smaller than ADR because DDI also has InteractionRecord nodes + edges
    # that count toward the per-tx ingest limit.
    p.add_argument("--n-compounds", type=int, default=40)
    p.add_argument("--n-genes", type=int, default=80)
    p.add_argument("--embedding-dim", type=int, default=64)
    p.add_argument("--n-patients", type=int, default=8)
    p.add_argument("--min-regimen", type=int, default=3)
    p.add_argument("--max-regimen", type=int, default=5)
    p.add_argument("--seed", type=int, default=20260516)
    return p.parse_args()


def _download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 1000:
        return dest
    print(f"Downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "uni-locy-ddi-prep"})
    with urllib.request.urlopen(req) as resp:
        dest.write_bytes(resp.read())
    return dest


def _read_nodes(path: Path) -> dict[str, dict[str, str]]:
    out = {}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            out[row["id"]] = row
    return out


def _local_id(hid: str) -> str:
    return hid.split("::", 1)[1]


def _safe(s: str) -> str:
    return s.replace("'", "''").replace("\\", "\\\\")


def _train_mlp_head(embeddings: np.ndarray, pairs: list[tuple[int, int, int]],
                    embedding_dim: int, seed: int) -> nn.Module:
    """Train a tiny 2-layer MLP on concat(emb1, emb2) → P(interact)."""
    torch.manual_seed(seed)
    X = []
    y = []
    for i, j, label in pairs:
        X.append(np.concatenate([embeddings[i], embeddings[j]]))
        y.append(label)
    X_t = torch.tensor(np.array(X), dtype=torch.float32)
    y_t = torch.tensor(y, dtype=torch.float32).unsqueeze(1)

    model = nn.Sequential(
        nn.Linear(2 * embedding_dim, 32),
        nn.ReLU(),
        nn.Linear(32, 1),
        nn.Sigmoid(),
    )
    opt = torch.optim.Adam(model.parameters(), lr=1e-2)
    loss_fn = nn.BCELoss()
    for epoch in range(150):
        opt.zero_grad()
        pred = model(X_t)
        loss = loss_fn(pred, y_t)
        loss.backward()
        opt.step()
    print(f"  MLP final BCE loss: {loss.item():.4f}")
    return model


def _export_onnx(model: nn.Module, embedding_dim: int, path: Path) -> None:
    model.eval()
    dummy = torch.zeros((1, 2 * embedding_dim), dtype=torch.float32)
    # Use the legacy (non-dynamo) exporter so the weights stay inside the
    # single .onnx file instead of being split into an .onnx.data sidecar.
    torch.onnx.export(
        model, dummy, str(path),
        input_names=["concat_embeddings"],
        output_names=["p_interact"],
        dynamic_axes={"concat_embeddings": {0: "batch"}, "p_interact": {0: "batch"}},
        opset_version=14,
        dynamo=False,
    )


def _write_parquet(path: Path, drug_ids: list[str], embeddings: np.ndarray) -> None:
    """Minimal parquet writer via pyarrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.table({
        "drug_id": drug_ids,
        **{f"e{i}": embeddings[:, i].tolist() for i in range(embeddings.shape[1])},
    })
    pq.write_table(table, path)


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)

    nodes_path = _download(NODES_URL, args.cache_dir / "nodes.tsv")
    edges_path = _download(EDGES_URL, args.cache_dir / "edges.sif.gz")
    nodes = _read_nodes(nodes_path)
    print(f"Read {len(nodes)} Hetionet nodes")

    # Index Compound→Gene via CbG.
    cbg_by_compound: dict[str, list[str]] = defaultdict(list)
    with gzip.open(edges_path, "rt", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            if row["metaedge"] == "CbG":
                cbg_by_compound[row["source"]].append(row["target"])

    candidate_compounds = sorted(cbg_by_compound, key=lambda c: -len(cbg_by_compound[c]))
    candidate_compounds = [c for c in candidate_compounds if len(cbg_by_compound[c]) >= 3]
    chosen_compounds = candidate_compounds[: args.n_compounds]

    gene_use: dict[str, int] = defaultdict(int)
    for c in chosen_compounds:
        for g in cbg_by_compound[c]:
            gene_use[g] += 1
    chosen_genes = sorted(gene_use, key=gene_use.get, reverse=True)[: args.n_genes]
    chosen_genes_set = set(chosen_genes)
    gene_idx = {g: i for i, g in enumerate(chosen_genes)}

    print(f"Chose {len(chosen_compounds)} compounds × {len(chosen_genes)} genes")

    # Build bipartite Compound×Gene adjacency, then TruncatedSVD to embedding_dim.
    A = np.zeros((len(chosen_compounds), len(chosen_genes)), dtype=np.float32)
    for ci, c in enumerate(chosen_compounds):
        for g in cbg_by_compound[c]:
            if g in chosen_genes_set:
                A[ci, gene_idx[g]] = 1.0

    n_components = min(args.embedding_dim, min(A.shape) - 1)
    svd = TruncatedSVD(n_components=n_components, random_state=args.seed)
    embeddings = svd.fit_transform(A)
    # Pad to embedding_dim if SVD produced fewer (rare for our sizes).
    if embeddings.shape[1] < args.embedding_dim:
        pad = np.zeros((embeddings.shape[0], args.embedding_dim - embeddings.shape[1]), dtype=np.float32)
        embeddings = np.concatenate([embeddings, pad], axis=1)
    embeddings = embeddings.astype(np.float32)
    print(f"Embeddings shape: {embeddings.shape}, "
          f"SVD explained variance: {svd.explained_variance_ratio_.sum():.3f}")

    # Vilar-style pseudo-DDI labels: compounds sharing ≥2 targeted genes ⇒
    # is_dangerous=true. Computed pairwise on the bipartite adjacency.
    shared_count = (A @ A.T).astype(np.int32)
    pairs: list[tuple[int, int, int]] = []
    interaction_rows: list[dict[str, str]] = []
    pair_idx = 0
    for i in range(len(chosen_compounds)):
        for j in range(i + 1, len(chosen_compounds)):
            s = int(shared_count[i, j])
            if s == 0:
                continue
            is_dangerous = s >= 2
            pair_idx += 1
            pid = f"PR{pair_idx:04d}"
            interaction_rows.append({
                "pair_id": pid,
                "drug_a_id": _local_id(chosen_compounds[i]),
                "drug_b_id": _local_id(chosen_compounds[j]),
                "shared_targets": str(s),
                "is_dangerous": "true" if is_dangerous else "false",
            })
            pairs.append((i, j, int(is_dangerous)))

    n_dangerous = sum(1 for _, _, lab in pairs if lab == 1)
    print(f"Pseudo-DDI pairs: {len(pairs)} ({n_dangerous} dangerous)")

    # Train MLP head on the labels, export to ONNX.
    print("Training MLP head ...")
    mlp = _train_mlp_head(embeddings, pairs, args.embedding_dim, args.seed)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    onnx_path = args.output_dir / "ddi_mlp_head.onnx"
    _export_onnx(mlp, args.embedding_dim, onnx_path)
    print(f"Wrote ONNX head: {onnx_path} ({onnx_path.stat().st_size} bytes)")

    # Vendor embeddings as parquet.
    drug_local_ids = [_local_id(c) for c in chosen_compounds]
    parquet_path = args.output_dir / "drug_embeddings.parquet"
    _write_parquet(parquet_path, drug_local_ids, embeddings)
    print(f"Wrote embeddings: {parquet_path} ({parquet_path.stat().st_size} bytes)")

    # Synthesise patient regimens biased toward dangerous pairs so the
    # joint_regimen_safety rollup has signal.
    dangerous_pair_ids = [(r["drug_a_id"], r["drug_b_id"])
                          for r in interaction_rows if r["is_dangerous"] == "true"]
    patients = []
    regimens = []
    for k in range(args.n_patients):
        pid = f"PAT{k+1:02d}"
        # Seed each patient's regimen with one drug from a dangerous pair
        # so most patients have at least one cross-class hit.
        if dangerous_pair_ids:
            a, b = rng.choice(dangerous_pair_ids)
            seed_drugs = [a, b]
        else:
            seed_drugs = []
        regimen_size = rng.randint(args.min_regimen, args.max_regimen)
        extra = rng.sample(
            [d for d in drug_local_ids if d not in seed_drugs],
            k=max(0, regimen_size - len(seed_drugs)),
        )
        regimen = list(dict.fromkeys(seed_drugs + extra))
        patients.append({"patient_id": pid})
        for d in regimen:
            regimens.append({"patient_id": pid, "drug_id": d})

    # Write CSVs.
    out = args.output_dir

    def _csv(name: str, rows: list[dict], fields: list[str]) -> None:
        with open(out / name, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)

    _csv(
        "hetionet_ddi_drugs.csv",
        [
            {
                "drug_id": _local_id(c),
                "name": _safe(nodes[c]["name"]),
                "hetionet_id": c,
            }
            for c in chosen_compounds
        ],
        ["drug_id", "name", "hetionet_id"],
    )
    _csv(
        "hetionet_ddi_genes.csv",
        [
            {"gene_id": _local_id(g), "name": _safe(nodes[g]["name"])}
            for g in chosen_genes
        ],
        ["gene_id", "name"],
    )
    _csv(
        "ddi_pairs.csv",
        interaction_rows,
        ["pair_id", "drug_a_id", "drug_b_id", "shared_targets", "is_dangerous"],
    )
    _csv(
        "ddi_patients.csv",
        patients,
        ["patient_id"],
    )
    _csv(
        "ddi_patient_regimens.csv",
        regimens,
        ["patient_id", "drug_id"],
    )

    manifest = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": {
            "name": "Hetionet v1.0",
            "doi": "10.7554/eLife.26726",
            "license": LICENSE,
            "nodes_url": NODES_URL,
            "edges_url": EDGES_URL,
            "citation": CITATION,
        },
        "embedding": {
            "algorithm": "scikit-learn TruncatedSVD",
            "dim": args.embedding_dim,
            "input_adjacency_shape": list(A.shape),
            "explained_variance_ratio_sum": float(svd.explained_variance_ratio_.sum()),
        },
        "mlp_head": {
            "framework": "torch -> ONNX (opset 14)",
            "architecture": "Linear(2*emb,32) -> ReLU -> Linear(32,1) -> Sigmoid",
            "training_epochs": 150,
            "loss": "BCE",
        },
        "ddi_labels": {
            "method": "Vilar-style shared-target heuristic",
            "is_dangerous_rule": "shared_targets >= 2",
            "n_pairs": len(pairs),
            "n_dangerous": n_dangerous,
        },
        "shape": {
            "compounds": len(chosen_compounds),
            "genes": len(chosen_genes),
            "pairs": len(pairs),
            "patients": len(patients),
            "regimen_edges": len(regimens),
        },
        "extract_params": {
            "n_compounds": args.n_compounds,
            "n_genes": args.n_genes,
            "embedding_dim": args.embedding_dim,
            "n_patients": args.n_patients,
            "min_regimen": args.min_regimen,
            "max_regimen": args.max_regimen,
            "seed": args.seed,
        },
        "notes": [
            "drug_embeddings.parquet is offline-trained at prep time; the",
            "  notebook only loads it via pyarrow.",
            "ddi_mlp_head.onnx is loaded at notebook runtime via onnxruntime.",
            "ddi_patients.csv + ddi_patient_regimens.csv are SYNTHETIC.",
        ],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (out / "README.md").write_text(
        "# Locy DDI Notebook Data\n\n"
        "Notebook-ready Hetionet-derived drug subgraph + trained embeddings +\n"
        "ONNX MLP head for the Locy DDI flagship.\n\n"
        "## Source\n\n"
        f"- Dataset: Hetionet v1.0\n- License: {LICENSE}\n- Citation: {CITATION}\n\n"
        "## Files\n\n"
        "- `hetionet_ddi_drugs.csv` — 40 real Hetionet compounds.\n"
        "- `hetionet_ddi_genes.csv` — genes those compounds bind.\n"
        "- `ddi_pairs.csv` — pseudo-DDI labels from Vilar shared-target heuristic.\n"
        "- `ddi_patients.csv` — synthesised polypharmacy patient list.\n"
        "- `ddi_patient_regimens.csv` — synthesised patient-drug TAKES edges.\n"
        "- `drug_embeddings.parquet` — 64-dim drug embeddings from TruncatedSVD.\n"
        "- `ddi_mlp_head.onnx` — trained MLP head for runtime inference.\n"
        "- `manifest.json` — provenance, embedding params, training metadata.\n\n"
        "Regenerate with `python website/scripts/prepare_drug_drug_interaction_notebook_data.py`.\n",
        encoding="utf-8",
    )
    print(f"Wrote {out}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
