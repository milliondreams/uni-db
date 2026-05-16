#!/usr/bin/env python3
"""Prepare notebook-ready ADR data from a real Hetionet v1.0 subgraph.

Downloads the Hetionet TSV nodes + edges (LFS-backed) once to
website/.cache/hetionet/, filters to a curated 50-compound subgraph plus
its neighbourhood (bound genes, participating pathways, caused side
effects), synthesises a small FAERS-shaped report stream, and vendors
CSVs + manifest under
website/docs/examples/data/locy_adverse_drug_reaction/.

Hetionet (Himmelstein DS et al., eLife 2017, DOI: 10.7554/elife.26726)
is distributed under CC0 1.0 Universal. The local-id half of each
Hetionet `kind::id` is preserved verbatim so the vendored CSVs can be
cross-referenced against the upstream node tables.

Metaedges we extract:

   CbG    Compound -[binds]->        Gene
   GpPW   Gene     -[participates]-> Pathway
   CcSE   Compound -[causes]->       SideEffect

The notebook walks Compound -[CbG]-> Gene -[GpPW]-> Pathway for
mechanism plausibility and uses Compound -[CcSE]-> SideEffect as the
direct adverse-event evidence.
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
        default=Path("website/docs/examples/data/locy_adverse_drug_reaction"),
    )
    p.add_argument(
        "--cache-dir", type=Path, default=Path("website/.cache/hetionet")
    )
    # Extract sizes are kept small so the notebook's single edge-creation
    # transaction stays well under the Locy per-tx ingest ceiling that
    # silently drops edges above ~2k MATCH+CREATE rows.
    p.add_argument("--n-compounds", type=int, default=30)
    p.add_argument("--n-genes", type=int, default=60)
    p.add_argument("--n-pathways", type=int, default=40)
    p.add_argument("--n-side-effects", type=int, default=60)
    p.add_argument("--max-ccse-per-compound", type=int, default=3)
    p.add_argument(
        "--n-reports", type=int, default=120,
        help="Synthetic FAERS-shaped reports drawn from the extract.",
    )
    p.add_argument("--n-signals", type=int, default=8,
                   help="How many (compound, side-effect) pairs to tag as signals.")
    p.add_argument("--seed", type=int, default=20260516)
    return p.parse_args()


def _download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 1000:
        return dest
    print(f"Downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "uni-locy-adr-prep"})
    with urllib.request.urlopen(req) as resp:
        dest.write_bytes(resp.read())
    return dest


def _read_nodes(path: Path) -> dict[str, dict[str, str]]:
    """Map full Hetionet id (`Kind::local`) → row dict."""
    out: dict[str, dict[str, str]] = {}
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            out[row["id"]] = row
    return out


def _stream_edges(gz_path: Path, metaedges: set[str]):
    """Yield (source, metaedge, target) tuples for relevant metaedges only."""
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if row["metaedge"] in metaedges:
                yield row["source"], row["metaedge"], row["target"]


def _local_id(hetionet_id: str) -> str:
    """`Compound::DB00001` → `DB00001`."""
    return hetionet_id.split("::", 1)[1]


def _safe(s: str) -> str:
    return s.replace("'", "''").replace("\\", "\\\\")


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)

    nodes_path = _download(NODES_URL, args.cache_dir / "nodes.tsv")
    edges_path = _download(EDGES_URL, args.cache_dir / "edges.sif.gz")

    nodes = _read_nodes(nodes_path)
    print(f"Read {len(nodes)} Hetionet nodes")

    # Pass 1: score every compound by (#CbG + #CcSE) so we can pick the
    # densely-connected ones for the extract.
    metaedges = {"CbG", "GpPW", "CcSE"}
    cbg_by_compound: dict[str, list[str]] = defaultdict(list)
    gppw_by_gene: dict[str, list[str]] = defaultdict(list)
    ccse_by_compound: dict[str, list[str]] = defaultdict(list)
    for src, me, tgt in _stream_edges(edges_path, metaedges):
        if me == "CbG":
            cbg_by_compound[src].append(tgt)
        elif me == "GpPW":
            gppw_by_gene[src].append(tgt)
        elif me == "CcSE":
            ccse_by_compound[src].append(tgt)

    candidate_compounds = [
        c for c in cbg_by_compound
        if c in ccse_by_compound and len(cbg_by_compound[c]) >= 2 and len(ccse_by_compound[c]) >= 3
    ]
    candidate_compounds.sort(
        key=lambda c: (len(cbg_by_compound[c]) + len(ccse_by_compound[c])),
        reverse=True,
    )
    chosen_compounds = candidate_compounds[: args.n_compounds]
    print(f"Chose {len(chosen_compounds)} compounds "
          f"(median CbG={sorted(len(cbg_by_compound[c]) for c in chosen_compounds)[len(chosen_compounds)//2]}, "
          f"median CcSE={sorted(len(ccse_by_compound[c]) for c in chosen_compounds)[len(chosen_compounds)//2]})")

    # Gather genes bound by the chosen compounds, ranked by reuse.
    gene_use: dict[str, int] = defaultdict(int)
    for c in chosen_compounds:
        for g in cbg_by_compound[c]:
            gene_use[g] += 1
    chosen_genes = sorted(gene_use, key=gene_use.get, reverse=True)[: args.n_genes]
    chosen_genes_set = set(chosen_genes)

    # Pathways those genes participate in, ranked by reuse.
    pathway_use: dict[str, int] = defaultdict(int)
    for g in chosen_genes:
        for p in gppw_by_gene[g]:
            pathway_use[p] += 1
    chosen_pathways = sorted(pathway_use, key=pathway_use.get, reverse=True)[: args.n_pathways]
    chosen_pathways_set = set(chosen_pathways)

    # Side effects those compounds cause, ranked by frequency in the extract.
    se_use: dict[str, int] = defaultdict(int)
    for c in chosen_compounds:
        for s in ccse_by_compound[c]:
            se_use[s] += 1
    chosen_ses = sorted(se_use, key=se_use.get, reverse=True)[: args.n_side_effects]
    chosen_ses_set = set(chosen_ses)

    # Now extract the edges that fall within the chosen subgraph.
    cbg_edges = [
        (c, g) for c in chosen_compounds for g in cbg_by_compound[c]
        if g in chosen_genes_set
    ]
    gppw_edges = [
        (g, p) for g in chosen_genes for p in gppw_by_gene[g]
        if p in chosen_pathways_set
    ]
    # Cap per-compound to keep total CCSE volume well below the per-tx
    # ingest ceiling. Ranking by side-effect frequency in the extract so
    # we keep the most-shared (highest mechanism-plausibility) edges.
    ccse_edges: list[tuple[str, str]] = []
    for c in chosen_compounds:
        in_extract = [s for s in ccse_by_compound[c] if s in chosen_ses_set]
        in_extract.sort(key=lambda s: se_use[s], reverse=True)
        for s in in_extract[: args.max_ccse_per_compound]:
            ccse_edges.append((c, s))
    print(f"Subgraph edges: CbG={len(cbg_edges)}, GpPW={len(gppw_edges)}, CcSE={len(ccse_edges)}")

    # Synthesise a FAERS-shaped report stream from the CcSE edges.
    # Each report references one (Compound, SideEffect) pair in the extract.
    # Signal pairs (n_signals of them) get more reports, higher narrative
    # similarity vs historical confirmed signals (the precomputed feature).
    signal_pairs = rng.sample(ccse_edges, k=min(args.n_signals, len(ccse_edges)))
    signal_pair_set = set(signal_pairs)

    reports = []
    # Seed at least 3 reports per signal pair so the held-out label has signal
    # to learn from. Fill the remainder with random non-signal pairs.
    reports_per_signal = 3
    for pair in signal_pairs:
        for _ in range(reports_per_signal):
            comp, se = pair
            count = 8.0 + rng.random() * 2.0
            similarity = 0.78 + rng.random() * 0.18
            reports.append({
                "report_id": f"R{len(reports)+1:04d}",
                "compound_id": _local_id(comp),
                "side_effect_id": _local_id(se),
                "report_count": f"{count:.3f}",
                "precomputed_similarity": f"{similarity:.3f}",
                "combined_evidence": f"{count * similarity:.3f}",
                "is_signal": "true",
            })
    non_signal_edges = [e for e in ccse_edges if e not in signal_pair_set]
    while len(reports) < args.n_reports:
        comp, se = rng.choice(non_signal_edges)
        is_signal = False
        count = 2.0 + rng.random() * 2.0
        similarity = 0.22 + rng.random() * 0.18
        reports.append({
            "report_id": f"R{len(reports)+1:04d}",
            "compound_id": _local_id(comp),
            "side_effect_id": _local_id(se),
            "report_count": f"{count:.3f}",
            "precomputed_similarity": f"{similarity:.3f}",
            "combined_evidence": f"{count * similarity:.3f}",
            "is_signal": "false",
        })

    # Write outputs.
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    def _csv(name: str, rows: list[dict], fieldnames: list[str]) -> None:
        with open(out / name, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)

    _csv(
        "hetionet_adr_compounds.csv",
        [
            {
                "compound_id": _local_id(c),
                "name": _safe(nodes[c]["name"]),
                "hetionet_id": c,
            }
            for c in chosen_compounds
        ],
        ["compound_id", "name", "hetionet_id"],
    )
    _csv(
        "hetionet_adr_genes.csv",
        [
            {"gene_id": _local_id(g), "name": _safe(nodes[g]["name"])}
            for g in chosen_genes
        ],
        ["gene_id", "name"],
    )
    _csv(
        "hetionet_adr_pathways.csv",
        [
            {"pathway_id": _local_id(p), "name": _safe(nodes[p]["name"])}
            for p in chosen_pathways
        ],
        ["pathway_id", "name"],
    )
    _csv(
        "hetionet_adr_side_effects.csv",
        [
            {
                "side_effect_id": _local_id(s),
                "meddra_term": _safe(nodes[s]["name"]),
            }
            for s in chosen_ses
        ],
        ["side_effect_id", "meddra_term"],
    )
    _csv(
        "hetionet_adr_cbg_edges.csv",
        [{"compound_id": _local_id(c), "gene_id": _local_id(g)} for c, g in cbg_edges],
        ["compound_id", "gene_id"],
    )
    _csv(
        "hetionet_adr_gppw_edges.csv",
        [{"gene_id": _local_id(g), "pathway_id": _local_id(p)} for g, p in gppw_edges],
        ["gene_id", "pathway_id"],
    )
    _csv(
        "hetionet_adr_ccse_edges.csv",
        [{"compound_id": _local_id(c), "side_effect_id": _local_id(s)} for c, s in ccse_edges],
        ["compound_id", "side_effect_id"],
    )
    _csv(
        "adr_reports.csv",
        reports,
        [
            "report_id", "compound_id", "side_effect_id",
            "report_count", "precomputed_similarity", "combined_evidence",
            "is_signal",
        ],
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
        "extract_params": {
            "n_compounds": args.n_compounds,
            "n_genes": args.n_genes,
            "n_pathways": args.n_pathways,
            "n_side_effects": args.n_side_effects,
            "n_reports": args.n_reports,
            "n_signals": args.n_signals,
            "seed": args.seed,
        },
        "shape": {
            "compounds": len(chosen_compounds),
            "genes": len(chosen_genes),
            "pathways": len(chosen_pathways),
            "side_effects": len(chosen_ses),
            "cbg_edges": len(cbg_edges),
            "gppw_edges": len(gppw_edges),
            "ccse_edges": len(ccse_edges),
            "reports": len(reports),
            "reports_flagged_as_signals": sum(1 for r in reports if r["is_signal"] == "true"),
        },
        "notes": [
            "Compounds, genes, pathways, side effects, and the three edge",
            "sets are an EXACT slice of Hetionet v1.0 (no synthetic edges).",
            "adr_reports.csv is SYNTHETIC: drawn from CcSE pairs in the",
            "  extract, with deterministic count/similarity/signal labels",
            "  seeded by the prep-script --seed parameter. Use this as a",
            "  reproducible FAERS-shaped stream, not as real reports.",
        ],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (out / "README.md").write_text(
        "# Locy ADR Notebook Data\n\n"
        "Notebook-ready Hetionet v1.0 subgraph for the Locy ADR flagship.\n\n"
        "## Source\n\n"
        "- Dataset: Hetionet v1.0\n"
        f"- DOI: 10.7554/eLife.26726\n"
        f"- License: {LICENSE}\n"
        f"- Citation: {CITATION}\n\n"
        "## Files\n\n"
        "- `hetionet_adr_compounds.csv` — 50 most-connected drugs (real Hetionet).\n"
        "- `hetionet_adr_genes.csv` — genes targeted by those drugs.\n"
        "- `hetionet_adr_pathways.csv` — pathways those genes participate in.\n"
        "- `hetionet_adr_side_effects.csv` — side effects caused by those drugs.\n"
        "- `hetionet_adr_cbg_edges.csv` — Compound binds Gene (real CbG edges).\n"
        "- `hetionet_adr_gppw_edges.csv` — Gene in Pathway (real GpPW edges).\n"
        "- `hetionet_adr_ccse_edges.csv` — Compound causes Side Effect (real CcSE edges).\n"
        "- `adr_reports.csv` — SYNTHETIC FAERS-shaped report stream drawn from CcSE pairs.\n"
        "- `manifest.json` — generation metadata, source provenance, shape.\n\n"
        "Regenerate with `python website/scripts/prepare_adverse_drug_reaction_notebook_data.py`.\n",
        encoding="utf-8",
    )
    print(f"Wrote {out}/ (compounds={len(chosen_compounds)}, "
          f"genes={len(chosen_genes)}, pathways={len(chosen_pathways)}, "
          f"side_effects={len(chosen_ses)}, reports={len(reports)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
