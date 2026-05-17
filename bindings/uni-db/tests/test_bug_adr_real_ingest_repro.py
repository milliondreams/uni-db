# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Tests the two ADR-ingest workarounds against the real vendored CSVs:
#
#   Approach 1 — multi-tx edge ingest. The notebook combines all metaedges
#                into one tx because we previously believed splitting them
#                across multiple txs corrupted earlier txs' edges. This
#                test issues one tx PER METAEDGE TYPE (the original
#                pre-workaround pattern) and asserts every earlier tx's
#                edges remain readable after every subsequent commit.
#
#   Approach 2 — per-step diagnostic probe. Same multi-tx ingest, but
#                between every commit we print per-edge-type counts so
#                that, if loss occurs, we know exactly which tx and which
#                metaedge degraded.
#
# If BOTH approaches pass at the current vendored scale, the workarounds
# in prepare_adverse_drug_reaction_notebook_data.py and the single-tx
# combined edge ingest in generate_adverse_drug_reaction_flagship_notebook.py
# are unnecessary and can be removed.

import csv
import os
from pathlib import Path

import pytest
import uni_db


# Override via env var to point at a larger / uncapped data directory
# without modifying the vendored CSVs. Example:
#   ADR_DATA_DIR=/tmp/adr-uncapped pytest tests/test_bug_adr_real_ingest_repro.py
DATA_DIR = Path(
    os.environ.get(
        "ADR_DATA_DIR",
        str(
            Path(__file__).resolve().parents[3]
            / "website"
            / "docs"
            / "examples"
            / "data"
            / "locy_adverse_drug_reaction"
        ),
    )
)


def _read_csv(name: str) -> list[dict]:
    with open(DATA_DIR / name, encoding="utf-8") as f:
        return list(csv.DictReader(f))


pytestmark = pytest.mark.skipif(
    not DATA_DIR.exists(),
    reason=f"vendored ADR data not found at {DATA_DIR}",
)


def _adr_schema_db():
    db = uni_db.UniBuilder.temporary().build()
    (
        db.schema()
        .label("Drug")
        .property("drug_id", "string")
        .property("name", "string")
        .done()
        .label("Gene")
        .property("gene_id", "string")
        .property("name", "string")
        .done()
        .label("Pathway")
        .property("pathway_id", "string")
        .property("name", "string")
        .done()
        .label("AdverseEvent")
        .property("event_id", "string")
        .property("meddra_term", "string")
        .done()
        .label("Report")
        .property("report_id", "string")
        .property("compound_id", "string")
        .property("side_effect_id", "string")
        .done()
        .edge_type("TARGETS", ["Drug"], ["Gene"])
        .done()
        .edge_type("IN_PATHWAY", ["Gene"], ["Pathway"])
        .done()
        .edge_type("CAUSES", ["Drug"], ["AdverseEvent"])
        .done()
        .edge_type("OF_DRUG", ["Report"], ["Drug"])
        .done()
        .edge_type("REPORTS_EVENT", ["Report"], ["AdverseEvent"])
        .done()
        .apply()
    )
    return db


def _esc(s: str) -> str:
    return str(s).replace("'", "\\'")


def _seed_all_nodes(db, compounds, genes, pathways, ses) -> None:
    tx = db.session().tx()
    for c in compounds:
        tx.execute(
            f"CREATE (:Drug {{drug_id: '{_esc(c['compound_id'])}', "
            f"name: '{_esc(c['name'])}'}})"
        )
    for g in genes:
        tx.execute(
            f"CREATE (:Gene {{gene_id: '{_esc(g['gene_id'])}', "
            f"name: '{_esc(g['name'])}'}})"
        )
    for p in pathways:
        tx.execute(
            f"CREATE (:Pathway {{pathway_id: '{_esc(p['pathway_id'])}', "
            f"name: '{_esc(p['name'])}'}})"
        )
    for s in ses:
        tx.execute(
            f"CREATE (:AdverseEvent "
            f"{{event_id: '{_esc(s['side_effect_id'])}', "
            f"meddra_term: '{_esc(s['meddra_term'])}'}})"
        )
    tx.commit()


def _count(db, query: str) -> int:
    return db.session().query(query)[0]["cnt"]


def _edge_counts(db) -> dict:
    return {
        "TARGETS": _count(db, "MATCH ()-[r:TARGETS]->() RETURN count(r) AS cnt"),
        "IN_PATHWAY": _count(
            db, "MATCH ()-[r:IN_PATHWAY]->() RETURN count(r) AS cnt"
        ),
        "CAUSES": _count(db, "MATCH ()-[r:CAUSES]->() RETURN count(r) AS cnt"),
        "OF_DRUG": _count(db, "MATCH ()-[r:OF_DRUG]->() RETURN count(r) AS cnt"),
        "REPORTS_EVENT": _count(
            db, "MATCH ()-[r:REPORTS_EVENT]->() RETURN count(r) AS cnt"
        ),
        "Report_NODES": _count(
            db, "MATCH (r:Report) RETURN count(r) AS cnt"
        ),
    }


# ── Approach 1 + 2 fused: multi-tx ingest with per-step probes ───────


def test_bug_adr_multi_tx_real_data_preserves_all_edges(capsys):
    """Mirrors the pre-workaround ingest pattern against vendored CSVs:
    one tx per metaedge type, with assertion + probe after every commit."""

    compounds = _read_csv("hetionet_adr_compounds.csv")
    genes = _read_csv("hetionet_adr_genes.csv")
    pathways = _read_csv("hetionet_adr_pathways.csv")
    ses = _read_csv("hetionet_adr_side_effects.csv")
    cbg = _read_csv("hetionet_adr_cbg_edges.csv")
    gppw = _read_csv("hetionet_adr_gppw_edges.csv")
    ccse = _read_csv("hetionet_adr_ccse_edges.csv")
    reports = _read_csv("adr_reports.csv")

    log_lines = [
        "",
        "ADR multi-tx ingest probe (real vendored CSVs)",
        f"  scale: {len(compounds)} drugs, {len(genes)} genes, "
        f"{len(pathways)} pathways, {len(ses)} AEs",
        f"  edges: CbG={len(cbg)}, GpPW={len(gppw)}, CcSE={len(ccse)}, "
        f"reports={len(reports)} (×3 edges each)",
    ]

    db = _adr_schema_db()

    # tx1 — nodes
    _seed_all_nodes(db, compounds, genes, pathways, ses)
    counts = _edge_counts(db)
    log_lines.append(f"  after tx1 (nodes only):  {counts}")
    expected_after_each = {
        "TARGETS": 0, "IN_PATHWAY": 0, "CAUSES": 0,
        "OF_DRUG": 0, "REPORTS_EVENT": 0, "Report_NODES": 0,
    }
    assert counts == expected_after_each

    # tx2 — TARGETS
    tx = db.session().tx()
    for e in cbg:
        tx.execute(
            f"MATCH (d:Drug {{drug_id: '{_esc(e['compound_id'])}'}}), "
            f"(g:Gene {{gene_id: '{_esc(e['gene_id'])}'}}) "
            f"CREATE (d)-[:TARGETS]->(g)"
        )
    tx.commit()
    counts = _edge_counts(db)
    log_lines.append(f"  after tx2 (TARGETS):     {counts}")
    assert counts["TARGETS"] == len(cbg), (
        f"tx2 lost TARGETS: got {counts['TARGETS']}/{len(cbg)}"
    )

    # tx3 — IN_PATHWAY (must not corrupt tx2's TARGETS)
    tx = db.session().tx()
    for e in gppw:
        tx.execute(
            f"MATCH (g:Gene {{gene_id: '{_esc(e['gene_id'])}'}}), "
            f"(p:Pathway {{pathway_id: '{_esc(e['pathway_id'])}'}}) "
            f"CREATE (g)-[:IN_PATHWAY]->(p)"
        )
    tx.commit()
    counts = _edge_counts(db)
    log_lines.append(f"  after tx3 (IN_PATHWAY):  {counts}")
    assert counts["TARGETS"] == len(cbg), (
        f"tx3 corrupted tx2 TARGETS: was {len(cbg)}, now {counts['TARGETS']}"
    )
    assert counts["IN_PATHWAY"] == len(gppw), (
        f"tx3 lost IN_PATHWAY: got {counts['IN_PATHWAY']}/{len(gppw)}"
    )

    # tx4 — CAUSES (must not corrupt tx2 or tx3)
    tx = db.session().tx()
    for e in ccse:
        tx.execute(
            f"MATCH (d:Drug {{drug_id: '{_esc(e['compound_id'])}'}}), "
            f"(s:AdverseEvent {{event_id: '{_esc(e['side_effect_id'])}'}}) "
            f"CREATE (d)-[:CAUSES]->(s)"
        )
    tx.commit()
    counts = _edge_counts(db)
    log_lines.append(f"  after tx4 (CAUSES):      {counts}")
    assert counts["TARGETS"] == len(cbg), (
        f"tx4 corrupted tx2 TARGETS: was {len(cbg)}, now {counts['TARGETS']}"
    )
    assert counts["IN_PATHWAY"] == len(gppw), (
        f"tx4 corrupted tx3 IN_PATHWAY: was {len(gppw)}, now "
        f"{counts['IN_PATHWAY']}"
    )
    assert counts["CAUSES"] == len(ccse), (
        f"tx4 lost CAUSES: got {counts['CAUSES']}/{len(ccse)}"
    )

    # tx5 — Report nodes + OF_DRUG + REPORTS_EVENT (must not corrupt earlier)
    tx = db.session().tx()
    for r in reports:
        tx.execute(
            f"MATCH (drug:Drug {{drug_id: '{_esc(r['compound_id'])}'}}), "
            f"(event:AdverseEvent {{event_id: '{_esc(r['side_effect_id'])}'}}) "
            f"CREATE (rep:Report {{report_id: '{_esc(r['report_id'])}', "
            f"compound_id: '{_esc(r['compound_id'])}', "
            f"side_effect_id: '{_esc(r['side_effect_id'])}'}}), "
            f"(rep)-[:OF_DRUG]->(drug), "
            f"(rep)-[:REPORTS_EVENT]->(event)"
        )
    tx.commit()
    counts = _edge_counts(db)
    log_lines.append(f"  after tx5 (Reports):     {counts}")
    assert counts["TARGETS"] == len(cbg), (
        f"tx5 corrupted tx2 TARGETS: was {len(cbg)}, now {counts['TARGETS']}"
    )
    assert counts["IN_PATHWAY"] == len(gppw), (
        f"tx5 corrupted tx3 IN_PATHWAY: was {len(gppw)}, now "
        f"{counts['IN_PATHWAY']}"
    )
    assert counts["CAUSES"] == len(ccse), (
        f"tx5 corrupted tx4 CAUSES: was {len(ccse)}, now {counts['CAUSES']}"
    )
    assert counts["Report_NODES"] == len(reports), (
        f"tx5 lost Report nodes: got {counts['Report_NODES']}/{len(reports)}"
    )
    assert counts["OF_DRUG"] == len(reports), (
        f"tx5 lost OF_DRUG: got {counts['OF_DRUG']}/{len(reports)}"
    )
    assert counts["REPORTS_EVENT"] == len(reports), (
        f"tx5 lost REPORTS_EVENT: got "
        f"{counts['REPORTS_EVENT']}/{len(reports)}"
    )

    # Print the per-step probe report BEFORE the expensive query, so we
    # see ingest health even if the join below times out.
    with capsys.disabled():
        print("\n".join(log_lines))

    # The mechanistic_path 6-hop join was the original "0 rows" symptom.
    # At uncapped Hetionet scale this join is genuinely huge (5k+ CcSE
    # × 600+ CbG with shared genes/pathways = explosive). We pin a
    # single drug to bound the search so we can verify NONZERO output
    # without timing out the test at uncapped scale.
    sample_drug = compounds[0]["compound_id"]
    mech_count = _count(
        db,
        f"MATCH (d:Drug {{drug_id: '{_esc(sample_drug)}'}})"
        "-[:TARGETS]->(g:Gene)-[:IN_PATHWAY]->(p:Pathway)"
        "<-[:IN_PATHWAY]-(g2:Gene)<-[:TARGETS]-(d2:Drug)-[:CAUSES]"
        "->(s:AdverseEvent) "
        f"WHERE d.drug_id <> d2.drug_id "
        "RETURN count(*) AS cnt",
    )
    with capsys.disabled():
        print(
            f"  mechanistic_path bounded-to-{sample_drug}: {mech_count} rows"
        )
    assert mech_count > 0, (
        f"mechanistic_path returned 0 rows for drug {sample_drug} — this "
        f"is the original symptom"
    )
