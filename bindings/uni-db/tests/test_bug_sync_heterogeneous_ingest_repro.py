# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Faithful reproducers for the silent-edge-drop symptom observed in the
# ADR Hetionet ingest. Critically: uses the *sync* uni_db.Uni API
# (uni_db.UniBuilder.temporary().build()), which is what the flagship
# notebook actually runs against. The earlier AsyncUni-based reproducers
# all passed, so if the bug is in the PyO3 binding it likely lives on
# the sync path.
#
# Two reproducers:
#
#   bug_d — heterogeneous-tx silent drop. One tx mixing CREATE-of-new-nodes
#           and 4 different edge types via per-row MATCH+CREATE, including
#           a Report-mediator pattern that creates a new node + two
#           outbound edges in a single execute() — the exact tx2 shape of
#           generate_adverse_drug_reaction_flagship_notebook.py.
#
#   bug_e — multi-edge-tx interference. tx2 creates edge batch A, commits;
#           tx3 creates edge batch B, commits; assert all of batch A is
#           still readable after tx3 commits. The notebook ingest comment
#           explicitly documents this failure mode: "the second edge-tx
#           commit wipes the first tx's edges."

import pytest
import uni_db


def _hetero_schema_db():
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
        .property("count", "int")
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


def _seed_nodes_sync(
    db, n_drugs: int, n_genes: int, n_pathways: int, n_aes: int
) -> None:
    session = db.session()
    tx = session.tx()
    for i in range(n_drugs):
        tx.execute(f"CREATE (:Drug {{drug_id: 'd{i}', name: 'drug-{i}'}})")
    for i in range(n_genes):
        tx.execute(f"CREATE (:Gene {{gene_id: 'g{i}', name: 'gene-{i}'}})")
    for i in range(n_pathways):
        tx.execute(
            f"CREATE (:Pathway {{pathway_id: 'p{i}', name: 'path-{i}'}})"
        )
    for i in range(n_aes):
        tx.execute(
            f"CREATE (:AdverseEvent "
            f"{{event_id: 'a{i}', meddra_term: 'term-{i}'}})"
        )
    tx.commit()


def _edge_count_sync(db, rel: str | None = None) -> int:
    pattern = f"[r:{rel}]" if rel else "[r]"
    rows = db.session().query(
        f"MATCH ()-{pattern}->() RETURN count(r) AS cnt"
    )
    return rows[0]["cnt"]


# ── Bug D: heterogeneous single-tx ingest (sync API) ─────────────────


def test_bug_d_heterogeneous_single_tx_ingest_no_drop():
    """Mirrors the ADR tx2 shape but at controlled scale.

    Per-row counts: 30 TARGETS + 40 IN_PATHWAY + 60 CAUSES + 50 reports
    (each producing 1 Report node + 2 edges) = 230 executes, producing
    130 simple edges + 50 nodes + 100 Report edges = 230 edges total.
    Then we ramp up: 600 + 800 + 1200 + 1000 reports = 3600 executes,
    producing ~5600 edges (the rough ADR scale)."""
    db = _hetero_schema_db()
    _seed_nodes_sync(db, 30, 60, 40, 60)

    n_targets = 600
    n_in_pathway = 800
    n_causes = 1200
    n_reports = 1000

    session = db.session()
    tx = session.tx()

    for k in range(n_targets):
        di = k % 30
        gi = k % 60
        tx.execute(
            f"MATCH (d:Drug {{drug_id: 'd{di}'}}), (g:Gene {{gene_id: 'g{gi}'}}) "
            f"CREATE (d)-[:TARGETS]->(g)"
        )
    for k in range(n_in_pathway):
        gi = k % 60
        pi = k % 40
        tx.execute(
            f"MATCH (g:Gene {{gene_id: 'g{gi}'}}), (p:Pathway {{pathway_id: 'p{pi}'}}) "
            f"CREATE (g)-[:IN_PATHWAY]->(p)"
        )
    for k in range(n_causes):
        di = k % 30
        ai = k % 60
        tx.execute(
            f"MATCH (d:Drug {{drug_id: 'd{di}'}}), (a:AdverseEvent {{event_id: 'a{ai}'}}) "
            f"CREATE (d)-[:CAUSES]->(a)"
        )
    for k in range(n_reports):
        di = k % 30
        ai = k % 60
        tx.execute(
            f"MATCH (d:Drug {{drug_id: 'd{di}'}}), (a:AdverseEvent {{event_id: 'a{ai}'}}) "
            f"CREATE (rep:Report {{report_id: 'r{k}', count: {k}}}), "
            f"(rep)-[:OF_DRUG]->(d), (rep)-[:REPORTS_EVENT]->(a)"
        )
    tx.commit()

    targets = _edge_count_sync(db, "TARGETS")
    in_pathway = _edge_count_sync(db, "IN_PATHWAY")
    causes = _edge_count_sync(db, "CAUSES")
    of_drug = _edge_count_sync(db, "OF_DRUG")
    reports_event = _edge_count_sync(db, "REPORTS_EVENT")
    reports = db.session().query(
        "MATCH (r:Report) RETURN count(r) AS cnt"
    )[0]["cnt"]

    assert targets == n_targets, f"TARGETS lost: {targets}/{n_targets}"
    assert in_pathway == n_in_pathway, (
        f"IN_PATHWAY lost: {in_pathway}/{n_in_pathway}"
    )
    assert causes == n_causes, f"CAUSES lost: {causes}/{n_causes}"
    assert reports == n_reports, f"Report nodes lost: {reports}/{n_reports}"
    assert of_drug == n_reports, f"OF_DRUG lost: {of_drug}/{n_reports}"
    assert reports_event == n_reports, (
        f"REPORTS_EVENT lost: {reports_event}/{n_reports}"
    )


# ── Bug E: multi-edge-tx interference (sync API) ─────────────────────


def test_bug_e_multi_edge_tx_first_batch_preserved():
    """Three sequential edge txs after a seed tx — each commits its own
    slice. After tx3 commits, the edges written by tx2 and tx3 must
    both still be readable. The notebook comment claims the second
    edge-tx commit can wipe the first's edges."""
    db = _hetero_schema_db()
    _seed_nodes_sync(db, 30, 60, 40, 60)

    # tx2 — 600 TARGETS
    tx = db.session().tx()
    for k in range(600):
        di, gi = k % 30, k % 60
        tx.execute(
            f"MATCH (d:Drug {{drug_id: 'd{di}'}}), (g:Gene {{gene_id: 'g{gi}'}}) "
            f"CREATE (d)-[:TARGETS]->(g)"
        )
    tx.commit()
    after_tx2_targets = _edge_count_sync(db, "TARGETS")
    assert after_tx2_targets == 600, (
        f"tx2 didn't land its own edges: {after_tx2_targets}/600"
    )

    # tx3 — 800 IN_PATHWAY
    tx = db.session().tx()
    for k in range(800):
        gi, pi = k % 60, k % 40
        tx.execute(
            f"MATCH (g:Gene {{gene_id: 'g{gi}'}}), (p:Pathway {{pathway_id: 'p{pi}'}}) "
            f"CREATE (g)-[:IN_PATHWAY]->(p)"
        )
    tx.commit()

    targets_after_tx3 = _edge_count_sync(db, "TARGETS")
    in_pathway_after_tx3 = _edge_count_sync(db, "IN_PATHWAY")
    assert targets_after_tx3 == 600, (
        f"tx3 commit corrupted tx2 TARGETS edges: was 600, now "
        f"{targets_after_tx3}"
    )
    assert in_pathway_after_tx3 == 800, (
        f"tx3 didn't land its own edges: {in_pathway_after_tx3}/800"
    )

    # tx4 — 1200 CAUSES
    tx = db.session().tx()
    for k in range(1200):
        di, ai = k % 30, k % 60
        tx.execute(
            f"MATCH (d:Drug {{drug_id: 'd{di}'}}), (a:AdverseEvent {{event_id: 'a{ai}'}}) "
            f"CREATE (d)-[:CAUSES]->(a)"
        )
    tx.commit()

    targets_final = _edge_count_sync(db, "TARGETS")
    in_pathway_final = _edge_count_sync(db, "IN_PATHWAY")
    causes_final = _edge_count_sync(db, "CAUSES")
    assert targets_final == 600, (
        f"tx4 commit corrupted tx2 TARGETS edges: was 600, now "
        f"{targets_final}"
    )
    assert in_pathway_final == 800, (
        f"tx4 commit corrupted tx3 IN_PATHWAY edges: was 800, now "
        f"{in_pathway_final}"
    )
    assert causes_final == 1200, (
        f"tx4 didn't land its own edges: {causes_final}/1200"
    )
