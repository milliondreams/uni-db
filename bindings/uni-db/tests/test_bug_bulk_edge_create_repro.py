# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Python-side reproducers for the same two suspected bugs covered by
# crates/uni/tests/bug_bulk_edge_create_repro.rs. The Rust tests pass at
# every scale we tried, so if these Python tests fail the bug is in the
# PyO3 binding layer (tx wrapping / GIL handling / result conversion),
# not in the storage/planner core.

import pytest

import uni_db


async def _bipartite_db():
    db = await uni_db.AsyncUni.temporary()
    await (
        db.schema()
        .label("Src")
        .property("sid", "int")
        .done()
        .label("Dst")
        .property("did", "int")
        .done()
        .edge_type("REL", ["Src"], ["Dst"])
        .apply()
    )
    return db


async def _seed_nodes(db, n_src: int, n_dst: int) -> None:
    session = db.session()
    tx = await session.tx()
    for i in range(n_src):
        await tx.execute(f"CREATE (:Src {{sid: {i}}})")
    for j in range(n_dst):
        await tx.execute(f"CREATE (:Dst {{did: {j}}})")
    await tx.commit()


async def _edge_count(db) -> int:
    rows = await db.session().query("MATCH ()-[r:REL]->() RETURN count(r) AS cnt")
    return rows[0]["cnt"]


# ── Bug A: single-tx MATCH+CREATE silent drop ────────────────────────


@pytest.mark.asyncio
async def test_bug_a_baseline_small_match_create():
    db = await _bipartite_db()
    await _seed_nodes(db, 10, 10)

    session = db.session()
    tx = await session.tx()
    await tx.execute("MATCH (s:Src), (d:Dst) CREATE (s)-[:REL]->(d)")
    await tx.commit()

    cnt = await _edge_count(db)
    assert cnt == 100, f"baseline cartesian-CREATE lost edges: got {cnt}"


@pytest.mark.asyncio
async def test_bug_a_match_create_above_suspected_ceiling():
    db = await _bipartite_db()
    await _seed_nodes(db, 100, 60)  # 6000 expected

    session = db.session()
    tx = await session.tx()
    await tx.execute("MATCH (s:Src), (d:Dst) CREATE (s)-[:REL]->(d)")

    in_tx_rows = await tx.query("MATCH ()-[r:REL]->() RETURN count(r) AS cnt")
    in_tx_cnt = in_tx_rows[0]["cnt"]
    await tx.commit()

    after_cnt = await _edge_count(db)

    assert in_tx_cnt == 6000, (
        f"PLANNER/BINDING-LEVEL DROP: in-tx read saw {in_tx_cnt} edges, expected 6000"
    )
    assert after_cnt == 6000, (
        f"WRITER/BINDING-LEVEL DROP: post-commit read saw {after_cnt} "
        f"edges, expected 6000 (in-tx had {in_tx_cnt})"
    )


@pytest.mark.asyncio
async def test_bug_a_locate_ceiling_diagnostic(capsys):
    """Ladder of expansion sizes. Always passes — prints a diagnostic so we
    can see where the first loss occurs even if the gating test above
    masks the precise threshold."""
    rungs = [(20, 20), (30, 30), (40, 40), (45, 45), (50, 50), (60, 60), (80, 80)]
    report_lines = ["", "bug_a ceiling diagnostic (Python binding)"]
    for ns, nd in rungs:
        db = await _bipartite_db()
        await _seed_nodes(db, ns, nd)
        expected = ns * nd

        tx = await db.session().tx()
        await tx.execute("MATCH (s:Src), (d:Dst) CREATE (s)-[:REL]->(d)")
        await tx.commit()

        got = await _edge_count(db)
        marker = "ok" if got == expected else "LOSS"
        report_lines.append(
            f"  {ns:>3} x {nd:>3} = {expected:>5} expected, {got:>5} actual  [{marker}]"
        )

    # Use capsys-bypassing print so the report shows even on pass.
    with capsys.disabled():
        print("\n".join(report_lines))


# ── Bug B: multi-tx edge corruption ──────────────────────────────────


@pytest.mark.asyncio
async def test_bug_b_sequential_edge_txs_preserve_each_other():
    db = await _bipartite_db()
    await _seed_nodes(db, 60, 60)

    predicates = [
        "(s.sid + d.did) % 3 = 0",
        "(s.sid + d.did) % 3 = 1",
        "(s.sid + d.did) % 3 = 2",
    ]

    running_total = 0
    for idx, pred in enumerate(predicates):
        tx = await db.session().tx()
        await tx.execute(f"MATCH (s:Src), (d:Dst) WHERE {pred} CREATE (s)-[:REL]->(d)")
        await tx.commit()

        cnt = await _edge_count(db)
        assert cnt > running_total, (
            f"tx{idx + 1} reduced total edge count: was {running_total}, now {cnt}"
        )
        running_total = cnt

    assert running_total == 3600, (
        f"multi-tx edge accumulation lost edges: final={running_total}, expected 3600"
    )


# ── Bug C: per-row sequential MATCH+CREATE inside one tx ────────────
#
# This is the pattern the ADR / DDI prep scripts actually use: for each
# edge row in a CSV, issue a separate `tx.execute("MATCH (a {id:X}),
# (b {id:Y}) CREATE (a)-[:REL]->(b)")`. Many sequential executes inside
# one tx, each producing one edge via id-lookup. Suspected to trigger
# the original silent-drop symptom.


async def _seed_keyed_nodes(db, n_src: int, n_dst: int) -> None:
    session = db.session()
    tx = await session.tx()
    for i in range(n_src):
        await tx.execute(f"CREATE (:Src {{sid: {i}}})")
    for j in range(n_dst):
        await tx.execute(f"CREATE (:Dst {{did: {j}}})")
    await tx.commit()


@pytest.mark.asyncio
async def test_bug_c_per_row_match_create_in_one_tx_baseline():
    """100 edges via 100 sequential per-row execute() calls in one tx."""
    db = await _bipartite_db()
    await _seed_keyed_nodes(db, 10, 10)

    tx = await db.session().tx()
    for i in range(10):
        for j in range(10):
            await tx.execute(
                f"MATCH (s:Src {{sid: {i}}}), (d:Dst {{did: {j}}}) "
                f"CREATE (s)-[:REL]->(d)"
            )
    await tx.commit()

    cnt = await _edge_count(db)
    assert cnt == 100, f"per-row baseline lost edges: got {cnt}/100"


@pytest.mark.asyncio
async def test_bug_c_per_row_match_create_above_suspected_ceiling():
    """3000 edges via 3000 sequential per-row execute() calls in one tx.
    Above the suspected ~2000 ceiling."""
    db = await _bipartite_db()
    await _seed_keyed_nodes(db, 60, 50)  # 3000 expected

    tx = await db.session().tx()
    issued = 0
    for i in range(60):
        for j in range(50):
            await tx.execute(
                f"MATCH (s:Src {{sid: {i}}}), (d:Dst {{did: {j}}}) "
                f"CREATE (s)-[:REL]->(d)"
            )
            issued += 1

    # Read inside the tx first to localise the layer of the drop.
    in_tx_rows = await tx.query("MATCH ()-[r:REL]->() RETURN count(r) AS cnt")
    in_tx_cnt = in_tx_rows[0]["cnt"]
    await tx.commit()

    after_cnt = await _edge_count(db)

    assert in_tx_cnt == issued == 3000, (
        f"PER-ROW IN-TX DROP: issued {issued} executes, in-tx read saw "
        f"{in_tx_cnt} edges"
    )
    assert after_cnt == 3000, (
        f"PER-ROW POST-COMMIT DROP: post-commit read saw {after_cnt} "
        f"edges, expected 3000 (in-tx had {in_tx_cnt})"
    )


@pytest.mark.asyncio
async def test_bug_c_locate_per_row_ceiling_diagnostic(capsys):
    """Ladder of per-row counts, isolated dbs per rung. Always passes."""
    rungs = [500, 1000, 1500, 2000, 2500, 3000, 4000, 5000]
    report_lines = ["", "bug_c per-row ceiling diagnostic (Python binding)"]
    for target_count in rungs:
        # Use a square-ish grid to hit target_count edges.
        side = max(1, int(target_count**0.5))
        # Round up so n_src * n_dst >= target_count, then trim.
        n_src = side
        n_dst = (target_count + side - 1) // side
        edges_to_issue = min(target_count, n_src * n_dst)

        db = await _bipartite_db()
        await _seed_keyed_nodes(db, n_src, n_dst)

        tx = await db.session().tx()
        issued = 0
        for i in range(n_src):
            if issued >= edges_to_issue:
                break
            for j in range(n_dst):
                if issued >= edges_to_issue:
                    break
                await tx.execute(
                    f"MATCH (s:Src {{sid: {i}}}), (d:Dst {{did: {j}}}) "
                    f"CREATE (s)-[:REL]->(d)"
                )
                issued += 1
        await tx.commit()

        got = await _edge_count(db)
        marker = "ok" if got == issued else "LOSS"
        report_lines.append(
            f"  issued {issued:>5} per-row executes, {got:>5} edges visible  [{marker}]"
        )

    with capsys.disabled():
        print("\n".join(report_lines))


@pytest.mark.asyncio
async def test_bug_b_many_small_edge_txs_preserve_each_other():
    db = await _bipartite_db()
    await _seed_nodes(db, 60, 60)

    n_slices = 10
    running_total = 0
    for slice_idx in range(n_slices):
        tx = await db.session().tx()
        await tx.execute(
            f"MATCH (s:Src), (d:Dst) "
            f"WHERE (s.sid + d.did) % {n_slices} = {slice_idx} "
            f"CREATE (s)-[:REL]->(d)"
        )
        await tx.commit()

        cnt = await _edge_count(db)
        assert cnt > running_total, (
            f"tx for slice {slice_idx} reduced edge count: "
            f"was {running_total}, now {cnt}"
        )
        running_total = cnt

    assert running_total == 3600, (
        f"10-tx edge accumulation lost edges: final={running_total}, expected 3600"
    )
