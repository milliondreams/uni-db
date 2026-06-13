# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Durability of the database-level Locy rule registry across restarts."""

import uni_db

REACH_RULE = "CREATE RULE reach AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b"


def _open(path):
    db = uni_db.UniBuilder.open(str(path)).build()
    return db


def _apply_schema(db):
    (
        db.schema()
        .label("Node")
        .property("name", "string")
        .done()
        .edge_type("EDGE", ["Node"], ["Node"])
        .done()
        .apply()
    )


def test_db_rules_survive_restart(tmp_path):
    db = _open(tmp_path)
    _apply_schema(db)
    db.rules().register(REACH_RULE)
    db.flush()
    del db

    db2 = _open(tmp_path)
    assert "reach" in db2.rules().list()


def test_duplicate_registration_is_idempotent(tmp_path):
    db = _open(tmp_path)
    _apply_schema(db)
    db.rules().register(REACH_RULE)
    db.rules().register(REACH_RULE)
    assert db.rules().count() == 1
    db.flush()
    del db

    db2 = _open(tmp_path)
    assert db2.rules().count() == 1
    db2.rules().register(REACH_RULE)  # startup re-register stays a no-op
    assert db2.rules().count() == 1


def test_clear_persists_empty(tmp_path):
    db = _open(tmp_path)
    _apply_schema(db)
    db.rules().register(REACH_RULE)
    db.rules().clear()
    db.flush()
    del db

    db2 = _open(tmp_path)
    assert db2.rules().count() == 0


async def test_async_db_rules_survive_restart(tmp_path):
    db = await uni_db.AsyncUniBuilder.open(str(tmp_path)).build()
    (
        await db.schema()
        .label("Node")
        .property("name", "string")
        .done()
        .edge_type("EDGE", ["Node"], ["Node"])
        .done()
        .apply()
    )
    await db.rules().register(REACH_RULE)
    assert "reach" in db.rules().list()
    await db.flush()
    del db

    db2 = await uni_db.AsyncUniBuilder.open(str(tmp_path)).build()
    assert "reach" in db2.rules().list()
