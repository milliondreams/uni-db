# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for the locy_evaluate binding on AsyncDatabase."""

import pytest

import uni_db

# ---------------------------------------------------------------------------
# Helpers (same programs as sync tests)
# ---------------------------------------------------------------------------

FRIENDS_PROGRAM = """
CREATE RULE friends AS
  MATCH (a:Person)-[:KNOWS]->(b:Person)
  YIELD KEY a, KEY b
"""

REACHABLE_PROGRAM = """
CREATE RULE reachable AS
  MATCH (a:Person)-[:KNOWS]->(b:Person)
  YIELD KEY a, KEY b
CREATE RULE reachable AS
  MATCH (a:Person)-[:KNOWS]->(mid:Person)
  WHERE mid IS reachable TO b
  YIELD KEY a, KEY b
"""

MULTI_RULE_PROGRAM = """
CREATE RULE friends AS
  MATCH (a:Person)-[:KNOWS]->(b:Person)
  YIELD KEY a, KEY b
CREATE RULE popular AS
  MATCH (n:Person)
  WHERE n IS friends TO m
  YIELD KEY n
"""

QUERY_PROGRAM = """
CREATE RULE friends AS
  MATCH (a:Person)-[:KNOWS]->(b:Person)
  YIELD KEY a, KEY b
QUERY friends WHERE a.name = 'Alice' RETURN b.name AS to_name
"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_basic_rule_evaluation(async_social_db_populated):
    """Async: a simple rule derives facts from graph edges."""
    db = async_social_db_populated
    result = await db.locy_evaluate(FRIENDS_PROGRAM)

    assert "derived" in result
    assert "stats" in result
    assert "command_results" in result

    friends_rows = result["derived"].get("friends", [])
    assert len(friends_rows) == 4, f"Expected 4 friend pairs, got {len(friends_rows)}"

    for row in friends_rows:
        assert "a" in row
        assert "b" in row


async def test_stats_returned(async_social_db_populated):
    """Async: result includes a LocyStats object with expected fields."""
    db = async_social_db_populated
    result = await db.locy_evaluate(FRIENDS_PROGRAM)

    stats = result["stats"]
    assert isinstance(stats, uni_db.LocyStats)
    assert stats.strata_evaluated >= 1
    # Non-recursive strata may complete in 0 fixpoint iterations
    assert stats.total_iterations >= 0
    assert stats.evaluation_time_secs >= 0.0
    assert stats.queries_executed >= 0
    assert stats.mutations_executed >= 0
    assert stats.peak_memory_bytes >= 0


async def test_query_command(async_social_db_populated):
    """Async: QUERY command result appears in command_results."""
    db = async_social_db_populated
    result = await db.locy_evaluate(QUERY_PROGRAM)

    cmd_results = result["command_results"]
    assert len(cmd_results) >= 1

    query_result = cmd_results[0]
    assert query_result["type"] == "query"
    rows = query_result["rows"]
    # Alice is connected to Bob and Charlie
    assert len(rows) >= 1
    to_names = {r["to_name"] for r in rows}
    assert "Bob" in to_names


async def test_recursive_rule(async_social_db_populated):
    """Async: recursive rule computes transitive closure."""
    db = async_social_db_populated
    result = await db.locy_evaluate(REACHABLE_PROGRAM)

    reachable_rows = result["derived"].get("reachable", [])
    assert len(reachable_rows) >= 3


async def test_multi_rule_program(async_social_db_populated):
    """Async: multiple rules can be defined in one program."""
    db = async_social_db_populated
    result = await db.locy_evaluate(MULTI_RULE_PROGRAM)

    assert "friends" in result["derived"]
    assert "popular" in result["derived"]
    assert len(result["derived"]["popular"]) >= 1


async def test_config_override(async_social_db_populated):
    """Async: custom config is respected (max_iterations)."""
    db = async_social_db_populated
    result = await db.locy_evaluate(FRIENDS_PROGRAM, config={"max_iterations": 1})
    assert len(result["derived"].get("friends", [])) == 4


async def test_empty_program_raises(async_social_db_populated):
    """Async: empty program raises RuntimeError (Locy requires at least one statement)."""
    db = async_social_db_populated
    with pytest.raises(RuntimeError):
        await db.locy_evaluate("")


async def test_error_on_invalid_program(async_social_db_populated):
    """Async: invalid Locy program text raises RuntimeError."""
    db = async_social_db_populated
    with pytest.raises(RuntimeError):
        await db.locy_evaluate("THIS IS COMPLETELY INVALID LOCY SYNTAX !!!!")


async def test_derived_facts_structure(async_social_db_populated):
    """Async: derived fact rows are Python dicts with string keys."""
    db = async_social_db_populated
    result = await db.locy_evaluate(FRIENDS_PROGRAM)

    for row in result["derived"].get("friends", []):
        assert isinstance(row, dict)
        for key in row:
            assert isinstance(key, str)
