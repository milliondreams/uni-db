# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Sync E2E tests for the locy_evaluate binding."""

import pytest

import uni_db

# ---------------------------------------------------------------------------
# Helpers
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


def test_basic_rule_evaluation(social_db_populated):
    """A simple rule derives facts from graph edges."""
    db = social_db_populated
    result = db.locy_evaluate(FRIENDS_PROGRAM)

    assert "derived" in result
    assert "stats" in result
    assert "command_results" in result

    # friends rule captures 4 KNOWS edges
    friends_rows = result["derived"].get("friends", [])
    assert len(friends_rows) == 4, f"Expected 4 friend pairs, got {len(friends_rows)}"

    # Each row has keys 'a' and 'b'
    for row in friends_rows:
        assert "a" in row
        assert "b" in row


def test_stats_returned(social_db_populated):
    """Result includes a LocyStats object with expected fields."""
    db = social_db_populated
    result = db.locy_evaluate(FRIENDS_PROGRAM)

    stats = result["stats"]
    assert isinstance(stats, uni_db.LocyStats)
    assert stats.strata_evaluated >= 1
    # Non-recursive strata may complete in 0 fixpoint iterations
    assert stats.total_iterations >= 0
    assert stats.evaluation_time_secs >= 0.0
    assert stats.queries_executed >= 0
    assert stats.mutations_executed >= 0
    assert stats.peak_memory_bytes >= 0


def test_query_command(social_db_populated):
    """QUERY command result appears in command_results."""
    db = social_db_populated
    result = db.locy_evaluate(QUERY_PROGRAM)

    cmd_results = result["command_results"]
    assert len(cmd_results) >= 1

    query_result = cmd_results[0]
    assert query_result["type"] == "query"
    rows = query_result["rows"]
    # Alice is connected to Bob and Charlie
    assert len(rows) >= 1
    to_names = {r["to_name"] for r in rows}
    assert "Bob" in to_names


def test_recursive_rule(social_db_populated):
    """Recursive rule computes transitive closure."""
    db = social_db_populated
    result = db.locy_evaluate(REACHABLE_PROGRAM)

    reachable_rows = result["derived"].get("reachable", [])
    # Alice→Bob, Bob→Charlie, Alice→Charlie (base), plus transitive pairs
    assert len(reachable_rows) >= 3


def test_multi_rule_program(social_db_populated):
    """Multiple rules can be defined in one program."""
    db = social_db_populated
    result = db.locy_evaluate(MULTI_RULE_PROGRAM)

    assert "friends" in result["derived"]
    assert "popular" in result["derived"]

    # All 5 people in the social DB have outgoing KNOWS edges (except Eve)
    popular_rows = result["derived"]["popular"]
    assert len(popular_rows) >= 1


def test_config_override(social_db_populated):
    """Custom config is respected (max_iterations)."""
    db = social_db_populated
    # Non-recursive program should succeed even with max_iterations=1
    result = db.locy_evaluate(FRIENDS_PROGRAM, config={"max_iterations": 1})
    assert len(result["derived"].get("friends", [])) == 4


def test_empty_program_raises(social_db_populated):
    """Empty/blank program raises RuntimeError (Locy requires at least one statement)."""
    db = social_db_populated
    with pytest.raises(RuntimeError):
        db.locy_evaluate("")


def test_error_on_invalid_program(social_db_populated):
    """Invalid Locy program text raises RuntimeError."""
    db = social_db_populated
    with pytest.raises(RuntimeError):
        db.locy_evaluate("THIS IS COMPLETELY INVALID LOCY SYNTAX !!!!")


def test_derived_facts_structure(social_db_populated):
    """Derived fact rows are Python dicts with string keys."""
    db = social_db_populated
    result = db.locy_evaluate(FRIENDS_PROGRAM)

    for row in result["derived"].get("friends", []):
        assert isinstance(row, dict)
        for key in row:
            assert isinstance(key, str)
