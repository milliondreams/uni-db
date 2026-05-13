# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Phase 5a-impl Step 8 — Python smoke for fork-local index fusion.

The full fork-local index machinery (build trigger, planner rewrite,
operator selection) is exercised in the Rust integration tests. This
smoke test only confirms the Python binding's fork query path stays
correct end-to-end with the Phase 5a-impl plumbing in place — the
auto-builder may or may not have fired by the time queries run; either
way the result set must match what Lance's `base_paths` chain returns.
"""

from __future__ import annotations

import uni_db


def test_forked_session_query_results_match_primary_plus_fork_writes():
    db = uni_db.Uni.builder().disable_fork_sweeper(True).build()
    db.schema().label("Person").property("email", "string").property(
        "name", "string"
    ).apply()

    primary = db.session()
    tx = primary.tx()
    tx.execute("CREATE (:Person {email: 'a@x.com', name: 'Alice'})")
    tx.execute("CREATE (:Person {email: 'b@x.com', name: 'Bob'})")
    tx.commit()
    db.flush()

    fork = primary.fork("scenario").build()
    tx = fork.tx()
    tx.execute("CREATE (:Person {email: 'c@x.com', name: 'Carol-on-fork'})")
    tx.commit()

    # Fork sees all three rows via Lance base_paths chain — irrespective
    # of whether the auto-builder has registered a fork-local index yet.
    rows = fork.query("MATCH (p:Person) RETURN p.name AS name")
    names = sorted(r["name"] for r in rows)
    assert names == ["Alice", "Bob", "Carol-on-fork"]

    # Primary doesn't see the fork's row.
    primary_rows = primary.query("MATCH (p:Person) RETURN p.name AS name")
    primary_names = sorted(r["name"] for r in primary_rows)
    assert primary_names == ["Alice", "Bob"]

    # Equality lookup that would benefit from fusion: still correct.
    fork_only = fork.query("MATCH (p:Person {email: 'c@x.com'}) RETURN p.name AS name")
    assert [r["name"] for r in fork_only] == ["Carol-on-fork"]

    primary_only = fork.query(
        "MATCH (p:Person {email: 'a@x.com'}) RETURN p.name AS name"
    )
    assert [r["name"] for r in primary_only] == ["Alice"]

    del fork
    db.drop_fork("scenario")
