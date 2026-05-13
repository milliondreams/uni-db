# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Phase 4b — fork audit workflow (sync API).

Spec §3.6: stage a counterfactual in a fork, tag it for audit
retention, then drop the fork. The Lance tag survives the drop, so the
audit-time state stays on disk GC-exempt.

Run with: ``python examples/fork_audit.py``
"""

from __future__ import annotations

import uni_db


def main() -> None:
    db = uni_db.Uni.builder().disable_fork_sweeper(True).build()
    db.schema().label("Loan").property("status", "string").property(
        "amount", "float"
    ).apply()

    primary = db.session()
    tx = primary.tx()
    tx.execute("CREATE (:Loan {status: 'current', amount: 250000.0})")
    tx.execute("CREATE (:Loan {status: 'current', amount: 75000.0})")
    tx.commit()
    db.flush()

    # Stage a stress-test scenario: add a stressed-portfolio loan that
    # only exists on the fork's branch, never on primary.
    scenario = primary.fork("default_stress_q1").build()
    tx = scenario.tx()
    tx.execute("CREATE (:Loan {status: 'default', amount: 250000.0})")
    tx.commit()
    scenario.flush()

    rows = scenario.query(
        "MATCH (l:Loan {status: 'default'}) RETURN count(l) AS defaulted"
    )
    print(f"loans flagged as defaulted in scenario: {rows[0]['defaulted']}")
    primary_rows = primary.query(
        "MATCH (l:Loan {status: 'default'}) RETURN count(l) AS defaulted"
    )
    print(f"loans flagged as defaulted in primary: {primary_rows[0]['defaulted']}")

    # Pin the scenario's branches with a Lance tag — GC-exempt, so the
    # state survives the fork drop and can be re-opened in the future
    # for audit / regulatory review.
    del scenario
    db.tag_fork("default_stress_q1", "audit-2026-q1")

    print(f"applied tags: {db.list_fork_tags('default_stress_q1')}")

    # Drop the fork. Branches go away from the fork registry; the
    # tagged versions remain referenceable through Lance refs.
    db.drop_fork("default_stress_q1")

    remaining = [f.name for f in db.list_forks()]
    print(f"forks remaining after drop: {remaining}")
    print("(tagged Lance refs preserved on disk for audit retention)")


if __name__ == "__main__":
    main()
