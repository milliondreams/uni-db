# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Phase 4b — fork quickstart (sync API).

Demonstrates the basic fork lifecycle: create, write, query, drop.
Run with: ``python examples/fork_quickstart.py``
"""

from __future__ import annotations

import uni_db


def main() -> None:
    db = uni_db.Uni.builder().disable_fork_sweeper(True).build()
    db.schema().label("Person").property("name", "string").apply()

    primary = db.session()
    tx = primary.tx()
    tx.execute("CREATE (:Person {name: 'Primary-Alice'})")
    tx.commit()
    db.flush()

    print("=== primary view ===")
    rows = primary.query("MATCH (p:Person) RETURN p.name AS name")
    print(f"  primary: {len(rows)} row(s) — {[r['name'] for r in rows]}")

    # Open a fork. Writes through it land on the fork's Lance branches
    # without touching primary.
    fork = primary.fork("scenario").build()
    tx = fork.tx()
    tx.execute("CREATE (:Person {name: 'Fork-Bob'})")
    tx.commit()

    print("\n=== fork view ===")
    rows = fork.query("MATCH (p:Person) RETURN p.name AS name")
    print(f"  fork: {len(rows)} row(s) — {sorted(r['name'] for r in rows)}")

    print("\n=== primary after fork write ===")
    rows = primary.query("MATCH (p:Person) RETURN p.name AS name")
    print(f"  primary: {len(rows)} row(s) — {[r['name'] for r in rows]}")

    # Release the session reference and drop the fork.
    del fork
    db.drop_fork("scenario")

    remaining = [f.name for f in db.list_forks()]
    print(f"\nforks remaining: {remaining}")


if __name__ == "__main__":
    main()
