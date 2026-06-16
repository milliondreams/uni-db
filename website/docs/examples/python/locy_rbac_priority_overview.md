# Authorization Conflict Resolution

**Industry**: Enterprise IT / Security | **Role**: CISO, Identity & Access Management Lead | **Time to value**: 2 hours

## The Problem

Every non-trivial access control system eventually produces contradictory rules. A user belongs to a group that is allowed access, but also matches a deny policy. When these conflicts are resolved by code ordering or last-write-wins, the result is unpredictable access decisions that auditors flag and engineers cannot explain.

## The Traditional Approach

Teams build custom middleware with nested if/else chains that encode precedence by hand. Deny-overrides-allow is a design intent, but in practice it lives in scattered conditionals across 500-2,000 lines of application code. When a new role is added, engineers must trace every branch to confirm the deny still wins. Policy changes require code deploys, and auditors receive screenshots instead of proofs.

## With Uni

The notebook defines allow and deny as two declarative rules with the same name, each annotated with an explicit `PRIORITY`. Uni's engine resolves conflicts automatically: when both allow and deny fire for the same principal-resource pair, the higher-priority rule wins deterministically, leaving one decision per pair in the materialized `access` relation. Changing precedence means editing a priority annotation, not refactoring middleware.

## What You'll See

- Correct access decisions when allow and deny rules overlap, with no manual precedence logic
- Explicit deny-override-allow semantics declared in 2 prioritized rules, not 800 lines of branching code
- An inspectable, declarative rule set whose precedence is a single `PRIORITY` annotation

## Why It Matters

Misconfigured access control is a leading root cause of cloud breaches. Replacing hand-coded precedence with auditable, priority-annotated rules eliminates an entire class of security defects without adding headcount.

---

[Run the notebook →](locy_rbac_priority.md)
