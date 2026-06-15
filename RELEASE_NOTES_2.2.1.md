# Uni 2.2.1 Release Notes

**Release scope:** `2.2.0` → `2.2.1` · Locy planner bug-fix patch
**Date:** 2026-06-15
**Version path:** `2.2.0` → `2.2.1`

Uni 2.2.1 is a focused **bug-fix patch**. It fixes two Locy logic-programming planner bugs that surfaced in the flagship `locy_patent_fto` notebook (which broke the 2.2.0 release-docs build). The on-disk format and all public APIs are unchanged.

---

## Fixes

### IS-ref value columns now resolve in FOLD / YIELD / WHERE

A clause's second and later positive `IS`-references alias their derived-scan columns with an `__isref{n}_` prefix to avoid name collisions. A `FOLD` aggregate input, `YIELD` expression, or deferred `WHERE` filter that referenced such a value column **by its bare name** resolved against a non-existent field, failing at plan time with:

```
DataFusion planning failed: Schema error: No field named mapping_conf.
Did you mean '__isref1_mapping_conf'?
```

This hit any rule shaped like:

```
WHERE c IS claim_elements TO ce, p IS element_mapped TO ce
FOLD infringement = MPROD(mapping_conf)   -- mapping_conf yielded by the 2nd IS-ref
```

References to a non-first IS-ref's non-KEY value columns are now rewritten to the aliased name across FOLD inputs, YIELD expressions, and deferred WHERE filters.

### Shared `IS`-reference target no longer inflates aggregates

When two `IS`-references in one clause share the **same target variable** (`... IS r1 TO ce, ... IS r2 TO ce`), the planner re-scanned the target node for each reference, cross-joining it with itself. Aggregates over the joined value columns were computed over the cartesian square of the shared set — e.g. `MPROD` returned `0.5⁴·0.4⁴` instead of `0.5·0.4`. A shared (or MATCH-bound) target is now bound as a join **constraint** rather than re-scanned, so aggregates are computed over the correct row set.

---

## Upgrade notes

No action required. No breaking changes, no data-format migration.
