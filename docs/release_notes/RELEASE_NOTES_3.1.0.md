# Release Notes — 3.1.0

**Shipped at:** `65289f3db` (**untagged** — no `v3.1.0` tag exists).
**Range:** `v3.0.1..65289f3db`

Written retrospectively from the commit range: this release shipped without
notes and without a tag.

## GraphCompute hardening — index spaces and scopes

The bulk of this release closes correctness gaps in the guest-authorable
GraphCompute surface introduced in 3.0.1. Several are **breaking** (`!`):

- **Index spaces.** Tensors, sets and edge-sets each gained an index space, and
  the edge-set algebra is guarded against mixing them. A guest can no longer
  abort the host by handing back a mis-shaped handle.
- **Egress keying.** Egressed vertex ids are keyed to the value's own
  projection rather than an ambient one.
- **Named scopes.** Pre-declared named scopes, a verified way to cross them, and
  scope verification on *every* loader — `scopes` is no longer ignored.
- **Fail-closed epochs.** Epoch exhaustion fails closed, and handle resolutions
  are traced.
- **Budget.** The native-work budget is exposed and its charging documented; the
  kernel count is derived rather than hardcoded.
- New `compare` kernel; `map_to_set` made shape-polymorphic; a guest can emit
  every column it declared.

## L0 visibility

- **Breaking:** fail loud on a detached L0 tier and on unresolvable property
  names.
- L0 visibility threaded into the row-path procedure hosts.
- A read-only open keeps its WAL-replayed L0 tier.

## Docs

- The 12-gap GraphCompute closure documented across the Black Book, website and
  skills.
