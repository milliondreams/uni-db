# Release Notes — 3.0.1

**Tag:** `v3.0.1` · **Range:** `v3.0.0..v3.0.1`

Written retrospectively from the commit range: this release shipped tagged but
without notes.

## GraphCompute — Plugin Compute ABI (phases 0-4)

The headline change is `feat(graph-compute): implement the Plugin Compute ABI
(phases 0-4)` — the guest-authorable graph-algorithm surface, letting a plugin
drive coarse compute kernels over opaque graph handles rather than reimplementing
traversal. See the GraphCompute chapter in `docs/UNI_BLACK_BOOK.md`.

## Housekeeping

- `uni-pydantic` `uv.lock` synced to 3.0.1.
- rustfmt violation fixed in `scratch.rs`.

Being a patch release on top of 3.0.0, this carries no breaking changes; the 3.0
breaking changes (including the removal of the four dead registrable plugin
surfaces) are documented in `RELEASE_NOTES_3.0.0.md`.
