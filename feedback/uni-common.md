# uni-common Code Simplification Feedback

Scope: `crates/uni-common/` (~7,250 LOC across 12 files). All file paths absolute.

## High-Value Findings

### 1. Massive Duplication: `Vid` and `Eid` are byte-for-byte identical newtypes
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/core/id.rs:17-212`

`Vid` (lines 17-111) and `Eid` (lines 118-212) are ~95 lines of structurally identical code: same constants (`INVALID`, `EPHEMERAL_BIT`), same methods (`new`, `as_u64`, `is_invalid`, `ephemeral`, `is_ephemeral`, `transient_id`), same `From<u64>`, `Into<u64>`, `Default`, `Debug`, `Display`, `FromStr` impls. Only the debug prefix string differs (`"Vid("` vs `"Eid("`).

Suggestion: Define one generic newtype with a phantom marker, e.g. `pub struct Id<K: IdKind>(u64)` and trait `IdKind { const NAME: &'static str; }`, then `pub type Vid = Id<VidMarker>; pub type Eid = Id<EidMarker>;`. Alternatively, use a `define_id_newtype!` macro. Either route collapses ~95 lines to ~5 type aliases and eliminates parallel-edit drift risk.

Effort: M (1-2 hours; touches downstream type inference but the public surface is preserved).

### 2. Massive Duplication: 24 near-identical `rmp_serde::from_slice` error blocks in codec decoder
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/cypher_value_codec.rs:96-307`

Every `decode` arm repeats:
```
rmp_serde::from_slice(payload).map_err(|e| UniError::Storage {
    message: format!("failed to decode <type>: {}", e),
    source: None,
})?
```
The same pattern recurs ~14 times in `decode`, plus ~12 mirror `expect("X encode failed")` calls in `encode_to_buf` (lines 423-573).

Suggestion: Introduce one helper `fn unpack<T: DeserializeOwned>(payload: &[u8], what: &str) -> Result<T, UniError>`. Each arm becomes `unpack::<bool>(payload, "bool")?`. Similarly factor a `push_msgpack<T: Serialize>(buf, tag, value, label)` helper for `encode_to_buf`. Cuts ~120 lines and eliminates copy-paste typo risk in the `"failed to decode X"` strings.

Effort: S (30-45 min).

### 3. Duplication: `add_label` vs `add_label_with_desc`, `add_edge_type` vs `add_edge_type_with_desc`
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/core/schema.rs:1040-1162, 1178-1252`

`add_label` (1040-1065) and `add_label_with_desc` (1067-1092) are identical except for the `description` field. Same for `add_edge_type` / `add_edge_type_with_desc` and `add_property` / `add_property_with_desc`. ~120 lines duplicated.

Suggestion: Keep only the `_with_desc` variant; have the short form call it with `None`. Or, make `description: Option<String>` a parameter on a single method. Note: `add_edge_type_with_desc` at line 1146 uses `MAX_SCHEMA_TYPE_ID` while `add_edge_type` at 1110 uses `VIRTUAL_EDGE_TYPE_ID_START` — this looks like an inconsistency bug worth investigating before consolidating.

Effort: S (consolidation is mechanical; the inconsistency review is the real work).

### 4. Repeated `acquire_write` boilerplate prelude in `SchemaManager`
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/core/schema.rs:1040-1482`

Roughly 15 methods open with:
```
let mut guard = acquire_write(&self.schema, "schema")?;
let schema = Arc::make_mut(&mut *guard);
```
This is verbose and easy to forget. Two methods (`get_index` 1351, `schema()` 887, `replace_schema` 956, `get_or_assign_edge_type_id` 1165) use `.expect("Schema lock poisoned")` instead of `acquire_*`, creating inconsistency.

Suggestion: Add `fn with_schema_mut<R>(&self, f: impl FnOnce(&mut Schema) -> R) -> Result<R>` and `fn with_schema<R>(&self, f: impl FnOnce(&Schema) -> R) -> Result<R>` helpers on `SchemaManager`. Migrate all mutating methods to the helper, ensuring consistent error handling. ~30 lines net reduction plus uniform poison handling.

Effort: M.

### 5. Dead/superseded API: `add_label` next-id logic duplicated three times
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/core/schema.rs:1013-1038, 1047, 1074, 1106, 1144`

`next_label_id()` and `next_type_id()` exist as standalone methods (1013-1038) but each `add_*` method recomputes `schema.labels.values().map(|l| l.id).max().unwrap_or(0) + 1` inline. The standalone methods take a read lock; the inline versions reuse the write lock. Net result: two divergent next-id implementations.

Suggestion: Either remove the public `next_label_id`/`next_type_id` (they leak race-prone ids and there's no evidence callers want them — confirm via cross-crate grep), or have the `add_*` paths call a `next_*_id_locked(&schema)` private helper that both reuse.

Effort: S.

### 6. Two Hash-by-discriminant blocks with the same shape (TemporalValue, Value)
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/value.rs:72-113, 736-761`

Both impls open with `std::mem::discriminant(self).hash(state);` then enumerate every variant. Each manual `Hash` impl has high drift risk vs `Clone`, `PartialEq` (which are derived) when variants are added.

Suggestion: Consider `#[derive(Hash)]` where possible — the only obstruction is `f64`/`f32` and `HashMap` interior; introduce a small `OrderedFloat`/sorted-keys newtype wrapper or a `hash_helpers` module so the variants delegate to a uniform path. Not strictly a win at small scale, but the `Vector` block at lines 752-757 manually hashes a `Vec<f32>` while `Float` at 744 hashes a single `f64.to_bits()` — both could share one routine.

Effort: M-L (cross-cutting; consider only if you keep adding `Value` variants).

### 7. Complex / repetitive `Display` for `TemporalValue::Duration`
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/value.rs:447-495`

48 lines of conditional formatting with `nanos_sign` sprinkled through. The `if nanos_sign < 0 && (secs != 0 || frac_nanos != 0)` branch (484) is hard to follow. Mixed sign handling and the empty-duration fallback at the bottom suggest extracting `format_duration_time(hours, mins, secs, frac_nanos, sign) -> String`.

Suggestion: Extract two helpers: `format_duration_date_part` and `format_duration_time_part`. Each returns a `String`; the main `fmt` impl concatenates. Lowers cognitive load and lets you unit-test the corners directly.

Effort: S.

### 8. `validate_path` does subtle double canonicalization
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/config.rs:349-407`

The function (1) canonicalizes input or parent, (2) clones every allowed path and canonicalizes each one *on every call*. For server-mode validation against N allowed paths the canonicalization runs every time. Also the `for allowed in &self.allowed_paths` loop silently swallows canonicalize errors with `unwrap_or_else(|_| allowed.clone())`, which is a security smell (CWE-22 mitigation expected per the doc comment) — a canonicalize failure on `/var/lib/uni/data` falls back to the raw path, allowing `/var/lib/uni/data/../../etc/passwd` if the original allowed path was non-canonical.

Suggestion: Canonicalize `allowed_paths` *once* at construction in `FileSandboxConfig::sandboxed`/`default_for_mode`, store canonical-only paths, and fail fast at config time if canonicalization fails. Then `validate_path` shrinks to ~15 lines.

Effort: M (touches the public constructor contract).

### 9. `ServerConfig::security_warning` is a nested-if cascade where match would be clearer
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/config.rs:250-269`

Three `else if` branches keyed off `(has_wildcard, has_api_key)` — a 2-bit state space. A `match (has_wildcard, self.api_key.is_some())` would be more obviously exhaustive.

Effort: XS (5 min).

### 10. `merge_atop` is O(n) but spelled out three times
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/core/fork.rs:224-257`

Three near-identical `BTreeMap::new()` + double for-loop blocks for labels, edge_types, and properties (the only difference is the key tuple).

Suggestion: Extract `fn merge_seq<K: Ord + Clone, V: Clone>(base: &[(K, V)], top: &[(K, V)]) -> Vec<(K, V)>` (top wins). Then `merge_atop` becomes 4 lines.

Effort: S.

### 11. Unused / dubious: `next_label_id` / `next_type_id` exposed publicly
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/core/schema.rs:1013-1038`

These methods return an id *without* reserving it, which is racy under concurrent `add_label`/`add_edge_type`. If they have no in-tree callers (worth a grep), they should be removed; if they do, document the race or make them `pub(crate)`.

Effort: XS (grep + delete).

### 12. `SimpleGraph::vertex` reimplements `contains_vertex`
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/graph/simple_graph.rs:195-202`

`vertex(vid)` returns `Some(vid)` if present else `None` — an identity-preserving membership check. Same as `contains_vertex` plus identity. The `Option<Vid>` return adds no information (caller already knows `vid`). Likely a vestigial gryf-API holdover.

Suggestion: Remove `vertex` (or rename to `vertex_if_present` and audit callers). If retained, simplify to `self.vertices.contains_key(&vid).then_some(vid)`.

Effort: XS.

### 13. `LabelSnapshot` and `EdgeSnapshot` are byte-identical structs
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/core/snapshot.rs:22-34`

Both have `version: u32, count: u64, lance_version: u64` and identical derives. The distinct types add no type-safety because they appear in separate hashmap values (`vertices: HashMap<String, LabelSnapshot>` vs `edges: HashMap<String, EdgeSnapshot>`); a single `EntitySnapshot` would suffice.

Effort: XS.

### 14. `extract_local_id` / `is_schemaless_edge_type` could be `impl` methods on a newtype
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/core/edge_type.rs:9, 29-53`

`EdgeTypeId = u32` plus six free functions. A newtype with inherent methods would prevent accidental raw-u32 arithmetic on edge type ids (the `make_schemaless_id` debug_assert at line 42 hints at the bug class). Lower priority unless audits show raw-u32 leaks.

Effort: M (touches consumers across crates).

### 15. Inconsistent error-handling pattern: `acquire_*` vs `.expect`
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/core/schema.rs:887, 956, 1165, 1351`

Within the same `SchemaManager`, some methods propagate poison via `acquire_write(...)?` while these four `.expect("Schema lock poisoned...")`. Pick one policy. The `sync.rs` helpers exist precisely to avoid panics — using `.expect` here defeats them.

Effort: S.

### 16. `decode_int`/`decode_float`/`decode_bool`/`decode_string` are four parallel functions
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/cypher_value_codec.rs:329-358`

Identical 5-line body modulo tag and return type:
```
if bytes.first().copied() != Some(TAG_X) { return None; }
rmp_serde::from_slice(&bytes[1..]).ok()
```
Same shape for the four `encode_*` functions at 365-394.

Suggestion: Generic `pub fn decode_typed<T: DeserializeOwned>(bytes: &[u8], expected_tag: u8) -> Option<T>` plus keep the four wrappers as thin one-liners for ergonomics. Same for encode.

Effort: XS.

### 17. `Properties` type alias is defined but the codebase mostly uses `HashMap<String, Value>` directly
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/lib.rs:38`

The alias exists but `Node.properties` and `Edge.properties` (value.rs:786, 837) are spelled `HashMap<String, Value>`. Either delete the alias as dead, or use it consistently across the crate.

Effort: XS.

### 18. `unival!` macro could be smaller
File: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-common/src/value.rs:1492-1525`

Five separate arms for `null`/`true`/`false`/array/map — fine. But the `expr` fallback then forces consumers to write `unival!(42_i64)` (test line 1615) because integer literals default to `i32` and there's no `From<i32>`-vs-`From<i64>` disambiguation. Add `From<i32> for Value` (line 1329 already does this, good) plus consider an explicit numeric arm so `unival!(42)` "just works" — improves ergonomics for the most common case.

Effort: XS.

## Summary Table

| # | Severity | Effort | Lines saved (est.) |
|---|---|---|---|
| 1 Vid/Eid dedup | High | M | ~95 |
| 2 codec decode helper | High | S | ~120 |
| 3 _with_desc duplicates | High | S | ~120 |
| 4 SchemaManager with_schema helpers | Med | M | ~30 |
| 5 next_id dedup | Med | S | ~10 |
| 6 Hash impls | Low | M | — |
| 7 Duration Display extract | Med | S | — |
| 8 validate_path canon-at-construct | High (security) | M | — |
| 9 ServerConfig::security_warning match | Low | XS | — |
| 10 merge_atop merge_seq | Med | S | ~25 |
| 11 next_*_id audit | Low | XS | — |
| 12 SimpleGraph::vertex | Low | XS | — |
| 13 LabelSnapshot/EdgeSnapshot unify | Low | XS | ~12 |
| 14 EdgeTypeId newtype | Low | M | — |
| 15 expect vs acquire | Med | S | — |
| 16 decode_typed generic | Low | XS | ~20 |
| 17 Properties alias usage | XS | XS | — |
| 18 unival! ergonomics | XS | XS | — |

Cross-cutting themes: (a) parallel-implementation duplication around `Vid`/`Eid` and codec arms is the largest single win, (b) `_with_desc` doubling in `SchemaManager` adds ~120 dead lines and at least one latent inconsistency bug (item 3, line 1146 vs 1110), (c) the `validate_path` canonicalization fallback is a genuine security concern worth elevating beyond pure code-style.
