# Systematic fix/enhancement plan: Python plugin ABI gaps (issue #150 and beyond)

**Status:** proposal · **Date:** 2026-07-16 · **Trigger:** GitHub issue #150
(guest GraphCompute algorithm plugin uninvokable from Python)

## 1. Summary

Issue #150 (a guest-authored GraphCompute algorithm plugin cannot be loaded or
invoked from the `uni-db` Python binding) is **not an isolated bug**. It is the
first-reported symptom of three overlapping, structural gaps:

1. **Grant-string drift.** The three hand-written grant-string parsers accept
   only **8 of 41** `Capability` variants. 33 are ungrantable from Python or the
   CLI. There is no single source of truth and no exhaustiveness test, and the
   enum is `#[non_exhaustive]`, so every capability added since the original 8
   became silently ungrantable. `Algorithm`/`GraphCompute` are just two of them.
2. **The guest-algorithm feature was never wired or tested through Python.**
   Zero Python tests load an `algorithms:` plugin, grant Algorithm/GraphCompute,
   or `CALL <plugin>.<algo>`. The whole guest-GraphCompute surface is
   Rust-implemented, Rust-tested, unvalidated from the binding people ship on.
3. **A universal observability gap.** `algorithms_registered` is absent from all
   four loaders' `LoadOutcome` (even the Rust path), so a registered algorithm is
   never surfaced in the load outcome.

Underneath all three: **the Python plugin test surface is a ScalarFn-only smoke
layer** — nothing past `ScalarFn` (no aggregates, procedures, algorithms, host
functions) and no capability-*enforcement* assertions are exercised from Python.

This plan fixes the immediate issue, installs the mechanism that prevents the
drift class from recurring, closes the observability gap, and adds the Python
plugin conformance suite that would have caught #150 at landing.

## 2. Evidence (file:line)

- Grant parsers (all 3, identical 8-name PascalCase matches, no shared source):
  `bindings/uni-db/src/builders.rs:29` (lenient), `:86` (strict, hard-errors on
  unknown at `:122`); `crates/uni-cli/src/main.rs:307` (warn+ignore at `:344`).
- `Capability` enum (41 variants, `#[non_exhaustive]`, `#[serde(tag="kind",
  rename_all="kebab-case")]`): `crates/uni-plugin/src/capability.rs:31`. No
  `Display`/`name()`/`as_str()` anywhere; only serde kebab tags.
- Rhai loader registers algorithms gated on `effective.contains(&Capability::
  Algorithm)`: `crates/uni-plugin-rhai/src/loader.rs:262-272`; auto-declares
  `Algorithm+GraphCompute+HostQuery` for any `algorithms:` manifest:
  `:370-383`. Registrar requires `Capability::Algorithm`: `registrar.rs:372`.
- CALL resolution of guest algorithms: `crates/uni-query/src/query/executor/
  procedure.rs:235-252` (`resolve_user_algorithm`).
- Working Rust reference test (grants the triple, loads, CALLs, asserts parity
  vs native `uni.algo.gcpagerank` within 1e-9):
  `crates/uni/tests/common/loaders/rhai_graph_compute.rs`.
- `denied_capabilities` computed with exact `contains` (not effective/variant):
  `capability.rs:224` vs the attenuating `intersect` at `:251` — so an
  attenuated-but-granted `HostQuery` is falsely reported denied.
- Python plugin tests (ScalarFn-only): `bindings/uni-db/tests/test_{python,
  async_python,rhai,wasm}_plugin.py`. All `procedures_registered` assertions
  check `== []`; no aggregate/algorithm/host-fn coverage.

## 3. Capability taxonomy (design foundation)

All 41 `Capability` variants classified by how they can be granted:

| Bucket | Count | Variants | Grant via string? |
|---|---|---|---|
| **A. Unit registration-gate** | 22 | `ScalarFn, AggregateFn, WindowFn, Procedure, ProcedureWrites, ProcedureSchema, ProcedureDbms, LocyAggregate, LocyPredicate, LocyGenerator, Operator, Index, Storage, Algorithm, GraphCompute, Crdt, Hook, Trigger, Type, Collation, PluginStorage, PluginDeclare` | **Yes** — bare name |
| **B. Allow-list payload** | 7 | `Network, Filesystem, HostQuery, Kms, Secret, Config, Lock` | **Yes** — bare name → documented default payload |
| **C. Resource quota (numeric)** | 8 | `MemoryBytes, FuelPerCall, WallClockMillisPerCall, ConcurrentInstances, TotalMemoryBytes, MaxResultRows, GraphComputeWork, GraphComputeArenaBytes` | **No** — needs a value; enters only via manifest `capabilities:` |
| **D. Internal / first-party** | 4 | `Auth, Authz, Cdc, Catalog` | **No** — not grantable to arbitrary guests |

Notes that shape the design:
- `BackgroundJob{max_concurrent}` is a registration-gate that carries a quota —
  gate in A, number in C.
- `Lock` carries a `LockGranularity` enum (not globs) and is *not* in the
  `attenuate_to_host` match — host can't narrow it today (minor gap).
- **Security finding (separate from #150):** quotas (C) pass through `intersect`
  **unclamped** and are *guest-authoritative* — a guest manifest can self-declare
  `GraphComputeWork(huge)` and it is honored with no host ceiling
  (`capability.rs:333-334`; consumed at `algorithms/bridge.rs:229-242`). Flag for
  hardening; out of scope for the #150 fix but must be tracked.
- `PascalCase` vs `kebab-case`: the parsers key on PascalCase (`"ScalarFn"`),
  serde uses kebab (`"scalar-fn"`). Existing users/tests pass PascalCase — we
  must keep accepting it. Recommendation: canonical grant name = PascalCase
  variant name; also accept kebab-case as an alias.

## 4. Design decisions (with recommendations)

- **D1 — Single source of truth, in `uni-plugin`.** Because the enum is
  `#[non_exhaustive]`, only code *in the defining crate* can match it
  exhaustively. Add to `capability.rs`:
  - `Capability::grant_name(&self) -> GrantClass` — an **exhaustive** match
    classifying every variant as `Grantable(&'static str)` (A+B),
    `NeedsValue(&'static str)` (C), or `Internal(&'static str)` (D).
  - `Capability::parse_grant(s: &str) -> Result<Capability, GrantError>` —
    accepts A (unit) and B (with default payload); returns a *descriptive* error
    for C ("`<name>` is a resource quota; declare it with a value in the plugin
    manifest `capabilities:` list") and D ("`<name>` is not grantable to guest
    plugins"); for unknown, lists the full supported set. Accept PascalCase and
    kebab-case.
  All three downstream parsers become thin wrappers over `parse_grant`.
- **D2 — Exhaustiveness test in `uni-plugin`.** A unit test matches every
  variant and asserts it maps to exactly one `GrantClass`, with the C/D lists
  written out explicitly. A new `#[non_exhaustive]` variant → **compile error**
  in `grant_name` until triaged. This is the anti-drift guarantee.
- **D3 — Default payloads for bucket B** are defined once in `parse_grant`
  (`HostQuery{read_only:true, scopes:["**"]}`, `Network{allow:["**"]}`, etc.),
  matching today's behavior, so existing callers are unchanged.
- **D4 — Do not put quotas in the string API.** Keep them manifest-only;
  `parse_grant` rejects them with the guidance above. (Revisit a `name=value`
  grant syntax later if demanded.)
- **D5 — Guard bucket D.** `parse_grant` refuses Auth/Authz/Cdc/Catalog by
  default. If first-party host code ever needs them, it constructs the
  `Capability` directly in Rust (as it does today) — never via the guest grant
  list.
- **D6 — `algorithms_registered` is a real channel**, added to all four
  `LoadOutcome`s and surfaced in Python, matching the existing
  `scalars_registered`/etc. pattern.
- **D7 — Fix `denied_capabilities` reporting** to diff against the *effective*
  set (variant-aware), so attenuated-but-granted caps aren't falsely listed.

## 5. Workstreams & touch-points

### WS-1 — Grant source-of-truth + anti-drift (fixes #150 at the root)
- `crates/uni-plugin/src/capability.rs`: add `GrantClass`, `grant_name`,
  `parse_grant`, `GrantError`, optional `Display`; add the exhaustiveness test.
- `bindings/uni-db/src/builders.rs:29,86`: replace both matches with
  `Capability::parse_grant`; keep strict = error-on-reject, lenient =
  skip-on-reject; update the "supported:" message to enumerate from the source.
- `crates/uni-cli/src/main.rs:307`: same replacement.
- Result: `grants=["Algorithm","GraphCompute","HostQuery"]` works end-to-end
  (verified via the attenuation analysis — `HostQuery{scopes:["**"]}` correctly
  covers the declared `scopes:[]`).

### WS-2 — `algorithms_registered` channel (observability)
- Structs + literals + manual Debug impls (add field, collect qname before the
  by-move registrar call):
  - rhai `loader.rs:44` struct / `:274` literal / loop `:262-272`
  - pyo3 `loader.rs:45` / `:249` / `register_algorithms :491`
  - wasm `loader.rs:726` / `:666` / `:1092-1117` / Debug `:745-757`
  - extism `loader.rs:704` / `:569` / `:542-565` / Debug `:725-736`
- Python surfacing: `builders.rs:135 load_outcome_to_pydict` (pyo3, add once);
  `sync_api.rs:597 wasm_outcome_to_pydict` (add param + 4 call sites: sync
  `:914,:947`, async `async_api.rs:223,:258`); inline rhai dicts
  `sync_api.rs:870-882` and `async_api.rs:302-306`.

### WS-3 — `denied_capabilities` correctness (WS optional, ships with WS-2)
- Compute denied relative to the effective set in each loader's `intersect_caps`
  (or add a `CapabilitySet::denied_against` helper in `uni-plugin`), variant-aware
  so an attenuated grant isn't reported denied.

### WS-4 — Python plugin conformance suite (closes the real hole)
New `bindings/uni-db/tests/test_plugin_conformance.py`, parametrized over
**loader × extension-kind**, plus an enforcement axis:

| Extension | pyo3 | rhai | wasm | extism | Enforcement assertion |
|---|---|---|---|---|---|
| ScalarFn | ✓ (exists) | ✓ | ✓ (geo fixture) | ✓ (geo) | load w/o ScalarFn denies register |
| AggregateFn | ✓ (`@aggregate_fn` inline) | ✓ | — | — | — |
| Procedure | ✓ (`@procedure` inline) | ✓ | — | — | grant `Procedure` vs denied |
| **Algorithm/GraphCompute** | (no py decorator) | ✓ (port PPR script) | ✓ (graph fixture) | ✓ (graph fixture) | **no grant → CALL ProcedureNotFound; granted → parity vs native `uni.algo.gcpagerank`** |
| Host-fn (net) | — | — | ✓ (net fixture) | ✓ (net fixture) | denied Network/HostQuery blocks call |

- Port `rhai_graph_compute.rs` (full PPR script + graph + parity assertion) to
  Python — the direct #150 regression guard.
- Add a `CALL uni.algo.gcpagerank` test to confirm the issue's premise (built-in
  GC algos callable from Python) — currently unguarded.
- Fixtures: reuse `scripts/build-wasm-fixtures.sh` outputs
  (`example-{wasm,extism}-{geo,net,graph}`); copy the skip-if-missing guard from
  `test_wasm_plugin.py:21-31,47` (fixtures are gitignored build artifacts).
- Assert the new `algorithms_registered` channel is populated.
- CI: the suite runs under the existing `python-tests` job; fixture-dependent
  cases skip cleanly when the wasm artifacts are absent, matching current
  behavior.

### WS-5 — Docs
- `website/docs`: document the grant-string set, guest-grantable vs internal
  capability policy, and the manifest path for quotas.
- Reference the exhaustiveness test as the contract for adding a capability.

## 6. Sequencing & effort

| Phase | Contents | Rationale | Rough size |
|---|---|---|---|
| **P0** | WS-1 (grant arms via source-of-truth) + the rhai PPR Python regression test | Unblocks #150 with a guard, minimal surface | S |
| **P1** | WS-1 exhaustiveness test + replace all 3 parsers | The "why does this keep happening" fix | S–M |
| **P2** | WS-2 (+ WS-3) algorithms channel + denied fix | Observability/correctness parity | M |
| **P3** | WS-4 full conformance suite | Converts "implemented in Rust" → "works through the binding" | M–L |
| **P4** | WS-5 docs | — | S |

P0+P1 together are the systematic core and are still small. P3 is the largest
but highest-leverage — it is what prevents the *next* silently-broken extension.

## 7. Risks & open questions

- **R1 — PascalCase vs kebab-case canonical form.** Recommendation: PascalCase
  canonical, kebab accepted as alias (back-compat). Confirm.
- **R2 — Bucket-D exposure.** Recommendation: never guest-grantable via string.
  Confirm no user workflow needs Auth/Authz/Cdc/Catalog from a guest.
- **R3 — Quota self-escalation (security).** Guest manifests can raise their own
  GraphCompute/fuel/memory budgets unclamped. Separate hardening item; track
  independently of #150. Needs a host-ceiling clamp in `attenuate_to_host` for
  bucket C, or an explicit "guest budgets are advisory only" decision.
- **R4 — WASM/extism cap derivation non-uniformity.** rhai/pyo3 auto-declare the
  algorithm cap triple from an `algorithms:` section; wasm/extism require the
  guest to declare caps explicitly in the manifest. The conformance suite must
  grant/declare accordingly per loader. Consider unifying derivation later.
- **R5 — Fixture availability in CI.** Conformance cases that need wasm artifacts
  skip when absent; ensure `build-wasm-fixtures.sh` runs before `python-tests`
  if we want them non-skipped in CI (today they'd skip).

## 8. Definition of done

- `grants=["Algorithm","GraphCompute","HostQuery"]` loads and `CALL
  <plugin>.<algo>` resolves from Python, asserted by a test porting
  `rhai_graph_compute.rs`.
- Adding a new `Capability` variant fails the `uni-plugin` exhaustiveness test
  until it is classified — no silent drift.
- One `parse_grant` source feeds Python-strict, Python-lenient, and CLI.
- `algorithms_registered` appears in every loader's outcome and the Python dict.
- Python conformance suite exercises each extension kind on each applicable
  loader and asserts capability enforcement (granted vs denied changes behavior).
