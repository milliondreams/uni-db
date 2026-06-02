# Known Gaps

Tracked stubs, placeholders, and unfinished features. These items look like
dead code at a glance — no callers, `_`-prefixed bindings, `#[allow(dead_code)]`,
or trivial bodies — but each one represents real unfinished functionality. They
are listed here so a future dead-code cleanup sweep does not silently erase the
gap. The fix is to **implement** (or explicitly decide to remove the feature),
not to delete the placeholder.

Originally surfaced during the 2026-05-27 workspace-wide code-simplifier review
(see `CODE_SIMPLIFIER_FEEDBACK.md` §3). Last reviewed against the code on
2026-06-01.

Format: one entry per gap, with the source anchor and the gap's nature.

---

## Notes

- This document is not a roadmap; it's a "do not silently delete" list.
- When a gap is closed, remove its entry here in the same commit that
  implements it.
- New stubs introduced after this review should land with a corresponding
  entry, not as an `_`-prefixed binding alone.
- Items judged *truly* dead (unreachable, no-op, never-constructed) are not
  listed here — those are safe to delete in a routine cleanup PR.

---

## Open gaps

### Extism / WASM host-fn cutover — remaining (binds the same traits, full attenuation)

The Rhai loader's `uni.{kms,secret,http,fs}.*` host fns are fully wired with
call-time (layer-3) attenuation — KMS key ids, secret ids, HTTP URLs, and now
filesystem read/write **paths** are matched against the granted `Capability`
allow-list (`Capability::{network,kms,secret,filesystem_read,filesystem_write}_allows`),
and an unconfigured host service fails loudly. KMS / HTTP bind the shared
`uni_plugin::{KmsProvider, HttpEgress}` traits (`reqwest::blocking` `BlockingHttpEgress`
on a dedicated OS thread to avoid a Tokio-runtime panic); secrets reuse
`uni_plugin::secrets::SecretStore`.

**Capability-model unification — DONE** (cutover phases C0 + E-seam,
CI-green): the WASM + Extism loaders now run on the rich
`uni_plugin::Capability`/`CapabilitySet` instead of name-only `Vec<String>`.
`ComponentManifest`/`ExtismPluginManifest` parse `uni_plugin::ManifestCapability`
(bare name | structured `{"kind":…,"allow":[…]}`); `prepare_parsed` intersects
declared ∩ granted with attenuation retained; the guest-grant API is
`&CapabilitySet` (breaking — `crates/uni::load_wasm_*` + the Python binding
updated, `list[str]` Python surface preserved via `build_capability_set`);
`HostFnSpec.required_capability` is `Option<Capability>`; `HostState` /
`PreparedComponent` carry the rich effective set + an `Option<Arc<dyn HttpEgress>>`;
`HttpEgress::{get,post}` take a `traceparent` (Rhai already injects it).

**Remaining (host-fn bodies — the feature layer on the C0 foundation):**

- **Extism** `uni.{kms,secret,http}`: add `ExtismLoader::with_{kms,secret_store,http}`
  handles; have `build_plugin_from_parts` construct **per-build** cap-aware
  `extism::Function`s (capturing `prepared.effective` + the service `Arc`s in
  `UserData`) when the capability variant is granted, with a JSON wire envelope +
  `CurrentPlugin` memory marshaling. Reference: the Rhai `host_fn_impls`.
- **WASM** `host-net` + `host-trace-context`: declare the WIT interfaces, add
  cap-gated `add_host_net` / `add_host_trace_context` to `linker.rs` (func_wrap
  reading `HostState.{http,effective}`), and add a guest fixture that *imports*
  host-net to exercise it end-to-end (the geo fixtures don't).
- **E**: the host-net bodies inject `current_trace_context().to_traceparent()`.
- Full design: `/home/rohit/.claude/plans/squishy-hatching-cupcake.md` Phases C–E.

### Conformance harness — WASM target — marker arm is intentional

The real bridge now exists: `WasmLoader::load_as_plugin`
(`crates/uni-plugin-wasm/src/loader.rs`) loads a component and presents it as a
`uni_plugin::Plugin` (synthesized manifest + `register` replay via the shared
`apply_registration`), and `crates/uni-plugin-wasm/tests/conformance_wasm.rs`
drives it through `run_against_wasm`, which runs the *same* probe suite as the
live-Rust target. The full-suite test soft-skips unless the
`example-wasm-geo` fixture is built (`./scripts/build-wasm-fixtures.sh`); the
load-failure path is covered unconditionally.

The `ConformanceTarget::WasmPath` arm of `run_against`
(`crates/uni-plugin-conformance/src/lib.rs`) **intentionally** stays a marker
pointing at `run_against_wasm`: `run_against` takes only an enum and, by
dep-graph design, the conformance crate cannot depend on wasmtime to construct a
loader itself. The test asserting the marker (`wasm_target_returns_runner_pointer`)
must not be "simplified" away.

### M11 observability — guest-boundary propagation (follow-up)

`current_trace_context()` (`crates/uni-plugin/src/observability.rs`) now performs
real OTel extraction behind the default-off `otel` feature (enabled by
`uni-plugin-host`): it reads the `SpanContext` bridged onto the current `tracing`
span and `to_traceparent()` renders the W3C header. The host's
`current_traceparent()` delegates to it, so host outbound HTTP and the plugin
ABI share one implementation.

Remaining follow-up — **injecting the traceparent into guest plugins** so an
isolated wasm/extism guest can continue the trace. This is blocked on the
host-net host-fn cutover that has not landed yet (the extism `host_fns.rs`
registry and the wasm `host-net` WIT interface are still scaffolding). When that
lands, the host-fn body injects `current_trace_context().to_traceparent()` into
guest HTTP, identical to `http_get_with_traceparent`.

### Sidecar IO consolidation (`CODE_SIMPLIFIER_FEEDBACK.md` §1.5) — CLOSED

All four copies of the atomic-JSON-sidecar pattern now share the
`uni-sidecar` crate's `SystemSidecar<T>` — a single atomic write-temp + fsync +
rename + **parent-dir fsync** implementation (the parent-dir fsync and the
scheduler's previously-missing temp-file fsync were latent durability bugs,
fixed in the consolidation). The three `uni-plugin-host` persisters
(`DeferralSidecar`, `CdcCheckpointSidecar`, `SystemLabelSchedulerPersistence`)
and `uni-plugin-custom`'s `JsonFilePersistence` all use it (the latter via
`SystemSidecar::at_path` to keep its exact `declared_plugins.json` location).
Caller-side locking and the scheduler's Cypher mirror compose around it. Nothing
left here.

---

## Recently closed (2026-06-01 review)

- **Phase 4 — Registry `Surface` migration** (was `CODE_SIMPLIFIER_FEEDBACK.md`
  §1.1) — **CLOSED.** The trait scaffolding at
  `crates/uni-plugin/src/surfaces/mod.rs` (now 1536 LOC) is live: the legacy
  `PendingRegistration` enum is gone and registration dispatches through
  `Box<dyn DynPendingRegistration>` (`PluginRegistry::apply_pending`,
  `registry.rs:851`) across all four family traits. `KeyedUniqueSurface::key_of`,
  append-family per-plugin removal, and the `SmolStr`-keyed `StorageBackend`
  (the previously-flagged `&'static str` inconsistency) are all in place;
  `PluginRecordSnapshot` stays `pub` for test compatibility. The one item left
  is unifying the Extism and wasm `build_pool` factories — intentionally kept
  structurally separate (`crates/uni-plugin/src/adapter_common/mod.rs:18`), to
  be revisited only if a third loader appears. That is a refactor opportunity,
  not dead-looking code, so it is no longer tracked here.
- The four items previously listed as "truly dead, safe to delete"
  (`BticError::SentinelExclusivity`, `AlgorithmConfig` limit fields, the
  `scheduler.rs` `mem::take` no-op, and the conformance `version.is_empty()`
  branch) have all been removed from the code.
