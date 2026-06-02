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

### Extism / WASM host-fn cutover — CLOSED (binds the same traits, full attenuation)

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

**Extism host fns — DONE** (Phase C): `ExtismLoader::with_{kms,secret_store,http}`
plus `register_default_host_svc` now expose `uni_kms_{sign,verify}`,
`uni_secret_acquire`, and `uni_http_{get,post}`, binding the shared
`uni_plugin::{KmsProvider, HttpEgress}` traits and `SecretStore` with full
call-time attenuation (key-id / secret-id / URL matched against the granted
allow-list) and host-trace-context (`current_trace_context().to_traceparent()`)
injection into outbound HTTP. The concrete `extism::Function`s are built **per
load** (`ExtismLoader::runtime_fns_for_load`) with that load's effective
`CapabilitySet` + service `Arc`s baked into `UserData`; the JSON wire envelope is
auto-marshaled by `extism::host_fn!` (no manual `CurrentPlugin` memory code). The
dispatch/attenuation logic lives in unit-tested `do_*` fns
(`src/host_svc/{kms,secret,net}.rs`); link-time gating is covered in
`tests/host_svc.rs`. End-to-end *guest invocation* is still unexercised (no guest
fixture imports these host fns — the same fixture gap as the WASM item below).

**WASM host-net + host-trace-context — DONE** (Phase D + E): `world.wit` declares
the capability-gated `interface host-net` (`http-get`/`http-post` →
`result<http-response, fn-error>`) and the always-available `host-trace-context`
(`get-traceparent`). `linker.rs` adds `host-trace-context` unconditionally and
`host-net` only when `Capability::Network` is in the effective set — a guest
importing `uni:plugin/host-net@0.1.0` without the grant fails at `instantiate`
(structural gating). The `func_wrap` bodies read egress + caps from `HostState`,
enforce the URL allow-list, clamp timeout/size to the granted ceiling, and inject
`current_trace_context().to_traceparent()` into the outbound call (Phase E for the
wasm path; the Rhai/Extism paths already inject). The two-pass loader bootstraps
with the host's *offered* grants so a `host-net`-importing guest's manifest can be
read (execution still uses `declared ∩ grants`). A real guest fixture
(`examples/example-wasm-net`, built by `scripts/build-wasm-fixtures.sh`) imports
and *calls* host-net; `tests/example_wasm_net_e2e.rs` covers granted-round-trip,
unconfigured-egress loud failure, and ungranted link-time failure end to end.

**The cutover is complete** (C0 + C + D + E). Both guest loaders now bind the
shared host-service traits with full capability attenuation and trace
propagation. (Note: the Extism path still has no guest fixture that *imports* its
host fns, so its end-to-end guest invocation is proven only at the linker/gate +
unit-dispatch level — the wasm path above has the full e2e.)

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

### M11 observability — guest-boundary propagation — CLOSED

`current_trace_context()` (`crates/uni-plugin/src/observability.rs`) performs real
OTel extraction behind the default-off `otel` feature (enabled by
`uni-plugin-host`): it reads the `SpanContext` bridged onto the current `tracing`
span and `to_traceparent()` renders the W3C header. The host's
`current_traceparent()` delegates to it, so host outbound HTTP and the plugin ABI
share one implementation.

Guest-boundary injection has now landed across **all** loaders: every `uni.http.*`
/ `host-net` host-fn body computes `current_trace_context().to_traceparent()` and
hands it to the shared `HttpEgress` (which sets the `traceparent` request header),
so an outbound call from an isolated Rhai / Extism / wasm guest continues the
host's trace. With the `otel` feature off the value is `None` (no fabricated ids).
The wasm `host-trace-context.get-traceparent` import additionally lets a guest SDK
read the same value to start a child span.

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
