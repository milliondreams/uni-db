# Plugins API Reference

This page is the precise API surface for loading plugins into a Uni instance.
For the conceptual model see [Concepts](concepts.md); for the permission model
see [Trust & Capabilities](trust-and-capabilities.md); for per-runtime loading
guides see [Loaders](loaders/index.md).

The Python load methods are instance-scoped: a plugin loaded through `Uni` /
`AsyncUni` is visible to every session on that instance until the instance is
dropped.

## Python API

Both the sync `Uni` and async `AsyncUni` expose the same two load methods. They
are gated behind the cargo features `wasm-plugins` (for `load_wasm_component`)
and `extism-plugins` (for `load_wasm_extism`). The default wheel bundles
wasmtime and ships both; some variant wheels omit one or both — if a method is
absent at runtime, the wheel was built without the corresponding feature.

`grants` is an optional list of capability/grant variant names (see
[Capability names](#capability-names)). When omitted it defaults to the surface
grants `ScalarFn`, `AggregateFn`, `Procedure`. The same list drives both the
**surface-registration gate** (which kinds the plugin may register) and the
**host-fn grant set** (which capability-gated host imports are linked in).

=== "Sync (Uni)"

    ```python
    def load_wasm_component(
        self,
        wasm_bytes: bytes,
        grants: list[str] | None = None,
    ) -> dict: ...

    def load_wasm_extism(
        self,
        wasm_bytes: bytes,
        grants: list[str] | None = None,
    ) -> dict: ...
    ```

    ```python
    from uni_db import Uni

    db = Uni.open("graph.uni")
    outcome = db.load_wasm_component(
        wasm_bytes,
        grants=["ScalarFn", "AggregateFn"],
    )
    print(outcome["plugin_id"], outcome["scalars_registered"])
    ```

=== "Async (AsyncUni)"

    ```python
    async def load_wasm_component(
        self,
        wasm_bytes: bytes,
        grants: list[str] | None = None,
    ) -> dict: ...

    async def load_wasm_extism(
        self,
        wasm_bytes: bytes,
        grants: list[str] | None = None,
    ) -> dict: ...
    ```

    The async variants return an awaitable.

    ```python
    from uni_db import AsyncUni

    async with AsyncUni.open("graph.uni") as db:
        outcome = await db.load_wasm_component(
            wasm_bytes,
            grants=["ScalarFn"],
        )
    ```

### Returned dict

Both methods return a `dict` with the following keys:

| Key | Type | Meaning |
| --- | --- | --- |
| `plugin_id` | `str` | Reverse-DNS plugin id read from the manifest. |
| `version` | `str` | Plugin version from the manifest. |
| `scalars_registered` | `list[str]` | Qnames registered as Cypher scalar functions. |
| `aggregates_registered` | `list[str]` | Qnames registered as Cypher aggregate functions. |
| `procedures_registered` | `list[str]` | Qnames registered as Cypher procedures. |
| `effective_capabilities` | `list[str]` | Capabilities granted (manifest declared ∩ host grants). |
| `denied_capabilities` | `list[str]` | Capabilities the plugin declared but the host did not grant. |

### Rhai and Python (PyO3) loads

The Rhai and PyO3 loaders are also Python-callable, but on different objects and
with different signatures from the two WASM methods above:

| Method | On | Async? | Signature | Scope |
| --- | --- | --- | --- | --- |
| `load_rhai_plugin` | `Uni` / `AsyncUni` | both | `(script: str, grants: list[str] \| None = None) -> dict` | instance |
| `load_python_plugin` | `Session` / `AsyncSession` (from `db.session()`) | both | `(module_src: str, module_name: str, grants: list[str] \| None = None) -> dict` | session |

`Session` also exposes the decorator surface — `@session.scalar_fn(...)` /
`@session.aggregate_fn(...)` / `@session.procedure(...)` plus
`session.finalize_plugin(plugin_id, version=None, grants=None) -> dict` — as an
alternative to loading from a source string. All of these return a dict with
`plugin_id`, `version`, `scalars_registered`, `aggregates_registered`,
`procedures_registered`, and `denied_capabilities` (note: no
`effective_capabilities` key, unlike the two WASM methods). See
[Rhai](loaders/rhai.md) and [PyO3](loaders/pyo3.md) for usage.

## Rust host API

The Rust host owns the loaders explicitly: you construct a `WasmLoader` /
`ExtismLoader`, pass the plugin bytes plus the grant list, and receive a
`LoadOutcome`. `host_grants` gates which host fns are linked into the plugin's
import table; `registrar_caps` gates which registration surfaces the plugin may
use.

```rust
#[cfg(feature = "wasm-plugins")]
pub fn load_wasm_component(
    &self,
    loader: &uni_plugin_wasm::WasmLoader,
    bytes: &[u8],
    host_grants: &[String],
    registrar_caps: &uni_plugin::CapabilitySet,
) -> Result<uni_plugin_wasm::loader::LoadOutcome>;

#[cfg(feature = "extism-plugins")]
pub fn load_wasm_extism(
    &self,
    loader: &uni_plugin_extism::ExtismLoader,
    bytes: &[u8],
    host_grants: &[String],
    registrar_caps: &uni_plugin::CapabilitySet,
) -> Result<uni_plugin_extism::loader::LoadOutcome>;
```

For typed, in-process plugins use `add_plugin`:

```rust
pub fn add_plugin<P: uni_plugin::Plugin>(&self, plugin: P) -> Result<()>;
```

`add_plugin` is where the host **signature trust policy is enforced today**: the
plugin's manifest is checked against the configured `PluginTrustConfig` before
any registration runs. The default policy (`SignaturePolicy::Disabled`) accepts
everything; `RequireSigned` rejects an unsigned manifest or one signed by an
untrusted key. Compile-time built-in plugins are implicitly trusted.

The trust policy is set at build time:

```rust
pub fn plugin_trust(mut self, cfg: plugin_trust::PluginTrustConfig) -> Self;
```

```rust
let db = Uni::open("graph.uni")
    .plugin_trust(PluginTrustConfig::new(SignaturePolicy::RequireSigned, trust_root))
    .build()
    .await?;
```

See [Trust & Capabilities](trust-and-capabilities.md) for the full trust model.

### LoadOutcome

`uni_plugin_wasm::loader::LoadOutcome` and
`uni_plugin_extism::loader::LoadOutcome` share the same observable shape (the
two differ only in the concrete instance-pool type, which is held internally to
keep adapters alive):

| Field | Type | Meaning |
| --- | --- | --- |
| `plugin_id` | `String` | Reverse-DNS plugin id from the manifest. |
| `version` | `String` | Plugin version from the manifest. |
| `effective_capabilities` | `Vec<String>` | Capabilities granted (declared ∩ host). |
| `denied_capabilities` | `Vec<String>` | Capabilities declared but not granted. |
| `scalars_registered` | `Vec<String>` | Qnames registered as scalar fns. |
| `aggregates_registered` | `Vec<String>` | Qnames registered as aggregate fns. |
| `procedures_registered` | `Vec<String>` | Qnames registered as procedures. |
| `pool` | `Arc<…InstancePool>` | Instance pool shared by every adapter bound to this plugin; keeps the pool alive while any adapter remains registered. |

The Python load methods project every field except `pool` into the returned
dict.

## Capability names

Grant strings (Python `grants`, Rust `host_grants`) and the entries in
`effective_capabilities` / `denied_capabilities` use the variant names of
`uni_plugin::Capability`. The authoritative set follows; see
[Trust & Capabilities](trust-and-capabilities.md) for how declared capabilities,
host grants, and the effective (intersected) set interact.

### Host-import surfaces

These gate capability-backed host functions. Most carry attenuation data in
Rust; as plain grant strings they request the variant.

| Name | Attenuation | Gates |
| --- | --- | --- |
| `Network` | `allow` URI glob patterns | HTTP / TCP egress. |
| `Filesystem` | `read` / `write` path globs | Filesystem read / write. |
| `HostQuery` | `read_only`, `scopes` | Cypher / Locy queries back into the host session. |
| `Kms` | `key_ids` | KMS sign / verify. |
| `Secret` | `ids` | Acquiring named secret handles. |
| `Lock` | `granularity` (`Nodes`/`Edges`/`Both`/`Global`) | Explicit node / edge lock primitives. **Reserved** — the capability is declared and intersected, but the host functions are not yet callable. |
| `Config` | `keys` patterns | Scoped configuration K/V access. |
| `PluginStorage` | — | Per-plugin scoped K/V store. **Reserved** — declared and intersected, host functions not yet callable. |

### Extension surfaces

These gate the registrar methods — which kinds of extension the plugin may
register.

| Name | Registers |
| --- | --- |
| `ScalarFn` | Cypher scalar functions. |
| `AggregateFn` | Cypher aggregate functions. |
| `WindowFn` | Cypher window functions. |
| `Procedure` | Cypher procedures (read-only). |
| `ProcedureWrites` | Procedures that may mutate the graph. |
| `ProcedureSchema` | Procedures that may issue DDL. |
| `ProcedureDbms` | Administrative procedures. |
| `LocyAggregate` | Locy aggregate functions. |
| `LocyPredicate` | Locy predicates (including neural). |
| `Operator` | Physical operators / optimizer rules. |
| `Index` | Index kinds. |
| `Storage` | Storage backends by URI scheme. |
| `Algorithm` | Graph algorithms. |
| `Crdt` | CRDT kinds. |
| `Hook` | Session / query lifecycle hooks. |
| `Trigger` | Fine-grained mutation triggers. |
| `BackgroundJob` | Background / scheduled jobs (carries `max_concurrent`). |
| `Type` | Logical (Arrow extension) types. |
| `Auth` | Authentication providers. |
| `Authz` | Authorization policies. |
| `Connector` | Wire / connector protocols. |
| `Collation` | Collations (sort orders). |
| `Cdc` | CDC output sinks. |
| `Catalog` | Catalogs / virtual schemas. |
| `PluginDeclare` | Authority to call `uni.plugin.declare*` meta-procedures. |

The Python load methods default to `ScalarFn`, `AggregateFn`, `Procedure` when
`grants` is omitted.

### Resource quotas

Quota variants carry a numeric bound.

| Name | Bound | Limits |
| --- | --- | --- |
| `MemoryBytes` | `u64` | Max wasm linear memory per instance. |
| `FuelPerCall` | `u64` | Max wasmtime fuel per call. |
| `WallClockMillisPerCall` | `u64` | Max wall-clock milliseconds per call. |
| `ConcurrentInstances` | `u32` | Max concurrent instances in the wasm pool. |
| `TotalMemoryBytes` | `u64` | Max total memory across all instances. |
| `MaxResultRows` | `u64` | Cap on rows yielded by a procedure. |

Today `FuelPerCall` and `MemoryBytes` are enforced (Rhai from the granted
capability; the WASM Component Model and Extism loaders from their manifest
fields). The remaining quota variants are recognized but not yet enforced.

## See also

- [Concepts](concepts.md) — the plugin model and lifecycle.
- [Trust & Capabilities](trust-and-capabilities.md) — the permission and trust model.
- [Loaders](loaders/index.md) — per-runtime loading guides.
- [Authoring](authoring.md) — writing your own plugin.
- Generated API docs: [Python API](../api/python/index.md), [Rust API](../api/rust/index.md).
