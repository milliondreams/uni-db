# PyO3 Plugins

The PyO3 loader runs **trusted, in-process Python** functions as uni-db plugins:
scalars, aggregates, and procedures authored in Python and called from the query
engine at native-ish speed. It is the only loader that can run a *vectorized*
Python scalar — one call per RecordBatch over zero-copy PyArrow arrays — but it
is also the only loader with **no sandbox**: the callable is a live Python object
in the host interpreter.

## Overview

!!! warning "PyO3 plugins are TRUSTED — there is no sandbox"
    A PyO3 plugin is a Python callable living in the host's embedded interpreter.
    It runs at **full host process privilege**: it can open files, make network
    calls, and import any module the host can. Capabilities (`ScalarFn`,
    `AggregateFn`, `Procedure`) are *declared metadata* enforced at the registry
    gate — they gate *what kind of plugin gets registered*, not what the Python
    code may do once running. This is unlike the
    [WASM Component Model](wasm-components.md), Extism, and [Rhai](rhai.md)
    loaders, which execute guest code inside a structural sandbox. Only load
    Python you trust as much as your own host code. See
    [Trust & capabilities](../trust-and-capabilities.md).

The distinguishing capability of the PyO3 loader is **vectorized scalars**. With
`vectorized=True`, the host marshals each input column to a PyArrow array via the
[Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
(zero-copy), the Python function runs **once per batch**, and the result array is
marshaled back to Arrow. This avoids per-row Python call overhead and lets a
NumPy/PyArrow-style function reach multi-million-rows/sec throughput. Without the
flag (`vectorized=False`, the default), the function is called **once per row**
with native Python argument values — simpler to write, roughly an order of
magnitude slower.

Both modes serialize on the Python GIL: the host holds the GIL across the rows of
a batch, so a multi-partition DataFusion scan running a PyO3 UDF collapses to
single-core throughput. That GIL serialization is the dominant operational
concern for PyO3 UDFs.

Choose the PyO3 loader when you want **Python logic at native-ish speed and you
trust the code** — for example, internal analytics functions that depend on the
scientific-Python stack. Choose a sandboxed loader instead
([WASM Component Model](wasm-components.md), Extism, or [Rhai](rhai.md)) when the
plugin is third-party or untrusted and must be isolated from the host.

For parity, the PyO3 `haversine` scalar matches the native Rust implementation to
within **4 ULP** (both row-by-row and vectorized), the same tolerance Rhai meets;
Component Model and Extism agree byte-for-byte.

## Authoring

A PyO3 plugin is a Python module. Entries are declared with **decorator calls** at
module-execution time against a `db` global the loader injects into the module
namespace before running it: `db.set_plugin_id(...)`, `db.set_version(...)`, and
`@db.scalar_fn(...)` / `@db.aggregate_fn(...)` / `@db.procedure(...)`. Each
decorator records into a manifest builder, and the loader drains the builder into
the host registry once the module finishes executing. (This decorator surface is
the PyO3 analogue of Rhai's `uni_manifest()` function and the JSON `manifest`
export read by Extism / Component Model.)

The worked example is `examples/example-pyo3-geo/geo.py`, a great-circle distance
scalar:

```python
import math

db.set_plugin_id("ai.dragonscale.geo")
db.set_version("0.3.1")

R = 6371.0  # Earth radius in km


@db.scalar_fn(
    "haversine",
    args=["float", "float", "float", "float"],
    returns="float",
    determinism="pure",
)
def haversine(lat1, lon1, lat2, lon2):
    """Great-circle distance in km using the asin-form haversine."""
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2.0) ** 2
    return R * 2.0 * math.asin(math.sqrt(a))
```

The decorator metadata is the manifest:

- `args` — argument type names, drawn from `"float"`, `"int"`, `"string"`,
  `"bool"`.
- `returns` — the result type name in the same naming.
- `determinism` — `"pure"`, `"session"`, or `"nondeterministic"`.
- `vectorized` — defaults to `False` (one call per row). Set `vectorized=True`
  to receive each argument as a **PyArrow array** and return a PyArrow array,
  running once per batch.

The function above is row-by-row: it takes scalar floats and handles `None`
(SQL `NULL`) explicitly. A vectorized variant would instead accept and return
PyArrow arrays. Aggregates are declared with `@db.aggregate_fn(...)` and supply
`init` / `accumulate` / `merge` / `finalize` callables; procedures with
`@db.procedure(...)` return an iterable of dicts. See
[Authoring](../authoring.md) for the cross-loader contract.

## Loading

!!! note "Where the Python load path lives — `Session`, not `Uni`"
    PyO3 plugins load on a **session**, not on the instance. The methods are
    `Session.load_python_plugin(...)` (sync) and
    `AsyncSession.load_python_plugin(...)` (async) — obtained via `db.session()`
    — plus the `@session.scalar_fn` / `aggregate_fn` / `procedure` decorator
    surface finalized with `session.finalize_plugin(...)`. Registration is
    **session-scoped**. This differs from the WASM loaders, which load on the
    instance (`Uni` / `AsyncUni`) and register instance-wide. The Rust host API
    exposes the same loader on the instance via `Uni::load_python_plugin`.

### From Python

Get a session, then load the module source. `load_python_plugin(module_src,
module_name, grants=None)` returns a dict with `plugin_id`, `version`,
`scalars_registered`, `aggregates_registered`, `procedures_registered`, and
`denied_capabilities`. The plugin is registered for that session.

```python
from uni_db import Uni

db = Uni.open("graph.uni")
session = db.session()

with open("geo.py") as f:
    outcome = session.load_python_plugin(
        f.read(),
        "ai.dragonscale.geo",   # module name / default plugin id
        grants=["ScalarFn"],
    )
print(outcome["plugin_id"], outcome["scalars_registered"])
```

The async form is identical on an `AsyncSession` (`db.session()` on an
`AsyncUni`), awaited: `outcome = await session.load_python_plugin(...)`.
Alternatively, build the plugin with decorators and finalize it:

```python
session = db.session()

@session.scalar_fn("haversine", args=["float"] * 4, returns="float")
def haversine(lat1, lon1, lat2, lon2):
    ...

outcome = session.finalize_plugin("ai.dragonscale.geo", grants=["ScalarFn"])
```

### From Rust

The Rust entry point is `Uni::load_python_plugin`. You construct a
`PythonPluginLoader` (giving it a default plugin id used when the module omits
`db.set_plugin_id(...)`), declare the capability set the registrar will admit,
and call `load_python_plugin` under the GIL. From
`examples/example-pyo3-geo/src/main.rs`:

```rust
use pyo3::Python;
use uni_db::Uni;
use uni_plugin::{Capability, CapabilitySet, QName};
use uni_plugin_pyo3::PythonPluginLoader;

const MODULE_SRC: &str = include_str!("../geo.py");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Python::initialize();

    let db = Uni::in_memory().build().await?;
    let loader = PythonPluginLoader::with_default_plugin_id("ai.dragonscale.geo");
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);

    // module_src = Python source, module_name = simulated `__name__`.
    let outcome = Python::attach(|py| {
        db.load_python_plugin(py, &loader, MODULE_SRC, "ai.dragonscale.geo", &caps)
    })?;
    println!(
        "loaded `{}` v{} ({} scalar fn(s))",
        outcome.plugin_id.as_str(),
        outcome.version,
        outcome.scalars_registered.len()
    );

    // Look the registered scalar up by qualified name and invoke it.
    let qn = QName::new("ai.dragonscale.geo", "haversine");
    let entry = db.plugin_registry().scalar_fn(&qn).expect("registered");
    // entry.function.invoke(&columnar_args, n_rows) -> ColumnarValue
    Ok(())
}
```

`load_python_plugin` executes `module_src` against a fresh module namespace, drains
the decorator-built manifest, and commits the scalar / aggregate / procedure
adapters onto the instance's `PluginRegistry` atomically. It returns a
`LoadOutcome` carrying the resolved `plugin_id`, `version`, and the lists of
registered scalar / aggregate / procedure names. Requires the `pyo3-plugins`
feature on `uni-db` (and `pyo3` on `uni-plugin-pyo3`). A capability the registrar
was not given is silently skipped at registration: e.g. without
`Capability::ScalarFn` the haversine scalar would not register.

To run the worked example:

```bash
cd examples/example-pyo3-geo
cargo run --release
```

## Scope & limitations

!!! note "PyO3 loader scope"
    - **Trusted / not sandboxed.** Plugins run at host process privilege in the
      embedded interpreter; capabilities are gating metadata, not isolation. Do
      not load untrusted Python — use a sandboxed loader for that. See
      [Trust & capabilities](../trust-and-capabilities.md).
    - **Loadable from Python and Rust.** From Python, load on a session
      (`Session.load_python_plugin(...)` / `AsyncSession.load_python_plugin(...)`,
      or the `@session.scalar_fn` decorators + `finalize_plugin(...)`). From
      Rust, the same loader is on the instance via `Uni::load_python_plugin`.
      There is no `load_python_plugin` on the Python `Uni` / `AsyncUni` — it is a
      session method.
    - **Plugin kinds:** scalar, aggregate, and procedure (v1).
    - **GIL serialization.** Both vectorized and row-by-row modes hold the GIL
      across a batch, so concurrent partitions running a PyO3 UDF do not scale
      across cores. Sub-interpreter / free-threaded parallelism is deferred.
    - **Scope depends on the entry point.** The Python session methods register
      **session-scoped**; the Rust `Uni::load_python_plugin` registers on the
      instance registry (drop with `Uni::remove_plugin` for instance semantics).

## See also

- [Rhai plugins](rhai.md) — sandboxed scripting loader (also ≤ 4 ULP parity).
- [WASM Component Model plugins](wasm-components.md) — sandboxed, byte-identical
  parity.
- [Authoring plugins](../authoring.md) — the cross-loader authoring contract.
- [Plugin reference](../reference.md) — capabilities, types, and registry API.
