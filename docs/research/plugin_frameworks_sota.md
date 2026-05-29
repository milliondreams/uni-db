# Plugin Frameworks — State of the Art

## A Survey of Rust, Database, and Adjacent Extension Architectures (2024–2026)

**Status:** Reference / research artifact for `docs/proposals/plugin_framework.md`
**Date:** 2026-05-22

---

## 0. Why this document exists

The uni-db plugin framework proposal in `docs/proposals/plugin_framework.md` makes opinionated design choices that look more grounded once read against the wider landscape of plugin systems. This document surveys that landscape — Rust language-level mechanisms, database extension architectures, and adjacent extensibility systems (eBPF, OSGi, audio, IDE) — and then maps the findings back to the proposal's choices. It is reference material, not a design document.

The survey covers:

- §1. The fundamental Rust plugin problem (unstable ABI) and the ecosystem's three answers.
- §2. WebAssembly as the universal plugin format — Component Model, Extism, the runtime ecosystem.
- §3. SQL database extension architectures — PostgreSQL (gold standard), DuckDB, SQLite, MySQL, ClickHouse, Trino, Spark DSv2, Snowflake/BigQuery.
- §4. The DataFusion-based ecosystem — InfluxDB 3, GreptimeDB, RisingWave, Databend, ParadeDB.
- §5. Graph databases — Neo4j + APOC, TigerGraph, JanusGraph, Apache AGE.
- §6. Vector and ML-database extensibility — Weaviate, LanceDB, Milvus, Qdrant, Vespa.
- §7. Document / multi-model — MongoDB, Couchbase, SurrealDB, EdgeDB/Gel.
- §8. Stream / time-series — RisingWave, Materialize, Flink, InfluxDB.
- §9. Adjacent extensibility worth stealing from — eBPF, OSGi, VS Code, Bevy, CLAP, browsers.
- §10. Capability-based security in plugin systems.
- §11. Comparison matrix.
- §12. Where the field is going (2024–2026 trend lines).
- §13. Implications for uni-db.

---

## 1. The Rust Plugin Problem

Rust deliberately does not stabilize its ABI. The compiler is free to change `repr(Rust)` layout, vtable shape, name mangling, and panic-unwinding conventions between versions — and does. This is not a bug to be fixed; it is the explicit price Rust pays for monomorphization, niche optimization, and zero-cost abstractions. The consequence: **you cannot reliably load a Rust `.so` compiled with rustc N into a host compiled with rustc M unless N == M and the compile flags match exactly.**

There are three real answers to "how do I build a plugin system in Rust."

### 1.1 Compile-time registration (the conservative path)

Plugins are Rust crates linked into the host binary at build time. Hot-loading is impossible by construction, but discovery can be made dynamic at compile time via:

- **`inventory`** (David Tolnay) — registers items into a global collection at static-initialization time. Plugins implement a trait, call `inventory::submit!`, and the host iterates via `inventory::iter::<MyTrait>`.
- **`linkme`** (David Tolnay, successor) — uses linker `#[distributed_slice]` to collect items into a section the linker concatenates. Slightly more efficient than `inventory`; no init-time cost.
- **`ctor`** (mmastrac) — runs constructor code at library load time. Useful for building registries inside `lazy_static` / `OnceLock`.

This is what most production Rust applications use. Bevy plugins, `tracing` layers, `tower` services, `serde` codecs, and Polars expression plugins all sit on this pattern.

Strengths: native performance, full Rust idiom (generics, lifetimes), no FFI hazards.
Weaknesses: no third-party hot-load, every plugin must compile with the host's rustc, plugins cannot ship as binaries.

### 1.2 Stable Rust ABI shims (the brave path)

Two production crates attempt to give Rust a stable, version-resilient ABI:

#### `abi_stable` (Rodrigo Kumpera / RustyYato)
The mature option. Provides `StableAbi` derive, FFI-safe replacements (`RString`, `RVec`, `ROption`, `RBox`, `RArc`), the `#[sabi_trait]` macro for FFI-safe trait objects, and `NonExhaustive` enums plus prefix-extended vtables/modules for backward-compatible evolution. At load time the host checks the loaded library's recorded layout against its own.

API hazards: every type crossing the boundary must be an `abi_stable` type. You write `RString` and `ROption<T>`, not `String` and `Option<T>`. The standard library is invisible across the boundary.

Real-world use: lyrics (music application), bracer-rs, several internal plugin systems. No major database uses it. The ecosystem is small.

#### `stabby` (Olivier Faure / Pleasant Software)
The newer alternative. Trait-driven rather than type-replacement-driven — you derive `Stabby` on your types and they get a stable layout. Niche optimization is preserved. Smaller surface area than `abi_stable`. Production use is also narrow.

Both crates work but neither has reached the critical mass where ecosystem libraries (tokio, serde, arrow) annotate themselves with the relevant derives. So even with `abi_stable` or `stabby`, you can't blindly pass a `tokio::sync::Mutex` or a `arrow::array::Array` across the boundary — you have to wrap and unwrap.

### 1.3 Out-of-process / sandboxed plugins (the practical path)

Run plugins outside the rust ABI entirely. Three flavors:

1. **WebAssembly** — sandboxed in-process. The dominant modern answer (see §2).
2. **gRPC sidecar** — HashiCorp's `go-plugin` model. Each plugin is a separate binary speaking gRPC over a local socket. Vault, Terraform, and Packer use this. Heavy isolation, language-agnostic, but high per-call cost.
3. **Embedded scripting languages** — `mlua` (Lua), `rhai` (a Rust DSL), `rune` (a Rust scripting language), `pyo3` (CPython), `boa` (JS in Rust), `quickjs-rs`. Sandbox quality varies; performance ceiling is low.

The 2024–2026 ecosystem trend is unmistakable: **WASM is winning the dynamic-plugin race.** Every system seriously investing in dynamic extensibility — Postgres (pg_wasm experiments), DuckDB (community extensions can be WASM), Redis (Redis Functions originally JS, exploring WASM), SurrealDB, RisingWave, InfluxDB — converges on WASM.

### 1.4 The hybrid pattern most Rust databases now ship

The pragmatic synthesis: **compile-time registration for built-ins and trusted extensions; WASM (or scripting) for everything else.** This matches DuckDB, Polars, RisingWave, and is the model the uni-db proposal adopts. Nobody serious uses `abi_stable` for a public plugin API anymore.

---

## 2. WebAssembly as the Universal Plugin Format

Two distinct WASM-plugin philosophies have emerged. They differ on whether to bet on the **Component Model** standard or to ship a **pragmatic shared-memory protocol**.

### 2.1 The WebAssembly Component Model

The Bytecode Alliance's effort to standardize cross-language WASM linking. Two layers:

- **WIT (WebAssembly Interface Types)** — an IDL for component interfaces. Defines `interface`, `world`, `import`, `export`, records, variants, lists, results, resources. Stable enough to build against.
- **WASI Preview 2 (WASI 0.2.0)** — a curated set of WIT interfaces (filesystem, sockets, http, cli, clocks, random). Released Jan 25 2024. Pin to `>=0.2.0` for stable APIs.

Component-model languages with production toolchains:
- **Rust** — `cargo-component`, `wit-bindgen` (mature).
- **Go** — TinyGo + `wit-bindgen-go` (good).
- **Python** — `componentize-py` (works; produces large components).
- **JavaScript** — `jco` (Bytecode Alliance; works).
- **C/C++** — `wasi-sdk` (mature for C; C++ has caveats).
- **C#** — .NET 9 has component-model support.
- **MoonBit** — built around components from day one.

What's still in flux as of 2026:
- **Async** is the big gap. WASI Preview 2 is synchronous; async support is on the WASI 0.3 roadmap. Production systems use blocking semantics inside per-call threads.
- **Component composition at runtime** (linking two components into one application) works but is awkward; static composition with `wasm-tools compose` is the mature path.
- **Resource lifetimes** across host-plugin boundaries have semver-evolution pitfalls; the standard is settled but not all tools handle resources uniformly.

When you use the Component Model, you get: typed cross-language contracts; per-world capability gating via host imports; multiple languages on the plugin side without per-language host glue. The trade-off is that the toolchain is opinionated — you write WIT files, you `cargo component build`, your components target `wasm32-wasip2`.

### 2.2 Extism (the pragmatic anti-Component-Model)

Dylibso's Extism takes the opposite bet: **standardize the host-plugin protocol; don't wait for the Component Model.** As of 2026 Extism is the de facto choice for projects that want WASM plugins now without committing to the WIT toolchain.

Architecture:
- **Raw WASM** modules (no Component Model). The protocol lives at the host-import / module-export level.
- **Memory exchange via host-controlled allocator.** Plugins call `extism_alloc(len)`, host writes input bytes, plugin calls `extism_input_load(ptr, len)`. Output is symmetric (`extism_output_set(ptr, len)`). No typed marshalling — bytes in, bytes out, and the format is up to the protocol layer (often JSON or MessagePack).
- **Host functions** are declared via the SDK and imported by the plugin.
- **Additional runtime utilities** the raw wasm runtimes don't ship: persistent memory across invocations, HTTP via host-controlled imports (no WASI dependency), timers, fuel limiters.

Host SDKs (2026): Rust, Go, Python, Java, .NET, Node.js, Ruby, PHP, Perl, Elixir, Haskell, OCaml, Zig — 13+.
Plugin PDKs: Rust, JavaScript, Go, Python, C, C++, AssemblyScript, Zig, .NET, Haskell.

Why Extism wins for ad-hoc plugin systems:
- Lower ceiling (no WIT-toolchain prerequisite).
- Better polyglot story today (13 host SDKs is more than the Component Model has).
- Mature persistent-state and HTTP stories.

Why the Component Model wins for foundational infrastructure:
- Typed contracts that the host *cannot* misinterpret. Extism's "bytes in / bytes out" pushes type discipline into a protocol layer the host has to implement.
- Composition across components is a first-class operation.
- The toolchain investment pays off as more languages reach component-model maturity.

For a database engine — where the host-plugin contract is load-bearing for soundness — the typed-contracts argument tips toward the Component Model. For an application framework adding scripting hooks, Extism is the pragmatic choice. This is the exact split the uni-db proposal makes by adopting the Component Model.

### 2.3 wasmtime vs wasmer

The two production WASM runtimes for Rust embedding:

- **`wasmtime`** (Bytecode Alliance) — the reference Component Model implementation. Active, well-funded, supports `wasm32-wasip2`, has the best Component Model story. Fuel metering, epoch interruption, resource limiters. Larger binary (~30–50 MB). The default choice for new projects.
- **`wasmer`** (Wasmer Inc.) — older, broader runtime support (multiple compiler backends: Cranelift, Singlepass, LLVM), smaller binary, has its own non-component-model package format (WAPM). Component Model support has lagged.

Most serious database integrations choose `wasmtime`. DuckDB, RisingWave, SurrealDB, and the uni-db proposal all use wasmtime.

Niche alternatives:
- **`wasmi`** — pure-Rust interpreter. Tiny binary, no JIT, used in `polkadot-sdk`. Useful for environments where you can't ship a JIT.
- **`wamr`** — WebAssembly Micro Runtime, embedded-focused, C-based.
- **`wagon`** / **`life`** — older, less actively maintained.

### 2.4 Composability — wasmCloud, Spin, lunatic

Three frameworks build *applications* (not just plugin systems) on top of WASM components. They are worth knowing because they pioneered patterns relevant to plugin design.

- **wasmCloud** (Bytecode Alliance) — actor-style WASM applications. Components ("actors") + capability providers (filesystem, HTTP, KV). Distinguishes "compute" from "capabilities" using the Component Model. Influential for the capability-import pattern.
- **Spin** (Fermyon) — WASM-component application framework with serverless ergonomics. `spin up`, `spin deploy`, HTTP triggers, components as functions. Less plugin-shaped, more app-shaped.
- **lunatic** — Erlang-style processes implemented as WASM modules. Per-process isolation, message passing, supervision trees, hot reload. Influential for the per-plugin-store isolation pattern.

The pattern these systems normalized: **capability imports are the unit of permission.** A WASM component has access to a host function if and only if it was linked against that host function at instantiation. Permission is enforced by absence of imports, not by runtime checks. The uni-db proposal adopts this directly.

---

## 3. SQL Database Extension Architectures

### 3.1 PostgreSQL — the gold standard

PostgreSQL has the most mature, most production-tested extension system of any database, full stop. Every system designing an extension architecture is implicitly compared to Postgres. Worth understanding in depth.

**Extension surfaces:**
1. **Functions / aggregates / window functions** — `CREATE FUNCTION ... LANGUAGE C` plus internal-language variants (PL/pgSQL, PL/Python, PL/v8, PL/Rust). 
2. **Custom types and operators** — full type lifecycle (`CREATE TYPE` with `INPUT`, `OUTPUT`, `RECV`, `SEND`, `TYPMOD_IN`, `TYPMOD_OUT`, etc.), operators with cost/selectivity hints, operator classes for indexing.
3. **Index access methods (AMs)** — `CREATE ACCESS METHOD` for new index kinds. `pg_trgm`, `pgvector`, `bloom`, `rum` all extend Postgres this way. Has cost estimation, page layout control, vacuum integration.
4. **Table access methods** — pluggable storage formats (since PG12). `zheap`, `cstore_fdw` (deprecated), recent in-memory engines.
5. **Foreign Data Wrappers (FDWs)** — `postgres_fdw`, `mysql_fdw`, `parquet_fdw`. The model SQL DBs adopted as "external tables."
6. **Hooks** — at parse, analyze, plan, executor-start, executor-run, executor-finish, executor-end, ProcessUtility, post-parse-analyze, commit, abort. These are global function pointers any extension can chain.
7. **Background workers** — `RegisterBackgroundWorker`, with state machine (Registered → Starting → Running → Stopping). `pg_cron`, `pg_partman`, autovacuum all use this.
8. **Logical decoding output plugins** — change-data-capture. `pgoutput` (built-in), `wal2json`, `decoderbufs`. The pattern the proposal's `CdcOutputProvider` follows.
9. **Custom scan / custom path / custom plan nodes** — physical-plan extensions. `pg_tle`'s trusted-language framework uses this; many sharded Postgres derivatives use it for distributed execution.
10. **Procedural languages (PLs)** — `CREATE EXTENSION plpython3u`, `plv8`, `plrust`, `plprql`. Each language is itself a plugin.
11. **GUC (Grand Unified Configuration) parameters** — extensions can register their own config namespace.
12. **Trigger functions** — row-level / statement-level, before/after.

The contract Postgres maintains is **C-level ABI + careful semver of internal headers between major versions**. Extensions ship as `.so` files installed in `$PGLIB/`. The C ABI is stable across minor versions; major versions (12 → 13 → 14 → …) require recompilation. There is no sandbox — extensions run in the postmaster process with full privileges. Extension authors are expected to be trusted infrastructure.

What makes Postgres extensions work despite the absence of sandboxing:
- Distribution via `pg_extension` system catalogs + the `PGXN` registry.
- The trust model is "the DBA installs `.so` files into `$PGLIB`; only superusers can `CREATE EXTENSION`."
- A flourishing ecosystem of high-quality extensions (`pgvector`, `pg_trgm`, `pg_stat_statements`, `pg_repack`, `pg_partman`, `pgcrypto`, `PostGIS`, `TimescaleDB`, `Citus`, `Hypopg`, `pg_hint_plan`, `pgaudit`) — none of which would exist without the extensive hook system.

**The lesson uni-db should take from Postgres:** Extension *surface area* is the moat. Postgres won the extensibility race not by having one elegant abstraction but by exposing every internal seam — hooks at every lifecycle stage, AMs for every index, FDWs for every external source, output plugins for every replication need, custom scans for every optimizer trick. The uni-db proposal's 25-surface inventory is consciously modeled on this.

### 3.2 pgrx — Postgres extensions in Rust

`pgrx` (formerly `pgx`) is the Rust framework for Postgres extensions, maintained by the PG Central Foundation. It is the single most important "Rust meets database extensions" project.

What it provides:
- **Macro-driven function declaration**: `#[pg_extern] fn add(a: i64, b: i64) -> i64 { a + b }`.
- **Panic safety**: Rust panics become Postgres `ERROR`s that abort the surrounding transaction, not the backend process.
- **NULL handling via `Option<T>`** instead of C's `IsNull` flag.
- **Custom types**: `#[derive(PostgresType)]` with automatic `pg_type` registration.
- **Triggers**: `#[pg_trigger]` decorators.
- **Hooks**: typed wrappers around Postgres' planner/executor hook chain.
- **SPI** (Server Programming Interface) — Rust-typed query execution.
- **Background workers**: `BackgroundWorkerBuilder` with Rust-typed callbacks.
- **`cargo-pgrx`** — manages downloading + building of supported Postgres versions, scaffolding, testing.
- **Cross-version support** — one codebase compiles against PG 13–18.

Production users (2026):
- **ParadeDB** — analytics + search extension built on pgrx (uses Tantivy for FTS, Apache DataFusion for analytics).
- **Supabase pg_graphql** — auto-generated GraphQL API as a Postgres extension.
- **TimescaleDB Toolkit** — statistical and time-series functions.
- **PlRust** — `LANGUAGE plrust` for trusted Rust UDFs in Postgres.
- **pg_search** — full-text search.
- **vector.rs** — vector operations.

What pgrx still lacks:
- **No async story**. Postgres is single-threaded per backend; pgrx inherits this. No tokio inside an extension.
- **Not 1.0** as of 2026. Maintainers reserve the right to break the API; soundness issues are documented but not all fixed.
- **Sessions during extension upgrades** see the old version until reconnection.

What pgrx *taught* the Rust database community:
1. A typed Rust macro layer over a C extension API works extraordinarily well — far better than hand-written FFI.
2. Panic-to-error translation is a critical safety feature. Without it, plugins kill the database.
3. The compile-time-versioned-against-each-host approach is acceptable for trusted infrastructure.

The uni-db proposal's "compile-time Rust + sandboxed WASM" split is a generalization of pgrx's approach: pgrx is the compile-time tier for Postgres, and Postgres has no sandboxed tier. We're adding one.

### 3.3 DuckDB extensions

DuckDB's extension system has gone through three phases:

**Phase 1 (≤2022):** C++ extensions only, statically or dynamically linked, no signing.

**Phase 2 (2023):** Signed extensions, official extensions hosted on `extensions.duckdb.org`. Auto-installation. Categories: data sources (`httpfs`, `aws`, `azure`), formats (`parquet`, `json`, `excel`, `arrow`), domain functions (`spatial`, `inet`, `fts`, `vss`).

**Phase 3 (2024+):** **Community Extensions program.** Third-party extensions hosted at `community-extensions.duckdb.org`, distribution centralized but build/maintenance distributed. Installed via `INSTALL X FROM community; LOAD X;`. Each extension is built by the Community Extension CI, signed, and delivered. Users opt in (or opt out via `SET allow_community_extensions = false`).

What an extension can extend:
- **Scalar functions** (`ScalarFunction` C++ API).
- **Aggregate functions** (`AggregateFunction`).
- **Table functions** (return tables — DuckDB's analogue of procedures).
- **Copy functions** (custom import/export formats — how `httpfs` plugs in S3, how `parquet` plugs in).
- **Replacement scans** — when DuckDB sees `FROM 'something_unknown.parquet'`, replacement scans get a chance to claim the table reference. **This is the pattern the uni-db proposal lifts as `ReplacementScanProvider`.**
- **Catalog plugins** — virtual schemas. Postgres tables exposed via `postgres` extension look like DuckDB tables.
- **Optimizer extensions** — register optimizer rules.
- **Settings** — register GUC-like parameters.

Languages:
- **C++** (native, most extensions).
- **Rust** via `duckdb-extension-template-rs` and `duckdb-rs` — works but the C-extension API was historically C++-shaped; the new C extension API (DuckDB 1.0+) makes this much cleaner.
- **WASM extensions** — DuckDB-Wasm (the browser build) loads extensions compiled to WASM. The same `.wasm` file works in WASM-built DuckDB and in some native scenarios.

Notable community extensions: `chsql` (ClickHouse SQL compat), `quack` (Bun.js bindings), `nanoarrow`, `duckdb_dynamic`, `prql`, several geo extensions, `evdc` (Excel VBA), `httpserver`.

The DuckDB lesson: **A central signed registry with opt-out is the pragmatic distribution model.** Not a marketplace, not a free-for-all. A CI-built signed channel that ships pre-compiled binaries for the matrix of platforms × DuckDB versions. The uni-db proposal §17.4 punts on this (recommending HTTP-URL distribution for v1); DuckDB's Community Extensions is the model to evolve toward.

### 3.4 SQLite extensions

SQLite has had loadable extensions since approximately forever. The mechanism:

1. Build a `.so` / `.dylib` with an entry point named `sqlite3_<name>_init`.
2. Call `sqlite3_load_extension('libfoo.so', 'sqlite3_foo_init')` from SQL or the C API.
3. The entry point registers functions, modules (virtual tables), and collations.

Extension capabilities:
- **Application-defined scalar / aggregate functions** (`sqlite3_create_function_v2`).
- **Virtual tables** (`sqlite3_create_module_v2`) — the most powerful surface. Any C struct can appear as a SQL table. Used by `FTS5`, `R*Tree`, `JSON1` (which moved to core), `csv` extension, `series`, `sqlite-vec`, `sqlite-vss`.
- **Collations** — custom sort orders.
- **VFS** (virtual filesystem) — custom storage layers.

**`sqlite-loadable-rs`** (Alex Garcia, mergestat) is the leading Rust framework for building SQLite extensions:
- Supports scalar functions, table functions (eponymous virtual tables), and full virtual tables.
- Roadmap: aggregate window functions, collations, VFS.
- Production extensions: `sqlite-xsv` (CSV — 1.5–1.7× faster than core CSV), `sqlite-regex`, `sqlite-vec` (vector search), `sqlite-base64`, `sqlite-html`, `sqlite-lines`, `sqlite-url`.
- Performance: ~10–15% slower than C-written extensions; binary size ~469 KB (vs 17 KB for C).
- Distinct from `rusqlite::Connection::create_scalar_function`: that registers functions in-process for a single Rust app; `sqlite-loadable-rs` produces real `.so` extensions any SQLite can load.

**Lesson:** The "virtual table" abstraction was way ahead of its time. SQLite normalized "anything that can produce rows is a table" decades before DuckDB's catalog plugins or Postgres FDWs were widespread. The uni-db `CatalogProvider` and `ReplacementScanProvider` are descendants of this idea.

### 3.5 MySQL / MariaDB

MySQL has three extension mechanisms:

1. **UDFs** (User-Defined Functions) — C `.so` loadable via `CREATE FUNCTION foo RETURNS STRING SONAME 'libfoo.so';`. Scalar and aggregate functions only. Crashes the server if they panic.
2. **Plugins** — audit plugins, password validation plugins, authentication plugins, daemon plugins, FTS parser plugins, INFORMATION_SCHEMA plugins. Loaded with `INSTALL PLUGIN`.
3. **Storage engines** — pluggable B-tree, columnstore, archive engines. MyRocks, TokuDB, InnoDB are all "plugins" in this sense, though most are statically compiled in.

Less mature than Postgres; far fewer hooks; the audit plugin API is the only thing approaching Postgres's hook system. The MySQL ecosystem mostly works around this with **proxy layers** (ProxySQL, Vitess) rather than in-server extensions.

MariaDB diverged with **Component Plugins** and more aggressive cleanups but the basic story is similar.

### 3.6 ClickHouse

ClickHouse's extensibility story is unusual:

- **UDFs** as shell-executed binaries — declare a UDF that runs an external binary on each batch via stdin/stdout, with JSON or TSV serialization. Genuinely unsafe but operationally pragmatic.
- **Executable UDFs** can be sandboxed via the `executable_user_defined_functions_lifetime` setting and resource limits.
- **Dictionaries** — externally-defined key-value lookups, refreshed on a schedule. The closest analogue to a catalog plugin.
- **Table functions** — built-in, not pluggable (you can't write your own table function as an extension).
- **Table engines** — pluggable in principle (the engine API exists) but rarely used externally; the ClickHouse community mostly contributes engines upstream.
- **Aggregate function combinators** — `-State`, `-Merge`, `-If`, `-Array`, etc. — a powerful built-in combinator system, but you cannot add new combinators.

ClickHouse essentially has no plugin ecosystem comparable to Postgres or DuckDB. The shell-UDF mechanism is a workaround, not a design. Most extensibility happens by forking and merging upstream.

### 3.7 Trino / Presto

Trino's `Connector` SPI is the analogue of FDWs but with much more first-class status. Every data source — Hive, Iceberg, Delta, Kafka, MySQL, Postgres, MongoDB, Elasticsearch — is a connector. Plugins implement:

- `ConnectorMetadata` — schemas, tables, columns, statistics.
- `ConnectorSplitManager` — partitioning.
- `ConnectorPageSource` — row groups.
- `ConnectorPageSink` — writes.
- Pushdown hooks — predicate pushdown, limit pushdown, aggregate pushdown, projection pushdown.

Plus separate plugin SPIs for:
- Functions and aggregates (Java).
- Custom types.
- Resource groups.
- Security plugins.
- Event listeners.

Plugins ship as JARs in a per-connector directory. No sandbox (JVM-level isolation only). The classloader-per-plugin pattern provides namespace isolation but not security.

**The Trino lesson:** First-class pushdown negotiation between planner and connector is the single most important optimization for federated queries. The connector declares which predicates / aggregates / limits it can handle and the planner pushes them down accordingly. This is exactly the **Spark DataSources V2** pattern and is what the uni-db proposal's §4.25 `Pushdown` composable trait reflects.

### 3.8 Spark DataSources V2

Spark's DSv2 (Java/Scala SPI) is the cleanest expression of pushdown-negotiation extensibility:

- `TableProvider`, `Table`, `ScanBuilder`, `Scan`, `Batch`, `PartitionReader`.
- `SupportsPushDownFilters`, `SupportsPushDownLimit`, `SupportsPushDownAggregates`, `SupportsPushDownTopN`, `SupportsPushDownRequiredColumns`.
- Each capability is a separate marker trait; the source opts in to whichever it can support.

Plus `Catalog`, `Function`, `View`, `UDT` registries.

The uni-db proposal's `Pushdown` composable trait borrows this pattern wholesale.

### 3.9 Snowflake / BigQuery / Databricks

Managed cloud DBs converged on a similar UDF model:

- **Multiple language frontends** — SQL, JavaScript, Python, Java, Scala, in the cloud provider's controlled sandbox.
- **Service-account integration** — UDFs can call out to KMS, secrets manager, cloud storage with managed permissions.
- **External functions** — Snowflake's "External Functions" and BigQuery's "Remote Functions" call out to AWS Lambda / GCP Cloud Functions. Plugins live on the customer's serverless infrastructure; the warehouse calls them like a UDF.
- **Network policies** — egress allow-lists are first-class for the python/JS UDF runtime.

Snowflake also has:
- **Snowpark Container Services** — run arbitrary containers in the warehouse, exposed as services.
- **Native Apps** — packaged applications running in the customer's Snowflake account, with a managed permission model.

These systems essentially built **plugin-as-service** rather than plugin-as-library. The boundary is gRPC or HTTP, not FFI. The pattern doesn't translate to embedded databases like uni-db directly, but the **capability-negotiation-at-deployment-time** mental model (declare what your function needs; cloud grants or denies) is exactly the uni-db proposal's manifest-capabilities model adapted to a managed environment.

### 3.10 CockroachDB / TiDB / YugabyteDB

Distributed SQL DBs with limited plugin stories:

- **CockroachDB** — no formal plugin system. CHANGEFEED for CDC. Cloud-side functions in the managed service.
- **TiDB** — TiPlugin (audit hooks, whitelist enforcement). Coprocessor pushdown (essentially DSv2-like, between TiDB and TiKV).
- **YugabyteDB** — inherits Postgres extensions for the YSQL layer. Notable for being a real distributed DB with the full Postgres extension API.

The lesson: **distributed execution and arbitrary plugins are hard to reconcile.** Postgres extensions assume one process with shared memory; distributing them requires careful API design. YugabyteDB's approach (run the Postgres extension on a coordinator node, materialize results) is one answer; ParadeDB's (push the work into a separate execution engine via DataFusion) is another.

---

## 4. The DataFusion-Based Ecosystem

A separate, increasingly important cluster of Rust databases built on Apache DataFusion. Worth its own section because uni-db itself uses DataFusion's execution layer.

### 4.1 Apache DataFusion — the foundation

DataFusion's extension surfaces:

1. **`ScalarUDF`** (`ScalarUDFImpl`) — scalar functions, `invoke_with_args(ScalarFunctionArgs)`.
2. **`AggregateUDF`** (`AggregateUDFImpl`) — aggregates, with `Accumulator` trait.
3. **`WindowUDF`** (`WindowUDFImpl`) — window functions, with `PartitionEvaluator`.
4. **Table functions** — return tables from a function call.
5. **`TableProvider`** — register external tables. The most widely-extended surface.
6. **`OptimizerRule`** — logical optimizer rules.
7. **`AnalyzerRule`** — earlier rules, run before logical optimization.
8. **`PhysicalOptimizerRule`** — physical-plan rules.
9. **`ExtensionPlanner`** — convert custom logical nodes to physical nodes.
10. **`QueryPlanner`** — replace the whole logical→physical planner.
11. **`SessionContext`** — programmatic registration of everything above.
12. **Custom `LogicalPlan` / `Expr`** — user-defined plan nodes via `Extension`.

The DataFusion ecosystem has been a force multiplier: any Rust database building on DataFusion inherits all of this. The uni-db proposal's `OperatorProvider`, `OptimizerRuleProvider`, and the DataFusion-aligned UDF traits map directly onto these.

Ecosystem extensions worth knowing:
- **`datafusion-table-providers`** — Postgres / MySQL / SQLite / DuckDB / Flight SQL as DataFusion tables.
- **`datafusion-functions-json`** — JSON scalar functions.
- **`datafusion-functions-extra`** — additional curated functions.
- **`datafusion-federation`** — execute parts of a plan on remote engines.

### 4.2 InfluxDB 3.0 — Python plugins via Processing Engine

InfluxDB 3.0 (formerly IOx) is built on DataFusion. Its Processing Engine (2024+) introduced **Python plugins** with three trigger types:

1. **WAL-flush triggers** — fire when data is written to a measurement.
2. **Scheduled triggers** — cron-style execution.
3. **On-demand HTTP triggers** — external trigger via HTTP endpoint.

Each plugin is a Python script with a defined entry point. The plugin receives the data (write batches as PyArrow tables), runs Python code, and can call back into the database to write data, query data, or emit metrics. Plugins are deployed via the `influxdb3 create trigger ...` CLI.

Use cases: downsampling, materialized views, alerting, data transformation, anomaly detection.

Architecture: Python is embedded via PyO3. The plugin gets a pre-imported `influxdb3_local` API surface. Sandboxing is minimal — plugins run with the database process's privileges. This is essentially the "PyO3 REPL UDF" pattern of the uni-db proposal, but built into the product.

The InfluxDB 3 lesson: **Python is the productivity ceiling for end-user data engineers. WASM is the productivity floor.** A system serving data engineers in 2026 has to provide both, and the Python tier needs PyArrow zero-copy or it's too slow.

### 4.3 GreptimeDB

A Rust time-series database built on DataFusion. Has Python coprocessors (PyO3) for stream transformations. Less elaborate extension story than InfluxDB; the design is similar.

### 4.4 RisingWave — multi-language UDFs

RisingWave is a Rust streaming SQL database. UDF support is its most polished extension surface:

- **Embedded Python UDFs** — declared inline in SQL with `CREATE FUNCTION ... LANGUAGE python AS $$ ... $$`. Runs in an embedded Python sandbox.
- **External Python UDFs** — gRPC service-style, plugin runs as a separate process.
- **Java UDFs** — JVM-side.
- **JavaScript UDFs** via QuickJS embedded.
- **Rust UDFs compiled to WASM** — the model the uni-db proposal converges with. Rust-side, ship a `.wasm` file, declare via `CREATE FUNCTION ... LANGUAGE wasm`.

RisingWave's choice to support **WASM-compiled-from-Rust** alongside Python and Java covers the productivity-vs-performance frontier directly. The uni-db proposal generalizes this to any language that compiles to wasm32-wasip2 components.

### 4.5 Databend

Rust analytics DB, DataFusion-influenced. UDFs in JavaScript via QuickJS and Python via remote services. WASM UDFs in development.

### 4.6 ParadeDB

Postgres + DataFusion via pgrx. The most interesting integration point: ParadeDB demonstrates that the **DataFusion-as-Postgres-extension** path is viable. uni-db sits squarely in the DataFusion ecosystem and benefits from the same maturing extension surfaces.

---

## 5. Graph Databases

### 5.1 Neo4j + APOC

Already covered in depth in the uni-db proposal's review. Summary:

- **`@Procedure`**, **`@UserFunction`**, **`@UserAggregationFunction`** annotations register Java/JVM extensions.
- APOC ships 450+ procedures and functions across ~30 namespaces.
- `apoc.custom.declareProcedure` / `declareFunction` — user-defined extensions registered from Cypher, persisted in the database, survive restart. This is the meta-plugin pattern.
- `apoc.trigger.*` — label/property/event-scoped triggers.
- `apoc.periodic.*` — background jobs, batched iteration.
- `apoc.cypher.run` — dynamic Cypher inside extensions.

Neo4j extensions ship as `.jar` files in `$NEO4J/plugins/`. JVM classloader isolation per plugin. No sandbox (full JVM privileges). Plugin updates require restart.

**The Neo4j lesson:** Surface area drives adoption. APOC is the single biggest reason Neo4j is the dominant graph DB. The procedure/function/aggregate trinity plus triggers and background jobs covers ~95% of what graph-DB users want to extend. uni-db replicating this is non-negotiable for ecosystem credibility.

### 5.2 TigerGraph

Different model: **GSQL** is the extension language. You write graph queries / algorithms / loaders in GSQL, compiled to C++ and linked into the engine. The compilation step gives near-native performance; the cost is a closed language with no general-purpose-language escape hatch.

GSQL Plus packages (essentially "extensions") ship as bundles of GSQL queries.

Less relevant to uni-db's architecture but worth knowing as an alternative endpoint of the "DSL-as-extension" spectrum.

### 5.3 JanusGraph / TinkerPop Gremlin

Apache TinkerPop's Gremlin is the Cypher-of-the-Gremlin-world. Extensions:

- **Strategies** — Gremlin traversal-rewrite rules, the analogue of optimizer extensions.
- **Custom steps** — new traversal verbs.
- **Plugins** — Gremlin console/server extensions.

JanusGraph itself supports pluggable storage backends (Cassandra, HBase, ScyllaDB, BerkeleyDB) and pluggable indexing backends (Elasticsearch, Solr, Lucene). The storage-backend-as-plugin pattern is the gold standard for storage-agnostic databases and is what uni-db's `StorageBackend` trait targets.

### 5.4 Apache AGE

Cypher-on-Postgres via a Postgres extension. Inherits the Postgres extension model. Demonstrates that "the graph DB is just an extension" is a viable architecture.

### 5.5 KuzuDB

Embedded graph DB in C++, built on a relational columnar engine. No formal plugin system as of 2026. Extensibility primarily by forking.

---

## 6. Vector and ML Database Extensibility

### 6.1 Weaviate — modules

Weaviate (Go) has the cleanest extension architecture in the vector-DB world:

- **Vectorizer modules**: `text2vec-openai`, `text2vec-cohere`, `text2vec-huggingface`, `text2vec-transformers`, `text2vec-contextionary`, etc. A module is a Go package implementing the `Vectorizer` interface.
- **Generative modules**: `generative-openai`, `generative-anthropic`, `generative-cohere`, etc. for RAG.
- **Reranker modules**: `reranker-cohere`, `reranker-transformers`.
- **QnA modules**: `qna-openai`, `qna-transformers`.
- **Backup modules**: `backup-s3`, `backup-gcs`, `backup-filesystem`.

Each module is configured per-class (per-collection). Modules can be enabled/disabled at runtime via configuration. They're statically linked into the Weaviate binary — there's no dynamic loading — but the *interface* surface is plugin-shaped.

**The Weaviate lesson:** When extensibility means "swap one of N implementations of a clear interface," static linking with a registry is sufficient and avoids the WASM/dynamic-load complexity. The downside is no third-party modules (everything is in-tree). Weaviate has been pragmatic about this and accepts upstream contributions liberally.

uni-db's `IndexKindProvider`, `CrdtKindProvider`, and `CatalogProvider` are essentially the same pattern.

### 6.2 LanceDB

Embedded vector DB built on Apache Arrow + Lance (a columnar format). Plugin-shaped surfaces:

- **Embedding functions** — register a Python function as `embed(text) -> vector`. Computed once on insert, cached.
- **Index types** — IVF-PQ, IVF-Flat, HNSW. Building new index types requires forking the Rust core.

The embedding-function pattern is small but well-designed: it captures the most common vector-DB extension need (compute embeddings outside the database) without exposing the full storage-backend complexity. Worth borrowing in uni-db for the embedding-function shape on top of `ScalarPluginFn`.

### 6.3 Milvus

C++ vector DB. Limited plugin story; index types are pluggable in-tree (FAISS, HNSW, DiskANN), but no third-party plugin path.

### 6.4 Qdrant

Rust vector DB. No formal plugin system as of 2026. Extensions land via upstream contributions or forks. Quantization (scalar, product, binary) and sparse vectors are configurable but not plugin-shaped.

### 6.5 Vespa

Yahoo/Verizon Media's search engine. OSGi-based Java plugin model — **document processors**, **searchers**, and **handlers** are OSGi bundles. The OSGi influence is rare in modern systems but Vespa demonstrates that a sophisticated module system with versioning and lifecycle can work in production search.

### 6.6 Pinecone

Managed-service vector DB. No plugin system; extensions are limited to the API surface offered.

---

## 7. Document / Multi-Model Databases

### 7.1 MongoDB

`$function` operator — run server-side JavaScript inside aggregation pipelines. Slow, sandboxed via V8, with quota enforcement. The historical answer to "how do I extend MongoDB" — and it remains the only answer. No first-class plugin system.

MongoDB Atlas adds **Atlas Functions** — serverless functions that can be called by Realm/App Services. This is the cloud-side answer (similar to Snowflake External Functions).

### 7.2 Couchbase

User-defined functions in JavaScript. **Eventing service** — triggers on document mutations, execute JavaScript handlers. Influential for fine-grained trigger design — the uni-db proposal's `TriggerPlugin` borrows the per-document, per-mutation granularity.

### 7.3 SurrealDB

Embedded multi-model DB in Rust. **`DEFINE FUNCTION`** with SurrealQL bodies; functions are persistent first-class schema objects (not transient). Permissions: `FULL`, `NONE`, `WHERE <condition>`. Recursive definitions supported.

WASM-language UDFs were in early stages as of 2026, with JS UDFs already shipped. The SurrealQL-only path is the most polished.

**The SurrealDB lesson:** **Persistent user-defined functions stored in the catalog** are a real productivity feature. APOC's `apoc.custom.declare*` is the gold standard; SurrealDB's `DEFINE FUNCTION` is the same idea expressed natively. The uni-db proposal's `uni.plugin.declareFunction` (§8 meta-plugin) is the analogue.

### 7.4 EdgeDB / Gel

EdgeDB (now Gel) has a formal **extensions** system: extensions like `pgvector`, `auth`, `pg_trgm` are loaded with `CREATE EXTENSION`. These are mostly thin wrappers around Postgres extensions (Gel is built on Postgres) but the user-facing model treats them as first-class Gel concepts.

### 7.5 ArangoDB

JavaScript-language UDFs (`@arangodb` Foxx framework) — register JS microservices that run inside the database. Closest analogue to Snowflake-style internal serverless. Mixed reception in production.

---

## 8. Stream / Time-Series — Brief

Covered in §4 for the DataFusion-based ones. Adjacent:

- **Apache Flink** — `RichFunction` / `ProcessFunction` are JVM-side plugins. Stateful streaming UDFs. SQL UDFs via Java/Scala/Python.
- **ksqlDB** — `CREATE FUNCTION` for UDF/UDAF/UDTF, Java only.
- **Materialize** — Rust streaming SQL DB. UDFs in development (Python plus internal Rust). Less mature than RisingWave.
- **InfluxDB Cloud / Telegraf** — Telegraf plugins are statically-compiled Go modules; very large catalog.

The stream-DB pattern that's worth highlighting: **windowed UDAFs** are harder than they look. Maintaining accumulator state across windows with watermarks, late events, retractions — the API surface for streaming aggregates is meaningfully different from batch aggregates. uni-db doesn't have streaming today but the proposal's `AggregatePluginFn` should remain agnostic enough to extend later.

---

## 9. Adjacent Extensibility — Worth Stealing From

### 9.1 eBPF — sandboxed kernel extensions

eBPF (extended Berkeley Packet Filter) is the gold standard for **sandboxed extensions inside a critical system**. Worth studying in depth because it solves the exact problem WASM plugins solve, but with stricter constraints.

The model:
- Programs compiled to eBPF bytecode (LLVM target).
- Loaded into the kernel via `bpf()` syscall.
- **Verifier** — a static analysis pass that proves termination, memory safety, no out-of-bounds access, no unbounded loops. Programs the verifier can't prove safe are rejected.
- **Maps** — typed kernel data structures plugins can read/write (hash maps, arrays, queues, ringbufs). The only persistent state available.
- **Helpers** — kernel-provided functions plugins can call. Capability-gated by program type.
- **Program types** — XDP (network), kprobe (function entry/exit), tracepoint, perf event, cgroup, sched_ext, etc. Each program type has a different set of allowed helpers and contexts.
- **CO-RE** (Compile Once, Run Everywhere) — programs reference kernel types by name + offset metadata, with BTF (BPF Type Format) handling kernel-version skew.

Rust support: **Aya** — a Rust SDK for both writing eBPF programs and the userspace loader. Production-quality.

Lessons applicable to uni-db plugins:
1. **Verifier-based safety.** WASM plugins could be statically verified for termination guarantees, memory safety properties, and resource bounds — beyond what the wasmtime sandbox enforces. The cost is the verifier complexity; the benefit is provable safety. Not in v1 but a real direction.
2. **Typed map / state primitives.** eBPF plugins don't malloc; they use typed maps. The uni-db `TxLocal<Arc<…>>` is a small step in this direction.
3. **Program-type-specific helper sets.** Different eBPF program types have different available helpers. The uni-db proposal's capability-per-plugin-kind ABI achieves something similar at a coarser grain.

### 9.2 OSGi (Java)

The oldest mature extension framework. Modules ("bundles") with versioned interfaces, declarative service registries, lifecycle (Installed → Resolved → Starting → Active → Stopping → Uninstalled), and capability-based imports. Influential on every modern plugin system; still in production use in Eclipse, IntelliJ, Vespa, and many enterprise Java apps.

What OSGi got right:
- **Semantic versioning of interfaces.** Bundle A imports `com.foo.bar` with version range `[1.2,2.0)`. Multiple versions of the same interface coexist in one JVM via classloader isolation.
- **Service registry.** Bundles register services typed by interface; bundles look up services by interface + optional filter.
- **Lifecycle.** Explicit state machine, predictable transitions.

What OSGi got wrong:
- Cognitive overhead. The classloader semantics are baroque. Setting up service tracking with `ServiceTracker` is verbose. Many "OSGi-lite" frameworks emerged (JBoss Modules, Java Platform Module System) trying to do less.
- No sandbox. OSGi-bundles run with full JVM privileges. Classloader isolation is namespace isolation, not security.

The uni-db proposal lifts OSGi's **versioned interface imports** pattern (the `AbiRange` field) and lifecycle (Loaded → Linked → Initialized → Active → Draining → Removed). It deliberately avoids the OSGi complexity.

### 9.3 VS Code — extension host

VS Code's extension model: each extension runs in a separate Node.js process (the **extension host**), communicating with the editor via RPC. The boundary keeps a misbehaving extension from crashing the editor.

Extension capabilities ("contribution points") are declared in the extension's `package.json`:
- `commands`, `keybindings`, `menus`
- `languages`, `grammars`, `snippets`
- `views`, `viewsContainers`
- `debuggers`, `taskDefinitions`
- `colors`, `iconThemes`, `themes`
- `configurationDefaults`, `configuration`

This declarative manifest is the model the uni-db proposal's `PluginManifest.provides` field follows. VS Code's contribution-points pattern is the cleanest production example of "the manifest is a typed contract about what the plugin extends."

VS Code also has **language servers** via LSP — a separate protocol for language-specific extensions. The LSP-as-extension-protocol pattern doesn't apply directly to databases but the design discipline (small protocol, language-agnostic, sandbox by separate process) is exemplary.

### 9.4 Bevy plugins (Rust game engine)

Bevy normalized the **plugin-as-trait** pattern in Rust:

```rust
pub trait Plugin {
    fn build(&self, app: &mut App);
}

app.add_plugins((DefaultPlugins, MyPlugin));
```

Compile-time only; no dynamic loading. Plugins are *registries of systems* — they register ECS systems, resources, events, schedules. The pattern is so natural that "Bevy plugin" became a community noun.

What's notable: Bevy plugins are *units of cohesion*, not *units of distribution*. They bundle several related additions (a Renderer plugin adds materials, shaders, draw systems, asset loaders). The uni-db proposal's notion of a `Plugin` calling many `Registrar` methods follows the same instinct.

### 9.5 CLAP — audio plugins

CLAP (CLever Audio Plugin) is a recent (2022) audio plugin format designed by audio-software vendors as an open alternative to VST3 / AU. Worth knowing because audio plugins solve the **hot-reload + parameter management + thread-safety** problems aggressively.

Things CLAP gets right:
- **Stable C ABI** (not C++). Plugin authors in any language can target it.
- **Capability-style "extensions"** — the host and plugin negotiate which extensions both support. Latency, MIDI, parameters, GUI, state — all are extensions, all are negotiable.
- **Hot reload** with state save/restore. The plugin saves its state to a blob, the host reloads the plugin, the plugin restores from the blob.
- **Thread annotations** — every CLAP entry point declares which thread it can be called on (main thread, audio thread, any thread). Real-time safety is explicit.
- **Parameter management** — first-class. Plugins declare parameters with units, ranges, automation behavior.

`clap-rs` is the Rust SDK; production-ready.

Lessons: **negotiable extensions are the most flexible API design.** The host doesn't have to know everything about the plugin; the plugin doesn't have to support everything the host might want. Both sides declare what they understand and use the intersection. uni-db's manifest-capabilities × granted-capabilities intersection is this same idea applied to security; CLAP applies it to functionality.

### 9.6 Browser extensions (WebExtensions / Manifest V3)

Web browser extensions are a real-world example of **capability-gated, sandboxed, multi-tier plugins running in performance-sensitive software**. Manifest V3 (Chrome 2020+, Firefox following) tightened the model:

- Manifest declares `permissions` and `host_permissions` explicitly.
- Service workers replaced background pages — extensions can't keep state in memory indefinitely.
- Content Security Policy restricts what extension code can do.
- Code must be reviewable (no `eval`, no remote code).

The Manifest V3 backlash (in particular around adblockers) is a useful study in plugin-system politics: tightening the security model can break legitimate use cases. The uni-db proposal's capability set should be tight by default but generous in what it can express (`Network { allow: vec![...] }` is more useful than a binary `Network: bool`).

---

## 10. Capability-Based Security in Plugin Systems

A short detour into the academic and systems-software lineage that underpins modern plugin security.

The capability-based security model has a long pedigree (KeyKOS, EROS, seL4, E language, Caja). The core idea: **authority is held in unforgeable references; presenting the reference is the only way to exercise the authority; no ambient authority exists.**

Modern WASM plugin systems are the most widespread practical application of this idea. The mapping:

| Capability concept       | WASM-plugin manifestation                                |
|--------------------------|----------------------------------------------------------|
| Unforgeable reference    | Host function pointer in the imports table               |
| Holding the capability   | Linker has bound the import; plugin can call             |
| Granting                 | Host adds the import to the linker config                |
| Revocation               | Detach the import (requires plugin restart in practice)  |
| Composition              | Wrap the host function in a more restrictive proxy       |
| Attenuation              | Provide a host function with a narrower interface        |

The uni-db proposal's `Capability::Network { allow: Vec<UriPattern> }` is an attenuated capability — the plugin gets *some* HTTP access, not *all* HTTP access. This is a strictly better model than binary permission grants.

The capability literature also gives us:
- **Sealer/unsealer pairs** — values can be sealed by one party and only unsealed by another. Useful for secrets: the host seals an API key into a value the plugin can pass to network calls but cannot read.
- **Membranes** — a wrapper that intercepts every access through a reference, enabling revocation and logging. The uni-db proposal's host imports for capability-gated operations are effectively a membrane.

For production plugin systems handling untrusted code, **declared capabilities + WIT-linker enforcement + attenuated host functions** is the SOTA. uni-db lands here directly.

---

## 11. Comparison Matrix

A condensed comparison of plugin systems across the categories that matter for uni-db's design. Columns are: sandbox model, hot reload, capability model, primary plugin language(s), distribution channel.

| System                          | Sandbox      | Hot reload   | Capabilities       | Plugin languages                     | Distribution        |
|---------------------------------|--------------|--------------|--------------------|--------------------------------------|---------------------|
| PostgreSQL extensions           | None         | No (restart) | superuser flag     | C, PL/*                             | PGXN, OS pkg mgr     |
| pgrx (Rust on Postgres)         | None         | No (restart) | superuser          | Rust                                | crates.io + cargo-pgrx |
| DuckDB Community Extensions     | None         | Load/unload  | binary signed flag | C++, Rust, WASM                     | community-extensions.duckdb.org |
| SQLite loadable                 | None         | Load only    | None               | C, Rust (sqlite-loadable-rs)        | manual / per-distro  |
| MySQL UDFs                      | None         | Restart      | privilege grants   | C, C++                              | manual               |
| ClickHouse executable UDFs      | OS-level     | Reconfig     | resource limits    | Any (stdin/stdout)                  | manual               |
| Trino connectors                | JVM classloader | Restart   | None               | Java                                | manual JAR install   |
| Spark DSv2                      | JVM classloader | Restart   | None               | Java, Scala                         | Maven                |
| Snowflake / BigQuery UDFs       | Managed (cloud) | Per-call  | network policies   | SQL, JS, Python, Java, Scala        | catalog DDL          |
| Neo4j APOC                      | JVM (no sandbox) | Restart  | None               | Java + APOC custom (Cypher)         | apoc-extended JAR    |
| RisingWave UDFs                 | Per-runtime  | Restart      | None               | Python, Java, Rust→WASM, JS→WASM    | DDL                  |
| InfluxDB 3 Processing Engine    | None (host)  | Live edit    | API surface only   | Python                              | DDL                  |
| SurrealDB DEFINE FUNCTION       | None         | Live edit    | FULL/NONE/WHERE    | SurrealQL                           | DDL                  |
| Weaviate modules                | None (static)| Restart      | None               | Go                                  | upstream merge       |
| VS Code extensions              | Separate process | Live      | manifest perms     | TS/JS                               | Marketplace          |
| Bevy plugins                    | None (Rust)  | No           | None               | Rust                                | crates.io            |
| eBPF programs                   | Kernel verifier | Live      | program type       | C, Rust (Aya)                       | libbpf, kernel      |
| Browser extensions (MV3)        | Service worker | Live       | manifest perms     | TS/JS, WASM                         | Web Store            |
| **Extism (host-agnostic)**      | wasmtime     | Live (reload) | host fns           | Rust, Go, JS, Python, C, …          | per-host             |
| **WASM Component Model**        | wasmtime     | Live (reload) | WIT imports       | Rust, Go, Python, JS, C/C++, …      | OCI artifacts (emerging) |
| **uni-db (proposed)**           | wasmtime + manifest gates | Live + epoch-fenced | manifest + WIT + attenuated | Rust + WASM (any CM lang) + PyO3 + Lua | manifest URL + hash pinning |

Read the proposal's loader matrix in §5.1 alongside this and the bets become clearer:
- Hot reload + sandbox + multiple plugin languages + capability gating: the proposal is in a small minority of systems offering all four.
- The closest production analogues are RisingWave (without hot reload) and Extism-based applications (with looser capability gating).
- Sandboxed *and* dynamic *and* persistent (`apoc.custom`-style) is rare and a real differentiator.

---

## 12. 2024–2026 Trend Lines

What's actually moving in the field, with citations to specific developments:

### 12.1 WASM-Component-Model maturation

WASI 0.2.0 (Jan 2024) marked the inflection. Production languages (Rust, Go, Python, JS, C#, MoonBit) have toolchains. Component composition tools (`wasm-tools compose`) stable. The async story is the remaining gap, on the WASI 0.3 roadmap.

Practical effect: **building plugin systems on the Component Model is now the conservative choice**, not the experimental one. Two years ago you'd reach for Extism by default; today you reach for the Component Model unless you specifically need Extism's polyglot host-SDK breadth.

### 12.2 The Rust-database explosion

Multiple production databases written in Rust shipped or matured: DataFusion-based InfluxDB 3, GreptimeDB, RisingWave, Databend; pgrx-based ParadeDB; embedded Lance + LanceDB; SurrealDB v2; the entire vector-DB cohort (Qdrant, Lance).

The cluster effect: extension surfaces converge. Every Rust DB shipping in 2026 has scalar UDFs, aggregate UDFs, and *some* form of multi-language plugin (Python or WASM or both). The uni-db proposal is in the mainstream of this cluster, not ahead of it.

### 12.3 Multi-language UDFs as table stakes

The list of databases supporting UDFs in more than one language is now long: Snowflake (SQL/JS/Python/Java/Scala), BigQuery (SQL/JS/Python/Java), Databricks, RisingWave (Python/Java/Rust→WASM/JS→WASM), DuckDB (Python via Python client, C++, WASM), Postgres (C, PL/pgSQL, PL/Python, PL/v8, PL/Rust via PlRust), Snowflake/BigQuery cloud functions.

The bar for a new database is *at least* SQL/expression + one general-purpose language. Two GPLs (Python + Rust→WASM) is the productive frontier.

### 12.4 The cloud-function pattern

Snowflake External Functions, BigQuery Remote Functions, and similar offerings have normalized **plugin-as-cloud-function**. The boundary is HTTPS + JSON, not FFI. The customer's serverless runtime hosts the function. Permissions are managed by the cloud provider's IAM.

This pattern doesn't translate to embedded uni-db directly, but the **boundary discipline** (typed contracts, capability negotiation, async OK because the network is involved anyway) is worth borrowing.

### 12.5 Polyglot UDFs over Arrow

Arrow's C Data Interface and Arrow IPC over linear memory have eliminated the serialization cost for cross-language UDFs:
- PyO3 + pyarrow → zero-copy Python UDFs.
- Java UDFs over Arrow Flight → near-zero copy.
- WASM UDFs over Arrow IPC linear memory → batch-amortized to near-native.

This is the technical foundation that made multi-language UDFs viable. Five years ago every UDF paid a per-row serialization tax; today the polyglot tax is per-batch and amortized.

### 12.6 Persistent user-defined extensions

`apoc.custom.declare*`, SurrealDB `DEFINE FUNCTION`, BigQuery `CREATE FUNCTION`, the InfluxDB 3 trigger system — all persist user-defined extensions in the database catalog and reactivate them on restart. This is becoming the default expectation; transient session-scoped UDFs alone are no longer enough.

The uni-db proposal's §8 meta-plugin pattern (`uni.plugin.declareFunction`) lands here.

### 12.7 Extension package management

DuckDB Community Extensions, Cargo for pgrx, Maven for Trino connectors, Marketplace for VS Code, Spack for HPC packages — every system that wanted dynamic third-party extension growth ended up needing a **package manager with signing and trust**. The pattern is consistent enough that "what's the equivalent of npm for $SYSTEM" is the right question to ask early.

The uni-db proposal defers this (§17.4 Open Questions). It will need to be answered before serious third-party extension growth.

### 12.8 Capability declaration in manifests

CLAP extensions, Bevy `Plugin::build`, Spark DSv2 `SupportsPushDown*`, Manifest V3 `permissions`, Snowflake UDF `EXTERNAL ACCESS INTEGRATIONS`, WASI Preview 2 worlds. The "declare what you need, host grants what's safe" pattern is universal in modern plugin systems.

---

## 13. Implications for uni-db

Pulling the survey back to the design choices in `docs/proposals/plugin_framework.md`. Where the proposal lands relative to the field:

### 13.1 Conservative choices (mainstream of the field)

- **WASM Component Model as the dynamic-plugin ABI.** Mainstream as of 2025. Two years ago this would have been bleeding-edge; today it's standard.
- **wasmtime as the runtime.** Universal choice across Rust DBs.
- **Capability-gated host imports.** Standard practice; tighter than rolling our own runtime checks.
- **PyO3 for Python live UDFs with PyArrow zero-copy.** What InfluxDB 3, GreptimeDB, RisingWave, and DataFusion have converged on.
- **Compile-time Rust trait registry for built-ins.** What every Rust DB does. Bevy / Polars / RisingWave pattern.
- **Procedures with streaming `RecordBatch` returns.** DataFusion-native. Trino/Spark/DuckDB all do this.
- **Pushdown negotiation traits.** Spark DSv2 pattern, lifted directly.
- **Phased hooks at parse/plan/execute/commit.** Postgres pattern, undisputed.

### 13.2 Ambitious-but-grounded choices

- **Lua scripting via piccolo-in-WASM.** Novel composition (not many systems ship a sandboxed-dynamic scripting tier). But each piece is mainstream: piccolo is production-quality, WASM-hosted Lua is the standard pattern for ML/data-tools scripting. The composition is uncommon but not unprecedented.
- **`apoc.custom.declareFunction` analogue (meta-plugin pattern).** APOC + SurrealDB show this works in production. The uni-db generalization (any plugin surface can be declared, not just functions) is an extension of the pattern but a natural one.
- **Locy aggregate refactor with Semilattice metadata.** Datalog-with-aggregates literature has explored this since the 1990s; the practical refactor is novel but the theory is mature.
- **Ephemeral Node/Edge variants for virtual entities.** APOC has shipped this for years; the proposal's `NodeIdentity::Ephemeral` is a clean Rust expression of the same idea.

### 13.3 Genuinely novel choices

- **One unified registry covering 25 surfaces with dogfooded built-ins.** Most production systems pick a smaller surface set and accept that "some built-ins are special." The uni-db invariant ("if the framework can't express a built-in, the framework is wrong") is uncommon — Postgres, DataFusion, and Spark all have surfaces their public extension APIs can't fully express. This is a stricter discipline.
- **Multi-version ABI coexistence via per-major Linker.** Few systems do this. OSGi gets close with versioned imports; most plugin systems require all plugins to use the same ABI major. The proposal commits to a stronger guarantee.
- **Hot reload with epoch-fenced cutover for *all* plugin kinds.** SurrealDB has hot-reloadable functions; InfluxDB 3 has live trigger edits. Reloading a storage backend or an index kind mid-flight is genuinely uncommon.
- **`Scope::Session` plugins.** Most systems are instance-scoped. The PyO3 REPL UDF use case forces session-scoped; the uni-db proposal makes this first-class.

### 13.4 Risks the survey surfaces

- **Distribution.** §17.4 punts on a registry. Every system that succeeded at third-party extensions (Postgres → PGXN, DuckDB → Community Extensions, VS Code → Marketplace, JetBrains plugins) eventually needed a signed package channel. uni-db will too. Plan for it.
- **Conformance burden.** With 25 surfaces, a community of plugin authors needs *very* clear conformance tests to know they're implementing the contract correctly. The proposal §16.4 conformance suite is the right idea; it will need to be substantial.
- **Documentation burden.** Postgres extensions thrive partly because the extension manual is exhaustive. uni-db will need similar — a 100+ page "uni-db plugin author's guide" minimum.
- **Async story.** The Component Model's async gap is real (§2.1). For storage backends and connectors with inherent I/O, the proposal commits to blocking semantics in v1 (§17.2 Open Questions). This is acceptable but will be a re-litigation point when WASI 0.3 ships async.
- **WASM cold-start.** Component instantiation has measurable overhead (~10–100 ms depending on the component size and complexity of imports). For short-lived UDF calls, this matters. Pre-warmed component pools per plugin are the standard mitigation; the proposal doesn't explicitly mention pooling but should.

### 13.5 Things the proposal could add

After this survey:

1. **Explicit OCI-artifact distribution path.** The Component Model community is converging on OCI as the distribution format for WASM components. uni-db should plan for `uni plugin install oci://registry.example/myplugin:1.2.3`.
2. **Pre-warmed component pools.** Per-plugin pool of pre-instantiated wasmtime `Store`s, lifted from idle to active per-call. Significant for low-latency UDF paths.
3. **Sealer/unsealer secrets pattern.** §4.9 capability `Capability::Secret { ids }` is sketched but the actual API for "host seals an API key, plugin passes it to host network calls without ever seeing the bytes" should be specified. The membrane pattern from §10 is the right model.
4. **Conformance test scaffolding.** Beyond §16.4, ship a `cargo plugin-conformance` runner as a real artifact, not just a description.
5. **Plugin observability via OpenTelemetry.** §12 covers tracing/metrics but the standard cross-language observability protocol in 2026 is OTel. The host should expose OTel exporters as capability-gated host imports so plugins can emit spans directly into the host's collector.

---

## 14. Bibliography (key references)

- **Postgres Extension Building Infrastructure** — https://www.postgresql.org/docs/current/extend-extensions.html (long-form authoritative reference for the gold-standard extension API)
- **pgrx** — https://github.com/pgcentralfoundation/pgrx (Rust-on-Postgres)
- **DuckDB Community Extensions** — https://duckdb.org/community_extensions/
- **sqlite-loadable-rs** — https://github.com/asg017/sqlite-loadable-rs
- **WASI Preview 2 announcement** — https://bytecodealliance.org/articles/WASI-0.2 (Jan 2024)
- **WebAssembly Component Model** — https://component-model.bytecodealliance.org/
- **Extism** — https://extism.org/ and https://github.com/extism/extism
- **abi_stable** — https://docs.rs/abi_stable/
- **stabby** — https://docs.rs/stabby/
- **wasmtime** — https://wasmtime.dev/
- **piccolo** — https://github.com/kyren/piccolo (pure-Rust Lua 5.4)
- **Apache DataFusion extension guide** — https://datafusion.apache.org/library-user-guide/extensions.html
- **DataFusion table providers** — https://github.com/datafusion-contrib/datafusion-table-providers
- **InfluxDB 3 Processing Engine** — https://docs.influxdata.com/influxdb3/core/process-data/
- **RisingWave UDFs** — https://docs.risingwave.com/processing/sql/udfs/
- **SurrealDB DEFINE FUNCTION** — https://surrealdb.com/docs/surrealql/statements/define/function
- **Neo4j APOC** — https://neo4j.com/labs/apoc/
- **Spark DataSources V2** — https://www.databricks.com/blog/2018/04/12/introducing-apache-spark-2-3.html (V2 announcement)
- **Trino Connector SPI** — https://trino.io/docs/current/develop/connectors.html
- **eBPF + Aya** — https://aya-rs.dev/
- **CLAP audio plugin spec** — https://github.com/free-audio/clap
- **Weaviate modules** — https://weaviate.io/developers/weaviate/modules
- **Polars plugins** — https://docs.pola.rs/user-guide/plugins/

---

**End of survey.**
