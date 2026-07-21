// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mode B-seq perf gate — the JIT'd-WASM arm (plugin-compute proposal §14 Q1 / Q-5).
//!
//! `crates/uni/benches/mode_b_seq_random_access.rs` established two of the three
//! arms the gate needs — the host-resident Rust baseline (`direct`) and the bare
//! JSON-ABI crossing cost (`json_abi`) — and recorded that "the JIT'd-WASM arm
//! proper is added once the WASM Mode B-seq fixture lands". This bench closes
//! that gap: it drives a *real* JIT'd component guest
//! (`examples/example-wasm-scratch`, `walk` export) over the identical
//! pointer-chase, alongside the same host baseline **in one process**, so
//! `wasm_jit_guest / direct_host_baseline` is exactly the quantity §7b's pinned
//! ≤ 10× bound is defined over.
//!
//! Both arms walk a graph the *host* builds and owns; the guest receives only an
//! opaque session id. Graph construction is excluded from every timing (criterion
//! `iter_batched` setup, and an explicit pre-timer build in the gate report), so
//! what is measured is purely the per-step random-access cost: two host crossings
//! per step (`neighbors` → pick → `get_field`).
//!
//! Run: `cargo bench -p uni-plugin-wasm --bench mode_b_seq_wasm`
//! (requires `./scripts/build-wasm-fixtures.sh` to have built the fixture).

#[cfg(not(feature = "wasmtime-runtime"))]
fn main() {
    eprintln!("mode_b_seq_wasm requires --features wasmtime-runtime (default); skipping.");
}

#[cfg(feature = "wasmtime-runtime")]
fn main() {
    gate::main();
}

#[cfg(feature = "wasmtime-runtime")]
mod gate {
    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use criterion::{BatchSize, Criterion};
    use uni_plugin_builtin::algorithms::graph_compute::scratch::{ScratchGraph, ScratchRegistry};
    use uni_plugin_builtin::algorithms::graph_compute::{Arena, WorkBudget};
    use wasmtime::component::{Component, ComponentType, Lift, Linker, Lower};
    use wasmtime::{Config, Engine, Store};
    use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

    /// Vertices in the pointer-chasing scratch graph (matches the uni bench).
    const NODES: u32 = 4_096;
    /// Random-access hops per walk (matches the uni bench).
    const STEPS: usize = 2_048;
    /// The proposal's pinned bound: JIT'd-WASM must stay within this multiple of
    /// the host-resident baseline, else B-seq needs a host-resident fast path.
    const GATE_RATIO: f64 = 10.0;
    /// Repetitions behind the printed gate verdict.
    const REPS: usize = 25;

    const WASM_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../examples/example-wasm-scratch/target/wasm32-wasip2/release/example_wasm_scratch.wasm"
    );

    // Host mirror of the guest's `host-graph` `result<string, fn-error>` error arm.
    #[derive(ComponentType, Lower, Lift)]
    #[component(record)]
    struct WasmFnError {
        code: u32,
        message: String,
        retryable: bool,
    }

    wasmtime::component::bindgen!({
        inline: "
            package uni:scratch@0.1.0;
            interface types { record fn-error { code: u32, message: string, retryable: bool } }
            interface host-graph {
                use types.{fn-error};
                graph-call: func(req: string) -> result<string, fn-error>;
                descend-batch: func(session: u64, active: list<u32>) -> result<list<u32>, fn-error>;
                visit-batch: func(session: u64, nodes: list<u32>, delta: f64) -> result<_, fn-error>;
            }
            world scratch-guest {
                import host-graph;
                export run: func(session: u64) -> string;
                export walk: func(session: u64, steps: u32) -> f64;
            }
        ",
    });

    struct HarnessState {
        registry: Arc<ScratchRegistry>,
        wasi: WasiCtx,
        table: ResourceTable,
    }

    impl WasiView for HarnessState {
        fn ctx(&mut self) -> WasiCtxView<'_> {
            WasiCtxView {
                ctx: &mut self.wasi,
                table: &mut self.table,
            }
        }
    }

    /// Ring + pseudo-random-chord adjacency, with a budget/arena generous enough
    /// that the walk never trips the meter. Identical to the uni bench's `build`.
    fn build() -> ScratchGraph {
        let mut g = ScratchGraph::new(
            WorkBudget::new(1_000_000_000),
            Arena::new(1 << 30, 1 << 24),
            0xB5E9,
        );
        for i in 0..NODES {
            g.add_node(f64::from(i)).unwrap();
        }
        for i in 0..NODES {
            g.add_edge(i, (i + 1) % NODES).unwrap();
            g.add_edge(i, (i.wrapping_mul(2_654_435_761)) % NODES)
                .unwrap();
        }
        g
    }

    /// The host-resident baseline: chases `STEPS` pointers via native accessors.
    fn walk_direct(g: &mut ScratchGraph) -> f64 {
        let mut node = 0u32;
        let mut acc = 0.0f64;
        for k in 0..STEPS {
            let nb = g.neighbors(node).unwrap();
            node = nb[k % nb.len()];
            acc += g.get_field(node).unwrap();
        }
        acc
    }

    /// The same walk driven through the JSON `host-graph` ABI *natively* — no
    /// WASM. Isolating this arm is what separates "the JSON crossing is slow"
    /// from "the JIT'd guest is slow": `json_abi / direct` is pure ABI overhead,
    /// and `wasm_jit / json_abi` is what the component call itself adds.
    fn walk_json(g: &mut ScratchGraph) -> f64 {
        let mut node = 0u32;
        let mut acc = 0.0f64;
        for k in 0..STEPS {
            let resp = g
                .call_json(&format!(r#"{{"op":"neighbors","a":{node}}}"#))
                .unwrap();
            let nb: Vec<u32> = serde_json::from_str::<serde_json::Value>(&resp)
                .ok()
                .and_then(|v| {
                    v.get("v").and_then(|l| l.as_array()).map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_u64().map(|u| u as u32))
                            .collect()
                    })
                })
                .unwrap_or_default();
            node = nb[k % nb.len()];
            let fr = g
                .call_json(&format!(r#"{{"op":"get_field","a":{node}}}"#))
                .unwrap();
            acc += serde_json::from_str::<serde_json::Value>(&fr)
                .ok()
                .and_then(|v| v.get("v").and_then(serde_json::Value::as_f64))
                .unwrap_or(0.0);
        }
        acc
    }

    /// Engine + component + linker + store + instantiated guest, wired to
    /// `registry` exactly as `scratch_wasm_e2e.rs` does.
    struct Guest {
        store: Store<HarnessState>,
        bindings: ScratchGuest,
        registry: Arc<ScratchRegistry>,
    }

    fn guest() -> Guest {
        let bytes = std::fs::read(WASM_PATH).unwrap_or_else(|e| {
            panic!(
                "missing WASM scratch fixture at {WASM_PATH}: {e}\n\
                 Run ./scripts/build-wasm-fixtures.sh"
            )
        });
        let registry = Arc::new(ScratchRegistry::new());

        let mut cfg = Config::new();
        cfg.wasm_component_model(true);
        let engine = Engine::new(&cfg).expect("engine");
        let component = Component::from_binary(&engine, &bytes).expect("component compiles");

        let mut linker: Linker<HarnessState> = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker).expect("wasi linked");
        {
            let mut hg = linker
                .instance("uni:scratch/host-graph@0.1.0")
                .expect("host-graph instance");
            hg.func_wrap(
                "graph-call",
                |store: wasmtime::StoreContextMut<'_, HarnessState>,
                 (req,): (String,)|
                 -> wasmtime::Result<(Result<String, WasmFnError>,)> {
                    Ok((Ok(store.data().registry.call_json(&req)),))
                },
            )
            .expect("host-graph graph-call linked");
            // This bench drives only `walk`, but the shared fixture *imports* the
            // typed batch ops, so every host must satisfy them or instantiation
            // fails. Missing these is what produced a cryptic wasmtime link error
            // ("`descend-batch` has the wrong type") rather than a typed denial —
            // the concrete case for additive interfaces + slice negotiation
            // (issue #152 gap 4).
            hg.func_wrap(
                "descend-batch",
                |store: wasmtime::StoreContextMut<'_, HarnessState>,
                 (session, active): (u64, Vec<u32>)|
                 -> wasmtime::Result<(Result<Vec<u32>, WasmFnError>,)> {
                    let out = store
                        .data()
                        .registry
                        .with_session(session, |g| g.descend_batch(&active));
                    Ok((match out {
                        Some(Ok(v)) => Ok(v),
                        Some(Err(e)) => Err(WasmFnError {
                            code: e.code,
                            message: e.to_string(),
                            retryable: false,
                        }),
                        None => Err(WasmFnError {
                            code: 0x860,
                            message: format!("no scratch session {session}"),
                            retryable: false,
                        }),
                    },))
                },
            )
            .expect("descend-batch linked");
            hg.func_wrap(
                "visit-batch",
                |store: wasmtime::StoreContextMut<'_, HarnessState>,
                 (session, nodes, delta): (u64, Vec<u32>, f64)|
                 -> wasmtime::Result<(Result<(), WasmFnError>,)> {
                    let out = store
                        .data()
                        .registry
                        .with_session(session, |g| g.visit_batch(&nodes, delta));
                    Ok((match out {
                        Some(Ok(())) => Ok(()),
                        Some(Err(e)) => Err(WasmFnError {
                            code: e.code,
                            message: e.to_string(),
                            retryable: false,
                        }),
                        None => Err(WasmFnError {
                            code: 0x860,
                            message: format!("no scratch session {session}"),
                            retryable: false,
                        }),
                    },))
                },
            )
            .expect("visit-batch linked");
        }

        let mut store = Store::new(
            &engine,
            HarnessState {
                registry: Arc::clone(&registry),
                wasi: WasiCtxBuilder::new().build(),
                table: ResourceTable::new(),
            },
        );
        let bindings =
            ScratchGuest::instantiate(&mut store, &component, &linker).expect("guest instantiates");
        Guest {
            store,
            bindings,
            registry,
        }
    }

    /// Times `REPS` walks per arm with graph construction excluded, then prints
    /// the ratio and the pass/fail verdict against [`GATE_RATIO`].
    fn gate_report(g: &mut Guest) {
        let mut direct = Duration::ZERO;
        for _ in 0..REPS {
            let mut sg = build();
            let t = Instant::now();
            black_box(walk_direct(&mut sg));
            direct += t.elapsed();
        }

        let mut json = Duration::ZERO;
        for _ in 0..REPS {
            let mut sg = build();
            let t = Instant::now();
            black_box(walk_json(&mut sg));
            json += t.elapsed();
        }

        let mut wasm = Duration::ZERO;
        for _ in 0..REPS {
            let sid = g.registry.open(build());
            let t = Instant::now();
            black_box(
                g.bindings
                    .call_walk(&mut g.store, sid, STEPS as u32)
                    .expect("guest walk"),
            );
            wasm += t.elapsed();
            g.registry.close(sid);
        }

        let d = direct.as_secs_f64() / REPS as f64;
        let j = json.as_secs_f64() / REPS as f64;
        let w = wasm.as_secs_f64() / REPS as f64;
        let ratio = w / d;
        let ops = (STEPS * 2) as f64; // neighbors + get_field per step
        let row = |name: &str, t: f64| {
            println!(
                "{name:<22}: {:>9.3} µs/walk  ({:>7.1} ns/op)  {:>8.1}× direct",
                t * 1e6,
                t * 1e9 / ops,
                t / d
            );
        };

        println!("\n=== Mode B-seq perf gate (proposal §14 Q1 / Q-5) ===");
        println!("steps/walk            : {STEPS} ({ops:.0} host crossings)");
        row("direct_host_baseline", d);
        row("json_abi_native", j);
        row("wasm_jit_guest", w);
        println!("ratio (wasm / direct) : {ratio:>9.2}×   (gate: ≤ {GATE_RATIO:.0}×)");
        // Decomposition: how much of the miss is the JSON crossing vs the guest?
        println!(
            "  ├─ JSON ABI share   : {:>9.2}×  (json_abi / direct)",
            j / d
        );
        println!("  └─ WASM call share  : {:>9.2}×  (wasm / json_abi)", w / j);
        // Name the dominant term rather than asserting a pre-scripted remedy:
        // if the JSON crossing alone already busts the bound, no guest technology
        // (compiled or interpreted) can meet it, and the ABI is what must change.
        let verdict = if ratio <= GATE_RATIO {
            "PASS — JIT'd-WASM random access is within budget; \
             no host-resident fast path required"
                .to_owned()
        } else if j / d > GATE_RATIO {
            format!(
                "FAIL — and the JSON ABI alone is {:.1}× (over the {GATE_RATIO:.0}× bound \
                 before any guest runs), so the crossing, not the guest, is the blocker",
                j / d
            )
        } else {
            "FAIL — the guest call, not the ABI, dominates; a host-resident \
             fast path is the indicated remedy"
                .to_owned()
        };
        println!("VERDICT               : {verdict}");
        println!("====================================================\n");
    }

    pub fn main() {
        let mut g = guest();
        // Print the gate verdict first so it is visible even under `--quick`.
        gate_report(&mut g);

        let mut c = Criterion::default().configure_from_args();
        {
            let mut group = c.benchmark_group("mode_b_seq_gate");
            group.bench_function("direct_host_baseline", |b| {
                b.iter_batched(
                    build,
                    |mut sg| black_box(walk_direct(&mut sg)),
                    BatchSize::SmallInput,
                );
            });
            group.bench_function("wasm_jit_guest", |b| {
                b.iter_batched(
                    || g.registry.open(build()),
                    |sid| {
                        let out = g
                            .bindings
                            .call_walk(&mut g.store, sid, STEPS as u32)
                            .expect("guest walk");
                        g.registry.close(sid);
                        black_box(out)
                    },
                    BatchSize::SmallInput,
                );
            });
            group.finish();
        }
        c.final_summary();
    }
}
