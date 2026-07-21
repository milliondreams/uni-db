// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Batched-descent MCTS through a **real JIT'd WASM guest** (issue #152).
//!
//! `crates/uni/benches/mcts_batched.rs` showed that batching the descent takes a
//! *native-JSON* guest from 88.8× the host baseline down to 2.8× — inside the
//! §14 Q1 ≤ 10× bound. That arm is not the answer to the gate, though: guests run
//! in WASM, and `mode_b_seq_wasm` measured the component boundary adding a
//! further 6.5× on top of JSON at per-op granularity. Extrapolating from JSON is
//! exactly the error the proposal made (it used `json_abi` as the Q-5 proxy and
//! was off by 6.5×), so this bench measures the JIT'd arm directly.
//!
//! The open question it answers: the 6.5× WASM multiplier was dominated by
//! *per-crossing fixed cost* (component call + canonical-ABI string lowering).
//! Batching removes 640× of the crossings, so that fixed cost should amortize
//! away — but "should" is a hypothesis, and this measures it.
//!
//! All arms run the identical search (complete binary tree, least-visited-child
//! descent, deterministic ties), with the host owning the tree.
//!
//! Run: `cargo bench -p uni-plugin-wasm --bench mcts_batched_wasm`

#[cfg(not(feature = "wasmtime-runtime"))]
fn main() {
    eprintln!("mcts_batched_wasm requires --features wasmtime-runtime (default); skipping.");
}

#[cfg(feature = "wasmtime-runtime")]
fn main() {
    imp::main();
}

#[cfg(feature = "wasmtime-runtime")]
mod imp {
    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use uni_plugin_builtin::algorithms::graph_compute::scratch::{ScratchGraph, ScratchRegistry};
    use uni_plugin_builtin::algorithms::graph_compute::{Arena, WorkBudget};
    use wasmtime::component::{Component, ComponentType, Lift, Linker, Lower};
    use wasmtime::{Config, Engine, Store};
    use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

    const DEPTH: u32 = 10;
    const ROLLOUTS: u32 = 4_096;
    const BATCH: u32 = 256;
    const GATE_RATIO: f64 = 10.0;
    const REPS: usize = 5;

    const WASM_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../examples/example-wasm-scratch/target/wasm32-wasip2/release/example_wasm_scratch.wasm"
    );

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
                export mcts-batched: func(session: u64, rollouts: u32, batch: u32, depth: u32) -> f64;
                export mcts-batched-typed: func(session: u64, rollouts: u32, batch: u32, depth: u32) -> f64;
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

    /// Complete binary tree of `DEPTH` levels, zeroed visit fields.
    fn build() -> ScratchGraph {
        let nodes = (1usize << (DEPTH + 1)) - 1;
        let mut g = ScratchGraph::new(
            WorkBudget::new(u64::MAX / 4),
            Arena::new(1 << 30, 1 << 24),
            0x4D43_5453,
        );
        for _ in 0..nodes {
            g.add_node(0.0).unwrap();
        }
        for p in 0..nodes {
            let (l, r) = (2 * p + 1, 2 * p + 2);
            if r < nodes {
                g.add_edge(p as u32, l as u32).unwrap();
                g.add_edge(p as u32, r as u32).unwrap();
            }
        }
        g
    }

    /// Host-resident baseline: the same search via the batched kernels called
    /// natively (no serialization, no boundary) — the floor for this shape.
    fn native(g: &mut ScratchGraph) -> f64 {
        let batches = ROLLOUTS / BATCH;
        for _ in 0..batches {
            let mut active = vec![0u32; BATCH as usize];
            for _ in 0..DEPTH {
                g.visit_batch(&active, 1.0).unwrap();
                active = g.descend_batch(&active).unwrap();
            }
        }
        g.get_field(0).unwrap()
    }

    /// The same batched search driven through the JSON ABI, natively (no WASM).
    fn batched_json(g: &mut ScratchGraph) -> f64 {
        let batches = ROLLOUTS / BATCH;
        for _ in 0..batches {
            let mut active = vec![0u32; BATCH as usize];
            for _ in 0..DEPTH {
                let list = serde_json::to_string(&active).unwrap();
                g.call_json(&format!(r#"{{"op":"visit_batch","v":{list},"f":1.0}}"#))
                    .unwrap();
                let resp = g
                    .call_json(&format!(r#"{{"op":"descend_batch","v":{list}}}"#))
                    .unwrap();
                active = serde_json::from_str::<serde_json::Value>(&resp)
                    .ok()
                    .and_then(|v| {
                        v.get("v").and_then(|l| l.as_array()).map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_u64().map(|u| u as u32))
                                .collect()
                        })
                    })
                    .unwrap_or_default();
                if active.is_empty() {
                    break;
                }
            }
        }
        g.get_field(0).unwrap()
    }

    /// Maps a `with_session` outcome onto the WIT `result<_, fn-error>` arm:
    /// an unknown session is a stale-handle error, a kernel error rides its own
    /// code, and success passes through.
    fn to_wasm_result<T>(
        session: u64,
        out: Option<Result<T, uni_plugin::errors::FnError>>,
    ) -> Result<T, WasmFnError> {
        match out {
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
        }
    }

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
            .expect("graph-call linked");
            // Typed batched ops: resolve the session once and call the kernel
            // directly — no JSON on either side of the boundary (issue #152).
            hg.func_wrap(
                "descend-batch",
                |store: wasmtime::StoreContextMut<'_, HarnessState>,
                 (session, active): (u64, Vec<u32>)|
                 -> wasmtime::Result<(Result<Vec<u32>, WasmFnError>,)> {
                    let out = store
                        .data()
                        .registry
                        .with_session(session, |g| g.descend_batch(&active));
                    Ok((to_wasm_result(session, out),))
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
                    Ok((to_wasm_result(session, out),))
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

    fn time_native(reps: usize, mut f: impl FnMut(&mut ScratchGraph) -> f64) -> Duration {
        let mut total = Duration::ZERO;
        for _ in 0..reps {
            let mut g = build();
            let t = Instant::now();
            black_box(f(&mut g));
            total += t.elapsed();
        }
        total / reps as u32
    }

    fn time_wasm(reps: usize, g: &mut Guest, typed: bool) -> Duration {
        let mut total = Duration::ZERO;
        for _ in 0..reps {
            let sid = g.registry.open(build());
            let t = Instant::now();
            let out = if typed {
                g.bindings
                    .call_mcts_batched_typed(&mut g.store, sid, ROLLOUTS, BATCH, DEPTH)
            } else {
                g.bindings
                    .call_mcts_batched(&mut g.store, sid, ROLLOUTS, BATCH, DEPTH)
            };
            black_box(out.expect("guest mcts"));
            total += t.elapsed();
            g.registry.close(sid);
        }
        total / reps as u32
    }

    /// Every arm must compute the *same* search, or the timings compare nothing.
    ///
    /// The root is visited once per rollout, so its field must equal `ROLLOUTS`
    /// in all four arms. This specifically catches a typed-import failure: the
    /// guest bails to `NaN` on error, which would otherwise look like a
    /// spectacularly fast run rather than a broken one.
    fn verify(gst: &mut Guest) {
        let expected = f64::from(ROLLOUTS);
        let mut a = build();
        let got_native = native(&mut a);
        let mut b = build();
        let got_json = batched_json(&mut b);

        let sid = gst.registry.open(build());
        let got_wasm_json = gst
            .bindings
            .call_mcts_batched(&mut gst.store, sid, ROLLOUTS, BATCH, DEPTH)
            .expect("wasm json arm");
        gst.registry.close(sid);

        let sid = gst.registry.open(build());
        let got_wasm_typed = gst
            .bindings
            .call_mcts_batched_typed(&mut gst.store, sid, ROLLOUTS, BATCH, DEPTH)
            .expect("wasm typed arm");
        gst.registry.close(sid);

        for (name, got) in [
            ("native_kernels", got_native),
            ("batched_json", got_json),
            ("batched_wasm_json", got_wasm_json),
            ("batched_wasm_typed", got_wasm_typed),
        ] {
            assert!(
                (got - expected).abs() < 1e-9,
                "{name} computed a different search: root visits {got}, expected {expected} \
                 (a NaN here means the guest's host calls failed and it did no work)"
            );
        }
        println!("correctness: all 4 arms agree, root visits = {expected}");
    }

    pub fn main() {
        let mut gst = guest();
        verify(&mut gst);
        let n = time_native(REPS, native).as_secs_f64();
        let j = time_native(REPS, batched_json).as_secs_f64();
        let w = time_wasm(REPS, &mut gst, false).as_secs_f64();
        let ty = time_wasm(REPS, &mut gst, true).as_secs_f64();
        let crossings = (ROLLOUTS / BATCH) * DEPTH * 2;
        let rps = |t: f64| f64::from(ROLLOUTS) / t;

        println!("\n=== Batched-descent MCTS through a JIT'd WASM guest (#152) ===");
        println!(
            "tree: complete binary depth {DEPTH} ({} nodes)   rollouts {ROLLOUTS}   batch {BATCH}",
            (1u32 << (DEPTH + 1)) - 1
        );
        println!("crossings (batched shape): {crossings}\n");
        let row = |name: &str, t: f64| {
            println!(
                "{name:<22}: {:>9.3} ms   {:>7.2}× native   {:>10.0} rollouts/s",
                t * 1e3,
                t / n,
                rps(t)
            );
        };
        row("native_kernels", n);
        row("batched_json", j);
        row("batched_wasm_json", w);
        row("batched_wasm_typed", ty);
        println!(
            "\n  JSON-string ABI cost : {:>7.2}×  (wasm_json / wasm_typed)",
            w / ty
        );
        println!("  typed vs native      : {:>7.2}×", ty / n);
        println!(
            "\nGATE (≤ {GATE_RATIO:.0}× native): json = {:.2}× → {} | typed = {:.2}× → {}",
            w / n,
            if w / n <= GATE_RATIO { "PASS" } else { "FAIL" },
            ty / n,
            if ty / n <= GATE_RATIO { "PASS" } else { "FAIL" }
        );
        println!("==============================================================\n");
    }
}
