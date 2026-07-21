// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Batched-descent MCTS authored in **Rhai** (issue #152).
//!
//! `mcts_batched_wasm` established that coarse kernels + a typed ABI put a JIT'd
//! WASM guest at 1.27× of native in-process Rust. This bench asks the question
//! #152 actually poses: can the same algorithm be authored in **Rhai** — the
//! interpreted loader that `scratch.rs::require_compiled_body` disqualifies with
//!
//! > "Mode B-seq requires a compiled body (WASM/Rust); an interpreted per-step
//! > body (Rhai row-mode) is disqualified on perf"
//!
//! That verdict was reached against *per-step* interpretation. At batch
//! granularity the script executes ~320 loop iterations instead of ~41,000
//! host round trips, and Rhai has a structural advantage over WASM here: it runs
//! in-process, so there is **no component boundary and no canonical-ABI copy at
//! all** — only interpreter overhead and `Dynamic` marshalling of the batch.
//!
//! Identical search to the WASM bench (complete binary tree, least-visited-child
//! descent, deterministic ties), so the numbers are directly comparable.
//!
//! Run: `cargo bench -p uni-plugin-rhai --bench mcts_batched_rhai`

#[cfg(not(feature = "rhai-runtime"))]
fn main() {
    eprintln!("mcts_batched_rhai requires --features rhai-runtime (default); skipping.");
}

#[cfg(feature = "rhai-runtime")]
fn main() {
    imp::main();
}

#[cfg(feature = "rhai-runtime")]
mod imp {
    use std::hint::black_box;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use parking_lot::Mutex;
    use rhai::{AST, Array, Dynamic, Engine, EvalAltResult, Scope};
    use uni_plugin_builtin::algorithms::graph_compute::scratch::{DescentPolicy, ScratchGraph};
    use uni_plugin_builtin::algorithms::graph_compute::{Arena, WorkBudget};

    /// The guest's exploration constant and virtual loss (its policy knobs).
    const UCB_C: f64 = 1.4;
    const UCB_VLOSS: f64 = 1.0;

    const DEPTH: i64 = 10;
    const ROLLOUTS: i64 = 4_096;
    const BATCH: i64 = 256;
    const REPS: usize = 5;

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

    /// Host-resident baseline: the batched kernels called directly.
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

    /// The Rhai-facing handle, mirroring `graph_compute::GcSession`: a shared
    /// session behind a mutex, with each registered fn locking internally.
    #[derive(Clone)]
    struct RhaiScratch {
        g: Arc<Mutex<ScratchGraph>>,
    }

    fn rt(e: uni_plugin::errors::FnError) -> Box<EvalAltResult> {
        format!("scratch: {e}").into()
    }

    impl RhaiScratch {
        /// `Array` → `Vec<u32>` → kernel → `Array`. The `Dynamic` marshalling of
        /// the batch is Rhai's analogue of the WASM canonical-ABI copy.
        fn descend_batch(&mut self, active: Array) -> Result<Array, Box<EvalAltResult>> {
            let slots: Vec<u32> = active
                .iter()
                .map(|d| d.as_int().unwrap_or(0) as u32)
                .collect();
            let out = self.g.lock().descend_batch(&slots).map_err(rt)?;
            Ok(out
                .into_iter()
                .map(|s| Dynamic::from(i64::from(s)))
                .collect())
        }

        fn visit_batch(&mut self, nodes: Array, delta: f64) -> Result<(), Box<EvalAltResult>> {
            let slots: Vec<u32> = nodes
                .iter()
                .map(|d| d.as_int().unwrap_or(0) as u32)
                .collect();
            self.g.lock().visit_batch(&slots, delta).map_err(rt)
        }

        fn root(&mut self) -> f64 {
            self.g.lock().get_field(0).unwrap_or(f64::NAN)
        }

        // --- Handle-passing surface (design P2): only an i64 crosses. ---

        fn batch_new(
            &mut self,
            len: i64,
            fill: i64,
            delta: f64,
        ) -> Result<i64, Box<EvalAltResult>> {
            self.g
                .lock()
                .batch_new(len as usize, fill as u32, delta)
                .map(i64::from)
                .map_err(rt)
        }

        /// One level for the whole active set; the batch never leaves the host.
        fn advance_batch(&mut self, h: i64, delta: f64) -> Result<(), Box<EvalAltResult>> {
            self.g
                .lock()
                .advance_batch(h as u32, delta, false)
                .map_err(rt)
        }

        // --- Guest-authored policy surface (issue #152 gap 6) ---

        fn column_new(&mut self) -> Result<i64, Box<EvalAltResult>> {
            self.g.lock().column_new().map(i64::from).map_err(rt)
        }

        fn col_fill(&mut self, h: i64, v: f64) -> Result<(), Box<EvalAltResult>> {
            self.g.lock().col_fill(h as u32, v).map_err(rt)
        }

        fn col_load_visits(&mut self, h: i64) -> Result<(), Box<EvalAltResult>> {
            self.g.lock().col_load_visits(h as u32).map_err(rt)
        }

        fn col_gather_parent(&mut self, d: i64, s: i64) -> Result<(), Box<EvalAltResult>> {
            self.g
                .lock()
                .col_gather_parent(d as u32, s as u32)
                .map_err(rt)
        }

        fn col_map(
            &mut self,
            d: i64,
            s: i64,
            op: rhai::ImmutableString,
            scale: f64,
            shift: f64,
        ) -> Result<(), Box<EvalAltResult>> {
            self.g
                .lock()
                .col_map(d as u32, s as u32, op.as_str(), scale, shift)
                .map_err(rt)
        }

        fn col_ewise(
            &mut self,
            d: i64,
            a: i64,
            b: i64,
            op: rhai::ImmutableString,
        ) -> Result<(), Box<EvalAltResult>> {
            self.g
                .lock()
                .col_ewise(d as u32, a as u32, b as u32, op.as_str())
                .map_err(rt)
        }

        /// Descend by the guest's own score column.
        fn advance_scored(
            &mut self,
            h: i64,
            delta: f64,
            score: i64,
            vloss: f64,
        ) -> Result<(), Box<EvalAltResult>> {
            self.g
                .lock()
                .advance_batch_scored(
                    h as u32,
                    delta,
                    DescentPolicy {
                        score_col: Some(score as u32),
                        maximize: true,
                        vloss,
                        stale: false,
                    },
                )
                .map_err(rt)
        }

        /// The *same* advance step, but the batch crosses as a Rhai `Array`.
        /// The only difference from `advance_batch` is the marshalling, which is
        /// precisely what the gap-1 experiment isolates.
        fn advance_array(
            &mut self,
            active: Array,
            delta: f64,
        ) -> Result<Array, Box<EvalAltResult>> {
            let slots: Vec<u32> = active
                .iter()
                .map(|d| d.as_int().unwrap_or(0) as u32)
                .collect();
            let out = self
                .g
                .lock()
                .advance_array(&slots, delta, false)
                .map_err(rt)?;
            Ok(out
                .into_iter()
                .map(|s| Dynamic::from(i64::from(s)))
                .collect())
        }
    }

    /// The guest program: a conductor loop over batches and levels.
    const SCRIPT: &str = r"
fn mcts(gc, rollouts, batch, depth) {
    let batches = rollouts / batch;
    for b in 0..batches {
        let active = [];
        for i in 0..batch { active.push(0); }
        for d in 0..depth {
            gc.visit_batch(active, 1.0);
            active = gc.descend_batch(active);
        }
    }
    gc.root()
}
";

    /// Array-passing, using the *fused* advance so it is byte-identical work to
    /// the handle script — isolating marshalling as the only variable.
    const SCRIPT_ARRAYS: &str = r"
fn mcts(gc, rollouts, batch, depth) {
    let batches = rollouts / batch;
    for b in 0..batches {
        let active = [];
        for i in 0..batch { active.push(0); }
        gc.visit_batch(active, 1.0);
        for d in 0..depth {
            active = gc.advance_array(active, 1.0);
        }
    }
    gc.root()
}
";

    /// Handle-passing: the active set is host-resident and the script only ever
    /// names it by `i64`. No `Dynamic` boxing of batch data at all.
    const SCRIPT_HANDLES: &str = r"
fn mcts(gc, rollouts, batch, depth) {
    let batches = rollouts / batch;
    for b in 0..batches {
        let h = gc.batch_new(batch, 0, 1.0);
        for d in 0..depth {
            gc.advance_batch(h, 1.0);
        }
    }
    gc.root()
}
";

    /// The guest's *own* UCB1 policy, composed from column primitives:
    ///
    /// ```text
    /// ucb[child] = w[child] + c * sqrt( ln(visits[parent]) / visits[child] )
    /// ```
    ///
    /// The exploration constant, the formula and the reward term all belong to
    /// the guest; the host only descends by the resulting column.
    const SCRIPT_UCB: &str = r#"
fn mcts(gc, rollouts, batch, depth, c, vloss) {
    let visits = gc.column_new();
    let pv     = gc.column_new();
    let tmp    = gc.column_new();
    let ucb    = gc.column_new();
    let w      = gc.column_new();
    gc.col_fill(w, 0.5);
    let batches = rollouts / batch;
    for b in 0..batches {
        let h = gc.batch_new(batch, 0, 1.0);
        for d in 0..depth {
            gc.col_load_visits(visits);
            gc.col_gather_parent(pv, visits);
            gc.col_map(pv, pv, "ln", 1.0, 0.0);
            gc.col_map(tmp, visits, "recip", 1.0, 0.0);
            gc.col_ewise(tmp, pv, tmp, "mul");
            gc.col_map(tmp, tmp, "sqrt", c, 0.0);
            gc.col_ewise(ucb, w, tmp, "add");
            gc.advance_scored(h, 1.0, ucb, vloss);
        }
    }
    gc.root()
}
"#;

    /// Same policy, recomputed once per *batch* rather than per level — cheaper,
    /// staler. Quantifies what the O(N) score recompute is costing.
    const SCRIPT_UCB_BATCH: &str = r#"
fn mcts(gc, rollouts, batch, depth, c, vloss) {
    let visits = gc.column_new();
    let pv     = gc.column_new();
    let tmp    = gc.column_new();
    let ucb    = gc.column_new();
    let w      = gc.column_new();
    gc.col_fill(w, 0.5);
    let batches = rollouts / batch;
    for b in 0..batches {
        let h = gc.batch_new(batch, 0, 1.0);
        gc.col_load_visits(visits);
        gc.col_gather_parent(pv, visits);
        gc.col_map(pv, pv, "ln", 1.0, 0.0);
        gc.col_map(tmp, visits, "recip", 1.0, 0.0);
        gc.col_ewise(tmp, pv, tmp, "mul");
        gc.col_map(tmp, tmp, "sqrt", c, 0.0);
        gc.col_ewise(ucb, w, tmp, "add");
        for d in 0..depth {
            gc.advance_scored(h, 1.0, ucb, vloss);
        }
    }
    gc.root()
}
"#;

    struct Scripts {
        two_call: AST,
        fused_array: AST,
        handles: AST,
        ucb: AST,
        ucb_batch: AST,
    }

    /// A complete binary tree built with `add_child`, so parent links exist and a
    /// guest policy can gather `visits[parent]` (UCB's exploration term).
    fn build_parented() -> ScratchGraph {
        let nodes = (1usize << (DEPTH + 1)) - 1;
        let mut g = ScratchGraph::new(
            WorkBudget::new(u64::MAX / 4),
            Arena::new(1 << 30, 1 << 24),
            0x4D43_5453,
        );
        g.add_node(0.0).unwrap(); // root = slot 0
        for p in 0..nodes {
            if 2 * p + 2 < nodes {
                g.add_child(p as u32, 0.0).unwrap();
                g.add_child(p as u32, 0.0).unwrap();
            }
        }
        g
    }

    /// The *same* UCB policy, composed from the same column primitives, but
    /// driven from Rust. Isolates the Rhai interpreter's share of the cost from
    /// the policy computation's share.
    fn native_ucb(g: &mut ScratchGraph, per_level: bool) -> f64 {
        let (visits, pv, tmp, ucb, w) = (
            g.column_new().unwrap(),
            g.column_new().unwrap(),
            g.column_new().unwrap(),
            g.column_new().unwrap(),
            g.column_new().unwrap(),
        );
        g.col_fill(w, 0.5).unwrap();
        let score = |g: &mut ScratchGraph| {
            g.col_load_visits(visits).unwrap();
            g.col_gather_parent(pv, visits).unwrap();
            g.col_map(pv, pv, "ln", 1.0, 0.0).unwrap();
            g.col_map(tmp, visits, "recip", 1.0, 0.0).unwrap();
            g.col_ewise(tmp, pv, tmp, "mul").unwrap();
            g.col_map(tmp, tmp, "sqrt", UCB_C, 0.0).unwrap();
            g.col_ewise(ucb, w, tmp, "add").unwrap();
        };
        let policy = DescentPolicy {
            score_col: Some(ucb),
            maximize: true,
            vloss: UCB_VLOSS,
            stale: false,
        };
        for _ in 0..(ROLLOUTS / BATCH) {
            let h = g.batch_new(BATCH as usize, 0, 1.0).unwrap();
            if !per_level {
                score(g);
            }
            for _ in 0..DEPTH {
                if per_level {
                    score(g);
                }
                g.advance_batch_scored(h, 1.0, policy).unwrap();
            }
        }
        g.get_field(0).unwrap()
    }

    fn run_rhai_ucb(eng: &Engine, ast: &AST, g: ScratchGraph) -> f64 {
        let gc = RhaiScratch {
            g: Arc::new(Mutex::new(g)),
        };
        let mut scope = Scope::new();
        eng.call_fn::<f64>(
            &mut scope,
            ast,
            "mcts",
            (gc, ROLLOUTS, BATCH, DEPTH, UCB_C, UCB_VLOSS),
        )
        .expect("rhai ucb mcts")
    }

    fn engine() -> (Engine, Scripts) {
        let mut eng = Engine::new();
        eng.register_type_with_name::<RhaiScratch>("GcScratch")
            .register_fn("visit_batch", RhaiScratch::visit_batch)
            .register_fn("descend_batch", RhaiScratch::descend_batch)
            .register_fn("advance_array", RhaiScratch::advance_array)
            .register_fn("batch_new", RhaiScratch::batch_new)
            .register_fn("advance_batch", RhaiScratch::advance_batch)
            .register_fn("column_new", RhaiScratch::column_new)
            .register_fn("col_fill", RhaiScratch::col_fill)
            .register_fn("col_load_visits", RhaiScratch::col_load_visits)
            .register_fn("col_gather_parent", RhaiScratch::col_gather_parent)
            .register_fn("col_map", RhaiScratch::col_map)
            .register_fn("col_ewise", RhaiScratch::col_ewise)
            .register_fn("advance_scored", RhaiScratch::advance_scored)
            .register_fn("root", RhaiScratch::root);
        let s = Scripts {
            two_call: eng.compile(SCRIPT).expect("script compiles"),
            fused_array: eng.compile(SCRIPT_ARRAYS).expect("array script compiles"),
            handles: eng.compile(SCRIPT_HANDLES).expect("handle script compiles"),
            ucb: eng.compile(SCRIPT_UCB).expect("ucb script compiles"),
            ucb_batch: eng
                .compile(SCRIPT_UCB_BATCH)
                .expect("ucb batch script compiles"),
        };
        (eng, s)
    }

    fn run_rhai(eng: &Engine, ast: &AST, g: ScratchGraph) -> f64 {
        let gc = RhaiScratch {
            g: Arc::new(Mutex::new(g)),
        };
        let mut scope = Scope::new();
        eng.call_fn::<f64>(&mut scope, ast, "mcts", (gc, ROLLOUTS, BATCH, DEPTH))
            .expect("rhai mcts")
    }

    fn time_native(reps: usize) -> Duration {
        let mut total = Duration::ZERO;
        for _ in 0..reps {
            let mut g = build();
            let t = Instant::now();
            black_box(native(&mut g));
            total += t.elapsed();
        }
        total / reps as u32
    }

    fn time_rhai(reps: usize, eng: &Engine, ast: &AST) -> Duration {
        let mut total = Duration::ZERO;
        for _ in 0..reps {
            let g = build();
            let t = Instant::now();
            black_box(run_rhai(eng, ast, g));
            total += t.elapsed();
        }
        total / reps as u32
    }

    /// Runs the batched search natively at a given batch width and staleness,
    /// returning the finished graph for inspection (§10 gap 3).
    fn run_batched_native(batch: usize, stale: bool) -> ScratchGraph {
        let mut g = build();
        let batches = (ROLLOUTS as usize) / batch;
        for _ in 0..batches {
            let h = g.batch_new(batch, 0, 1.0).expect("batch_new");
            for _ in 0..DEPTH {
                g.advance_batch(h, 1.0, stale).expect("advance");
            }
        }
        g
    }

    /// How many distinct leaves the search actually reached — the diversity
    /// proxy for search quality.
    fn leaves_touched(g: &mut ScratchGraph) -> usize {
        let nodes = (1usize << (DEPTH + 1)) - 1;
        let first_leaf = (1usize << DEPTH) - 1;
        (first_leaf..nodes)
            .filter(|&s| g.get_field(s as u32).is_ok_and(|v| v > 0.0))
            .count()
    }

    pub fn main() {
        let (eng, scripts) = engine();
        let expected = ROLLOUTS as f64;
        let rps = |t: f64| ROLLOUTS as f64 / t;

        // Every arm must compute the same search, or the timings compare nothing.
        let mut a = build();
        let got_native = native(&mut a);
        for (name, ast) in [
            ("two_call", &scripts.two_call),
            ("fused_array", &scripts.fused_array),
            ("handles", &scripts.handles),
        ] {
            let got = run_rhai(&eng, ast, build());
            assert!(
                (got - expected).abs() < 1e-9,
                "{name} disagrees: root visits {got}, expected {expected}"
            );
        }
        assert!((got_native - expected).abs() < 1e-9);
        println!("correctness: native + all 3 Rhai arms agree, root visits = {expected}");

        let n = time_native(REPS).as_secs_f64();
        let t2 = time_rhai(REPS, &eng, &scripts.two_call).as_secs_f64();
        let tf = time_rhai(REPS, &eng, &scripts.fused_array).as_secs_f64();
        let th = time_rhai(REPS, &eng, &scripts.handles).as_secs_f64();

        println!("\n=== GAP 1: does handle-passing close the Rhai gap? ===");
        println!("tree: complete binary depth {DEPTH}   rollouts {ROLLOUTS}   batch {BATCH}\n");
        let row = |name: &str, t: f64| {
            println!(
                "{name:<26}: {:>9.3} ms   {:>7.2}× native   {:>10.0} rollouts/s",
                t * 1e3,
                t / n,
                rps(t)
            );
        };
        row("native_kernels", n);
        row("rhai_arrays_2call", t2);
        row("rhai_arrays_fused", tf);
        row("rhai_handles", th);
        println!(
            "\n  marshalling cost   : {:>7.2}×  (fused_array / handles, identical work)",
            tf / th
        );
        println!(
            "  PREDICTION {}: handle-passing brings Rhai to {:.2}× native",
            if th / n < 3.0 { "CONFIRMED" } else { "NOT MET" },
            th / n
        );

        println!("\n=== GAP 3: what does batching cost the search? ===");
        let leaves_total = 1usize << DEPTH;
        let mut seq = run_batched_native(1, false);
        let mut stale = run_batched_native(BATCH as usize, true);
        let mut vloss = run_batched_native(BATCH as usize, false);
        let (l_seq, l_stale, l_vl) = (
            leaves_touched(&mut seq),
            leaves_touched(&mut stale),
            leaves_touched(&mut vloss),
        );
        println!("distinct leaves reached (of {leaves_total}), same {ROLLOUTS} rollouts:");
        println!("  sequential (batch=1)      : {l_seq:>6}");
        println!("  batched stale (batch={BATCH}) : {l_stale:>6}   <- naive lock-step");
        println!("  batched virtual-loss      : {l_vl:>6}   <- visit applied in-loop");

        // ---- GAP 6: can the guest own the policy? ----
        println!("\n=== GAP 6: guest-authored policy (UCB1) ===");

        // The guest's UCB and the same UCB driven from Rust must agree exactly,
        // or the policy is not actually being applied.
        let mut a = build_parented();
        let native_val = native_ucb(&mut a, true);
        let guest_val = run_rhai_ucb(&eng, &scripts.ucb, build_parented());
        assert!(
            (native_val - guest_val).abs() < 1e-9,
            "guest UCB {guest_val} != native UCB {native_val}"
        );
        // Re-run the guest into a graph we retain, so diversity is measured on
        // the guest's own result rather than inferred from the native twin.
        let shared = Arc::new(Mutex::new(build_parented()));
        {
            let gc = RhaiScratch {
                g: Arc::clone(&shared),
            };
            let mut scope = Scope::new();
            eng.call_fn::<f64>(
                &mut scope,
                &scripts.ucb,
                "mcts",
                (gc, ROLLOUTS, BATCH, DEPTH, UCB_C, UCB_VLOSS),
            )
            .expect("ucb");
        }
        println!("correctness: guest UCB == native UCB (root visits {guest_val})");

        let tn = time_native(REPS).as_secs_f64();
        let tnu = {
            let mut total = Duration::ZERO;
            for _ in 0..REPS {
                let mut g = build_parented();
                let t = Instant::now();
                black_box(native_ucb(&mut g, true));
                total += t.elapsed();
            }
            (total / REPS as u32).as_secs_f64()
        };
        let tgu = {
            let mut total = Duration::ZERO;
            for _ in 0..REPS {
                let g = build_parented();
                let t = Instant::now();
                black_box(run_rhai_ucb(&eng, &scripts.ucb, g));
                total += t.elapsed();
            }
            (total / REPS as u32).as_secs_f64()
        };
        let tgb = {
            let mut total = Duration::ZERO;
            for _ in 0..REPS {
                let g = build_parented();
                let t = Instant::now();
                black_box(run_rhai_ucb(&eng, &scripts.ucb_batch, g));
                total += t.elapsed();
            }
            (total / REPS as u32).as_secs_f64()
        };

        let prow = |name: &str, t: f64| {
            println!(
                "{name:<26}: {:>9.3} ms   {:>7.2}× hardcoded   {:>10.0} rollouts/s",
                t * 1e3,
                t / tn,
                rps(t)
            );
        };
        println!();
        prow("hardcoded_policy", tn);
        prow("native_ucb_per_level", tnu);
        prow("guest_ucb_per_level", tgu);
        prow("guest_ucb_per_batch", tgb);
        println!(
            "\n  rhai interpreter share : {:>7.2}×  (guest / native, same policy)",
            tgu / tnu
        );
        println!(
            "  policy recompute cost  : {:>7.2}×  (per_level / per_batch)",
            tgu / tgb
        );
        println!(
            "  guest UCB leaves       : {:>6} of {leaves_total}  (does guest scoring keep diversity?)",
            leaves_touched(&mut shared.lock())
        );
        println!("====================================================\n");
    }
}
