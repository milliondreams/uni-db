//! Decision-driving microbenchmark for L0 snapshot-isolation strategies.
//!
//! The SSI/OCC design needs reads to pin a consistent snapshot of the
//! `L0Buffer`. The open question
//! this bench answers: is a per-transaction snapshot affordable as a plain deep
//! clone, or must it use structural sharing? See the plan
//! `~/.claude/plans/plaan-the-prototype-phaase-tranquil-badger.md`.
//!
//! Strategies measured:
//! - **A** — std deep clone: `L0Buffer::clone` (today's behavior).
//! - **C/D** — `Arc` share: `Arc::clone` of an immutable buffer (O(1) snapshot).
//! - **B** — structural sharing: `imbl::HashMap` for the property map.
//!
//! Metrics: snapshot latency, real reader-path latency (the cost A/C/D all pay,
//! since they present a std `L0Buffer` to readers), the `imbl` per-op read/write
//! tax (the B veto signal), and per-snapshot memory via a counting allocator that
//! forwards to mimalloc. Run with `cargo bench -p uni-db --bench l0_snapshot_bench`.

use std::alloc::{GlobalAlloc, Layout};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use mimalloc::MiMalloc;
use std::hint::black_box;
use parking_lot::RwLock;

use uni_common::core::id::{Eid, Vid};
use uni_common::{Properties, Value};
use uni_store::runtime::l0_visibility::{lookup_vertex_prop, overlay_vertex_batch};
use uni_store::runtime::{L0Buffer, QueryContext};

// ----------------------------------------------------------------------------
// Counting allocator (wraps mimalloc; powers the memory report).
// ----------------------------------------------------------------------------

/// Total bytes requested from the allocator since process start.
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
/// Total bytes returned to the allocator since process start.
static DEALLOCATED: AtomicUsize = AtomicUsize::new(0);
/// Number of allocation calls (alloc/zeroed/realloc) since process start.
static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Allocator that forwards to [`MiMalloc`] while counting bytes and calls.
///
/// Wrapping mimalloc keeps the throughput of the timing benchmarks intact while
/// letting the memory report read per-operation allocation deltas. `size_bytes`
/// on `L0Buffer` is intentionally not used because it undercounts embeddings.
struct CountingAlloc;

// SAFETY: implementing `GlobalAlloc` is unsafe because callers rely on the
// standard allocator contract. We uphold it by forwarding every call verbatim
// to mimalloc (itself a sound `GlobalAlloc`) and only adding relaxed atomic
// bookkeeping, which cannot affect allocation correctness.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is a valid layout forwarded unchanged to mimalloc.
        let ptr = unsafe { MiMalloc.alloc(layout) };
        if !ptr.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr`/`layout` originate from a prior `alloc` with this layout.
        unsafe { MiMalloc.dealloc(ptr, layout) };
        DEALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: `layout` is a valid layout forwarded unchanged to mimalloc.
        let ptr = unsafe { MiMalloc.alloc_zeroed(layout) };
        if !ptr.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `ptr`/`layout` originate from a prior allocation; `new_size`
        // satisfies the `GlobalAlloc::realloc` contract. Forwarded to mimalloc.
        let new_ptr = unsafe { MiMalloc.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            ALLOCATED.fetch_add(new_size, Ordering::Relaxed);
            DEALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// A point-in-time read of the global allocator counters.
#[derive(Clone, Copy)]
struct AllocStats {
    allocated: usize,
    count: usize,
}

fn alloc_stats() -> AllocStats {
    AllocStats {
        allocated: ALLOCATED.load(Ordering::Relaxed),
        count: ALLOC_COUNT.load(Ordering::Relaxed),
    }
}

// ----------------------------------------------------------------------------
// Fixtures.
// ----------------------------------------------------------------------------

/// Property shape of a fixture's vertices.
#[derive(Clone, Copy)]
enum Shape {
    /// Three scalar properties; structural sharing has little to share.
    Scalar,
    /// A 384-dim embedding plus scalars (MiniLM-class).
    Embed384,
    /// A 768-dim embedding plus scalars (OpenAI-small-class).
    Embed768,
}

impl Shape {
    fn label(self) -> &'static str {
        match self {
            Shape::Scalar => "scalar",
            Shape::Embed384 => "embed384",
            Shape::Embed768 => "embed768",
        }
    }

    fn embed_dim(self) -> usize {
        match self {
            Shape::Scalar => 0,
            Shape::Embed384 => 384,
            Shape::Embed768 => 768,
        }
    }
}

/// `10_000` is `auto_flush_threshold` — the realistic max main-L0 size before
/// rotation; `1_000` is the common small-tx case; `50_000` is a pre-rotation
/// worst case.
const SCALES: [usize; 3] = [1_000, 10_000, 50_000];
const SHAPES: [Shape; 3] = [Shape::Scalar, Shape::Embed384, Shape::Embed768];

/// Deterministic Fibonacci-hash index for pseudo-random key selection.
fn scatter(k: usize, n: usize) -> usize {
    (k.wrapping_mul(2_654_435_761) % n) + 1
}

fn make_vertex_props(shape: Shape, i: usize) -> Properties {
    let mut props = Properties::new();
    props.insert("id".to_string(), Value::Int(i as i64));
    props.insert("name".to_string(), Value::String(format!("node_{i}")));
    let dim = shape.embed_dim();
    if dim > 0 {
        let embedding: Vec<f32> = (0..dim).map(|j| ((i * 31 + j) as f32) * 0.001).collect();
        props.insert("embedding".to_string(), Value::Vector(embedding));
    }
    props
}

/// Builds an `L0Buffer` of `n` vertices (avg degree 4) with the given shape.
fn build_l0(shape: Shape, n: usize) -> L0Buffer {
    let mut l0 = L0Buffer::new(0, None);
    let label = ["Node".to_string()];
    for i in 0..n {
        let vid = Vid::from((i as u64) + 1);
        l0.insert_vertex_with_labels(vid, make_vertex_props(shape, i), &label);
    }
    for i in 0..(2 * n) {
        let src = Vid::from(scatter(i, n) as u64);
        let dst = Vid::from(scatter(i * 7 + 3, n) as u64);
        let eid = Eid::from((i as u64) + 1);
        let mut edge_props = Properties::new();
        edge_props.insert("w".to_string(), Value::Int((i % 100) as i64));
        l0.insert_edge(src, dst, 1, eid, edge_props, Some("REL".to_string()))
            .expect("insert_edge in fixture");
    }
    l0
}

/// Builds an outer property map (`Vid -> Properties`) of `n` entries.
fn build_prop_entries(shape: Shape, n: usize) -> Vec<(Vid, Properties)> {
    (0..n)
        .map(|i| (Vid::from((i as u64) + 1), make_vertex_props(shape, i)))
        .collect()
}

// ----------------------------------------------------------------------------
// Benchmarks.
// ----------------------------------------------------------------------------

/// Strategy A (`L0Buffer::clone`) vs C/D (`Arc::clone`) snapshot latency.
fn bench_snapshot_latency(c: &mut Criterion) {
    print_memory_report_once();
    let mut group = c.benchmark_group("snapshot_latency");
    group.sample_size(10);
    for &shape in &SHAPES {
        for &scale in &SCALES {
            let id = format!("{}/{scale}", shape.label());
            let src = build_l0(shape, scale);
            group.bench_function(BenchmarkId::new("clone_std", &id), |b| {
                b.iter(|| black_box(src.clone()));
            });
            // Reuse `src` for the Arc case to avoid a second large build.
            let arc = Arc::new(src);
            group.bench_function(BenchmarkId::new("arc_clone", &id), |b| {
                b.iter(|| black_box(Arc::clone(&arc)));
            });
        }
    }
    group.finish();
}

/// Real reader-path latency on a std `L0Buffer` (the cost A/C/D all pay).
fn bench_read_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_path");
    group.sample_size(20);
    let fixtures = [
        (Shape::Scalar, 10_000_usize),
        (Shape::Embed768, 10_000_usize),
    ];
    for &(shape, n) in &fixtures {
        let ctx = QueryContext::new(Arc::new(RwLock::new(build_l0(shape, n))));
        let probe: Vec<Vid> = (0..1024).map(|k| Vid::from(scatter(k, n) as u64)).collect();

        let mut i = 0_usize;
        group.bench_function(BenchmarkId::new("lookup_vertex_prop", shape.label()), |b| {
            b.iter(|| {
                let vid = probe[i % probe.len()];
                i = i.wrapping_add(1);
                black_box(lookup_vertex_prop(vid, "name", Some(&ctx)))
            });
        });

        let batch: Vec<Vid> = (0..1000)
            .map(|k| Vid::from(scatter(k * 3 + 1, n) as u64))
            .collect();
        let mut vid_to_idx: HashMap<Vid, usize> = HashMap::with_capacity(batch.len());
        for (idx, vid) in batch.iter().enumerate() {
            vid_to_idx.insert(*vid, idx);
        }
        group.bench_function(
            BenchmarkId::new("overlay_vertex_batch_1k", shape.label()),
            |b| {
                b.iter_batched(
                    || {
                        (
                            vec![Properties::new(); batch.len()],
                            vec![false; batch.len()],
                        )
                    },
                    |(mut result, mut deleted)| {
                        overlay_vertex_batch(&vid_to_idx, &mut result, &mut deleted, Some(&ctx));
                        black_box((result, deleted));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Strategy B veto signal: `imbl::HashMap` per-op tax vs `std::HashMap`.
fn bench_map_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_ops_imbl_tax");
    group.sample_size(10);
    let n = 10_000_usize;

    // Heavy values (embed768) to expose clone savings and lookup tax.
    let heavy = build_prop_entries(Shape::Embed768, n);
    let mut std_heavy: HashMap<Vid, Properties> = HashMap::with_capacity(n);
    let mut imbl_heavy: imbl::HashMap<Vid, Properties> = imbl::HashMap::new();
    for (k, v) in &heavy {
        std_heavy.insert(*k, v.clone());
        imbl_heavy.insert(*k, v.clone());
    }
    let probe: Vec<Vid> = (0..1024).map(|k| Vid::from(scatter(k, n) as u64)).collect();

    let mut gi = 0_usize;
    group.bench_function("get/std", |b| {
        b.iter(|| {
            let k = probe[gi % probe.len()];
            gi = gi.wrapping_add(1);
            black_box(std_heavy.get(&k))
        });
    });
    let mut gj = 0_usize;
    group.bench_function("get/imbl", |b| {
        b.iter(|| {
            let k = probe[gj % probe.len()];
            gj = gj.wrapping_add(1);
            black_box(imbl_heavy.get(&k))
        });
    });

    group.bench_function("clone/std", |b| b.iter(|| black_box(std_heavy.clone())));
    group.bench_function("clone/imbl", |b| b.iter(|| black_box(imbl_heavy.clone())));

    // Insert tax on a light (scalar) base so per-iter setup stays cheap; the
    // base stays alive so imbl pays its path-copy tax on a shared structure.
    let light = build_prop_entries(Shape::Scalar, n);
    let mut std_light: HashMap<Vid, Properties> = HashMap::with_capacity(n);
    let mut imbl_light: imbl::HashMap<Vid, Properties> = imbl::HashMap::new();
    for (k, v) in &light {
        std_light.insert(*k, v.clone());
        imbl_light.insert(*k, v.clone());
    }
    let extra: Vec<Vid> = (0..1000).map(|i| Vid::from((n + i + 1) as u64)).collect();
    let small = {
        let mut p = Properties::new();
        p.insert("w".to_string(), Value::Int(1));
        p
    };
    group.bench_function("insert1k/std", |b| {
        b.iter_batched(
            || std_light.clone(),
            |mut m| {
                for k in &extra {
                    m.insert(*k, small.clone());
                }
                black_box(m);
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("insert1k/imbl", |b| {
        b.iter_batched(
            || imbl_light.clone(),
            |mut m| {
                for k in &extra {
                    m.insert(*k, small.clone());
                }
                black_box(m);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Prints the per-snapshot memory report once, before timing loops run.
///
/// Numbers are approximate single-shot allocator deltas (other threads may add
/// noise), but the contrast — deep clone allocates tens of MB across thousands
/// of allocations while `Arc::clone` allocates nothing — is unambiguous.
fn print_memory_report_once() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        eprintln!("\n=== L0 snapshot memory report (approx single-shot deltas) ===");
        eprintln!(
            "{:<20} {:>14} {:>13} {:>12} {:>11}",
            "fixture", "clone_bytes", "clone_allocs", "arc_bytes", "arc_allocs"
        );
        for (shape, n) in [
            (Shape::Scalar, 10_000_usize),
            (Shape::Embed768, 10_000_usize),
            (Shape::Embed768, 50_000_usize),
        ] {
            let src = build_l0(shape, n);

            let before = alloc_stats();
            let cloned = src.clone();
            let after = alloc_stats();
            black_box(&cloned);
            let clone_bytes = after.allocated.saturating_sub(before.allocated);
            let clone_allocs = after.count.saturating_sub(before.count);
            drop(cloned);

            let arc = Arc::new(src);
            let before_arc = alloc_stats();
            let snap = Arc::clone(&arc);
            let after_arc = alloc_stats();
            black_box(&snap);
            let arc_bytes = after_arc.allocated.saturating_sub(before_arc.allocated);
            let arc_allocs = after_arc.count.saturating_sub(before_arc.count);

            eprintln!(
                "{:<20} {:>14} {:>13} {:>12} {:>11}",
                format!("{}/{n}", shape.label()),
                clone_bytes,
                clone_allocs,
                arc_bytes,
                arc_allocs
            );
        }
        eprintln!("=== end report ===\n");
    });
}

criterion_group!(
    benches,
    bench_snapshot_latency,
    bench_read_path,
    bench_map_ops
);
criterion_main!(benches);
