// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! MUVERA — Fixed Dimensional Encoding (FDE) for multi-vector (ColBERT) retrieval.
//!
//! MUVERA (arXiv:2405.19504) maps a *multi-vector* (a set of per-token vectors,
//! the ColBERT/late-interaction representation) into ONE fixed-dimensional dense
//! vector — the FDE — such that the **inner product** of two FDEs approximates the
//! exact MaxSim score:
//!
//! ```text
//! ⟨encode_query(q), encode_doc(d)⟩ ≈ MaxSim(q, d)  (= Σ_i max_j ⟨q_i, d_j⟩)
//! ```
//!
//! This lets the fast, mature single-*vector* ANN index do first-stage retrieval
//! over a derived FDE column, with the exact MaxSim kernel
//! (`uni_query_functions::similar_to::maxsim`) re-ranking the candidates. Because the
//! approximation is an inner product, the FDE
//! ANN index must always use the **Dot** metric, independent of the metric the exact
//! re-rank uses. This holds because ColBERT tokens are L2-normalised (per-token cosine
//! equals dot).
//!
//! ## Algorithm (one *repetition*, `B = 2^k_sim` buckets)
//! - **SimHash buckets:** `k_sim` random Gaussian hyperplanes; a token's bucket id in
//!   `[0, B)` is the sign-bit pattern of its dot products with the hyperplanes.
//! - **Inner projection (optional):** project each token from `input_dim` down to
//!   `d_proj` via a random ±1/√d_proj matrix (skipped when `d_proj == 0`).
//! - **Document FDE:** each bucket holds the **centroid** (mean) of the (projected)
//!   doc tokens that fall in it; empty buckets are filled from the non-empty bucket at
//!   smallest Hamming distance on the `k_sim` bits (ties → lowest index — deterministic).
//! - **Query FDE:** each bucket holds the **sum** of the (projected) query tokens in it;
//!   no centroid, no empty-bucket filling.
//! - Repeat `reps` times with independent matrices and concatenate →
//!   `fde_dim = reps * 2^k_sim * (d_proj or input_dim)`.
//!
//! ## Determinism
//! All random matrices are derived from a persisted `seed` using a self-contained
//! SplitMix64 PRNG + Box–Muller Gaussian transform (no external RNG crate). This
//! guarantees bit-for-bit identical matrices across platforms **and binary upgrades**
//! — essential because the document FDEs are materialised at write time and the query
//! FDE is computed later (possibly after a restart/upgrade); both must use the *same*
//! transform or the inner-product approximation breaks.
//!
//! ## Parameter tuning (important)
//! FDE recall is corpus-dependent. The shipped defaults (`k_sim=4, reps=20, d_proj=16`,
//! see `uni_common::vector_index_opts`) are reasonable starting points but are **not**
//! validated for recall on any particular corpus. Higher `reps`/`k_sim` raise recall at the
//! cost of a larger `fde_dim`. Synthetic self-retrieval (an exact-match doc ranking first)
//! is robust at any setting and is NOT evidence of real recall; measure recall@k on a real
//! ColBERT corpus with `crates/uni-store/examples/multivec_recall_real.rs` and tune from
//! there. The exact MaxSim re-rank means a poor FDE only costs recall, never precision.

use serde::{Deserialize, Serialize};

/// Errors produced while building or applying an FDE transform.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FdeError {
    /// A token vector's length does not match the configured `input_dim`.
    #[error("muvera: token dimension {got} != configured input_dim {expected}")]
    DimensionMismatch { got: usize, expected: usize },

    /// The parameters are out of the supported range.
    #[error("muvera: invalid params: {0}")]
    InvalidParams(String),
}

/// Default master seed used when a MUVERA index is created without an explicit one.
/// Fixed so behaviour is reproducible across runs (golden-ratio constant, matching the
/// repo's other seeded RNG defaults).
pub const DEFAULT_FDE_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// Upper bound on `k_sim` (so `2^k_sim` buckets stays sane) and on the resulting
/// `fde_dim`, to fail fast on absurd configurations rather than allocating gigabytes.
const MAX_K_SIM: u32 = 16;
const MAX_FDE_DIM: usize = 200_000;

/// Per-axis caps on the user-supplied `reps` and `d_proj`, applied before the
/// `fde_dim = reps · 2^k_sim · proj_dim` product is formed (M-DOCUMENTED-MAGIC).
///
/// With `k_sim ≤ 16`, `reps ≤ 1024`, and `proj_dim ≤ 4096`, the product is at most
/// `2^16 · 1024 · 4096 ≈ 2.7e14`, far below `usize::MAX`, so `fde_dim` cannot overflow
/// `usize` and wrap *under* the `MAX_FDE_DIM` guard (which previously let an absurd
/// config bypass validation or panic an overflow-checked build — issue #96). The real
/// ceiling is still enforced by `MAX_FDE_DIM`; these only bound the multiplication.
const MAX_REPS: u32 = 1024;
const MAX_PROJ_DIM: u32 = 4096;

/// Parameters of an FDE transform. Persisted (via the raw fields on
/// `VectorIndexType::Muvera`) so query-time encoding reproduces document-time encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FdeParams {
    /// Number of SimHash hyperplanes per repetition; produces `2^k_sim` buckets.
    pub k_sim: u32,
    /// Number of independent repetitions concatenated into the final FDE.
    pub reps: u32,
    /// Inner-projection target dimension. `0` means "no projection" (use `input_dim`).
    pub d_proj: u32,
    /// Dimension of each input token vector (resolved from the source column at build).
    pub input_dim: u32,
    /// Master seed; all hyperplanes/projections are derived from it.
    pub seed: u64,
}

impl FdeParams {
    /// Per-bucket vector dimension (the projected dim, or `input_dim` if no projection).
    #[inline]
    pub fn proj_dim(&self) -> usize {
        if self.d_proj == 0 {
            self.input_dim as usize
        } else {
            self.d_proj as usize
        }
    }

    /// Number of buckets per repetition (`2^k_sim`).
    #[inline]
    pub fn buckets(&self) -> usize {
        // `checked_shl` guards an out-of-range `k_sim` (≥ `usize::BITS`) that a raw
        // `1usize << k_sim` would panic on; `validate` rejects `k_sim > MAX_K_SIM`
        // regardless, so this only hardens unvalidated callers.
        1usize.checked_shl(self.k_sim).unwrap_or(0)
    }

    /// Final FDE dimension: `reps * 2^k_sim * proj_dim`.
    ///
    /// Saturates to `usize::MAX` on overflow rather than panicking (M-PANIC-IS-STOP):
    /// an unvalidated caller with absurd `reps`/`d_proj` must not crash, and a saturated
    /// value cleanly trips the `dim > MAX_FDE_DIM` check in [`FdeParams::validate`].
    #[inline]
    pub fn fde_dim(&self) -> usize {
        self.buckets()
            .checked_mul(self.proj_dim())
            .and_then(|x| x.checked_mul(self.reps as usize))
            .unwrap_or(usize::MAX)
    }

    /// Validate the parameters, returning a descriptive error if unsupported.
    pub fn validate(&self) -> Result<(), FdeError> {
        if self.k_sim == 0 || self.k_sim > MAX_K_SIM {
            return Err(FdeError::InvalidParams(format!(
                "k_sim must be in 1..={MAX_K_SIM}, got {}",
                self.k_sim
            )));
        }
        if self.reps == 0 || self.reps > MAX_REPS {
            return Err(FdeError::InvalidParams(format!(
                "reps must be in 1..={MAX_REPS}, got {}",
                self.reps
            )));
        }
        if self.input_dim == 0 {
            return Err(FdeError::InvalidParams(
                "input_dim must be >= 1".to_string(),
            ));
        }
        // Bound `d_proj` before forming the `fde_dim` product so the multiplication
        // cannot overflow `usize` (issue #96). `d_proj == 0` legitimately means
        // "no projection" (use `input_dim`), so only the upper bound is checked here.
        if self.d_proj > MAX_PROJ_DIM {
            return Err(FdeError::InvalidParams(format!(
                "d_proj must be <= {MAX_PROJ_DIM}, got {}",
                self.d_proj
            )));
        }
        let dim = self.fde_dim();
        if dim == 0 || dim > MAX_FDE_DIM {
            return Err(FdeError::InvalidParams(format!(
                "fde_dim {dim} out of range (1..={MAX_FDE_DIM}); reduce k_sim/reps/d_proj"
            )));
        }
        Ok(())
    }
}

/// A minimal, fully-specified SplitMix64 PRNG. Deterministic and portable across
/// platforms and binary versions — unlike `rand`'s `StdRng`, whose algorithm is not
/// guaranteed stable. Only what the FDE encoder needs (uniform + Gaussian) is exposed.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    #[inline]
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform f64 in `[0, 1)` using the top 53 bits.
    #[inline]
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// One standard-normal sample via the Box–Muller transform (cos branch).
    #[inline]
    fn next_gaussian(&mut self) -> f32 {
        // Clamp u1 away from 0 so ln() is finite.
        let u1 = self.next_f64().max(1e-12);
        let u2 = self.next_f64();
        let r = (-2.0 * u1.ln()).sqrt();
        (r * (2.0 * std::f64::consts::PI * u2).cos()) as f32
    }
}

/// Mix a master seed with a repetition index into a distinct sub-seed, so each
/// repetition's matrices are independent (SplitMix64-style finaliser).
#[inline]
fn rep_seed(base: u64, rep: u32) -> u64 {
    let mut s = base.wrapping_add((rep as u64).wrapping_mul(0xD1B5_4A32_D192_ED03));
    s = (s ^ (s >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    s = (s ^ (s >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    s ^ (s >> 31)
}

/// Precomputed random matrices for one repetition.
struct RepMatrices {
    /// `k_sim * input_dim`, row-major (one hyperplane per row). Gaussian entries.
    hyperplanes: Vec<f32>,
    /// `d_proj * input_dim`, row-major. `±1/√d_proj` entries. `None` = no projection.
    projection: Option<Vec<f32>>,
}

impl RepMatrices {
    fn build(params: &FdeParams, rep: u32) -> Self {
        let mut rng = SplitMix64::new(rep_seed(params.seed, rep));
        let d = params.input_dim as usize;
        let hyperplanes = (0..params.k_sim as usize * d)
            .map(|_| rng.next_gaussian())
            .collect();
        let projection = if params.d_proj == 0 {
            None
        } else {
            let pd = params.d_proj as usize;
            let scale = 1.0f32 / (pd as f32).sqrt();
            // Draw the ±1 entries AFTER the hyperplanes so the draw order is fixed.
            let proj = (0..pd * d)
                .map(|_| {
                    if rng.next_u64() & 1 == 0 {
                        scale
                    } else {
                        -scale
                    }
                })
                .collect();
            Some(proj)
        };
        Self {
            hyperplanes,
            projection,
        }
    }

    /// SimHash bucket id of a (raw, `input_dim`) token: sign-bit pattern over hyperplanes.
    #[inline]
    fn bucket_of(&self, token: &[f32], k_sim: u32, d: usize) -> usize {
        let mut bucket = 0usize;
        for h in 0..k_sim as usize {
            let row = &self.hyperplanes[h * d..(h + 1) * d];
            let mut dot = 0.0f32;
            for i in 0..d {
                dot += row[i] * token[i];
            }
            if dot > 0.0 {
                bucket |= 1 << h;
            }
        }
        bucket
    }

    /// Project a raw token to `proj_dim` (identity if no projection matrix).
    #[inline]
    fn project(&self, token: &[f32], proj_dim: usize, d: usize) -> Vec<f32> {
        match &self.projection {
            None => token.to_vec(),
            Some(p) => {
                let mut out = vec![0.0f32; proj_dim];
                for (r, slot) in out.iter_mut().enumerate() {
                    let row = &p[r * d..(r + 1) * d];
                    let mut acc = 0.0f32;
                    for i in 0..d {
                        acc += row[i] * token[i];
                    }
                    *slot = acc;
                }
                out
            }
        }
    }
}

/// A reusable FDE encoder holding all repetitions' random matrices. Build it ONCE per
/// flush batch / per query (matrix generation is the expensive part) and reuse it
/// across many `encode_doc`/`encode_query` calls.
pub struct FdeEncoder {
    params: FdeParams,
    reps: Vec<RepMatrices>,
}

impl FdeEncoder {
    /// Materialise all random matrices from the seed. Validates `params`.
    pub fn new(params: &FdeParams) -> Result<Self, FdeError> {
        params.validate()?;
        let reps = (0..params.reps)
            .map(|r| RepMatrices::build(params, r))
            .collect();
        Ok(Self {
            params: params.clone(),
            reps,
        })
    }

    /// The parameters this encoder was built from.
    #[inline]
    pub fn params(&self) -> &FdeParams {
        &self.params
    }

    /// Output FDE dimension (== `self.params().fde_dim()`).
    #[inline]
    pub fn fde_dim(&self) -> usize {
        self.params.fde_dim()
    }

    fn check_tokens(&self, tokens: &[Vec<f32>]) -> Result<(), FdeError> {
        let d = self.params.input_dim as usize;
        for tok in tokens {
            if tok.len() != d {
                return Err(FdeError::DimensionMismatch {
                    got: tok.len(),
                    expected: d,
                });
            }
        }
        Ok(())
    }

    /// Encode a **document** multi-vector: per-bucket centroid + empty-bucket fill.
    pub fn encode_doc(&self, tokens: &[Vec<f32>]) -> Result<Vec<f32>, FdeError> {
        self.check_tokens(tokens)?;
        let pd = self.params.proj_dim();
        let b = self.params.buckets();
        let d = self.params.input_dim as usize;
        let mut out = vec![0.0f32; self.params.fde_dim()];

        for (ri, rep) in self.reps.iter().enumerate() {
            let base = ri * b * pd;
            let mut sums = vec![0.0f32; b * pd];
            let mut counts = vec![0u32; b];
            for tok in tokens {
                let bk = rep.bucket_of(tok, self.params.k_sim, d);
                let proj = rep.project(tok, pd, d);
                let slot = &mut sums[bk * pd..(bk + 1) * pd];
                for (s, p) in slot.iter_mut().zip(proj.iter()) {
                    *s += *p;
                }
                counts[bk] += 1;
            }
            // Centroid per non-empty bucket, written into the output region directly.
            for bk in 0..b {
                if counts[bk] > 0 {
                    let inv = 1.0f32 / counts[bk] as f32;
                    let dst = &mut out[base + bk * pd..base + (bk + 1) * pd];
                    let src = &sums[bk * pd..(bk + 1) * pd];
                    for (o, s) in dst.iter_mut().zip(src.iter()) {
                        *o = *s * inv;
                    }
                }
            }
            // fill_empty: copy the centroid of the Hamming-nearest non-empty bucket.
            for bk in 0..b {
                if counts[bk] == 0
                    && let Some(src) = nearest_nonempty(bk, &counts)
                {
                    let (lo, hi) = (bk.min(src), bk.max(src));
                    // Split to satisfy the borrow checker, then copy src→bk.
                    let (left, right) = out[base..base + b * pd].split_at_mut(hi * pd);
                    let (src_slice, dst_slice) = if bk == lo {
                        // dst (bk) is in `left`, src is in `right`
                        (&right[0..pd], &mut left[bk * pd..bk * pd + pd])
                    } else {
                        // src is in `left`, dst (bk) is in `right`
                        (&left[src * pd..src * pd + pd], &mut right[0..pd])
                    };
                    dst_slice.copy_from_slice(src_slice);
                }
            }
        }
        Ok(out)
    }

    /// Encode a **query** multi-vector: per-bucket sum, no centroid, no fill_empty.
    pub fn encode_query(&self, tokens: &[Vec<f32>]) -> Result<Vec<f32>, FdeError> {
        self.check_tokens(tokens)?;
        let pd = self.params.proj_dim();
        let b = self.params.buckets();
        let d = self.params.input_dim as usize;
        let mut out = vec![0.0f32; self.params.fde_dim()];

        for (ri, rep) in self.reps.iter().enumerate() {
            let base = ri * b * pd;
            for tok in tokens {
                let bk = rep.bucket_of(tok, self.params.k_sim, d);
                let proj = rep.project(tok, pd, d);
                let dst = &mut out[base + bk * pd..base + (bk + 1) * pd];
                for (o, p) in dst.iter_mut().zip(proj.iter()) {
                    *o += *p;
                }
            }
        }
        Ok(out)
    }
}

/// Index of the non-empty bucket at smallest Hamming distance from `bucket` (ties →
/// lowest index). `None` when every bucket is empty (an empty document).
#[inline]
fn nearest_nonempty(bucket: usize, counts: &[u32]) -> Option<usize> {
    let mut best: Option<(u32, usize)> = None;
    for (cand, &c) in counts.iter().enumerate() {
        if c > 0 {
            let h = (bucket ^ cand).count_ones();
            match best {
                Some((bh, _)) if h >= bh => {}
                _ => best = Some((h, cand)),
            }
        }
    }
    best.map(|(_, idx)| idx)
}

/// Encode a single document multi-vector (builds a transient encoder). Prefer
/// [`FdeEncoder`] when encoding many vectors with the same params.
pub fn encode_doc(tokens: &[Vec<f32>], params: &FdeParams) -> Result<Vec<f32>, FdeError> {
    FdeEncoder::new(params)?.encode_doc(tokens)
}

/// Encode a single query multi-vector (builds a transient encoder). Prefer
/// [`FdeEncoder`] when encoding many vectors with the same params.
pub fn encode_query(tokens: &[Vec<f32>], params: &FdeParams) -> Result<Vec<f32>, FdeError> {
    FdeEncoder::new(params)?.encode_query(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exact MaxSim under the dot metric: Σ_i max_j ⟨q_i, d_j⟩ (empty doc → 0). Local to
    /// the test so this foundational module stays dependency-free; the production kernel
    /// lives in `uni_query_functions::similar_to::maxsim`.
    fn maxsim_dot(query: &[Vec<f32>], doc: &[Vec<f32>]) -> f32 {
        query
            .iter()
            .map(|q| {
                if doc.is_empty() {
                    0.0
                } else {
                    doc.iter()
                        .map(|d| dot(q, d))
                        .fold(f32::NEG_INFINITY, f32::max)
                }
            })
            .sum()
    }

    /// Deterministic unit-norm random multi-vector generator (own PRNG, no rand crate).
    struct Gen(SplitMix64);
    impl Gen {
        fn new(seed: u64) -> Self {
            Self(SplitMix64::new(seed))
        }
        fn unit_token(&mut self, dim: usize) -> Vec<f32> {
            let mut v: Vec<f32> = (0..dim).map(|_| self.0.next_gaussian()).collect();
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
            for x in &mut v {
                *x /= norm;
            }
            v
        }
        fn multivec(&mut self, n: usize, dim: usize) -> Vec<Vec<f32>> {
            (0..n).map(|_| self.unit_token(dim)).collect()
        }
        fn count(&mut self, lo: usize, hi: usize) -> usize {
            lo + (self.0.next_u64() as usize) % (hi - lo + 1)
        }
    }

    fn params(k_sim: u32, reps: u32, d_proj: u32, input_dim: u32) -> FdeParams {
        FdeParams {
            k_sim,
            reps,
            d_proj,
            input_dim,
            seed: DEFAULT_FDE_SEED,
        }
    }

    fn dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| x * y).sum()
    }

    fn pearson(xs: &[f32], ys: &[f32]) -> f32 {
        let n = xs.len() as f32;
        let mx = xs.iter().sum::<f32>() / n;
        let my = ys.iter().sum::<f32>() / n;
        let mut cov = 0.0;
        let mut vx = 0.0;
        let mut vy = 0.0;
        for (x, y) in xs.iter().zip(ys) {
            let dx = x - mx;
            let dy = y - my;
            cov += dx * dy;
            vx += dx * dx;
            vy += dy * dy;
        }
        cov / (vx.sqrt() * vy.sqrt()).max(1e-12)
    }

    #[test]
    fn fde_dim_arithmetic() {
        assert_eq!(params(4, 20, 16, 96).fde_dim(), 20 * 16 * 16);
        // d_proj == 0 → use input_dim.
        assert_eq!(params(3, 2, 0, 8).fde_dim(), 2 * 8 * 8);
        assert_eq!(params(4, 20, 16, 96).buckets(), 16);
    }

    #[test]
    fn validate_rejects_bad_params() {
        assert!(params(0, 1, 0, 8).validate().is_err()); // k_sim 0
        assert!(params(MAX_K_SIM + 1, 1, 0, 8).validate().is_err());
        assert!(params(4, 0, 0, 8).validate().is_err()); // reps 0
        assert!(params(4, 1, 0, 0).validate().is_err()); // input_dim 0
        // absurd fde_dim
        assert!(params(16, 1000, 64, 96).validate().is_err());
        assert!(params(4, 20, 16, 96).validate().is_ok());
    }

    #[test]
    fn validate_rejects_overflowing_reps_and_d_proj_without_panicking() {
        // Regression for issue #96: an unbounded `reps`/`d_proj` made
        // `fde_dim = reps · 2^k_sim · proj_dim` overflow `usize`, which panicked an
        // overflow-checked build inside `validate` itself, or wrapped *under* the
        // `MAX_FDE_DIM` guard in release. The per-axis bounds + `checked_mul` must
        // reject these cleanly (an `Err`, never a panic and never an `Ok`).
        assert!(params(16, u32::MAX, u32::MAX, 96).validate().is_err());
        assert!(params(16, MAX_REPS + 1, 16, 96).validate().is_err());
        assert!(params(16, 20, MAX_PROJ_DIM + 1, 96).validate().is_err());
        // The historical wrap-bypass witness (k_sim=1) must also be rejected, not pass.
        assert!(
            params(1, 2_147_516_416, 4_294_901_761, 96)
                .validate()
                .is_err()
        );
        // `fde_dim` itself saturates instead of panicking for an unvalidated caller.
        assert_eq!(params(16, u32::MAX, u32::MAX, 96).fde_dim(), usize::MAX);
        // A `k_sim` at/above `usize::BITS` cannot panic the shift in `buckets`.
        assert_eq!(params(64, 1, 0, 8).buckets(), 0);
        // Parameters at the new ceilings still validate.
        assert!(params(16, MAX_REPS, MAX_PROJ_DIM, 96).validate().is_err()); // exceeds MAX_FDE_DIM
        assert!(params(4, MAX_REPS, 16, 96).validate().is_err()); // exceeds MAX_FDE_DIM but no overflow/panic
    }

    #[test]
    fn fde_self_retrieval_ranks_first() {
        // LOAD-BEARING correctness guard. A document queried by its OWN tokens must be
        // the FDE-dot top-1 against a corpus of other (random) documents. This is the
        // strong-signal property a faithful MUVERA estimator must satisfy and it holds
        // even on cluster-free synthetic data (where *random-pair* recall is meaningless
        // — see the project's documented "don't trust synthetic ANN recall" lesson; the
        // real recall/latency gate is the multivec_recall_real bench on ColBERT data).
        let dim = 32usize;
        let p = params(4, 20, 16, dim as u32); // minimal/default params on purpose
        let enc = FdeEncoder::new(&p).unwrap();
        let mut g = Gen::new(7);
        let corpus: Vec<Vec<Vec<f32>>> = (0..50)
            .map(|_| {
                let n = g.count(4, 16);
                g.multivec(n, dim)
            })
            .collect();
        let dfde: Vec<Vec<f32>> = corpus.iter().map(|d| enc.encode_doc(d).unwrap()).collect();
        for (j, d) in corpus.iter().enumerate() {
            let fq = enc.encode_query(d).unwrap();
            let top = (0..corpus.len())
                .max_by(|&a, &b| dot(&fq, &dfde[a]).total_cmp(&dot(&fq, &dfde[b])))
                .unwrap();
            assert_eq!(top, j, "doc {j} did not self-retrieve as FDE top-1");
        }
    }

    #[test]
    fn fde_dot_positively_correlates_with_maxsim() {
        // Regression guard: the FDE inner product must track exact MaxSim. The estimator
        // is biased (centroid < max) so over cluster-free random pairs the correlation
        // tops out well below 1.0; assert a conservative floor that a correct impl clears
        // comfortably (observed ~0.68 at these minimal params). Quality on real data is
        // the bench's job, not this unit test's.
        let dim = 32usize;
        let p = params(4, 24, 16, dim as u32);
        let enc = FdeEncoder::new(&p).unwrap();
        let mut g = Gen::new(42);

        let n_pairs = 400;
        let mut fde_scores = Vec::with_capacity(n_pairs);
        let mut exact_scores = Vec::with_capacity(n_pairs);
        for _ in 0..n_pairs {
            let (qn, dn) = (g.count(2, 6), g.count(4, 16));
            let q = g.multivec(qn, dim);
            let d = g.multivec(dn, dim);
            fde_scores.push(dot(
                &enc.encode_query(&q).unwrap(),
                &enc.encode_doc(&d).unwrap(),
            ));
            exact_scores.push(maxsim_dot(&q, &d));
        }
        let r = pearson(&fde_scores, &exact_scores);
        assert!(r >= 0.55, "FDE/MaxSim correlation regressed: {r}");
    }

    #[test]
    fn deterministic_across_rebuild() {
        // Two encoders from identical params (simulating doc-time vs query-time after a
        // restart) must produce byte-identical output.
        let p = params(4, 8, 8, 16);
        let e1 = FdeEncoder::new(&p).unwrap();
        let e2 = FdeEncoder::new(&p).unwrap();
        let mut g = Gen::new(7);
        let d = g.multivec(10, 16);
        assert_eq!(e1.encode_doc(&d).unwrap(), e2.encode_doc(&d).unwrap());
        let q = g.multivec(3, 16);
        assert_eq!(e1.encode_query(&q).unwrap(), e2.encode_query(&q).unwrap());
    }

    #[test]
    fn different_seed_changes_output() {
        let mut p = params(4, 8, 8, 16);
        let e1 = FdeEncoder::new(&p).unwrap();
        p.seed = DEFAULT_FDE_SEED ^ 0xDEAD_BEEF;
        let e2 = FdeEncoder::new(&p).unwrap();
        let mut g = Gen::new(11);
        let d = g.multivec(10, 16);
        assert_ne!(e1.encode_doc(&d).unwrap(), e2.encode_doc(&d).unwrap());
    }

    #[test]
    fn empty_doc_is_all_zero() {
        let p = params(4, 4, 8, 16);
        let enc = FdeEncoder::new(&p).unwrap();
        let fde = enc.encode_doc(&[]).unwrap();
        assert_eq!(fde.len(), p.fde_dim());
        assert!(fde.iter().all(|&x| x == 0.0));
    }

    #[test]
    fn empty_query_scores_zero() {
        let p = params(4, 4, 8, 16);
        let enc = FdeEncoder::new(&p).unwrap();
        let mut g = Gen::new(3);
        let fq = enc.encode_query(&[]).unwrap();
        let fd = enc.encode_doc(&g.multivec(8, 16)).unwrap();
        assert_eq!(dot(&fq, &fd), 0.0);
    }

    #[test]
    fn dim_mismatch_errors() {
        let p = params(4, 4, 8, 16);
        let enc = FdeEncoder::new(&p).unwrap();
        let bad = vec![vec![1.0f32; 15]]; // 15 != 16
        assert_eq!(
            enc.encode_doc(&bad),
            Err(FdeError::DimensionMismatch {
                got: 15,
                expected: 16
            })
        );
        assert!(enc.encode_query(&bad).is_err());
    }

    #[test]
    fn single_token_doc_fills_all_buckets() {
        // One token → exactly one non-empty bucket → fill_empty copies it everywhere,
        // so every per-bucket slot equals that token's projection.
        let p = params(3, 1, 0, 8); // no projection, 1 rep, 8 buckets
        let enc = FdeEncoder::new(&p).unwrap();
        let mut g = Gen::new(99);
        let tok = g.unit_token(8);
        let fde = enc.encode_doc(&[tok]).unwrap();
        let pd = p.proj_dim();
        let first = &fde[0..pd];
        for bk in 1..p.buckets() {
            assert_eq!(&fde[bk * pd..(bk + 1) * pd], first, "bucket {bk} differs");
        }
        assert!(first.iter().any(|&x| x != 0.0));
    }

    #[test]
    fn query_leaves_empty_buckets_zero() {
        // A single query token → exactly one non-empty bucket; the rest stay zero
        // (no fill_empty for queries).
        let p = params(3, 1, 0, 8);
        let enc = FdeEncoder::new(&p).unwrap();
        let mut g = Gen::new(123);
        let tok = g.unit_token(8);
        let fde = enc.encode_query(&[tok]).unwrap();
        let pd = p.proj_dim();
        let nonzero_buckets = (0..p.buckets())
            .filter(|&bk| fde[bk * pd..(bk + 1) * pd].iter().any(|&x| x != 0.0))
            .count();
        assert_eq!(nonzero_buckets, 1);
    }

    #[test]
    fn free_fns_match_encoder() {
        let p = params(4, 4, 8, 16);
        let enc = FdeEncoder::new(&p).unwrap();
        let mut g = Gen::new(55);
        let d = g.multivec(6, 16);
        assert_eq!(encode_doc(&d, &p).unwrap(), enc.encode_doc(&d).unwrap());
        let q = g.multivec(2, 16);
        assert_eq!(encode_query(&q, &p).unwrap(), enc.encode_query(&q).unwrap());
    }
}
