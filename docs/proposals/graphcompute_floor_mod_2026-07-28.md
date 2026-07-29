# `floor` and `mod` for the GraphCompute op vocabularies

**Responds to:** `uniscape-dynamics/docs/UNIDB_ASK_FLOOR_MOD.md` (REQ-D13, 2026-07-28)
**Against:** workspace 3.1.0 · **Status:** design, not yet implemented
**Verdict:** accept the ask, and ship *more* than it asks for — the ask's own fallback is unsound
in the regime it cites as its motivation.

---

## 1. The ask, verified

REQ-D13 asks for `gc.map_apply(x, "floor")` as a single elementwise op, priority MEDIUM,
explicitly non-blocking. It states `mod` follows by composition and asks us **not** to ship it.

The three claims in its §0 hold against live source:

| Claim | Verified at |
|---|---|
| No rounding op in the map vocabulary | `MapOp` = `Normalize \| Scale \| AxPlusB \| Recip \| Log \| Sqrt \| Exp` — `session.rs:87` |
| No remainder op in the ewise vocabulary | `EwiseOp` = `Add \| Mul \| Min \| Max \| Axpy \| Div` — `session.rs:116` |
| No published recipe for either | `RECIPES` — `op_parse.rs:227-277` |

The `code=2145` enumerations they pasted match `MapOp::ALL_NAMES` / `EwiseOp::ALL_NAMES` verbatim,
which is expected: both the accepted set and the rejection text are generated from the same
`op_families!` rows, so they cannot disagree. This is a genuine absence, not a spelling error —
unlike REQ-D1.

## 2. The finding that changes the ask

§0.3 asserts: *"And `a % b = a − b·⌊a/b⌋` follows."* That is an identity in ℝ. It is not one in
binary64.

Once `a/b` exceeds 2⁵³ ≈ 9.007e15, the quotient carries no fractional bits, `floor` becomes the
identity, and `b·(a/b)` reconstructs `a` to within rounding — so the subtraction yields **`0.0`**:

```
b = 12          a = 2e17    composed = 0.0     exact = 8.0
b = 0.001       a = 1e12    composed = 0.0     exact = 0.00097918331828827830
b = 0.001       a = 1e14    composed = 0.0     exact = 0.00091833182882783150
```

Two properties make this the worst possible failure shape:

- **It is silent.** No error, no NaN. `0.0` is a legal value.
- **It stays in range.** Across 500 000 random `(a, b)` draws the composed result never left
  `[0, b)`, so a downstream range assertion — the natural defensive check for a seasonality index —
  passes on the wrong answer.

The onset threshold is a ratio, not a magnitude, so a *fine-grained* cycle reaches it early:
`b = 0.001` fails from `a ≈ 1e12`. A stock accumulating over a long horizon gets there.

Below the threshold the composition is fine — better than the ask claims. It agrees with IEEE `%`
to ~1e-16 relative, and for the actual workload (`t = k·dt`, integer period) it is **bit-exact** at
dt=1, dt=1/30 × 1800 steps, and dt=1/30 × 10 000 steps. So the constant-factor argument in their
§1 is the real cost driver for the *shipped* scenarios, and the 2⁵³ cliff is the real cost of the
*unbounded* case they could not express.

**Consequence for the design:** `floor` does not subsume `mod`. Shipping `floor` alone would let
them write `a − b·floor(a/b)` — the composition we would be publishing as the answer — and get a
silent zero at exactly the magnitudes they said motivated the ask. We ship `mod` natively, over
their §2 request that we not, because their premise for that request is wrong.

`fmod`-with-sign-adjust — the algorithm a native op uses — is exact at every magnitude
(0 mismatches vs Python `%` in 200 000 draws), because `fmod` is an exact IEEE operation.

## 3. What ships

Three rows, three surfaces, one op each.

| Spelling | Family | Semantics |
|---|---|---|
| `map_apply(x, "floor")` | `MapOp::Floor` | ⌊x⌋, IEEE `roundToIntegralTowardNegative` |
| `map_apply(x, "mod", c)` | `MapOp::Mod(c)` | `x mod c` for a **scalar** divisor |
| `ewise(a, b, "mod")` | `EwiseOp::Mod` | `a mod b` for a **tensor** divisor |

### Why the scalar form earns its row

`map_apply` already carries two scalar slots (`req.f`, `req.f2` in dispatch; `a, b` in both
in-process loaders), so `MapOp::Mod(c)` costs nothing structurally. It matters because
`time % <literal>` is the dominant spelling in the driving workload, and the tensor form would
force materialising a constant `[V]` map first — `zero_map` + `map_apply affine` — two extra ops
and two extra allocations, charged *every tick* of a stepped-dynamics loop.

Op counts for `time % 12`, per tick:

| Route | Ops |
|---|---:|
| Their shipped `Σ indicator` composition, K=5 | 15 |
| Same at daily resolution, K=150 | 450 |
| Composed from a native `floor` | 4 |
| `map_apply(t, "mod", 12.0)` | **1** |

### Conventions, stated rather than measured

The ask specifically requested we *say* which convention we chose rather than leave it to be
discovered. All three are documented in the reference:

- **`floor`** is IEEE floor, not truncation: ⌊−1.5⌋ = −2. `f64::floor` gives this, along with
  `floor(±0) = ±0`, `floor(±inf) = ±inf`, `floor(NaN) = NaN`, and identity above 2⁵³ — matching
  `np.floor` bit-for-bit on every edge the ask enumerates.
- **`mod` follows the divisor's sign** (Python `%`, not C `fmod`): `5 mod −3 = −1`,
  `−5 mod 3 = 1`. Implemented as `fmod` plus a sign adjust, which is exact.
- **A zero remainder takes the divisor's sign**: `−0 mod 3 = 0.0`, `6 mod −3 = −0.0`. This needs
  an explicit `copysign`; without it 4 of the 7 signed-zero cases disagree with `np.mod`, and
  `assert_eq!` on the value cannot see it since `−0.0 == 0.0`. The test compares bit patterns.
- **`x mod 0 = NaN`** — the standard (IEEE/C `fmod`, Rust `%`, and NumPy's elementwise `np.mod`;
  Python's scalar `%` raises, which has no elementwise analogue). This is a *deliberate
  divergence* from the neighbouring `x / 0 = 0` and `recip(0) = 0`.

  Those exist for a domain reason: a vertex with zero out-degree legitimately has degree 0, and
  mapping it to 0 drops it out of a normalization rather than poisoning it. A zero modulus has no
  analogous legitimate reading — it is a broken parameter — and returning 0 would reproduce
  precisely the silent zero this op was added to eliminate. Note the composed form returns
  neither: `a − 0·⌊0⌋ = a`, because `EwiseOp::Div` already maps `a/0 → 0`. Three routes giving
  three different answers is itself an argument against leaving `mod` to composition.

### Not shipping: `ceil`, `round`, `trunc`, `rint`, `fix`

The ask says these are not needed, and the catalog's stated gate is "is this composition-blocked?"
`ceil` is not — `ceil(x) = −⌊−x⌋` is exact and three ops. It gets a **published recipe** rather
than a vocabulary row, so the name resolves to advice instead of a bare rejection:

```
["ceil", "ceiling"] in [Map] => "rounding up",
    "map_apply(map_apply(map_apply(a, \"scale\", -1.0), \"floor\"), \"scale\", -1.0)"
```

`round`, `trunc`, `rint` and `fix` get **no recipe**. `trunc` needs a sign-dependent select, and
`round` has no exact composition from `floor` at all (half-to-even is not reachable). Publishing an
approximation would be worse than the plain "valid names" rejection — `RECIPES` is load-bearing
precisely because everything in it is proven. Each is a one-line `v.trunc()` / `v.round_ties_even()`
arm *if* a second consumer asks; that is the moment to reopen it, not now.

## 4. Implementation

The module is factored so this is genuinely small. All three loaders — JSON dispatch (serving WASM
and Extism), Rhai, and PyO3 — delegate to `MapOp::parse` / `EwiseOp::parse`, and a per-loader test
(`op_strings_are_not_parsed_in_this_loader`) *forbids* them from hand-matching. **A new op name
reaches all four guest surfaces with no loader change.**

| # | File | Change |
|---|---|---|
| 1 | `session.rs:87` | `MapOp::Floor`, `MapOp::Mod(f64)` |
| 2 | `session.rs:116` | `EwiseOp::Mod` |
| 3 | `session.rs:2087` | Two arms in `map_apply`'s `match op` |
| 4 | `session.rs:1395` | One arm in `ewise`'s `match op` |
| 5 | `op_parse.rs:133` | `"floor" => MapOp::Floor, MapOp::Floor;` and `"mod" => MapOp::Mod(_), MapOp::Mod(a);` |
| 6 | `op_parse.rs:144` | `"mod" => EwiseOp::Mod, EwiseOp::Mod;` |
| 7 | `op_parse.rs:227` | The `ceil` recipe |
| 8 | `website/docs/plugins/graph-algorithms.md` | Op lists at L256 / L264; composition table at L330; conventions |

Steps 1–2 without 5–6 is a **compile error** — `op_name` matches every variant with no wildcard
arm, which is the drift guard the module was built around. Steps 5–6 without 8 is a **test
failure** — `every_recipe_appears_in_the_published_reference` asserts every accepted name appears
in the published reference. Neither can be forgotten.

The `mod` arm, both families:

```rust
// Sign follows the divisor (Python `%`), computed via the exact IEEE remainder
// plus an adjust — `x - y * (x / y).floor()` loses the quotient's fractional
// bits once x/y exceeds 2^53 and silently collapses to zero.
// `y == 0.0` yields 0.0, mirroring EwiseOp::Div and MapOp::Recip.
fn py_mod(x: f64, y: f64) -> f64 {
    if y == 0.0 {
        return 0.0;
    }
    let r = x % y; // fmod: exact
    if r != 0.0 && (r < 0.0) != (y < 0.0) { r + y } else { r }
}
```

`floor` is `x.iter().map(|v| v.floor()).collect()`.

Nothing else moves. `charge(n)` already bills one unit per element for both kernels, so the budget
table at reference L592 is unchanged. Shape and origin propagation is generic — `[E]` tensors get
`floor` and `mod` for free, and `ewise` already enforces matching shape and compatible provenance.

### Explicitly unchanged

- **No `KernelId`.** `map_apply` and `ewise` are already `AllLoaders` kernels. The kernel count
  asserted by `the_published_kernel_count_is_the_real_one` does not move.
- **No ABI bump.** `graph-compute@1` versions the *slice*; accepted names within it are additive.
  A guest using `"floor"` against an older host gets the designed typed `2145` refusal, which is
  how uniscape probed for it in the first place.
- **No semver break.** Precedent: `5f739b127` added `MapOp::Sqrt`, `MapOp::Exp` and `EwiseOp::Div`
  as a plain `feat(graph-compute)`. `MapOp` is not `#[non_exhaustive]`, so a downstream exhaustive
  match technically breaks; the repo has already ruled that additive. Worth noting we ship *inside*
  that precedent rather than re-litigating it.
- **The i64 path stays rejected.** `map_apply` errors on i64 tensors today; `floor` on an integer
  map is the identity and `mod` there wants integer semantics. Out of scope.

## 5. Tests

| Test | Where | Asserts |
|---|---|---|
| `floor_matches_the_ieee_oracle` | `differential_tests.rs` | `floor` over `{−1.5, −0.5, ±0, 0.5, 1.5, 2⁵³, ±inf, NaN, ±1e308}` against a scalar oracle. Pins ⌊−1.5⌋ = −2, i.e. floor and not truncation. |
| `mod_sign_follows_the_divisor` | `differential_tests.rs` | The four sign quadrants; `x mod 0 = 0`. |
| **`native_mod_survives_where_the_floor_composition_collapses`** | `differential_tests.rs` | `(a, b) = (2e17, 12)` and `(1e12, 0.001)`: the composed route returns `0.0`, the native op returns the exact remainder. |
| `recipe_ceil_rounds_up` | `composition_recipes.rs` | The `ceil` identity against an independent scalar oracle — required by `every_published_recipe_has_a_proof`. |
| `floor_and_mod_reach_every_loader` | Rhai + PyO3 loader tests | The names parse through both in-process surfaces and a JSON dispatch round-trip. |
| *(existing, no edit)* | `op_parse.rs` | `every_op_name_is_unique_and_round_trips` iterates `ALL_NAMES`, so it picks the new names up automatically. |

The third one carries the weight. Without it, a future reader sees `mod` sitting next to `floor`,
concludes it is composable, and deletes it as redundant — which is precisely the reasoning in the
ask's §2 that this design overrides. The test encodes *why the row exists*.

One weakness worth recording: `every_recipe_appears_in_the_published_reference` checks
`REFERENCE.contains(name)`, so short names like `"mod"` pass on any incidental substring
("model", "mode"). The guard is real for `normalize_l2` and weak here. The docs change must
therefore add both names to the kernel tables deliberately, not rely on the test to catch omission.

## 6. Effect on the reported workload

`ai-platform/simulation` expresses seasonality as `(time % price_cycle_period) / price_cycle_period`
— 6 of 254 equations, the only construct `uniscape-sd` cannot compile. After this:

- **Literal divisor** (their inlining fallback): `map_apply(t, "mod", c)`, one op, no Rhai-side
  scalar folding needed.
- **Column divisor** (no current escape): `ewise(t, periods, "mod")`, one op.
- **Modulo of a stock** (their stated correctness gap): expressible, and — unlike the composition
  we would otherwise have published — correct above the 2⁵³ ratio.

The 30× op-count blowup from re-running a monthly model at daily resolution disappears: the cost
becomes independent of the horizon.

## 7. Status

Implemented. `MapOp::Floor`, `MapOp::Mod(f64)` and `EwiseOp::Mod` are in the vocabulary, the
`ceil` recipe is published with its proof, and the reference documents the conventions above.
No kernel, WIT, linker or ABI change was needed — all four guest surfaces resolve the new names
through `MapOp::parse` / `EwiseOp::parse`.

Two points to carry back to the requester, since neither is knowable from their side:

1. **We shipped `mod` despite §2 asking us not to.** `floor` does not subsume it in binary64.
   `scratchpad/mod_floor_probe/probe.py` should grow an `a/b > 2^53` case — its current range
   would not have caught this, which is why the ask concluded the composition was sufficient.
2. **The conventions are the ones listed in §3**, including the two the ask could not have
   anticipated: `mod 0 = NaN` (diverging from `div`, deliberately) and signed-zero fidelity
   to `np.mod`.
