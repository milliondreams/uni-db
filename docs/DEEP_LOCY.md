# Deep Locy: Probabilistic & Neural Reasoning for Uni

## Status: Design Specification with Implementation Gap Analysis

**Version:** 1.0 — March 2026

**Companion Documents:**
- [Probabilistic Reasoning Spec v1.1](../uni-locy-docs/locy_probabilistic_reasoning_spec_v1.1.docx.md) — MNOR, MPROD, PROB complement, proof-aware inference
- [Neural Predicates Addendum](../uni-locy-docs/locy_neural_predicates_addendum.docx.md) — CREATE MODEL, calibration, training hooks
- [Semantic Stress Corpus](../uni-locy-docs/locy_semantic_stress_corpus.docx.md) — 24 adversarial edge cases
- [similar_to() Spec](../uni-locy-docs/similar_to_spec.docx.md) — Unified similarity expression function

---

## 1. What Locy Is Today

Locy is Uni's Datalog-inspired logic programming layer, operating over Uni's property graph. It provides recursive rules with fixpoint evaluation, stratified negation, path-carried accumulation, aggregation, witness selection, hypothetical reasoning, abductive inference, and proof traces.

### 1.1 Implemented Feature Matrix

| Feature | Parser | Compiler | Runtime | TCK Tests |
|---------|:------:|:--------:|:-------:|:---------:|
| CREATE RULE (multi-clause, overloaded) | Done | Done | Done | Done |
| Recursive rules with semi-naive fixpoint | Done | Done | Done | Done |
| IS references (positive) | Done | Done | Done | Done |
| IS NOT (stratified negation, Boolean) | Done | Done | Done | Done |
| FOLD (SUM, COUNT, MIN, MAX, AVG, COLLECT) | Done | Done | Done | Done |
| Monotonic FOLD (MSUM, MMAX, MMIN, MCOUNT) | Done | Done | Done | Done |
| ALONG (path-carried accumulation) | Done | Done | Partial | Done |
| BEST BY (witness selection) | Done | Done | Done | Done |
| ASSUME (hypothetical reasoning) | Done | Done | Done | Done |
| ABDUCE (abductive inference) | Done | Done | Done | Done |
| EXPLAIN RULE (proof traces) | Done | Done | Done | Done |
| DERIVE (graph materialization) | Done | Done | Done | Done |
| PRIORITY (rule precedence) | Done | Done | Done | Done |
| Module system (USE declarations) | Done | Done | Done | Done |
| Goal-directed queries (QUERY) | Done | Done | Done | Done |

### 1.2 Key Implementation Locations

| Component | Location |
|-----------|----------|
| Locy grammar (PEG) | `crates/uni-cypher/src/grammar/locy.pest` |
| Locy AST | `crates/uni-cypher/src/locy_ast.rs` |
| Locy compiler pipeline | `crates/uni-locy/src/compiler/mod.rs` |
| Dependency graph | `crates/uni-locy/src/compiler/dependency.rs` |
| Stratification (Tarjan SCC) | `crates/uni-locy/src/compiler/stratify.rs` |
| Type checking | `crates/uni-locy/src/compiler/typecheck.rs` |
| Locy config | `crates/uni-locy/src/config.rs` |
| Compiled program types | `crates/uni-locy/src/types.rs` |
| Fixpoint evaluation loop | `crates/uni-query/src/query/df_graph/locy_fixpoint.rs` |
| FOLD execution (DataFusion) | `crates/uni-query/src/query/df_graph/locy_fold.rs` |
| BEST BY execution | `crates/uni-query/src/query/df_graph/locy_best_by.rs` |
| EXPLAIN RULE | `crates/uni-query/src/query/df_graph/locy_explain.rs` |
| ASSUME execution | `crates/uni-query/src/query/df_graph/locy_assume.rs` |
| ABDUCE execution | `crates/uni-query/src/query/df_graph/locy_abduce.rs` |
| Locy planner | `crates/uni-query/src/query/df_graph/locy_planner.rs` |
| Locy program builder | `crates/uni-query/src/query/df_graph/locy_program.rs` |
| similar_to expression | `crates/uni-query/src/query/similar_to.rs` |
| similar_to DataFusion expr | `crates/uni-query/src/query/df_graph/similar_to_expr.rs` |
| Score fusion (RRF, weighted) | `crates/uni-query/src/query/fusion.rs` |
| Uni-Xervo facade | `crates/uni/src/api/xervo.rs` |
| Locy TCK feature files | `crates/uni-locy-tck/features/` |

### 1.3 Current Data Structures

**LocyConfig** (`crates/uni-locy/src/config.rs`):
```rust
pub struct LocyConfig {
    pub max_iterations: usize,           // default: 1000
    pub timeout: Duration,               // default: 300s
    pub max_explain_depth: usize,        // default: 100
    pub max_slg_depth: usize,            // default: 1000
    pub max_abduce_candidates: usize,    // default: 20
    pub max_abduce_results: usize,       // default: 10
    pub max_derived_bytes: usize,        // default: 256 MB
    pub deterministic_best_by: bool,     // default: true
}
```

**CompiledProgram** (`crates/uni-locy/src/types.rs`):
```rust
pub struct CompiledProgram {
    pub strata: Vec<Stratum>,
    pub rule_catalog: HashMap<String, CompiledRule>,
    pub warnings: Vec<CompilerWarning>,
    pub commands: Vec<CompiledCommand>,
}
```

**YieldColumn** (`crates/uni-locy/src/types.rs`):
```rust
pub struct YieldColumn {
    pub name: String,
    pub is_key: bool,
}
```

**FoldAggKind** (`crates/uni-query/src/query/df_graph/locy_fold.rs`):
```rust
pub enum FoldAggKind {
    Sum, Max, Min, Count, Avg, Collect,
}
```

**WarningCode** (`crates/uni-locy/src/types.rs`):
```rust
pub enum WarningCode {
    MsumNonNegativity,
}
```

**ProvenanceAnnotation** (`crates/uni-query/src/query/df_graph/locy_explain.rs`):
```rust
pub struct ProvenanceAnnotation {
    pub rule_name: String,
    pub clause_index: usize,
    pub support: Vec<ProofTerm>,        // currently always vec![]
    pub along_values: HashMap<String, Value>,
    pub iteration: usize,
    pub fact_row: Row,
}
```

### 1.4 Fixpoint Convergence Model

The fixpoint loop (`locy_fixpoint.rs`) uses semi-naive evaluation: each iteration feeds new deltas through derived scan handles. Convergence is defined as:

```
converged = delta.is_empty() && monotonic_agg.is_stable()
```

Where `MonotonicAggState::is_stable()` compares current accumulators against a snapshot from the previous iteration using `f64::EPSILON` tolerance. The loop is bounded by `max_iterations`.

### 1.5 ALONG Status: Partial Implementation

ALONG is parsed into the AST (`AlongBinding` with `LocyExpr` supporting `PrevRef`), compiled, and tracked for EXPLAIN output. However, value accumulation across recursive hops during fixpoint iteration is not fully threaded through — the `along_values` field in `ProvenanceAnnotation` is initialized to `HashMap::new()` in the provenance recorder. This means ALONG values appear in derivation trees but may not be correctly propagated through multi-hop recursive evaluation.

---

## 2. The Probabilistic Gap

Locy can reason about graph structure and compute recursive fixpoints, but it cannot correctly handle probabilities. The gap versus ProbLog-class systems:

| Capability | ProbLog | Locy Today | What's Missing |
|------------|:-------:|:----------:|----------------|
| AND along a proof chain | Yes | Partial (ALONG) | ALONG execution completion |
| OR across proof paths (noisy-OR) | Yes | No | **MNOR** operator |
| AND across grouped facts (product) | Yes | No | **MPROD** operator |
| Probabilistic complement | Yes | No | **PROB** column kind + IS NOT rewrite |
| Exact inference (shared sub-proofs) | Yes | No | BDD-based proof computation |
| Well-calibrated outputs | N/A | N/A | Calibration framework |

### 2.1 Why This Matters

In a property graph, paths constantly share intermediate nodes. Without correct probability combination:

- **MSUM** overestimates — two paths at 0.7 and 0.5 give 1.2, which is not a valid probability.
- **MMAX** underestimates — it gives 0.7 and ignores the contribution of the second path entirely.
- **Noisy-OR** gives the correct answer: 1 - (1-0.7)(1-0.5) = 0.85.

The property graph setting makes this worse than in Prolog-based systems because nodes have identity, paths share intermediate vertices, and the independence assumption is routinely violated.

---

## 3. What Deep Locy Adds

Deep Locy is the umbrella for six extensions that take Locy from structural graph reasoning to neuro-probabilistic graph reasoning:

```
┌─────────────────────────────────────────────────────────┐
│                    DECISION SUPPORT                      │
│   QUERY · ASSUME · ABDUCE · EXPLAIN                     │
├─────────────────────────────────────────────────────────┤
│               PROBABILISTIC LAYER                        │
│   MNOR (noisy-OR) · MPROD (product) · PROB complement   │
│   Proof-aware inference · Shared-proof detection (BDD)  │
├─────────────────────────────────────────────────────────┤
│                 NEURAL LAYER                             │
│   CREATE MODEL · Calibration · Neural provenance        │
│   Retrieval-backed FEATURES · Training hooks            │
├─────────────────────────────────────────────────────────┤
│            SIMILARITY FOUNDATION                         │
│   similar_to() expression function                      │
│   Vector · FTS · Hybrid fusion · Auto-embedding         │
├─────────────────────────────────────────────────────────┤
│              EXISTING LOCY CORE                          │
│   CREATE RULE · IS/IS NOT · FOLD · ALONG · BEST BY     │
│   ASSUME · ABDUCE · EXPLAIN · DERIVE · Modules          │
├─────────────────────────────────────────────────────────┤
│         UNI GRAPH ENGINE + STORAGE                       │
│   Property graph · Lance columnar · Vector indexes       │
│   FTS indexes · Adjacency CSR · Object store            │
└─────────────────────────────────────────────────────────┘
```

---

## 4. Extension R1: MNOR — Monotonic Noisy-OR

### 4.1 Semantics

MNOR computes the noisy-OR over probability values sharing the same key group:

```
P = 1 − ∏(1 − pᵢ)
```

This is the correct way to combine probabilities across independent proof paths under OR semantics ("at least one path delivers").

### 4.2 Syntax

```cypher
CREATE RULE part_availability AS
MATCH (part:Part)
WHERE (part, supplier) IS supply_path
FOLD avail = MNOR(reliability)
YIELD KEY part, avail
```

### 4.3 Properties

- **Domain**: Input values must be in [0, 1].
- **Monotonicity**: Non-decreasing. Adding a derivation with p > 0 can only increase the result. Bounded above by 1.0.
- **Identity**: MNOR over an empty set yields 0.0.
- **Accumulator**: Internally tracks ∏(1 − pᵢ). Final result is 1 − product.
- **Fixpoint admissibility**: Same operational criterion as MSUM — non-decreasing, bounded, converges when no new facts arrive.

### 4.4 Implementation Plan

The existing monotonic fold infrastructure handles all the hard work. MNOR requires:

1. **Add `FoldAggKind::Nor` variant** to `crates/uni-query/src/query/df_graph/locy_fold.rs`
2. **Add parser case** `"MNOR"` → `FoldAggKind::Nor` in `parse_fold_aggregate()` at `locy_program.rs:784-818`
3. **Add accumulator logic** to `MonotonicAggState::update()` in `locy_fixpoint.rs`:
   - Init: `0.0`
   - Update: `new = 1.0 - (1.0 - acc) * (1.0 - val)` (equivalently, track complement product)
4. **Add to monotonic whitelist** in `typecheck.rs:260-276` (allow `"MNOR"` in recursive rules)
5. **Add domain validation**: Emit `WarningCode::ProbabilityDomainViolation` if input column is not provably in [0, 1]

### 4.5 Example

Four supply paths with reliability 0.72, 0.54, 0.56, 0.42:

```
MNOR = 1 − (1−0.72)(1−0.54)(1−0.56)(1−0.42)
     = 1 − (0.28)(0.46)(0.44)(0.58)
     = 1 − 0.033
     = 0.967
```

Compare: MMAX gives 0.72 (ignores redundancy), MSUM gives 2.24 (invalid probability).

---

## 5. Extension R2: MPROD — Monotonic Product

### 5.1 Semantics

MPROD computes the product of probability values across derivations sharing a key group:

```
P = ∏ pᵢ
```

This is the correct way to combine probabilities when ALL conditions must be met simultaneously (assembly requires every subcomponent).

### 5.2 Syntax

```cypher
CREATE RULE assembly_availability AS
MATCH (asm:Part)-[:REQUIRES]->(sub:Part)
WHERE sub IS part_availability
FOLD joint = MPROD(avail)
YIELD KEY asm, joint AS PROB
```

### 5.3 Properties

- **Domain**: Input values must be in [0, 1].
- **Monotonicity**: Non-increasing. Adding a derivation with p < 1 can only decrease the product. Bounded below by 0.0.
- **Identity**: MPROD over an empty set yields 1.0 (multiplicative identity).
- **Underflow protection**: Log-space computation when product drops below `probability_epsilon` (default 1e-15). Track `log(∏ pᵢ) = ∑ log(pᵢ)` and convert back at read time.
- **Fixpoint admissibility**: Same operational criterion as MMIN — non-increasing, bounded. The runtime's existing bidirectional convergence check handles this.

### 5.4 Implementation Plan

Mirrors MNOR:

1. **Add `FoldAggKind::Prod` variant** to `locy_fold.rs`
2. **Add parser case** `"MPROD"` → `FoldAggKind::Prod` in `parse_fold_aggregate()`
3. **Add accumulator logic**: Init `1.0`, update `acc *= val`, with log-space fallback
4. **Add to monotonic whitelist** in `typecheck.rs`
5. **Domain validation**: Same as MNOR

### 5.5 The Three Probability Operations

| Operation | Operator | Use Case | Example |
|-----------|----------|----------|---------|
| Sequential AND (chain) | ALONG with `prev.x * edge.y` | Reliability of a supply chain hop-by-hop | 0.9 × 0.8 = 0.72 |
| Parallel AND (all needed) | FOLD MPROD(availability) | Assembly requiring ALL subcomponents | 0.967 × 0.85 × 0.92 = 0.756 |
| Parallel OR (any suffices) | FOLD MNOR(reliability) | Part available if ANY path delivers | 1-(1-0.72)(1-0.56) = 0.877 |

---

## 6. Extension R3: PROB Column Kind and Probabilistic Complement

### 6.1 The PROB Annotation

PROB is a column kind annotation on a YIELD column, analogous to KEY. It marks a column as carrying a probability value in [0, 1] with defined combination and complement semantics.

```cypher
CREATE RULE risky_combined AS
MATCH (acct:Account)
WHERE (acct, _) IS risk_path
FOLD risk = MNOR(path_risk)
YIELD KEY acct, risk AS PROB
```

### 6.2 Rules

- **One PROB column per rule output (v1 restriction).** Multiple scores require only one to be the probability.
- **Explicit annotation required.** PROB is not inferred — the rule must write `AS PROB` to opt in.
- **MNOR and MPROD output is automatically PROB.** Explicit annotation is optional but recommended.
- **IS NOT targets the PROB column.** Complement (1 − p) applies to the PROB column.
- **Non-PROB rules retain Boolean IS NOT.** Full backward compatibility.

### 6.3 Probabilistic Complement for IS NOT

Today, IS NOT is Boolean — it excludes rows via anti-join (`locy_fixpoint.rs` `apply_anti_join()`). With PROB:

```cypher
-- risky derives Account A with PROB 0.7
-- safe uses complement:
CREATE RULE safe AS
MATCH (acct:Account)
WHERE acct IS NOT risky_combined
YIELD KEY acct
-- PROB = 1 - 0.7 = 0.3
```

If the negated rule has no PROB column, IS NOT remains Boolean. This is determined at compile time by inspecting the negated rule's yield schema.

### 6.4 Cross-Predicate Conjunction

When a rule body contains multiple probabilistic conditions:

```cypher
WHERE a IS risky_combined, a IS NOT trusted
```

The combined probability is the product: `risk × (1 − trust)`. This assumes independence between the two predicates. If they share underlying base facts, the true probability differs — but resolving this is explicitly out of scope (see Section 8).

### 6.5 Implementation Plan

1. **Extend `YieldColumn`** in `crates/uni-locy/src/types.rs`:
   ```rust
   pub struct YieldColumn {
       pub name: String,
       pub is_key: bool,
       pub is_prob: bool,    // NEW
   }
   ```
2. **Parse PROB keyword** in YIELD items (grammar + walker)
3. **Validate one-PROB-per-rule** in `typecheck.rs`
4. **Rewrite IS NOT** in `locy_fixpoint.rs`: when negated rule has a PROB column, compute `1 − p` instead of anti-join exclusion
5. **Cross-predicate product**: when multiple IS-refs bind PROB values in one rule body, multiply them
6. **Add `strict_probability_domain`** to `LocyConfig`

---

## 7. Extension R4: Proof-Aware Probability Computation

### 7.1 The Problem: Shared Sub-Proofs

MNOR assumes all proof paths are probabilistically independent. In a property graph, this is frequently violated:

```
Path 1: Blade -> Supplier A (0.9) -> Smelter 1 (0.8) = 0.72
Path 2: Blade -> Supplier B (0.7) -> Smelter 1 (0.8) = 0.56

MNOR (independence):  1 - (1-0.72)(1-0.56) = 0.877
Correct (shared):     0.8 × (1-(1-0.9)(1-0.7)) = 0.776

Overestimation: +10 percentage points
```

### 7.2 Existing Infrastructure

The `ProvenanceStore` already records provenance per derived fact (rule name, clause index, iteration, fact row). The `support` field exists in `ProvenanceAnnotation` but is always `vec![]` — it was designed for base-fact tracking but never populated.

### 7.3 Phased Delivery

**Phase A: Independence Mode (default, immediate)**
- MNOR and MPROD use running accumulators. Independence assumed.
- Ships with R1/R2 implementation. No new infrastructure needed.

**Phase B: Shared-Proof Detection (warning)**
- Populate `ProvenanceAnnotation.support` during fixpoint evaluation.
- Track which base probabilistic facts contribute to each derivation.
- If derivations in the same MNOR/MPROD group share a base fact: emit `WarningCode::SharedProbabilisticDependency`.

**Phase C: Exact Mode (opt-in)**
- `LocyConfig::exact_probability = true` enables BDD-based evaluation.
- Dependencies stored as a Boolean formula over base fact identifiers using a BDD (recommended: `biodivine-lib-bdd`, pure Rust).
- At query time, the BDD is evaluated to compute exact probability.
- `max_bdd_variables` bounds memory usage (default: 1000). Exceeded → fallback to independence mode with `WarningCode::BddLimitExceeded` and `approximate: true` flag on results.

### 7.4 Scope of Exactness

Phase C guarantees exact probability **within a single MNOR or MPROD aggregate group**. It does NOT automatically cover:
- Correlations across different predicates joined in one rule body
- Correlations between positive and complemented predicates
- Correlations across upstream and downstream aggregations

Cross-group correlation produces `WarningCode::CrossGroupCorrelationNotExact`.

---

## 8. similar_to() — The Similarity Foundation

### 8.1 Current Implementation Status: ~90% Complete

`similar_to()` is a unified expression function that scores a bound node against a query, returning a float in [0, 1]. It works in WHERE, RETURN, ALONG, FOLD, and Locy rule bodies.

```cypher
similar_to(sources, queries [, options]) → FLOAT [0, 1]
```

### 8.2 What's Implemented

| Feature | Status | Location |
|---------|--------|----------|
| Vector-to-vector cosine similarity | Done | `similar_to.rs:216-244` |
| FTS/BM25 scoring with saturation normalization | Done | `similar_to.rs:246-254` |
| Auto-embedding via Xervo | Done | `similar_to_expr.rs:549-613` |
| Multi-source weighted fusion | Done | `fusion.rs:98-101` |
| Multi-source RRF (point context fallback) | Done | `fusion.rs:110-117` |
| DataFusion PhysicalExpr columnar execution | Done | `similar_to_expr.rs` |
| Options map (method, weights, k, fts_k) | Done | `similar_to.rs:79-100` |
| Compile-time validation (type/index checks) | Done | `similar_to.rs` |
| Memoization per (props, query, vid) | Done | `similar_to_expr.rs` |
| Locy integration (WHERE, YIELD, ALONG, FOLD) | Done | Integration tests: 1188 lines |
| `vector_similarity` backward compatibility | Done | `df_udfs.rs:6876` |

### 8.3 Known Gaps

**L2/Dot metric support**: `similar_to()` hardcodes cosine similarity. The metric-aware `calculate_score()` function exists in `common.rs:1287-1299` (used by `db.idx.vector.query()` procedures) but is not wired into the `similar_to()` expression path. The schema correctly tracks `DistanceMetric` per index.

**RRF point-context warning**: `fuse_rrf_point()` returns a `used_fallback` boolean that is discarded at `similar_to.rs:341`. No mechanism threads warnings from the expression evaluator to `QueryResult`. The `QueryWarning` enum exists but `QueryResult.warnings` is always constructed empty.

### 8.4 Role in Deep Locy

Because `similar_to()` returns [0, 1], its output is directly usable as a PROB value:

```cypher
CREATE RULE evidence_strength AS
MATCH (v:Vulnerability)-[:DOCUMENTED_IN]->(doc:KnowledgeDoc)
YIELD KEY v, KEY doc,
  similar_to([doc.embedding, doc.content],
    'actively exploited RCE') AS relevance PROB
```

This feeds cleanly into MNOR, MPROD, complement, and the full probabilistic stack. `similar_to()` is the bridge between neural perception (embeddings, FTS) and symbolic reasoning (Locy rules).

---

## 9. Extension R5: Neural Predicates (CREATE MODEL)

### 9.1 Concept

A neural predicate wraps a Uni-Xervo model alias with typed input/output semantics, producing values that participate in MNOR, MPROD, complement, and proof traces as first-class citizens.

### 9.2 Syntax

```cypher
CREATE MODEL supplier_risk_scorer AS
INPUT (s:Supplier)
FEATURES s.country, s.annual_revenue, s.years_active, s.embedding
OUTPUT PROB risk
USING xervo('classify/supplier-risk-v3')
CALIBRATION platt_scaling
VERSION '3.1.0'
```

### 9.3 Output Types

| Output Type | Value Range | Probability Semiring |
|-------------|-------------|---------------------|
| PROB | [0, 1] | Direct: MNOR, MPROD, complement |
| SCORE | (-∞, +∞) | Requires transformation (sigmoid) |
| LABEL | Categorical | WHERE equality checks only |
| VECTOR | Float array | vector_similarity / similar_to |

### 9.4 Usage in Rules

```cypher
-- Neural predicate as a probabilistic fact source
CREATE RULE risky_supplier AS
MATCH (s:Supplier)
YIELD KEY s, supplier_risk_scorer(s) AS risk PROB

-- Neural predicate in WHERE as a filter
CREATE RULE suspect_invoice AS
MATCH (inv:Document)-[:SUBMITTED_BY]->(s:Supplier)
WHERE invoice_fraud_detector(inv) > 0.7
YIELD KEY inv, KEY s, invoice_fraud_detector(inv) AS fraud_prob PROB
```

### 9.5 Existing Infrastructure

**Uni-Xervo** (`crates/uni/src/api/xervo.rs`) provides:
- `ModelRuntime` with 12+ providers (Candle, MistralRS, OpenAI, Gemini, Anthropic, Cohere, VoyageAI, Azure, VertexAI, Mistral, FastEmbed)
- `embed()` and `generate()` methods
- `ModelAliasSpec` catalog with typed tasks and warmup policies (Eager/Lazy/Background)
- Instrumented models with reliability/retry/timeout wrappers

**What's missing**:
- `ModelTask::Classify` (only Embed/Rerank/Generate exist)
- `CREATE MODEL` parser support and AST variant
- Model registry in `CompiledProgram`
- Memoization planning for model calls in fixpoint evaluation
- `ClassificationModel` trait in Xervo

### 9.6 Implementation Plan

1. **Grammar**: Add `CREATE MODEL` to `locy.pest` and `locy_walker.rs`
2. **AST**: Add `LocyStatement::Model(ModelDefinition)` variant to `locy_ast.rs`
3. **Compiler**: Validate model refs, track output types, register in `CompiledProgram`
4. **Xervo**: Add `ModelTask::Classify` and `classify()` method
5. **Runtime**: Invoke models during rule evaluation with per-node memoization

---

## 10. Extension R6: Neural Provenance in EXPLAIN

### 10.1 Concept

When EXPLAIN RULE traces a derivation involving neural predicates, the proof tree shows neural contributions alongside symbolic derivation steps.

### 10.2 Provenance Record

```rust
NeuralProvenance {
    model_name: String,          // 'supplier_risk_scorer'
    model_version: String,       // '3.1.0'
    xervo_alias: String,         // 'classify/supplier-risk-v3'
    input_node_ids: Vec<Vid>,
    features_used: Vec<String>,  // ['country', 'annual_revenue', ...]
    output_value: f64,           // 0.73
    output_type: OutputType,     // PROB
    calibration: Option<String>, // 'platt_scaling'
    confidence: Option<ConfidenceBand>, // {lower: 0.61, upper: 0.84}
    timestamp: DateTime,
}
```

### 10.3 Decomposed Explanation

```
EXPLAIN RULE assembly_readiness WHERE asm.name = 'Turbine Blade'

assembly_readiness(Turbine Blade) = 0.689  [MPROD]
├── part_availability(Alloy X) = 0.967  [MNOR]
│   ├── supply_path(Blade→SupA→Smelter1) = 0.72  [symbolic, ALONG]
│   └── supply_path(Blade→SupB→Smelter1) = 0.56  [symbolic, ALONG]
├── part_quality(Ceramic Coat) = 0.85
│   └── part_defect_detector(Ceramic) = 0.15  [neural, v2.4.0, isotonic, CI: 0.09-0.22]
│       (quality = availability × (1 - defect_prob))
└── usable_supplier(Smelter 1) = 0.95  [complement]
    └── sanctions_signal(Smelter 1) = 0.05  [neural, v1.0, platt]
```

This lets an auditor see: which parts of the derivation are symbolic vs. neural, what model versions produced each score, calibration methods used, and confidence bands — all in one trace.

### 10.4 Existing Infrastructure

The `ProvenanceStore` and `ProvenanceAnnotation` in `locy_explain.rs` provide the skeleton. The `explain_rule()` function already builds derivation trees with cycle detection and depth limiting. What's missing is `NeuralProvenance` nodes in the tree and confidence/calibration metadata.

---

## 11. Extension R7: Calibration and Probability Hygiene

### 11.1 The Problem

Neural models rarely output well-calibrated probabilities. An MPROD of three overconfident 0.95 predictions gives 0.857; if the true calibrated values are 0.75, the correct answer is 0.422. Miscalibration compounds through the probability semiring.

### 11.2 CALIBRATION Clause

```cypher
CREATE MODEL supplier_risk_scorer AS
INPUT (s:Supplier)
FEATURES s.country, s.revenue, s.embedding
OUTPUT PROB risk
USING xervo('classify/supplier-risk-v3')
CALIBRATION platt_scaling        -- ← calibration method
VERSION '3.1.0'
```

| Method | Description |
|--------|-------------|
| `platt_scaling` | Logistic regression on model logits |
| `isotonic_regression` | Non-parametric monotonic transform |
| `temperature_scaling` | Single temperature on softmax logits |
| `beta_calibration` | Beta distribution fit for skewed classes |
| `none` | Raw output; compiler emits warning |

### 11.3 CALIBRATE Command

```cypher
CALIBRATE supplier_risk_scorer
ON MATCH (s:Supplier) WHERE s.label IS NOT NULL
TARGET s.label
METHOD platt_scaling
HOLDOUT 0.2
```

Batch operation: run model over subgraph, collect predictions vs. ground truth, fit calibration transform, store parameters in `ModelRegistry` node.

### 11.4 VALIDATE Command

```cypher
VALIDATE assembly_availability
ON MATCH (asm:Part) WHERE asm.true_availability IS NOT NULL
TARGET asm.true_availability
METRICS brier_score, ece, auc
RETURN model_name, metric, value
```

Evaluates the full Locy program (symbolic + neural) against ground truth and reports accuracy metrics.

### 11.5 Implementation Status

Entirely missing. No calibration transforms, no ECE/Brier scoring, no `CalibrationDrift` warning, no `ModelRegistry` graph storage. This is a new module.

---

## 12. Extension R8: Retrieval-Backed Neural Predicates

### 12.1 Concept

Locy can go beyond DeepProbLog by letting neural predicates consume retrieved evidence from the graph — vector search results, graph neighborhoods, path context — as part of their input.

### 12.2 FEATURES with Graph Context

```cypher
-- Property features (basic)
FEATURES s.country, s.annual_revenue

-- Semantic match (retrieval-backed via similar_to)
FEATURES semantic_match(s.profile, 'export control violation')

-- Graph neighborhood features
FEATURES s.degree_centrality, s.pagerank_score

-- Aggregated neighborhood
FEATURES avg_neighbor(s, 'SUPPLIES', 'risk_score')

-- Path context from a prior Locy derivation
FEATURES (s, path_risk) FROM supply_path
```

`semantic_match()` is syntactic sugar over `similar_to()`, leveraging the already-implemented similarity foundation.

### 12.3 Existing Infrastructure

`similar_to()` is ~90% complete and provides the retrieval foundation. What's missing is the `FEATURES` clause syntax in `CREATE MODEL`, `semantic_match()` as an alias, and dynamic feature materialization during model invocation.

---

## 13. Extension R9: Training Hooks

### 13.1 Phased Delivery

**Phase A: Offline Calibration** — `CALIBRATE` (see Section 11.3)

**Phase B: Supervised Validation** — `VALIDATE` (see Section 11.4)

**Phase C: Semi-Differentiable Training**:

```cypher
TRAIN supplier_risk_scorer
TO MINIMIZE brier_score(assembly_availability.joint, asm.true_availability)
ON MATCH (asm:Part) WHERE asm.true_availability IS NOT NULL
EPOCHS 50
LEARNING_RATE 0.001
FREEZE symbolic  -- only train neural parameters
```

The engine evaluates the full Locy program forward, computes loss against ground truth, and routes gradients back through MPROD/MNOR aggregation and neural predicate invocations. Symbolic rules are treated as fixed computation (`FREEZE symbolic`).

This is the most complex deliverable. Phases 5–9 alone (neural predicates + calibration + provenance + retrieval) are already a strong differentiator even without end-to-end training.

---

## 14. LocyConfig Extensions

The existing `LocyConfig` needs these additions for the probabilistic and neural extensions:

```rust
pub struct LocyConfig {
    // --- existing fields ---
    pub max_iterations: usize,
    pub timeout: Duration,
    pub max_explain_depth: usize,
    pub max_slg_depth: usize,
    pub max_abduce_candidates: usize,
    pub max_abduce_results: usize,
    pub max_derived_bytes: usize,
    pub deterministic_best_by: bool,

    // --- probabilistic extensions (R1-R4) ---

    /// Enable exact probability via BDD when shared sub-proofs
    /// are detected in MNOR/MPROD groups.
    /// Scope: within a single aggregate group.
    /// Default: false.
    pub exact_probability: bool,

    /// Max BDD variables before falling back to independence mode.
    /// Affected results are flagged approximate in LocyResult.
    /// Default: 1000.
    pub max_bdd_variables: usize,

    /// Underflow threshold for log-space computation in MPROD.
    /// Default: 1e-15.
    pub probability_epsilon: f64,

    /// When true, out-of-domain [0,1] values in MNOR/MPROD
    /// cause a hard runtime error instead of warn-and-clamp.
    /// Default: false (permissive mode).
    pub strict_probability_domain: bool,
}
```

---

## 15. WarningCode Extensions

The existing `WarningCode` enum (currently only `MsumNonNegativity`) needs these additions:

```rust
pub enum WarningCode {
    MsumNonNegativity,                    // existing

    // --- probabilistic warnings ---
    ProbabilityDomainViolation,           // input to MNOR/MPROD outside [0,1]
    SharedProbabilisticDependency,        // derivations in same group share base fact
    CrossGroupCorrelationNotExact,        // correlation spans separate MNOR/MPROD groups
    BddLimitExceeded,                     // max_bdd_variables exceeded, fell back

    // --- neural warnings ---
    UncalibratedNeuralPredicate,          // CALIBRATION none without VERSION
    CalibrationDrift,                     // model predictions diverge from calibration stats
    SharedNeuralInput,                    // two neural predicates share input features

    // --- similar_to warnings ---
    RrfInPointContext,                    // RRF degenerated to equal-weight in point context
}
```

---

## 16. Full Integrated Example

A single Locy program combining symbolic rules, neural predicates, retrieval-backed features, and probabilistic combination:

```cypher
-- ================================================================
-- Neural predicates (perception layer)
-- ================================================================

CREATE MODEL supplier_risk_scorer AS
INPUT (s:Supplier)
FEATURES s.country, s.revenue, s.years_active, s.profile_embedding
OUTPUT PROB risk
USING xervo('classify/supplier-risk-v3')
CALIBRATION platt_scaling
VERSION '3.1.0'

CREATE MODEL sanctions_signal AS
INPUT (s:Supplier)
FEATURES semantic_match(s.profile, 'export control sanctions violation')
OUTPUT PROB signal
USING xervo('classify/sanctions-v1')
CALIBRATION platt_scaling

-- ================================================================
-- Layer 1: Propagate reliability down supply chains
--          ALONG multiplies sequentially (AND along path)
-- ================================================================

CREATE RULE supply_path AS
MATCH (part:Part)-[s:SUPPLIED_BY]->(supplier:Supplier)
ALONG reliability = s.base_reliability * (1 - supplier_risk_scorer(supplier))
YIELD KEY part, KEY supplier, reliability

CREATE RULE supply_path AS
MATCH (part:Part)-[s:SUPPLIED_BY]->(mid:Supplier)-[:SOURCES]->(sub:Part)
WHERE (sub, supplier) IS supply_path
ALONG reliability = prev.reliability * s.base_reliability
  * (1 - supplier_risk_scorer(mid))
YIELD KEY part, KEY supplier, reliability

-- ================================================================
-- Layer 2: Combine redundant supply paths per part
--          MNOR: P(at least one path delivers)
-- ================================================================

CREATE RULE part_availability AS
MATCH (part:Part)
WHERE (part, supplier) IS supply_path
FOLD avail = MNOR(reliability)
YIELD KEY part, avail AS PROB

-- ================================================================
-- Layer 3: Assembly requires ALL subcomponents
--          MPROD: P(all available simultaneously)
-- ================================================================

CREATE RULE assembly_availability AS
MATCH (asm:Part)-[:REQUIRES]->(sub:Part)
WHERE sub IS part_availability
FOLD joint = MPROD(avail)
YIELD KEY asm, joint AS PROB

-- ================================================================
-- Layer 4: Sanctions risk (probabilistic complement)
-- ================================================================

CREATE RULE sanctioned AS
MATCH (s:Supplier)
YIELD KEY s, sanctions_signal(s) AS risk PROB

CREATE RULE usable_supplier AS
MATCH (s:Supplier)
WHERE s IS NOT sanctioned
YIELD KEY s
-- PROB = 1 - sanctioned.risk (or 1.0 if absent)

-- ================================================================
-- Decision support
-- ================================================================

-- Q1: Current availability
QUERY assembly_availability
WHERE asm.name = 'Turbine Blade'
RETURN asm.name, joint AS availability

-- Q2: What if Smelter 1 goes offline?
ASSUME {
  MATCH (s:Supplier {name: 'Smelter 1'}) DETACH DELETE s
} THEN {
  QUERY assembly_availability
  WHERE asm.name = 'Turbine Blade'
  RETURN joint AS availability_without_smelter1
}

-- Q3: Minimum change for >= 95% availability?
ABDUCE assembly_availability
WHERE asm.name = 'Turbine Blade', joint >= 0.95
RETURN remediation

-- Q4: Full provenance decomposition
EXPLAIN RULE assembly_availability
WHERE asm.name = 'Turbine Blade'
-- Shows: symbolic paths, neural risk scores, neural sanctions signals,
-- with model versions, calibration methods, confidence bands
```

---

## 17. Semantic Edge Cases (Stress Corpus Summary)

The [Semantic Stress Corpus](../uni-locy-docs/locy_semantic_stress_corpus.docx.md) defines 24 adversarial programs across 10 categories. Key design decisions:

| ID | Scenario | Verdict | Key Takeaway |
|----|----------|---------|--------------|
| A1 | Two PROB predicates sharing base fact | RUNTIME WARNING | CrossGroupCorrelationNotExact under exact mode |
| A2 | IS + IS NOT sharing evidence | DEFINED | Independence assumption, documented |
| A3 | Neural predicates sharing embedding input | OPEN QUESTION | Recommend SharedNeuralInput heuristic warning |
| B1 | MNOR → MPROD → MNOR three-layer stack | DEFINED | Each layer independent; EXPLAIN shows full tree |
| B2 | Recursive MNOR (infection propagation) | DEFINED | MNOR monotonic, safe in recursive strata |
| B3 | MPROD in recursion conflating path/group | OPEN QUESTION | Recommend FoldInRecursivePath warning |
| C1 | IS NOT of PROB rule inside recursion | DEFINED | Stratification guarantees complement on fixpoint |
| C2 | Double complement (NOT NOT) | DEFINED | Mechanical, no algebraic simplification |
| D1–D3 | ABDUCE over probability thresholds | DEFINED | Structural changes only; property abduction is future |
| E1 | ALONG with mixed neural/symbolic hops | DEFINED | EXPLAIN shows per-hop source; point estimate overall |
| F1 | Overloaded clauses, different PROB values | DEFINED | Both derivations coexist; user picks FOLD or BEST BY |
| G1–G2 | ASSUME + probability interaction | DEFINED | Full re-evaluation including neural predicates |
| H1–H3 | Degenerate values (zeros, ones, overflow) | DEFINED | strict mode = error; permissive = clamp + warn |
| I2 | BEST BY + MNOR in same rule | COMPILE ERROR | BestByWithMonotonicFold applies to MNOR/MPROD |
| J1–J2 | Cross-stratum probability flow | DEFINED / WARNING | Valid if expression provably in [0,1] |

**Tally**: 18 DEFINED BEHAVIOR, 2 COMPILE ERROR, 2 RUNTIME WARNING, 2 OPEN QUESTIONS.

The two open questions (A3: shared neural inputs, B3: FOLD-in-recursive-path) both have recommended resolutions as heuristic warnings.

---

## 18. Competitive Positioning

### 18.1 Against ProbLog

| Capability | ProbLog | Locy (today) | Locy (Deep) |
|------------|:-------:|:------------:|:-----------:|
| Recursive rules / fixpoint | Yes | Yes | Yes |
| Stratified negation | Yes | Yes | Yes |
| AND along proof chain | Yes | Partial | Yes (ALONG) |
| OR across proof paths | Yes | No | Yes (MNOR) |
| AND across grouped facts | Yes | No | Yes (MPROD) |
| Probabilistic complement | Yes | No | Yes (IS NOT + PROB) |
| Exact inference (shared proofs) | Yes | No | Yes (Phase C, BDD) |
| Graph-native execution | No | Yes | Yes |
| Hypothetical what-if | No | Yes | Yes |
| Abductive inference | Limited | Yes | Yes |
| Path-carried accumulation | Manual | Partial | Yes |
| Witness selection | No | Yes | Yes |
| Graph materialization | No | Yes | Yes |

### 18.2 Against DeepProbLog

| Capability | DeepProbLog | Locy (Deep) |
|------------|:-----------:|:-----------:|
| First-class neural predicates | Yes (nn/3) | Yes (CREATE MODEL) |
| Typed neural outputs | Implicit | Yes (PROB/SCORE/LABEL/VECTOR) |
| Calibration awareness | No | Yes (CALIBRATE, ECE, Brier) |
| End-to-end training | Yes (PyTorch) | Yes (TRAIN, semi-differentiable) |
| Neural provenance in proofs | Limited | Yes (decomposed EXPLAIN) |
| Retrieval-backed predicates | No | Yes (FEATURES semantic_match) |
| Graph-native execution | No (Prolog terms) | Yes (property graph) |
| Path-carried accumulation | No | Yes (ALONG + neural) |
| Hypothetical reasoning | No | Yes (ASSUME) |
| Abductive inference | No | Yes (ABDUCE) |
| Model version tracking | No | Yes (VERSION, ModelRegistry) |
| Drift detection | No | Yes (CalibrationDrift) |
| Confidence bands | No | Yes (NeuralProvenance) |

---

## 19. Implementation Roadmap

### 19.1 Phase Dependencies

```
similar_to() [~90% done]
     │
     ├──→ Phase 1: MNOR + MPROD ──→ Phase 2: PROB + IS NOT complement
     │                                    │
     │                                    ├──→ Phase 3: Shared-proof detection
     │                                    │         │
     │                                    │         └──→ Phase 7: Exact BDD mode
     │                                    │
     └──→ Phase 5: CREATE MODEL ──→ Phase 6: Neural provenance
              │                           │
              ├──→ Phase 8: Calibration   │
              │                           │
              └──→ Phase 9: Retrieval-backed FEATURES
                       │
                       └──→ Phase 10: Training hooks
```

### 19.2 Delivery Order and Estimates

| Order | Phase | Deliverables | Depends On | Status |
|:-----:|:-----:|-------------|------------|--------|
| 0 | — | similar_to() expression function | None | ~90% complete |
| 1 | Phase 1 | MNOR + MPROD (independence mode) | None | Not started |
| 2 | Phase 2 | PROB annotation + IS NOT complement | Phase 1 | Not started |
| 3 | Phase 3 | Shared-proof detection (warning mode) | Phase 1 | Skeleton exists |
| 4 | Phase 5 | CREATE MODEL + Xervo Classify | Phase 1 | Xervo exists |
| 5 | Phase 6 | Neural provenance in EXPLAIN | Phase 5 | EXPLAIN exists |
| 6 | Phase 8 | Calibration (CALIBRATE + VALIDATE) | Phase 5 | Not started |
| 7 | Phase 9 | Retrieval-backed FEATURES | Phase 5 + similar_to | similar_to exists |
| 8 | Phase 7 | Exact BDD-based probability | Phase 3 | Not started |
| 9 | Phase 10 | TRAIN (semi-differentiable) | All above | Not started |

### 19.3 Affected Crates

| Crate | Phases Affected | Changes |
|-------|:---------------:|---------|
| `uni-cypher` | 2, 5 | PROB keyword in YIELD; CREATE MODEL / CALIBRATE / VALIDATE / TRAIN statements |
| `uni-locy` | 1, 2, 3 | Monotonic whitelist; PROB column kind; domain validation; warning codes; config extensions |
| `uni-query` | 1, 2, 3, 5, 6 | MNOR/MPROD accumulators; complement rewrite; model invocation; neural provenance |
| `uni` (runtime) | 3, 5, 7, 8 | Proof tracking; BDD module; calibration module; training module |
| `uni-xervo` | 5 | ModelTask::Classify; classify() method |
| `uni-locy-tck` | All | New feature files for all operators and modes |

### 19.4 Quick Wins (Low Effort, High Value)

1. **MNOR + MPROD** — Mechanical addition to existing fold infrastructure. One enum variant, one parser case, one accumulator formula, one whitelist entry each.
2. **PROB annotation** — Extend `YieldColumn` with `is_prob: bool`, parse the keyword, validate one-per-rule.
3. **similar_to() L2/Dot metrics** — Wire existing `calculate_score()` into the expression evaluator.
4. **New WarningCode variants** — Add the probability warning codes to the existing enum.
5. **Populate `ProvenanceAnnotation.support`** — The field exists but is always empty; populate it during fixpoint for Phase B.

---

## 20. Open Design Questions

1. **MPROD and BEST BY**: Should MPROD be exempt from `BestByWithMonotonicFold`? There may be valid use cases for "keep the assembly with highest joint probability." Deferred to design review.

2. **ABDUCE with probability thresholds**: ABDUCE could suggest property-value changes (e.g., "increase Supplier A reliability to 0.95") in addition to structural changes. Valuable but out of scope for v1.

3. **Multiple PROB columns**: The v1 restriction of one PROB column per rule is conservative. Future versions could support named probability channels. Deferred to avoid premature complexity.

4. **Neural output correlation** (Stress Corpus A3): Two neural predicates consuming the same embedding produce correlated outputs that proof-graph analysis cannot detect. Recommended: `SharedNeuralInput` heuristic warning when two neural predicates in the same rule body consume the same node.

5. **FOLD-in-recursive-path** (Stress Corpus B3): A FOLD aggregate in a rule that also has a recursive IS reference and no ALONG clause is almost certainly a semantic mistake. Recommended: `FoldInRecursivePath` compiler warning.

6. **BDD library selection**: Candidates are `biodivine-lib-bdd` (pure Rust, actively maintained) and `cudd-sys` (FFI to CUDD). Recommendation: `biodivine-lib-bdd` for Rust-native compilation.

7. **ALONG execution completion**: The ALONG accumulator is parsed and tracked but not fully threaded through recursive fixpoint evaluation. This must be fixed before probability propagation along paths can work correctly.
