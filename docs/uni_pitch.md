# Uni: The Reasoning and Memory Infrastructure for Intelligent Systems

> **Strategic Positioning Document**
>
> *How Uni should be understood, sold, and marketed in the age of agentic AI.*

---

## Table of Contents

1. [The Moment](#1-the-moment)
2. [The Gap in Every AI Stack](#2-the-gap-in-every-ai-stack)
3. [Where Intelligence Actually Happens](#3-where-intelligence-actually-happens)
4. [The Five Pillars](#4-the-five-pillars)
5. [The Composite Picture](#5-the-composite-picture)
6. [Competitive Landscape](#6-competitive-landscape)
7. [Go-to-Market](#7-go-to-market)
8. [The Pitch](#8-the-pitch)

---

## 1. The Moment

The defining transformation in AI is not better models — it is **agents**. The industry is shifting from LLMs as conversational tools to LLMs as autonomous actors that reason over structured knowledge, plan multi-step actions, maintain persistent memory, explain their decisions, and operate continuously.

Every major lab is racing toward this. The entire industry is converging on the same realization: **the bottleneck isn't intelligence — it's the infrastructure that lets intelligence operate on the real world.**

LeCun has been articulating why for years. LLMs are System 1 — fast, intuitive, pattern-based. When they appear to "reason," they're pattern-matching against reasoning traces seen in training data. There's no internal deliberation, no search, no verification. That's why they confidently hallucinate.

Real intelligence requires a world model — a structured representation you can query, simulate, and plan against. It requires System 2 — a formal computational layer for logical inference, constraint satisfaction, and search. And it requires these as *separate infrastructure*, not emergent properties of scale.

The industry's response has been to make models bigger and add chain-of-thought prompting. This treats the symptom. Chain-of-thought asks System 1 to *impersonate* System 2. Sometimes it works. You can never verify when it doesn't.

The infrastructure gap remains wide open.

---

## 2. The Gap in Every AI Stack

Today's agent stacks look like this: an LLM bolted onto a vector store, with tool definitions and a system prompt. The "memory" is a bag of embeddings. The "reasoning" is prompt engineering. The "planning" is hope.

What agents actually need — and what no mainstream component provides:

**Structured memory.** A vector store can tell you "this chunk is semantically similar to your query." It cannot tell you "Alice manages Bob, who has access to the production database, which was last audited 6 months ago." Relationships matter. Structure matters. Context is a graph, not a flat list of similar documents.

**Formal reasoning.** RAG gets you the right documents. It doesn't compute transitive closure ("who can transitively access this resource?"), propagate risk ("if this supplier fails, what's the blast radius?"), or resolve permissions ("given all inherited roles, can this user perform this action?"). These require logic, not lookup.

**Hypothetical planning.** Before an agent acts, it should be able to ask "what would happen if I did X?" and get an answer without actually doing X. Today, agents either act and hope, or they ask the LLM to imagine the consequences — which is hallucination dressed up as planning.

**Explainability.** When an agent makes a decision, you need to know *why*. Not "the model said so." An actual derivation chain. Auditors need this. Regulators need this. Humans in the loop need this.

**Lightweight, everywhere deployment.** Agents run on laptops, in serverless functions, at the edge, inside CI/CD pipelines. They can't all phone home to a database cluster.

---

## 3. Where Intelligence Actually Happens

The world is full of structured domains — regulatory frameworks, organizational hierarchies, infrastructure dependencies, financial networks, manufacturing processes, codebases — where knowledge is *constructed*, not learned from pixels. The rules governing these domains come from domain expertise, regulations, business logic, and engineering knowledge. They must be modeled explicitly and reasoned over formally.

This is where Uni lives.

Uni is the **reasoning and memory infrastructure for structured knowledge domains**. It provides the cognitive substrate that intelligent systems need to *think* about specific worlds — not in the loose, metaphorical sense, but in the precise, formal, verifiable sense.

The LLM handles natural language understanding and creative problem-solving. Uni handles structured knowledge, formal inference, and verifiable conclusions. The LLM is the intuition. Uni is where the structured thinking happens.

Together, they form the architecture the field has been describing: System 1 (pattern recognition, language) paired with System 2 (rules, simulation, verification). As the field matures — as learned world models emerge for continuous dynamics and perception — Uni remains the layer where *discrete, relational, rule-governed* reasoning occurs. That layer doesn't get replaced by better neural networks. It complements them.

---

## 4. The Five Pillars

### Pillar 1: The Graph Is Structured Memory

Every intelligent system needs a way to represent what it knows about the world. Not as a flat pile of documents. Not as a latent vector. As *structured knowledge* — entities that have properties, connected by relationships that have meaning.

A human brain doesn't store "Alice" as an embedding. It stores Alice as a node in a web of associations: Alice *manages* Bob, Alice *works at* Acme, Alice *has access to* the production cluster, Alice *approved* the last deployment. The structure *is* the understanding.

Uni's property graph is this structured memory. Schema-typed, relationship-rich, queryable via OpenCypher. Backed by columnar storage (Arrow/DataFusion) for analytical performance. Persisted to object stores (S3/GCS/Azure) for cloud-native durability. Running embedded, in-process, with zero infrastructure overhead.

This is the substrate on which everything else operates.

### Pillar 2: Graph Traversal + Semantic Search = Associative Memory

Human memory is *associatively accessible* in two ways: by following chains of connections ("who does Alice manage? what systems do they have access to?") and by similarity ("what's similar to this situation?"). These work together, not in separate silos.

Today's AI stack forces a choice. Graph databases give traversal but no semantic recall. Vector stores give similarity but no structure.

Uni combines both. A single query can start with vector similarity (HNSW, IVF_PQ), traverse graph relationships (OpenCypher pattern matching), apply full-text search (BM25), fuse rankings (reciprocal rank fusion via `uni.search`), and filter on structured properties — all in one pass.

The existing RAG notebooks demonstrate the pattern: vector seeds into graph bridging via shared entity mentions into assembled context. This is how associative memory actually works — similarity and structure operating together.

### Pillar 3: Locy Rules Encode the Physics of the Domain

In a physical world model, "physics" means the rules governing how the world behaves. In structured, relational domains, the physics is different but equally real:

- "If a user has Role X, and Role X inherits from Role Y, and Role Y grants Permission Z, then the user effectively has Permission Z." — the physics of access control.
- "If Supplier A fails, and Part B is sourced exclusively from Supplier A, and Product C requires Part B, then Product C is at risk." — the physics of supply chains.
- "If a batch deviates and feeds into a downstream campaign, risk propagates with a carry factor along each hop." — the physics of manufacturing quality.

Locy — Logic + Cypher — encodes this physics as formal, executable rules with recursive evaluation to guaranteed fixed points. Rules are:

**Predefined and versioned.** Authored, reviewed, tested against Uni's TCK (117/117 scenarios passing at 100%), deployed, and updated as the domain evolves. Version-controlled like code.

**Composable.** The module system (`MODULE acme.compliance; USE acme.common`) lets different teams own different domains of physics. They compose cleanly via stratified evaluation with guaranteed convergence. Security owns `acme.security`. Supply chain owns `acme.supply`. They interoperate without conflicts.

**Evolvable.** As the system encounters new patterns, new rules can be generated and added. An LLM observes a pattern, formulates a candidate rule in Locy, and the system tests it formally via ASSUME. If it holds, the rule enters the knowledge base. The domain's physics literally grows richer over time.

This creates a feedback loop between System 1 and System 2:

```
LLM observes pattern (System 1)
    → formulates candidate rule (natural language → Locy)
    → system tests rule via ASSUME/QUERY (System 2 verification)
    → if valid, rule enters the knowledge base
    → richer physics → better reasoning → better observations → better rules
```

When an LLM "reasons" via chain-of-thought, it generates tokens that *look* like inference. When Locy evaluates rules, it *performs* inference — with termination guarantees, stratified negation safety, and semi-naive evaluation for efficiency. The difference is the difference between writing "2+2=4" in an essay and actually computing it.

### Pillar 4: Simulate the Future, Imagine Alternatives

The physics is encoded. Now you can *use* it.

**ASSUME + DERIVE: Forward simulation.** "Given the world as it is, what happens if I change X?"

```cypher
ASSUME {
    DELETE (firewall)-[:PROTECTS]->(server)
}
THEN {
    QUERY reachable_from_internet 
    WHERE target.name = 'DatabaseServer'
    RETURN attack_path, risk_score
}
```

Apply a hypothetical mutation inside a transaction savepoint. All rules re-evaluate under the hypothetical state. Results come back. The savepoint rolls back automatically. The world model is unchanged. This is *formal simulation over a structured world model* — not an LLM imagining consequences, but a reasoning engine computing them.

**ABDUCE: Backward imagination.** "Given a desired future, what changes would produce it?"

```cypher
ABDUCE compliant 
WHERE system.name = 'ProductionCluster' 
RETURN required_changes
```

The system searches backward from a desired conclusion to the minimal set of world modifications that would make it true. "What's the smallest set of changes that makes this system compliant?" "What edges need to exist for this node to be reachable?" The agent doesn't generate candidate plans and hope. The reasoning engine *searches* for the minimal intervention.

Together: **two directions of the same cognitive capability — thinking about states of the world that don't currently exist.** Forward simulation and counterfactual reasoning. The ability to mentally explore alternate realities before committing to action. This is what distinguishes deliberate planning from reactive behavior.

### Pillar 5: Transparent, Explainable Reasoning

Every conclusion Uni reaches via Locy has a full derivation tree.

```cypher
EXPLAIN RULE flagged 
WHERE account.id = 'ACC-001' 
RETURN derivation
```

"This account was flagged (rule: risk_chain, iteration 3) because it received a transfer from Account X (rule: risk_chain, iteration 2) which was flagged (rule: flagged, base case) because its fraud_score exceeded 0.8."

Not "the model said so." Not confidence scores. An auditable chain of logic from conclusion back to base facts. For compliance: audit trails. For debugging: root cause analysis. For trust: the difference between "trust me" and "here's exactly why."

---

## 5. The Composite Picture

| Cognitive Function | What It Does | Uni Component |
|---|---|---|
| **Structured memory** | Know the world as entities and relationships | Property graph (schema-typed, OpenCypher, columnar) |
| **Associative recall** | Find relevant knowledge by connection *or* similarity | Graph traversal + hybrid vector/FTS search (unified) |
| **Domain physics** | Encode how the world behaves as formal rules | Locy rules (versioned, modular, evolvable) |
| **Mental simulation** | Predict consequences of hypothetical actions | ASSUME + DERIVE (automatic rollback) |
| **Counterfactual reasoning** | Imagine what changes would achieve a goal | ABDUCE (backward search from goals to actions) |
| **Introspection** | Explain your own reasoning transparently | EXPLAIN RULE (full proof traces) |

This is a **cognitive architecture for structured domains** — the working memory, the rule system, the simulation engine, and the explanation mechanism. It's where structured thinking actually happens.

---

## 6. Competitive Landscape

### The Capability Matrix

| Capability | Uni | CozoDB | TypeDB | Neo4j |
|---|---|---|---|---|
| Embedded (in-process) | ✅ | ✅ | ❌ Server | ❌ Server |
| Property graph + OpenCypher | ✅ | ❌ Datalog | ❌ TypeQL | ✅ |
| Recursive logic rules | ✅ Locy | ✅ Datalog | ✅ TypeQL | ❌ |
| Vector search (HNSW/IVF_PQ) | ✅ | ✅ HNSW | ❌ Planned | ✅ Recent |
| Hybrid search with rank fusion | ✅ RRF | ❌ | ❌ | ❌ |
| 36 built-in graph algorithms | ✅ | Partial | ❌ | ✅ GDS |
| **Hypothetical simulation (ASSUME)** | **✅** | **❌** | **❌** | **❌** |
| **Abductive reasoning (ABDUCE)** | **✅** | **❌** | **❌** | **❌** |
| **Proof traces (EXPLAIN RULE)** | **✅** | **❌** | **Partial** | **❌** |
| Rule module system | ✅ | ❌ | ❌ | ❌ |
| Time travel / snapshots | ✅ | ✅ | ❌ | ❌ |
| Object store backend (S3/GCS/Azure) | ✅ | ❌ | ❌ | ❌ |
| Columnar analytics (Arrow/DataFusion) | ✅ | ❌ | ❌ | ❌ |
| Apache 2.0 license | ✅ | MPL 2.0 | MPL 2.0 | GPL (CE) |

### Where Uni Stands Alone

CozoDB is strong on embedded + Datalog + vector. TypeDB has genuine rule inference and a strong type system. Both are credible systems worth respecting.

What no other shipping product offers is the **simulation-planning-explanation triad**: ASSUME for hypothetical what-if with automatic rollback, ABDUCE for backward search from goals to minimal interventions, and EXPLAIN RULE for full derivation trees — all integrated with graph storage, vector search, hybrid retrieval, columnar analytics, and 36 graph algorithms in a single embedded engine.

This triad is what transforms a database with rules into **reasoning infrastructure**.

---

## 7. Go-to-Market

### Positioning

> **"Uni is the reasoning and memory infrastructure for intelligent systems."**

### The Story

> "Every AI agent needs a brain. Vector stores gave agents memory. Uni gives them *reasoning* — the ability to simulate futures, plan from goals, and explain every conclusion. One `pip install`. Zero servers."

### Target Audiences

**Primary: Teams building agentic AI.** Coding agents, security agents, compliance agents, DevOps agents, research agents — anyone whose agent needs to understand relationships, compute transitive effects, plan before acting, or explain its decisions.

**Secondary: Startups and mid-size companies** who want graph + AI capabilities without managing Neo4j, Pinecone, and a separate analytics warehouse.

**Tertiary: Data scientists and researchers** who want graph modeling, algorithms, and vector search in Jupyter without leaving Python.

### Wedge Use Cases

1. **Security / compliance** — RBAC resolution, blast radius analysis, audit explainability. ABDUCE answers "what do I need to fix to become compliant?"
2. **Code agents** — codebase as a graph, impact analysis via Locy, hypothetical refactoring via ASSUME.
3. **Supply chain / operations** — BOM explosion, risk propagation, what-if scenario planning.
4. **Pharma / manufacturing** — batch genealogy, deviation propagation, remediation decisioning.
5. **Cyber security** — exposure-to-remediation reasoning, hybrid evidence retrieval, risk prioritization.
6. **Research / knowledge management** — citation graphs, entity-linked RAG, hybrid recommendation.

### Channels

**Developer-led growth.** The product sells itself through notebooks and docs. The existing portfolio — sales analytics, supply chain, RAG, compliance remediation, pharma batch genealogy, cyber exposure twin — covers the wedge use cases. These should be front and center.

**Content marketing themes:**

- "The reasoning layer LLMs are missing"
- "Why ASSUME beats chain-of-thought for agent planning"
- "From five databases to one import statement"
- "Graph + AI without the infrastructure tax"

**Community.** Discord or forum for early adopters. Conference talks at PyCon, RustConf, AI/ML meetups. The Rust + Python dual-language story appeals broadly. Apache 2.0 and clear testing infrastructure (100% TCK pass rates for both OpenCypher and Locy) lower contribution barriers.

### Business Model Direction

Apache 2.0 embedded library drives adoption. Commercial value captured via: managed cloud service (hosted Uni with dashboards), enterprise support and SLAs, or commercial tier with features like multi-writer replication, RBAC, and audit logging. Let the product sell itself through notebooks. Capture value when teams go to production.

---

## 8. The Pitch

### For the Research-Aware Audience

> "LeCun is right that LLMs can't reason and intelligence needs world models and System 2. The structured knowledge domains that enterprises run on — policies, hierarchies, dependencies, regulations — need their own reasoning engine. Uni is that engine: a property graph for structured knowledge, a logic programming layer for verifiable inference, hypothetical simulation for planning, and proof traces for explainability. System 2 for structured domains, shipping today."

### For the Pragmatic Builder

> "Your agents need to reason over structured knowledge — who reports to whom, what depends on what, which policies apply, what breaks if this fails. LLMs hallucinate these answers. Uni computes them. Graph for structure. Locy for logic. ASSUME for what-if. ABDUCE for how-to-fix. EXPLAIN for why. One `pip install`, zero servers."

### For the Enterprise Buyer

> "Your AI agents are making decisions you can't audit, based on reasoning you can't verify, using memory you can't inspect. Uni gives you explainable AI decisions with full proof traces, hypothetical impact analysis before actions are taken, and time-travel audit trails — all running inside your security perimeter, persisting to your own object store."

### The One-Liner

> **"Uni gives intelligent systems structured memory, formal reasoning, and explainable decisions — embedded, with zero infrastructure."**

### The Thesis

> "The world is building agents. Agents need structured knowledge and formal reasoning, not just language models and vector stores. Uni is the only embedded engine that provides both — with cognitive primitives no other system has: hypothetical simulation, abductive planning, and explainable inference. That's not a database. That's the reasoning infrastructure for the agentic era."

---

## Appendix: Technical Quick Reference

| Feature | What It Is | Why It Matters |
|---|---|---|
| **Embedded/Serverless** | `Uni::open("./my-graph").build()` | No Docker, no ports, no ops. In-process like SQLite. |
| **Object-Store-First** | S3/GCS/Azure native backends | Cloud durability without managing infrastructure. |
| **OpenCypher (100% TCK)** | Standard graph query language | Existing Cypher knowledge transfers. Migration path from Neo4j. |
| **Columnar Analytics** | Arrow/DataFusion engine | Analytical performance without a separate warehouse. |
| **36 Graph Algorithms** | `CALL algo.pageRank(...)` | Centrality, community, pathfinding, flow — built in. |
| **Hybrid Search** | Vector + FTS with RRF fusion | Semantic and lexical retrieval unified in `uni.search`. |
| **Locy (100% TCK)** | Logic + Cypher programming | Recursive rules, stratified fixpoint, semi-naive evaluation. |
| **ASSUME** | Hypothetical what-if with rollback | Simulate future states without mutation. |
| **ABDUCE** | Backward search from goals | "What changes achieve this outcome?" |
| **EXPLAIN RULE** | Full derivation trees | Auditable proof chains for every conclusion. |
| **Module System** | `MODULE` / `USE` composition | Teams own different rule domains; they compose cleanly. |
| **Snapshots & Time Travel** | `VERSION AS OF` queries | Recover any historical state. Replay reasoning at any point. |
| **Pydantic OGM** | Type-safe Python models | Modern DX with validation, lifecycle hooks, relationships. |
| **Apache 2.0** | Permissive license | No GPL restrictions for commercial deployment. |
| **CRDTs** | Conflict-free replicated data types | GCounter, ORSet, LWWMap, VectorClock — built in. |
| **Window Functions** | `ROW_NUMBER`, `RANK`, `LAG` OVER | Partitioned analytics in a single query. |
| **Auto-Embedding** | Candle/MistralRS integration | Generate embeddings on insert, no external API needed. |
| **Bulk Ingest** | High-throughput loading API | Initial loads and large updates at scale. |

---

*Version 1.0 — March 2026*
