# Why Uni

**AI agents can generate anything. They can't reason about anything.**

Today's AI agents retrieve documents, generate text, and call tools. But they cannot reason over structured knowledge, simulate consequences before acting, or explain why they reached a conclusion. They operate in a world of token prediction — fluent, confident, and structurally blind.

The missing layer isn't another vector store or graph database. It's cognitive infrastructure: a unified engine where an agent can store structured knowledge, recall it by meaning or by relationship, apply formal rules, test hypotheticals, and trace every conclusion back to the facts that produced it.

That's what Uni provides. Not a bigger context window. Not a better prompt. A reasoning substrate.

---

## The Duct-Tape Architecture

A typical 2026 agent stack looks something like this: a graph database for relationships, a vector store for semantic search, a full-text index for keyword retrieval, a custom rules engine for business logic, and a few hundred lines of glue code to stitch it all together.

Each system has its own data model, its own consistency boundary, its own deployment overhead. The graph database runs as a separate server process. The vector store runs as another. The rules engine is a bespoke script that nobody wants to maintain. Data flows between them through ETL pipelines that break silently.

The agent can't reason *across* these systems. It queries each one independently and stitches results together with prompt engineering. "Here are the graph relationships. Here are the similar documents. Here are the rules that fired. Now, LLM, figure out what to do." The LLM obliges — fluently, confidently, and without any formal guarantee that the answer is consistent with the facts it was given.

This architecture lacks what engineers call *mechanical sympathy* — it isn't designed for how agents actually need to think. Agents need to traverse a relationship chain, check whether the result is semantically relevant, apply domain rules, simulate what would happen if a fact changed, and explain the whole chain. That requires one engine, not five services and a prayer.

The result is fragile, slow, unexplainable, and impossible to audit. Not because any individual component is bad, but because the architecture is a category error — it treats cognition as a data plumbing problem.

---

## Five Cognitive Gaps

Strip away the tooling and look at what agents actually lack. There are five gaps, and no single product in the current ecosystem closes more than one or two of them.

### 1. No Structured Memory

Vector stores capture meaning but lose relationships. Graph databases capture relationships but don't embed. An agent that needs both — and they all do — ends up maintaining two systems with two data models and no unified query path. Knowledge gets fragmented across storage backends, and the agent loses the ability to ask questions that span both structure and semantics.

[How Uni closes this gap →](index.md#structured-memory)

### 2. No Associative Recall

Retrieval today forces a choice: search by meaning (vectors) or search by structure (graph traversal) or search by keyword (full-text). But cognition doesn't work that way. An agent diagnosing a production incident needs to find components *semantically similar* to the failing one, then traverse dependency edges to find upstream causes, then filter by keyword for the specific error signature. That's one thought, not three queries to three systems.

[How Uni closes this gap →](index.md#associative-recall)

### 3. No Domain Physics

Chain-of-thought prompting generates tokens that *look* like reasoning but aren't formal inference. The LLM doesn't know your business rules — it hallucinates plausible ones. Real domain reasoning requires declared rules evaluated by a logic engine: if a supplier is high-risk AND they're a sole source, then the component is critical. That's not a prompt. That's a rule, and it needs to be deterministic, reproducible, and version-controlled.

[How Uni closes this gap →](index.md#domain-physics)

### 4. No Mental Simulation

Agents act and hope. There's no way to ask "what if supplier X goes offline?" without actually changing the data, running the query, and then reverting. Real planning requires hypothetical reasoning — temporarily assuming facts, propagating consequences through rules, and inspecting the results — all without mutating the real state. Without this, agents can't plan. They can only react.

[How Uni closes this gap →](index.md#mental-simulation)

### 5. No Explainable Decisions

"The model said so" isn't auditable. When an agent recommends rejecting a loan, flagging a transaction, or rerouting a supply chain, someone needs to know *why*. Not a confidence score. Not an attention heatmap. A formal derivation chain: this conclusion follows from these rules applied to these facts. Today, that chain doesn't exist because the reasoning never happened — the LLM generated an answer that looked right.

[How Uni closes this gap →](index.md#explainable-decisions)

---

## Where Alternatives Fall Short

Each tool in the current ecosystem solves a real problem. None of them solve the right problem.

### Graph Databases (Neo4j, etc.)

Relationships without reasoning. Graph databases store and traverse structured data well, but they don't do recursive logic programming, hypothetical reasoning, or abductive inference. Queries are ad-hoc Cypher statements, not declared rules that compose and propagate. There's no `ASSUME`, no `ABDUCE`, no stratified negation. And every query crosses a network boundary — a separate server process with its own deployment, scaling, and failure modes.

### Vector Stores (Pinecone, Chroma, etc.)

Similarity without structure. Vector stores find "semantically similar" items, but they can't follow relationship chains, enforce schema constraints, or apply graph algorithms. They answer "what's near this?" but not "what's connected to this, and what rules apply to the result?" Without structure, similarity is a blunt instrument.

### Agent Memory Systems (Mem0, Zep, Graphiti)

Store and retrieve memories, but can't *reason* over them. These systems give agents persistence — they can remember past interactions and retrieved facts. But memory without cognition is a filing cabinet, not a brain. No formal rules, no simulation, no proof traces. The agent can recall that something happened; it can't derive what that implies.

### Agent Frameworks (LangGraph, CrewAI, etc.)

Orchestrate agent workflows, but don't provide cognitive infrastructure. The framework decides *when* the agent thinks and *which tools* it calls. But the agent still needs somewhere to store structured knowledge AND reason over it. Orchestration is necessary but not sufficient — it's the nervous system without the cortex.

---

## Uni's Design Philosophy

Uni doesn't try to do everything. It does one thing: give agents the cognitive infrastructure they need to reason, not just retrieve.

### Reasoning-first, not storage-first

The graph, vectors, and full-text index exist to support five cognitive capabilities — structured memory, associative recall, domain physics, mental simulation, and explainable decisions. Storage is a means, not the end. Every data structure in Uni earns its place by enabling a reasoning pattern, not by checking a feature-matrix box.

### One engine, not a stack

Graph traversals, vector search, full-text retrieval, logic programming, and hypothetical reasoning all execute against the same data, in the same process, with the same transactional guarantees. No ETL pipelines. No cross-system joins. No consistency boundaries to reconcile. One query can traverse a graph edge, filter by vector similarity, apply a Locy rule, and return an explained result — because it's all one engine.

### Embedded, not client-server

Uni runs in the agent's process. `pip install uni-db` and it's there — no Docker containers, no connection strings, no server process to monitor. Reasoning happens at memory speed, not network speed. For agents making dozens of cognitive operations per decision loop, the difference between a function call and a network round-trip is the difference between real-time and too-slow.

### Formal inference, not token generation

Locy rules are evaluated by a logic engine using SLG resolution with stratified negation — not generated by an LLM. When a rule says "a component is critical if its supplier is high-risk and it has no alternative source," that evaluation is deterministic, reproducible, and auditable. It produces the same answer every time, on every machine, regardless of temperature settings. The LLM decides *what to ask*. The logic engine decides *what's true*.

### Auditable by design

`EXPLAIN RULE` produces a proof trace from conclusion back to base facts. Not a confidence score. Not "the model is 87% sure." A formal derivation chain: this conclusion was derived from this rule, which matched these facts, which were retrieved from these graph paths. When a regulator asks "why did the system flag this transaction?", you hand them a proof — not a prompt log.

---

## See It In Action

See a complete example — defining rules, simulating a what-if, explaining a decision, and computing a minimal fix — on the [homepage](index.md#see-it-in-action) or in the [Quick Start](getting-started/quickstart.md).

---

## Get Started

<div class="grid cards" markdown>

-   **Install Uni**

    One command. No servers, no Docker, no infrastructure.

    [Installation Guide →](getting-started/installation.md)

-   **Quick Start**

    Five minutes through all five cognitive capabilities.

    [Quick Start →](getting-started/quickstart.md)

-   **Locy Reasoning**

    Rules, simulation, abduction, and proof traces.

    [Locy Overview →](locy/index.md)

</div>
