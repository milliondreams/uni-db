
**Engineering Specification**

Locy Session: Multi-Client Isolation for Locy Evaluation

Version 1.0 — March 2026

# **Table of Contents**

1. [Motivation](#1-motivation)
2. [Design Goals](#2-design-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [Core Types](#4-core-types)
5. [Session Lifecycle](#5-session-lifecycle)
6. [Isolation Model](#6-isolation-model)
7. [Commit Protocol](#7-commit-protocol)
8. [ASSUME and Savepoints](#8-assume-and-savepoints)
9. [DERIVE Within Sessions](#9-derive-within-sessions)
10. [Rule Registry Isolation](#10-rule-registry-isolation)
11. [Rust API Reference](#11-rust-api-reference)
12. [Python API Reference](#12-python-api-reference)
13. [Conflict Detection](#13-conflict-detection)
14. [Implementation Plan](#14-implementation-plan)
15. [Files to Create](#15-files-to-create)
16. [Files to Modify](#16-files-to-modify)
17. [Test Plan](#17-test-plan)
18. [Design Decisions and Trade-offs](#18-design-decisions-and-trade-offs)
19. [Future Work](#19-future-work)

# **1. Motivation**

Today, Locy evaluation runs against the shared `Uni` database with no client-level isolation. The `LocyEngine` borrows `&Uni` and operates directly on the shared Writer, L0 buffer, and rule registry. This creates three problems:

**Problem 1: ASSUME serializes on the shared Writer.** `begin_savepoint()` acquires `Arc<RwLock<Writer>>` for the entire duration of an ASSUME block. Two concurrent clients cannot both run ASSUME — they serialize or deadlock on the Writer lock.

**Problem 2: DERIVE mutations are globally visible immediately.** When a client runs `DERIVE reachable(X,Y) :- path(X,Y).`, the materialized facts are written to the shared L0. Another client's evaluation may observe partially-materialized facts mid-fixpoint.

**Problem 3: Rule registration leaks across clients.** The shared `locy_rule_registry` means rules registered by one client are visible to all others. There is no concept of a private rule namespace.

These problems prevent multi-client scenarios: parallel notebook cells, concurrent API requests, batch pipelines running alongside interactive queries.

**Theoretical basis.** Database systems solve this with transaction isolation (Berenson et al., "A Critique of ANSI SQL Isolation Levels", SIGMOD 1995). The session model proposed here provides **Read Committed** isolation with **optimistic concurrency control** (OCC) at commit time. This matches the existing Uni transaction model, which uses a similar single-writer-with-buffered-writes approach.

# **2. Design Goals**

1. **Parallel evaluation.** Multiple Locy sessions can evaluate, ASSUME, ABDUCE, and DERIVE concurrently without blocking each other.
2. **Write isolation.** Mutations within a session are invisible to other sessions until explicitly committed.
3. **Rule isolation.** Rules registered in a session are private to that session until committed.
4. **Minimal lock contention.** The shared Writer lock is held only during commit (milliseconds), not during evaluation (potentially seconds).
5. **Reuse existing machinery.** The session Writer shares the same `IdAllocator`, `StorageManager`, `SchemaManager`, and L0 merge logic. No duplication of constraint validation or ID allocation.
6. **Backward compatible.** The existing `db.locy().evaluate()` API continues to work unchanged, operating on the shared state as before.

# **3. Architecture Overview**

```
┌──────────────────────────────────────────────────────────┐
│                     Uni (shared)                         │
│  StorageManager    SchemaManager    PropertyManager      │
│  IdAllocator (Arc) AdjacencyManager Writer (shared)      │
│  locy_rule_registry (Arc<RwLock<LocyRuleRegistry>>)      │
└──────────┬──────────────────┬────────────────────────────┘
           │                  │
   ┌───────▼────────┐  ┌─────▼──────────┐
   │  LocySession A │  │  LocySession B │
   │                │  │                │
   │  session_writer│  │  session_writer│  ← private Writer clone
   │   ├ L0Manager  │  │   ├ L0Manager  │  ← private L0 buffer
   │   ├ AdjMgr     │  │   ├ AdjMgr     │  ← private adjacency overlay
   │   └ IdAlloc ───┼──┼───┘ (shared)   │  ← shared Arc<IdAllocator>
   │                │  │                │
   │  rule_registry │  │  rule_registry │  ← cloned at session start
   └────────────────┘  └────────────────┘
```

Each session owns a **session-scoped Writer** — a lightweight clone of the shared Writer that shares `Arc`-wrapped read-only components but has its own:
- `L0Manager` with a private `L0Buffer` for mutations
- `AdjacencyManager` for session-private edge traversal

The shared `IdAllocator` is safe to share because it uses `tokio::Mutex<AllocatorState>` with batch-based allocation — concurrent sessions get disjoint ID ranges.

# **4. Core Types**

## 4.1 LocySession

```rust
/// An isolated Locy evaluation context.
///
/// All mutations (DERIVE, Cypher within Locy) write to a session-private
/// L0 buffer. Reads see the shared database state (read-committed) plus
/// the session's own uncommitted mutations.
///
/// On commit, session mutations are merged into the shared database with
/// OCC conflict detection. On rollback (or Drop), session state is discarded.
pub struct LocySession {
    /// Shared database reference.
    db: Arc<Uni>,

    /// Session-scoped Writer with private L0 and AdjacencyManager.
    /// Shares IdAllocator, StorageManager, SchemaManager with the shared Writer.
    session_writer: Arc<RwLock<Writer>>,

    /// Clone-on-write rule registry. Cloned from db.locy_rule_registry at
    /// session creation. Mutations are session-local until commit.
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,

    /// Entities present in the shared L0 at session creation time.
    /// Used for write-set conflict detection at commit time.
    /// Maps VID/EID to the version they had when the session was created.
    baseline_vertex_versions: HashMap<Vid, u64>,
    baseline_edge_versions: HashMap<Eid, u64>,

    /// Session identifier for logging and diagnostics.
    id: String,

    /// Whether commit or rollback has been called.
    completed: bool,
}
```

## 4.2 LocySessionEngine

```rust
/// Locy evaluation engine scoped to a session.
///
/// Analogous to `LocyEngine` but reads/writes against the session's
/// private state. Returned by `LocySession::locy()`.
pub struct LocySessionEngine<'a> {
    session: &'a LocySession,
}
```

## 4.3 NativeSessionAdapter

```rust
/// Bridges Locy execution to the session-scoped Writer.
///
/// Analogous to `NativeExecutionAdapter` but routes all mutations
/// to the session Writer's L0 buffer.
pub(crate) struct NativeSessionAdapter<'a> {
    db: &'a Uni,
    session_writer: Arc<RwLock<Writer>>,
    warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
}
```

This struct implements the `LocyExecutionContext` trait, providing `execute_pattern`, `execute_mutation`, `begin_savepoint`, and `rollback_savepoint`. All mutations route to the session Writer.

## 4.4 CommitResult

```rust
pub struct CommitResult {
    /// Number of graph mutations (vertex/edge creates/deletes) committed.
    pub mutations_committed: usize,
    /// Number of rule definitions promoted to the shared registry.
    pub rules_promoted: usize,
}
```

## 4.5 Error Variants

```rust
/// OCC conflict: another session modified an entity this session also wrote.
UniError::SessionConflict { entity: String }

/// Commit or rollback already called.
UniError::SessionAlreadyCompleted

/// Rule name collision during commit.
UniError::RuleConflict { rule_name: String }
```

# **5. Session Lifecycle**

```
  locy_session()        evaluate() / DERIVE / ASSUME
       │                     │   │   │
       ▼                     ▼   ▼   ▼
   ┌────────┐  locy()  ┌──────────────────┐
   │ CREATED ├────────►│     ACTIVE        │
   └────────┘          │                  │
                       │  Session Writer  │
                       │  receives all    │
                       │  mutations       │
                       └───────┬──────────┘
                               │
                    ┌──────────┼──────────┐
                    ▼                     ▼
              ┌──────────┐         ┌───────────┐
              │ COMMITTED │         │ ROLLED    │
              │           │         │ BACK      │
              └──────────┘         └───────────┘
                    │                     │
                    ▼                     ▼
              Mutations merged      Session state
              to shared Writer      discarded
              Rules promoted
```

1. **Creation** (`db.locy_session().await`): Reads the shared Writer (briefly) to clone Arc fields and create a session-scoped Writer with a fresh L0. Clones the rule registry.

2. **Active**: All `evaluate()`, `register()`, DERIVE, ASSUME, ABDUCE calls operate against the session state.

3. **Commit** (`session.commit().await`): Acquires the shared Writer lock, performs write-set conflict check, writes session L0 to WAL, merges into shared L0, replays adjacency entries, promotes rules. Releases lock.

4. **Rollback** (`session.rollback()` or `Drop`): Discards session Writer, L0, and rule registry. No shared lock needed.

# **6. Isolation Model**

## 6.1 Read Isolation: Read Committed

Each `evaluate()` call within a session sees:

1. **Shared L0 chain** (current state): `pending_flush_l0s → main_l0`
2. **Session L0** (as `transaction_l0` in `QueryContext`): session's uncommitted mutations

The session does **not** capture a frozen snapshot at creation time. If another session commits between two evaluations in the same session, the second evaluation sees the committed data. This is **Read Committed** isolation.

```
Session A:                   Session B:
  evaluate("?- count(X).")
    → sees 10 rows
                              DERIVE new_fact(...).
                              commit()
  evaluate("?- count(X).")
    → sees 11 rows            ← reads live shared state
```

**Rationale.** For exploratory Locy sessions (notebooks, interactive queries), users generally want to see fresh data. True snapshot isolation would require `Uni::at_snapshot()` which creates a read-only database pinned to a past state — this is available as a future enhancement (see §19).

## 6.2 Write Isolation: Session-Private L0

All mutations within a session write to the session Writer's L0 buffer:

```
Session A:                   Session B:
  DERIVE hot(X) :- ...
    → writes to Session A's L0
                              evaluate("?- hot(X).")
                                → sees nothing (A's L0 is private)
  commit()
                              evaluate("?- hot(X).")
                                → sees hot(X) facts
```

## 6.3 Rule Isolation: Cloned Registry

Rules registered in a session are visible only within that session:

```
Session A:                   Session B:
  register("path(X,Y) :- edge(X,Y).")
                              evaluate("?- path(a, X).")
                                → ERROR: unknown predicate 'path'
  commit()
                              evaluate("?- path(a, X).")
                                → OK: rules now in shared registry
```

# **7. Commit Protocol**

## 7.1 Overview

```
commit()
  │
  ├─ 1. Acquire shared Writer lock (write)
  │
  ├─ 2. Conflict detection (write-set intersection)
  │     • For each VID in session L0's vertex_properties ∪ vertex_tombstones:
  │       check if VID exists in shared main_l0 or pending_flush_l0s
  │       with a different version than at session creation
  │     • Same for EIDs
  │     • New IDs (from shared IdAllocator) never conflict
  │
  ├─ 3. Write session L0 mutations to WAL
  │     (same pattern as Writer::commit_transaction, lines 226-283)
  │
  ├─ 4. Flush WAL — THIS IS THE COMMIT POINT
  │     (if this fails, session remains active, can retry)
  │
  ├─ 5. Merge session L0 → shared main L0
  │     (calls L0Buffer::merge, same as commit_transaction line 295)
  │
  ├─ 6. Replay edges → shared AdjacencyManager
  │     (same pattern as commit_transaction lines 300-313)
  │
  ├─ 7. Promote rules to shared registry (with conflict policy)
  │
  ├─ 8. Release shared Writer lock
  │
  └─ 9. Clear session state, set completed = true
```

## 7.2 Writer::merge_session_l0

A new method on `Writer` that encapsulates steps 2-6:

```rust
impl Writer {
    /// Merge a session's L0 buffer into the shared L0.
    ///
    /// Performs write-set conflict detection, WAL durability, L0 merge,
    /// and adjacency replay. The caller must hold &mut self (exclusive
    /// Writer access).
    pub async fn merge_session_l0(
        &mut self,
        session_l0: &L0Buffer,
        session_adjacency: &AdjacencyManager,
    ) -> Result<()> {
        // 1. Conflict detection
        let main_l0_arc = self.l0_manager.get_current();
        {
            let main_l0 = main_l0_arc.read();
            self.check_write_set_conflicts(&main_l0, session_l0)?;
            // Also check pending_flush_l0s
            for pending in self.l0_manager.get_pending_flush() {
                let pending_l0 = pending.read();
                self.check_write_set_conflicts(&pending_l0, session_l0)?;
            }
        }

        // 2. WAL write (same pattern as commit_transaction)
        {
            let main_l0 = main_l0_arc.read();
            if let Some(wal) = main_l0.wal.as_ref() {
                // Write vertex insertions, deletions, edge insertions, deletions
                // ... (reuse existing WAL mutation serialization)
            }
        }

        // 3. Flush WAL (commit point)
        self.flush_wal().await?;

        // 4. Merge L0
        {
            let mut main_l0 = main_l0_arc.write();
            main_l0.merge(session_l0)?;
        }

        // 5. Replay adjacency entries
        self.replay_session_adjacency(session_adjacency, session_l0)?;

        Ok(())
    }
}
```

## 7.3 Durability Guarantee

The WAL flush (step 4) is the commit point. If the process crashes:
- **Before WAL flush**: session mutations are lost. No shared state was modified.
- **After WAL flush, before L0 merge**: WAL replay on restart re-applies the mutations.
- **After L0 merge**: mutations are visible and durable.

This matches the existing transaction commit guarantee.

# **8. ASSUME and Savepoints**

ASSUME uses savepoints to create a temporary mutation scope:

```
ASSUME { CREATE (:Node {name: "x"}) }
?- path("x", Y).
```

Execution:
1. `begin_savepoint()` — creates a nested `transaction_l0` on the **session Writer**
2. Execute mutations (CREATE) — writes to the nested `transaction_l0`
3. Evaluate body (`?- path("x", Y).`) — reads see: nested L0 → session L0 → shared L0
4. `rollback_savepoint()` — discards the nested `transaction_l0`

The critical point: savepoints operate on the **session Writer**, not the shared Writer. This means:

- Two concurrent sessions can both run ASSUME simultaneously (no lock contention)
- ASSUME mutations are doubly isolated: invisible even within the session after rollback
- The session's own L0 is unaffected by ASSUME's temporary mutations

```rust
impl NativeSessionAdapter {
    async fn begin_savepoint(&self) -> Result<SavepointId, LocyError> {
        let mut w = self.session_writer.write().await;
        w.begin_transaction()?;  // creates nested transaction_l0
        Ok(SavepointId(0))
    }

    async fn rollback_savepoint(&self, _id: SavepointId) -> Result<(), LocyError> {
        let mut w = self.session_writer.write().await;
        w.rollback_transaction()?;  // discards nested transaction_l0
        Ok(())
    }
}
```

# **9. DERIVE Within Sessions**

DERIVE materializes facts by generating Cypher CREATE mutations:

```
DERIVE reachable(X, Y) :- path(X, Y).
```

Execution path:
1. Locy fixpoint evaluation runs the body (`path(X, Y)`) — reads shared + session L0
2. For each derived fact, `derive_command()` builds a Cypher CREATE
3. `execute_mutation()` on `NativeSessionAdapter` routes the CREATE to the session Writer
4. The new vertex/edge lands in the session's L0 with a fresh ID from the shared `IdAllocator`

**On commit:** The derived facts merge into the shared L0 like any other mutation. Conflict detection only checks entity-level overlap, and since DERIVE creates new entities with unique IDs, it almost never conflicts.

**What is NOT committed:** The derivation provenance (which rules produced which facts) is ephemeral — it exists only during evaluation. Only the materialized graph mutations persist.

# **10. Rule Registry Isolation**

## 10.1 Clone at Session Start

```rust
let shared_registry = db.locy_rule_registry.read().unwrap();
let session_registry = shared_registry.clone();
```

The `LocyRuleRegistry` is `#[derive(Clone)]` with two fields:
- `rules: HashMap<String, CompiledRule>` — compiled rules by name
- `strata: Vec<Stratum>` — execution strata in dependency order

Cloning is O(n) in the number of registered rules. For typical workloads (tens of rules), this is negligible.

## 10.2 Session-Local Mutations

`register()` modifies only the session's cloned registry:

```rust
impl<'a> LocySessionEngine<'a> {
    pub fn register(&self, program: &str) -> Result<()> {
        let compiled = compile(program)?;
        let mut registry = self.session.rule_registry.write().unwrap();
        for rule in compiled.rules {
            registry.rules.insert(rule.name.clone(), rule);
        }
        registry.strata = recompute_strata(&registry.rules);
        Ok(())
    }
}
```

## 10.3 Rule Promotion on Commit

When a session commits, its rules are promoted to the shared registry. A conflict policy governs what happens when rule names collide:

```rust
pub enum RuleConflictPolicy {
    /// Error if the shared registry already has a rule with the same name.
    /// This is the default — explicit is better than implicit.
    Error,

    /// Replace existing shared rules with session rules.
    /// Use when the session intentionally redefines rules.
    Replace,

    /// Skip session rules that already exist in the shared registry.
    /// Use when the session only wants to add new rules.
    Skip,
}
```

Default is `Error`. The policy can be set per-session:

```rust
let mut session = db.locy_session().await?;
session.set_rule_conflict_policy(RuleConflictPolicy::Replace);
```

## 10.4 Detecting Which Rules are Session-Local

To determine which rules to promote, we compare the session registry against the original shared registry snapshot. Only rules that differ (new or modified) are promoted.

```rust
fn rules_to_promote(
    session: &LocyRuleRegistry,
    baseline: &LocyRuleRegistry,
) -> Vec<(String, CompiledRule)> {
    session.rules.iter()
        .filter(|(name, rule)| {
            baseline.rules.get(*name) != Some(rule)
        })
        .map(|(name, rule)| (name.clone(), rule.clone()))
        .collect()
}
```

# **11. Rust API Reference**

## 11.1 Session Creation

```rust
impl Uni {
    /// Create an isolated Locy evaluation session.
    ///
    /// Briefly reads the shared Writer to clone Arc fields and create
    /// the session-scoped Writer. The session sees shared state (read-committed)
    /// plus its own uncommitted mutations.
    pub async fn locy_session(self: &Arc<Self>) -> Result<LocySession>;
}
```

Takes `&Arc<Self>` because `LocySession` must own an `Arc<Uni>` — the same pattern used by the Python bindings which store `Arc<Uni>` for `Transaction`.

## 11.2 LocySession Methods

```rust
impl LocySession {
    /// Access the session-scoped Locy engine.
    /// Returns a borrowed engine tied to this session's lifetime.
    pub fn locy(&self) -> LocySessionEngine<'_>;

    /// Merge session mutations and rules into the shared database.
    ///
    /// Acquires the shared Writer lock, performs write-set conflict detection,
    /// writes to WAL, merges L0, replays adjacency, promotes rules.
    ///
    /// Returns SessionConflict if another session modified an entity
    /// this session also wrote.
    pub async fn commit(&mut self) -> Result<CommitResult>;

    /// Discard all session state. No I/O, no lock acquisition.
    pub fn rollback(&mut self);

    /// True if session has uncommitted graph mutations or rule changes.
    pub fn is_dirty(&self) -> bool;

    /// Set the conflict policy for rule promotion on commit.
    pub fn set_rule_conflict_policy(&mut self, policy: RuleConflictPolicy);

    /// Session identifier (UUID) for logging.
    pub fn id(&self) -> &str;
}

impl Drop for LocySession {
    fn drop(&mut self) {
        if !self.completed {
            // Auto-rollback: discard session state silently.
            // Log a warning if the session was dirty.
            if self.is_dirty() {
                tracing::warn!(
                    session_id = %self.id,
                    "LocySession dropped with uncommitted mutations — rolling back"
                );
            }
        }
    }
}
```

## 11.3 LocySessionEngine Methods

```rust
impl<'a> LocySessionEngine<'a> {
    /// Register rules in the session's private rule registry.
    /// Sync — modifies only session-local state.
    pub fn register(&self, program: &str) -> Result<()>;

    /// Evaluate a Locy program within the session.
    ///
    /// Reads see shared state (read-committed) plus session mutations.
    /// Writes (DERIVE, Cypher mutations) go to the session's L0.
    /// ASSUME creates nested savepoints on the session Writer.
    pub async fn evaluate(&self, program: &str) -> Result<LocyResult>;

    /// Evaluate with explicit configuration overrides.
    pub async fn evaluate_with_config(
        &self,
        program: &str,
        config: &LocyConfig,
    ) -> Result<LocyResult>;

    /// Compile a Locy program without executing it.
    /// Sync — no I/O.
    pub fn compile_only(&self, program: &str) -> Result<CompiledProgram>;

    /// Clear all rules in the session's private registry.
    /// Does not affect the shared registry.
    pub fn clear_rules(&self);
}
```

## 11.4 Usage Examples

### Basic Session

```rust
let db: Arc<Uni> = UniBuilder::new("./mydb").build().await?;

let mut session = db.locy_session().await?;

session.locy().register(r#"
    path(X, Y) :- edge(X, Y).
    path(X, Z) :- edge(X, Y), path(Y, Z).
"#)?;

let result = session.locy().evaluate("?- path(a, X).").await?;
for row in result.rows() {
    println!("{:?}", row);
}

session.commit().await?;
```

### Parallel Sessions

```rust
let db: Arc<Uni> = UniBuilder::new("./mydb").build().await?;

let (r1, r2) = tokio::join!(
    {
        let db = Arc::clone(&db);
        async move {
            let mut s = db.locy_session().await?;
            s.locy().evaluate("DERIVE hot(X) :- temp(X, T), T > 100.").await?;
            s.commit().await
        }
    },
    {
        let db = Arc::clone(&db);
        async move {
            let mut s = db.locy_session().await?;
            s.locy().evaluate("DERIVE cold(X) :- temp(X, T), T < 0.").await?;
            s.commit().await
        }
    },
);

r1?; // Ok — disjoint write sets, no conflict
r2?; // Ok
```

### OCC Conflict Handling

```rust
let mut s1 = db.locy_session().await?;
let mut s2 = db.locy_session().await?;

// Both sessions mutate the same vertex via Cypher within Locy
s1.locy().evaluate("DERIVE status(node1, 'hot').").await?;
s2.locy().evaluate("DERIVE status(node1, 'cold').").await?;

s1.commit().await?;  // succeeds — first writer wins

match s2.commit().await {
    Err(UniError::SessionConflict { entity }) => {
        eprintln!("Conflict on {entity}, retrying with fresh session...");
        s2.rollback();
        // Create new session and retry
    }
    other => other?,
}
```

### ASSUME (Hypothetical Reasoning)

```rust
let mut session = db.locy_session().await?;

// ASSUME is self-cleaning — mutations exist only during body evaluation
let result = session.locy().evaluate(r#"
    ASSUME { CREATE (:City {name: "Atlantis"})-[:ROAD]->(:City {name: "Athens"}) }
    ?- path("Atlantis", X).
"#).await?;

// Session is still clean — ASSUME rolled back its mutations
assert!(!session.is_dirty());
```

# **12. Python API Reference**

## 12.1 Sync API

```python
class LocySession:
    """Isolated Locy evaluation session with private state.

    Mutations are invisible to other sessions until committed.
    Auto-rollback on context manager exit or garbage collection.
    """

    def register(self, program: str) -> None:
        """Register rules in the session's private registry."""
        ...

    def evaluate(self, program: str) -> LocyResult:
        """Evaluate a Locy program within the session."""
        ...

    def evaluate_with_config(self, program: str, config: dict) -> LocyResult:
        """Evaluate with configuration overrides."""
        ...

    def compile(self, program: str) -> CompiledProgram:
        """Compile without executing."""
        ...

    def clear_rules(self) -> None:
        """Clear session-local rules."""
        ...

    def commit(self) -> CommitResult:
        """Merge session state into the shared database.

        Raises:
            SessionConflictError: if write-set overlaps with concurrent commit.
        """
        ...

    def rollback(self) -> None:
        """Discard all session state."""
        ...

    @property
    def is_dirty(self) -> bool:
        """True if there are uncommitted mutations or rules."""
        ...

    def __enter__(self) -> "LocySession":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self._completed:
            self.rollback()
```

### Sync Usage

```python
import uni_db as uni

db = uni.Database("./mydb")

# Context manager pattern (recommended)
with db.locy_session() as session:
    session.register("""
        path(X, Y) :- edge(X, Y).
        path(X, Z) :- edge(X, Y), path(Y, Z).
    """)
    result = session.evaluate("?- path('a', X).")
    for row in result.rows:
        print(row)
    session.commit()
# Auto-rollback if exception or no commit

# Manual lifecycle
session = db.locy_session()
try:
    session.evaluate("DERIVE reachable(X, Y) :- path(X, Y).")
    session.commit()
except uni.SessionConflictError:
    session.rollback()
```

## 12.2 Async API

```python
class AsyncLocySession:
    """Async variant of LocySession.

    evaluate() and commit() are awaitable. register(), rollback(),
    clear_rules() are sync (session-local, no I/O).
    """

    def register(self, program: str) -> None: ...
    async def evaluate(self, program: str) -> LocyResult: ...
    async def evaluate_with_config(self, program: str, config: dict) -> LocyResult: ...
    def compile(self, program: str) -> CompiledProgram: ...
    def clear_rules(self) -> None: ...
    async def commit(self) -> CommitResult: ...
    def rollback(self) -> None: ...

    @property
    def is_dirty(self) -> bool: ...

    async def __aenter__(self) -> "AsyncLocySession": ...
    async def __aexit__(self, *exc) -> None: ...
```

### Async Usage

```python
import uni_db as uni
import asyncio

db = await uni.AsyncDatabase.open("./mydb")

# Parallel sessions
async def analyze_hot(db):
    async with db.locy_session() as session:
        session.register("hot(X) :- temp(X, T), T > 100.")
        await session.evaluate("DERIVE hot(X) :- temp(X, T), T > 100.")
        await session.commit()

async def analyze_cold(db):
    async with db.locy_session() as session:
        session.register("cold(X) :- temp(X, T), T < 0.")
        await session.evaluate("DERIVE cold(X) :- temp(X, T), T < 0.")
        await session.commit()

await asyncio.gather(analyze_hot(db), analyze_cold(db))

# Interactive notebook pattern
session = db.locy_session()

# Cell 1
session.register("path(X,Y) :- edge(X,Y). path(X,Z) :- edge(X,Y), path(Y,Z).")

# Cell 2
result = await session.evaluate("?- path('a', X).")
result.to_pandas()

# Cell 3 — hypothetical
result = await session.evaluate("""
    ASSUME { CREATE (:Node {name: "bridge"})-[:CONNECTS]->(:Node {name: "island"}) }
    ?- path('a', 'island').
""")

# Cell 4 — commit the rules
await session.commit()
```

## 12.3 CommitResult

```python
class CommitResult:
    mutations_committed: int  # graph mutations merged
    rules_promoted: int       # rules added to shared registry
```

## 12.4 Exception Hierarchy

```python
class UniError(Exception): ...
class SessionConflictError(UniError):
    """Write-set overlap detected at commit time."""
    entity: str  # description of the conflicting entity
class SessionAlreadyCompletedError(UniError):
    """Commit or rollback already called."""
    ...
class RuleConflictError(UniError):
    """Rule name collision during commit."""
    rule_name: str
```

# **13. Conflict Detection**

## 13.1 Write-Set Intersection (OCC)

At commit time, we check whether any entity the session *wrote* (inserted, updated, or deleted) was also modified in the shared state since the session was created.

**Why not version-based OCC?** The `L0Buffer.current_version` is a per-buffer counter that increments on every mutation and resets when the L0 is rotated during flush. It cannot reliably detect inter-session conflicts because:
1. It's too coarse — any mutation bumps the version, even on unrelated entities.
2. L0 rotation resets the counter — the session's baseline version becomes meaningless.

**Write-set intersection** checks at entity granularity:

```rust
fn check_write_set_conflicts(
    shared_l0: &L0Buffer,
    session_l0: &L0Buffer,
) -> Result<()> {
    // Check vertices the session modified
    for vid in session_l0.vertex_properties.keys()
        .chain(session_l0.vertex_tombstones.iter())
    {
        // If this VID also exists in the shared L0 and was
        // NOT there when the session was created, it's a conflict
        if shared_l0.vertex_properties.contains_key(vid)
            || shared_l0.vertex_tombstones.contains(vid)
        {
            // Check if this entity existed at session baseline
            // (if it did, someone else modified it concurrently)
            return Err(UniError::SessionConflict {
                entity: format!("vertex {}", vid),
            });
        }
    }

    // Same for edges
    for eid in session_l0.edge_endpoints.keys() {
        if !session_l0.tombstones.contains_key(eid) {
            continue; // new edges with unique IDs can't conflict
        }
        if shared_l0.edge_endpoints.contains_key(eid)
            || shared_l0.tombstones.contains_key(eid)
        {
            return Err(UniError::SessionConflict {
                entity: format!("edge {}", eid),
            });
        }
    }

    Ok(())
}
```

## 13.2 Why New Inserts Almost Never Conflict

The `IdAllocator` guarantees globally unique IDs:
- It uses `tokio::Mutex<AllocatorState>` with batch reservation
- Each session's Writer shares the same `Arc<IdAllocator>`
- Allocated IDs are never reused

When DERIVE creates new vertices/edges, they get fresh IDs that cannot exist in any other L0. Therefore:
- Pure-insert sessions (the common case for DERIVE) commit conflict-free
- Conflicts only arise when sessions delete or update **pre-existing** entities

## 13.3 Pending Flush L0s

The conflict check must also scan `pending_flush_l0s` — L0 buffers that are being flushed to L1 but still hold recent mutations. If a session was created before a flush started, and another session committed mutations that are now in a pending-flush L0, the conflict check must detect this.

```rust
// In merge_session_l0:
for pending in self.l0_manager.get_pending_flush() {
    let pending_l0 = pending.read();
    check_write_set_conflicts(&pending_l0, session_l0)?;
}
```

# **14. Implementation Plan**

## Phase 1: Core Infrastructure

| Step | Description | Files |
|------|-------------|-------|
| 1.1 | Add error variants (`SessionConflict`, `SessionAlreadyCompleted`, `RuleConflict`) | `uni-common/src/error.rs` |
| 1.2 | Add `Writer::merge_session_l0()` method | `uni-store/src/runtime/writer.rs` |
| 1.3 | Add `Writer::new_session_writer()` constructor | `uni-store/src/runtime/writer.rs` |
| 1.4 | Add `L0Buffer::is_empty()` helper | `uni-store/src/runtime/l0.rs` |

## Phase 2: Session Types

| Step | Description | Files |
|------|-------------|-------|
| 2.1 | Create `LocySession`, `LocySessionEngine`, `NativeSessionAdapter` | `crates/uni/src/api/locy_session.rs` (NEW) |
| 2.2 | Implement `LocyExecutionContext` for `NativeSessionAdapter` | `crates/uni/src/api/locy_session.rs` |
| 2.3 | Wire into module system | `crates/uni/src/api/mod.rs`, `crates/uni/src/lib.rs` |
| 2.4 | Add `Uni::locy_session()` factory method | `crates/uni/src/api/mod.rs` |

## Phase 3: Evaluation Path

| Step | Description | Files |
|------|-------------|-------|
| 3.1 | Extract shared evaluation logic from `evaluate_with_config` | `crates/uni/src/api/impl_locy.rs` |
| 3.2 | Implement `LocySessionEngine::evaluate` using shared logic | `crates/uni/src/api/locy_session.rs` |
| 3.3 | Implement `NativeSessionAdapter::execute_mutation` routing to session Writer | `crates/uni/src/api/locy_session.rs` |
| 3.4 | Implement `NativeSessionAdapter::begin_savepoint` / `rollback_savepoint` | `crates/uni/src/api/locy_session.rs` |

## Phase 4: Python Bindings

| Step | Description | Files |
|------|-------------|-------|
| 4.1 | Add core functions (`locy_session_*_core`) | `bindings/uni-db/src/core.rs` |
| 4.2 | Add `LocySession` pyclass (sync) | `bindings/uni-db/src/sync_api.rs` |
| 4.3 | Add `AsyncLocySession` pyclass (async) | `bindings/uni-db/src/async_api.rs` |
| 4.4 | Register classes in module | `bindings/uni-db/src/lib.rs` |

## Phase 5: Tests

| Step | Description | Files |
|------|-------------|-------|
| 5.1 | Rust integration tests | `crates/uni/tests/locy_session_test.rs` (NEW) |
| 5.2 | Python tests | `bindings/uni-db/tests/test_locy_session.py` (NEW) |

# **15. Files to Create**

## 15.1 `crates/uni/src/api/locy_session.rs`

The core new file. Contains:
- `LocySession` struct and impl
- `LocySessionEngine` struct and impl
- `NativeSessionAdapter` struct and `LocyExecutionContext` impl
- `CommitResult` struct
- `RuleConflictPolicy` enum
- Helper function `rules_to_promote()`
- Drop impl for auto-rollback

## 15.2 `crates/uni/tests/locy_session_test.rs`

Integration tests. See §17 for full test plan.

## 15.3 `bindings/uni-db/tests/test_locy_session.py`

Python-level tests for sync and async APIs.

# **16. Files to Modify**

## 16.1 `crates/uni-store/src/runtime/writer.rs`

**Add `new_session_writer()` constructor (~line 70):**

Creates a session-scoped Writer that shares `Arc`-wrapped components but has its own L0Manager and AdjacencyManager.

```rust
pub fn new_session_writer(shared: &Writer) -> Result<Writer> {
    let session_l0 = Arc::new(RwLock::new(L0Buffer::new(
        shared.l0_manager.get_current().read().current_version,
        None, // No WAL for session — written at commit time
    )));
    Ok(Writer {
        l0_manager: Arc::new(L0Manager::new_with_l0(session_l0)),
        storage: shared.storage.clone(),
        schema_manager: shared.schema_manager.clone(),
        allocator: shared.allocator.clone(),       // Shared — unique IDs
        config: shared.config.clone(),
        xervo_runtime: shared.xervo_runtime.clone(),
        transaction_l0: None,
        property_manager: shared.property_manager.clone(),
        adjacency_manager: Arc::new(AdjacencyManager::new()), // Private
        last_flush_time: Instant::now(),
        compaction_handle: Arc::new(RwLock::new(None)),        // No compaction
        index_rebuild_manager: None,                            // No index rebuild
    })
}
```

**Add `merge_session_l0()` method (~line 327):**

As described in §7.2.

**May need:** `L0Manager::new_with_l0()` constructor if it doesn't exist — creates an L0Manager initialized with a given L0 buffer.

## 16.2 `crates/uni/src/api/impl_locy.rs`

**Extract shared evaluation logic.**

The body of `LocyEngine::evaluate_with_config` (lines 118-341) needs to be callable from both `LocyEngine` and `LocySessionEngine`. Two approaches:

**Option A: Extract into a free function.**

```rust
pub(crate) async fn evaluate_with_config_inner(
    db: &Uni,
    registry: &LocyRuleRegistry,
    writer: &Arc<RwLock<Writer>>,
    adapter: &dyn LocyExecutionContext,
    program: &str,
    config: &LocyConfig,
) -> Result<LocyResult> {
    // ... body of current evaluate_with_config
}
```

`LocyEngine` calls this with `(self.db, &shared_registry, &shared_writer, &NativeExecutionAdapter, ...)`.
`LocySessionEngine` calls this with `(self.session.db, &session_registry, &session_writer, &NativeSessionAdapter, ...)`.

**Option B: Duplicate with session-specific wiring.**

Less refactoring, more code duplication. Acceptable if the function is large and tightly coupled.

**Recommendation: Option A** — the function is already well-structured, and the injection points are clear (registry, writer, adapter).

## 16.3 `crates/uni/src/api/mod.rs`

- Add `pub mod locy_session;` (~line 14)
- Add `Uni::locy_session()` method (~line 200)

## 16.4 `crates/uni/src/lib.rs`

- Add `pub use api::locy_session::{LocySession, LocySessionEngine, CommitResult, RuleConflictPolicy};`

## 16.5 `crates/uni-common/src/error.rs` (or equivalent)

- Add `SessionConflict`, `SessionAlreadyCompleted`, `RuleConflict` error variants

## 16.6 `crates/uni-store/src/runtime/l0.rs`

- Add `pub fn is_empty(&self) -> bool` helper

## 16.7 `bindings/uni-db/src/core.rs`

Add core functions:
- `locy_session_core(db: &Arc<Uni>) -> Result<LocySession>`
- `locy_session_register_core(session: &LocySession, program: &str) -> Result<()>`
- `locy_session_evaluate_core(session: &LocySession, program: &str) -> Result<LocyResult>`
- `locy_session_commit_core(session: &mut LocySession) -> Result<CommitResult>`
- `locy_session_rollback_core(session: &mut LocySession)`

## 16.8 `bindings/uni-db/src/sync_api.rs`

- Add `LocySession` pyclass with methods: `register`, `evaluate`, `evaluate_with_config`, `compile`, `clear_rules`, `commit`, `rollback`, `is_dirty`, `__enter__`, `__exit__`
- Add `Database::locy_session()` method

## 16.9 `bindings/uni-db/src/async_api.rs`

- Add `AsyncLocySession` pyclass mirroring sync API with async methods
- Add `AsyncDatabase::locy_session()` method

## 16.10 `bindings/uni-db/src/lib.rs`

- Register `LocySession` and `AsyncLocySession` classes

# **17. Test Plan**

## 17.1 Rust Integration Tests

| Test | Description |
|------|-------------|
| `test_session_basic_evaluate` | Create session, evaluate Locy query, verify results |
| `test_session_register_and_evaluate` | Register rules in session, evaluate using them |
| `test_session_write_isolation` | Session A DERIVEs facts, Session B cannot see them |
| `test_session_read_committed` | Session A commits, Session B sees committed data on next evaluate |
| `test_session_commit_promotes_mutations` | After commit, shared queries see the derived facts |
| `test_session_commit_promotes_rules` | After commit, shared `locy().evaluate()` sees the registered rules |
| `test_session_rollback_discards` | After rollback, shared state is unchanged |
| `test_session_auto_rollback_on_drop` | Drop without commit discards changes, logs warning if dirty |
| `test_session_occ_conflict_on_same_entity` | Two sessions update same vertex, first commits, second gets SessionConflict |
| `test_session_occ_no_conflict_disjoint` | Two sessions write disjoint entities, both commit successfully |
| `test_session_occ_no_conflict_pure_inserts` | Two sessions DERIVE new facts (fresh IDs), both commit successfully |
| `test_session_assume_isolation` | ASSUME mutations are invisible after ASSUME block, session L0 unchanged |
| `test_session_assume_concurrent` | Two sessions run ASSUME concurrently without blocking each other |
| `test_session_rule_isolation` | Rules registered in Session A are invisible to Session B |
| `test_session_rule_conflict_error` | Commit fails with RuleConflict when shared registry already has the rule (Error policy) |
| `test_session_rule_conflict_replace` | Commit succeeds and replaces shared rules (Replace policy) |
| `test_session_double_commit_error` | Second commit returns SessionAlreadyCompleted |
| `test_session_evaluate_after_commit_error` | Evaluate after commit returns error |
| `test_session_is_dirty` | `is_dirty()` returns false initially, true after DERIVE, false after commit |

## 17.2 Python Tests

| Test | Description |
|------|-------------|
| `test_sync_session_lifecycle` | Create, evaluate, commit via sync API |
| `test_sync_context_manager` | `with` block auto-rollback on exception |
| `test_async_session_lifecycle` | Create, evaluate, commit via async API |
| `test_async_context_manager` | `async with` block auto-rollback on exception |
| `test_sync_parallel_sessions` | Two threads, each with own session, both commit |
| `test_async_parallel_sessions` | Two tasks, each with own session, both commit via `asyncio.gather` |

# **18. Design Decisions and Trade-offs**

## 18.1 Session-Scoped Writer vs. Direct L0 Writes

**Chosen: Session-scoped Writer.**

| Approach | Pros | Cons |
|----------|------|------|
| Session-scoped Writer | Reuses all constraint checks, ID allocation, schema validation, adjacency updates | Must construct a Writer clone (cheap — all Arc clones) |
| Direct L0 writes | Simpler, no Writer dependency | Must reimplement constraint validation, ID allocation, property encoding |

The session Writer shares the `IdAllocator` (unique IDs), `StorageManager` (read path), and `SchemaManager` (validation). Only `L0Manager` and `AdjacencyManager` are session-private.

## 18.2 Read Committed vs. Snapshot Isolation

**Chosen: Read Committed.**

| Level | Pros | Cons |
|-------|------|------|
| Read Committed | Simple, users see fresh data, no stale reads | Non-repeatable reads within a session |
| Snapshot Isolation | Repeatable reads, consistent multi-step reasoning | Stale data for long-lived sessions, more complex implementation |

Read Committed matches the common use case (interactive notebooks, API requests). Snapshot Isolation can be added later via `Uni::at_snapshot()` (see §19).

## 18.3 Write-Set Intersection vs. Version-Based OCC

**Chosen: Write-set intersection.**

| Approach | Pros | Cons |
|----------|------|------|
| Write-set intersection | Entity-level precision, survives L0 rotation | O(session writes) check at commit |
| Version comparison | O(1) check | Too coarse (any mutation conflicts), breaks on L0 rotation |

Version-based OCC fails because `L0Buffer.current_version` is per-buffer, not global, and resets on L0 rotation during flush.

## 18.4 Rule Conflict Policy Default

**Chosen: Error (fail on collision).**

Silent replacement is dangerous — a session could unknowingly overwrite rules that other sessions depend on. Explicit Error is safest; users who want replacement can opt in with `set_rule_conflict_policy(Replace)`.

## 18.5 Context Manager for Sync API

**Chosen: Add `__enter__`/`__exit__` to sync `LocySession`.**

This diverges from the existing sync `Transaction` (which lacks context manager support), but sessions are longer-lived and more likely to be used in `with` blocks. Context manager support for `Transaction` can be added as a follow-up.

## 18.6 Async vs. Sync Method Split

| Method | Why Async/Sync |
|--------|---------------|
| `locy_session()` | **Async** — reads shared Writer to init session Writer |
| `register()` | **Sync** — modifies session-local rule registry, no I/O |
| `evaluate()` | **Async** — DataFusion queries, storage reads |
| `compile_only()` | **Sync** — grammar parse + type check, no I/O |
| `clear_rules()` | **Sync** — clears session-local HashMap |
| `commit()` | **Async** — acquires Writer lock, WAL write, L0 merge |
| `rollback()` | **Sync** — discards session state, no I/O |
| `is_dirty()` | **Sync** — reads session-local flag |

# **19. Future Work**

## 19.1 Snapshot Isolation Mode

Add `db.locy_session_with_snapshot()` that captures a frozen view at creation time using `Uni::at_snapshot()`. All reads within the session see the snapshot state, providing repeatable reads at the cost of potentially stale data.

## 19.2 Session Pooling

For server workloads, maintain a pool of pre-initialized sessions to amortize the cost of Writer cloning and rule registry cloning.

## 19.3 Nested Sessions

Allow creating a child session from a parent session. The child sees the parent's uncommitted mutations. Committing the child merges into the parent (not the shared database). This enables speculative sub-computations within a session.

## 19.4 Session Metrics

Expose per-session metrics via the existing metrics infrastructure:
- `uni_locy_session_duration_seconds` (histogram)
- `uni_locy_session_commits_total` / `uni_locy_session_rollbacks_total` (counters)
- `uni_locy_session_conflicts_total` (counter)
- `uni_locy_session_mutations_committed` (histogram)

## 19.5 Pessimistic Locking (SELECT FOR UPDATE equivalent)

For workloads with high contention on specific entities, add optional pessimistic locking where a session can acquire an exclusive lock on specific vertices/edges before modifying them. This avoids OCC retry loops at the cost of blocking.

## 19.6 Session-Level Query Cache

Cache DataFusion query results within a session. If the session's L0 hasn't changed since the last evaluation of the same query, return cached results. Useful for iterative notebook workflows.
