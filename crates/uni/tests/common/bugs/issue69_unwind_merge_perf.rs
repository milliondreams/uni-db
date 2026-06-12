// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro / correctness gate for issue #69.
//!
//! `UNWIND $rows AS e MERGE (n:L {k: e.k}) ON CREATE SET ... ON MATCH SET ...`
//! executes per-row: each row builds and runs a full DataFusion plan in the
//! non-unique-constraint fallback (`executor::write::execute_merge_match`),
//! so a batched MERGE is no faster (and in practice slower) than a per-entity
//! loop. There is no batched-write fast path analogous to `UNWIND ... CREATE`.
//!
//! This file pins the *observable semantics* that any optimization must
//! preserve (intra-batch dedup, ON MATCH reading existing state, multi-match,
//! RETURN ordering, fallback for non-indexed keys) and provides an `#[ignore]`
//! performance harness that buckets per-batch latency by table size to locate
//! the dominant cost.
//!
//! Run correctness:
//!   cargo nextest run -p uni --test integration bugs::issue69
//! Run the perf harness:
//!   cargo nextest run -p uni --test integration bugs::issue69 -- --ignored --no-capture

use anyhow::Result;
use std::collections::HashMap;
use std::time::Instant;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

/// The MERGE statement under test: upsert by `entity_id`, accumulating `freq`.
const MERGE_UPSERT: &str = "\
    UNWIND $batch AS e \
    MERGE (n:Entity {entity_id: e.entity_id}) \
    ON CREATE SET n.name = e.name, n.freq = e.c \
    ON MATCH SET n.freq = n.freq + e.c \
    RETURN n.entity_id AS id, n.freq AS freq";

/// Build a `{entity_id, name, c}` map for one UNWIND row.
fn entity(id: &str, name: &str, c: i64) -> Value {
    let mut m = HashMap::new();
    m.insert("entity_id".to_string(), Value::String(id.to_string()));
    m.insert("name".to_string(), Value::String(name.to_string()));
    m.insert("c".to_string(), Value::Int(c));
    Value::Map(m)
}

/// Schema with scalar hash indexes on `entity_id` and `name`, plus an
/// unindexed `tag` property — and deliberately NO unique constraint.
async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    // All properties are nullable: a MERGE sets only its key property at node
    // creation, then populates the rest via ON CREATE/ON MATCH SET afterward.
    db.schema()
        .label("Entity")
        .property_nullable("entity_id", DataType::String)
        .property_nullable("name", DataType::String)
        .property_nullable("freq", DataType::Int64)
        .property_nullable("tag", DataType::String)
        .index("entity_id", IndexType::Scalar(ScalarType::Hash))
        .index("name", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await?;
    Ok(db)
}

/// Count `Entity` nodes with a given `entity_id`.
async fn count_by_id(db: &Uni, id: &str) -> Result<i64> {
    Ok(db
        .session()
        .query_with("MATCH (n:Entity) WHERE n.entity_id = $id RETURN count(n) AS c")
        .param("id", id)
        .fetch_all()
        .await?
        .rows()[0]
        .get::<i64>("c")?)
}

/// Read the single `freq` for a given `entity_id` (asserts exactly one node).
async fn freq_by_id(db: &Uni, id: &str) -> Result<i64> {
    let res = db
        .session()
        .query_with("MATCH (n:Entity) WHERE n.entity_id = $id RETURN n.freq AS freq")
        .param("id", id)
        .fetch_all()
        .await?;
    assert_eq!(res.rows().len(), 1, "expected exactly one node for {id}");
    Ok(res.rows()[0].get::<i64>("freq")?)
}

// ============================================================================
// C-a: intra-batch duplicate key within ONE UNWIND must MATCH the just-created
// node (same tx, uncommitted) and accumulate ON MATCH SET — never double-create.
// ============================================================================

#[tokio::test]
async fn merge_intrabatch_duplicate_key_accumulates_two() -> Result<()> {
    let db = setup().await?;

    let batch = vec![entity("e1", "Alice", 3), entity("e1", "Alice", 5)];
    let tx = db.session().tx().await?;
    let res = tx
        .query_with(MERGE_UPSERT)
        .param("batch", Value::List(batch))
        .fetch_all()
        .await?;
    tx.commit().await?;

    // Per-row RETURN reflects post-SET state: row 0 created (3), row 1 matched (8).
    assert_eq!(res.rows().len(), 2);
    assert_eq!(res.rows()[0].get::<i64>("freq")?, 3);
    assert_eq!(res.rows()[1].get::<i64>("freq")?, 8);

    assert_eq!(count_by_id(&db, "e1").await?, 1, "must not double-create");
    assert_eq!(freq_by_id(&db, "e1").await?, 8);
    Ok(())
}

#[tokio::test]
async fn merge_intrabatch_duplicate_key_accumulates_three() -> Result<()> {
    let db = setup().await?;

    let batch = vec![
        entity("e1", "Alice", 3),
        entity("e1", "Alice", 5),
        entity("e1", "Alice", 2),
    ];
    let tx = db.session().tx().await?;
    tx.query_with(MERGE_UPSERT)
        .param("batch", Value::List(batch))
        .fetch_all()
        .await?;
    tx.commit().await?;

    assert_eq!(count_by_id(&db, "e1").await?, 1);
    assert_eq!(freq_by_id(&db, "e1").await?, 10);
    Ok(())
}

// ============================================================================
// C-b: ON MATCH SET must read committed node state.
// ============================================================================

#[tokio::test]
async fn merge_on_match_reads_committed_state() -> Result<()> {
    let db = setup().await?;

    // Pre-create + commit a node with freq = 10.
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Entity {entity_id: $id, name: $n, freq: $f})")
        .param("id", "e2")
        .param("n", "Bob")
        .param("f", 10_i64)
        .run()
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.query_with(MERGE_UPSERT)
        .param("batch", Value::List(vec![entity("e2", "Bob", 5)]))
        .fetch_all()
        .await?;
    tx.commit().await?;

    assert_eq!(count_by_id(&db, "e2").await?, 1);
    assert_eq!(freq_by_id(&db, "e2").await?, 15);
    Ok(())
}

// ============================================================================
// C-b2: ON MATCH SET must match a row that was flushed out of L0 to the
// persisted table (the index fast path must find persisted rows, not only L0).
// ============================================================================

#[tokio::test]
async fn merge_on_match_after_flush() -> Result<()> {
    let db = setup().await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Entity {entity_id: $id, name: $n, freq: $f})")
        .param("id", "fe")
        .param("n", "Flushed")
        .param("f", 10_i64)
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.query_with(MERGE_UPSERT)
        .param("batch", Value::List(vec![entity("fe", "Flushed", 5)]))
        .fetch_all()
        .await?;
    tx.commit().await?;

    assert_eq!(
        count_by_id(&db, "fe").await?,
        1,
        "must match the flushed row"
    );
    assert_eq!(freq_by_id(&db, "fe").await?, 15);
    Ok(())
}

/// `ON MATCH SET n.x = null` on a FLUSHED row must remove the property in the
/// per-row RETURN and in a fresh MATCH — pins the null-tombstone L0 layering
/// through the statement-level property prefetch (the prefetch base still
/// carries the flushed value; the L0 overlay must win).
#[tokio::test]
async fn merge_on_match_set_null_after_flush() -> Result<()> {
    let db = setup().await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Entity {entity_id: $id, name: $n, freq: $f})")
        .param("id", "nn")
        .param("n", "ToClear")
        .param("f", 10_i64)
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query_with(
            "UNWIND $batch AS e \
             MERGE (n:Entity {entity_id: e.entity_id}) \
             ON MATCH SET n.name = null \
             RETURN n.entity_id AS id, n.name AS name",
        )
        .param("batch", Value::List(vec![entity("nn", "ignored", 0)]))
        .fetch_all()
        .await?;
    tx.commit().await?;

    assert_eq!(res.rows().len(), 1);
    assert!(
        res.rows()[0].get::<String>("name").is_err(),
        "RETURN must see the null-SET (property removed)"
    );

    let dump = db
        .session()
        .query_with("MATCH (n:Entity) WHERE n.entity_id = $id RETURN n.name AS name, n.freq AS f")
        .param("id", "nn")
        .fetch_all()
        .await?;
    assert_eq!(dump.rows().len(), 1, "matched, not re-created");
    assert!(
        dump.rows()[0].get::<String>("name").is_err(),
        "fresh MATCH must see the property removed"
    );
    assert_eq!(dump.rows()[0].get::<i64>("f")?, 10, "other props untouched");
    Ok(())
}

// ============================================================================
// C-c: a non-unique key matching >1 node applies ON MATCH SET to ALL matches
// and creates nothing.
// ============================================================================

#[tokio::test]
async fn merge_multi_match_updates_all() -> Result<()> {
    let db = setup().await?;

    // Two distinct nodes share entity_id = "dup" (allowed: no unique constraint).
    let tx = db.session().tx().await?;
    for name in ["one", "two"] {
        tx.execute_with("CREATE (:Entity {entity_id: $id, name: $n, freq: $f})")
            .param("id", "dup")
            .param("n", name)
            .param("f", 0_i64)
            .run()
            .await?;
    }
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.query_with(MERGE_UPSERT)
        .param("batch", Value::List(vec![entity("dup", "ignored", 7)]))
        .fetch_all()
        .await?;
    tx.commit().await?;

    assert_eq!(
        count_by_id(&db, "dup").await?,
        2,
        "must not create a 3rd node"
    );
    let freqs = db
        .session()
        .query_with("MATCH (n:Entity) WHERE n.entity_id = $id RETURN n.freq AS freq")
        .param("id", "dup")
        .fetch_all()
        .await?;
    for row in freqs.rows() {
        assert_eq!(
            row.get::<i64>("freq")?,
            7,
            "ON MATCH SET must hit every match"
        );
    }
    Ok(())
}

// ============================================================================
// C-f: MERGE must match a node created by a PRIOR statement in the SAME, still
// uncommitted transaction (the batch L0 snapshot includes the transaction L0).
// ============================================================================

#[tokio::test]
async fn merge_matches_prior_statement_same_tx() -> Result<()> {
    let db = setup().await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Entity {entity_id: $id, name: $n, freq: $f})")
        .param("id", "ps")
        .param("n", "P")
        .param("f", 4_i64)
        .run()
        .await?;
    // Same tx, later statement: MERGE the same key must MATCH, not re-create.
    tx.query_with(MERGE_UPSERT)
        .param("batch", Value::List(vec![entity("ps", "P", 6)]))
        .fetch_all()
        .await?;
    tx.commit().await?;

    assert_eq!(count_by_id(&db, "ps").await?, 1);
    assert_eq!(freq_by_id(&db, "ps").await?, 10);
    Ok(())
}

// ============================================================================
// C-g: a single batch mixing an existing-committed key, a brand-new key, an
// intra-batch duplicate, and another new key — all handled correctly together.
// ============================================================================

#[tokio::test]
async fn merge_mixed_batch() -> Result<()> {
    let db = setup().await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Entity {entity_id: $id, name: $n, freq: $f})")
        .param("id", "x")
        .param("n", "X")
        .param("f", 1_i64)
        .run()
        .await?;
    tx.commit().await?;

    // x: matches committed (+2); y: new (3) then intra-batch dup (+4); z: new (5).
    let batch = vec![
        entity("x", "X", 2),
        entity("y", "Y", 3),
        entity("y", "Y", 4),
        entity("z", "Z", 5),
    ];
    let tx = db.session().tx().await?;
    tx.query_with(MERGE_UPSERT)
        .param("batch", Value::List(batch))
        .fetch_all()
        .await?;
    tx.commit().await?;

    assert_eq!(count_by_id(&db, "x").await?, 1);
    assert_eq!(freq_by_id(&db, "x").await?, 3);
    assert_eq!(count_by_id(&db, "y").await?, 1);
    assert_eq!(freq_by_id(&db, "y").await?, 7);
    assert_eq!(count_by_id(&db, "z").await?, 1);
    assert_eq!(freq_by_id(&db, "z").await?, 5);
    Ok(())
}

// ============================================================================
// C-d: a MERGE on a NON-indexed key takes the fast path too (the gate no longer
// requires an index — it only skips per-row planning; the persisted lookup
// degrades to a filtered scan). Must dedup + accumulate correctly. Uses the
// unindexed `tag` property.
// ============================================================================

#[tokio::test]
async fn merge_non_indexed_key_still_correct() -> Result<()> {
    let db = setup().await?;

    const Q: &str = "\
        UNWIND $batch AS e \
        MERGE (n:Entity {tag: e.tag}) \
        ON CREATE SET n.freq = e.c \
        ON MATCH SET n.freq = n.freq + e.c \
        RETURN n.freq AS freq";

    let mk = |tag: &str, c: i64| {
        let mut m = HashMap::new();
        m.insert("tag".to_string(), Value::String(tag.to_string()));
        m.insert("c".to_string(), Value::Int(c));
        Value::Map(m)
    };

    let tx = db.session().tx().await?;
    tx.query_with(Q)
        .param("batch", Value::List(vec![mk("t", 4), mk("t", 6)]))
        .fetch_all()
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query_with("MATCH (n:Entity) WHERE n.tag = $t RETURN count(n) AS c, n.freq AS f")
        .param("t", "t")
        .fetch_all()
        .await?;
    assert_eq!(res.rows()[0].get::<i64>("c")?, 1);
    assert_eq!(res.rows()[0].get::<i64>("f")?, 10);
    Ok(())
}

// ============================================================================
// C-e: RETURN preserves input row order, one output row per input row.
// ============================================================================

#[tokio::test]
async fn merge_return_preserves_input_order() -> Result<()> {
    let db = setup().await?;

    let batch = vec![
        entity("a", "A", 1),
        entity("b", "B", 1),
        entity("c", "C", 1),
    ];
    let tx = db.session().tx().await?;
    let res = tx
        .query_with(MERGE_UPSERT)
        .param("batch", Value::List(batch))
        .fetch_all()
        .await?;
    tx.commit().await?;

    let ids: Vec<String> = res
        .rows()
        .iter()
        .map(|r| r.get::<String>("id").unwrap())
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
    Ok(())
}

// ============================================================================
// Removal guard: a UNIQUE-constrained label still dedups on repeated MERGE
// after the dead `composite_lookup` "optimized" branch was removed. Single-key
// MERGE on the (indexed) unique key now flows through the issue-#69 fast path;
// this confirms the unique-constraint case end-to-end.
// ============================================================================

#[tokio::test]
async fn merge_unique_constraint_dedups() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("U")
        .property("code", DataType::String)
        .property_nullable("hits", DataType::Int64)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE CONSTRAINT u_code ON (u:U) ASSERT u.code IS UNIQUE")
        .await?;
    tx.commit().await?;

    // MERGE the same unique key three times across separate transactions.
    for _ in 0..3 {
        let tx = db.session().tx().await?;
        tx.execute_with(
            "MERGE (u:U {code: $code}) \
             ON CREATE SET u.hits = 1 \
             ON MATCH SET u.hits = u.hits + 1",
        )
        .param("code", "k")
        .run()
        .await?;
        tx.commit().await?;
    }

    let res = db
        .session()
        .query("MATCH (u:U) WHERE u.code = 'k' RETURN count(u) AS c")
        .await?;
    assert_eq!(res.rows()[0].get::<i64>("c")?, 1, "unique key must dedup");
    let hits = db
        .session()
        .query("MATCH (u:U) WHERE u.code = 'k' RETURN u.hits AS h")
        .await?;
    assert_eq!(hits.rows()[0].get::<i64>("h")?, 3);
    Ok(())
}

// ============================================================================
// Performance harness (diagnostic). Buckets per-batch latency by table size to
// distinguish flat per-row planning cost from O(N^2) scan-not-using-index.
// ============================================================================

/// Upsert `n` distinct entities in batches and report timing vs a CREATE
/// baseline. `#[ignore]` — run explicitly with `--ignored --no-capture`.
#[tokio::test]
#[ignore = "perf diagnostic, run explicitly"]
async fn merge_batched_perf_profile() -> Result<()> {
    const N: usize = 2000;
    const BATCH: usize = 20;

    // ---- Batched MERGE (the issue's pattern) ----
    let db = setup().await?;
    let mut first_batches = 0u128;
    let mut last_batches = 0u128;
    let total_start = Instant::now();
    let num_batches = N / BATCH;
    for b in 0..num_batches {
        let batch: Vec<Value> = (0..BATCH)
            .map(|j| {
                let i = b * BATCH + j;
                entity(&format!("e{i}"), &format!("name{i}"), 1)
            })
            .collect();
        let tx = db.session().tx().await?;
        let start = Instant::now();
        tx.query_with(MERGE_UPSERT)
            .param("batch", Value::List(batch))
            .fetch_all()
            .await?;
        let elapsed = start.elapsed().as_micros();
        tx.commit().await?;
        // Bucket: first 5 batches (small table) vs last 5 batches (large table).
        if b < 5 {
            first_batches += elapsed;
        } else if b >= num_batches - 5 {
            last_batches += elapsed;
        }
    }
    let merge_total = total_start.elapsed();

    // ---- CREATE baseline (no merge) ----
    let db2 = setup().await?;
    let create_start = Instant::now();
    for b in 0..num_batches {
        let batch: Vec<Value> = (0..BATCH)
            .map(|j| {
                let i = b * BATCH + j;
                entity(&format!("e{i}"), &format!("name{i}"), 1)
            })
            .collect();
        let tx = db2.session().tx().await?;
        tx.query_with(
            "UNWIND $batch AS e CREATE (n:Entity {entity_id: e.entity_id, name: e.name, freq: e.c}) RETURN id(n) AS nid",
        )
        .param("batch", Value::List(batch))
        .fetch_all()
        .await?;
        tx.commit().await?;
    }
    let create_total = create_start.elapsed();

    eprintln!("\n=== issue #69 MERGE-in-UNWIND perf ({N} entities, batch {BATCH}) ===");
    eprintln!("batched MERGE total : {merge_total:?}");
    eprintln!("CREATE baseline     : {create_total:?}");
    eprintln!(
        "MERGE first-5-batches avg : {} us/batch (small table)",
        first_batches / 5
    );
    eprintln!(
        "MERGE last-5-batches  avg : {} us/batch (large table)",
        last_batches / 5
    );
    eprintln!(
        "growth factor (last/first): {:.2}x  (>~1 => per-row scan scales with table size; ~1 => flat per-row planning cost)",
        last_batches as f64 / first_batches.max(1) as f64
    );

    assert_eq!(count_by_id(&db, "e0").await?, 1);

    // Regression guard: pre-fix this was ~18s (per-row query planning + per-row
    // L0 walk). The fast path brings it to ~1s; 8s leaves ample CI headroom
    // while still catching a return to the quadratic per-row behavior.
    assert!(
        merge_total.as_secs() < 8,
        "batched MERGE of {N} entities took {merge_total:?}, expected < 8s — perf regression"
    );
    Ok(())
}

// ============================================================================
// Batched persisted lookup (review perf #4): the per-statement prefetch must
// chunk past MERGE_SCAN_CHUNK keys and group mixed-type literals correctly.
// ============================================================================

/// A batch with >1000 distinct keys (exercising scan chunking) against a
/// flushed table: every previously flushed key must MATCH (freq accumulates),
/// every new key must CREATE — and no duplicates may appear.
#[tokio::test]
async fn merge_batch_over_chunk_limit() -> Result<()> {
    let db = setup().await?;
    let s = db.session();

    // Seed 600 entities and flush so they are persisted (Lance), not L0.
    let seed: Vec<Value> = (0..600).map(|i| entity(&format!("k{i}"), "n", 1)).collect();
    let tx = s.tx().await?;
    tx.execute_with(MERGE_UPSERT)
        .param("batch", Value::List(seed))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // 1200 distinct keys: 600 existing (k0..k599) + 600 new (k600..k1199).
    let batch: Vec<Value> = (0..1200)
        .map(|i| entity(&format!("k{i}"), "n", 1))
        .collect();
    let tx = s.tx().await?;
    tx.execute_with(MERGE_UPSERT)
        .param("batch", Value::List(batch))
        .run()
        .await?;
    tx.commit().await?;

    assert_eq!(freq_by_id(&db, "k0").await?, 2, "existing key matched");
    assert_eq!(freq_by_id(&db, "k599").await?, 2, "existing key matched");
    assert_eq!(freq_by_id(&db, "k600").await?, 1, "new key created once");
    assert_eq!(freq_by_id(&db, "k1199").await?, 1, "new key created once");
    assert_eq!(count_by_id(&db, "k42").await?, 1, "no duplicates");
    Ok(())
}

/// Mixed Int and String key values for the SAME key column in one batch:
/// the batched filter must group literals by type (a mixed-type IN list is
/// rejected by the scan parser, which would turn a batch the per-row path
/// handled fine into a statement error). The label is SCHEMALESS, so the
/// persisted lookup must find the flushed seeds via the main vertex table
/// (per-label tables only exist for declared labels): both seeded keys MATCH
/// (Float(1.0) canonicalizes to the stored Int(1)), the two new keys CREATE.
#[tokio::test]
async fn merge_batch_mixed_key_types() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // Schemaless label: the key column takes whatever type each row supplies.
    let s = db.session();

    fn keyed(v: Value) -> Value {
        let mut m = HashMap::new();
        m.insert("k".to_string(), v);
        Value::Map(m)
    }
    const MERGE_K: &str = "UNWIND $batch AS e \
        MERGE (n:Mixed {k: e.k}) \
        ON CREATE SET n.created = coalesce(n.created, 0) + 1 \
        RETURN n.k AS k";

    // Seed two nodes (one Int-keyed, one String-keyed) and flush.
    let tx = s.tx().await?;
    tx.execute_with(MERGE_K)
        .param(
            "batch",
            Value::List(vec![keyed(Value::Int(1)), keyed(Value::String("a".into()))]),
        )
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Mixed batch: the two seeded keys plus one new key of each type. The
    // statement must succeed (type-grouped batched filter) and match the two
    // flushed schemaless seeds instead of re-creating them.
    let tx = s.tx().await?;
    tx.execute_with(MERGE_K)
        .param(
            "batch",
            Value::List(vec![
                keyed(Value::Float(1.0)),
                keyed(Value::String("a".into())),
                keyed(Value::Int(2)),
                keyed(Value::String("b".into())),
            ]),
        )
        .run()
        .await?;
    tx.commit().await?;

    let dump = s
        .query("MATCH (n:Mixed) RETURN n.k AS k, n.created AS created")
        .await?;
    let rows: Vec<String> = dump.rows().iter().map(|r| format!("{r:?}")).collect();
    assert_eq!(
        rows.len(),
        4,
        "flushed schemaless seeds must MATCH, new keys CREATE once; got {rows:?}"
    );
    for row in dump.rows() {
        assert_eq!(
            row.get::<i64>("created")?,
            1,
            "ON CREATE SET must fire exactly once per key; got {rows:?}"
        );
    }
    Ok(())
}

// ============================================================================
// Schemaless persisted matching: MERGE on an UNDECLARED label must match rows
// that were flushed to the main vertex table (no per-label table exists), not
// only L0-resident rows.
// ============================================================================

/// Build a `{sk, c}` map for one schemaless UNWIND row.
fn schemaless_row(key: &str, c: i64) -> Value {
    let mut m = HashMap::new();
    m.insert("sk".to_string(), Value::String(key.to_string()));
    m.insert("c".to_string(), Value::Int(c));
    Value::Map(m)
}

const MERGE_SCHEMALESS: &str = "UNWIND $batch AS e \
    MERGE (n:UndeclaredSL {sk: e.sk}) \
    ON CREATE SET n.freq = e.c \
    ON MATCH SET n.freq = n.freq + e.c \
    RETURN n.sk AS sk, n.freq AS freq";

/// Flushed schemaless rows must MATCH on re-MERGE — no duplicates; new keys
/// in the same batch still CREATE.
#[tokio::test]
async fn merge_schemaless_flushed_no_duplicates() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let s = db.session();

    // Seed 5 schemaless nodes and flush them out of L0.
    let seed: Vec<Value> = (0..5)
        .map(|i| schemaless_row(&format!("s{i}"), 1))
        .collect();
    let tx = s.tx().await?;
    tx.execute_with(MERGE_SCHEMALESS)
        .param("batch", Value::List(seed))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Re-MERGE the 5 flushed keys plus 3 new ones.
    let batch: Vec<Value> = (0..8)
        .map(|i| schemaless_row(&format!("s{i}"), 1))
        .collect();
    let tx = s.tx().await?;
    tx.execute_with(MERGE_SCHEMALESS)
        .param("batch", Value::List(batch))
        .run()
        .await?;
    tx.commit().await?;

    let total = s
        .query("MATCH (n:UndeclaredSL) RETURN count(n) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(total, 8, "5 flushed keys matched + 3 new keys created");
    for (key, want_freq) in [("s0", 2_i64), ("s4", 2), ("s5", 1), ("s7", 1)] {
        let res = s
            .query_with("MATCH (n:UndeclaredSL) WHERE n.sk = $k RETURN n.freq AS f")
            .param("k", key)
            .fetch_all()
            .await?;
        assert_eq!(res.rows().len(), 1, "exactly one node for {key}");
        assert_eq!(res.rows()[0].get::<i64>("f")?, want_freq, "freq for {key}");
    }
    Ok(())
}

/// ON MATCH SET must apply to a flushed schemaless row (and create nothing).
#[tokio::test]
async fn merge_schemaless_on_match_set_after_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let s = db.session();

    let tx = s.tx().await?;
    tx.execute_with(MERGE_SCHEMALESS)
        .param("batch", Value::List(vec![schemaless_row("m0", 10)]))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = s.tx().await?;
    let res = tx
        .query_with(MERGE_SCHEMALESS)
        .param("batch", Value::List(vec![schemaless_row("m0", 5)]))
        .fetch_all()
        .await?;
    tx.commit().await?;

    // RETURN reflects the post-SET state of the matched (not re-created) node.
    assert_eq!(res.rows().len(), 1);
    assert_eq!(res.rows()[0].get::<i64>("freq")?, 15);

    let dump = s
        .query("MATCH (n:UndeclaredSL) RETURN count(n) AS c")
        .await?;
    assert_eq!(dump.rows()[0].get::<i64>("c")?, 1, "must not re-create");
    Ok(())
}

/// A DELETED-and-flushed schemaless row must not MATCH (its main-table
/// tombstone outranks the older live version — no resurrection through the
/// MVCC dedup), so MERGE creates a fresh node.
#[tokio::test]
async fn merge_schemaless_deleted_after_flush_recreates() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let s = db.session();

    let tx = s.tx().await?;
    tx.execute_with(MERGE_SCHEMALESS)
        .param("batch", Value::List(vec![schemaless_row("d0", 10)]))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = s.tx().await?;
    tx.execute_with("MATCH (n:UndeclaredSL) WHERE n.sk = $k DELETE n")
        .param("k", "d0")
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = s.tx().await?;
    let res = tx
        .query_with(MERGE_SCHEMALESS)
        .param("batch", Value::List(vec![schemaless_row("d0", 5)]))
        .fetch_all()
        .await?;
    tx.commit().await?;

    // ON CREATE fired: freq is the fresh 5, not 10 + 5.
    assert_eq!(res.rows().len(), 1);
    assert_eq!(res.rows()[0].get::<i64>("freq")?, 5);
    let dump = s
        .query("MATCH (n:UndeclaredSL) RETURN count(n) AS c")
        .await?;
    assert_eq!(dump.rows()[0].get::<i64>("c")?, 1, "exactly the new node");
    Ok(())
}

/// Int and non-integral Float key literals against one DECLARED Float64 key
/// column in a single batch: the type-grouped batched filter must keep the
/// two literal forms in separate IN lists, and Int/Float coercion must match
/// the stored values exactly once each.
#[tokio::test]
async fn merge_batch_int_float_key_grouping() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("FK")
        .property_nullable("k", DataType::Float64)
        .property_nullable("seen", DataType::Int64)
        .index("k", IndexType::Scalar(ScalarType::BTree))
        .done()
        .apply()
        .await?;
    let s = db.session();

    fn keyed(v: Value) -> Value {
        let mut m = HashMap::new();
        m.insert("k".to_string(), v);
        Value::Map(m)
    }
    const MERGE_FK: &str = "UNWIND $batch AS e \
        MERGE (n:FK {k: e.k}) \
        ON CREATE SET n.seen = 1 \
        ON MATCH SET n.seen = n.seen + 1 \
        RETURN n.k AS k";

    // Seed k=1.0 and k=1.5, flush to Lance.
    let tx = s.tx().await?;
    tx.execute_with(MERGE_FK)
        .param(
            "batch",
            Value::List(vec![keyed(Value::Float(1.0)), keyed(Value::Float(1.5))]),
        )
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // One batch mixing an Int spelling of the integral key (must coerce and
    // MATCH the stored 1.0) with the non-integral float key — two literal
    // type groups in one batched filter.
    let tx = s.tx().await?;
    tx.execute_with(MERGE_FK)
        .param(
            "batch",
            Value::List(vec![keyed(Value::Int(1)), keyed(Value::Float(1.5))]),
        )
        .run()
        .await?;
    tx.commit().await?;

    let count = s.query("MATCH (n:FK) RETURN count(n) AS c").await?.rows()[0].get::<i64>("c")?;
    assert_eq!(count, 2, "both keys matched, nothing re-created");
    Ok(())
}
