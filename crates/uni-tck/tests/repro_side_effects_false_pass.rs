//! Repros for the side-effect step handlers in crates/uni-tck/src/steps/and.rs.
//!
//! * `and.rs:17` (`no side effects`): the handler only compares net node/edge
//!   counts and label sets; it never inspects the gross counters
//!   (`properties_added` / `properties_removed`, `nodes_created` /
//!   `nodes_deleted`, ...). A pure property overwrite therefore falsely
//!   satisfies "no side effects".
//! * `and.rs:45` (`the side effects should be:`): the handler only asserts the
//!   counters named in the scenario table and never checks that unlisted
//!   counters are zero. A query that creates an undeclared relationship passes
//!   as long as the listed `+nodes` row matches.
//!
//! Both repros drive the REAL `UniWorld` against a REAL in-memory `uni-db`
//! through the same capture path the TCK harness uses (`capture_state_before`
//! / mutation via tx / `capture_state_after` / `side_effects()`), then replay
//! the handler's exact decision logic against the captured `SideEffects`.

use uni_tck::steps::and::{check_no_side_effects, check_side_effects_should_be};
use uni_tck::UniWorld;

/// Run a Cypher statement inside a committed transaction, mirroring the
/// harness's `execute_via_tx` in steps/when_step.rs.
async fn exec(world: &UniWorld, query: &str) {
    let session = world.db().session();
    let tx = session.tx().await.expect("open tx");
    tx.query_with(query)
        .fetch_all()
        .await
        .expect("execute query");
    tx.commit().await.expect("commit tx");
}

/// A property overwrite (net counts + labels unchanged) falsely passes
/// `no side effects` even though `properties_added`/`properties_removed` > 0.
#[tokio::test]
async fn property_overwrite_falsely_passes_no_side_effects() {
    let mut world = UniWorld::default();
    world.init_db().await.expect("init db");

    // Seed a single node with property p = 1.
    exec(&world, "CREATE (:N {p: 1})").await;

    // Capture, mutate the property in place, capture again -- same path as the
    // real `when executing query:` step.
    world.capture_state_before().await.expect("capture before");
    exec(&world, "MATCH (n:N) SET n.p = 2").await;
    world.capture_state_after().await.expect("capture after");

    let e = world.side_effects();

    // Net counts and label set are unchanged -- the loose pre-fix check passed on
    // exactly these three conditions.
    assert_eq!(e.nodes_before, e.nodes_after, "net node count unchanged");
    assert_eq!(e.edges_before, e.edges_after, "net edge count unchanged");
    assert_eq!(e.labels_before, e.labels_after, "label set unchanged");

    // ...but a genuine mutation occurred: the SET overwrote p=1 -> p=2.
    assert_eq!(e.properties_added, 1, "property overwrite adds a value");
    assert_eq!(
        e.properties_removed, 1,
        "property overwrite removes a value"
    );

    // CORRECT behavior (post-fix): `no side effects` now inspects the gross
    // counters and rejects the property overwrite.
    let outcome = check_no_side_effects(e);
    assert!(
        outcome.is_err(),
        "property overwrite must be reported as a side effect, got {:?}",
        outcome
    );
}

/// A create-then-churn also passes `no side effects`: net node count is
/// unchanged but `nodes_created`/`nodes_deleted` are both non-zero.
#[tokio::test]
async fn create_delete_churn_falsely_passes_no_side_effects() {
    let mut world = UniWorld::default();
    world.init_db().await.expect("init db");

    exec(&world, "CREATE (:N {})").await;

    world.capture_state_before().await.expect("capture before");
    // Delete the existing N and create a fresh N: net count 1 -> 1.
    exec(&world, "MATCH (n:N) DELETE n CREATE (:N {})").await;
    world.capture_state_after().await.expect("capture after");

    let e = world.side_effects();

    // Net counts and labels unchanged.
    assert_eq!(e.nodes_before, e.nodes_after, "net node count unchanged");
    assert_eq!(e.edges_before, e.edges_after, "net edge count unchanged");
    assert_eq!(e.labels_before, e.labels_after, "label set unchanged");

    // Real churn happened: a node was deleted and another created.
    assert!(
        e.nodes_created >= 1 && e.nodes_deleted >= 1,
        "create+delete churn (created={}, deleted={})",
        e.nodes_created,
        e.nodes_deleted
    );

    // CORRECT behavior (post-fix): the gross create/delete counters make this
    // fail `no side effects`.
    let outcome = check_no_side_effects(e);
    assert!(
        outcome.is_err(),
        "create+delete churn must be reported as a side effect, got {:?}",
        outcome
    );
}

/// `the side effects should be:` with an under-declared table (only `+nodes`)
/// falsely passes when the query also creates a relationship.
#[tokio::test]
async fn underdeclared_table_falsely_passes_side_effects_should_be() {
    let mut world = UniWorld::default();
    world.init_db().await.expect("init db");

    world.capture_state_before().await.expect("capture before");
    // Creates 2 nodes AND 1 relationship.
    exec(&world, "CREATE (:A)-[:R]->(:B)").await;
    world.capture_state_after().await.expect("capture after");

    let e = world.side_effects();

    // A real, undeclared relationship was created alongside the two nodes.
    assert_eq!(e.nodes_created, 2, "two nodes created");
    assert_eq!(e.edges_created, 1, "one relationship created");

    // An under-declared table that lists ONLY `+nodes = 2`. TCK semantics require
    // absent counters to be 0.
    let table: Vec<(String, i64)> = vec![("+nodes".to_string(), 2)];

    // CORRECT behavior (post-fix): the unlisted `+relationships` is implicitly 0,
    // so the created edge forces a failure.
    let outcome = check_side_effects_should_be(e, &table);
    assert!(
        outcome.is_err(),
        "undeclared +relationships=1 must force failure (absent => 0), got {:?}",
        outcome
    );

    // Sanity: fully declaring every non-zero counter passes. `CREATE (:A)-[:R]->(:B)`
    // also introduces two new labels (A, B), which the strict check now requires
    // to be declared -- omitting them (as the under-declared table above does) is
    // exactly the class of gap this fix closes.
    let full_table: Vec<(String, i64)> = vec![
        ("+nodes".to_string(), 2),
        ("+relationships".to_string(), 1),
        ("+labels".to_string(), 2),
    ];
    assert!(
        check_side_effects_should_be(e, &full_table).is_ok(),
        "fully-declared table must pass, got {:?}",
        check_side_effects_should_be(e, &full_table)
    );
}
