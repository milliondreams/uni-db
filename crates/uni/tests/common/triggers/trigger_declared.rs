#![allow(clippy::all)]
// Rust guideline compliant
//! WS-A end-to-end: `uni.plugin.declareTrigger` installs a REAL
//! `TriggerPlugin` that fires on the commit path (not a stealth
//! never-firing procedure).
//!
//! v1 scope: only `AfterCommit` + `Async`. `[SYNC]` is rejected at
//! declare time.

use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::Uni;
use uni_plugin::QName;

async fn count_label(db: &Uni, label: &str) -> Result<i64> {
    let res = db
        .session()
        .query(&format!("MATCH (n:{label}) RETURN count(n) AS n"))
        .await?;
    Ok(res.rows()[0].get::<i64>("n")?)
}

/// Poll the count of `label` every 20ms until it reaches `want` or
/// `timeout` elapses; returns the last observed count. Fully async — no
/// nested `block_on`.
async fn wait_for_count(db: &Uni, label: &str, want: i64, timeout: Duration) -> i64 {
    let deadline = Instant::now() + timeout;
    loop {
        let c = count_label(db, label).await.unwrap_or(0);
        if c >= want || Instant::now() >= deadline {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// ACCEPTANCE — declare a trigger, commit a matching mutation in a
/// separate tx, await the async fire, and observe the action's write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_trigger_fires_on_commit() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareTrigger(
            'mycorp.audit',
            'CREATE ON :Person',
            'CREATE (:AuditLog {msg:"added"})'
        )"#,
    )
    .await?;
    tx.commit().await?;

    // Separate, committed tx that matches the trigger's event filter.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name:'x'})").await?;
    tx.commit().await?;

    // Await the async after-commit fire.
    let n = wait_for_count(&db, "AuditLog", 1, Duration::from_secs(5)).await;
    assert!(
        n >= 1,
        "declared trigger never fired: expected an :AuditLog node after committing a :Person"
    );
    assert_eq!(count_label(&db, "AuditLog").await?, 1);

    Ok(())
}

/// REGRESSION — declaring kind "trigger" with a synthesizer installs
/// into `reg.triggers()` (non-empty) and NOT as a callable
/// `reg.procedure(qname)`. Directly asserts the WS-A bug is fixed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_trigger_lands_in_trigger_surface_not_procedure() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let registry = db.plugin_registry();
    let before = registry.triggers().len();

    let tx = db.session().tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareTrigger(
            'mycorp.audit',
            'CREATE ON :Person',
            'CREATE (:AuditLog {msg:"added"})'
        )"#,
    )
    .await?;
    tx.commit().await?;

    // Landed in the trigger surface.
    assert_eq!(
        registry.triggers().len(),
        before + 1,
        "declared trigger did not register into reg.triggers()"
    );
    // NOT registered as a callable procedure (the old stealth-procedure bug).
    assert!(
        registry.procedure(&QName::new("mycorp", "audit")).is_none(),
        "declared trigger must NOT be a callable procedure"
    );

    Ok(())
}

/// `[SYNC]` is rejected at declare time (v1 supports only async
/// after-commit actions).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_trigger_rejects_sync_marker() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let baseline = db.plugin_registry().triggers().len();

    let tx = db.session().tx().await?;
    let res = tx
        .execute(
            r#"CALL uni.plugin.declareTrigger(
                'mycorp.audit',
                'CREATE ON :Person [SYNC]',
                'CREATE (:AuditLog {msg:"added"})'
            )"#,
        )
        .await;
    assert!(res.is_err(), "[SYNC] declared trigger must be rejected");
    let msg = format!("{:?}", res.err().unwrap()).to_lowercase();
    assert!(
        msg.contains("synchronous"),
        "error should explain synchronous actions are unsupported, got: {msg}"
    );

    // And nothing leaked into the trigger surface (a builtin baseline
    // trigger — `LabelAuditTrigger` — is always present; the rejected
    // declaration must not add to it).
    assert_eq!(
        db.plugin_registry().triggers().len(),
        baseline,
        "rejected [SYNC] declaration must not register a trigger"
    );
    Ok(())
}

/// A self-referential trigger (`CREATE ON :Person` whose action creates
/// another `:Person`) TERMINATES rather than driving an unbounded async
/// write storm. Because `execute_inner_query(Write)` writes straight to
/// main L0 + WAL (not through a trigger-dispatching commit), the action's
/// write does not re-enter the trigger path, so the chain is bounded by
/// construction; the depth guard is a defense-in-depth backstop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_referential_trigger_terminates() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareTrigger(
            'mycorp.spawn',
            'CREATE ON :Person',
            'CREATE (:Person {gen: 1})'
        )"#,
    )
    .await?;
    tx.commit().await?;

    // One user-committed Person → trigger fires once → creates one more
    // Person, whose direct-write does NOT re-fire the trigger.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name:'seed'})").await?;
    tx.commit().await?;

    // Wait for the fire, then give any (hypothetical) runaway chain time
    // to blow up, and assert the count is small + stable.
    let _ = wait_for_count(&db, "Person", 2, Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let count = count_label(&db, "Person").await?;
    assert_eq!(
        count, 2,
        "self-referential trigger must terminate at 2 :Person nodes (seed + one action write), \
         got {count} — an unbounded storm would be far larger"
    );
    Ok(())
}

/// `WHEN n.age > 18` predicate fires only for matching rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_trigger_when_predicate_filters() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        r#"CALL uni.plugin.declareTrigger(
            'mycorp.adults',
            'CREATE ON :Person WHEN n.age > 18',
            'CREATE (:AdultLog {ok:true})'
        )"#,
    )
    .await?;
    tx.commit().await?;

    // Below threshold — must NOT fire.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name:'kid', age:10})").await?;
    tx.commit().await?;

    // Above threshold — must fire.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name:'grown', age:40})")
        .await?;
    tx.commit().await?;

    let n = wait_for_count(&db, "AdultLog", 1, Duration::from_secs(5)).await;
    assert!(n >= 1, "predicate trigger never fired for the matching row");
    // Exactly one — the age:10 row was filtered out by the predicate.
    assert_eq!(
        count_label(&db, "AdultLog").await?,
        1,
        "predicate must have gated out the age:10 Person"
    );
    Ok(())
}

/// RESTART — declare a trigger against a persistent Uni, drop it, reopen
/// from the same path, and confirm the trigger re-installs into
/// `reg.triggers()` and still fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_trigger_survives_restart() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("meta").to_string_lossy().to_string();

    let baseline;
    {
        let db = Uni::open(path.clone()).build().await?;
        baseline = db.plugin_registry().triggers().len();
        let tx = db.session().tx().await?;
        tx.execute(
            r#"CALL uni.plugin.declareTrigger(
                'mycorp.audit',
                'CREATE ON :Person',
                'CREATE (:AuditLog {msg:"added"})'
            )"#,
        )
        .await?;
        tx.commit().await?;
        assert_eq!(db.plugin_registry().triggers().len(), baseline + 1);
        db.shutdown().await?;
    }

    // Reopen from the same path — reactivate_into_registry must re-install
    // the declared trigger into the trigger surface (baseline builtin
    // trigger + the reactivated declared one).
    let db = Uni::open(path).build().await?;
    assert_eq!(
        db.plugin_registry().triggers().len(),
        baseline + 1,
        "declared trigger did not survive restart into reg.triggers()"
    );

    // And it still fires.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name:'x'})").await?;
    tx.commit().await?;

    let n = wait_for_count(&db, "AuditLog", 1, Duration::from_secs(5)).await;
    assert!(
        n >= 1,
        "restored declared trigger did not fire after restart"
    );
    Ok(())
}
