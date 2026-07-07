//! Runnable repros for verified correctness findings in `uni-plugin`.
//!
//! Each test exercises the REAL public API with REAL inputs and asserts on the
//! OBSERVED (currently-buggy) behavior. Where the buggy value is observed, the
//! assertion is on the actual value with a `// BUG:` comment pointing at the
//! offending source location. These stay green (documenting the defect) so CI
//! is not disrupted; flip the asserts once the bug is fixed.

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::SystemTime;

use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use uni_plugin::scheduler::Scheduler;
use uni_plugin::traits::background::Schedule;
use uni_plugin::traits::scalar::{ArgType, FnSignature, ScalarPluginFn};
use uni_plugin::{
    Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, PluginRegistry, QName,
};

// ── Shared minimal scalar-fn stub ────────────────────────────────────────

struct NoopScalar;

impl ScalarPluginFn for NoopScalar {
    fn signature(&self) -> &FnSignature {
        static SIG: OnceLock<FnSignature> = OnceLock::new();
        SIG.get_or_init(|| {
            FnSignature::new(
                vec![ArgType::Primitive(DataType::Float64)],
                ArgType::Primitive(DataType::Float64),
                Volatility::Immutable,
            )
        })
    }
    fn invoke(&self, args: &[ColumnarValue], _rows: usize) -> Result<ColumnarValue, FnError> {
        Ok(args[0].clone())
    }
}

fn scalar_sig() -> FnSignature {
    FnSignature::new(
        vec![ArgType::Primitive(DataType::Float64)],
        ArgType::Primitive(DataType::Float64),
        Volatility::Immutable,
    )
}

// ─────────────────────────────────────────────────────────────────────────
// [1] registry.rs:911 — apply_pending overwrites (not merges) the per-plugin
//     ownership record, so a second commit under the same plugin id orphans
//     surfaces registered by earlier commits.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn repro_apply_pending_overwrites_ownership_record() {
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let pid = PluginId::new("mycorp");

    // Commit #1: plugin `mycorp` registers `mycorp.f1` (its own registrar).
    {
        let mut r = PluginRegistrar::new(pid.clone(), &caps, &registry);
        r.scalar_fn(
            QName::new("mycorp", "f1"),
            scalar_sig(),
            Arc::new(NoopScalar),
        )
        .unwrap();
        r.commit_to_registry().unwrap();
    }
    // Commit #2: same plugin id `mycorp` registers `mycorp.f2` (a fresh
    // registrar with its own pending list — exactly the two-declareFunction
    // scenario).
    {
        let mut r = PluginRegistrar::new(pid.clone(), &caps, &registry);
        r.scalar_fn(
            QName::new("mycorp", "f2"),
            scalar_sig(),
            Arc::new(NoopScalar),
        )
        .unwrap();
        r.commit_to_registry().unwrap();
    }

    // Both functions coexist and remain callable in the scalars slot.
    assert!(registry.scalar_fn(&QName::new("mycorp", "f1")).is_some());
    assert!(registry.scalar_fn(&QName::new("mycorp", "f2")).is_some());

    // The per-plugin OWNERSHIP record, however, only reflects the last commit.
    let snap = registry
        .iter_for_plugin(&pid)
        .expect("plugin record exists");
    // FIXED (registry.rs): apply_pending MERGES the second commit's record into
    // the existing one, so the ownership record lists both f1 and f2.
    assert_eq!(
        snap.scalars.len(),
        2,
        "ownership record must contain both commits' scalars (merged, not overwritten)"
    );
    assert!(snap.scalars.contains(&QName::new("mycorp", "f1")));
    assert!(snap.scalars.contains(&QName::new("mycorp", "f2")));

    // Downstream impact: remove_plugin now sees both, so neither is orphaned.
    registry.remove_plugin(&pid);
    assert!(
        registry.scalar_fn(&QName::new("mycorp", "f2")).is_none(),
        "f2 removed"
    );
    assert!(
        registry.scalar_fn(&QName::new("mycorp", "f1")).is_none(),
        "f1 must also be removed — merged record tracks it, so no orphan leaks"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// [2] manifest.rs:68 — AbiRange::matches probes with minor/patch = u64::MAX/2,
//     so any requirement with an UPPER bound on minor/patch ("~1.2", "=1.2.3",
//     ">=1.2, <1.6") reports the host major as unsupported even though some
//     1.x version genuinely satisfies the requirement.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn repro_abi_range_upper_bounded_minor_reports_unsupported() {
    use uni_plugin::AbiRange;

    // FIXED (manifest.rs): matches() probes the comparators' in-range coordinates,
    // so an upper-bounded minor/patch range recognizes a supported host major.
    // `~1.2` desugars to `>=1.2.0, <1.3.0`; host major 1 IS supported (1.2.0).
    assert!(
        AbiRange::parse("~1.2").unwrap().matches(1),
        "~1.2 must report host major 1 as supported"
    );

    // `=1.2.3` matches only 1.2.3; host major 1 IS supported.
    assert!(
        AbiRange::parse("=1.2.3").unwrap().matches(1),
        "=1.2.3 must report host major 1 as supported"
    );

    // `>=1.2, <1.6` — host major 1 is supported (e.g. 1.2..1.6).
    assert!(
        AbiRange::parse(">=1.2, <1.6").unwrap().matches(1),
        ">=1.2,<1.6 must report host major 1 as supported"
    );

    // And a genuinely-unsupported major is still rejected.
    assert!(
        !AbiRange::parse("~1.2").unwrap().matches(2),
        "~1.2 must reject host major 2"
    );

    // Control: the caret cases the probe was designed for DO work correctly.
    assert!(AbiRange::parse("^1.2").unwrap().matches(1));
    assert!(AbiRange::parse("^1").unwrap().matches(1));
}

// ─────────────────────────────────────────────────────────────────────────
// [3] registry.rs:902 — apply_pending preflight only checks each pending
//     registration against the LIVE registry, never against the rest of the
//     batch, so duplicate names within one register() call bypass the
//     DuplicateRegistration check and silently last-write-wins.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn repro_intra_batch_duplicate_bypasses_preflight() {
    let registry = PluginRegistry::new();
    let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
    let pid = PluginId::new("dupco");

    let mut r = PluginRegistrar::new(pid.clone(), &caps, &registry);
    // Same qname registered TWICE in one register()/pending batch.
    r.scalar_fn(
        QName::new("dupco", "myfn"),
        scalar_sig(),
        Arc::new(NoopScalar),
    )
    .unwrap();
    r.scalar_fn(
        QName::new("dupco", "myfn"),
        scalar_sig(),
        Arc::new(NoopScalar),
    )
    .unwrap();

    // FIXED (registry.rs): apply_pending now rejects an intra-batch duplicate
    // unique qname up front — the batch preflight tracks qnames it has already
    // seen, so nothing is applied.
    let result = r.commit_to_registry();
    assert!(
        matches!(&result, Err(uni_plugin::PluginError::DuplicateRegistration(q)) if *q == QName::new("dupco", "myfn")),
        "intra-batch duplicate qname must be rejected as DuplicateRegistration, got {result:?}"
    );

    // Nothing was applied: the qname is not registered and there is no record.
    assert!(registry.scalar_fn(&QName::new("dupco", "myfn")).is_none());
    assert!(registry.iter_for_plugin(&pid).is_none());
}

// ─────────────────────────────────────────────────────────────────────────
// [4] scheduler.rs:238 — tick_at treats next_fire_at == None as immediately
//     due, so a Cron job whose expression fails to parse (next_after → None)
//     is dispatched and executed once instead of never.
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn repro_unparseable_cron_dispatched_once() {
    let s = Scheduler::new();
    s.resume();
    s.add_scheduled_job(
        QName::builtin("bad_cron"),
        Schedule::Cron(smol_str::SmolStr::new("this is not a valid cron")),
    );

    // An unparseable cron yields next_fire_at == None (next_after logged +
    // returned None). The intent (per the code comment) is "treated as not
    // currently due rather than silently lost".
    let jobs = s.list();
    assert_eq!(jobs.len(), 1);
    assert!(
        jobs[0].next_fire_at.is_none(),
        "unparseable cron has no computed fire time"
    );

    let due = s.tick_at(SystemTime::now());
    // FIXED (scheduler.rs): a None fire time (unparseable cron) is treated as
    // never-due, so the job is skipped rather than dispatched once.
    assert!(
        due.is_empty(),
        "unparseable-cron job must be skipped (never due), got {due:?}"
    );
}
