//! Repro for crates/uni-locy-tck/src/steps/given.rs:35
//!
//! Finding: the `#[given("having executed:")]` step guards its graph-setup
//! work with `if let Some(query) = step.docstring()`. When a `having executed:`
//! step is authored WITHOUT an attached docstring, `step.docstring()` returns
//! `None`, the whole setup block is skipped, and the step silently succeeds
//! against an empty graph. By contrast the sibling `when evaluating ...` step
//! (when_evaluate.rs) calls `.expect("Expected a docstring ...")` and panics
//! loudly on a missing docstring.
//!
//! This repro drives the REAL cucumber TCK harness (the same `LocyWorld` +
//! step registry used by `tests/locy_tck.rs`) over a generated feature file
//! and OBSERVES that:
//!   * a `having executed:` step with NO docstring passes silently against an
//!     empty graph (BUG: intended setup was skipped, nothing flagged it), while
//!   * the sibling `evaluating ...` step with NO docstring fails loudly.

use std::sync::{Arc, Mutex};

use cucumber::{writer, StatsWriter, World, WriterExt};

/// Minimal thread-safe writer so we can inspect cucumber's rendered output
/// (panic messages, pass/fail summary) after a scenario runs.
#[derive(Clone, Default)]
struct CaptureBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CaptureBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("buffer lock poisoned").write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl CaptureBuf {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("buffer lock poisoned")).into_owned()
    }
}

/// The feature file exercised by this repro. Three scenarios, each authored so
/// that the ONLY difference from a correct feature is a missing docstring.
const FEATURE: &str = r#"Feature: having-executed docstring guard repro

  # CONTROL: the setup docstring IS present -> graph is populated.
  Scenario: control with docstring
    Given having executed:
      """
      CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})
      """
    When evaluating the following Locy program:
      """
      MATCH (n:Person) RETURN n.name AS name
      """
    Then evaluation should succeed
    And the graph should contain 2 nodes with label 'Person'

  # BUG: the author FORGOT the setup docstring. The step silently no-ops,
  # the graph stays empty, and the scenario passes anyway.
  Scenario: bug missing docstring
    Given having executed:
    When evaluating the following Locy program:
      """
      MATCH (n:Person) RETURN n.name AS name
      """
    Then evaluation should succeed
    And the graph should contain 0 nodes with label 'Person'

  # SIBLING: the same missing-docstring mistake on `evaluating ...` fails loudly.
  Scenario: sibling evaluating without docstring
    Given an empty graph
    When evaluating the following Locy program:
    Then evaluation should succeed
"#;

/// Run a single scenario (matched by name) through the real cucumber harness.
/// Returns `(execution_failed, rendered_output)`.
fn run_scenario(feature_path: std::path::PathBuf, scenario_name: &'static str) -> (bool, String) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let buf = CaptureBuf::default();
    let buf_for_writer = buf.clone();
    let failed = rt.block_on(async move {
        let cucumber_writer = writer::Basic::new(
            buf_for_writer,
            writer::Coloring::Never,
            writer::Verbosity::Default,
        )
        .summarized();

        let w = uni_locy_tck::LocyWorld::cucumber()
            .with_writer(cucumber_writer)
            .with_default_cli()
            .max_concurrent_scenarios(Some(1))
            .filter_run(feature_path, move |_feat, _rule, sc| {
                sc.name == scenario_name
            })
            .await;

        w.execution_has_failed()
    });

    (failed, buf.contents())
}

fn write_feature() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("create temp dir");
    let path = dir.path().join("repro.feature");
    std::fs::write(&path, FEATURE).expect("write feature file");
    dir
}

/// CORRECT behavior (post-fix): a `having executed:` step with no docstring
/// fails the scenario loudly, mirroring the sibling `evaluating ...` step,
/// instead of silently no-opping against an empty graph.
#[test]
fn having_executed_missing_docstring_fails_loudly() {
    let dir = write_feature();
    let feature_path = dir.path().join("repro.feature");

    // Sanity control: with a docstring the setup runs and the graph is populated.
    let (control_failed, control_out) =
        run_scenario(feature_path.clone(), "control with docstring");
    assert!(
        !control_failed,
        "control scenario (with docstring) should pass; output:\n{control_out}"
    );

    // Identical scenario minus the docstring. The step must now fail rather than
    // silently no-op against an empty graph.
    let (bug_failed, bug_out) = run_scenario(feature_path.clone(), "bug missing docstring");

    // CORRECT behavior (fix for given.rs:35): the malformed `having executed:`
    // step panics with "Expected a docstring with the setup query", failing the
    // scenario.
    assert!(
        bug_failed,
        "missing-docstring `having executed:` step must fail the scenario; output:\n{bug_out}"
    );

    // And the harness surfaces the missing-docstring problem.
    assert!(
        bug_out.contains("Expected a docstring"),
        "harness must flag the missing setup docstring; output:\n{bug_out}"
    );
}

/// Control proving the asymmetry: the SIBLING `evaluating ...` step with no
/// docstring fails loudly (panics with an "Expected a docstring" message),
/// unlike `having executed:`.
#[test]
fn sibling_evaluating_missing_docstring_fails_loudly() {
    let dir = write_feature();
    let feature_path = dir.path().join("repro.feature");

    let (failed, out) = run_scenario(feature_path, "sibling evaluating without docstring");

    assert!(
        failed,
        "expected the sibling `evaluating ...` step to FAIL on a missing docstring; \
         output:\n{out}"
    );
    assert!(
        out.contains("Expected a docstring"),
        "expected a loud 'Expected a docstring' panic from the sibling step; output:\n{out}"
    );
}
