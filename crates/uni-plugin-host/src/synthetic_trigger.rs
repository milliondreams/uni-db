// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Synthetic trigger plugin for declared-trigger action execution (WS-A).
//!
//! `uni.plugin.declareTrigger('qname', '<event_filter>', '<body>')`
//! records a declaration; on `CustomPlugin::reactivate_into_registry`
//! (restart) and at declare time the host's [`CypherTriggerSynthesizer`]
//! is called for each `trigger`-kind record, returning a
//! [`SyntheticTriggerPlugin`] that registers into `PluginRegistry::triggers()`
//! (NOT as a callable procedure). The commit-path [`crate::triggers::TriggerRouter`]
//! then fires it on matching mutations.
//!
//! # v1 scope (load-bearing safety)
//!
//! Only [`TriggerPhase::AfterCommit`] + [`FireMode::Async`] are
//! supported. A before-commit synchronous WRITE action is unsafe
//! (orphaned side effects if the outer tx aborts, writer-lock deadlock,
//! isolation paradox), so the event-filter parser rejects a `[SYNC]`
//! mode marker at declare time. The action body runs via the
//! write-enabled `QueryProcedureHost::execute_inner_query` reached by
//! downcasting the [`TriggerContext`]'s host handle.
//!
//! # Re-entrancy (WS-A R1)
//!
//! `fire` refuses to run once the host's
//! [`QueryProcedureHost::trigger_depth`] reaches
//! [`QueryProcedureHost::MAX_TRIGGER_DEPTH`], so a self-referential
//! trigger (an action that writes a row re-firing the same trigger)
//! terminates instead of driving an unbounded async write storm.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray, UInt8Array};
use smol_str::SmolStr;
use tracing::warn;
use uni_plugin::FnError;
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin_custom::{DeclaredPlugin, TriggerBodySynthesizer};
use uni_query::query::executor::procedure_host::QueryProcedureHost;

/// A trigger whose `fire()` runs a stored Cypher action body through the
/// host's write-enabled `execute_inner_query`.
///
/// Lives in `uni-plugin-host` (not `uni-plugin-custom`) because the
/// implementation downcasts the trigger context's host to
/// `uni_query::QueryProcedureHost` — `uni-plugin-custom` does not depend
/// on `uni-query`.
pub struct SyntheticTriggerPlugin {
    qname: String,
    body: String,
    subscription: TriggerSubscription,
}

impl std::fmt::Debug for SyntheticTriggerPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyntheticTriggerPlugin")
            .field("qname", &self.qname)
            .field("body", &self.body)
            .finish_non_exhaustive()
    }
}

/// Outcome of parsing a declared trigger's `event_filter` string.
#[derive(Debug)]
struct ParsedFilter {
    events: TriggerEventMask,
    labels: Option<Vec<SmolStr>>,
    edge_types: Option<Vec<SmolStr>>,
    predicate_source: Option<String>,
}

impl SyntheticTriggerPlugin {
    /// Construct from a declared-trigger record.
    ///
    /// Reads the `event_filter` out of `decl.signature_json` (persisted
    /// by the `declareTrigger` meta-procedure), parses it into a
    /// [`TriggerSubscription`] pinned to `AfterCommit` + `Async`, and
    /// uses `decl.body` as the Cypher action.
    ///
    /// # Errors
    ///
    /// Returns an error string when the signature JSON is malformed or
    /// the `event_filter` requests the unsupported synchronous mode
    /// (`[SYNC]`).
    pub fn from_declaration(decl: &DeclaredPlugin) -> Result<Self, String> {
        let sig_meta: serde_json::Value = serde_json::from_str(&decl.signature_json)
            .map_err(|e| format!("signature_json parse: {e}"))?;
        let event_filter = sig_meta
            .get("event_filter")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let parsed = parse_event_filter(event_filter, &decl.qname)?;
        let subscription = TriggerSubscription {
            // v1 pins phase + fire mode — before-commit synchronous
            // WRITE actions are unsafe (see module docs).
            phase: TriggerPhase::AfterCommit,
            events: parsed.events,
            labels: parsed.labels,
            edge_types: parsed.edge_types,
            properties: None,
            predicate_source: parsed.predicate_source,
            fire_mode: FireMode::Async,
            // The first docs line is the router's stable trigger name
            // (`subscription_name`), so lead with the qname.
            docs: format!(
                "{}\nDeclared trigger action (event_filter: `{event_filter}`).",
                decl.qname
            ),
        };
        Ok(Self {
            qname: decl.qname.clone(),
            body: decl.body.clone(),
            subscription,
        })
    }
}

impl TriggerPlugin for SyntheticTriggerPlugin {
    fn subscription(&self) -> &TriggerSubscription {
        &self.subscription
    }

    fn fire(
        &self,
        ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        let host = ctx
            .host()
            .ok_or_else(|| {
                FnError::new(
                    0xD10,
                    format!("declared trigger `{}`: missing host context", self.qname),
                )
            })?
            .as_any()
            .downcast_ref::<QueryProcedureHost>()
            .ok_or_else(|| {
                FnError::new(
                    0xD11,
                    format!(
                        "declared trigger `{}`: host is not a QueryProcedureHost",
                        self.qname
                    ),
                )
            })?;

        // WS-A R1 — re-entrancy depth guard. Refuse to run the action
        // once the chain has recursed past the cap, so a self-referential
        // trigger (action writes a row that re-fires this trigger)
        // terminates instead of an unbounded async write storm. Log +
        // drop (Continue) rather than error — an error here is only
        // logged by the async dispatcher anyway.
        if host.trigger_depth() >= QueryProcedureHost::MAX_TRIGGER_DEPTH {
            warn!(
                trigger = %self.qname,
                depth = host.trigger_depth(),
                cap = QueryProcedureHost::MAX_TRIGGER_DEPTH,
                "declared trigger re-entrancy cap hit; dropping fire"
            );
            return Ok(TriggerOutcome::Continue);
        }

        // Run the action body once per event row, binding the row's
        // event columns as `$`-params so the body can reference
        // `$vid` / `$label` / `$event_kind` (unused params are ignored).
        let batch = &events.events;
        let row_count = batch.num_rows();
        let vid_col = batch
            .column_by_name("vid_or_eid")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let label_col = batch
            .column_by_name("label")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let kind_col = batch
            .column_by_name("event_kind")
            .and_then(|c| c.as_any().downcast_ref::<UInt8Array>());

        // No rows: nothing to do (the router only calls `fire` with a
        // non-empty filtered batch, but stay defensive).
        for row in 0..row_count {
            let mut params: HashMap<String, uni_common::Value> = HashMap::new();
            if let Some(c) = vid_col
                && !c.is_null(row)
            {
                params.insert("vid".to_owned(), uni_common::Value::Int(c.value(row)));
            }
            if let Some(c) = label_col
                && !c.is_null(row)
            {
                params.insert(
                    "label".to_owned(),
                    uni_common::Value::String(c.value(row).to_owned()),
                );
            }
            if let Some(c) = kind_col
                && !c.is_null(row)
            {
                params.insert(
                    "event_kind".to_owned(),
                    uni_common::Value::Int(i64::from(c.value(row))),
                );
            }

            let body = self.body.clone();
            let host_clone = host.clone();
            let qname = self.qname.clone();

            // Bridge the sync `fire()` to async `execute_inner_query`
            // via `block_in_place` + `Handle::current().block_on(...)`,
            // mirroring `SyntheticProcedurePlugin::invoke`. Requires a
            // multi-thread tokio runtime (Uni's default). The action
            // runs in Write mode so `CREATE` / `SET` / `MERGE` land and
            // commit through the outer writer.
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    host_clone
                        .execute_inner_query(
                            &body,
                            &params,
                            uni_plugin::traits::procedure::ProcedureMode::Write,
                        )
                        .await
                })
            })
            .map_err(|e| FnError::new(0xD12, format!("declared trigger `{qname}` action: {e}")))?;
        }

        Ok(TriggerOutcome::Continue)
    }
}

/// Parse a declared trigger's `event_filter` string into a
/// [`ParsedFilter`].
///
/// # Grammar (v1)
///
/// ```text
/// filter := verbs on_clause? when_clause? mode_clause?
/// verbs  := VERB ('|' VERB)*                  (VERB = CREATE|UPDATE|DELETE)
/// on_clause   := 'ON' (':' Label | '-[:' Type ']-')
/// when_clause := 'WHEN' <predicate…>          (raw Cypher boolean expr)
/// mode_clause := '[' ('ASYNC' | 'SYNC') ']'
/// ```
///
/// Keywords are case-insensitive; the label / edge type / predicate keep
/// their original casing. An `ON :Label` restricts to node events; an
/// `ON -[:Type]-` restricts to edge events; no `ON` means both node and
/// edge events. `[SYNC]` is rejected (v1 supports only async
/// after-commit actions). An empty or otherwise unrecognized filter
/// falls back to "all node + edge CREATE/UPDATE/DELETE events" with a
/// warning (does not fail the declaration).
///
/// # Errors
///
/// Returns an error string only for the explicit `[SYNC]` mode marker.
fn parse_event_filter(raw: &str, qname: &str) -> Result<ParsedFilter, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        warn!(trigger = %qname, "declared trigger has empty event_filter; \
            defaulting to all node+edge CREATE/UPDATE/DELETE events");
        return Ok(fallback_all_events());
    }

    let mut rest = trimmed.to_owned();

    // 1. Strip a trailing `[ASYNC]` / `[SYNC]` mode marker.
    if let Some(open) = rest.rfind('[') {
        let tail = rest[open..].trim();
        if tail.ends_with(']') {
            let mode = tail
                .trim_start_matches('[')
                .trim_end_matches(']')
                .trim()
                .to_ascii_uppercase();
            match mode.as_str() {
                "SYNC" => {
                    return Err(
                        "synchronous declared-trigger actions are not yet supported \
                         (drop the `[SYNC]` marker; declared triggers fire \
                         asynchronously after commit)"
                            .to_owned(),
                    );
                }
                "ASYNC" => {
                    rest.truncate(open);
                }
                // Not a mode marker (e.g. a `[...]` inside a predicate).
                // Leave it in `rest` for the WHEN clause to keep.
                _ => {}
            }
        }
    }
    let rest = rest.trim().to_owned();

    // 2. Split off an optional `WHEN <predicate>` clause.
    let (events_and_on, predicate_source) = match find_keyword(&rest, "WHEN") {
        Some(idx) => {
            let before = rest[..idx].trim().to_owned();
            let after = rest[idx + "WHEN".len()..].trim().to_owned();
            let pred = if after.is_empty() { None } else { Some(after) };
            (before, pred)
        }
        None => (rest, None),
    };

    // 3. Split off an optional `ON <target>` clause.
    let (verbs_part, on_target) = match find_keyword(&events_and_on, "ON") {
        Some(idx) => {
            let before = events_and_on[..idx].trim().to_owned();
            let after = events_and_on[idx + "ON".len()..].trim().to_owned();
            (before, Some(after))
        }
        None => (events_and_on, None),
    };

    // 4. Resolve node vs edge scope from the ON target.
    let (labels, edge_types, node_scoped, edge_scoped) = match on_target.as_deref() {
        Some(t) if t.starts_with(':') => {
            let label = t.trim_start_matches(':').trim();
            (Some(vec![SmolStr::new(label)]), None, true, false)
        }
        Some(t) if t.contains("-[:") || t.starts_with("[:") || t.starts_with("-[") => {
            // Extract the type between `-[:` and `]`.
            let etype = t
                .trim_start_matches('-')
                .trim_start_matches('[')
                .trim_start_matches(':')
                .split(']')
                .next()
                .unwrap_or("")
                .trim();
            (None, Some(vec![SmolStr::new(etype)]), false, true)
        }
        Some(t) if !t.is_empty() => {
            // Bare identifier after ON — treat as a node label.
            (Some(vec![SmolStr::new(t.trim())]), None, true, false)
        }
        // No ON clause (or empty target) — both node and edge events.
        _ => (None, None, true, true),
    };

    // 5. Fold the verbs into an event mask.
    let mut mask = TriggerEventMask::default();
    let mut recognized = false;
    for verb in verbs_part.split('|') {
        let v = verb.trim().to_ascii_uppercase();
        let (node_bit, edge_bit) = match v.as_str() {
            "CREATE" | "INSERT" => (TriggerEventMask::NODE_CREATE, TriggerEventMask::EDGE_CREATE),
            "UPDATE" | "SET" => (TriggerEventMask::NODE_UPDATE, TriggerEventMask::EDGE_UPDATE),
            "DELETE" | "REMOVE" => (TriggerEventMask::NODE_DELETE, TriggerEventMask::EDGE_DELETE),
            _ => continue,
        };
        recognized = true;
        if node_scoped {
            mask = mask.union(node_bit);
        }
        if edge_scoped {
            mask = mask.union(edge_bit);
        }
    }

    if !recognized {
        warn!(trigger = %qname, filter = %raw, "declared trigger event_filter \
            names no recognized verb (CREATE|UPDATE|DELETE); defaulting to \
            all node+edge events");
        // Keep any ON / WHEN scoping the user did provide.
        let fb = fallback_all_events();
        let mut mask = fb.events;
        if !node_scoped {
            mask = TriggerEventMask(
                mask.0
                    & !(TriggerEventMask::NODE_CREATE
                        .union(TriggerEventMask::NODE_UPDATE)
                        .union(TriggerEventMask::NODE_DELETE)
                        .0),
            );
        }
        if !edge_scoped {
            mask = TriggerEventMask(
                mask.0
                    & !(TriggerEventMask::EDGE_CREATE
                        .union(TriggerEventMask::EDGE_UPDATE)
                        .union(TriggerEventMask::EDGE_DELETE)
                        .0),
            );
        }
        return Ok(ParsedFilter {
            events: mask,
            labels,
            edge_types,
            predicate_source,
        });
    }

    Ok(ParsedFilter {
        events: mask,
        labels,
        edge_types,
        predicate_source,
    })
}

/// The "match everything" fallback subscription mask: node + edge
/// CREATE / UPDATE / DELETE.
fn fallback_all_events() -> ParsedFilter {
    let events = TriggerEventMask::NODE_CREATE
        .union(TriggerEventMask::NODE_UPDATE)
        .union(TriggerEventMask::NODE_DELETE)
        .union(TriggerEventMask::EDGE_CREATE)
        .union(TriggerEventMask::EDGE_UPDATE)
        .union(TriggerEventMask::EDGE_DELETE);
    ParsedFilter {
        events,
        labels: None,
        edge_types: None,
        predicate_source: None,
    }
}

/// Find the byte index of a whole-word, case-insensitive keyword in
/// `haystack`, bounded by whitespace (or string ends). Returns the index
/// of the first match, or `None`.
fn find_keyword(haystack: &str, keyword: &str) -> Option<usize> {
    let upper = haystack.to_ascii_uppercase();
    let kw = keyword.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let mut search_from = 0;
    while let Some(rel) = upper[search_from..].find(&kw) {
        let idx = search_from + rel;
        let before_ok = idx == 0 || bytes[idx - 1].is_ascii_whitespace();
        let after = idx + kw.len();
        let after_ok = after >= bytes.len() || bytes[after].is_ascii_whitespace();
        if before_ok && after_ok {
            return Some(idx);
        }
        search_from = idx + kw.len();
        if search_from >= bytes.len() {
            break;
        }
    }
    None
}

/// Host-side [`TriggerBodySynthesizer`] implementation.
///
/// Constructs a [`SyntheticTriggerPlugin`] from each declared trigger
/// record. Installed on the host's [`uni_plugin_custom::CustomPlugin`]
/// via [`uni_plugin_custom::CustomPlugin::with_trigger_synthesizer`]
/// during `Uni::build`.
#[derive(Debug, Default)]
pub struct CypherTriggerSynthesizer;

impl CypherTriggerSynthesizer {
    /// Construct.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl TriggerBodySynthesizer for CypherTriggerSynthesizer {
    fn synthesize(&self, decl: &DeclaredPlugin) -> Result<Arc<dyn TriggerPlugin>, String> {
        let plugin = SyntheticTriggerPlugin::from_declaration(decl)?;
        Ok(Arc::new(plugin))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decl_with_filter(event_filter: &str, body: &str) -> DeclaredPlugin {
        DeclaredPlugin {
            qname: "mycorp.audit".to_owned(),
            kind: "trigger".to_owned(),
            body: body.to_owned(),
            signature_json: serde_json::json!({
                "event_filter": event_filter,
                "body": body,
            })
            .to_string(),
            dependencies: vec![],
            declared_by: "alice".to_owned(),
            active: true,
        }
    }

    fn node_mask() -> u32 {
        TriggerEventMask::NODE_CREATE
            .union(TriggerEventMask::NODE_UPDATE)
            .union(TriggerEventMask::NODE_DELETE)
            .0
    }

    #[test]
    fn parses_create_on_label() {
        let p = parse_event_filter("CREATE ON :Person", "q").expect("parse");
        assert_eq!(p.events.0, TriggerEventMask::NODE_CREATE.0);
        assert_eq!(
            p.labels.as_deref().map(|v| v.to_vec()),
            Some(vec![SmolStr::new("Person")])
        );
        assert!(p.edge_types.is_none());
        assert!(p.predicate_source.is_none());
    }

    #[test]
    fn parses_multi_verb() {
        let p = parse_event_filter("CREATE|UPDATE ON :Person", "q").expect("parse");
        let expect = TriggerEventMask::NODE_CREATE
            .union(TriggerEventMask::NODE_UPDATE)
            .0;
        assert_eq!(p.events.0, expect);
    }

    #[test]
    fn parses_edge_pattern() {
        let p = parse_event_filter("CREATE ON -[:KNOWS]-", "q").expect("parse");
        assert_eq!(p.events.0, TriggerEventMask::EDGE_CREATE.0);
        assert_eq!(
            p.edge_types.as_deref().map(|v| v.to_vec()),
            Some(vec![SmolStr::new("KNOWS")])
        );
        assert!(p.labels.is_none());
    }

    #[test]
    fn parses_when_predicate() {
        let p = parse_event_filter("CREATE ON :Person WHEN n.age > 18", "q").expect("parse");
        assert_eq!(p.predicate_source.as_deref(), Some("n.age > 18"));
        assert_eq!(
            p.labels.as_deref().map(|v| v.to_vec()),
            Some(vec![SmolStr::new("Person")])
        );
    }

    #[test]
    fn no_on_clause_matches_node_and_edge() {
        let p = parse_event_filter("CREATE", "q").expect("parse");
        assert!(p.events.contains(TriggerEventMask::NODE_CREATE));
        assert!(p.events.contains(TriggerEventMask::EDGE_CREATE));
    }

    #[test]
    fn async_marker_accepted_and_stripped() {
        let p = parse_event_filter("CREATE ON :Person [ASYNC]", "q").expect("parse");
        assert_eq!(p.events.0, TriggerEventMask::NODE_CREATE.0);
    }

    #[test]
    fn sync_marker_rejected() {
        let err = parse_event_filter("CREATE ON :Person [SYNC]", "q").expect_err("must reject");
        assert!(err.contains("synchronous"), "got: {err}");
    }

    #[test]
    fn empty_filter_falls_back_to_all_events() {
        let p = parse_event_filter("", "q").expect("parse");
        assert_eq!(p.events.0, fallback_all_events().events.0);
        assert!(p.labels.is_none());
    }

    #[test]
    fn unrecognized_verb_falls_back_but_keeps_scope() {
        // Node-scoped (`ON :Person`) but no valid verb → node events only.
        let p = parse_event_filter("FROBNICATE ON :Person", "q").expect("parse");
        assert_eq!(p.events.0, node_mask());
        assert_eq!(
            p.labels.as_deref().map(|v| v.to_vec()),
            Some(vec![SmolStr::new("Person")])
        );
    }

    #[test]
    fn from_declaration_pins_phase_and_mode() {
        let decl = decl_with_filter("CREATE ON :Person", "CREATE (:AuditLog)");
        let plugin = SyntheticTriggerPlugin::from_declaration(&decl).expect("synthesize");
        assert_eq!(plugin.subscription().phase, TriggerPhase::AfterCommit);
        assert_eq!(plugin.subscription().fire_mode, FireMode::Async);
        assert_eq!(plugin.body, "CREATE (:AuditLog)");
    }

    #[test]
    fn from_declaration_rejects_sync() {
        let decl = decl_with_filter("CREATE ON :Person [SYNC]", "CREATE (:AuditLog)");
        assert!(SyntheticTriggerPlugin::from_declaration(&decl).is_err());
    }

    #[test]
    fn synthesizer_round_trips() {
        let synth = CypherTriggerSynthesizer::new();
        let decl = decl_with_filter("CREATE ON :Person", "CREATE (:AuditLog)");
        let plugin = synth.synthesize(&decl).expect("synthesize");
        assert_eq!(plugin.subscription().phase, TriggerPhase::AfterCommit);
    }
}
