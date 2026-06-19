// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Structured execution profile for a Locy program.
//!
//! Mirrors the per-operator detail of Cypher's [`crate::ProfileOutput`] but
//! reflects Locy's actual cost model: a program is a sequence of strata, each
//! stratum evaluates one or more rules, and a recursive stratum iterates a
//! fixpoint until no new facts are derived. Every fixpoint iteration re-plans
//! and re-executes each rule's clause bodies, so the per-operator metrics
//! ([`OperatorStats`]) are retained *per iteration* rather than collapsed into
//! a single tree.
//!
//! The data is collected only when profiling is explicitly requested (via the
//! `profile()` builder terminal); a plain `run()` carries zero overhead.

use crate::query::executor::core::OperatorStats;
use std::collections::HashMap;
use std::sync::Mutex;

/// Full execution profile of a Locy program.
///
/// Returned (wrapped) from the Locy `profile()` API. Strata appear in
/// evaluation order; see [`LocyStratumProfile`] for the per-stratum breakdown.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct LocyExecProfile {
    /// Wall-clock time for the entire program evaluation, in milliseconds.
    pub total_elapsed_ms: f64,
    /// Peak derived-fact memory across all strata, in bytes.
    pub peak_memory_bytes: usize,
    /// One entry per evaluated stratum, in evaluation order.
    pub strata: Vec<LocyStratumProfile>,
}

/// Per-stratum slice of a [`LocyExecProfile`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LocyStratumProfile {
    /// Zero-based stratum index in evaluation order.
    pub index: usize,
    /// Whether this stratum was evaluated as a recursive fixpoint.
    pub recursive: bool,
    /// Wall-clock time spent in this stratum, in milliseconds.
    pub elapsed_ms: f64,
    /// Number of fixpoint iterations run (1 for a non-recursive stratum).
    pub iterations: usize,
    /// Total facts derived by this stratum's rules.
    pub facts_derived: usize,
    /// Per-rule detail for the rules in this stratum.
    pub rules: Vec<LocyRuleProfile>,
}

/// Per-rule slice of a [`LocyStratumProfile`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LocyRuleProfile {
    /// Rule name.
    pub name: String,
    /// Final derived-fact count for this rule.
    pub facts: usize,
    /// Per-iteration detail — one entry per fixpoint pass that evaluated this
    /// rule (a single entry for a non-recursive rule).
    pub iterations: Vec<LocyIterationProfile>,
}

/// Per-iteration slice of a [`LocyRuleProfile`].
///
/// Because the fixpoint re-plans clause bodies every pass, [`Self::operators`]
/// is the operator tree for *this iteration only* — the timeline is retained,
/// not summed across iterations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LocyIterationProfile {
    /// Zero-based fixpoint iteration index.
    pub iteration: usize,
    /// New facts produced by this rule in this iteration (delta size).
    pub delta_facts: usize,
    /// Wall-clock time for this rule's evaluation in this iteration, in
    /// milliseconds.
    pub elapsed_ms: f64,
    /// Per-operator metrics for the rule's clause-body DataFusion plan in this
    /// iteration, produced by the same plan walk Cypher's profile uses
    /// (`collect_plan_metrics`).
    pub operators: Vec<OperatorStats>,
}

/// Thread-safe accumulator that the fixpoint and non-recursive stratum
/// evaluators write per-rule, per-iteration profile rows into when profiling is
/// enabled. One collector is built per evaluated stratum; afterwards
/// [`Self::into_rules`] drains it into the stratum's [`LocyRuleProfile`]s.
#[derive(Debug, Default)]
pub struct LocyProfileCollector {
    inner: Mutex<CollectorInner>,
}

#[derive(Debug, Default)]
struct CollectorInner {
    /// Rule names in first-seen order, so output is stable and deterministic.
    order: Vec<String>,
    /// Per-rule iteration rows.
    by_rule: HashMap<String, Vec<LocyIterationProfile>>,
    /// Final derived-fact count per rule (set after convergence).
    final_facts: HashMap<String, usize>,
}

impl LocyProfileCollector {
    /// Record one rule's evaluation in one fixpoint iteration.
    pub fn record(
        &self,
        rule: &str,
        iteration: usize,
        delta_facts: usize,
        elapsed_ms: f64,
        operators: Vec<OperatorStats>,
    ) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !g.by_rule.contains_key(rule) {
            g.order.push(rule.to_string());
        }
        g.by_rule
            .entry(rule.to_string())
            .or_default()
            .push(LocyIterationProfile {
                iteration,
                delta_facts,
                elapsed_ms,
                operators,
            });
    }

    /// Record a rule's final derived-fact count (after convergence).
    pub fn set_final_facts(&self, rule: &str, facts: usize) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !g.by_rule.contains_key(rule) && !g.final_facts.contains_key(rule) {
            g.order.push(rule.to_string());
        }
        g.final_facts.insert(rule.to_string(), facts);
    }

    /// Drain the collector into per-rule profiles, in first-seen order.
    pub fn into_rules(&self) -> Vec<LocyRuleProfile> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let order = std::mem::take(&mut g.order);
        let mut by_rule = std::mem::take(&mut g.by_rule);
        let final_facts = std::mem::take(&mut g.final_facts);
        order
            .into_iter()
            .map(|name| {
                let iterations = by_rule.remove(&name).unwrap_or_default();
                let facts = final_facts.get(&name).copied().unwrap_or(0);
                LocyRuleProfile {
                    name,
                    facts,
                    iterations,
                }
            })
            .collect()
    }
}
