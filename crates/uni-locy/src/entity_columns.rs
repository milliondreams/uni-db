// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Which of a rule's columns hold relationships.
//!
//! A rule's facts carry a node as its VID and a single-hop relationship as its
//! EID, both unsigned integers, and both are hydrated into entities after the
//! fixpoint. The two ID spaces overlap, so hydration must know which columns
//! hold EIDs: resolving an EID as a VID substitutes whatever node has that
//! number.
//!
//! A column holds relationships when every clause of its rule yields a
//! relationship there: the column kinds the binding plan infers
//! ([`ColumnKinds`]), so hydration and binding agree on what a column holds.

// Rust guideline compliant

use std::collections::{HashMap, HashSet};

use crate::binding_plan::{ColumnKind, ColumnKinds};
use crate::types::CompiledRule;

/// The relationship columns of every rule in `catalog`, keyed by rule name.
///
/// Rules without one are absent.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// let catalog = HashMap::new();
/// assert!(uni_locy::entity_columns::edge_columns(&catalog).is_empty());
/// ```
#[must_use]
pub fn edge_columns(catalog: &HashMap<String, CompiledRule>) -> HashMap<String, HashSet<String>> {
    let kinds = ColumnKinds::infer(catalog);
    catalog
        .keys()
        .filter_map(|name| {
            let columns: HashSet<String> = kinds
                .columns_of_kind(name, ColumnKind::Edge)
                .map(str::to_string)
                .collect();
            (!columns.is_empty()).then(|| (name.clone(), columns))
        })
        .collect()
}
