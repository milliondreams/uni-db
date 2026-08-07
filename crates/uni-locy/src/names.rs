// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Matching as-written rule references against canonical catalog keys.
//!
//! Locy stores rules under their **module-qualified** name — `CompiledRule.name`,
//! `CompiledProgram::rule_catalog` keys and the session rule registry all hold
//! `"m.adult"` for a rule declared as `adult` inside `MODULE m`. References in
//! the AST (`IsReference::rule_name`, `GoalQuery::rule_name`, …) keep the
//! **bare, as-written** spelling; the compiler resolves them through
//! [`crate::compiler::modules::resolve_rule_name`] but never rewrites the AST.
//!
//! Consumers downstream of compilation therefore have to bridge the two forms.
//! This module holds the single predicate that does so, plus the one policy
//! layered on top of it, so the bare/qualified rule lives in one place.
//!
//! `USE` cannot rename: `UseDecl::imports` has no alias form, and both the glob
//! and selective import paths map `bare -> "module.bare"`. Every canonical key
//! reachable from an as-written reference `n` therefore satisfies
//! `key == n || key.ends_with(&format!(".{n}"))`, which makes suffix matching a
//! superset of compile-time resolution — it can admit too much, never too little.

/// Returns whether `key` is a legal spelling target for `reference`.
///
/// An exact match always counts. A *bare* reference — one containing no `.` —
/// additionally matches a module-qualified key whose final segment equals it,
/// which is how a rule declared inside `MODULE m` is named from within that
/// module. A dotted reference never falls back to a bare key.
///
/// This is a *candidate* test, not a resolution: several keys may match one
/// bare reference when two modules export the same leaf name. Callers choose
/// what to do about that; see [`resolve_unique`] for the strict policy.
///
/// # Examples
///
/// ```
/// use uni_locy::names::name_matches;
///
/// assert!(name_matches("adult", "adult"));
/// assert!(name_matches("m.adult", "adult"));
/// // A dotted reference is never satisfied by a bare key.
/// assert!(!name_matches("adult", "m.adult"));
/// // Only whole segments match, not arbitrary suffixes.
/// assert!(!name_matches("m.grandadult", "adult"));
/// ```
#[must_use]
pub fn name_matches(key: &str, reference: &str) -> bool {
    key == reference
        || (!reference.contains('.')
            && key
                .rsplit_once('.')
                .is_some_and(|(_, bare)| bare == reference))
}

/// Returns the single key matching `reference`, or `None` when it is ambiguous.
///
/// An exact match wins outright. Otherwise a bare reference resolves to the one
/// qualified key whose final segment equals it; if two or more keys qualify —
/// two modules exporting the same leaf name — this returns `None` rather than
/// guessing, and the caller is expected to surface that as a failure.
///
/// Use this where picking the wrong rule would be worse than refusing. Where
/// the cost is reversed, match every candidate with [`name_matches`] instead.
///
/// # Examples
///
/// ```
/// use uni_locy::names::resolve_unique;
///
/// let keys = ["m.adult", "other.minor"];
/// assert_eq!(resolve_unique(keys, "adult"), Some("m.adult"));
///
/// // Ambiguity is refused, not guessed.
/// let ambiguous = ["m1.adult", "m2.adult"];
/// assert_eq!(resolve_unique(ambiguous, "adult"), None);
/// ```
#[must_use]
pub fn resolve_unique<'a, I>(keys: I, reference: &str) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut matched: Option<&'a str> = None;
    for key in keys {
        if key == reference {
            return Some(key);
        }
        if !name_matches(key, reference) {
            continue;
        }
        if matched.is_some() {
            // Two qualified keys share this leaf; refuse rather than pick.
            return None;
        }
        matched = Some(key);
    }
    matched
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match_always_counts() {
        assert!(name_matches("adult", "adult"));
        assert!(name_matches("m.adult", "m.adult"));
    }

    #[test]
    fn bare_reference_matches_qualified_key() {
        assert!(name_matches("m.adult", "adult"));
        assert!(name_matches("a.b.adult", "adult"));
    }

    #[test]
    fn dotted_reference_never_falls_back_to_bare() {
        assert!(!name_matches("adult", "m.adult"));
    }

    #[test]
    fn suffix_match_respects_segment_boundary() {
        assert!(!name_matches("m.grandadult", "adult"));
        assert!(!name_matches("adultish", "adult"));
    }

    #[test]
    fn resolve_unique_prefers_an_exact_key_over_a_qualified_one() {
        // A bare key and a qualified key both "match"; the exact one wins even
        // when it is scanned second, so ordering cannot change the answer.
        assert_eq!(resolve_unique(["m.adult", "adult"], "adult"), Some("adult"));
        assert_eq!(resolve_unique(["adult", "m.adult"], "adult"), Some("adult"));
    }

    #[test]
    fn resolve_unique_refuses_ambiguity() {
        assert_eq!(resolve_unique(["m1.adult", "m2.adult"], "adult"), None);
    }

    #[test]
    fn resolve_unique_returns_none_when_nothing_matches() {
        assert_eq!(resolve_unique(["m.minor"], "adult"), None);
    }
}
