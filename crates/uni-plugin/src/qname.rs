//! Qualified plugin item names — `namespace.local`.
//!
//! Every plugin-registered extension is identified by a [`QName`]: the
//! plugin's owning namespace (reverse-DNS, e.g. `ai.dragonscale.geo`) plus a
//! local name (e.g. `haversine`). Stored case-sensitively; matched
//! case-insensitively at Cypher call sites, case-sensitively at Locy call
//! sites.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::errors::PluginError;

/// Reserved single-token plugin ids that are exempt from the reverse-DNS
/// id-format requirement.
///
/// Third-party plugins must use reverse-DNS ids (e.g. `ai.example.geo`).
/// The framework ships a handful of single-token ids for its own
/// built-ins and migration aids; conformance probes accept these as
/// valid id shapes.
pub const RESERVED_PLUGIN_IDS: &[&str] = &["builtin", "apoc-core", "custom", "user.legacy"];

/// Returns `true` if `id` is one of the framework-reserved single-token
/// plugin ids exempt from the reverse-DNS requirement.
#[must_use]
pub fn is_reserved_plugin_id(id: &str) -> bool {
    RESERVED_PLUGIN_IDS.contains(&id)
}

/// Qualified plugin item name — `namespace.local`.
///
/// `QName` is the address every plugin-registered extension is looked up by.
/// The namespace is the registering plugin's id; the local is the per-plugin
/// item name. Built-ins use the reserved namespace [`QName::BUILTIN_NS`].
///
/// # Examples
///
/// ```
/// use uni_plugin::QName;
/// let q = QName::parse("ai.dragonscale.geo.haversine").unwrap();
/// assert_eq!(q.namespace(), "ai.dragonscale.geo");
/// assert_eq!(q.local(), "haversine");
/// ```
///
/// # Errors
///
/// [`QName::parse`] returns [`PluginError::InvalidQName`] if the input does
/// not contain at least one `.` separating namespace from local, or if either
/// side is empty.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QName {
    namespace: SmolStr,
    local: SmolStr,
}

impl QName {
    /// Reserved namespace for uni-db built-in extensions.
    ///
    /// Built-ins registered by `uni-plugin-builtin` use this namespace so
    /// they are distinguishable from third-party plugins at the registry
    /// level. The user-facing Cypher / Locy syntax does not require the
    /// namespace prefix for built-ins — `RETURN toUpper(s)` resolves to
    /// `builtin.toUpper` through Cypher's case-insensitive matching.
    pub const BUILTIN_NS: &'static str = "builtin";

    /// Construct a `QName` from already-validated parts.
    ///
    /// # Panics
    ///
    /// Panics if `namespace` or `local` is empty, since this is a programming
    /// error rather than a fallible parse — use [`QName::parse`] to validate
    /// untrusted input.
    #[must_use]
    pub fn new(namespace: impl Into<SmolStr>, local: impl Into<SmolStr>) -> Self {
        let namespace = namespace.into();
        let local = local.into();
        assert!(!namespace.is_empty(), "QName namespace must not be empty");
        assert!(!local.is_empty(), "QName local must not be empty");
        Self { namespace, local }
    }

    /// Construct a `QName` in the [`QName::BUILTIN_NS`] namespace.
    ///
    /// Shorthand for built-in registrations.
    ///
    /// # Examples
    ///
    /// ```
    /// use uni_plugin::QName;
    /// let q = QName::builtin("MIN");
    /// assert_eq!(q.namespace(), "builtin");
    /// assert_eq!(q.local(), "MIN");
    /// ```
    #[must_use]
    pub fn builtin(local: impl Into<SmolStr>) -> Self {
        Self::new(Self::BUILTIN_NS, local)
    }

    /// Parse a fully-qualified name like `"ai.dragonscale.geo.haversine"`.
    ///
    /// The last segment (after the final `.`) is taken as the local name; the
    /// preceding segments are joined back as the namespace. A namespace with
    /// no `.` (e.g. `"builtin.MIN"`) is also accepted.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::InvalidQName`] if the input lacks a `.`, or if
    /// either side of the final `.` is empty.
    pub fn parse(s: impl AsRef<str>) -> Result<Self, PluginError> {
        let s = s.as_ref();
        let (ns, local) = s
            .rsplit_once('.')
            .ok_or_else(|| PluginError::InvalidQName(s.to_owned()))?;
        if ns.is_empty() || local.is_empty() {
            return Err(PluginError::InvalidQName(s.to_owned()));
        }
        Ok(Self {
            namespace: SmolStr::new(ns),
            local: SmolStr::new(local),
        })
    }

    /// Every way to split a dotted name into `(namespace, local)`, yielded
    /// from the **first** dot to the **last** dot.
    ///
    /// Resolution must be convention-agnostic because two registration
    /// conventions coexist: dynamic loaders register the *whole* (possibly
    /// dotted) plugin id as the namespace (`ai.example` + `myfn` ⇒
    /// `("ai.example", "myfn")`, a last-dot split), while the M9-declared and
    /// builtin/apoc paths use a first-dot split (`apoc-core` + `bitwise.and`,
    /// `uni` + `plugin.declareAggregate`). Neither a pure first-dot nor a pure
    /// last-dot split resolves both. A caller looks up each candidate against
    /// the registry (exact `QName` keyed) and takes the first hit.
    ///
    /// First-dot is yielded first so that, in the (vanishingly unlikely) event
    /// two registrations would both match, resolution stays identical to the
    /// historical `split_once('.')` behavior.
    ///
    /// A name with no `.` (or with an empty side at every split) yields nothing.
    ///
    /// ```
    /// # use uni_plugin::QName;
    /// let cands: Vec<_> = QName::candidate_splits("a.b.c").collect();
    /// assert_eq!(cands, vec![QName::new("a", "b.c"), QName::new("a.b", "c")]);
    /// assert_eq!(QName::candidate_splits("bare").count(), 0);
    /// ```
    pub fn candidate_splits(name: &str) -> impl Iterator<Item = QName> + '_ {
        name.match_indices('.').filter_map(move |(i, _)| {
            let (ns, local) = (&name[..i], &name[i + 1..]);
            if ns.is_empty() || local.is_empty() {
                None
            } else {
                Some(QName::new(ns, local))
            }
        })
    }

    /// Returns the namespace portion (the plugin id).
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the local portion (the per-plugin item name).
    #[must_use]
    pub fn local(&self) -> &str {
        &self.local
    }

    /// Returns `true` if this name is in the reserved built-in namespace.
    #[must_use]
    pub fn is_builtin(&self) -> bool {
        self.namespace == Self::BUILTIN_NS
    }

    /// Cypher-style case-insensitive equality.
    ///
    /// Cypher function-call sites compare names case-insensitively
    /// (`toUpper` and `TOUPPER` resolve identically). Locy uses
    /// [`PartialEq`] (case-sensitive) directly.
    #[must_use]
    pub fn matches_cypher(&self, other: &Self) -> bool {
        self.namespace.eq_ignore_ascii_case(&other.namespace)
            && self.local.eq_ignore_ascii_case(&other.local)
    }
}

impl fmt::Display for QName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.local)
    }
}

impl FromStr for QName {
    type Err = PluginError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple() {
        let q = QName::parse("foo.bar").unwrap();
        assert_eq!(q.namespace(), "foo");
        assert_eq!(q.local(), "bar");
    }

    #[test]
    fn parse_nested_namespace() {
        let q = QName::parse("ai.dragonscale.geo.haversine").unwrap();
        assert_eq!(q.namespace(), "ai.dragonscale.geo");
        assert_eq!(q.local(), "haversine");
    }

    #[test]
    fn parse_rejects_empty_local() {
        assert!(matches!(
            QName::parse("foo."),
            Err(PluginError::InvalidQName(_))
        ));
    }

    #[test]
    fn parse_rejects_empty_namespace() {
        assert!(matches!(
            QName::parse(".bar"),
            Err(PluginError::InvalidQName(_))
        ));
    }

    #[test]
    fn parse_rejects_no_dot() {
        assert!(matches!(
            QName::parse("nodothere"),
            Err(PluginError::InvalidQName(_))
        ));
    }

    #[test]
    fn builtin_helper() {
        let q = QName::builtin("MIN");
        assert!(q.is_builtin());
        assert_eq!(q.local(), "MIN");
    }

    #[test]
    fn candidate_splits_orders_first_dot_to_last() {
        let cands: Vec<_> = QName::candidate_splits("a.b.c").collect();
        assert_eq!(
            cands,
            vec![QName::new("a", "b.c"), QName::new("a.b", "c")],
            "candidates must run first-dot → last-dot"
        );
    }

    #[test]
    fn candidate_splits_single_dot() {
        let cands: Vec<_> = QName::candidate_splits("mycorp.fn").collect();
        assert_eq!(cands, vec![QName::new("mycorp", "fn")]);
    }

    #[test]
    fn candidate_splits_covers_both_registration_conventions() {
        // Dotted-id loader plugin registers ("ai.example", "agg") — last-dot;
        // M9 declared registers ("ai", "example.agg") — first-dot. Both forms
        // must appear among the candidates so resolution finds whichever the
        // registry actually holds.
        let cands: Vec<_> = QName::candidate_splits("ai.example.agg").collect();
        assert!(cands.contains(&QName::new("ai", "example.agg")));
        assert!(cands.contains(&QName::new("ai.example", "agg")));
    }

    #[test]
    fn candidate_splits_skips_empty_sides_and_bare_names() {
        assert_eq!(QName::candidate_splits("bare").count(), 0);
        assert_eq!(QName::candidate_splits(".bar").count(), 0);
        assert_eq!(QName::candidate_splits("foo.").count(), 0);
        // The interior split of "a..b" has an empty side on each adjacent dot.
        let cands: Vec<_> = QName::candidate_splits("a..b").collect();
        assert_eq!(cands, vec![QName::new("a", ".b"), QName::new("a.", "b")]);
    }

    #[test]
    fn cypher_match_case_insensitive() {
        let a = QName::builtin("toUpper");
        let b = QName::builtin("TOUPPER");
        assert!(a.matches_cypher(&b));
        assert_ne!(a, b);
    }

    #[test]
    fn display_round_trip() {
        let q = QName::new("foo.bar", "baz");
        assert_eq!(q.to_string(), "foo.bar.baz");
        let parsed: QName = "foo.bar.baz".parse().unwrap();
        assert_eq!(q, parsed);
    }
}
