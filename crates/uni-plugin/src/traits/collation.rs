//! Collation (sort order) plugins.

use std::cmp::Ordering;

/// A custom collation — sort order for string comparison.
pub trait CollationProvider: Send + Sync {
    /// Collation name (`"icu.en_US"`, `"case_insensitive_ascii"`, …).
    fn name(&self) -> &str;

    /// Compare two strings under this collation.
    fn compare(&self, a: &str, b: &str) -> Ordering;

    /// Whether this collation supports substring search (for FTS / LIKE
    /// compatibility).
    fn supports_substring_search(&self) -> bool {
        true
    }

    /// Canonicalize a string for index lookups (e.g., lowercase + NFC).
    fn normalize(&self, s: &str) -> String {
        s.to_owned()
    }
}
